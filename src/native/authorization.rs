// SPDX-License-Identifier: RPL-1.5
// Copyright (c) 2026 Karl Livesey
// Unless explicitly acquired and licensed from Licensor under another license,
// the contents of this file are subject to the Reciprocal Public License
// ("RPL") Version 1.5, or subsequent versions as allowed by the RPL, and You may
// not copy or use this file in either source code or executable form, except
// in compliance with the terms and conditions of the RPL.
//
// All software distributed under the RPL is provided strictly on an "AS IS"
// basis, WITHOUT WARRANTY OF ANY KIND, EITHER EXPRESS OR IMPLIED, AND LICENSOR
// HEREBY DISCLAIMS ALL SUCH WARRANTIES, INCLUDING WITHOUT LIMITATION, ANY
// WARRANTIES OF MERCHANTABILITY, FITNESS FOR A PARTICULAR PURPOSE, QUIET
// ENJOYMENT, OR NON-INFRINGEMENT. See the RPL for specific language governing
// rights and limitations under the RPL.
//! Immutable bounded authenticated-identity and service-scope access plan.
use crate::{authorization::*, identity::*, runtime::MonoTime, secure::*};
use std::{collections::BTreeMap, sync::Arc};
#[derive(Clone, Copy, Debug)]
pub struct ServiceAccess {
    pub principal: PrincipalId,
    pub peer: PeerIdentity,
    pub scope: GroupIdentity,
    pub permissions: ServicePermissions,
}
type PeerKey = (NodeId, StoreId, StoreIncarnation);
fn key(peer: PeerIdentity) -> PeerKey {
    (peer.node, peer.store.id, peer.store.incarnation)
}
struct Plan {
    generation: CredentialGeneration,
    lifetime_ms: u64,
    identities: BTreeMap<PeerKey, PrincipalId>,
    grants: BTreeMap<(PrincipalId, GroupIdentity), ServicePermissions>,
}
/// Clones share immutable policy, but close affects only that credential view.
/// Fresh snapshots/restarts must explicitly select their credential generation.
#[derive(Clone)]
pub struct NativeServiceAccess {
    plan: Arc<Plan>,
    closed: bool,
}
impl NativeServiceAccess {
    pub fn new(
        generation: CredentialGeneration,
        lifetime_ms: u64,
        entries: Vec<ServiceAccess>,
    ) -> Result<Self, (AuthorizationError, Vec<ServiceAccess>)> {
        if entries.is_empty()
            || entries.len() > 4096
            || entries.capacity() > 4096
            || lifetime_ms == 0
            || lifetime_ms > 3_600_000
        {
            return Err((AuthorizationError::InvalidPlan, entries));
        }
        let mut identities = BTreeMap::new();
        let mut principals = BTreeMap::new();
        let mut grants = BTreeMap::new();
        for entry in &entries {
            let peer = key(entry.peer);
            if identities.get(&peer).is_some_and(|p| *p != entry.principal)
                || principals.get(&entry.principal).is_some_and(|p| *p != peer)
                || grants
                    .insert((entry.principal, entry.scope), entry.permissions)
                    .is_some()
            {
                return Err((AuthorizationError::InvalidPlan, entries));
            }
            identities.insert(peer, entry.principal);
            principals.insert(entry.principal, peer);
        }
        Ok(Self {
            plan: Arc::new(Plan {
                generation,
                lifetime_ms,
                identities,
                grants,
            }),
            closed: false,
        })
    }
    pub fn generation(&self) -> CredentialGeneration {
        self.plan.generation
    }
}
impl PrincipalCredentials for NativeServiceAccess {
    fn bind(
        &self,
        session: &dyn SecureSession,
        context: CredentialContext,
        now: MonoTime,
    ) -> Result<BoundPrincipal, AuthorizationError> {
        if self.closed {
            return Err(AuthorizationError::Closed);
        }
        if context.generation != self.plan.generation {
            return Err(AuthorizationError::WrongGeneration);
        }
        let binding = require_authenticated(session).map_err(AuthorizationError::Session)?;
        let peer = PeerIdentity {
            node: binding.peer.node,
            store: binding.peer.store.identity,
        };
        let principal = *self
            .plan
            .identities
            .get(&key(peer))
            .ok_or(AuthorizationError::UnknownPrincipal)?;
        if now < context.authenticated_at {
            return Err(AuthorizationError::ClockRegressed);
        }
        let expires_at = MonoTime(
            context
                .authenticated_at
                .0
                .checked_add(self.plan.lifetime_ms)
                .ok_or(AuthorizationError::Expired)?,
        );
        if now >= expires_at {
            return Err(AuthorizationError::Expired);
        }
        Ok(BoundPrincipal {
            principal,
            session: binding,
            generation: context.generation,
            issued_at: context.authenticated_at,
            expires_at,
        })
    }
    fn close(&mut self) {
        self.closed = true;
    }
}
impl ServiceAuthorizer for NativeServiceAccess {
    fn authorize(&self, request: AuthorizationRequest) -> Result<(), AuthorizationError> {
        if self.closed {
            return Err(AuthorizationError::Closed);
        }
        let bound = request.principal;
        if bound.generation != self.plan.generation {
            return Err(AuthorizationError::WrongGeneration);
        }
        if request.now < bound.issued_at {
            return Err(AuthorizationError::ClockRegressed);
        }
        if request.now >= bound.expires_at
            || bound.expires_at <= bound.issued_at
            || bound.expires_at.0 - bound.issued_at.0 > self.plan.lifetime_ms
        {
            return Err(AuthorizationError::Expired);
        }
        let peer = PeerIdentity {
            node: bound.session.peer.node,
            store: bound.session.peer.store.identity,
        };
        if self.plan.identities.get(&key(peer)) != Some(&bound.principal) {
            return Err(AuthorizationError::WrongBinding);
        }
        if !self
            .plan
            .grants
            .get(&(bound.principal, request.scope))
            .is_some_and(|p| p.contains(request.action))
        {
            return Err(AuthorizationError::Denied);
        }
        Ok(())
    }
}
