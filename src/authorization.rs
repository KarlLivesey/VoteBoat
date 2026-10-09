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
//! Service principals and scopes, independent of voting and ownership authority.
use crate::{identity::GroupIdentity, runtime::MonoTime, secure::*};
use std::num::NonZeroU64;
pub const SERVICE_AUTHORIZATION_CONTRACT_VERSION: u32 = 1;
macro_rules! checked_id {
    ($name:ident) => {
        #[derive(Clone, Copy, Debug, Eq, PartialEq, Ord, PartialOrd)]
        pub struct $name(NonZeroU64);
        impl $name {
            pub fn new(value: u64) -> Option<Self> {
                NonZeroU64::new(value).map(Self)
            }
            pub fn get(self) -> u64 {
                self.0.get()
            }
        }
    };
}
checked_id!(PrincipalId);
checked_id!(CredentialGeneration);
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum ServiceAction {
    Inspect,
    Read,
    Write,
    Checkpoint,
    Shutdown,
    Configure,
}
impl ServiceAction {
    fn bit(self) -> u8 {
        1 << (self as u8)
    }
}
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub struct ServicePermissions(u8);
impl ServicePermissions {
    pub const READER: Self = Self(3);
    pub const WRITER: Self = Self(7);
    pub const ADMIN: Self = Self(63);
    pub fn from_bits(bits: u8) -> Option<Self> {
        (bits > 0 && bits & !63 == 0).then_some(Self(bits))
    }
    pub fn contains(self, action: ServiceAction) -> bool {
        self.0 & action.bit() != 0
    }
}
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum AuthorizationError {
    InvalidPlan,
    UnknownPrincipal,
    WrongBinding,
    WrongGeneration,
    Expired,
    ClockRegressed,
    Denied,
    Closed,
    Session(SessionError),
}
/// Host-local snapshot, never an untrusted bearer credential. Only a credential
/// provider may attest it after authenticated channel verification.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub struct BoundPrincipal {
    pub principal: PrincipalId,
    pub session: SessionBinding,
    pub generation: CredentialGeneration,
    pub issued_at: MonoTime,
    pub expires_at: MonoTime,
}
#[derive(Clone, Copy, Debug)]
pub struct AuthorizationRequest {
    pub principal: BoundPrincipal,
    pub scope: GroupIdentity,
    pub action: ServiceAction,
    pub now: MonoTime,
}
#[derive(Clone, Copy, Debug)]
pub struct CredentialContext {
    pub generation: CredentialGeneration,
    pub authenticated_at: MonoTime,
}
/// Bounded nonblocking binding to the current authenticated channel. The host
/// captures generation when constructing that channel, not from network text.
/// Replacement snapshots use a new generation; an old channel cannot silently
/// acquire fresh credential authority. Closing a view preserves other views.
pub trait PrincipalCredentials {
    fn bind(
        &self,
        session: &dyn SecureSession,
        context: CredentialContext,
        now: MonoTime,
    ) -> Result<BoundPrincipal, AuthorizationError>;
    fn close(&mut self);
}
/// Authorization is checked before service admission. It cannot grant voting,
/// committed ownership, term authority or undo an already accepted operation.
pub trait ServiceAuthorizer {
    fn authorize(&self, request: AuthorizationRequest) -> Result<(), AuthorizationError>;
}
/// Public/native common gate. Recheck provider results before executing any
/// caller-owned command; a successful result is not a consensus receipt.
pub fn authorize_session(
    credentials: &impl PrincipalCredentials,
    policy: &impl ServiceAuthorizer,
    session: &dyn SecureSession,
    context: CredentialContext,
    scope: GroupIdentity,
    action: ServiceAction,
    now: MonoTime,
) -> Result<BoundPrincipal, AuthorizationError> {
    let binding = require_authenticated(session).map_err(AuthorizationError::Session)?;
    let principal = credentials.bind(session, context, now)?;
    if principal.session != binding || session.binding() != Some(binding) {
        return Err(AuthorizationError::WrongBinding);
    }
    if principal.generation != context.generation {
        return Err(AuthorizationError::WrongGeneration);
    }
    if principal.issued_at != context.authenticated_at || now < principal.issued_at {
        return Err(AuthorizationError::ClockRegressed);
    }
    if now >= principal.expires_at || principal.expires_at <= principal.issued_at {
        return Err(AuthorizationError::Expired);
    }
    // Providers must not turn a failed/changed session into a principal.
    if require_authenticated(session).map_err(AuthorizationError::Session)? != binding {
        return Err(AuthorizationError::WrongBinding);
    }
    policy.authorize(AuthorizationRequest {
        principal,
        scope,
        action,
        now,
    })?;
    if require_authenticated(session).map_err(AuthorizationError::Session)? != binding {
        return Err(AuthorizationError::WrongBinding);
    }
    Ok(principal)
}
