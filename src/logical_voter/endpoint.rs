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
//! Immutable routes derived only from the selected original backing ownership.
use super::*;
use crate::{raft::Raft, runtime::RuntimeAuthorization, secure::LocalIdentity};
mod hop;
mod shape;
/// This initial composition profile supports at most eight logical backing hops.
pub const LOGICAL_ENDPOINT_MAX_DEPTH: usize = 8;
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum LogicalEndpointError {
    WrongBinding,
    StaleOwner,
    TooDeep,
    Cycle,
    DuplicateIdentity,
    InvalidObservation,
}
/// Diagnostic provenance for one logical hop. Copies grant no runtime authority.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub struct LogicalEndpointHop {
    mapping: LogicalStoreMapping,
    representative: RepresentativeBinding,
    claim: LogicalReceipt,
    barrier: crate::raft::ReadBarrier,
    child_applied: u64,
    child_configuration: ConfigurationId,
    eligible: crate::secure::PeerIdentity,
}
/// A bounded sealed route ending in an actual physical owner. There is no public
/// scalar constructor: a route cannot be supplied as a startup certificate.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct LogicalEndpointChain {
    hops: Vec<LogicalEndpointHop>,
    leaf_group: GroupIdentity,
    leaf: LocalIdentity,
}
impl LogicalEndpointChain {
    pub fn hops(&self) -> &[LogicalEndpointHop] {
        &self.hops
    }
    pub fn leaf(&self) -> LocalIdentity {
        self.leaf
    }
    pub fn leaf_group(&self) -> GroupIdentity {
        self.leaf_group
    }
    pub fn depth(&self) -> usize {
        self.hops.len()
    }
    pub(crate) fn physical(
        core: &Raft,
        authorization: RuntimeAuthorization,
    ) -> Result<Self, LogicalEndpointError> {
        if authorization.is_logical() {
            return Err(LogicalEndpointError::WrongBinding);
        }
        let chain = Self {
            hops: Vec::new(),
            leaf_group: core.state().bootstrap.group,
            leaf: LocalIdentity {
                node: core.local_node(),
                store: authorization.owner().store,
            },
        };
        chain.current(authorization, core)
    }
    /// Facades use their actual current core and authorization. An unchanged
    /// scalar store identity cannot substitute for a current logical fence.
    pub(crate) fn current(
        &self,
        authorization: RuntimeAuthorization,
        core: &Raft,
    ) -> Result<Self, LogicalEndpointError> {
        self.validate()?;
        let local = LocalIdentity {
            node: core.local_node(),
            store: authorization.owner().store,
        };
        let eligible = if authorization.is_logical() {
            core.membership().replica_store(local.node) == Some(local.store.identity)
                && core
                    .state()
                    .membership_at(core.state().commit_index)
                    .is_ok_and(|m| m.replica_store(local.node) == Some(local.store.identity))
        } else {
            core.membership().voter_store(local.node) == Some(local.store.identity)
        };
        if self.owner_group() != core.state().bootstrap.group
            || self.owner() != local
            || core.storage_binding() != local.store
            || !eligible
            || !self.matches_authorization(authorization)
        {
            return Err(LogicalEndpointError::StaleOwner);
        }
        Ok(self.clone())
    }
    pub(crate) fn matches_authorization(&self, a: RuntimeAuthorization) -> bool {
        self.owner().store == a.owner().store
            && match self.hops.first() {
                Some(h) => a.logical().is_some_and(|l| {
                    l.mapping == h.mapping && l.representative == h.representative
                }),
                None => !a.is_logical(),
            }
    }
    pub(super) fn preflight(
        &self,
        mapping: LogicalStoreMapping,
    ) -> Result<(), LogicalEndpointError> {
        self.validate()?;
        if self.hops.len() >= LOGICAL_ENDPOINT_MAX_DEPTH {
            return Err(LogicalEndpointError::TooDeep);
        }
        let mut mappings = Vec::with_capacity(self.hops.len() + 1);
        mappings.push(mapping);
        mappings.extend(self.hops.iter().map(|h| h.mapping));
        shape::validate(&mappings, self.leaf_group, self.leaf)
    }
    pub(super) fn extend_observed(
        &self,
        image: &LogicalImage,
        claim: LogicalReceipt,
        barrier: crate::raft::ReadBarrier,
    ) -> Result<Self, LogicalEndpointError> {
        self.preflight(image.mapping)?;
        let hop = LogicalEndpointHop::observed(image, claim, barrier)?;
        if hop.immediate() != self.owner() || image.mapping.child != self.owner_group() {
            return Err(LogicalEndpointError::WrongBinding);
        }
        let mut hops = Vec::with_capacity(self.hops.len() + 1);
        hops.push(hop);
        hops.extend_from_slice(&self.hops);
        let chain = Self {
            hops,
            leaf_group: self.leaf_group,
            leaf: self.leaf,
        };
        chain.validate()?;
        Ok(chain)
    }
    fn owner_group(&self) -> GroupIdentity {
        self.hops
            .first()
            .map_or(self.leaf_group, |h| h.mapping.parent)
    }
    fn owner(&self) -> LocalIdentity {
        self.hops
            .first()
            .map_or(self.leaf, LogicalEndpointHop::owner)
    }
    fn validate(&self) -> Result<(), LogicalEndpointError> {
        if self.hops.len() > LOGICAL_ENDPOINT_MAX_DEPTH {
            return Err(LogicalEndpointError::TooDeep);
        }
        let mappings = self.hops.iter().map(|h| h.mapping).collect::<Vec<_>>();
        shape::validate(&mappings, self.leaf_group, self.leaf)?;
        for (i, hop) in self.hops.iter().enumerate() {
            hop.validate()?;
            let next = self
                .hops
                .get(i + 1)
                .map_or(self.leaf, LogicalEndpointHop::owner);
            if hop.immediate() != next {
                return Err(LogicalEndpointError::WrongBinding);
            }
        }
        Ok(())
    }
}
