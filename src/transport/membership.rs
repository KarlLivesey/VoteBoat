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
//! Connection admission derived from validated local cores, not address hints.
use super::*;
use crate::{
    identity::*,
    raft::{Raft, RaftError},
};
use std::collections::BTreeMap;
#[derive(Clone, Debug, Eq, PartialEq)]
pub enum PeerAssignmentsError {
    InvalidLimits,
    WrongBinding,
    ConflictingStore,
    Overloaded,
    Core(RaftError),
}
/// Ephemeral snapshot of required exact peers across all supplied hosted groups.
/// Rebuild at the serialized owner before reconciliation; this is not a durable
/// certificate or permission to change any core's membership.
pub struct PeerAssignments {
    local: LocalIdentity,
    peers: BTreeMap<NodeId, StoreIdentity>,
}
impl PeerAssignments {
    pub fn from_cores<'a>(
        local: LocalIdentity,
        cores: impl IntoIterator<Item = &'a Raft>,
        limit: usize,
    ) -> Result<Self, PeerAssignmentsError> {
        if limit == 0 || limit > 65536 {
            return Err(PeerAssignmentsError::InvalidLimits);
        }
        let mut peers = BTreeMap::new();
        for core in cores {
            if core.local_node() != local.node || core.storage_binding() != local.store {
                return Err(PeerAssignmentsError::WrongBinding);
            }
            for (node, store) in core
                .connection_replicas(limit.saturating_add(1))
                .map_err(PeerAssignmentsError::Core)?
            {
                if node == local.node {
                    continue;
                }
                if peers.get(&node).is_some_and(|old| *old != store) {
                    return Err(PeerAssignmentsError::ConflictingStore);
                }
                if !peers.contains_key(&node) && peers.len() == limit {
                    return Err(PeerAssignmentsError::Overloaded);
                }
                peers.insert(node, store);
            }
        }
        Ok(Self { local, peers })
    }
    pub fn local(&self) -> LocalIdentity {
        self.local
    }
    pub fn store(&self, node: NodeId) -> Option<StoreIdentity> {
        self.peers.get(&node).copied()
    }
    pub fn peers(&self) -> impl Iterator<Item = (NodeId, StoreIdentity)> + '_ {
        self.peers.iter().map(|(&n, &s)| (n, s))
    }
}
