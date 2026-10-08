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
//! Connection retention derived from committed and rollback-reachable views.
use super::*;
impl Raft {
    /// Exact stores needed by committed, accepted and pending rollback views.
    /// Connection permission is not voting, membership activation or durability.
    pub fn connection_replicas(
        &self,
        limit: usize,
    ) -> Result<BTreeMap<NodeId, StoreIdentity>, RaftError> {
        if self.fenced {
            return Err(RaftError::Fenced);
        }
        let mut peers = BTreeMap::new();
        let mut add = |node, store| -> Result<(), RaftError> {
            if peers.get(&node).is_some_and(|old| *old != store) {
                return Err(RaftError::WrongIdentity);
            }
            if !peers.contains_key(&node) && peers.len() == limit {
                return Err(StorageError::Rejected("connection peer budget").into());
            }
            peers.insert(node, store);
            Ok(())
        };
        for state in std::iter::once(&self.durable).chain(self.pending.iter().map(|p| &p.next)) {
            let committed = state
                .membership_at(state.commit_index)
                .map_err(|_| RaftError::InvalidRecovery)?;
            for (node, store) in committed.replicas() {
                add(node, store)?;
            }
            for entry in state
                .entries
                .iter()
                .filter(|entry| entry.index > state.commit_index)
            {
                if let EntryPayload::Configuration(record) = &entry.payload {
                    use crate::membership::ConfigurationChange;
                    let next = match &record.change {
                        ConfigurationChange::Learners(next)
                        | ConfigurationChange::Joint { next, .. } => Some(next),
                        ConfigurationChange::Final { .. } => None,
                    };
                    if let Some(next) = next {
                        for (&node, &store) in next.voter_stores().iter().chain(next.learners()) {
                            add(node, store)?;
                        }
                    }
                }
            }
        }
        Ok(peers)
    }
}
