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
//! Owned queue/core peer reservations; no transport or protocol authority.
use super::*;
use crate::secure::LocalIdentity;

/// Fixed connection ceiling and retained exact identity history for one owner.
/// Seed history from all tracked roster records, including inactive peers.
/// New accepted/pending peers remain retained until a drained owner replacement;
/// queued events alone reserve capacity only while their payloads are owned.
#[derive(Clone, Debug)]
pub struct ConnectionBudget {
    local: LocalIdentity,
    limit: usize,
    history: BTreeMap<NodeId, StoreIdentity>,
    provisioned: Option<BTreeMap<NodeId, StoreIdentity>>,
}
impl ConnectionBudget {
    pub fn new(
        local: LocalIdentity,
        limit: usize,
        history: BTreeMap<NodeId, StoreIdentity>,
    ) -> Result<Self, RuntimeError> {
        if limit == 0 || limit > 65536 {
            return Err(RuntimeError::InvalidLimits);
        }
        let mut budget = Self {
            local,
            limit,
            history: BTreeMap::new(),
            provisioned: None,
        };
        for (node, store) in history {
            budget.retain(node, store)?;
        }
        Ok(budget)
    }
    pub fn local(&self) -> LocalIdentity {
        self.local
    }
    pub fn limit(&self) -> usize {
        self.limit
    }
    pub fn retained_peers(&self) -> impl Iterator<Item = (NodeId, StoreIdentity)> + '_ {
        self.history.iter().map(|(&n, &s)| (n, s))
    }
    pub fn provisioned_peers(&self) -> Option<impl Iterator<Item = (NodeId, StoreIdentity)> + '_> {
        self.provisioned
            .as_ref()
            .map(|peers| peers.iter().map(|(&n, &s)| (n, s)))
    }
    pub(super) fn with_provisioned(
        mut self,
        peers: BTreeMap<NodeId, StoreIdentity>,
    ) -> Result<Self, RuntimeError> {
        if peers.len() > 65536 || peers.contains_key(&self.local.node) {
            return Err(RuntimeError::InvalidLimits);
        }
        if peers
            .iter()
            .any(|(n, s)| self.history.get(n).is_some_and(|old| old != s))
        {
            return Err(RuntimeError::PeerStoreConflict);
        }
        self.provisioned = Some(peers);
        Ok(self)
    }
    fn retain(&mut self, node: NodeId, store: StoreIdentity) -> Result<(), RuntimeError> {
        add(&mut self.history, self.local, self.limit, node, store)
    }
}
fn add(
    peers: &mut BTreeMap<NodeId, StoreIdentity>,
    local: LocalIdentity,
    limit: usize,
    node: NodeId,
    store: StoreIdentity,
) -> Result<(), RuntimeError> {
    if node == local.node {
        return if store == local.store.identity {
            Ok(())
        } else {
            Err(RuntimeError::PeerStoreConflict)
        };
    }
    if peers.get(&node).is_some_and(|old| *old != store) {
        return Err(RuntimeError::PeerStoreConflict);
    }
    if !peers.contains_key(&node) && peers.len() == limit {
        return Err(RuntimeError::PeerCapacity);
    }
    peers.insert(node, store);
    Ok(())
}
impl<Q: ReadyScheduler> Shard<Q> {
    pub(super) fn reserved_connections(
        &self,
        budget: &ConnectionBudget,
        incoming: Option<(GroupIdentity, &Event)>,
        registering: Option<&Raft>,
    ) -> Result<BTreeMap<NodeId, StoreIdentity>, RuntimeError> {
        if budget.local.store != self.owner.store {
            return Err(RuntimeError::WrongOwner);
        }
        let mut peers = budget.history.clone();
        let mut inspect = |core: &Raft, event: Option<&Event>| -> Result<(), RuntimeError> {
            if core.local_node() != budget.local.node
                || core.storage_binding() != budget.local.store
            {
                return Err(RuntimeError::WrongOwner);
            }
            if matches!(event, Some(Event::Receive(m)) if m.group != core.state().bootstrap.group || m.to != budget.local.node)
            {
                return Err(RuntimeError::WrongOwner);
            }
            let required = match event {
                Some(e) => core.event_connection_replicas(e, budget.limit + 1),
                None => core.connection_replicas(budget.limit + 1),
            }
            .map_err(|e| match e {
                RaftError::WrongIdentity => RuntimeError::PeerStoreConflict,
                RaftError::Fenced => RuntimeError::Fenced,
                _ => RuntimeError::PeerCapacity,
            })?;
            for (node, store) in required {
                if node != budget.local.node
                    && budget
                        .provisioned
                        .as_ref()
                        .is_some_and(|p| p.get(&node) != Some(&store))
                {
                    return Err(RuntimeError::PeerUnavailable);
                }
                add(&mut peers, budget.local, budget.limit, node, store)?;
            }
            Ok(())
        };
        for group in self.groups.values().filter(|g| !g.fenced) {
            inspect(&group.core, None)?;
            for queued in group.queues.iter().flat_map(|q| q.iter()) {
                if changes_connections(&queued.event) {
                    inspect(&group.core, Some(&queued.event))?;
                }
            }
        }
        if let Some((group, event)) = incoming {
            let core = self.groups.get(&group).ok_or(RuntimeError::UnknownGroup)?;
            inspect(&core.core, Some(event))?;
        }
        if let Some(core) = registering {
            inspect(core, None)?;
        }
        Ok(peers)
    }
    /// Install/tighten only after the entire owned queue/core union fits. Failure
    /// preserves the previous budget. This grants no credentials or membership.
    pub fn set_connection_budget(
        &mut self,
        mut budget: ConnectionBudget,
    ) -> Result<(), RuntimeError> {
        // Public capacity changes cannot restore a stale cloned route policy.
        // Only the driver's pin-checked owned-plan path may replace that set.
        budget.provisioned = self
            .connection_budget
            .as_ref()
            .and_then(|b| b.provisioned.clone());
        let budget = self.prepare_connection_budget(budget)?;
        self.install_connection_budget(budget);
        Ok(())
    }
    pub(super) fn prepare_connection_budget(
        &self,
        mut budget: ConnectionBudget,
    ) -> Result<ConnectionBudget, RuntimeError> {
        if let Some(previous) = &self.connection_budget {
            if previous.local != budget.local {
                return Err(RuntimeError::WrongOwner);
            }
            for (&node, &store) in &previous.history {
                budget.retain(node, store)?;
            }
            if budget.provisioned.is_none() {
                budget.provisioned.clone_from(&previous.provisioned);
            }
            if budget.provisioned.as_ref().is_some_and(|p| {
                p.iter()
                    .any(|(n, s)| budget.history.get(n).is_some_and(|old| old != s))
            }) {
                return Err(RuntimeError::PeerStoreConflict);
            }
        }
        self.reserved_connections(&budget, None, None)?;
        for g in self.groups.values().filter(|g| !g.fenced) {
            for (node, store) in g
                .core
                .connection_replicas(budget.limit + 1)
                .map_err(|_| RuntimeError::PeerCapacity)?
            {
                budget.retain(node, store)?;
            }
        }
        Ok(budget)
    }
    pub(super) fn install_connection_budget(&mut self, budget: ConnectionBudget) {
        self.connection_budget = Some(budget);
    }
    pub fn connection_budget(&self) -> Option<&ConnectionBudget> {
        self.connection_budget.as_ref()
    }
    /// Current union includes queued future peers; history alone does not.
    pub fn reserved_connection_peers(&self) -> Result<Option<usize>, RuntimeError> {
        self.connection_budget
            .as_ref()
            .map(|b| self.reserved_connections(b, None, None).map(|p| p.len()))
            .transpose()
    }
    pub(super) fn check_connection_event(
        &self,
        group: GroupIdentity,
        event: &Event,
    ) -> Result<(), RuntimeError> {
        if let Some(budget) = &self.connection_budget {
            self.reserved_connections(budget, Some((group, event)), None)?;
        }
        Ok(())
    }
    pub(super) fn retain_connection_history(
        &mut self,
        group: GroupIdentity,
    ) -> Result<(), RuntimeError> {
        if let Some(budget) = &mut self.connection_budget {
            let core = &self.groups[&group].core;
            for (node, store) in core
                .connection_replicas(budget.limit + 1)
                .map_err(|_| RuntimeError::PeerCapacity)?
            {
                budget.retain(node, store)?;
            }
        }
        Ok(())
    }
}
pub(super) fn changes_connections(event: &Event) -> bool {
    matches!(event, Event::Receive(Message { rpc: Rpc::Append { entries, .. }, .. })
        if entries.iter().any(|e| matches!(e.payload, crate::log::EntryPayload::Configuration(_))))
        || matches!(
            event,
            Event::Receive(Message {
                rpc: Rpc::Snapshot { .. } | Rpc::AuthorityReply { granted: true, .. },
                ..
            })
        )
}
