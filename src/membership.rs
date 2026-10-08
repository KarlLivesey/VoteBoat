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
//! Accepted-log configuration journal. Quorum evaluation is mandatory protocol
//! logic; this is not a replaceable policy callback or a live configuration API.
use crate::{
    identity::*,
    log::{Bootstrap, EntryPayload, LogEntry},
    quorum::{Limits, Policy, Tree},
};
use std::collections::{BTreeMap, BTreeSet};

/// Retain operation identities across compaction without unbounded tombstones.
/// Exhaustion rejects further transitions; identities are never silently evicted.
pub const MAX_CONFIGURATION_OPERATIONS: usize = 16384;

/// A stable configuration, including separate non-voting learners. Construction
/// bounds the combined replica set and validates every durable store binding.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct Configuration {
    id: ConfigurationId,
    policy: Policy,
    voter_stores: BTreeMap<NodeId, StoreIdentity>,
    learners: BTreeMap<NodeId, StoreIdentity>,
}
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum MembershipError {
    InvalidConfiguration,
    StaleConfiguration,
    TransitionInProgress,
    ReusedOperation,
    LearnerChangesVoters,
    UnpreparedVoter,
    ChangedVoterStore,
    JointNotCommitted,
    InvalidFinal,
    InvalidHistory,
    HistoryFull,
}
impl Configuration {
    pub fn new(
        id: ConfigurationId,
        policy: Policy,
        voter_stores: BTreeMap<NodeId, StoreIdentity>,
        learners: BTreeMap<NodeId, StoreIdentity>,
    ) -> Result<Self, MembershipError> {
        Policy::new(policy.tree().clone(), Limits::default())
            .map_err(|_| MembershipError::InvalidConfiguration)?;
        if !voter_stores.keys().eq(policy.voters().iter())
            || learners.keys().any(|n| voter_stores.contains_key(n))
            || voter_stores.len().saturating_add(learners.len()) > Limits::default().max_voters
        {
            return Err(MembershipError::InvalidConfiguration);
        }
        Ok(Self {
            id,
            policy,
            voter_stores,
            learners,
        })
    }
    pub fn id(&self) -> ConfigurationId {
        self.id
    }
    pub fn policy(&self) -> &Policy {
        &self.policy
    }
    pub fn voter_stores(&self) -> &BTreeMap<NodeId, StoreIdentity> {
        &self.voter_stores
    }
    pub fn learners(&self) -> &BTreeMap<NodeId, StoreIdentity> {
        &self.learners
    }
    /// Conservative retained allocation charge (including tree/vector capacity
    /// and B-tree nodes); also bounds persistence/range admission for this data.
    pub fn retained_bytes(&self) -> usize {
        fn tree(t: &Tree) -> usize {
            match t {
                Tree::Voter(_) => 0,
                Tree::Majority(c) => c
                    .capacity()
                    .saturating_mul(std::mem::size_of::<Tree>())
                    .saturating_add(c.iter().map(tree).fold(0usize, usize::saturating_add)),
                Tree::Weighted(c) => c
                    .capacity()
                    .saturating_mul(std::mem::size_of::<crate::quorum::WeightedChild>())
                    .saturating_add(
                        c.iter()
                            .map(|c| tree(&c.node))
                            .fold(0usize, usize::saturating_add),
                    ),
            }
        }
        std::mem::size_of::<Self>()
            .saturating_add(tree(self.policy.tree()))
            .saturating_add(self.policy.voters().len().saturating_mul(128))
            .saturating_add(
                self.voter_stores
                    .len()
                    .saturating_add(self.learners.len())
                    .saturating_mul(192),
            )
    }
}

/// All IDs are replicated data. IDs strictly increase through learner, joint,
/// and final records; final uses the target ID reserved in its joint record.
#[derive(Clone, Debug, Eq, PartialEq)]
pub enum ConfigurationChange {
    Learners(Configuration),
    Joint {
        id: ConfigurationId,
        next: Configuration,
    },
    Final {
        id: ConfigurationId,
    },
}
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct ConfigurationRecord {
    pub operation: OperationId,
    pub expected: ConfigurationId,
    pub change: ConfigurationChange,
}
impl ConfigurationRecord {
    pub fn retained_bytes(&self) -> usize {
        std::mem::size_of::<Self>().saturating_add(match &self.change {
            ConfigurationChange::Learners(c) | ConfigurationChange::Joint { next: c, .. } => {
                c.retained_bytes()
            }
            ConfigurationChange::Final { .. } => 0,
        })
    }
}
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct JointConfiguration {
    pub operation: OperationId,
    pub id: ConfigurationId,
    /// Index of the joint record, a contiguous-log boundary, not a maximum ack.
    pub index: u64,
    pub next: Configuration,
}
/// Recomputed from the surviving accepted log. This value is not a durability
/// receipt. The caller scopes/authenticates every ballot/prefix before using it.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct Membership {
    stable: Configuration,
    joint: Option<JointConfiguration>,
    last_index: u64,
    operations: BTreeSet<OperationId>,
}
impl Membership {
    pub fn replay(
        bootstrap: &Bootstrap,
        entries: &[LogEntry],
        committed_prefix: u64,
    ) -> Result<Self, MembershipError> {
        Self::replay_from(bootstrap, None, 0, entries, committed_prefix)
    }
    pub fn replay_from(
        bootstrap: &Bootstrap,
        base: Option<&Self>,
        base_index: u64,
        entries: &[LogEntry],
        committed_prefix: u64,
    ) -> Result<Self, MembershipError> {
        if committed_prefix < base_index {
            return Err(MembershipError::InvalidHistory);
        }
        let mut result = if let Some(base) = base {
            base.validate_checkpoint(bootstrap, base_index)?;
            base.clone()
        } else {
            Self {
                stable: Configuration::new(
                    bootstrap.configuration,
                    bootstrap.policy.clone(),
                    bootstrap.voter_stores.clone(),
                    BTreeMap::new(),
                )?,
                joint: None,
                last_index: 0,
                operations: BTreeSet::new(),
            }
        };
        let mut previous = base_index;
        for entry in entries {
            if previous.checked_add(1) != Some(entry.index) {
                return Err(MembershipError::InvalidHistory);
            }
            previous = entry.index;
            if let EntryPayload::Configuration(record) = &entry.payload {
                result.accept(entry.index, record, committed_prefix)?;
            }
        }
        Ok(result)
    }
    pub(crate) fn validate_next(
        &self,
        index: u64,
        record: &ConfigurationRecord,
        committed_prefix: u64,
    ) -> Result<(), MembershipError> {
        let mut next = self.clone();
        next.accept(index, record, committed_prefix)
    }
    fn accept(
        &mut self,
        index: u64,
        record: &ConfigurationRecord,
        committed_prefix: u64,
    ) -> Result<(), MembershipError> {
        if record.expected != self.id() {
            return Err(MembershipError::StaleConfiguration);
        }
        match &record.change {
            ConfigurationChange::Final { id } => {
                let joint = self.joint.as_ref().ok_or(MembershipError::InvalidFinal)?;
                if record.operation != joint.operation || *id != joint.next.id {
                    return Err(MembershipError::InvalidFinal);
                }
                if joint.index > committed_prefix {
                    return Err(MembershipError::JointNotCommitted);
                }
                self.stable = joint.next.clone();
                self.joint = None;
            }
            change => {
                if self.joint.is_some() || self.last_index > committed_prefix {
                    return Err(MembershipError::TransitionInProgress);
                }
                if self.operations.contains(&record.operation) {
                    return Err(MembershipError::ReusedOperation);
                }
                if self.operations.len() == MAX_CONFIGURATION_OPERATIONS {
                    return Err(MembershipError::HistoryFull);
                }
                match change {
                    ConfigurationChange::Learners(next) => {
                        if next.id <= self.id() {
                            return Err(MembershipError::StaleConfiguration);
                        }
                        if next.policy != self.stable.policy
                            || next.voter_stores != self.stable.voter_stores
                        {
                            return Err(MembershipError::LearnerChangesVoters);
                        }
                        self.stable = next.clone();
                    }
                    ConfigurationChange::Joint { id, next } => {
                        if *id <= self.id() || next.id <= *id {
                            return Err(MembershipError::StaleConfiguration);
                        }
                        for (node, store) in &next.voter_stores {
                            match self.stable.voter_stores.get(node) {
                                Some(old) if old != store => {
                                    return Err(MembershipError::ChangedVoterStore)
                                }
                                Some(_) => (),
                                None if self.stable.learners.get(node) != Some(store) => {
                                    return Err(MembershipError::UnpreparedVoter)
                                }
                                None => (),
                            }
                        }
                        // A node cannot supply two different stores in the old
                        // and new replication sets, including retained learners.
                        for (node, store) in &next.learners {
                            if self
                                .stable
                                .voter_stores
                                .get(node)
                                .or_else(|| self.stable.learners.get(node))
                                .is_some_and(|old| old != store)
                            {
                                return Err(MembershipError::ChangedVoterStore);
                            }
                        }
                        let members: BTreeSet<_> = self
                            .stable
                            .voter_stores
                            .keys()
                            .chain(self.stable.learners.keys())
                            .chain(next.voter_stores.keys())
                            .chain(next.learners.keys())
                            .collect();
                        if members.len() > Limits::default().max_voters {
                            return Err(MembershipError::InvalidConfiguration);
                        }
                        self.joint = Some(JointConfiguration {
                            operation: record.operation,
                            id: *id,
                            index,
                            next: next.clone(),
                        });
                    }
                    ConfigurationChange::Final { .. } => unreachable!(),
                }
                self.operations.insert(record.operation);
            }
        }
        self.last_index = index;
        Ok(())
    }
    pub fn id(&self) -> ConfigurationId {
        self.joint.as_ref().map_or(self.stable.id, |j| j.id)
    }
    pub fn stable(&self) -> &Configuration {
        &self.stable
    }
    pub fn joint(&self) -> Option<&JointConfiguration> {
        self.joint.as_ref()
    }
    pub fn last_configuration_index(&self) -> u64 {
        self.last_index
    }
    pub fn operations(&self) -> &BTreeSet<OperationId> {
        &self.operations
    }
    /// Restore a bounded checkpoint, then bind it to bootstrap with
    /// validate_checkpoint. This validates structure, not remote provenance.
    pub fn from_checkpoint(
        stable: Configuration,
        joint: Option<JointConfiguration>,
        last_index: u64,
        operations: BTreeSet<OperationId>,
        boundary: u64,
    ) -> Result<Self, MembershipError> {
        if last_index > boundary
            || operations.len() > MAX_CONFIGURATION_OPERATIONS
            || operations.len() as u64 > last_index
            || (last_index == 0) != operations.is_empty()
        {
            return Err(MembershipError::InvalidHistory);
        }
        if let Some(joint) = &joint {
            if joint.index != last_index || !operations.contains(&joint.operation) {
                return Err(MembershipError::InvalidHistory);
            }
            let mut check = Self {
                stable: stable.clone(),
                joint: None,
                last_index: 0,
                operations: BTreeSet::new(),
            };
            check.accept(
                joint.index,
                &ConfigurationRecord {
                    operation: joint.operation,
                    expected: stable.id,
                    change: ConfigurationChange::Joint {
                        id: joint.id,
                        next: joint.next.clone(),
                    },
                },
                boundary,
            )?;
        }
        Ok(Self {
            stable,
            joint,
            last_index,
            operations,
        })
    }
    pub fn validate_checkpoint(
        &self,
        bootstrap: &Bootstrap,
        boundary: u64,
    ) -> Result<(), MembershipError> {
        Self::from_checkpoint(
            self.stable.clone(),
            self.joint.clone(),
            self.last_index,
            self.operations.clone(),
            boundary,
        )?;
        let initial = Configuration::new(
            bootstrap.configuration,
            bootstrap.policy.clone(),
            bootstrap.voter_stores.clone(),
            BTreeMap::new(),
        )?;
        if self.stable.id < initial.id
            || (self.stable.id == initial.id && self.stable != initial)
            || (self.last_index == 0 && (self.stable != initial || self.joint.is_some()))
            || (self.last_index > 0 && self.joint.is_none() && self.stable.id == initial.id)
        {
            return Err(MembershipError::InvalidHistory);
        }
        Ok(())
    }
    pub fn retained_bytes(&self) -> usize {
        std::mem::size_of::<Self>()
            .saturating_add(self.stable.retained_bytes())
            .saturating_add(self.joint.as_ref().map_or(0, |j| j.next.retained_bytes()))
            .saturating_add(self.operations.len().saturating_mul(192))
    }
    /// Exact store allowed to cast a ballot in either active joint predicate.
    /// Learner assignment alone never grants voting authority.
    pub fn voter_store(&self, node: NodeId) -> Option<StoreIdentity> {
        self.stable.voter_stores.get(&node).copied().or_else(|| {
            self.joint
                .as_ref()
                .and_then(|j| j.next.voter_stores.get(&node).copied())
        })
    }
    /// Replication identity, including learners and both sides of a joint state.
    pub fn replica_store(&self, node: NodeId) -> Option<StoreIdentity> {
        self.voter_store(node)
            .or_else(|| self.stable.learners.get(&node).copied())
            .or_else(|| {
                self.joint
                    .as_ref()
                    .and_then(|j| j.next.learners.get(&node).copied())
            })
    }
    /// Each replication identity exactly once, without allocating a union map.
    /// Iteration is deterministic; it is not globally sorted across roles.
    pub fn replicas(&self) -> impl Iterator<Item = (NodeId, StoreIdentity)> + '_ {
        let old = self
            .stable
            .voter_stores
            .iter()
            .chain(self.stable.learners.iter());
        let new = self
            .joint
            .iter()
            .flat_map(|j| j.next.voter_stores.iter().chain(j.next.learners.iter()))
            .filter(|(node, _)| {
                !self.stable.voter_stores.contains_key(node)
                    && !self.stable.learners.contains_key(node)
            });
        old.chain(new).map(|(node, store)| (*node, *store))
    }
    pub fn is_voter(&self, node: NodeId) -> bool {
        self.stable.policy.voters().contains(&node)
            || self
                .joint
                .as_ref()
                .is_some_and(|j| j.next.policy.voters().contains(&node))
    }
    pub fn is_satisfied(&self, acknowledgements: &BTreeSet<NodeId>) -> bool {
        self.stable.policy.is_satisfied(acknowledgements)
            && self
                .joint
                .as_ref()
                .is_none_or(|j| j.next.policy.is_satisfied(acknowledgements))
    }
    /// Greatest matching durable contiguous prefix satisfying both policies.
    /// Term/local persistence checks remain the consensus core's obligation.
    pub fn frontier(&self, prefixes: &BTreeMap<NodeId, u64>) -> u64 {
        let old = self.stable.policy.frontier(prefixes);
        self.joint
            .as_ref()
            .map_or(old, |j| old.min(j.next.policy.frontier(prefixes)))
    }
}
