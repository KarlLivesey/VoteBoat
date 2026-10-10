// SPDX-License-Identifier: RPL-1.5
// Copyright (c) 2026 Karl Livesey
use super::*;
use crate::{identity::*, membership::*, placement::PlannedVoterChange, raft::*};
use std::mem::size_of;
mod digest;
mod progress;
mod retained;
pub use progress::MembershipDrainAction;
pub use retained::DrainRetainedGroup;

pub const MAX_DRAIN_PLAN_BYTES: usize = 4 * 1024 * 1024;
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub struct DrainPlanDigest([u8; 32]);
impl DrainPlanDigest {
    pub fn from_bytes(bytes: [u8; 32]) -> Self {
        Self(bytes)
    }
    pub fn as_bytes(&self) -> &[u8; 32] {
        &self.0
    }
}
/// One voter evacuation. The source remains a non-voting learner long enough
/// to observe the final commit. Removing that learner is a later operation.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct DrainMembershipGroup {
    pub group: GroupIdentity,
    pub original: Configuration,
    pub handoff: PeerIdentity,
    pub change: PlannedVoterChange,
}
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum DrainPlanError {
    InvalidPlan,
    TooLarge,
    Journal(DrainJournalError),
    WrongIntent,
    Unrestored,
    UnknownGroup,
    StateChanged,
}
/// Immutable bounded plan. The host retains/reloads its original input and
/// verifies the journal binding before execution. This grants no placement,
/// readiness or configuration authorization and starts no background work.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct MembershipDrainPlan {
    owner: PeerIdentity,
    operation: OperationId,
    groups: Vec<DrainMembershipGroup>,
    retained: Vec<DrainRetainedGroup>,
    assignments: Vec<crate::runtime::DrainGroup>,
    digest: DrainPlanDigest,
}
impl MembershipDrainPlan {
    pub fn new(
        owner: PeerIdentity,
        operation: OperationId,
        groups: Vec<DrainMembershipGroup>,
    ) -> Result<Self, DrainPlanError> {
        Self::with_retained_learners(owner, operation, groups, Vec::new())
    }
    /// Retain already non-voting source assignments without inventing a
    /// membership operation. Both input lists must be sorted and disjoint.
    pub fn with_retained_learners(
        owner: PeerIdentity,
        operation: OperationId,
        groups: Vec<DrainMembershipGroup>,
        retained: Vec<DrainRetainedGroup>,
    ) -> Result<Self, DrainPlanError> {
        let count = groups.len().saturating_add(retained.len());
        if count == 0
            || count > MAX_LOCAL_DRAIN_GROUPS
            || groups.windows(2).any(|pair| pair[0].group >= pair[1].group)
            || retained
                .windows(2)
                .any(|pair| pair[0].group >= pair[1].group)
        {
            return Err(DrainPlanError::InvalidPlan);
        }
        let initial = groups
            .capacity()
            .checked_mul(size_of::<DrainMembershipGroup>())
            .ok_or(DrainPlanError::TooLarge)?;
        let bytes = groups.iter().try_fold(initial, |sum, entry| {
            sum.checked_add(entry.original.retained_bytes())?
                .checked_add(entry.change.joint.retained_bytes())?
                .checked_add(entry.change.finalize.retained_bytes())
        });
        let bytes = retained::retained_bytes(bytes, &retained, count);
        if bytes.is_none_or(|n| n > MAX_DRAIN_PLAN_BYTES) {
            return Err(DrainPlanError::TooLarge);
        }
        for entry in &groups {
            entry.validate(owner)?;
        }
        for entry in &retained {
            entry.validate(owner)?;
        }
        let assignments = retained::assignments(&groups, &retained)?;
        let digest = digest::with_retained(digest::plan(owner, operation, &groups), &retained);
        Ok(Self {
            owner,
            operation,
            groups,
            retained,
            assignments,
            digest,
        })
    }
    /// Voter evacuations only. Use `assignments` for the complete source set.
    pub fn groups(&self) -> &[DrainMembershipGroup] {
        &self.groups
    }
    pub fn retained_learners(&self) -> &[DrainRetainedGroup] {
        &self.retained
    }
    /// Complete sorted inventory, including already non-voting assignments.
    pub fn assignments(&self) -> &[crate::runtime::DrainGroup] {
        &self.assignments
    }
    pub fn digest(&self) -> DrainPlanDigest {
        self.digest
    }
    pub fn record(&self, sequence: u64) -> Result<DrainRecord, DrainPlanError> {
        let record = DrainRecord {
            owner: self.owner,
            sequence,
            request: LocalDrainRequest {
                operation: self.operation,
                groups: self.assignments.clone(),
            },
            phase: DrainPhase::Active,
            plan: Some(self.digest),
        };
        record.validate().map_err(DrainPlanError::Journal)?;
        Ok(record)
    }
    pub fn verify(&self, journal: &impl DrainJournal) -> Result<DrainRecord, DrainPlanError> {
        let record = journal
            .latest()
            .map_err(DrainPlanError::Journal)?
            .ok_or(DrainPlanError::WrongIntent)?;
        if journal.owner() != self.owner || record != self.record(record.sequence)? {
            return Err(DrainPlanError::WrongIntent);
        }
        Ok(record)
    }
}
impl DrainMembershipGroup {
    fn validate(&self, owner: PeerIdentity) -> Result<(), DrainPlanError> {
        let invalid = DrainPlanError::InvalidPlan;
        let ConfigurationChange::Joint { id, next } = &self.change.joint.change else {
            return Err(invalid);
        };
        if self.original.voter_stores().get(&owner.node) != Some(&owner.store)
            || self.original.voter_stores().get(&self.handoff.node) != Some(&self.handoff.store)
            || self.handoff.node == owner.node
            || next.voter_stores().contains_key(&owner.node)
            || next.learners().get(&owner.node) != Some(&owner.store)
            || next.voter_stores().get(&self.handoff.node) != Some(&self.handoff.store)
            || self.change.joint.expected != self.original.id()
            || *id <= self.original.id()
            || next.id() <= *id
            || self.change.finalize.operation != self.change.joint.operation
            || self.change.finalize.expected != *id
            || self.change.finalize.change != (ConfigurationChange::Final { id: next.id() })
        {
            return Err(invalid);
        }
        for (node, store) in next.voter_stores().iter().chain(next.learners()) {
            if self
                .original
                .voter_stores()
                .get(node)
                .or_else(|| self.original.learners().get(node))
                != Some(store)
            {
                return Err(invalid);
            }
        }
        Ok(())
    }
    pub(super) fn target(&self) -> &Configuration {
        let ConfigurationChange::Joint { next, .. } = &self.change.joint.change else {
            unreachable!()
        };
        next
    }
}
