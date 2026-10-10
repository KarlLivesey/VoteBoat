// SPDX-License-Identifier: RPL-1.5
// Copyright (c) 2026 Karl Livesey
//! Opt-in durable maintenance records in the application's existing Raft log.
use crate::{application::*, identity::*, log::*, raft::*, secure::PeerIdentity};
use std::{
    collections::{BTreeMap, BTreeSet},
    mem::size_of,
};
mod codec;
mod state;
use codec::{decode, Envelope};

pub const MAX_MAINTENANCE_RECORDS: usize = 1024;

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub struct LeadershipIntent {
    pub request: LeadershipTransferRequest,
    /// Original caller-bound voter, preserved across admission/election/retry.
    /// This identity does not assert current leadership at admission.
    pub source: PeerIdentity,
}
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum LeadershipPhase {
    Pending,
    /// Historical achievement of target leadership, not current authority.
    /// The target may already lead when the original intent is admitted.
    Completed {
        index: u64,
        term: u64,
    },
    /// Retrying stopped; a delivered handoff cannot be rolled back.
    Cancelled {
        index: u64,
        term: u64,
    },
}
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub struct LeadershipRecord {
    pub intent: LeadershipIntent,
    pub index: u64,
    pub term: u64,
    pub phase: LeadershipPhase,
}
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum LeadershipCommand {
    Begin(LeadershipIntent),
    Complete {
        intent: LeadershipIntent,
        index: u64,
        term: u64,
    },
    Cancel {
        intent: LeadershipIntent,
        index: u64,
    },
}
impl LeadershipCommand {
    pub fn intent(self) -> LeadershipIntent {
        match self {
            Self::Begin(i) | Self::Complete { intent: i, .. } | Self::Cancel { intent: i, .. } => i,
        }
    }
    pub fn encode(self) -> Result<Vec<u8>, ApplicationError> {
        codec::encode(self)
    }
}
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum MaintenanceOutcome {
    Recorded(LeadershipRecord),
    Conflict,
    Busy,
}
#[derive(Clone, Debug, Eq, PartialEq)]
pub enum MaintenanceReceipt<R> {
    Data(R),
    Administration {
        operation: OperationId,
        index: u64,
        outcome: MaintenanceOutcome,
    },
}
#[derive(Clone, Debug, Eq, PartialEq)]
pub enum MaintenanceQuery<Q> {
    Data(Q),
    Leadership(OperationId),
}
#[derive(Clone, Debug, Eq, PartialEq)]
pub enum MaintenanceRead<R> {
    Data(R),
    Leadership(Option<LeadershipRecord>),
}
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum LeadershipAction {
    Transfer(LeadershipTransferRequest),
    Complete(LeadershipCommand),
    Wait,
}
/// A separate explicit application schema. Construct from a pristine inner
/// application; an existing unwrapped store needs migration, not hot wrapping.
/// Data and administrative operation IDs occupy distinct framed namespaces.
/// Select this as the outer application so Node checks its proposal context.
#[derive(Clone)]
pub struct Maintenance<A> {
    group: GroupIdentity,
    schema: u64,
    capacity: usize,
    inner: A,
    records: BTreeMap<OperationId, LeadershipRecord>,
}
impl<A: CheckpointStateMachine> Maintenance<A> {
    pub fn new(
        group: GroupIdentity,
        schema: u64,
        capacity: usize,
        inner: A,
    ) -> Result<Self, ApplicationError> {
        if schema == 0
            || schema == inner.schema_version()
            || inner.applied_index() != 0
            || capacity == 0
            || capacity > MAX_MAINTENANCE_RECORDS
        {
            return Err(ApplicationError::InvalidCheckpoint);
        }
        inner.validate_group(group)?;
        Ok(Self {
            group,
            schema,
            capacity,
            inner,
            records: BTreeMap::new(),
        })
    }
}
impl<A: StateMachine> Maintenance<A> {
    pub fn inner(&self) -> &A {
        &self.inner
    }
    /// Local applied state only. Use a completed Node read for remote decisions.
    pub fn record(&self, operation: OperationId) -> Option<LeadershipRecord> {
        self.records.get(&operation).copied()
    }
    pub fn pending(&self) -> Option<LeadershipRecord> {
        self.records
            .values()
            .find(|r| r.phase == LeadershipPhase::Pending)
            .copied()
    }
    /// Pure next action for the exact local core. The host budgets/polls the
    /// returned action and retains every original request/result ticket.
    pub fn leadership_action(&self, core: &Raft) -> Result<LeadershipAction, ApplicationError> {
        self.check_group(core.state().bootstrap.group)?;
        let Some(record) = self.pending() else {
            return Ok(LeadershipAction::Wait);
        };
        if core.role() != Role::Leader
            || core.membership().id() != record.intent.request.configuration
            || core.membership().joint().is_some()
        {
            return Ok(LeadershipAction::Wait);
        }
        if local(core) == record.intent.request.target {
            let command = LeadershipCommand::Complete {
                intent: record.intent,
                index: record.index,
                term: core.state().hard_state.term,
            };
            if self.check_control_context(core, command).is_ok() {
                return Ok(LeadershipAction::Complete(command));
            }
        } else if core.leadership_transfer().is_none() {
            return Ok(LeadershipAction::Transfer(record.intent.request));
        }
        Ok(LeadershipAction::Wait)
    }
    /// Frame a data command for the selected maintenance schema.
    pub fn data(bytes: &[u8], max_bytes: usize) -> Result<Vec<u8>, ApplicationError> {
        codec::data(bytes, max_bytes)
    }
    fn check_control_context(
        &self,
        core: &Raft,
        command: LeadershipCommand,
    ) -> Result<(), ApplicationError> {
        self.check_group(core.state().bootstrap.group)?;
        if core.role() != Role::Leader {
            return Err(ApplicationError::NotApplied);
        }
        let intent = command.intent();
        let original = self.record(intent.request.operation);
        if original.is_some_and(|r| r.intent != intent) {
            return Err(ApplicationError::OperationConflict);
        }
        if original.is_some_and(|r| r.intent == intent && r.phase != LeadershipPhase::Pending) {
            return Ok(());
        }
        match command {
            LeadershipCommand::Begin(_) if original.is_some_and(|r| r.intent == intent) => Ok(()),
            LeadershipCommand::Begin(_) => {
                if core.membership().id() != intent.request.configuration
                    || core.membership().joint().is_some()
                    || core.membership().last_configuration_index() > core.state().commit_index
                    || intent.source == intent.request.target
                    || core.membership().voter_store(intent.source.node)
                        != Some(intent.source.store)
                    || core.membership().voter_store(intent.request.target.node)
                        != Some(intent.request.target.store)
                {
                    return Err(ApplicationError::InvalidCommand);
                }
                Ok(())
            }
            LeadershipCommand::Complete { index, term, .. } => {
                let log = core.state();
                if !original.is_some_and(|r| r.intent == intent && r.index == index)
                    || local(core) != intent.request.target
                    || core.membership().id() != intent.request.configuration
                    || core.membership().joint().is_some()
                    || term != log.hard_state.term
                    || original.is_some_and(|r| term < r.term)
                    || log.commit_index < index
                    || log.term_at(log.commit_index) != Some(term)
                {
                    return Err(ApplicationError::InvalidCommand);
                }
                Ok(())
            }
            LeadershipCommand::Cancel { index, .. } => {
                if original.is_none_or(|r| r.intent != intent || r.index != index) {
                    return Err(ApplicationError::InvalidCommand);
                }
                Ok(())
            }
        }
    }
    fn check_group(&self, group: GroupIdentity) -> Result<(), ApplicationError> {
        if group != self.group {
            return Err(ApplicationError::InvalidCommand);
        }
        self.inner.validate_group(group)
    }
}
fn local(core: &Raft) -> PeerIdentity {
    PeerIdentity {
        node: core.local_node(),
        store: core.storage_binding().identity,
    }
}
impl<R: ApplicationReceipt> ApplicationReceipt for MaintenanceReceipt<R> {
    fn index(&self) -> u64 {
        match self {
            Self::Data(r) => r.index(),
            Self::Administration { index, .. } => *index,
        }
    }
    fn operation(&self) -> OperationId {
        match self {
            Self::Data(r) => r.operation(),
            Self::Administration { operation, .. } => *operation,
        }
    }
    fn nested_bytes(&self, limit: usize) -> Result<usize, ApplicationError> {
        match self {
            Self::Data(r) => r.nested_bytes(limit),
            _ => Ok(0),
        }
    }
}
