// SPDX-License-Identifier: RPL-1.5
// Copyright (c) 2026 Karl Livesey
//! Explicit executable profiles; the ordinary counter keeps its original schema.
use super::setup::{checked, group, Failure};
use voteboat::{application::*, identity::*, log::LogEntry, maintenance::*, raft::Raft};

pub type Receipt = MaintenanceReceipt<CounterReceipt>;
pub type Query = MaintenanceQuery<()>;
pub type ReadResult = MaintenanceRead<i64>;

#[derive(Clone)]
pub enum Application {
    Counter(Counter),
    Maintenance(Maintenance<Counter>),
}
/// Application schema of the leadership-maintenance profile. It must differ
/// from the inner Counter's schema (2), so the profile is schema 3.
pub const MAINTENANCE_SCHEMA: u64 = 3;
macro_rules! delegate {
    ($self:expr, $app:ident => $expression:expr) => {
        match $self {
            Application::Counter($app) => $expression,
            Application::Maintenance($app) => $expression,
        }
    };
}
impl Application {
    pub fn new(counter: Counter, enabled: bool) -> Result<Self, Failure> {
        Self::for_group(group(), counter, enabled)
    }
    pub fn for_group(
        group: GroupIdentity,
        counter: Counter,
        enabled: bool,
    ) -> Result<Self, Failure> {
        Ok(if enabled {
            Self::Maintenance(checked(Maintenance::new(
                group,
                MAINTENANCE_SCHEMA,
                64,
                counter,
            ))?)
        } else {
            Self::Counter(counter)
        })
    }
    pub fn maintenance(&self) -> Result<&Maintenance<Counter>, String> {
        match self {
            Self::Maintenance(app) => Ok(app),
            _ => Err("leadership maintenance profile disabled".into()),
        }
    }
    /// Frame counter command bytes (a registration's empty bytes or
    /// `Counter::command`) for the selected profile.
    pub fn data(&self, bytes: &[u8]) -> Result<Vec<u8>, String> {
        match self {
            Self::Counter(_) => Ok(bytes.to_vec()),
            Self::Maintenance(_) => Maintenance::<Counter>::data(bytes, 9 + COUNTER_COMMAND_BYTES)
                .map_err(|e| format!("{e:?}")),
        }
    }
}
impl StateMachine for Application {
    type Receipt = Receipt;
    fn validate_group(&self, group: GroupIdentity) -> Result<(), ApplicationError> {
        delegate!(self, app => app.validate_group(group))
    }
    fn deployment_requirements(&self) -> Option<voteboat::raft::ReadinessRequirements> {
        delegate!(self, app => app.deployment_requirements())
    }
    fn retention(&self) -> Retention {
        delegate!(self, app => app.retention())
    }
    fn applied_index(&self) -> u64 {
        delegate!(self, app => app.applied_index())
    }
    fn apply_batch(&mut self, entries: &[LogEntry]) -> Result<Vec<Receipt>, ApplicationError> {
        match self {
            Self::Counter(app) => Ok(app
                .apply_batch(entries)?
                .into_iter()
                .map(Receipt::Data)
                .collect()),
            Self::Maintenance(app) => app.apply_batch(entries),
        }
    }
}
impl BoundedStateMachine for Application {
    fn receipt_bytes_bound(&self, entries: &[LogEntry]) -> Result<usize, ApplicationError> {
        match self {
            Self::Counter(_) => entries
                .iter()
                .filter(|e| matches!(e.payload, voteboat::log::EntryPayload::Command { .. }))
                .count()
                .checked_mul(std::mem::size_of::<Receipt>())
                .ok_or(ApplicationError::ReceiptBudget),
            Self::Maintenance(app) => app.receipt_bytes_bound(entries),
        }
    }
}
impl ProposalAdmission for Application {
    fn supports_ready_batch(&self) -> bool {
        delegate!(self, app => app.supports_ready_batch())
    }

    fn prepare_ready_admission<'pending>(
        &self,
        pending: impl Iterator<Item = (OperationId, &'pending [u8])>,
    ) -> Option<ReadyAdmission<'_>> {
        match self {
            Self::Counter(app) => {
                let mut admission = app.prepare_ready_admission(pending)?;
                Some(Box::new(move |operation, bytes| {
                    admission(operation, bytes)?;
                    Ok(std::mem::size_of::<Receipt>())
                }))
            }
            Self::Maintenance(_) => None,
        }
    }

    fn validate_proposal_context(
        &self,
        core: &Raft,
        op: OperationId,
        bytes: &[u8],
    ) -> Result<(), ApplicationError> {
        delegate!(self, app => app.validate_proposal_context(core, op, bytes))
    }
    fn validate_proposal<'a>(
        &self,
        op: OperationId,
        bytes: &[u8],
        pending: impl Iterator<Item = (OperationId, &'a [u8])>,
    ) -> Result<usize, ApplicationError> {
        match self {
            Self::Counter(app) => {
                app.validate_proposal(op, bytes, pending)?;
                Ok(std::mem::size_of::<Receipt>())
            }
            Self::Maintenance(app) => app.validate_proposal(op, bytes, pending),
        }
    }
}
impl CheckpointStateMachine for Application {
    fn schema_version(&self) -> u64 {
        delegate!(self, app => app.schema_version())
    }
    fn checkpoint(&self, max: usize) -> Result<Vec<u8>, ApplicationError> {
        delegate!(self, app => app.checkpoint(max))
    }
    fn restore_checkpoint(
        &mut self,
        schema: u64,
        applied: u64,
        bytes: &[u8],
    ) -> Result<(), ApplicationError> {
        delegate!(self, app => app.restore_checkpoint(schema, applied, bytes))
    }
}
impl ReadableStateMachine for Application {
    type Query = Query;
    type ReadResult = ReadResult;
    fn read_at(&self, index: u64, query: Query) -> Result<ReadResult, ApplicationError> {
        match self {
            Self::Counter(app) => match query {
                Query::Data(()) => app.read_at(index, ()).map(ReadResult::Data),
                _ => Err(ApplicationError::InvalidCommand),
            },
            Self::Maintenance(app) => app.read_at(index, query),
        }
    }
}
impl BoundedReadableStateMachine for Application {
    fn query_bytes(&self, query: &Query, max: usize) -> Result<usize, ApplicationError> {
        match self {
            Self::Counter(app) => match query {
                Query::Data(()) => app.query_bytes(&(), max),
                _ => Err(ApplicationError::InvalidCommand),
            },
            Self::Maintenance(app) => app.query_bytes(query, max),
        }
    }
    fn read_result_bound(&self, query: &Query) -> Result<usize, ApplicationError> {
        match self {
            Self::Counter(_) => match query {
                Query::Data(()) => Ok(std::mem::size_of::<ReadResult>()),
                _ => Err(ApplicationError::InvalidCommand),
            },
            Self::Maintenance(app) => app.read_result_bound(query),
        }
    }
    fn read_result_bytes(
        &self,
        result: &ReadResult,
        max: usize,
    ) -> Result<usize, ApplicationError> {
        match self {
            Self::Counter(app) => match result {
                ReadResult::Data(n) => app.read_result_bytes(n, max),
                _ => Err(ApplicationError::InvalidCommand),
            },
            Self::Maintenance(app) => app.read_result_bytes(result, max),
        }
    }
}
