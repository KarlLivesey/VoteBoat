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
//! Ordered application seam and a deterministic counter with operation retries.
use crate::{
    identity::{GroupIdentity, OperationId},
    log::{EntryPayload, LogEntry},
    raft::{Raft, RaftError, ReadBarrier},
    retention::SessionLimits,
};

mod counter;
pub use counter::{
    Counter, CounterLookup, CounterOutcome, CounterReceipt, COUNTER_COMMAND_BYTES, COUNTER_FORMAT,
};

pub const PROPOSAL_ADMISSION_CONTRACT_VERSION: u32 = 1;

/// Layer tags of composed application schemas ([`layered_schema`]). Tags
/// 1..=4 are the leadership-maintenance profile formats
/// ([`crate::maintenance::maintenance_schema`]); tag 5 is never assigned
/// (in-tree schemas at or above 2^60 begin with ASCII `V`, 0x56); the others
/// are reserved.
pub const LOGICAL_ACTIVATION_LAYER: u64 = 8;
/// The schema of one wrapper layer `tag` (1..=15) around an application with
/// schema `inner`: `tag << 60 | inner`. `None` unless `inner` is non-zero and
/// below 2^60. A composed schema decodes to exactly one (tag, inner), is at or
/// above 2^60 while every accepted inner schema is below it, and so no layer
/// equals a leaf schema below 2^60, and a composed application cannot be
/// wrapped again (its schema is not an accepted inner one). Over leaves below
/// 2^60 with at most one maintenance or activation layer, distinct
/// compositions have distinct schemas; every other composition is refused.
pub const fn layered_schema(tag: u64, inner: u64) -> Option<u64> {
    if tag == 0 || tag > 0xF || inner == 0 || inner >> 60 != 0 {
        return None;
    }
    Some(tag << 60 | inner)
}

/// Optional validation state for one synchronous already-ready prefix. The
/// capability borrows immutable application state and owns retained pending
/// data; it must not borrow temporary iterator items. Successful checks reserve
/// their original operations for following checks. It performs no I/O or state
/// application and grants no admission, authentication or durability authority.
/// Drop it before application mutation and never reuse it across a Core/group
/// transition or after a rejected candidate.
pub type ReadyAdmission<'app> =
    Box<dyn FnMut(OperationId, &[u8]) -> Result<usize, ApplicationError> + 'app>;

/// Deterministic application admission against all unapplied durable commands
/// and retained queued/in-flight client commands. The caller serializes this
/// check with application and routes every service proposal through it.
/// Return a per-command receipt retention bound (including inline receipt).
/// Validation changes no state and performs no I/O; accepted capacity remains
/// represented by the caller's pending commands until log/application takeover.
pub trait ProposalAdmission: BoundedStateMachine
where
    Self::Receipt: ApplicationReceipt,
{
    /// Opt into validating already-ready commands against one unchanged Core
    /// context and their reserved predecessor commands. Default preserves
    /// singleton execution for applications with context-dependent guards.
    /// This grants no durability, admission or result authority.
    fn supports_ready_batch(&self) -> bool {
        false
    }

    /// Optionally prepare exact reservations against the same pending commands
    /// as `validate_proposal`. The result must preserve that method's per-command
    /// receipt bound and error ordering, incorporating successful predecessors.
    /// The constructor may consume pending items but cannot retain their borrowed
    /// bytes. None preserves the original validator for this entire prefix.
    fn prepare_ready_admission<'pending>(
        &self,
        _pending: impl Iterator<Item = (OperationId, &'pending [u8])>,
    ) -> Option<ReadyAdmission<'_>> {
        None
    }

    /// Optional deterministic check against the current consensus context.
    /// The owning client router calls this at admission and again immediately
    /// before execution. It may restrict proposals; it grants no consensus or
    /// authentication authority. Hosts bypassing that router must do the same.
    /// Composite applications must forward this check for their inner commands.
    fn validate_proposal_context(
        &self,
        _core: &Raft,
        _operation: OperationId,
        _bytes: &[u8],
    ) -> Result<(), ApplicationError> {
        Ok(())
    }
    fn validate_proposal<'a>(
        &self,
        operation: OperationId,
        bytes: &[u8],
        pending: impl Iterator<Item = (OperationId, &'a [u8])>,
    ) -> Result<usize, ApplicationError>;
    /// Decide without seeing the pending commands when `pending_bound` (an
    /// upper bound on how many there are) already settles the question; the
    /// result must equal what `validate_proposal` would return for any such
    /// pending set. `None` asks the router for the full check.
    fn validate_proposal_bounded(
        &self,
        _operation: OperationId,
        _bytes: &[u8],
        _pending_bound: usize,
    ) -> Option<Result<usize, ApplicationError>> {
        None
    }
}

/// Optional bounded receipt capability; wire and checkpoint schemas are separate.
pub const BOUNDED_APPLICATION_CONTRACT_VERSION: u32 = 1;

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum ApplicationError {
    IndexGap,
    InvalidCommand,
    /// A retained or pending operation has a different immutable binding.
    OperationConflict,
    DedupCapacity,
    ReceiptBudget,
    NotApplied,
    InvalidCheckpoint,
    UnsupportedSchema,
    /// Admission refusal: the command would leave more than the session window
    /// of outcomes unacknowledged. Retryable once the client acknowledges.
    SessionWindow,
    /// Readiness and membership refusal: the application declares no
    /// retention rule ([`StateMachine::retention`]).
    RetentionUndeclared,
}

/// How an application bounds the state it retains for operations and
/// lifecycle history. Readiness and membership checks refuse `Undeclared`.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum Retention {
    /// No rule declared (the default). Refused by readiness and membership.
    Undeclared,
    /// Client outcomes kept in a [`crate::retention::SessionLedger`]: at most
    /// `sessions × window` receipts whatever the uptime.
    Sessions(SessionLimits),
    /// Lifecycle records retired behind a replicated generation or epoch fence.
    Fenced,
    /// No per-operation state is retained.
    Stateless,
    /// A declared lifetime-sized table: it refuses new operations once a
    /// configured lifetime count is spent. Accepted only until the lifetime
    /// bounds programme migrates the application (each one is an open row in
    /// docs/IMPLEMENTATION.md); new applications must not use it.
    LifetimeBounded,
}

impl Retention {
    /// The rule a wrapper reports when it keeps a table of its own under rule
    /// `own` around an inner application with rule `inner`. The weaker rule
    /// wins: an undeclared inner application stays undeclared through any
    /// wrapper (so readiness and membership still refuse it), and a lifetime
    /// table on either side makes the whole lifetime-bounded. Otherwise the
    /// wrapper's table is bounded and the inner declaration is reported.
    pub fn wrapping(own: Self, inner: Self) -> Self {
        match (own, inner) {
            (Self::Undeclared, _) | (_, Self::Undeclared) => Self::Undeclared,
            (Self::LifetimeBounded, _) | (_, Self::LifetimeBounded) => Self::LifetimeBounded,
            (_, inner) => inner,
        }
    }
}

/// A host applies only committed, contiguous entries and emits client success
/// after application. Local reads do not create a linearizability proof; the
/// core's read-barrier protocol must authorize distributed linearizable reads.
/// Application checkpoints and restore join this seam with snapshot support.
pub trait StateMachine {
    type Receipt;
    /// Validate the concrete group before assembly, execution or recovery.
    /// Generic applications may accept any group; ownership-bound applications
    /// must reject another group without changing state or performing I/O.
    fn validate_group(&self, _group: GroupIdentity) -> Result<(), ApplicationError> {
        Ok(())
    }
    /// Steady-state bounds under the application's declared retention rule
    /// ([`Self::retention`]). Every retained item (receipts, sessions,
    /// lifecycle records, outbox items) must be reclaimed by a deterministic
    /// function of applied entries, so the bound holds for unbounded uptime at
    /// any sustained rate. Readiness and membership checks size storage and
    /// snapshots against this bound. A lifetime-sized table is not a valid
    /// implementation of this contract (see [`Retention::LifetimeBounded`]).
    /// Current occupancy is insufficient: report the largest steady state.
    /// None preserves ordinary execution but refuses membership/readiness use.
    /// Reporting bounds is a trusted provider assertion, not a reservation.
    fn deployment_requirements(&self) -> Option<crate::raft::ReadinessRequirements> {
        None
    }
    /// The declared retention rule. The default, `Undeclared`, is refused by
    /// readiness and membership checks. A wrapper reports
    /// [`Retention::wrapping`] of its own rule and its inner application's, so
    /// an undeclared inner application is refused through any wrapper.
    fn retention(&self) -> Retention {
        Retention::Undeclared
    }
    /// Whether every committed binding entry this application and each layer
    /// it wraps compare (a family's bootstrap entry: a Routed grant and
    /// initial checkpoint, a directory plan, a target or namespace binding)
    /// has applied in its applied state. Readiness refuses a positive receipt
    /// before then (`ReadinessError::BootstrapPending`): inputs outside the
    /// compared identity are attested only once that entry applied, since a
    /// family applies it only when the committed bytes equal its own binding
    /// and restore compares the binding exactly. `true` (the default) for an
    /// application with no such entry. A wrapper must AND its inner
    /// application's; reporting `true` around an unbound inner application is
    /// a trusted-provider obligation, like declaring `InnerProfile::LEAF`.
    fn deployment_bound(&self) -> bool {
        true
    }
    /// Pure, nonblocking check against the selected application's enforced
    /// bounds. Refuses an application that declares no retention rule.
    fn validate_deployment_requirements(
        &self,
        declared: crate::raft::ReadinessRequirements,
    ) -> Result<(), ApplicationError> {
        if self.retention() == Retention::Undeclared {
            return Err(ApplicationError::RetentionUndeclared);
        }
        let actual = self
            .deployment_requirements()
            .ok_or(ApplicationError::InvalidCheckpoint)?;
        if actual.application_schema == 0
            || actual.command_bytes == 0
            || actual.snapshot_bytes == 0
            || declared.command_bytes == 0
            || declared.snapshot_bytes == 0
        {
            return Err(ApplicationError::InvalidCheckpoint);
        }
        // Exact: every layer's family, schema and decision-affecting
        // configuration. Only the two reservation sizes below may be larger.
        if declared.application_schema != actual.application_schema
            || declared.identity != actual.identity
            || declared.inner_profile != actual.inner_profile
            || actual.inner_profile.layers().is_none()
        {
            return Err(ApplicationError::UnsupportedSchema);
        }
        if declared.command_bytes < actual.command_bytes
            || declared.snapshot_bytes < actual.snapshot_bytes
        {
            return Err(ApplicationError::InvalidCheckpoint);
        }
        Ok(())
    }
    fn applied_index(&self) -> u64;
    /// Apply committed, contiguous entries. Never return `Err` for a capacity
    /// or window reason: a committed entry that would exceed a bound gets a
    /// typed refusal outcome, still advancing the applied index, because a
    /// committed entry replays identically after restart. `Err` is reserved
    /// for index gaps, malformed bytes admission refuses, and corruption.
    fn apply_batch(&mut self, entries: &[LogEntry])
        -> Result<Vec<Self::Receipt>, ApplicationError>;
}

/// One ordered command outcome. Nested retained capacity excludes the inline
/// receipt itself, which the result router charges through vector capacity.
/// Host implementations are trusted to report their actual allocations.
pub trait ApplicationReceipt {
    fn index(&self) -> u64;
    fn operation(&self) -> OperationId;
    fn nested_bytes(&self, limit: usize) -> Result<usize, ApplicationError>;
}

/// Optional bounded result capability over the ordinary application seam.
/// Exactly one receipt per Command, in input order; no receipt for other entries.
/// The bound includes vector spare capacity and all nested retained allocations.
/// Computing it must not mutate the application or perform blocking I/O.
/// Successful apply advances exactly through the batch's last index. Failure
/// must leave state unchanged; the router nevertheless fences the owner on an
/// application failure because a generic host cannot prove partial progress away.
pub trait BoundedStateMachine: StateMachine
where
    Self::Receipt: ApplicationReceipt,
{
    fn receipt_bytes_bound(&self, entries: &[LogEntry]) -> Result<usize, ApplicationError>;
}

/// A checkpoint contains the retained state plus every watermark needed to
/// refuse retired operation IDs, and its applied boundary: replay of the log
/// suffix from it must equal replay from the start. Nothing below a watermark
/// is encoded. Restore must reject unsupported schemas, incompatible configured
/// limits and any state that breaks the declared retention rule.
pub trait CheckpointStateMachine: StateMachine + Clone {
    fn schema_version(&self) -> u64;
    fn checkpoint(&self, max_bytes: usize) -> Result<Vec<u8>, ApplicationError>;
    fn restore_checkpoint(
        &mut self,
        schema: u64,
        applied_index: u64,
        bytes: &[u8],
    ) -> Result<(), ApplicationError>;
}

/// Read support is explicit and optional. `read_at` must observe immutable
/// applied state and reject a boundary it has not applied. Hosts keep the group
/// owner and its application serialized while invoking `read_at_barrier`.
pub trait ReadableStateMachine: StateMachine {
    type Query;
    type ReadResult;
    fn read_at(
        &self,
        required_index: u64,
        query: Self::Query,
    ) -> Result<Self::ReadResult, ApplicationError>;
}

/// Optional bounded read execution contract, version 1. Byte counts include
/// retained allocation capacity, not just logical length. Bounds include inline
/// result storage; queries report nested storage only. Implementations must not
/// perform I/O or mutate applied state during validation or read execution.
pub trait BoundedReadableStateMachine: ReadableStateMachine {
    fn query_bytes(&self, query: &Self::Query, limit: usize) -> Result<usize, ApplicationError>;
    fn read_result_bound(&self, query: &Self::Query) -> Result<usize, ApplicationError>;
    fn read_result_bytes(
        &self,
        result: &Self::ReadResult,
        nested_limit: usize,
    ) -> Result<usize, ApplicationError>;
}

pub const BOUNDED_READ_CONTRACT_VERSION: u32 = 1;

#[derive(Clone, Debug, Eq, PartialEq)]
pub enum ReadError {
    Consensus(RaftError),
    Application(ApplicationError),
}

/// Serve the original pending invocation after a quorum and application catchup.
/// The barrier is consumed once, including when the application rejects a query.
pub fn read_at_barrier<A: ReadableStateMachine>(
    raft: &mut Raft,
    barrier: &ReadBarrier,
    application: &A,
    query: A::Query,
) -> Result<A::ReadResult, ReadError> {
    raft.finish_read(barrier, application.applied_index())
        .map_err(ReadError::Consensus)?;
    application
        .validate_group(raft.state().bootstrap.group)
        .map_err(ReadError::Application)?;
    application
        .read_at(barrier.index(), query)
        .map_err(ReadError::Application)
}
