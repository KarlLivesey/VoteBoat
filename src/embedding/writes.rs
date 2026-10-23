// SPDX-License-Identifier: RPL-1.5
// Copyright (c) 2026 Karl Livesey
//! Bounded reusable original-write observations for a caller-driven host.
//!
//! This module owns no executor, I/O, consensus or durability authority. The
//! unique host must validate admission, drive the real group, apply the WHOLE
//! committed interval successfully and validate every owned application receipt
//! before publishing any result. A routing binding/LogEntry is not a commit
//! certificate. Native/logical hosts retain their existing release contracts.
//!
//! Open independent ports fallibly once per concurrent client. A port has one
//! active invocation and one borrowed waiter; polling `try_take` is noncancelling.
//! Dropping an accepted observation never rolls back work. Reuse waits for all
//! old capabilities, including exported envelopes, to disappear. Recover a dead
//! host with a fresh domain; a factory cannot revive one.
//!
//! Limits charge retained capacities and conservative handoff reservations;
//! caller/executor/application/core budgets remain separate. Nested receipt
//! reporting is a trusted provider assertion. Standard allocator failure policy
//! applies to Arc/control allocations; fallible Vec reservations are checked.
use crate::{
    identity::{GroupIdentity, NodeId, OperationId, StoreBinding},
    log::{EntryPayload, LogEntry},
    runtime::ProposalPosition,
};
use std::{
    future::Future,
    mem::size_of,
    pin::Pin,
    sync::{
        atomic::{AtomicBool, Ordering},
        Arc, Mutex, MutexGuard, Weak,
    },
    task::{Context, Poll, Waker},
};

mod domain;
mod invocation;
mod producer;
mod state;
use state::*;

#[cfg(test)]
mod tests;

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub struct WriteBinding {
    pub group: GroupIdentity,
    pub node: NodeId,
    pub store: StoreBinding,
}
#[derive(Clone, Copy, Debug)]
pub struct WriteDomainLimits {
    pub ports: usize,
    pub retained_bytes: usize,
}
#[derive(Clone, Copy, Debug)]
pub struct WritePortLimits {
    pub originals: usize,
    pub command_bytes: usize,
    pub command_capacity_bytes: usize,
    /// Per receipt, including inline R and all nested allocated capacity.
    pub receipt_bytes: usize,
    /// Caller-declared executor storage attributable to the installed Waker.
    pub waker_bytes: usize,
    pub retained_bytes: usize,
}
#[derive(Clone, Copy, Debug)]
pub struct WriteInput<'a> {
    pub operation: OperationId,
    pub bytes: &'a [u8],
}
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum WriteUnknown {
    CancelledWait,
    LeadershipChanged,
    OwnerFailed,
    Aborted,
}
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum WriteError {
    InvalidLimits,
    InvalidShape,
    WrongBinding,
    BusyDrain,
    Closed,
    Overloaded,
    StaleInvocation,
    UnknownOriginal,
    AlreadyTerminal,
    ConsumerClosed,
    ProviderViolation,
    Allocation,
    Poisoned,
}
#[derive(Debug, Eq, PartialEq)]
pub enum OriginalOutcome<R> {
    Applied {
        position: ProposalPosition,
        receipt: R,
    },
    NotSubmitted,
    Unknown(WriteUnknown),
}
#[derive(Debug)]
pub struct DeliveryRejected<R> {
    pub reason: WriteError,
    pub receipt: R,
}
#[derive(Clone, Copy, Debug, Default, Eq, PartialEq)]
pub struct WriteUsage {
    pub ports: usize,
    pub retained_bytes: usize,
}
/// Unique host publication and close authority. Move it with the real host.
pub struct WriteDomain<R> {
    registry: Arc<Registry<R>>,
}
/// Cold registration capability; never carries publication or close authority.
pub struct WriteFactory<R> {
    registry: Arc<Registry<R>>,
}
pub struct WritePort<R> {
    shared: Arc<Shared<R>>,
}
/// One extracted immutable command and its affine reply capability.
pub type ExtractedOriginal<R> = (OperationId, Vec<u8>, OriginalReply<R>);
pub struct WriteSubmission<R> {
    key: Lease<R>,
    next: usize,
    len: usize,
}
pub struct WriteInvocation<R> {
    key: Lease<R>,
    len: usize,
}
pub struct OriginalReply<R> {
    key: Lease<R>,
    ordinal: usize,
}
pub struct OriginalWait<'a, R> {
    invocation: &'a mut WriteInvocation<R>,
    ordinal: usize,
    completed: bool,
}
/// An individually correlated outcome. Holding it retains the old invocation.
pub struct OriginalEnvelope<R> {
    outcome: OriginalOutcome<R>,
    lease: EnvelopeLease<R>,
    ordinal: usize,
    operation: OperationId,
}
struct EnvelopeLease<R>(Lease<R>);
impl<R> OriginalEnvelope<R> {
    pub fn binding(&self) -> WriteBinding {
        self.lease.0.shared.binding()
    }
    pub fn operation(&self) -> OperationId {
        self.operation
    }
    pub fn ordinal(&self) -> usize {
        self.ordinal
    }
    pub fn outcome(&self) -> &OriginalOutcome<R> {
        &self.outcome
    }
    /// The caller continues to charge any exported R to its output budget.
    pub fn into_outcome(self) -> OriginalOutcome<R> {
        self.outcome
    }
}
impl<R> std::fmt::Debug for OriginalEnvelope<R>
where
    R: std::fmt::Debug,
{
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("OriginalEnvelope")
            .field("binding", &self.binding())
            .field("ordinal", &self.ordinal)
            .field("operation", &self.operation)
            .field("outcome", &self.outcome)
            .finish()
    }
}
impl WritePortLimits {
    /// Conservative retained + simultaneous handoff reservation. Includes cell
    /// and payload capacity, exported handles/R, two command-copy ceilings,
    /// whole-interval owned receipts alongside ready cells and executor charge.
    /// Arc allowance is two refcount words plus64 bytes alignment per control.
    /// Allocator metadata/host infrastructure follow the trusted allocator policy.
    pub fn required_bytes<R>(self) -> Result<usize, WriteError> {
        self.required_for::<R>(self.originals, self.command_capacity_bytes)
    }
    fn required_for<T>(self, cells: usize, commands: usize) -> Result<usize, WriteError> {
        let fixed = size_of::<Shared<T>>()
            + size_of::<WritePort<T>>()
            + size_of::<WriteSubmission<T>>()
            + size_of::<WriteInvocation<T>>()
            + 3 * (2 * size_of::<usize>() + 64)
            + 2 * size_of::<Arc<Shared<T>>>();
        let each = self
            .command_bytes
            .checked_mul(2)
            .and_then(|n| {
                self.receipt_bytes
                    .checked_mul(2)
                    .and_then(|r| n.checked_add(r))
            })
            .and_then(|n| {
                n.checked_add(
                    size_of::<OriginalReply<T>>()
                        + size_of::<OriginalEnvelope<T>>()
                        + 2 * size_of::<Arc<Shared<T>>>(),
                )
            });
        cells
            .checked_mul(size_of::<Cell<T>>())
            .and_then(|n| n.checked_add(commands))
            .and_then(|n| {
                each.and_then(|each| self.originals.checked_mul(each))
                    .and_then(|extra| n.checked_add(extra))
            })
            .and_then(|n| n.checked_add(fixed))
            .and_then(|n| n.checked_add(self.waker_bytes))
            .ok_or(WriteError::InvalidLimits)
    }
}
