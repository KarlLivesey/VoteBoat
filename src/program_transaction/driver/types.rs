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
use super::*;
use std::num::NonZeroU64;

/// The host MUST select a fresh generation for every attach under the same
/// RuntimeOwner. Reusing owner+generation can alias an old invocation sequence;
/// generation allocation is explicit, just as for Node's existing routers.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub struct ProgramDriverGeneration(NonZeroU64);
impl ProgramDriverGeneration {
    pub const fn new(value: u64) -> Option<Self> {
        match NonZeroU64::new(value) {
            Some(v) => Some(Self(v)),
            None => None,
        }
    }
    pub const fn get(self) -> u64 {
        self.0.get()
    }
}
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub struct ProgramDriverLimits {
    active: usize,
}
impl ProgramDriverLimits {
    pub fn new(active: usize) -> Result<Self, ProgramDriverError> {
        if active == 0 || active > 2048 {
            return Err(ProgramDriverError::InvalidLimits);
        }
        Ok(Self { active })
    }
    pub fn active(self) -> usize {
        self.active
    }
}
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub struct ProgramDriverBinding {
    pub owner: RuntimeOwner,
    pub generation: ProgramDriverGeneration,
}
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub struct ProgramDriverTicket {
    pub(super) binding: ProgramDriverBinding,
    pub(super) sequence: u64,
    pub(super) transaction: TransactionId,
}
impl ProgramDriverTicket {
    pub fn binding(self) -> ProgramDriverBinding {
        self.binding
    }
    pub fn sequence(self) -> u64 {
        self.sequence
    }
    pub fn transaction(self) -> TransactionId {
        self.transaction
    }
}
#[derive(Clone, Debug, Eq, PartialEq)]
pub enum ProgramDriverError {
    InvalidLimits,
    InvalidAssembly,
    Closed,
    Overloaded,
    InFlight,
    Exhausted,
    WrongTicket,
    TimeWentBack,
    Inconsistent,
    ProviderViolation,
    Application(ApplicationError),
    Node {
        node: usize,
        reason: Box<NodeError>,
    },
    Client {
        node: usize,
        reason: Box<ClientError>,
    },
    Read {
        node: usize,
        reason: Box<ReadInvocationError>,
    },
}
#[derive(Debug)]
pub struct ProgramDriverSubmitRejected {
    pub reason: ProgramDriverError,
    pub program: Box<Program>,
}
#[derive(Debug)]
pub struct ProgramDriverResumeRejected {
    pub reason: ProgramDriverError,
    pub transaction: TransactionId,
}
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum ProgramDriverUnknown {
    Stopped,
    NoRetainedStart,
    DriverFailed,
}
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum ProgramDriverOutcome {
    Finished {
        transaction: TransactionId,
        decision: Decision,
        first_index: u64,
    },
    Unknown {
        transaction: TransactionId,
        reason: ProgramDriverUnknown,
    },
}
#[derive(Debug)]
pub struct ProgramDriverCompletion {
    pub(super) ticket: ProgramDriverTicket,
    pub(super) outcome: ProgramDriverOutcome,
}
impl ProgramDriverCompletion {
    pub fn ticket(&self) -> ProgramDriverTicket {
        self.ticket
    }
    pub fn outcome(&self) -> &ProgramDriverOutcome {
        &self.outcome
    }
}
#[derive(Debug)]
pub struct ProgramDriverCompletionRejected {
    pub reason: ProgramDriverError,
    pub completion: Box<ProgramDriverCompletion>,
}
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum ProgramDriverStage {
    Start,
    CoordinatorStatus,
    Prepare(usize),
    Decide,
    ApplyDecision(usize),
    AppliedAll,
    Release(usize),
    Finish,
    Terminal,
}
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub struct ProgramDriverTransactionProgress {
    pub stage: ProgramDriverStage,
    pub outstanding: bool,
    pub node: Option<usize>,
    pub operation: Option<OperationId>,
}
#[derive(Clone, Copy, Debug, Default, Eq, PartialEq)]
pub struct ProgramDriverUsage {
    pub requests: usize,
    pub pending: usize,
    pub outputs: usize,
    pub exported: usize,
    pub command_bytes: usize,
    /// Nested immutable programs, commands and retained phase vectors.
    pub nested_bytes: usize,
    pub retained_faults: usize,
    /// Boxed original fault outputs and their bounded variable result storage.
    pub retained_output_bytes: usize,
}
#[derive(Clone, Copy, Debug)]
pub struct ProgramDriverBudget {
    pub node_polls: usize,
    pub completions: usize,
    pub transitions: usize,
    pub node: NodePollBudget,
}
impl Default for ProgramDriverBudget {
    fn default() -> Self {
        Self {
            node_polls: 64,
            completions: 64,
            transitions: 64,
            node: NodePollBudget::default(),
        }
    }
}
#[derive(Clone, Copy, Debug, Default, Eq, PartialEq)]
pub struct ProgramDriverProgress {
    pub node_polls: usize,
    pub completions: usize,
    pub transitions: usize,
    pub usage: ProgramDriverUsage,
}
impl ProgramDriverBudget {
    pub(super) fn validate(self) -> Result<(), ProgramDriverError> {
        let r = self.node.replica;
        let p = self.node.peers;
        if self.node_polls > 64
            || self.completions > 4096
            || self.transitions > 4096
            || [
                r.worker_events,
                r.snapshot_events,
                r.steps,
                r.effects,
                r.retries,
            ]
            .iter()
            .any(|v| *v == 0 || *v > 4096)
            || r.reconcile == 0
            || r.reconcile > 65536
            || p.connection_visits > 4096
            || p.peer_visits > 4096
            || p.sends > 4096
            || p.ingress > 65536
        {
            return Err(ProgramDriverError::InvalidLimits);
        }
        p.connector
            .validate()
            .map_err(|_| ProgramDriverError::InvalidLimits)?;
        p.transport
            .validate()
            .map_err(|_| ProgramDriverError::InvalidLimits)?;
        Ok(())
    }
}
