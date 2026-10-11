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
pub struct TransferDriverGeneration(NonZeroU64);
impl TransferDriverGeneration {
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
pub struct TransferDriverLimits {
    active: usize,
}
impl TransferDriverLimits {
    pub fn new(active: usize) -> Result<Self, DriverError> {
        if active == 0 || active > 4096 {
            return Err(DriverError::InvalidLimits);
        }
        Ok(Self { active })
    }
    pub fn active(self) -> usize {
        self.active
    }
}
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub struct DriverBinding {
    pub owner: RuntimeOwner,
    pub generation: TransferDriverGeneration,
}
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub struct DriverTicket {
    pub(super) binding: DriverBinding,
    pub(super) sequence: u64,
    pub(super) transaction: TransactionId,
}
impl DriverTicket {
    pub fn binding(self) -> DriverBinding {
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
pub enum DriverError {
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
pub struct DriverSubmitRejected {
    pub reason: DriverError,
    pub spec: Box<TransferSpec>,
}
#[derive(Debug)]
pub struct DriverResumeRejected {
    pub reason: DriverError,
    pub transaction: TransactionId,
}
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum DriverUnknown {
    Stopped,
    NoRetainedStart,
    DriverFailed,
}
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum DriverOutcome {
    Finished {
        transaction: TransactionId,
        decision: TransferDecision,
        first_index: u64,
    },
    Unknown {
        transaction: TransactionId,
        reason: DriverUnknown,
    },
}
#[derive(Debug)]
pub struct DriverCompletion {
    pub(super) ticket: DriverTicket,
    pub(super) outcome: DriverOutcome,
}
impl DriverCompletion {
    pub fn ticket(&self) -> DriverTicket {
        self.ticket
    }
    pub fn outcome(&self) -> &DriverOutcome {
        &self.outcome
    }
}
#[derive(Debug)]
pub struct DriverCompletionRejected {
    pub reason: DriverError,
    pub completion: Box<DriverCompletion>,
}
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum DriverStage {
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
pub struct DriverTransactionProgress {
    pub stage: DriverStage,
    pub outstanding: bool,
    pub node: Option<usize>,
    pub operation: Option<OperationId>,
}
#[derive(Clone, Copy, Debug, Default, Eq, PartialEq)]
pub struct DriverUsage {
    pub requests: usize,
    pub pending: usize,
    pub outputs: usize,
    pub exported: usize,
    pub command_bytes: usize,
    pub retained_faults: usize,
}
#[derive(Clone, Copy, Debug)]
pub struct DriverBudget {
    pub node_polls: usize,
    pub completions: usize,
    pub transitions: usize,
    pub node: NodePollBudget,
}
impl Default for DriverBudget {
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
pub struct DriverProgress {
    pub node_polls: usize,
    pub completions: usize,
    pub transitions: usize,
    pub usage: DriverUsage,
}
impl DriverBudget {
    pub(super) fn validate(self) -> Result<(), DriverError> {
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
            return Err(DriverError::InvalidLimits);
        }
        p.connector
            .validate()
            .map_err(|_| DriverError::InvalidLimits)?;
        p.transport
            .validate()
            .map_err(|_| DriverError::InvalidLimits)?;
        Ok(())
    }
}
