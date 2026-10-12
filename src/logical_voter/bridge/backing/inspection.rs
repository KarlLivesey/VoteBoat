// SPDX-License-Identifier: RPL-1.5
// Copyright (c) 2026 Karl Livesey
//! Borrowed original work; no child output, permit or mutable owner escapes.
use super::*;
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum LogicalBridgeWorkPhase {
    BeforeClaim,
    BeforeEnrollment,
    Enrollment,
    Applying,
    Resolving,
    AfterClaim,
    Snapshots,
    ActivationHistory,
    ActivationRepresentative,
}
/// An accepted ticket is admission only, never a successful child observation.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum LogicalBridgeChildTicket {
    Client(ClientTicket),
    Read {
        ticket: ReadInvocationTicket,
        query: LogicalQuery,
    },
}
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub struct LogicalBridgeAttemptInspection {
    node: usize,
    owner: RuntimeOwner,
    ticket: LogicalBridgeChildTicket,
}
impl LogicalBridgeAttemptInspection {
    pub fn node(self) -> usize {
        self.node
    }
    pub fn owner(self) -> RuntimeOwner {
        self.owner
    }
    pub fn ticket(self) -> LogicalBridgeChildTicket {
        self.ticket
    }
}
/// Transient original command data. This is not a consumed-child proof, and
/// idle or absent attempts do not establish that earlier work completed.
#[derive(Debug)]
pub struct LogicalBridgeWorkInspection<'a> {
    phase: LogicalBridgeWorkPhase,
    operation: OperationId,
    bytes: Option<&'a [u8]>,
    capsule: Option<&'a LogicalCapsule>,
    attempt: Option<LogicalBridgeAttemptInspection>,
    retained_output: bool,
}
impl<'a> LogicalBridgeWorkInspection<'a> {
    pub fn phase(&self) -> LogicalBridgeWorkPhase {
        self.phase
    }
    pub fn operation(&self) -> OperationId {
        self.operation
    }
    pub fn command_bytes(&self) -> Option<&'a [u8]> {
        self.bytes
    }
    pub fn capsule(&self) -> Option<&'a LogicalCapsule> {
        self.capsule
    }
    pub fn attempt(&self) -> Option<LogicalBridgeAttemptInspection> {
        self.attempt
    }
    pub fn retained_output(&self) -> bool {
        self.retained_output
    }
}
impl<N: LogicalBackingChild> LogicalBackingBridge<'_, N> {
    /// Inspect the current original work even during closing/failed cleanup.
    /// The callback cannot retain a borrow or consume the child output streams.
    pub fn inspect_work<R>(
        &self,
        inspect: impl for<'p> FnOnce(Option<LogicalBridgeWorkInspection<'p>>) -> R,
    ) -> R {
        inspect(self.work.as_ref().map(|work| {
            let (phase, operation, bytes, capsule) = stage(&work.stage);
            LogicalBridgeWorkInspection {
                phase,
                operation,
                bytes,
                capsule,
                attempt: work.attempt.map(|attempt| self.inspect_attempt(attempt)),
                retained_output: self.retained.is_some(),
            }
        }))
    }
    fn inspect_attempt(&self, attempt: ChildAttempt) -> LogicalBridgeAttemptInspection {
        let (node, ticket) = match attempt {
            ChildAttempt::Client { node, ticket } => {
                (node, LogicalBridgeChildTicket::Client(ticket))
            }
            ChildAttempt::Read {
                node,
                ticket,
                query,
            } => (node, LogicalBridgeChildTicket::Read { ticket, query }),
        };
        LogicalBridgeAttemptInspection {
            node,
            owner: self.nodes[node].local().owner.identity(),
            ticket,
        }
    }
}
type StageView<'a> = (
    LogicalBridgeWorkPhase,
    OperationId,
    Option<&'a [u8]>,
    Option<&'a LogicalCapsule>,
);
fn stage(stage: &BridgeStage) -> StageView<'_> {
    use LogicalBridgeWorkPhase as Phase;
    match stage {
        BridgeStage::Applying { command, bytes } | BridgeStage::Resolving { command, bytes } => (
            if matches!(stage, BridgeStage::Applying { .. }) {
                Phase::Applying
            } else {
                Phase::Resolving
            },
            command.operation,
            Some(bytes),
            match &command.body {
                CommandBody::Authorize(capsule) => Some(capsule),
                _ => None,
            },
        ),
        BridgeStage::BeforeClaim(operation) => (Phase::BeforeClaim, *operation, None, None),
        BridgeStage::BeforeEnrollment { operation, .. } => {
            (Phase::BeforeEnrollment, *operation, None, None)
        }
        BridgeStage::Enrollment { claim, .. } => (Phase::Enrollment, claim.operation, None, None),
        BridgeStage::ActivationHistory(operation) => {
            (Phase::ActivationHistory, *operation, None, None)
        }
        BridgeStage::ActivationRepresentative(operation) => {
            (Phase::ActivationRepresentative, *operation, None, None)
        }
        BridgeStage::AfterClaim { claim, .. } => (Phase::AfterClaim, claim.operation, None, None),
        BridgeStage::Snapshots(reads) => (Phase::Snapshots, reads.claim.operation, None, None),
    }
}
