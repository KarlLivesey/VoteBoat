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
use super::{TransactionId, TransferSpec};
use crate::identity::{GroupIdentity, OperationId};

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum TransferRole {
    Coordinator,
    Participant(usize),
}
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum TransferDecision {
    Commit,
    Abort,
}
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum PrepareRefusal {
    Locked(TransactionId),
    InsufficientFunds,
    CreditOverflow,
}
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum PrepareOutcome {
    Prepared { before: i64, after: i64 },
    Refused(PrepareRefusal),
    Closed(TransferDecision),
}
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum TransferPhase {
    Started,
    Decided(TransferDecision),
    AppliedAll(TransferDecision),
    Finished(TransferDecision),
    Absent,
    Prepared,
    DecisionApplied(TransferDecision),
    Released(TransferDecision),
}
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum TransferStep {
    Start,
    Prepare,
    Decide,
    ApplyDecision,
    AppliedAll,
    Release,
    Finish,
}
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum TransferConflict {
    Operation,
    Transaction,
    Phase,
}
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum TransferOutcome {
    Started,
    Prepare(PrepareOutcome),
    Decided(TransferDecision),
    DecisionApplied(TransferDecision),
    AppliedAll(TransferDecision),
    Released(TransferDecision),
    Finished(TransferDecision),
    Conflict(TransferConflict),
}
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub struct TransferReceipt {
    /// Current invocation's applied position, including a duplicate invocation.
    pub index: u64,
    /// Original retained phase position. Zero for an unretained binding conflict.
    pub first_index: u64,
    pub operation: OperationId,
    pub outcome: TransferOutcome,
    pub duplicate: bool,
}
/// Scoped by the enclosing immutable TransferSpec and phase. This is retained
/// historical provenance, not a certificate; only the trusted driver constructs
/// internal commands after correlating real Applied/status observations.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub struct PhaseFact {
    pub(crate) group: GroupIdentity,
    pub(crate) operation: OperationId,
    pub(crate) index: u64,
}
impl PhaseFact {
    pub fn group(self) -> GroupIdentity {
        self.group
    }
    pub fn operation(self) -> OperationId {
        self.operation
    }
    pub fn index(self) -> u64 {
        self.index
    }
}
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub struct PhaseStatus {
    pub step: TransferStep,
    pub decision: Option<TransferDecision>,
    pub index: u64,
    pub operation: OperationId,
    pub outcome: TransferOutcome,
    pub facts: [Option<PhaseFact>; 2],
    pub refusal: Option<(usize, PrepareRefusal)>,
}
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub struct TransferStatus {
    pub spec: TransferSpec,
    pub phase: TransferPhase,
    pub prepare: Option<PrepareOutcome>,
    pub phases: [Option<PhaseStatus>; 4],
}
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum AccountRead {
    Balance(i64),
    Locked(TransactionId),
    Unknown,
}
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum TransferQuery {
    Status(TransactionId),
    Account(super::AccountId),
}
#[derive(Clone, Debug, Eq, PartialEq)]
pub enum TransferRead {
    Status(Option<Box<TransferStatus>>),
    Account(AccountRead),
}
