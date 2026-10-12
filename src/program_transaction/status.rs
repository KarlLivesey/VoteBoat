// SPDX-License-Identifier: RPL-1.5
use super::*;

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum ProgramRole {
    Coordinator,
    Participant,
}
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum Decision {
    Commit,
    Abort,
}
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum PrepareRefusal {
    Locked { key: KeyId, owner: TransactionId },
    Version { key: KeyId, actual: KeyVersion },
    VersionExhausted { key: KeyId },
}
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum PrepareOutcome {
    Prepared,
    Refused(PrepareRefusal),
    Closed(Decision),
}
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum ProgramPhase {
    Started,
    Decided(Decision),
    AppliedAll(Decision),
    Finished(Decision),
    Absent,
    Prepared,
    DecisionApplied(Decision),
    Released(Decision),
}
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum ProgramStep {
    Start,
    Prepare,
    Decide,
    ApplyDecision,
    AppliedAll,
    Release,
    Finish,
}
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum ProgramConflict {
    Operation,
    Transaction,
    Phase,
}
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum ProgramOutcome {
    Started,
    Prepare(PrepareOutcome),
    Decided(Decision),
    DecisionApplied(Decision),
    AppliedAll(Decision),
    Released(Decision),
    Finished(Decision),
    Conflict(ProgramConflict),
}
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub struct ProgramReceipt {
    pub index: u64,
    pub first_index: u64,
    pub operation: OperationId,
    pub outcome: ProgramOutcome,
    pub duplicate: bool,
}
/// Historical trusted-driver assertion, never a public foreign certificate.
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
pub struct PreparedKey {
    pub key: KeyId,
    pub value: i64,
    pub version: KeyVersion,
}
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct PhaseStatus {
    pub step: ProgramStep,
    pub decision: Option<Decision>,
    pub index: u64,
    pub operation: OperationId,
    pub outcome: ProgramOutcome,
    pub facts: Box<[PhaseFact]>,
    pub refusal: Option<(usize, PrepareRefusal)>,
}
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct ProgramStatus {
    pub program: Program,
    pub phase: ProgramPhase,
    pub prepare: Option<PrepareOutcome>,
    pub prepared: Box<[PreparedKey]>,
    pub phases: [Option<PhaseStatus>; 4],
}
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum KeyRead {
    Value { value: i64, version: KeyVersion },
    Locked(TransactionId),
    Unknown,
}
#[derive(Clone, Debug, Eq, PartialEq)]
pub enum ProgramQuery {
    Status(TransactionId),
    Keys(Box<[KeyId]>),
}
#[derive(Clone, Debug, Eq, PartialEq)]
pub enum ProgramRead {
    Status(Option<Box<ProgramStatus>>),
    Keys(Box<[(KeyId, KeyRead)]>),
}
impl ProgramStatus {
    pub(crate) fn nested_bytes(&self) -> usize {
        self.program.nested_bytes()
            + std::mem::size_of_val(&*self.prepared)
            + self
                .phases
                .iter()
                .flatten()
                .map(|p| std::mem::size_of_val(&*p.facts))
                .sum::<usize>()
    }
}
