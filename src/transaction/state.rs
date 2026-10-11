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
use crate::identity::OperationId;

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub(super) struct Account {
    pub opening: i64,
    pub balance: i64,
    pub lock: Option<TransactionId>,
}
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub(super) struct StoredPhase {
    pub command: Command,
    pub outcome: TransferOutcome,
    pub index: u64,
}
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub(super) struct Record {
    pub spec: TransferSpec,
    pub phase: TransferPhase,
    pub prepare: Option<PrepareOutcome>,
    pub phases: [Option<StoredPhase>; 4],
}
impl Record {
    pub fn status(&self, group: GroupIdentity) -> TransferStatus {
        TransferStatus {
            spec: self.spec,
            phase: self.phase,
            prepare: self.prepare,
            phases: self.phases.map(|p| {
                p.map(|p| PhaseStatus {
                    step: p.command.step,
                    decision: p.command.decision,
                    index: p.index,
                    operation: p.command.operation(group).expect("checked retained phase"),
                    outcome: p.outcome,
                    facts: p.command.facts,
                    refusal: p.command.refusal,
                })
            }),
        }
    }
}
impl TransferApplication {
    pub(super) fn check_command(
        &self,
        operation: OperationId,
        command: Command,
    ) -> Result<(), ApplicationError> {
        command.validate()?;
        if command.spec.profile() != self.profile || command.operation(self.group)? != operation {
            return Err(ApplicationError::InvalidCommand);
        }
        if let TransferRole::Participant(_) = self.role {
            let account = command
                .spec
                .account(self.group)
                .ok_or(ApplicationError::InvalidCommand)?;
            if !self.accounts.contains_key(&account.account()) {
                return Err(ApplicationError::InvalidCommand);
            }
        }
        Ok(())
    }
    pub(super) fn execute(
        &mut self,
        command: Command,
        index: u64,
    ) -> Result<(TransferOutcome, bool, u64), ApplicationError> {
        let id = command.spec.id();
        if let Some(record) = self.records.get(&id) {
            if record.spec != command.spec {
                return Ok((
                    TransferOutcome::Conflict(TransferConflict::Transaction),
                    true,
                    0,
                ));
            }
            if let Some(original) = record.phases[command.slot()] {
                return Ok((
                    if original.command == command {
                        original.outcome
                    } else {
                        TransferOutcome::Conflict(TransferConflict::Operation)
                    },
                    true,
                    original.index,
                ));
            }
        } else {
            if self
                .check_binding(command.spec, self.records.values().map(|r| r.spec))
                .is_err()
            {
                return Ok((
                    TransferOutcome::Conflict(TransferConflict::Operation),
                    false,
                    0,
                ));
            }
            if self.records.len() == self.limits.transactions() {
                return Err(ApplicationError::DedupCapacity);
            }
            let phase = match (self.role, command.step, command.decision) {
                (TransferRole::Coordinator, TransferStep::Start, _) => TransferPhase::Started,
                (TransferRole::Participant(_), TransferStep::Prepare, _)
                | (
                    TransferRole::Participant(_),
                    TransferStep::ApplyDecision,
                    Some(TransferDecision::Abort),
                ) => TransferPhase::Absent,
                _ => return Err(ApplicationError::InvalidCommand),
            };
            self.records.insert(
                id,
                Record {
                    spec: command.spec,
                    phase,
                    prepare: None,
                    phases: [None; 4],
                },
            );
        }
        let mut record = self.records[&id];
        let outcome = match self.role {
            TransferRole::Coordinator => coordinator(&mut record, command),
            TransferRole::Participant(_) => self.participant_phase(&mut record, command)?,
        };
        record.phases[command.slot()] = Some(StoredPhase {
            command,
            outcome,
            index,
        });
        self.records.insert(id, record);
        Ok((outcome, false, index))
    }
    fn participant_phase(
        &mut self,
        record: &mut Record,
        command: Command,
    ) -> Result<TransferOutcome, ApplicationError> {
        let account = record
            .spec
            .account(self.group)
            .ok_or(ApplicationError::InvalidCommand)?
            .account();
        let stored = self
            .accounts
            .get_mut(&account)
            .ok_or(ApplicationError::InvalidCommand)?;
        Ok(match command.step {
            TransferStep::Prepare => prepare(record, stored, self.group),
            TransferStep::ApplyDecision => apply_decision(record, stored, command),
            TransferStep::Release => release(record, stored, command),
            _ => TransferOutcome::Conflict(TransferConflict::Phase),
        })
    }
}
fn coordinator(record: &mut Record, command: Command) -> TransferOutcome {
    use TransferPhase::*;
    match (record.phase, command.step, command.decision) {
        (Started, TransferStep::Start, None) => TransferOutcome::Started,
        (Started, TransferStep::Decide, Some(d)) => {
            record.phase = Decided(d);
            TransferOutcome::Decided(d)
        }
        (Decided(d), TransferStep::AppliedAll, Some(original)) if d == original => {
            record.phase = AppliedAll(d);
            TransferOutcome::AppliedAll(d)
        }
        (AppliedAll(d), TransferStep::Finish, Some(original)) if d == original => {
            record.phase = Finished(d);
            TransferOutcome::Finished(d)
        }
        _ => TransferOutcome::Conflict(TransferConflict::Phase),
    }
}
fn prepare(record: &mut Record, account: &mut Account, group: GroupIdentity) -> TransferOutcome {
    let outcome = match record.phase {
        TransferPhase::DecisionApplied(d) | TransferPhase::Released(d) => PrepareOutcome::Closed(d),
        TransferPhase::Absent => {
            if let Some(owner) = account.lock {
                PrepareOutcome::Refused(PrepareRefusal::Locked(owner))
            } else {
                let after = if record.spec.debit().group() == group {
                    account
                        .balance
                        .checked_sub(record.spec.amount())
                        .filter(|v| *v >= 0)
                } else {
                    account.balance.checked_add(record.spec.amount())
                };
                match after {
                    Some(after) => {
                        account.lock = Some(record.spec.id());
                        record.phase = TransferPhase::Prepared;
                        PrepareOutcome::Prepared {
                            before: account.balance,
                            after,
                        }
                    }
                    None => PrepareOutcome::Refused(if record.spec.debit().group() == group {
                        PrepareRefusal::InsufficientFunds
                    } else {
                        PrepareRefusal::CreditOverflow
                    }),
                }
            }
        }
        _ => return TransferOutcome::Conflict(TransferConflict::Phase),
    };
    record.prepare = Some(outcome);
    TransferOutcome::Prepare(outcome)
}
fn apply_decision(record: &mut Record, account: &mut Account, command: Command) -> TransferOutcome {
    let Some(d) = command.decision else {
        return TransferOutcome::Conflict(TransferConflict::Phase);
    };
    match (record.phase, d, record.prepare) {
        (
            TransferPhase::Prepared,
            TransferDecision::Commit,
            Some(PrepareOutcome::Prepared { after, .. }),
        ) if account.lock == Some(record.spec.id()) => {
            account.balance = after;
        }
        (TransferPhase::Prepared, TransferDecision::Abort, _)
            if account.lock == Some(record.spec.id()) => {}
        (TransferPhase::Absent, TransferDecision::Abort, _) => {}
        _ => return TransferOutcome::Conflict(TransferConflict::Phase),
    }
    record.phase = TransferPhase::DecisionApplied(d);
    TransferOutcome::DecisionApplied(d)
}
fn release(record: &mut Record, account: &mut Account, command: Command) -> TransferOutcome {
    let Some(d) = command.decision else {
        return TransferOutcome::Conflict(TransferConflict::Phase);
    };
    if record.phase != TransferPhase::DecisionApplied(d) {
        return TransferOutcome::Conflict(TransferConflict::Phase);
    }
    if account.lock == Some(record.spec.id()) {
        account.lock = None;
    }
    record.phase = TransferPhase::Released(d);
    TransferOutcome::Released(d)
}
