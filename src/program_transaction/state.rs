// SPDX-License-Identifier: RPL-1.5
use super::*;
mod participant;

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub(super) struct Key {
    pub opening: i64,
    pub value: i64,
    pub version: KeyVersion,
    pub lock: Option<TransactionId>,
}
#[derive(Clone, Debug, Eq, PartialEq)]
pub(super) struct StoredPhase {
    pub command: Command,
    pub outcome: ProgramOutcome,
    pub index: u64,
}
#[derive(Clone, Debug, Eq, PartialEq)]
pub(super) struct Record {
    pub program: Program,
    pub phase: ProgramPhase,
    pub prepare: Option<PrepareOutcome>,
    pub prepared: Box<[PreparedKey]>,
    pub phases: [Option<StoredPhase>; 4],
}
impl Record {
    pub fn status(&self, group: GroupIdentity) -> ProgramStatus {
        ProgramStatus {
            program: self.program.clone(),
            phase: self.phase,
            prepare: self.prepare,
            prepared: self.prepared.clone(),
            phases: std::array::from_fn(|i| {
                self.phases[i].as_ref().map(|p| PhaseStatus {
                    step: p.command.step,
                    decision: p.command.decision,
                    index: p.index,
                    operation: p.command.operation(group).expect("retained checked phase"),
                    outcome: p.outcome,
                    facts: p.command.facts.clone(),
                    refusal: p.command.refusal,
                })
            }),
        }
    }
}
impl ProgramApplication {
    pub(super) fn check_command(
        &self,
        operation: OperationId,
        command: &Command,
    ) -> Result<(), ApplicationError> {
        command.validate()?;
        if command.program.profile() != &self.profile || command.operation(self.group)? != operation
        {
            return Err(ApplicationError::InvalidCommand);
        }
        match self.role {
            ProgramRole::Coordinator if command.program.coordinator() != self.group => {
                Err(ApplicationError::InvalidCommand)
            }
            ProgramRole::Participant => {
                let p = command
                    .program
                    .participant(self.group)
                    .ok_or(ApplicationError::InvalidCommand)?;
                if command.program.participants()[p]
                    .keys()
                    .iter()
                    .any(|k| !self.keys.contains_key(&k.key))
                {
                    Err(ApplicationError::InvalidCommand)
                } else {
                    Ok(())
                }
            }
            _ => Ok(()),
        }
    }
    pub(super) fn execute(
        &mut self,
        command: Command,
        index: u64,
    ) -> Result<(ProgramOutcome, bool, u64), ApplicationError> {
        let id = command.program.id();
        if let Some(record) = self.records.get(&id) {
            if record.program != command.program {
                return Ok((
                    ProgramOutcome::Conflict(ProgramConflict::Transaction),
                    true,
                    0,
                ));
            }
            if let Some(original) = &record.phases[command.slot()] {
                return Ok((
                    if original.command == command {
                        original.outcome
                    } else {
                        ProgramOutcome::Conflict(ProgramConflict::Operation)
                    },
                    true,
                    original.index,
                ));
            }
        } else {
            if self.records.len() == self.transaction_capacity() {
                return Err(ApplicationError::DedupCapacity);
            }
            let phase = match (self.role, command.step, command.decision) {
                (ProgramRole::Coordinator, ProgramStep::Start, None) => ProgramPhase::Started,
                (ProgramRole::Participant, ProgramStep::Prepare, None)
                | (ProgramRole::Participant, ProgramStep::ApplyDecision, Some(Decision::Abort)) => {
                    ProgramPhase::Absent
                }
                _ => return Err(ApplicationError::InvalidCommand),
            };
            self.records.insert(
                id,
                Record {
                    program: command.program.clone(),
                    phase,
                    prepare: None,
                    prepared: Box::new([]),
                    phases: std::array::from_fn(|_| None),
                },
            );
        }
        let mut record = self.records[&id].clone();
        let outcome = match self.role {
            ProgramRole::Coordinator => coordinator(&mut record, &command),
            ProgramRole::Participant => self.participant_phase(&mut record, &command)?,
        };
        let slot = command.slot();
        record.phases[slot] = Some(StoredPhase {
            command,
            outcome,
            index,
        });
        self.records.insert(id, record);
        Ok((outcome, false, index))
    }
}
fn coordinator(record: &mut Record, command: &Command) -> ProgramOutcome {
    use ProgramPhase::*;
    match (record.phase, command.step, command.decision) {
        (Started, ProgramStep::Start, None) => ProgramOutcome::Started,
        (Started, ProgramStep::Decide, Some(d)) => {
            record.phase = Decided(d);
            ProgramOutcome::Decided(d)
        }
        (Decided(d), ProgramStep::AppliedAll, Some(original)) if d == original => {
            record.phase = AppliedAll(d);
            ProgramOutcome::AppliedAll(d)
        }
        (AppliedAll(d), ProgramStep::Finish, Some(original)) if d == original => {
            record.phase = Finished(d);
            ProgramOutcome::Finished(d)
        }
        _ => ProgramOutcome::Conflict(ProgramConflict::Phase),
    }
}
