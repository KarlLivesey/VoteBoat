// SPDX-License-Identifier: RPL-1.5
use super::*;
impl ProgramApplication {
    pub(super) fn participant_phase(
        &mut self,
        record: &mut Record,
        command: &Command,
    ) -> Result<ProgramOutcome, ApplicationError> {
        match command.step {
            ProgramStep::Prepare => self.prepare(record),
            ProgramStep::ApplyDecision => self.apply_decision(
                record,
                command.decision.ok_or(ApplicationError::InvalidCommand)?,
            ),
            ProgramStep::Release => self.release(
                record,
                command.decision.ok_or(ApplicationError::InvalidCommand)?,
            ),
            _ => Ok(ProgramOutcome::Conflict(ProgramConflict::Phase)),
        }
    }
    fn prepare(&mut self, record: &mut Record) -> Result<ProgramOutcome, ApplicationError> {
        let outcome = match record.phase {
            ProgramPhase::DecisionApplied(d) | ProgramPhase::Released(d) => {
                PrepareOutcome::Closed(d)
            }
            ProgramPhase::Absent => {
                let p = record
                    .program
                    .participant(self.group)
                    .ok_or(ApplicationError::InvalidCommand)?;
                let accesses = record.program.participants()[p].keys();
                if let Some(refusal) = accesses.iter().find_map(|a| self.prepare_refusal(*a)) {
                    PrepareOutcome::Refused(refusal)
                } else {
                    record.prepared = accesses
                        .iter()
                        .map(|a| {
                            let key = &self.keys[&a.key];
                            PreparedKey {
                                key: a.key,
                                value: key.value,
                                version: key.version,
                            }
                        })
                        .collect();
                    for access in accesses {
                        self.keys.get_mut(&access.key).expect("checked key").lock =
                            Some(record.program.id());
                    }
                    record.phase = ProgramPhase::Prepared;
                    PrepareOutcome::Prepared
                }
            }
            _ => return Ok(ProgramOutcome::Conflict(ProgramConflict::Phase)),
        };
        record.prepare = Some(outcome);
        Ok(ProgramOutcome::Prepare(outcome))
    }
    fn prepare_refusal(&self, access: KeyAccess) -> Option<PrepareRefusal> {
        let key = &self.keys[&access.key];
        if let Some(owner) = key.lock {
            Some(PrepareRefusal::Locked {
                key: access.key,
                owner,
            })
        } else if key.version != access.expected_version {
            Some(PrepareRefusal::Version {
                key: access.key,
                actual: key.version,
            })
        } else if access.write.is_some() && key.version.next().is_none() {
            Some(PrepareRefusal::VersionExhausted { key: access.key })
        } else {
            None
        }
    }
    fn owns_footprint(&self, record: &Record) -> bool {
        let Some(p) = record.program.participant(self.group) else {
            return false;
        };
        record.program.participants()[p]
            .keys()
            .iter()
            .all(|a| self.keys[&a.key].lock == Some(record.program.id()))
    }
    fn apply_decision(
        &mut self,
        record: &mut Record,
        decision: Decision,
    ) -> Result<ProgramOutcome, ApplicationError> {
        match (record.phase, decision) {
            (ProgramPhase::Prepared, d) if self.owns_footprint(record) => {
                if d == Decision::Commit {
                    let p = record
                        .program
                        .participant(self.group)
                        .ok_or(ApplicationError::InvalidCommand)?;
                    for access in record.program.participants()[p].keys() {
                        if let Some(value) = access.write {
                            let key = self.keys.get_mut(&access.key).expect("checked key");
                            key.version =
                                key.version.next().ok_or(ApplicationError::InvalidCommand)?;
                            key.value = value;
                        }
                    }
                }
            }
            (ProgramPhase::Absent, Decision::Abort) => {}
            _ => return Ok(ProgramOutcome::Conflict(ProgramConflict::Phase)),
        }
        record.phase = ProgramPhase::DecisionApplied(decision);
        Ok(ProgramOutcome::DecisionApplied(decision))
    }
    fn release(
        &mut self,
        record: &mut Record,
        decision: Decision,
    ) -> Result<ProgramOutcome, ApplicationError> {
        if record.phase != ProgramPhase::DecisionApplied(decision) {
            return Ok(ProgramOutcome::Conflict(ProgramConflict::Phase));
        }
        let p = record
            .program
            .participant(self.group)
            .ok_or(ApplicationError::InvalidCommand)?;
        for access in record.program.participants()[p].keys() {
            let key = self.keys.get_mut(&access.key).expect("checked key");
            if key.lock == Some(record.program.id()) {
                key.lock = None;
            }
        }
        record.phase = ProgramPhase::Released(decision);
        Ok(ProgramOutcome::Released(decision))
    }
}
