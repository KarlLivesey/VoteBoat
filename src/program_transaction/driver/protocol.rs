// SPDX-License-Identifier: RPL-1.5
use super::*;
impl Work {
    fn program(&self) -> Result<&Program, ProgramDriverError> {
        self.program
            .as_ref()
            .ok_or(ProgramDriverError::Inconsistent)
    }
    pub(super) fn command(&self) -> Result<(GroupIdentity, Command), ProgramDriverError> {
        let program = self.program()?;
        let mut command = Command::start(program.clone());
        let group = match self.stage {
            ProgramDriverStage::Start => program.coordinator(),
            ProgramDriverStage::Prepare(p) => {
                command.step = ProgramStep::Prepare;
                program
                    .participants()
                    .get(p)
                    .ok_or(ProgramDriverError::Inconsistent)?
                    .group()
            }
            ProgramDriverStage::Decide => {
                command.step = ProgramStep::Decide;
                command.decision = self.decision;
                command.facts = self.facts.clone().into_boxed_slice();
                command.refusal = self.refusal;
                program.coordinator()
            }
            ProgramDriverStage::ApplyDecision(p) => {
                command.step = ProgramStep::ApplyDecision;
                command.decision = self.decision;
                command.facts =
                    Box::new([self.decision_fact.ok_or(ProgramDriverError::Inconsistent)?]);
                program
                    .participants()
                    .get(p)
                    .ok_or(ProgramDriverError::Inconsistent)?
                    .group()
            }
            ProgramDriverStage::AppliedAll => {
                command.step = ProgramStep::AppliedAll;
                command.decision = self.decision;
                command.facts = self.facts.clone().into_boxed_slice();
                program.coordinator()
            }
            ProgramDriverStage::Release(p) => {
                command.step = ProgramStep::Release;
                command.decision = self.decision;
                command.facts =
                    Box::new([self.all_fact.ok_or(ProgramDriverError::Inconsistent)?]);
                program
                    .participants()
                    .get(p)
                    .ok_or(ProgramDriverError::Inconsistent)?
                    .group()
            }
            ProgramDriverStage::Finish => {
                command.step = ProgramStep::Finish;
                command.decision = self.decision;
                command.facts = self.facts.clone().into_boxed_slice();
                program.coordinator()
            }
            _ => return Err(ProgramDriverError::Inconsistent),
        };
        command
            .validate()
            .map_err(ProgramDriverError::Application)?;
        Ok((group, command))
    }
    pub(super) fn retry_status(&mut self) {
        self.stage = ProgramDriverStage::CoordinatorStatus;
        self.route = self.route.wrapping_add(1)
    }
    pub(super) fn receipt(
        &mut self,
        group: GroupIdentity,
        command: Command,
        receipt: ProgramReceipt,
    ) -> Result<(), ProgramDriverError> {
        if self.program() != Ok(&command.program)
            || command
                .operation(group)
                .map_err(ProgramDriverError::Application)?
                != receipt.operation
            || receipt.first_index == 0
            || receipt.first_index > receipt.index
            || (!receipt.duplicate && receipt.first_index != receipt.index)
        {
            return Err(ProgramDriverError::Inconsistent);
        }
        let fact = PhaseFact {
            group,
            operation: receipt.operation,
            index: receipt.first_index,
        };
        match (self.stage, receipt.outcome) {
            (ProgramDriverStage::Start, ProgramOutcome::Started) => {
                self.stage = ProgramDriverStage::Prepare(0)
            }
            (ProgramDriverStage::Prepare(p), ProgramOutcome::Prepare(PrepareOutcome::Prepared)) => {
                self.record_fact(p, fact)?;
                if p + 1 < self.program()?.participants().len() {
                    self.stage = ProgramDriverStage::Prepare(p + 1)
                } else {
                    self.decision = Some(Decision::Commit);
                    self.stage = ProgramDriverStage::Decide
                }
            }
            (
                ProgramDriverStage::Prepare(p),
                ProgramOutcome::Prepare(PrepareOutcome::Refused(reason)),
            ) => {
                self.facts.clear();
                self.facts.push(fact);
                self.refusal = Some((p, reason));
                self.decision = Some(Decision::Abort);
                self.stage = ProgramDriverStage::Decide;
            }
            (
                ProgramDriverStage::Prepare(_),
                ProgramOutcome::Prepare(PrepareOutcome::Closed(_)),
            ) => self.retry_status(),
            (ProgramDriverStage::Decide, ProgramOutcome::Decided(d))
                if Some(d) == self.decision =>
            {
                self.decision_fact = Some(fact);
                self.facts.clear();
                self.stage = ProgramDriverStage::ApplyDecision(0)
            }
            (ProgramDriverStage::ApplyDecision(p), ProgramOutcome::DecisionApplied(d))
                if Some(d) == self.decision =>
            {
                self.record_fact(p, fact)?;
                self.stage = if p + 1 < self.program()?.participants().len() {
                    ProgramDriverStage::ApplyDecision(p + 1)
                } else {
                    ProgramDriverStage::AppliedAll
                }
            }
            (ProgramDriverStage::AppliedAll, ProgramOutcome::AppliedAll(d))
                if Some(d) == self.decision =>
            {
                self.all_fact = Some(fact);
                self.facts.clear();
                self.stage = ProgramDriverStage::Release(0)
            }
            (ProgramDriverStage::Release(p), ProgramOutcome::Released(d))
                if Some(d) == self.decision =>
            {
                self.record_fact(p, fact)?;
                self.stage = if p + 1 < self.program()?.participants().len() {
                    ProgramDriverStage::Release(p + 1)
                } else {
                    ProgramDriverStage::Finish
                }
            }
            (ProgramDriverStage::Finish, ProgramOutcome::Finished(d))
                if Some(d) == self.decision =>
            {
                self.finish(ProgramDriverOutcome::Finished {
                    transaction: self.ticket.transaction,
                    decision: d,
                    first_index: receipt.first_index,
                })
            }
            _ => return Err(ProgramDriverError::Inconsistent),
        }
        Ok(())
    }
    fn record_fact(&mut self, p: usize, fact: PhaseFact) -> Result<(), ProgramDriverError> {
        if self.facts.len() != p {
            return Err(ProgramDriverError::Inconsistent);
        }
        self.facts.push(fact);
        Ok(())
    }
    pub(super) fn finish(&mut self, outcome: ProgramDriverOutcome) {
        self.outcome = Some(outcome);
        self.stage = ProgramDriverStage::Terminal
    }
    pub(super) fn status(
        &mut self,
        status: Option<Box<ProgramStatus>>,
        barrier: u64,
    ) -> Result<(), ProgramDriverError> {
        let Some(status) = status else {
            self.finish(ProgramDriverOutcome::Unknown {
                transaction: self.ticket.transaction,
                reason: ProgramDriverUnknown::NoRetainedStart,
            });
            return Ok(());
        };
        if status.program.id() != self.ticket.transaction
            || self.program.as_ref().is_some_and(|p| *p != status.program)
            || status.prepare.is_some()
            || !status.prepared.is_empty()
        {
            return Err(ProgramDriverError::Inconsistent);
        }
        let (slot, decision) = match status.phase {
            ProgramPhase::Started => (0, None),
            ProgramPhase::Decided(d) => (1, Some(d)),
            ProgramPhase::AppliedAll(d) => (2, Some(d)),
            ProgramPhase::Finished(d) => (3, Some(d)),
            _ => return Err(ProgramDriverError::Inconsistent),
        };
        let mut previous = 0;
        for i in 0..4 {
            match &status.phases[i] {
                Some(phase) if i <= slot => {
                    validate_phase(&status.program, phase, i, decision)?;
                    if phase.index <= previous || phase.index > barrier {
                        return Err(ProgramDriverError::Inconsistent);
                    }
                    previous = phase.index;
                }
                None if i > slot => {}
                _ => return Err(ProgramDriverError::Inconsistent),
            }
        }
        let phase = status.phases[slot]
            .as_ref()
            .ok_or(ProgramDriverError::Inconsistent)?;
        let fact = PhaseFact {
            group: status.program.coordinator(),
            operation: phase.operation,
            index: phase.index,
        };
        self.program = Some(status.program.clone());
        self.decision = decision;
        self.facts.clear();
        self.refusal = None;
        match slot {
            0 => self.stage = ProgramDriverStage::Prepare(0),
            1 => {
                self.decision_fact = Some(fact);
                self.stage = ProgramDriverStage::ApplyDecision(0)
            }
            2 => {
                self.all_fact = Some(fact);
                self.stage = ProgramDriverStage::Release(0)
            }
            _ => self.finish(ProgramDriverOutcome::Finished {
                transaction: self.ticket.transaction,
                decision: decision.ok_or(ProgramDriverError::Inconsistent)?,
                first_index: phase.index,
            }),
        }
        Ok(())
    }
}
fn validate_phase(
    program: &Program,
    phase: &PhaseStatus,
    slot: usize,
    decision: Option<Decision>,
) -> Result<(), ProgramDriverError> {
    let step = [
        ProgramStep::Start,
        ProgramStep::Decide,
        ProgramStep::AppliedAll,
        ProgramStep::Finish,
    ][slot];
    let expected = match slot {
        0 => ProgramOutcome::Started,
        1 => ProgramOutcome::Decided(decision.ok_or(ProgramDriverError::Inconsistent)?),
        2 => ProgramOutcome::AppliedAll(decision.ok_or(ProgramDriverError::Inconsistent)?),
        _ => ProgramOutcome::Finished(decision.ok_or(ProgramDriverError::Inconsistent)?),
    };
    if phase.index == 0
        || phase.operation
            != program
                .operation(slot)
                .map_err(ProgramDriverError::Application)?
        || phase.step != step
        || phase.outcome != expected
        || phase.decision != if slot == 0 { None } else { decision }
    {
        return Err(ProgramDriverError::Inconsistent);
    }
    Command {
        program: program.clone(),
        step,
        decision: phase.decision,
        facts: phase.facts.clone(),
        refusal: phase.refusal,
    }
    .validate()
    .map_err(|_| ProgramDriverError::Inconsistent)
}
