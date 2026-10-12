// SPDX-License-Identifier: RPL-1.5
use super::*;

#[derive(Clone, Debug, Eq, PartialEq)]
pub(crate) struct Command {
    pub(crate) program: Program,
    pub(crate) step: ProgramStep,
    pub(crate) decision: Option<Decision>,
    pub(crate) facts: Box<[PhaseFact]>,
    pub(crate) refusal: Option<(usize, PrepareRefusal)>,
}
impl Command {
    pub(crate) fn start(program: Program) -> Self {
        Self {
            program,
            step: ProgramStep::Start,
            decision: None,
            facts: Box::new([]),
            refusal: None,
        }
    }
    pub(crate) fn slot(&self) -> usize {
        match self.step {
            ProgramStep::Start | ProgramStep::Prepare => 0,
            ProgramStep::Decide | ProgramStep::ApplyDecision => 1,
            ProgramStep::AppliedAll | ProgramStep::Release => 2,
            ProgramStep::Finish => 3,
        }
    }
    pub(crate) fn operation(&self, group: GroupIdentity) -> Result<OperationId, ApplicationError> {
        let valid = match self.step {
            ProgramStep::Start
            | ProgramStep::Decide
            | ProgramStep::AppliedAll
            | ProgramStep::Finish => group == self.program.coordinator(),
            _ => self.program.participant(group).is_some(),
        };
        if !valid {
            return Err(ApplicationError::InvalidCommand);
        }
        self.program.operation(self.slot())
    }
    pub(crate) fn validate(&self) -> Result<(), ApplicationError> {
        use ProgramStep::*;
        match (self.step, self.decision, self.refusal) {
            (Start | Prepare, None, None) if self.facts.is_empty() => Ok(()),
            (Decide, Some(Decision::Commit), None) => self.participant_facts(0),
            (Decide, Some(Decision::Abort), Some((p, _)))
                if p < self.program.participants().len() =>
            {
                if self.facts.len() != 1 {
                    return Err(ApplicationError::InvalidCommand);
                }
                self.fact(&self.facts[0], self.program.participants()[p].group(), 0)
            }
            (ApplyDecision, Some(_), None) => self.coordinator_fact(1),
            (AppliedAll, Some(_), None) => self.participant_facts(1),
            (Release, Some(_), None) => self.coordinator_fact(2),
            (Finish, Some(_), None) => self.participant_facts(2),
            _ => Err(ApplicationError::InvalidCommand),
        }
    }
    fn coordinator_fact(&self, slot: usize) -> Result<(), ApplicationError> {
        if self.facts.len() != 1 {
            return Err(ApplicationError::InvalidCommand);
        }
        self.fact(&self.facts[0], self.program.coordinator(), slot)
    }
    fn participant_facts(&self, slot: usize) -> Result<(), ApplicationError> {
        if self.facts.len() != self.program.participants().len() {
            return Err(ApplicationError::InvalidCommand);
        }
        for (fact, p) in self.facts.iter().zip(self.program.participants()) {
            self.fact(fact, p.group(), slot)?;
        }
        Ok(())
    }
    fn fact(
        &self,
        fact: &PhaseFact,
        group: GroupIdentity,
        slot: usize,
    ) -> Result<(), ApplicationError> {
        if fact.group != group || fact.operation != self.program.operation(slot)? || fact.index == 0
        {
            Err(ApplicationError::InvalidCommand)
        } else {
            Ok(())
        }
    }
}
