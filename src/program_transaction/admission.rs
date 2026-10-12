// SPDX-License-Identifier: RPL-1.5
use super::*;
use crate::{application::*, raft::Raft};
impl ProgramApplication {
    fn reserve(
        &self,
        operation: OperationId,
        command: Command,
        reserved: &mut BTreeMap<TransactionId, Program>,
        phases: &mut BTreeMap<OperationId, Command>,
    ) -> Result<(), ApplicationError> {
        self.check_command(operation, &command)?;
        if reserved
            .get(&command.program.id())
            .is_some_and(|p| p != &command.program)
            || phases.get(&operation).is_some_and(|old| old != &command)
            || self
                .records
                .get(&command.program.id())
                .and_then(|r| r.phases[command.slot()].as_ref())
                .is_some_and(|old| old.command != command)
        {
            return Err(ApplicationError::OperationConflict);
        }
        if !reserved.contains_key(&command.program.id())
            && !matches!(
                (self.role, command.step, command.decision),
                (ProgramRole::Coordinator, ProgramStep::Start, None)
                    | (ProgramRole::Participant, ProgramStep::Prepare, None)
                    | (
                        ProgramRole::Participant,
                        ProgramStep::ApplyDecision,
                        Some(Decision::Abort)
                    )
            )
        {
            return Err(ApplicationError::InvalidCommand);
        }
        reserved.insert(command.program.id(), command.program.clone());
        if reserved.len() > self.transaction_capacity() {
            return Err(ApplicationError::DedupCapacity);
        }
        phases.insert(operation, command);
        Ok(())
    }
}
impl ProposalAdmission for ProgramApplication {
    fn validate_proposal_context(
        &self,
        core: &Raft,
        operation: OperationId,
        bytes: &[u8],
    ) -> Result<(), ApplicationError> {
        self.validate_group(core.state().bootstrap.group)?;
        self.check_command(operation, &codec::decode(bytes)?)
    }
    fn validate_proposal<'a>(
        &self,
        operation: OperationId,
        bytes: &[u8],
        pending: impl Iterator<Item = (OperationId, &'a [u8])>,
    ) -> Result<usize, ApplicationError> {
        let mut reserved: BTreeMap<_, _> = self
            .records
            .values()
            .map(|r| (r.program.id(), r.program.clone()))
            .collect();
        let mut phases = BTreeMap::new();
        for (op, bytes) in pending {
            self.reserve(op, codec::decode(bytes)?, &mut reserved, &mut phases)?;
        }
        self.reserve(operation, codec::decode(bytes)?, &mut reserved, &mut phases)?;
        Ok(std::mem::size_of::<ProgramReceipt>())
    }
}
