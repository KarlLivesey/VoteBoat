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
use codec::Envelope;
impl<A: ProposalAdmission + CheckpointStateMachine> LogicalActivationApplication<A>
where
    A::Receipt: ApplicationReceipt,
{
    fn check_control(
        &self,
        operation: OperationId,
        c: &ControlCommand,
    ) -> Result<(), ApplicationError> {
        if let ControlCommand::Begin(intent) = c {
            self.check_intent(intent)?;
        }
        if self.control.operation(c)? != operation {
            return Err(ApplicationError::OperationConflict);
        }
        let mut state = self.control.clone();
        state.execute(c.clone(), 1, 1, self.limits)?;
        Ok(())
    }
    /// Stateless: every phase ID is reserved and no business ID may be, so the
    /// two kinds stay disjoint for the group's life without any history.
    pub(super) fn check_business(operation: OperationId) -> Result<(), ApplicationError> {
        if ChildActivationOperations::is_reserved(operation) {
            return Err(ApplicationError::InvalidCommand);
        }
        Ok(())
    }
}
impl<A: ProposalAdmission + CheckpointStateMachine> ProposalAdmission
    for LogicalActivationApplication<A>
where
    A::Receipt: ApplicationReceipt,
{
    fn validate_proposal_context(
        &self,
        core: &crate::raft::Raft,
        operation: OperationId,
        bytes: &[u8],
    ) -> Result<(), ApplicationError> {
        if core.state().bootstrap.group != self.parent {
            return Err(ApplicationError::InvalidCommand);
        }
        if let Envelope::Business(raw) = codec::decode(bytes, self.limits.command_bytes)? {
            self.inner.validate_proposal_context(core, operation, raw)?;
        }
        Ok(())
    }
    fn validate_proposal<'a>(
        &self,
        operation: OperationId,
        bytes: &[u8],
        pending: impl Iterator<Item = (OperationId, &'a [u8])>,
    ) -> Result<usize, ApplicationError> {
        let mut beginnings = BTreeMap::new();
        let mut business = Vec::new();
        let mut inputs = Vec::new();
        let mut retained_bytes = 0usize;
        for (id, encoded) in pending {
            retained_bytes = retained_bytes
                .checked_add(encoded.len())
                .ok_or(ApplicationError::ReceiptBudget)?;
            if inputs.len() >= 65536
                || encoded.len() > self.limits.command_bytes
                || retained_bytes > self.limits.checkpoint_bytes
            {
                return Err(ApplicationError::ReceiptBudget);
            }
            inputs.push((id, encoded.to_vec()));
        }
        inputs.push((operation, bytes.to_vec()));
        for (id, encoded) in &inputs {
            let id = *id;
            match codec::decode(encoded, self.limits.command_bytes)? {
                Envelope::Business(raw) => {
                    Self::check_business(id)?;
                    business.push((id, raw));
                }
                Envelope::Control(c) => {
                    self.check_control(id, &c)?;
                    if let ControlCommand::Begin(intent) = c {
                        if let Some(previous) = beginnings.insert(id, intent.clone()) {
                            if previous != intent {
                                return Err(ApplicationError::OperationConflict);
                            }
                        }
                    }
                }
            }
            if business.len() > 65536 || beginnings.len() > self.limits.intents {
                return Err(ApplicationError::DedupCapacity);
            }
        }
        self.check_reservations(&beginnings)?;
        let mut bound = std::mem::size_of::<ActivationReceipt<A::Receipt>>();
        if let Envelope::Business(raw) = codec::decode(bytes, self.limits.command_bytes)? {
            bound = bound
                .checked_add(
                    self.inner
                        .validate_proposal(operation, raw, business.into_iter())?,
                )
                .ok_or(ApplicationError::ReceiptBudget)?;
        }
        Ok(bound)
    }
}
impl<A: ProposalAdmission + CheckpointStateMachine> LogicalActivationApplication<A>
where
    A::Receipt: ApplicationReceipt,
{
    fn check_reservations(
        &self,
        beginnings: &BTreeMap<OperationId, Box<ChildActivationIntent>>,
    ) -> Result<(), ApplicationError> {
        let mut reservations = BTreeMap::new();
        let mut ids = BTreeSet::new();
        for intent in self
            .control
            .records
            .values()
            .map(|r| &r.intent)
            .chain(beginnings.values())
        {
            ids.insert(intent.operations.begin);
            for phase in intent.operations.ids() {
                if let Some(existing) = reservations.insert(phase, intent.operations.begin) {
                    if existing != intent.operations.begin {
                        return Err(ApplicationError::OperationConflict);
                    }
                }
            }
        }
        if ids.len() > self.limits.intents {
            return Err(ApplicationError::DedupCapacity);
        }
        Ok(())
    }
    pub(super) fn execute_control(
        &mut self,
        operation: OperationId,
        c: ControlCommand,
        index: u64,
        term: u64,
    ) -> Result<ChildActivationReceipt, ApplicationError> {
        self.check_control(operation, &c)?;
        self.control.execute(c, index, term, self.limits)
    }
}
