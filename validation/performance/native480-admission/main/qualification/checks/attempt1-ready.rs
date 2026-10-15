// SPDX-License-Identifier: RPL-1.5
// Copyright (c) 2026 Karl Livesey
//! Application checks for original commands in a ready prefix.
use super::*;

#[derive(Default)]
pub(super) struct ReadyValidation<'a> {
    prepared: bool,
    context: Option<ReadyAdmission<'a>>,
}

impl<R: ApplicationReceipt> ClientRouter<R> {
    pub(super) fn check_ready_execution<'a, A: ProposalAdmission<Receipt = R> + 'a>(
        &self,
        core: &Raft,
        event: &Event,
        prefix: &[ReadyProposal],
        application: &impl Fn(GroupIdentity) -> Option<&'a A>,
        configuration: &mut impl FnMut(&Raft, &Event) -> Result<(), RaftError>,
        validation: &mut ReadyValidation<'a>,
    ) -> Result<(), RaftError> {
        let group = core.state().bootstrap.group;
        if let Event::Configure(proposal) = event {
            application(group)
                .ok_or(RaftError::Admission(ApplicationError::NotApplied))?
                .validate_deployment_requirements(proposal.requirements)
                .map_err(RaftError::Admission)?;
        }
        configuration(core, event)?;
        let Event::Propose { operation, bytes } = event else {
            return Ok(());
        };
        if core.role() != Role::Leader {
            return Err(RaftError::NotLeader);
        }
        let app = application(group).ok_or(RaftError::Admission(ApplicationError::NotApplied))?;
        if !prefix.is_empty() && !app.supports_ready_batch() {
            return Err(RaftError::Busy);
        }
        app.validate_proposal_context(core, *operation, bytes)
            .map_err(RaftError::Admission)?;
        if app.applied_index() < core.state().base_index()
            || app.applied_index() > core.state().commit_index
        {
            return Err(RaftError::Admission(ApplicationError::NotApplied));
        }
        let bound = self
            .validate_ready_proposal(core, app, (*operation, bytes), prefix, validation)
            .map_err(RaftError::Admission)?;
        let reserved = self
            .pending
            .values()
            .filter(|p| {
                p.ticket.group == group && p.ticket.operation == *operation && p.bytes == *bytes
            })
            .map(|p| p.receipt_bound)
            .min()
            .unwrap_or(self.limits.receipt_bytes);
        if bound < size_of::<R>() || bound > reserved || bound > self.limits.receipt_bytes {
            return Err(RaftError::Admission(ApplicationError::ReceiptBudget));
        }
        Ok(())
    }

    fn validate_ready_proposal<'a, A: ProposalAdmission<Receipt = R> + 'a>(
        &self,
        core: &Raft,
        app: &'a A,
        (operation, bytes): (OperationId, &[u8]),
        prefix: &[ReadyProposal],
        validation: &mut ReadyValidation<'a>,
    ) -> Result<usize, ApplicationError> {
        if !prefix.is_empty() && !validation.prepared {
            validation.context = app.prepare_ready_admission(self.ready_pending(
                core,
                app.applied_index(),
                prefix,
            ));
            validation.prepared = true;
        }
        match validation.context.as_mut() {
            Some(context) => context(operation, bytes),
            None => app.validate_proposal(
                operation,
                bytes,
                self.ready_pending(core, app.applied_index(), prefix),
            ),
        }
    }

    fn ready_pending<'a>(
        &'a self,
        core: &'a Raft,
        applied: u64,
        prefix: &'a [ReadyProposal],
    ) -> impl Iterator<Item = (OperationId, &'a [u8])> {
        let group = core.state().bootstrap.group;
        let log = core
            .state()
            .entries
            .iter()
            .skip((applied - core.state().base_index()) as usize)
            .filter_map(|e| match &e.payload {
                EntryPayload::Command { operation, bytes } => Some((*operation, bytes.as_slice())),
                _ => None,
            });
        let pending = self
            .pending
            .values()
            .filter(|p| p.ticket.group == group)
            .map(|p| (p.ticket.operation, p.bytes.as_slice()));
        // Registered originals already occur in the router's pending iterator.
        // Add only otherwise unrepresented selected commands, once each.
        let selected = prefix
            .iter()
            .filter(|selected| {
                !self
                    .admissions
                    .get(&selected.admission.sequence)
                    .and_then(|sequence| self.pending.get(sequence))
                    .is_some_and(|p| {
                        p.admission == selected.admission
                            && p.ticket.operation == selected.command.operation
                            && p.bytes == selected.command.bytes
                    })
            })
            .map(|p| (p.command.operation, p.command.bytes.as_slice()));
        log.chain(pending).chain(selected)
    }
}
