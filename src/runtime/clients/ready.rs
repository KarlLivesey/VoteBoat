// SPDX-License-Identifier: RPL-1.5
// Copyright (c) 2026 Karl Livesey
//! Application checks for original commands in a ready prefix.
use super::*;

impl<R: ApplicationReceipt> ClientRouter<R> {
    pub(super) fn check_ready_execution<'a, A: ProposalAdmission<Receipt = R> + 'a>(
        &self,
        core: &Raft,
        event: &Event,
        prefix: &[ReadyProposal],
        application: &impl Fn(GroupIdentity) -> Option<&'a A>,
        configuration: &mut impl FnMut(&Raft, &Event) -> Result<(), RaftError>,
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
        let log = core
            .state()
            .entries
            .iter()
            .skip((app.applied_index() - core.state().base_index()) as usize)
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
        let bound = app
            .validate_proposal(*operation, bytes, log.chain(pending).chain(selected))
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
}
