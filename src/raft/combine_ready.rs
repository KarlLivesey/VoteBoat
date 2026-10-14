// SPDX-License-Identifier: RPL-1.5
// Copyright (c) 2026 Karl Livesey
//! Fuse only an exact, still-private commit request with ready ordinary input.
use super::*;

impl Raft {
    // The old suffix is locally durable and already covered by a current-term
    // quorum. This predicate grants no durable commit or new-entry authority.
    pub(super) fn ready_commit_tip(&self, commit: u64) -> bool {
        commit > self.durable.commit_index
            && commit == self.durable.last_index()
            && self.durable.term_at(commit) == Some(self.durable.hard_state.term)
            && self.membership().frontier(&self.progress) >= commit
    }

    pub(crate) fn can_combine_ready_commit(&self, update: &LogUpdate) -> bool {
        let Some(pending) = &self.pending else {
            return false;
        };
        let metadata = pending.next.metadata();
        !self.fenced
            && self.role == Role::Leader
            && self.local_voter()
            && self.log_summary.is_plain()
            && self.membership.joint().is_none()
            && self.membership.last_configuration_index() <= self.durable.commit_index
            && self.transfer.is_none()
            && self.read.is_none()
            && self.ready_read.is_none()
            && self.readiness_check.is_none()
            && self.learner_readiness.is_none()
            && self.ready_learner.is_none()
            && self.staged_snapshot.is_none()
            && self.application_install.is_none()
            && self.checkpoint_requested.is_none()
            && self.checkpoint_reconcile.is_none()
            && pending.ticket.is_none()
            && pending.membership.is_none()
            && pending.reply.is_none()
            && matches!(pending.after, After::Commit)
            && pending.next.is_append()
            && pending.next.matches_update(update)
            && update.expected_revision == self.durable.revision
            && update.hard_state == self.durable.hard_state
            && update.snapshot.is_none()
            && update.snapshot_membership.is_none()
            && update.suffix.is_none()
            && metadata.generation == self.durable.generation
            && metadata.ballot_origin == self.durable.ballot_origin
            && self.ready_commit_tip(update.commit_index)
    }

    /// The caller still owns the sole exact commit effect and every queued
    /// original. An error changes neither the pending request nor that effect;
    /// the caller must immediately retain the original serialized path.
    pub(crate) fn combine_ready_commit(
        &mut self,
        update: &LogUpdate,
        proposals: Vec<Proposal>,
    ) -> Result<Vec<Effect>, RaftError> {
        if self.fenced {
            return Err(RaftError::Fenced);
        }
        if !self.can_combine_ready_commit(update) {
            return Err(RaftError::WrongCompletion);
        }
        let first = self.validate_proposal_batch(&proposals, proposals.capacity())?;
        self.validate_conversion_peak(&proposals, proposals.capacity())?;
        let entries = proposals
            .into_iter()
            .enumerate()
            .map(|(offset, proposal)| LogEntry {
                index: first + offset as u64,
                term: self.durable.hard_state.term,
                payload: EntryPayload::Command {
                    operation: proposal.operation,
                    bytes: proposal.bytes.into(),
                },
            })
            .collect();
        let combined = LogUpdate {
            suffix: Some(Suffix {
                from: first,
                entries,
            }),
            ..update.clone()
        };
        // Plan against the original durable predecessor without taking or
        // modifying the existing commit dependency, even on a late error.
        let next = OwnedNextLog::plan_copy(
            &LogView::new(&self.durable),
            &self.log_summary,
            &combined,
            self.limits,
        )?;
        if !next.is_append() {
            return Err(RaftError::WrongCompletion);
        }
        let effects = vec![Effect::Persist(combined)];
        self.pending = Some(Pending {
            next,
            membership: None,
            ticket: None,
            after: After::LeaderAppend,
            reply: None,
        });
        Ok(effects)
    }
}

#[cfg(test)]
mod tests;
