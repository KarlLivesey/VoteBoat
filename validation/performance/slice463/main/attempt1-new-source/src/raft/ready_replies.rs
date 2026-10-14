// SPDX-License-Identifier: RPL-1.5
// Copyright (c) 2026 Karl Livesey
//! Read-only planning of ordered ready replies and an ordinary command prefix.
use super::*;

impl Raft {
    /// No queued input or Core field is changed on Err. The runtime retains its
    /// original inputs and executes the ordinary first event on any refusal.
    pub(crate) fn propose_after_ready_replies(
        &mut self,
        replies: &[Message],
        proposals: Vec<Proposal>,
    ) -> Result<Vec<Effect>, RaftError> {
        self.ready_reply_state(replies.len())?;
        let progress = self.ready_reply_progress(replies)?;
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
        let update = LogUpdate {
            group: self.durable.bootstrap.group,
            expected_revision: self.durable.revision,
            hard_state: self.durable.hard_state,
            commit_index: self.durable.last_index(),
            suffix: Some(Suffix {
                from: first,
                entries,
            }),
            snapshot: None,
            snapshot_membership: None,
        };
        let next = OwnedNextLog::plan_copy(
            &LogView::new(&self.durable),
            &self.log_summary,
            &update,
            self.limits,
        )?;
        if !next.is_append() {
            return Err(RaftError::Busy);
        }
        // Allocate the outward container before the sole infallible promotion.
        let effects = vec![Effect::Persist(update)];
        self.batch_limits_locked = true;
        self.progress = progress;
        for reply in replies {
            self.retire_replication(reply.from);
            self.next_index.insert(reply.from, first);
        }
        self.pending = Some(Pending {
            next,
            membership: None,
            ticket: None,
            after: After::LeaderAppend,
            reply: None,
        });
        Ok(effects)
    }

    fn ready_reply_state(&self, count: usize) -> Result<(), RaftError> {
        if self.fenced {
            return Err(RaftError::Fenced);
        }
        if self.role != Role::Leader || !self.local_voter() {
            return Err(RaftError::NotLeader);
        }
        if count == 0
            || count > self.batch_limits.max_ready_reply_inputs
            || self.has_pending_dependency()
            || !self.log_summary.is_plain()
            || self.membership.joint().is_some()
            || self.membership.last_configuration_index() > self.durable.commit_index
            || self.transfer.is_some()
            || self.read.is_some()
            || self.ready_read.is_some()
            || self.authority_request.is_some()
            || self.replication_permit.is_some()
            || self.learner_readiness.is_some()
            || self.ready_learner.is_some()
            || self.durable.last_index() <= self.durable.commit_index
            || self.durable.term_at(self.durable.last_index()) != Some(self.durable.hard_state.term)
        {
            return Err(RaftError::Busy);
        }
        // The serialized history first commits, then appends. Do not rescue a
        // proposal that would fail its second revision or later request budget.
        self.durable
            .revision
            .get()
            .checked_add(2)
            .ok_or(RaftError::Exhausted)?;
        let contexts = self
            .membership
            .replicas()
            .count()
            .checked_mul(4)
            .and_then(|n| n.checked_add(count))
            .ok_or(RaftError::Exhausted)?;
        self.request_sequence
            .checked_add(contexts as u64)
            .ok_or(RaftError::Exhausted)?;
        Ok(())
    }

    fn ready_reply_progress(
        &self,
        replies: &[Message],
    ) -> Result<BTreeMap<NodeId, u64>, RaftError> {
        let tip = self.durable.last_index();
        let next = tip.checked_add(1).ok_or(RaftError::Exhausted)?;
        let mut progress = self.progress.clone();
        let mut peers = BTreeSet::new();
        for (index, reply) in replies.iter().enumerate() {
            self.authenticated_message_source(reply)?;
            let sent = self
                .requests
                .get(&reply.from)
                .ok_or(RaftError::InvalidMessage)?;
            if reply.term != self.durable.hard_state.term
                || reply.rpc
                    != (Rpc::Appended {
                        success: true,
                        matching_index: tip,
                    })
                || sent.context != reply.context
                || sent.configuration != reply.configuration
                || sent.snapshot.is_some()
                || sent.end != tip
                || sent.start == 0
                || sent.start > next
                || self.empty_append_aliases.contains_key(&reply.from)
                || self.admitted_ranges.contains_key(&reply.from)
                || !peers.insert(reply.from)
            {
                return Err(RaftError::InvalidMessage);
            }
            progress.insert(reply.from, tip);
            if index == 0 && self.membership.frontier(&progress) < tip {
                return Err(RaftError::Busy);
            }
        }
        Ok(progress)
    }
}

#[cfg(test)]
mod tests;
