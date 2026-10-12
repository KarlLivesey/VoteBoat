// SPDX-License-Identifier: RPL-1.5
// Copyright (c) 2026 Karl Livesey
//! No response collector is invented for these genuine original output paths.
use super::*;

impl Raft {
    pub(in crate::raft::request_window) fn original_one_way(&self, m: &Message) -> bool {
        if let Rpc::TimeoutNow {
            operation,
            index,
            log_term,
        } = m.rpc
        {
            return self.leadership_transfer().is_some_and(|t| {
                t.signal_sent
                    && t.request.target.node == m.to
                    && t.request.operation == operation
                    && t.request.configuration == m.configuration
                    && t.context == m.context
                    && t.term == m.term
                    && t.index == index
                    && t.term == log_term
            });
        }
        let Rpc::Append {
            previous_index,
            previous_term,
            entries,
            leader_commit,
        } = &m.rpc
        else {
            return false;
        };
        if m.configuration != self.membership().id() {
            return false;
        }
        if !self.local_voter() && entries.is_empty() && leader_commit == previous_index {
            return self
                .retirement_predecessor()
                .is_some_and(|(index, term, previous)| {
                    *previous_index == index
                        && *previous_term == term
                        && m.term == term
                        && previous.voter_store(self.node) == Some(self.binding.identity)
                        && self.membership().replica_store(m.to).is_some()
                });
        }
        self.candidate_joint_output(m, *previous_index, *previous_term, entries, *leader_commit)
    }
    fn candidate_joint_output(
        &self,
        m: &Message,
        previous: u64,
        term: u64,
        entries: &[LogEntry],
        commit: u64,
    ) -> bool {
        let Some(joint) = self.membership().joint() else {
            return false;
        };
        if self.role != Role::Candidate
            || self.batched_joint_repair
            || commit != 0
            || joint.index <= self.durable.commit_index
            || self.membership().stable().voter_stores().get(&self.node)
                != Some(&self.binding.identity)
            || self.membership().stable().learners().get(&m.to)
                != joint.next.voter_stores().get(&m.to)
            || !joint.next.voter_stores().contains_key(&m.to)
            || self.durable.term_at(previous) != Some(term)
            || entries.is_empty()
            || entries.len() > 64
        {
            return false;
        }
        entries.last().is_some_and(|e| e.index == joint.index)
            && entries.iter().enumerate().all(|(offset, e)| {
                previous.checked_add(offset as u64 + 1) == Some(e.index)
                    && self.durable.entry_at(e.index) == Some(e)
                    && (e.index == joint.index
                        || !matches!(e.payload, EntryPayload::Configuration(_)))
            })
    }
}
