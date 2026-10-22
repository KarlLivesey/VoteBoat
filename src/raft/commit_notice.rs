// SPDX-License-Identifier: RPL-1.5
// Copyright (c) 2026 Karl Livesey
//! Explicit commit-purpose output, never replication or reply evidence.
use super::*;

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub(super) struct CommitNoticeEmission {
    context: RequestContext,
    configuration: ConfigurationId,
    term: u64,
    index: u64,
    log_term: u64,
    recipient: StoreIdentity,
}
impl Raft {
    /// Select an equal-guarantee physical delivery composition before activity.
    /// Every endpoint and the host delivery path must understand CommitNotice
    /// (native consumers require wire9). Logical assemblies do not support it.
    /// No voter/progress/durability authority is granted. Recovery requires a
    /// fresh selection; ordinary heartbeats/probes/retries remain acknowledged.
    pub fn enable_commit_notices(&mut self) -> Result<(), RaftError> {
        if self.fenced {
            return Err(RaftError::Fenced);
        }
        if self.role != Role::Follower
            || self.has_pending_dependency()
            || self.request_sequence != 0
            || self.election_reset != 0
            || self.replication_permit.is_some()
            || self.authority_request.is_some()
            || !self.notice_membership()
        {
            return Err(RaftError::InvalidRecovery);
        }
        self.commit_notices = true;
        Ok(())
    }
    fn notice_membership(&self) -> bool {
        // Static checkpointing deliberately retains None membership; this
        // summary therefore survives ordinary prefix compaction.
        self.durable.summary().is_plain()
            && self.membership().id() == self.durable.bootstrap.configuration
            && self.local_voter()
    }
    pub(super) fn commit_notice_or_append(&mut self, peer: NodeId) -> Result<Effect, RaftError> {
        let index = self.durable.commit_index;
        if !self.commit_notices
            || self.fenced
            || self.role != Role::Leader
            || !self.notice_membership()
            || !self.progress_sends_enabled()
            || self.has_pending_dependency()
            || self.replication_permit.is_some()
            || index == 0
            || index != self.durable.last_index()
            || self.durable.term_at(index) != Some(self.durable.hard_state.term)
            || !self.durable.bootstrap.voter_stores.contains_key(&peer)
            || peer == self.node
            || self
                .progress
                .get(&peer)
                .is_none_or(|prefix| *prefix < index)
            || self.next_index.get(&peer).copied() != index.checked_add(1)
            || self.requests.contains_key(&peer)
            || self.admitted_ranges.contains_key(&peer)
            || self.empty_append_aliases.contains_key(&peer)
        {
            return self.append_for(peer);
        }
        let context = self.context()?;
        let log_term = self.durable.hard_state.term;
        let message = self.message(peer, context, Rpc::CommitNotice { index, log_term });
        self.commit_notice_emissions.insert(
            peer,
            CommitNoticeEmission {
                context,
                configuration: message.configuration,
                term: message.term,
                index,
                log_term,
                recipient: self.durable.bootstrap.voter_stores[&peer],
            },
        );
        Ok(Effect::Send(message))
    }
    #[cfg(feature = "native")]
    pub(super) fn original_commit_notice(&self, m: &Message) -> bool {
        let Rpc::CommitNotice { index, log_term } = m.rpc else {
            return false;
        };
        self.commit_notices
            && !self.fenced
            && self.role == Role::Leader
            && self.notice_membership()
            && self.progress_sends_enabled()
            && self.replication_permit.is_none()
            && self
                .commit_notice_emissions
                .get(&m.to)
                .is_some_and(|stamp| {
                    stamp.context == m.context
                        && stamp.configuration == m.configuration
                        && stamp.term == m.term
                        && stamp.index == index
                        && stamp.log_term == log_term
                        && self.membership().voter_store(m.to) == Some(stamp.recipient)
                })
    }
    pub(super) fn validate_commit_notice(&self, m: &Message) -> Result<(), RaftError> {
        if !self.commit_notices
            || !self.notice_membership()
            || self.replication_permit.is_some()
            || m.to != self.node
            || m.from == self.node
            || m.group != self.durable.bootstrap.group
            || m.configuration != self.membership().id()
            || self.membership().voter_store(m.from) != Some(m.sender.identity)
            || m.context.origin != m.sender
        {
            return Err(RaftError::WrongIdentity);
        }
        let Rpc::CommitNotice { index, log_term } = m.rpc else {
            return Err(RaftError::InvalidMessage);
        };
        if m.term == 0
            || m.context.sequence == 0
            || index == 0
            || log_term == 0
            || log_term > m.term
        {
            return Err(RaftError::InvalidMessage);
        }
        Ok(())
    }
    pub(super) fn receive_commit_notice_into(
        &mut self,
        m: &Message,
        hard: HardState,
        index: u64,
        log_term: u64,
        out: &mut VecDeque<Effect>,
    ) -> Result<(), RaftError> {
        if m.term < self.durable.hard_state.term {
            return Ok(());
        }
        self.role = Role::Follower;
        self.clear_reads();
        self.vote_context = None;
        self.clear_replication_requests();
        self.repair_requests.clear();
        let commit = if self.durable.term_at(index) == Some(log_term) {
            self.durable.commit_index.max(index)
        } else {
            self.durable.commit_index
        };
        if hard != self.durable.hard_state || commit != self.durable.commit_index {
            self.persist_into(hard, commit, None, After::Reply, None, out)?;
        }
        Ok(())
    }
}

#[cfg(test)]
mod tests;
