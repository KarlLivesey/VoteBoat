// SPDX-License-Identifier: RPL-1.5
// Copyright (c) 2026 Karl Livesey
use super::*;

/// Ask the current leader to catch up one exact stable voter and invite it to
/// campaign normally. The caller must select a compatible wire capability.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub struct LeadershipTransferRequest {
    pub operation: OperationId,
    pub target: PeerIdentity,
    pub configuration: ConfigurationId,
}
/// Volatile, term-bound attempt. Signal-sent is not election or durable success.
/// The host owns deadlines, durable administrative records and result queries.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub struct LeadershipTransfer {
    pub request: LeadershipTransferRequest,
    pub context: RequestContext,
    pub term: u64,
    pub index: u64,
    pub signal_sent: bool,
}
impl Raft {
    /// Current volatile attempt, if this core still leads its original term
    /// and configuration. Absence is not evidence of successful transfer.
    pub fn leadership_transfer(&self) -> Option<LeadershipTransfer> {
        self.transfer.filter(|t| {
            self.role == Role::Leader
                && t.term == self.durable.hard_state.term
                && t.request.configuration == self.membership().id()
        })
    }
    pub(super) fn refresh_transfer(&mut self) {
        self.transfer = self.leadership_transfer();
    }
    pub(super) fn start_transfer(
        &mut self,
        request: LeadershipTransferRequest,
    ) -> Result<Vec<Effect>, RaftError> {
        if self.role != Role::Leader || !self.local_voter() {
            return Err(RaftError::NotLeader);
        }
        if let Some(current) = self.transfer {
            return if current.request == request {
                Ok(Vec::new())
            } else {
                Err(RaftError::Busy)
            };
        }
        if request.configuration != self.membership().id()
            || self.membership().joint().is_some()
            || self.membership().last_configuration_index() > self.durable.commit_index
            || request.target.node == self.node
            || self.membership().voter_store(request.target.node) != Some(request.target.store)
        {
            return Err(RaftError::WrongIdentity);
        }
        let context = self.context()?;
        let effects = self.broadcast()?;
        self.transfer = Some(LeadershipTransfer {
            request,
            context,
            term: self.durable.hard_state.term,
            index: self.durable.last_index(),
            signal_sent: false,
        });
        Ok(effects)
    }
    pub(super) fn cancel_transfer(
        &mut self,
        context: RequestContext,
    ) -> Result<Vec<Effect>, RaftError> {
        if self.transfer.is_none_or(|t| t.context != context) {
            return Err(RaftError::StaleTransfer);
        }
        self.transfer = None;
        Ok(Vec::new())
    }
    pub(super) fn drive_transfer(&mut self) -> Result<Vec<Effect>, RaftError> {
        self.refresh_transfer();
        let Some(t) = self.transfer else {
            return Ok(Vec::new());
        };
        if t.signal_sent
            || self.has_pending_dependency()
            || self.durable.commit_index < t.index
            || self.durable.last_index() != t.index
            || self.durable.term_at(t.index) != Some(t.term)
            || self
                .progress
                .get(&t.request.target.node)
                .copied()
                .unwrap_or(0)
                < t.index
        {
            return Ok(Vec::new());
        }
        self.transfer.as_mut().unwrap().signal_sent = true;
        Ok(vec![Effect::Send(self.message(
            t.request.target.node,
            t.context,
            Rpc::TimeoutNow {
                operation: t.request.operation,
                index: t.index,
                log_term: t.term,
            },
        ))])
    }
    pub(super) fn receive_timeout_now(
        &mut self,
        m: &Message,
        index: u64,
        log_term: u64,
    ) -> Result<Vec<Effect>, RaftError> {
        if m.term != self.durable.hard_state.term || self.role != Role::Follower {
            return Ok(Vec::new());
        }
        if !self.local_voter()
            || self.membership().joint().is_some()
            || self.membership().last_configuration_index() > self.durable.commit_index
            || self.leader_contact != Some((m.sender, m.from, m.term))
        {
            return Err(RaftError::WrongIdentity);
        }
        if index == 0
            || log_term != m.term
            || self.durable.last_index() != index
            || self.durable.term_at(index) != Some(log_term)
        {
            return Err(RaftError::InvalidMessage);
        }
        self.campaign()
    }
}
