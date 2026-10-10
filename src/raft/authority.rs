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
//! Direct, bounded witness authorization for promoted replica catch-up.
use super::*;
use crate::membership::ConfigurationChange;

/// Local volatile observation, never a portable authorization or durable receipt.
/// Queued host controls have not taken effect until the owner executes them.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum ReplicationAuthorizationStatus {
    None,
    Pending {
        witness: PeerIdentity,
        candidate: PeerIdentity,
        configuration: ConfigurationId,
        base: ConfigurationId,
        context: RequestContext,
    },
    Granted {
        candidate: PeerIdentity,
        configuration: ConfigurationId,
        base: ConfigurationId,
    },
}

pub(super) struct PendingAuthority {
    witness: PeerIdentity,
    candidate: PeerIdentity,
    configuration: ConfigurationId,
    base: ConfigurationId,
    context: RequestContext,
}
pub(super) struct ReplicationPermit {
    candidate: PeerIdentity,
    configuration: ConfigurationId,
    base: ConfigurationId,
}
impl Raft {
    /// Observe the single request/permit for host-driven timeout and retry.
    /// Pending takes precedence when a new query coexists with an older permit;
    /// cancel first when the host intends to revoke that earlier authorization.
    pub fn replication_authorization_status(&self) -> ReplicationAuthorizationStatus {
        if let Some(pending) = &self.authority_request {
            return ReplicationAuthorizationStatus::Pending {
                witness: pending.witness,
                candidate: pending.candidate,
                configuration: pending.configuration,
                base: pending.base,
                context: pending.context,
            };
        }
        if let Some(permit) = &self.replication_permit {
            return ReplicationAuthorizationStatus::Granted {
                candidate: permit.candidate,
                configuration: permit.configuration,
                base: permit.base,
            };
        }
        ReplicationAuthorizationStatus::None
    }

    pub(super) fn connection_permit(&self) -> Option<PeerIdentity> {
        self.replication_permit
            .as_ref()
            .filter(|p| p.base == self.membership().id())
            .map(|p| p.candidate)
    }
    pub(super) fn clear_replication_authority(&mut self) {
        self.authority_request = None;
        self.replication_permit = None;
    }
    pub(super) fn authorize_replication(
        &mut self,
        witness: PeerIdentity,
        candidate: PeerIdentity,
        configuration: ConfigurationId,
    ) -> Result<Vec<Effect>, RaftError> {
        if self.authority_request.is_some() {
            return Err(RaftError::Busy);
        }
        if witness.node == self.node
            || candidate.node == self.node
            || candidate.node == witness.node
            || self.membership().voter_store(witness.node) != Some(witness.store)
            || self.membership().is_voter(candidate.node)
            || configuration <= self.membership().id()
        {
            return Err(RaftError::WrongIdentity);
        }
        let context = self.context()?;
        let base = self.membership().id();
        let mut message = self.scoped_message(
            base,
            witness.node,
            context,
            Rpc::AuthorityRequest {
                candidate,
                configuration,
            },
        );
        // Control envelopes use a positive wire term, without granting any
        // authority to change hard state. A term-zero learner can query too.
        message.term = message.term.max(1);
        self.authority_request = Some(PendingAuthority {
            witness,
            candidate,
            configuration,
            base,
            context,
        });
        Ok(vec![Effect::Send(message)])
    }
    pub(super) fn permitted_replication(&self, message: &Message) -> bool {
        self.replication_permit.as_ref().is_some_and(|permit| {
            permit.base == self.membership().id()
                && permit.configuration == message.configuration
                && permit.candidate.node == message.from
                && permit.candidate.store == message.sender.identity
                && matches!(message.rpc, Rpc::Append { .. } | Rpc::Snapshot { .. })
        })
    }
    // Reconstruct only retained, committed history. A compacted-away old view
    // is unavailable; a remote claim never recreates it.
    fn authority_base(&self, id: ConfigurationId) -> Option<Membership> {
        let floor = self.durable.membership_at(self.durable.base_index()).ok()?;
        if floor.id() == id {
            return Some(floor);
        }
        let entry = self
            .durable
            .entries
            .iter()
            .take_while(|entry| entry.index <= self.durable.commit_index)
            .find(|entry| match &entry.payload {
                EntryPayload::Configuration(record) => match &record.change {
                    ConfigurationChange::Learners(next) => next.id() == id,
                    ConfigurationChange::Joint { id: next, .. }
                    | ConfigurationChange::Final { id: next } => *next == id,
                },
                _ => false,
            })?;
        self.durable.membership_at(entry.index).ok()
    }
    pub(super) fn receive_authority(&mut self, message: Message) -> Result<Vec<Effect>, RaftError> {
        if message.group != self.durable.bootstrap.group
            || message.to != self.node
            || message.from == self.node
            || message.context.sequence == 0
            || message.term == 0
        {
            return Err(RaftError::WrongIdentity);
        }
        match message.rpc {
            Rpc::AuthorityRequest {
                candidate,
                configuration,
            } => self.receive_authority_request(&message, candidate, configuration),
            Rpc::AuthorityReply {
                candidate,
                configuration,
                committed_index,
                committed_term,
                granted,
            } => self.receive_authority_reply(
                &message,
                candidate,
                configuration,
                committed_index,
                committed_term,
                granted,
            ),
            _ => unreachable!(),
        }
    }
    fn receive_authority_request(
        &mut self,
        message: &Message,
        candidate: PeerIdentity,
        configuration: ConfigurationId,
    ) -> Result<Vec<Effect>, RaftError> {
        if message.context.origin != message.sender
            || candidate.node == message.from
            || candidate.node == self.node
            || configuration <= message.configuration
        {
            return Err(RaftError::WrongIdentity);
        }
        let base = self
            .authority_base(message.configuration)
            .ok_or(RaftError::WrongIdentity)?;
        if base.replica_store(message.from) != Some(message.sender.identity)
            || base.voter_store(self.node) != Some(self.binding.identity)
        {
            return Err(RaftError::WrongIdentity);
        }
        let committed = self
            .durable
            .membership_at(self.durable.commit_index)
            .map_err(|_| RaftError::InvalidRecovery)?;
        let granted = committed.voter_store(candidate.node) == Some(candidate.store)
            && (configuration == committed.id()
                || committed
                    .joint()
                    .is_some_and(|joint| configuration == joint.next.id()))
            && self.durable.commit_index > 0;
        let mut reply = self.reply(
            message,
            Rpc::AuthorityReply {
                candidate,
                configuration,
                committed_index: if granted {
                    self.durable.commit_index
                } else {
                    0
                },
                committed_term: if granted {
                    self.durable
                        .term_at(self.durable.commit_index)
                        .ok_or(RaftError::InvalidRecovery)?
                } else {
                    0
                },
                granted,
            },
        );
        reply.term = reply.term.max(1);
        Ok(vec![Effect::Send(reply)])
    }
    fn receive_authority_reply(
        &mut self,
        message: &Message,
        candidate: PeerIdentity,
        configuration: ConfigurationId,
        committed_index: u64,
        committed_term: u64,
        granted: bool,
    ) -> Result<Vec<Effect>, RaftError> {
        let pending = self
            .authority_request
            .as_ref()
            .ok_or(RaftError::WrongIdentity)?;
        if pending.base != self.membership().id()
            || message.configuration != pending.base
            || pending.witness.node != message.from
            || pending.witness.store != message.sender.identity
            || self.membership().voter_store(message.from) != Some(message.sender.identity)
            || pending.context != message.context
            || message.context.origin != self.binding
            || pending.candidate != candidate
            || pending.configuration != configuration
        {
            return Err(RaftError::WrongIdentity);
        }
        if (granted
            && (committed_index <= self.membership().last_configuration_index()
                || committed_term == 0
                || committed_term > message.term))
            || (!granted && (committed_index != 0 || committed_term != 0))
        {
            return Err(RaftError::InvalidMessage);
        }
        self.replication_permit = granted.then_some(ReplicationPermit {
            candidate,
            configuration,
            base: pending.base,
        });
        self.authority_request = None;
        Ok(Vec::new())
    }
}
