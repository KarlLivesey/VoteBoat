// SPDX-License-Identifier: RPL-1.5
// Copyright (c) 2026 Karl Livesey
//! Mandatory child authorization must not consume an original ballot window.
use super::*;

/// Exact volatile response correlation. Durable ballot remains in GroupLog.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub(super) struct GrantedVoteReply {
    candidate: NodeId,
    configuration: ConfigurationId,
    term: u64,
    context: RequestContext,
}
impl GrantedVoteReply {
    fn from_message(message: &Message) -> Self {
        Self {
            candidate: message.to,
            configuration: message.configuration,
            term: message.term,
            context: message.context,
        }
    }
}

impl Raft {
    /// Remember only the original response actually emitted by this core.
    pub(super) fn remember_granted_vote(&mut self, effects: &[Effect]) -> bool {
        let grant = effects.iter().find_map(|effect| match effect {
            Effect::Send(message) if matches!(message.rpc, Rpc::Voted { granted: true }) => {
                Some(GrantedVoteReply::from_message(message))
            }
            _ => None,
        });
        if let Some(grant) = grant {
            self.granted_vote_reply = Some(grant);
            true
        } else {
            false
        }
    }
    /// Called only after the original authorized logical Send has transferred
    /// to its selected outbound owner. Acceptance is not peer receipt or a vote.
    pub(crate) fn note_logical_vote_transferred(
        &mut self,
        message: &Message,
    ) -> Result<(), RaftError> {
        if self.fenced
            || self.pending.is_some()
            || !self.campaigning_enabled
            || !self.local_voter()
            || message.group != self.durable.bootstrap.group
            || message.configuration != self.membership().id()
            || message.from != self.node
            || message.sender != self.binding
            || message.to == self.node
            || !self.membership().is_voter(message.to)
            || message.term != self.durable.hard_state.term
        {
            return Ok(());
        }
        let matches = match message.rpc {
            Rpc::Vote {
                last_index,
                last_term,
            } => {
                self.role == Role::Candidate
                    && self.durable.hard_state.voted_for == Some(self.node)
                    && self.vote_context == Some(message.context)
                    && last_index == self.durable.last_index()
                    && last_term == self.durable.last_term()
            }
            Rpc::Voted { granted: true } => {
                self.role == Role::Follower
                    && self.durable.hard_state.voted_for == Some(message.to)
                    && self.granted_vote_reply == Some(GrantedVoteReply::from_message(message))
                    && self.durable.ballot_origin.is_some_and(|ballot| {
                        ballot.configuration == message.configuration
                            && ballot.candidate_store == message.context.origin.identity
                            && self.membership().voter_store(message.to)
                                == Some(ballot.candidate_store)
                    })
            }
            _ => false,
        };
        if matches {
            self.reset_election()?;
        }
        Ok(())
    }
}

#[cfg(test)]
mod tests;
