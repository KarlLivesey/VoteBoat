// SPDX-License-Identifier: RPL-1.5
// Copyright (c) 2026 Karl Livesey
//! Readiness, witness queries and one-way originals keep their own contracts.
use super::*;
mod one_way;

impl Raft {
    pub(super) fn special_window(&self, m: &Message) -> Option<(PeerIdentity, Boundary)> {
        match &m.rpc {
            Rpc::LearnerReadinessRequest(request)
                if self.learner_readiness == Some(**request)
                    && request.group == m.group
                    && request.configuration == m.configuration
                    && request.leader == m.from
                    && request.term == m.term
                    && request.context == m.context
                    && request.learner.node == m.to
                    && self.role == Role::Leader
                    && self.local_voter()
                    && self.membership().id() == m.configuration =>
            {
                Some((request.learner, Boundary::Readiness(**request)))
            }
            Rpc::AuthorityRequest {
                candidate,
                configuration,
            } => {
                let ReplicationAuthorizationStatus::Pending {
                    witness,
                    candidate: expected,
                    configuration: target,
                    base,
                    context,
                } = self.replication_authorization_status()
                else {
                    return None;
                };
                (witness.node == m.to
                    && *candidate == expected
                    && *configuration == target
                    && base == m.configuration
                    && base == self.membership().id()
                    && context == m.context)
                    .then_some((
                        witness,
                        Boundary::Authority {
                            candidate: *candidate,
                            configuration: *configuration,
                        },
                    ))
            }
            _ => self.repair_window(m),
        }
    }
}
impl RequestWindow {
    pub(super) fn current_boundary(&self, core: &Raft) -> bool {
        match self.boundary {
            Boundary::Vote { index, term } => {
                core.role == Role::Candidate
                    && core.local_voter()
                    && core.membership().id() == self.configuration
                    && core.membership().voter_store(self.recipient.node)
                        == Some(self.recipient.store)
                    && core.vote_context == Some(self.context)
                    && !core.votes.contains(&self.recipient.node)
                    && core.durable.hard_state.voted_for == Some(core.node)
                    && core.durable.last_index() == index
                    && core.durable.last_term() == term
            }
            Boundary::Read => {
                core.role == Role::Leader
                    && core.local_voter()
                    && core.membership().id() == self.configuration
                    && core.membership().voter_store(self.recipient.node)
                        == Some(self.recipient.store)
                    && core.read.as_ref().is_some_and(|r| {
                        r.barrier.context == self.context
                            && !r.acknowledgements.contains(&self.recipient.node)
                    })
            }
            Boundary::Readiness(request) => {
                core.role == Role::Leader
                    && core.local_voter()
                    && core.membership().id() == self.configuration
                    && core.learner_readiness == Some(request)
            }
            Boundary::Authority {
                candidate,
                configuration,
            } => matches!(core.replication_authorization_status(),
                ReplicationAuthorizationStatus::Pending { witness, candidate: found, configuration: target, base, context }
                    if witness == self.recipient && found == candidate && target == configuration
                        && base == self.configuration && base == core.membership().id() && context == self.context),
            _ => self.current_replication(core),
        }
    }
}
