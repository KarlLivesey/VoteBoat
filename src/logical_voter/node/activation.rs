// SPDX-License-Identifier: RPL-1.5
// Copyright (c) 2026 Karl Livesey
//! Execute-time sealed control and actual authenticated readiness bindings.
use super::*;
use crate::raft::{ConfigurationProposalError, Raft, RaftError, ReadinessRequirements};
impl<
        S: ReadyScheduler,
        T: TimerService,
        E: ElectionEntropy,
        A: ProposalAdmission + LogicalResponseCodec + CheckpointStateMachine,
        U: SecureSession,
        C: LogCodec + Clone,
        V: SnapshotCodec + Clone,
        B: WireCodec + Clone,
    > LogicalNode<S, T, E, A, U, C, V, B>
where
    A::Receipt: ApplicationReceipt,
{
    pub(super) fn current_peer_binding(&self, learner: NodeId) -> Option<StoreBinding> {
        let slot = self.sessions.get(&learner)?;
        let binding = slot.session.node_binding()?;
        let core = self.local.owner.core(self.group)?;
        (slot.session.state() == TransportState::Open
            && binding.authorization == self.local.owner.authorization()
            && binding.group == self.group
            && binding.peer.node == learner
            && core.membership().replica_store(learner) == Some(binding.logical.identity))
        .then_some(binding.logical)
    }
    /// Queue the original core request using the attached authenticated session.
    /// Admission is neither successful execution nor promotion readiness.
    pub fn request_learner_readiness_tracked(
        &mut self,
        group: GroupIdentity,
        learner: NodeId,
        requirements: ReadinessRequirements,
    ) -> Result<AdmissionTicket, LogicalNodeError> {
        if self.state != NodeState::Running {
            return Err(LogicalNodeError::Closed);
        }
        if group != self.group {
            return Err(LogicalNodeError::WrongBinding);
        }
        let source = self
            .current_peer_binding(learner)
            .ok_or(LogicalNodeError::MissingPeers)?;
        let version = self.sessions[&learner]
            .session
            .node_binding()
            .ok_or(LogicalNodeError::WrongBinding)?
            .wire_version;
        if version < 4 {
            return Err(LogicalNodeError::Owner(EffectOwnerError::Consensus(
                ConfigurationProposalError::UnsupportedWireVersion(version).into(),
            )));
        }
        self.local
            .owner
            .admit_tracked(
                group,
                Event::CheckLearnerReadiness {
                    learner: crate::secure::PeerIdentity {
                        node: learner,
                        store: source.identity,
                    },
                    session: source.session,
                    requirements: Box::new(requirements),
                },
            )
            .map_err(|r| LogicalNodeError::Owner(EffectOwnerError::Runtime(r.reason)))
    }
    /// Cancel only by admitting the original tracked cancellation event.
    pub fn cancel_learner_readiness_tracked(
        &mut self,
        group: GroupIdentity,
    ) -> Result<AdmissionTicket, LogicalNodeError> {
        if self.state != NodeState::Running {
            return Err(LogicalNodeError::Closed);
        }
        if group != self.group {
            return Err(LogicalNodeError::WrongBinding);
        }
        self.local
            .owner
            .admit_tracked(group, Event::CancelLearnerReadiness)
            .map_err(|r| LogicalNodeError::Owner(EffectOwnerError::Runtime(r.reason)))
    }
    pub(super) fn authorize_activation(
        sessions: &BTreeMap<NodeId, NodeSession<U, C, V, B>>,
        deferred: &Option<(NodeId, StoreIdentity)>,
        max_peers: usize,
        core: &Raft,
        event: &Event,
        authorization: Option<&LogicalActivationAuthorization>,
    ) -> Result<(), RaftError> {
        let Event::Configure(proposal) = event else {
            return Ok(());
        };
        match authorization {
            Some(auth) => auth.authorize(core, proposal).map_err(RaftError::from)?,
            None => configuration_auth_required(event)?,
        }
        let connections = core.event_connection_replicas(event, max_peers.saturating_add(1))?;
        let mut next_deferred = *deferred;
        for (node, store) in connections {
            if node == core.local_node() {
                continue;
            }
            if let Some(slot) = sessions.get(&node) {
                let b = slot
                    .session
                    .node_binding()
                    .ok_or(RaftError::WrongIdentity)?;
                if b.logical.identity != store {
                    return Err(RaftError::WrongIdentity);
                }
                if b.wire_version < 2 {
                    return Err(
                        ConfigurationProposalError::UnsupportedWireVersion(b.wire_version).into(),
                    );
                }
            } else if next_deferred == Some((node, store)) {
                // Existing declared connection metadata supplies no input authority.
            } else if authorization
                .is_some_and(|a| a.learner().node == node && a.learner().store == store)
                && next_deferred.is_none()
            {
                next_deferred = Some((node, store));
            } else {
                return Err(ConfigurationProposalError::MissingPeerTransport.into());
            }
        }
        for proof in &proposal.readiness {
            if !sessions
                .get(&proof.ready.request().learner.node)
                .is_some_and(|s| {
                    s.session.state() == TransportState::Open
                        && s.session.node_binding().is_some_and(|b| {
                            b.wire_version >= 4 && b.logical == proof.authenticated
                        })
                })
            {
                return Err(ConfigurationProposalError::AuthenticationRequired.into());
            }
        }
        Ok(())
    }
}
