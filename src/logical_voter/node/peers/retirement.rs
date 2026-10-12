// SPDX-License-Identifier: RPL-1.5
// Copyright (c) 2026 Karl Livesey
//! Return obsolete settled sessions without manufacturing delivery outcomes.
use super::*;
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
    /// Return the same settled session only after current and rollback views no
    /// longer require its peer. The host remains responsible for provider close.
    pub fn retire_peer_session(
        &mut self,
        peer: NodeId,
    ) -> Result<LogicalPeerSession<U, C, V, B>, LogicalNodeError> {
        if self.state != NodeState::Running {
            return Err(LogicalNodeError::Closed);
        }
        let slot = self
            .sessions
            .get(&peer)
            .ok_or(LogicalNodeError::WrongBinding)?;
        let core = self
            .local
            .owner
            .core(self.group)
            .ok_or(LogicalNodeError::WrongBinding)?;
        if core
            .connection_replicas(self.local.outbound.limits().max_peers.saturating_add(1))
            .map_err(|e| LogicalNodeError::Owner(EffectOwnerError::Consensus(e)))?
            .contains_key(&peer)
            || self.local.owner.has_pending_peer_ingress(self.group, peer)
            || self.local.owner.has_pending_send(peer)
            || self.local.outbound.peer_usage(peer) != OutboundUsage::default()
            || self.pending.iter().any(|b| b.ticket().peer == peer)
            || slot.incoming.is_some()
            || slot.failed_completion.is_some()
            || slot.shutdown_closure.is_some()
            || !slot.session.work_is_drained()
        {
            return Err(LogicalNodeError::NotQuiescent);
        }
        let catalogs = self
            .sessions
            .iter()
            .filter(|(node, _)| **node != peer)
            .map(|(&node, slot)| {
                slot.session
                    .contracted_catalog(peer)
                    .map(|plan| (node, plan))
            })
            .collect::<Result<Vec<_>, _>>()
            .map_err(LogicalNodeError::Network)?;
        for (node, plan) in catalogs {
            self.sessions
                .get_mut(&node)
                .expect("checked unchanged catalog")
                .session
                .install_catalog(plan);
        }
        Ok(self
            .sessions
            .remove(&peer)
            .expect("checked owned session")
            .session)
    }
}
