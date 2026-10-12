// SPDX-License-Identifier: RPL-1.5
// Copyright (c) 2026 Karl Livesey
//! One missing declared member retains original proof-bearing batches and credits.
use super::*;
mod retirement;
pub struct LogicalPeerAttachRejected<U: SecureSession, C, V, B> {
    pub reason: LogicalNodeError,
    pub session: LogicalPeerSession<U, C, V, B>,
}
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
    pub(super) fn refresh_deferred(&mut self) -> Result<(), LogicalNodeError> {
        let core = self
            .local
            .owner
            .core(self.group)
            .ok_or(LogicalNodeError::WrongBinding)?;
        let connections = core
            .connection_replicas(self.local.outbound.limits().max_peers.saturating_add(1))
            .map_err(|e| LogicalNodeError::Owner(EffectOwnerError::Consensus(e)))?;
        for (peer, store) in connections {
            if peer == core.local_node() || self.sessions.contains_key(&peer) {
                continue;
            }
            if core.state().bootstrap.voter_stores.contains_key(&peer)
                || self.deferred.is_some_and(|old| old != (peer, store))
            {
                return Err(LogicalNodeError::MissingPeers);
            }
            self.deferred = Some((peer, store));
        }
        Ok(())
    }
    /// Borrow exact deferred original batches; this supplies no delivery proof.
    pub fn inspect_deferred(&self) -> impl Iterator<Item = &LogicalNetworkBatch> {
        self.pending.iter().filter(|b| {
            self.deferred
                .is_some_and(|(node, _)| b.ticket().peer == node)
        })
    }
    /// Attach an actually authenticated session built from a fresh verified target
    /// descriptor. Rejection returns that whole session and changes no queued work.
    pub fn attach_peer_session(
        &mut self,
        session: LogicalPeerSession<U, C, V, B>,
    ) -> Result<(), Box<LogicalPeerAttachRejected<U, C, V, B>>> {
        let checked = self.check_attachment(&session);
        let peer = match checked {
            Ok(peer) => peer,
            Err(reason) => return Err(Box::new(LogicalPeerAttachRejected { reason, session })),
        };
        let catalogs = self
            .sessions
            .iter()
            .map(|(&node, slot)| {
                slot.session
                    .expanded_catalog(&session)
                    .map(|plan| (node, plan))
            })
            .collect::<Result<Vec<_>, _>>();
        let catalogs = match catalogs {
            Ok(plans) => plans,
            Err(reason) => {
                return Err(Box::new(LogicalPeerAttachRejected {
                    reason: LogicalNodeError::Network(reason),
                    session,
                }))
            }
        };
        for (node, plan) in catalogs {
            self.sessions
                .get_mut(&node)
                .expect("checked unchanged catalog")
                .session
                .install_catalog(plan);
        }
        self.sessions.insert(
            peer,
            NodeSession {
                session,
                incoming: None,
                failed_completion: None,
                shutdown_closure: None,
            },
        );
        self.deferred = None;
        Ok(())
    }
    fn check_attachment(
        &self,
        session: &LogicalPeerSession<U, C, V, B>,
    ) -> Result<NodeId, LogicalNodeError> {
        if self.state != NodeState::Running {
            return Err(LogicalNodeError::Closed);
        }
        let b = session
            .node_binding()
            .ok_or(LogicalNodeError::WrongBinding)?;
        let core = self
            .local
            .owner
            .core(self.group)
            .ok_or(LogicalNodeError::WrongBinding)?;
        let committed = core
            .state()
            .membership_at(core.state().commit_index)
            .map_err(|_| LogicalNodeError::WrongBinding)?;
        if b.group != self.group
            || b.local != core.local_node()
            || b.authorization != self.local.owner.authorization()
            || b.outbound != self.local.outbound.binding()
            || b.logical.identity != b.peer.store
            || self.deferred != Some((b.peer.node, b.peer.store))
            || self.sessions.contains_key(&b.peer.node)
            || self.sessions.len() >= self.local.outbound.limits().max_peers
            || core.membership().replica_store(b.peer.node) != Some(b.peer.store)
            || committed.replica_store(b.peer.node) != Some(b.peer.store)
            || core
                .connection_replicas(65536)
                .map_err(|e| LogicalNodeError::Owner(EffectOwnerError::Consensus(e)))?
                .get(&b.peer.node)
                != Some(&b.peer.store)
            || session.state() != TransportState::Open
            || !session.work_is_drained()
            || !session.compatible_limits(self.local.outbound.network_limits())
        {
            return Err(LogicalNodeError::WrongBinding);
        }
        Ok(b.peer.node)
    }
}
