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
    pub fn from_parts(
        image: &VerifiedLogicalImage,
        mut parts: LogicalNodeParts<S, T, E, A, U, C, V, B>,
        limits: LogicalNodeLimits,
        now: MonoTime,
    ) -> LogicalNodeConstruct<S, T, E, A, U, C, V, B> {
        let checked = (|| {
            Self::validate_parts(image, &parts)?;
            let configuration =
                ConfigurationRequests::new(parts.local.owner.identity(), limits.configuration)
                    .map_err(LogicalNodeError::Configuration)?;
            let replica = LogicalReplicaDriver::new(
                &parts.local.as_parts(),
                limits.replica,
                limits.releases,
                now,
            )
            .map_err(LogicalNodeError::Replica)?;
            let deferred = Self::missing_declared_peer(&parts.local, &parts.sessions)?;
            Ok((replica, configuration, deferred))
        })();
        let (replica, configuration, deferred) = match checked {
            Ok(r) => r,
            Err(reason) => {
                return Err(LogicalNodeRejected {
                    reason,
                    parts: Box::new(parts),
                })
            }
        };
        let sessions = parts
            .sessions
            .into_iter()
            .map(|(peer, session)| {
                (
                    peer,
                    NodeSession {
                        session,
                        incoming: None,
                        failed_completion: None,
                        shutdown_closure: None,
                    },
                )
            })
            .collect();
        Ok(Self {
            local: parts.local,
            persistence: parts.persistence,
            snapshots: parts.snapshots,
            replica,
            configuration,
            endpoint: image.endpoint_chain().clone(),
            group: image.mapping().parent,
            sessions,
            pending: VecDeque::new(),
            deferred,
            session_cursor: None,
            state: NodeState::Running,
            now,
            failure: None,
        })
    }
    fn validate_parts(
        image: &VerifiedLogicalImage,
        p: &LogicalNodeParts<S, T, E, A, U, C, V, B>,
    ) -> Result<(), LogicalNodeError> {
        let group = image.mapping().parent;
        let core = p
            .local
            .owner
            .core(group)
            .ok_or(LogicalNodeError::WrongBinding)?;
        if p.local
            .applications
            .get(&group)
            .ok_or(LogicalNodeError::WrongBinding)?
            .logical_activation_control_profile()
            .as_ref()
            != image.activation_control_profile()
        {
            return Err(LogicalNodeError::WrongBinding);
        }
        image
            .endpoint_chain()
            .current(p.local.owner.authorization(), core)
            .map_err(LogicalNodeError::Endpoint)?;
        if core.state() != &image.image().parent
            || p.local.owner.groups().count() != 1
            || p.persistence.scope() != (image.mapping(), image.representative())
            || p.persistence.binding() != p.local.persistence.binding()
            || p.persistence.binding() != p.local.owner.worker_binding()
            || p.local.outbound.binding().store != image.binding()
            || p.local.outbound.binding().node != image.mapping().voter
        {
            return Err(LogicalNodeError::WrongBinding);
        }
        if !p.persistence.is_drained() {
            return Err(LogicalNodeError::NotQuiescent);
        }
        match (&p.local.snapshots, &p.snapshots) {
            (Some(local), Some(port)) => {
                if port.scope()
                    != (
                        image.mapping(),
                        image.representative(),
                        p.persistence.binding(),
                    )
                    || port.binding() != local.worker.binding()
                {
                    return Err(LogicalNodeError::WrongBinding);
                }
                if !port.is_drained() {
                    return Err(LogicalNodeError::NotQuiescent);
                }
            }
            (None, None) if p.sessions.is_empty() && core.state().base_index() == 0 => {}
            _ => return Err(LogicalNodeError::Replica(ReplicaError::MissingSnapshots)),
        }
        if p.sessions.len() > p.local.outbound.limits().max_peers {
            return Err(LogicalNodeError::InvalidLimits);
        }
        let connections = core
            .connection_replicas(65536)
            .map_err(|e| LogicalNodeError::Owner(EffectOwnerError::Consensus(e)))?;
        for (peer, store) in connections {
            if peer == core.local_node() {
                continue;
            }
            let Some(session) = p.sessions.get(&peer) else {
                if !core.state().bootstrap.voter_stores.contains_key(&peer) {
                    continue;
                }
                return Err(LogicalNodeError::MissingPeers);
            };
            if session.node_binding().is_none_or(|b| b.peer.store != store) {
                return Err(LogicalNodeError::WrongBinding);
            }
        }
        for (&peer, session) in &p.sessions {
            let b = session
                .node_binding()
                .ok_or(LogicalNodeError::WrongBinding)?;
            if b.group != group
                || b.local != core.local_node()
                || b.peer.node != peer
                || b.authorization != p.local.owner.authorization()
                || b.outbound != p.local.outbound.binding()
                || core
                    .connection_replicas(65536)
                    .map_err(|e| LogicalNodeError::Owner(EffectOwnerError::Consensus(e)))?
                    .get(&peer)
                    != Some(&b.peer.store)
                || session.state() != TransportState::Open
                || !session.work_is_drained()
            {
                return Err(LogicalNodeError::WrongBinding);
            }
        }
        Ok(())
    }
    fn missing_declared_peer(
        local: &LogicalNodeLocal<S, T, E, A>,
        sessions: &BTreeMap<NodeId, LogicalPeerSession<U, C, V, B>>,
    ) -> Result<Option<(NodeId, StoreIdentity)>, LogicalNodeError> {
        let core = local
            .owner
            .groups()
            .next()
            .and_then(|g| local.owner.core(g))
            .ok_or(LogicalNodeError::WrongBinding)?;
        let mut deferred = None;
        for (peer, store) in core
            .connection_replicas(local.outbound.limits().max_peers.saturating_add(1))
            .map_err(|e| LogicalNodeError::Owner(EffectOwnerError::Consensus(e)))?
        {
            if peer == core.local_node() || sessions.contains_key(&peer) {
                continue;
            }
            if core.state().bootstrap.voter_stores.contains_key(&peer) || deferred.is_some() {
                return Err(LogicalNodeError::MissingPeers);
            }
            deferred = Some((peer, store));
        }
        Ok(deferred)
    }
}
