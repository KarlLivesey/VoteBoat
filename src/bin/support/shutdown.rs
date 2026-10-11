// SPDX-License-Identifier: RPL-1.5
// Copyright (c) 2026 Karl Livesey
//! Keep the ordinary work cutoff while respecting an original protocol close.
use super::{Failure, Service};
use std::{
    collections::{BTreeMap, BTreeSet},
    time::{Duration, Instant},
};
use voteboat::{
    connect::ConnectUsage,
    identity::NodeId,
    outbound::OutboundQueue,
    runtime::{MonoTime, NodeState, PeerDriverUsage, ReplicaDriverUsage},
    secure::{SessionBinding, SessionState},
    snapshot_worker::SnapshotWorker,
    transport::{PeerDrainStatus, TransportState, TransportUsage},
    worker::PersistenceWorker,
};

#[cfg(test)]
#[path = "shutdown/contract.rs"]
mod contract;

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub(super) struct ProtocolClose {
    pub(super) binding: SessionBinding,
    pub(super) deadline: MonoTime,
}
impl ProtocolClose {
    fn inspect(status: PeerDrainStatus) -> Option<Self> {
        if !status.closed
            || !status.authorized
            || status.fenced
            || status.retiring
            || status.attempt.is_some()
            || status.accepted.is_some()
        {
            return None;
        }
        let transport = status.transport?;
        let session = transport.session?;
        if status.binding != Some(transport.binding)
            || status.peer.node != transport.binding.peer.node
            || status.peer.store != transport.binding.peer.store.identity
            || transport.state != TransportState::Draining
            || transport.usage != TransportUsage::default()
            || transport.received.is_some()
            || transport.close_started != Some(true)
            || session.state != SessionState::Closing
            || !session.flushed
            || session.close_started != Some(true)
            || session.protocol_drained != Some(false)
        {
            return None;
        }
        Some(Self {
            binding: transport.binding,
            deadline: session.next_deadline?,
        })
    }
}
type Remaining = BTreeMap<NodeId, ProtocolClose>;
pub(super) struct ProtocolWindow {
    pub(super) original: Remaining,
    pub(super) end: MonoTime,
    live: BTreeSet<NodeId>,
    polled_at: MonoTime,
}
impl ProtocolWindow {
    fn pin(original: Remaining, now: MonoTime) -> Option<Self> {
        if original.is_empty() || original.values().any(|p| p.deadline <= now) {
            return None;
        }
        let end = original.values().map(|p| p.deadline).max()?;
        Instant::now().checked_add(Duration::from_millis(end.0.checked_sub(now.0)?))?;
        let live = original.keys().copied().collect();
        Some(Self {
            original,
            end,
            live,
            polled_at: now,
        })
    }
    fn retains(&mut self, remaining: &Remaining, now: MonoTime) -> bool {
        let valid = now >= self.polled_at
            && now < self.end
            && !remaining.is_empty()
            && remaining.iter().all(|(key, close)| {
                self.live.contains(key)
                    && self.original.get(key) == Some(close)
                    && now < close.deadline
            });
        if valid {
            self.live.retain(|key| remaining.contains_key(key));
            self.polled_at = now;
        }
        valid
    }
}
fn local_work_drained(service: &Service) -> bool {
    let local = service.local();
    service.state() == NodeState::Draining
        && local.clients.is_drained()
        && local.reads.is_drained()
        && local.results.is_drained()
        && local.owner.is_drained()
        && local.persistence.is_drained()
        && local.persistence.usage() == Default::default()
        && local.outbound.is_drained()
        && service.replica_usage() == ReplicaDriverUsage::default()
        && service.checkpoints().pending.is_empty()
        && local.snapshots.as_ref().is_none_or(|s| {
            s.router.is_drained()
                && s.worker.is_drained()
                && s.router.usage() == Default::default()
                && s.worker.usage() == Default::default()
        })
        && service.peers().is_some_and(|p| {
            p.usage() == PeerDriverUsage::default()
                && p.connector_usage() == ConnectUsage::default()
                && p.connector_is_drained()
                && p.ingress().is_drained()
                && p.roster().usage().connecting == 0
        })
}
fn remaining(service: &mut Service) -> Option<Remaining> {
    if !local_work_drained(service) {
        return None;
    }
    let peers = service
        .peers()?
        .roster()
        .tracked_peers()
        .collect::<Vec<_>>();
    let mut pending = BTreeMap::new();
    for peer in peers {
        let status = service.peer_drain_status(peer)?;
        if status.transport.is_none() {
            if status.binding.is_some()
                || status.attempt.is_some()
                || status.accepted.is_some()
                || status.retiring
                || !status.closed
                || status.fenced
            {
                return None;
            }
        } else {
            pending.insert(peer, ProtocolClose::inspect(status)?);
        }
    }
    (service.peers()?.roster().usage().connections == pending.len()).then_some(pending)
}

#[derive(Default)]
pub(super) struct Shutdown {
    pub(super) started: Option<Instant>,
    pub(super) protocol: Option<ProtocolWindow>,
}
impl Shutdown {
    pub(super) fn advance(
        &mut self,
        service: &mut Service,
        requested: bool,
        now: MonoTime,
    ) -> Result<(), Failure> {
        if requested && self.started.is_none() {
            service.begin_shutdown();
            self.started = Some(Instant::now());
        }
        if service.is_drained()
            || !self
                .started
                .is_some_and(|t| t.elapsed() > Duration::from_secs(10))
        {
            return Ok(());
        }
        let pending =
            remaining(service).ok_or("shutdown timed out; recover durable state on restart")?;
        if let Some(window) = &mut self.protocol {
            if window.retains(&pending, now) {
                return Ok(());
            }
        } else if let Some(window) = ProtocolWindow::pin(pending, now) {
            println!(
                "shutdown waiting_for_peer_close_ms={}",
                window.end.0 - now.0
            );
            self.protocol = Some(window);
            return Ok(());
        }
        Err("shutdown timed out; recover durable state on restart".into())
    }
}
