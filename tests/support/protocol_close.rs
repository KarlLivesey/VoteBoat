// SPDX-License-Identifier: RPL-1.5
// Copyright (c) 2026 Karl Livesey
//! Test-only bounded wait for original, already-flushed protocol closure.
use std::{
    collections::{BTreeMap, BTreeSet},
    time::{Duration, Instant},
};
use voteboat::{
    application::{
        ApplicationReceipt, BoundedReadableStateMachine, CheckpointStateMachine, ProposalAdmission,
    },
    connect::ConnectUsage,
    identity::NodeId,
    native::{connect::NativeServiceConnector, node::NativeNode},
    outbound::OutboundQueue,
    runtime::{MonoTime, NodeState, PeerDriverUsage, ReplicaDriverUsage},
    secure::{SessionBinding, SessionState},
    snapshot_worker::SnapshotWorker,
    transport::{PeerDrainStatus, TransportState, TransportUsage},
    worker::PersistenceWorker,
};
#[path = "protocol_close/contract.rs"]
mod contract;
type Node<A> = NativeNode<A, NativeServiceConnector>;

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub(crate) struct ProtocolClose {
    pub(crate) binding: SessionBinding,
    pub(crate) deadline: MonoTime,
}
impl ProtocolClose {
    pub(crate) fn inspect(status: PeerDrainStatus) -> Option<Self> {
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
type Remaining = BTreeMap<(usize, NodeId), ProtocolClose>;
pub(crate) struct ProtocolWindow {
    original: Remaining,
    pub(crate) end: MonoTime,
    live: BTreeSet<(usize, NodeId)>,
    polled_at: MonoTime,
}
impl ProtocolWindow {
    pub(crate) fn pin(original: Remaining, now: MonoTime) -> Option<Self> {
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
    pub(crate) fn retains(&mut self, remaining: &Remaining, now: MonoTime) -> bool {
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
fn local_work_drained<A>(n: &Node<A>) -> bool
where
    A: ProposalAdmission + BoundedReadableStateMachine + CheckpointStateMachine,
    A::Receipt: ApplicationReceipt,
{
    let local = n.local();
    n.state() == NodeState::Draining
        && local.clients.is_drained()
        && local.reads.is_drained()
        && local.results.is_drained()
        && local.owner.is_drained()
        && local.persistence.is_drained()
        && local.persistence.usage() == Default::default()
        && local.outbound.is_drained()
        && n.replica_usage() == ReplicaDriverUsage::default()
        && n.checkpoints().pending.is_empty()
        && local.snapshots.as_ref().is_none_or(|s| {
            s.router.is_drained()
                && s.worker.is_drained()
                && s.router.usage() == Default::default()
                && s.worker.usage() == Default::default()
        })
        && n.peers().is_some_and(|p| {
            p.usage() == PeerDriverUsage::default()
                && p.connector_usage() == ConnectUsage::default()
                && p.ingress().is_drained()
                && p.connector_is_drained()
                && p.roster().usage().connecting == 0
        })
}
fn remaining<A>(nodes: &mut [Node<A>]) -> Option<Remaining>
where
    A: ProposalAdmission + BoundedReadableStateMachine + CheckpointStateMachine,
    A::Receipt: ApplicationReceipt,
{
    let mut pending = BTreeMap::new();
    for (index, n) in nodes.iter_mut().enumerate() {
        if n.is_drained() {
            continue;
        }
        if !local_work_drained(n) {
            return None;
        }
        let peers = n.peers()?.roster().tracked_peers().collect::<Vec<_>>();
        let mut count = 0;
        for peer in peers {
            let status = n.peer_drain_status(peer)?;
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
                pending.insert((index, peer), ProtocolClose::inspect(status)?);
                count += 1;
            }
        }
        if n.peers()?.roster().usage().connections != count {
            return None;
        }
    }
    Some(pending)
}
#[track_caller]
pub(crate) fn drive<A>(
    nodes: &mut [Node<A>],
    clock: &Instant,
    mut poll: impl FnMut(&mut [Node<A>], &Instant),
    mut diagnose: impl FnMut(&mut [Node<A>], &Instant),
) where
    A: ProposalAdmission + BoundedReadableStateMachine + CheckpointStateMachine,
    A::Receipt: ApplicationReceipt,
{
    let cutoff = Instant::now() + Duration::from_secs(15);
    loop {
        poll(nodes, clock);
        if nodes.iter().all(|n| n.is_drained()) {
            return;
        }
        if Instant::now() >= cutoff {
            break;
        }
        std::thread::park_timeout(Duration::from_millis(1));
    }
    let now = MonoTime(clock.elapsed().as_millis().try_into().unwrap());
    let Some(mut window) = remaining(nodes).and_then(|r| ProtocolWindow::pin(r, now)) else {
        fail(nodes, clock, &mut diagnose, "not settled protocol cleanup");
    };
    eprintln!(
        "close retained original protocol deadlines at {now:?}: {:?}",
        window.original
    );
    loop {
        let polled_at = MonoTime(clock.elapsed().as_millis().try_into().unwrap());
        poll(nodes, clock);
        if nodes.iter().all(|n| n.is_drained()) {
            return;
        }
        if !remaining(nodes).is_some_and(|r| window.retains(&r, polled_at)) {
            fail(
                nodes,
                clock,
                &mut diagnose,
                "original protocol close did not drain",
            );
        }
        std::thread::park_timeout(Duration::from_millis(1));
    }
}
#[track_caller]
fn fail<A>(
    nodes: &mut [Node<A>],
    clock: &Instant,
    diagnose: &mut impl FnMut(&mut [Node<A>], &Instant),
    reason: &str,
) -> !
where
    A: ProposalAdmission + BoundedReadableStateMachine + CheckpointStateMachine,
    A::Receipt: ApplicationReceipt,
{
    for (index, node) in nodes.iter_mut().enumerate() {
        let peers = node
            .peers()
            .map(|p| p.roster().tracked_peers().collect::<Vec<_>>())
            .unwrap_or_default();
        for peer in peers {
            eprintln!(
                "close original node={index} peer={peer:?} status={:?}",
                node.peer_drain_status(peer)
            );
        }
    }
    diagnose(nodes, clock);
    panic!("native protocol cleanup timed out: {reason}");
}
