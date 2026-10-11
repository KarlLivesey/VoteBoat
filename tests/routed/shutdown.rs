// SPDX-License-Identifier: RPL-1.5
// Copyright (c) 2026 Karl Livesey
//! Fixture-only terminal cleanup after all application/owned work has drained.
use super::*;
use voteboat::{
    connect::ConnectUsage,
    secure::{SessionBinding, SessionState},
    snapshot_worker::SnapshotWorker,
    transport::{PeerDrainStatus, TransportState, TransportUsage},
    worker::PersistenceWorker,
};
#[path = "shutdown/contract.rs"]
mod contract;
#[cfg(feature = "quic")]
#[path = "shutdown/long_rtt.rs"]
mod long_rtt;

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
struct ProtocolClose {
    binding: SessionBinding,
    deadline: MonoTime,
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
type Remaining = BTreeMap<(usize, NodeId), ProtocolClose>;
struct ProtocolWindow {
    original: Remaining,
    end: MonoTime,
}
impl ProtocolWindow {
    fn pin(original: Remaining, now: MonoTime) -> Option<Self> {
        if original.is_empty() || original.values().any(|p| p.deadline <= now) {
            return None;
        }
        let end = original.values().map(|p| p.deadline).max()?;
        Some(Self { original, end })
    }
    fn retains(&self, remaining: &Remaining, polled_at: MonoTime) -> bool {
        !remaining.is_empty()
            && remaining.iter().all(|(key, close)| {
                self.original.get(key) == Some(close) && polled_at < close.deadline
            })
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
        && local.outbound.is_drained()
        && n.replica_usage() == ReplicaDriverUsage::default()
        && n.checkpoints().pending.is_empty()
        && local
            .snapshots
            .as_ref()
            .is_none_or(|s| s.router.is_drained() && s.worker.is_drained())
        && n.peers().is_some_and(|p| {
            p.usage() == PeerDriverUsage::default()
                && p.connector_usage() == ConnectUsage::default()
                && p.ingress().is_drained()
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
            }
        }
    }
    Some(pending)
}
#[track_caller]
pub(super) fn drive<A>(nodes: &mut [Node<A>], clock: &Instant, background: &mut impl FnMut())
where
    A: ProposalAdmission + BoundedReadableStateMachine + CheckpointStateMachine,
    A::Receipt: ApplicationReceipt,
{
    let cutoff = Instant::now() + Duration::from_secs(15);
    loop {
        poll_nodes(nodes, clock);
        background();
        if nodes.iter().all(|n| n.is_drained()) {
            return;
        }
        if Instant::now() >= cutoff {
            break;
        }
        std::thread::park_timeout(Duration::from_millis(1));
    }
    let now = MonoTime(clock.elapsed().as_millis() as u64);
    let window = remaining(nodes).and_then(|r| ProtocolWindow::pin(r, now));
    let Some(window) = window else {
        diagnose_shutdown(nodes, clock);
        panic!("native routed progress timed out before settled protocol cleanup");
    };
    assert!(clock
        .checked_add(Duration::from_millis(window.end.0))
        .is_some());
    eprintln!(
        "routed close retained original protocol deadlines at {now:?}: {:?}",
        window.original
    );
    loop {
        let polled_at = MonoTime(clock.elapsed().as_millis() as u64);
        poll_nodes(nodes, clock);
        background();
        if nodes.iter().all(|n| n.is_drained()) {
            return;
        }
        let retained = remaining(nodes).is_some_and(|r| window.retains(&r, polled_at));
        if !retained || polled_at >= window.end {
            diagnose_shutdown(nodes, clock);
            panic!("native routed original protocol close failed to drain");
        }
        std::thread::park_timeout(Duration::from_millis(1));
    }
}
