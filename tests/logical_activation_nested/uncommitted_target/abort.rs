// SPDX-License-Identifier: RPL-1.5
// Copyright (c) 2026 Karl Livesey
//! Inert aborted owners stay owned while the actual native providers finish.
use super::*;
use crate::harness::{FixtureClock, HangGuard, SimClock};
use std::time::Duration;
use voteboat::{
    native::{node::NativeLocalParts, transport::NativeTransportFactory, wire::NativeWireCodec},
    outbound::OutboundQueue,
    runtime::{NodeError, NodeRecovery, PeerDriverBudget, PeerDriverProgress},
    snapshot_worker::{SnapshotWorkEvent, SnapshotWorker},
    worker::{PersistenceWorker, ReclaimEvent, WorkerEvent},
};
type Recovery = NodeRecovery<
    NativeLocalParts<LogicalApplication>,
    LogicalReceipt,
    crate::dials::Probed,
    NativeTransportFactory<NativeWireCodec>,
>;
pub(super) struct Domain {
    original: Vec<Recovery>,
    worker: Vec<WorkerEvent>,
    reclaims: Vec<ReclaimEvent>,
    snapshots: Vec<SnapshotWorkEvent>,
    network: Vec<PeerDriverProgress>,
}
pub(super) fn join(cluster: &mut Cluster) -> Domain {
    let mut domain = Domain {
        original: Vec::new(),
        worker: Vec::new(),
        reclaims: Vec::new(),
        snapshots: Vec::new(),
        network: Vec::new(),
    };
    for mut node in cluster.nodes.drain(..) {
        node.abort();
        let mut recovery = node
            .into_recovery()
            .unwrap_or_else(|_| panic!("actual original aborted target"));
        assert_eq!(recovery.reason, NodeError::Aborted);
        domain
            .network
            .extend(network(&mut recovery, &cluster.clock));
        workers(&mut recovery, &mut domain);
        // No core poll, fake success or discard_failed on this possibly nonfailed owner.
        domain.original.push(recovery);
    }
    cluster.rebind();
    domain
}
fn network(recovery: &mut Recovery, clock: &SimClock) -> Vec<PeerDriverProgress> {
    let mut terminal = Vec::new();
    let peers = recovery.peers.as_mut().unwrap();
    peers.close();
    // The original 10 s bound, in protocol time. Only dial work holds the
    // clock here: the drain reports every still-open peer on every poll, so
    // its output is not finite progress, and a closing QUIC connection needs
    // protocol time to finish.
    let end = harness::Wait::new(clock, 10_000);
    while !peers.is_drained() {
        let now = clock.round(Observed::default().dialing(peers.connector_usage().dialing));
        let progress = peers
            .drain(
                &mut recovery.local.outbound,
                now,
                PeerDriverBudget::default(),
            )
            .unwrap();
        if !progress.completions.is_empty()
            || !progress.peers.is_empty()
            || !progress.ingress.completed.is_empty()
        {
            terminal.push(progress);
            assert!(
                terminal.len() <= 4096,
                "bounded original native peer terminal observations"
            );
        }
        assert!(
            end.within(clock),
            "same original aborted target network credits drain"
        );
        std::thread::park_timeout(Duration::from_millis(1));
    }
    assert!(recovery.local.outbound.is_drained());
    let parts = recovery
        .peers
        .take()
        .unwrap()
        .into_parts()
        .unwrap_or_else(|_| panic!("original network really drained"));
    let mut dialer = parts
        .connector
        .into_dialer()
        .unwrap_or_else(|_| panic!("original connector drained"));
    // Joining the dial worker takes no protocol time: only the hang guard applies.
    while dialer.as_mut().is_some_and(|d| !d.try_finish().unwrap()) {
        assert!(end.within(clock), "original target dialer joins");
        std::thread::park_timeout(Duration::from_millis(1));
    }
    assert!(parts.roster.is_drained() && parts.ingress.is_drained());
    terminal
}
fn workers(recovery: &mut Recovery, domain: &mut Domain) {
    // Collecting terminal worker events and joining the worker threads takes
    // no protocol time, so this wait has only the real-time hang guard.
    let end = HangGuard::new();
    let mut wal = false;
    let mut snapshots = false;
    loop {
        if !wal {
            domain.worker.extend(recovery.local.persistence.poll(64));
            domain
                .reclaims
                .extend(recovery.local.persistence.poll_reclaims(64));
            wal = recovery.local.persistence.try_reclaim().unwrap().is_some();
        }
        if !snapshots {
            let worker = &mut recovery.local.snapshots.as_mut().unwrap().worker;
            domain.snapshots.extend(worker.poll(64));
            snapshots = worker.try_reclaim().unwrap().is_some();
        }
        if wal && snapshots {
            break;
        }
        end.check("actual aborted target worker terminal events and joins");
        std::thread::park_timeout(Duration::from_millis(1));
    }
    assert!(recovery.local.persistence.is_drained());
    assert!(recovery
        .local
        .snapshots
        .as_ref()
        .unwrap()
        .worker
        .is_drained());
}
impl Domain {
    pub(super) fn retire(self, fresh: &VerifiedLogicalImage) {
        assert_eq!(self.original.len(), 3);
        for owner in &self.original {
            assert_eq!(owner.reason, NodeError::Aborted);
            assert!(owner.local.owner.identity().store.session.get() > 0);
            assert!(owner.peers.is_none());
            assert!(owner.local.persistence.is_drained());
        }
        assert!(fresh.claim().first_index > 0);
        // All original inert owners/replica leases and terminal events retire only here.
        drop(self);
    }
}
