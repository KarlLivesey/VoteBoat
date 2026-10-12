// SPDX-License-Identifier: RPL-1.5
// Copyright (c) 2026 Karl Livesey
//! Trusted local snapshot inputs; original Raft outputs require real child permits.
use super::super::{
    aliases::Bridge,
    cluster::{Cluster, HISTORY},
    drive::claim_in,
    parent::Parent,
    persistence::{append, input, pump_providers, settle},
    setup, support,
};
use std::time::{Duration, Instant};
use voteboat::{
    application::CheckpointStateMachine,
    logical_voter::*,
    native::connect::NativePeerProtocol,
    raft::{Event, Rpc},
    snapshot::{Snapshot, SnapshotMetadata},
};
fn snapshot(parent: &Parent) -> Snapshot {
    let core = parent.owner.core(setup::mapping().parent).unwrap();
    let app = &parent.apps[&setup::mapping().parent];
    Snapshot {
        metadata: SnapshotMetadata {
            bootstrap: core.state().bootstrap.clone(),
            membership: core.state().checkpoint_membership(1).unwrap(),
            index: 1,
            term: core.state().term_at(1).unwrap(),
            application_schema: app.schema_version(),
        },
        application: app.checkpoint(4096).unwrap(),
    }
}
fn acknowledge(
    bridge: &mut Bridge<'_>,
    parent: &mut Parent,
    clock: Instant,
    snapshot: &Snapshot,
    sequence: u64,
) {
    let before = parent
        .owner
        .core(setup::mapping().parent)
        .unwrap()
        .state()
        .clone();
    input(
        parent,
        Rpc::Snapshot {
            snapshot: Box::new(snapshot.clone()),
        },
        sequence,
    );
    parent.poll();
    let charged = parent.driver.releases().usage();
    assert_eq!(charged.pending, 1);
    assert!(charged.capsules > 0);
    for _ in 0..16 {
        pump_providers(bridge, parent, clock, 0);
        parent.poll();
        assert_eq!(parent.driver.releases().usage(), charged);
        assert_eq!(
            parent.owner.core(setup::mapping().parent).unwrap().state(),
            &before
        );
        assert!(parent.take_sends().is_empty());
    }
    let messages = settle(bridge, parent, clock);
    assert_eq!(messages.len(), 1);
    assert_eq!(messages[0].rpc, Rpc::SnapshotAck { index: 1 });
    assert_eq!(messages[0].context.sequence, sequence);
    assert_eq!(
        parent.owner.core(setup::mapping().parent).unwrap().state(),
        &before
    );
}
fn history(protocol: NativePeerProtocol) {
    let _guard = HISTORY.lock().unwrap();
    let mut cluster = Cluster::new(protocol);
    let clock = cluster.clock;
    let mut bridge =
        LogicalChildBridge::new(&mut cluster.nodes, setup::mapping(), support::node(1)).unwrap();
    let image = claim_in(&mut bridge, clock, 9300, false);
    let mut parent = Parent::with_snapshots(&image);
    input(
        &mut parent,
        Rpc::Vote {
            last_index: 0,
            last_term: 0,
        },
        1,
    );
    settle(&mut bridge, &mut parent, clock);
    append(&mut parent, 1, 5);
    settle(&mut bridge, &mut parent, clock);
    let original = snapshot(&parent);
    append(&mut parent, 2, 3);
    settle(&mut bridge, &mut parent, clock);
    assert_eq!(
        parent
            .owner
            .core(setup::mapping().parent)
            .unwrap()
            .state()
            .base_index(),
        0
    );
    acknowledge(&mut bridge, &mut parent, clock, &original, 10);
    parent
        .owner
        .admit(setup::mapping().parent, Event::Checkpoint)
        .unwrap();
    let end = Instant::now() + Duration::from_secs(15);
    loop {
        pump_providers(&mut bridge, &mut parent, clock, 4);
        parent.poll();
        assert!(parent.take_sends().is_empty());
        if parent.is_drained() && bridge.is_drained() {
            break;
        }
        assert!(Instant::now() < end, "original checkpoint must settle");
    }
    assert_eq!(
        parent
            .owner
            .core(setup::mapping().parent)
            .unwrap()
            .state()
            .base_index(),
        2
    );
    acknowledge(&mut bridge, &mut parent, clock, &original, 11);
    bridge.close();
    assert!(bridge.is_drained());
    parent.close();
    drop(bridge);
    cluster.finish();
}
#[test]
fn original_duplicate_snapshot_ack_above_and_below_base_tcp() {
    history(NativePeerProtocol::TcpTls);
}
#[cfg(feature = "quic")]
#[test]
fn original_duplicate_snapshot_ack_above_and_below_base_quic() {
    history(NativePeerProtocol::Quic);
}
