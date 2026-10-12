// SPDX-License-Identifier: RPL-1.5
// Copyright (c) 2026 Karl Livesey
//! Original parent checkpoint leases, real child publication and pin release.
use super::{
    aliases::Bridge,
    cluster::{Cluster, HISTORY},
    drive::claim_in,
    parent::Parent,
    persistence::{append, input, pump_providers, settle},
    setup, support,
};
use std::time::{Duration, Instant};
use voteboat::{
    application::*,
    log::*,
    logical_voter::*,
    native::{connect::NativePeerProtocol, startup::NativeOpenMode},
    raft::*,
    snapshot::*,
    snapshot_worker::*,
    worker::*,
};

fn state(parent: &Parent) -> &GroupLog {
    parent.owner.core(setup::mapping().parent).unwrap().state()
}
fn references(parent: &Parent) -> Vec<SnapshotRef> {
    parent
        .snapshots
        .as_ref()
        .unwrap()
        .port
        .retained_references()
        .collect()
}
fn until(
    bridge: &mut Bridge<'_>,
    parent: &mut Parent,
    clock: Instant,
    predicate: impl Fn(&Parent) -> bool,
    poll_parent: bool,
) {
    let end = Instant::now() + Duration::from_secs(15);
    while !predicate(parent) {
        pump_providers(bridge, parent, clock, 4);
        if poll_parent {
            parent.poll();
        }
        assert!(parent.take_sends().is_empty());
        assert!(Instant::now() < end, "actual checkpoint dependency timeout");
        std::thread::park_timeout(Duration::from_millis(1));
    }
}
fn withheld(
    bridge: &mut Bridge<'_>,
    parent: &mut Parent,
    clock: Instant,
    old: Option<SnapshotRef>,
    retained: &[SnapshotRef],
) {
    pump_providers(bridge, parent, clock, 0);
    let usage = parent.snapshots.as_ref().unwrap().worker.usage();
    assert_eq!(usage.requests, 1);
    let original = parent
        .snapshots
        .as_ref()
        .unwrap()
        .port
        .pending_request()
        .unwrap();
    for _ in 0..16 {
        pump_providers(bridge, parent, clock, 0);
        assert_eq!(state(parent).snapshot, old);
        assert_eq!(references(parent), retained);
        assert_eq!(parent.snapshots.as_ref().unwrap().worker.usage(), usage);
        assert_eq!(
            parent.snapshots.as_ref().unwrap().port.pending_request(),
            Some(original)
        );
        assert!(parent.take_sends().is_empty());
    }
}
fn checkpoint(bridge: &mut Bridge<'_>, parent: &mut Parent, clock: Instant) -> SnapshotRef {
    let old = state(parent).snapshot;
    let old_references = references(parent);
    parent
        .owner
        .admit(setup::mapping().parent, Event::Checkpoint)
        .unwrap();
    parent.poll();
    withheld(bridge, parent, clock, old, &old_references);
    until(
        bridge,
        parent,
        clock,
        |p| references(p).len() == old_references.len() + 1,
        false,
    );
    let published = *references(parent).last().unwrap();
    assert_eq!(state(parent).snapshot, old);
    assert_eq!(
        parent.snapshots.as_ref().unwrap().worker.usage().requests,
        1
    );
    assert_eq!(
        parent.snapshots.as_ref().unwrap().router.usage().requests,
        1
    );
    assert_eq!(parent.worker.usage().requests, 0);
    assert!(parent
        .snapshots
        .as_ref()
        .unwrap()
        .port
        .pending_request()
        .is_none());
    parent.poll();
    assert_eq!(parent.worker.usage().requests, 1);
    for _ in 0..16 {
        pump_providers(bridge, parent, clock, 0);
        assert_eq!(state(parent).snapshot, old);
        assert_eq!(parent.worker.usage().requests, 1);
        assert!(parent
            .owner
            .core(setup::mapping().parent)
            .unwrap()
            .has_pending_dependency());
    }
    until(
        bridge,
        parent,
        clock,
        |p| state(p).snapshot == Some(published),
        true,
    );
    assert!(references(parent).contains(&published));
    if let Some(old) = old {
        assert!(references(parent).contains(&old));
    }
    let retained = references(parent);
    withheld(bridge, parent, clock, Some(published), &retained);
    assert!(settle(bridge, parent, clock).is_empty());
    assert_eq!(references(parent), vec![published]);
    assert_eq!(state(parent).base_index(), published.index);
    assert!(state(parent).entries.is_empty());
    published
}
fn prepare(cluster: &mut Cluster) -> GroupLog {
    let clock = cluster.clock;
    let mut bridge =
        LogicalChildBridge::new(&mut cluster.nodes, setup::mapping(), support::node(1)).unwrap();
    let image = claim_in(&mut bridge, clock, 9101, false);
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
    let first = checkpoint(&mut bridge, &mut parent, clock);
    append(&mut parent, 2, -2);
    settle(&mut bridge, &mut parent, clock);
    let second = checkpoint(&mut bridge, &mut parent, clock);
    assert!(second.generation > first.generation);
    assert_eq!(second.index, 2);
    assert!(parent.snapshots.as_ref().unwrap().port.cache_bytes() > 0);
    bridge.close();
    assert!(bridge.is_drained());
    parent.close()
}
fn suffix(cluster: &mut Cluster, anchored: &GroupLog) -> GroupLog {
    let clock = cluster.clock;
    let mut bridge =
        LogicalChildBridge::new(&mut cluster.nodes, setup::mapping(), support::node(2)).unwrap();
    let image = claim_in(&mut bridge, clock, 9102, false);
    assert_eq!(&image.image().parent, anchored);
    assert_eq!(image.image().pins, vec![anchored.snapshot.unwrap()]);
    let mut parent = Parent::with_snapshots(&image);
    assert_eq!(
        parent.apps[&setup::mapping().parent]
            .read_at(2, ())
            .unwrap(),
        3
    );
    append(&mut parent, 3, 4);
    settle(&mut bridge, &mut parent, clock);
    assert_eq!(
        parent.apps[&setup::mapping().parent]
            .read_at(3, ())
            .unwrap(),
        7
    );
    bridge.close();
    assert!(bridge.is_drained());
    parent.close()
}
fn history(protocol: NativePeerProtocol) {
    let _serial = HISTORY.lock().unwrap();
    let mut cluster = Cluster::new(protocol);
    let anchored = prepare(&mut cluster);
    cluster.checkpoint();
    let expected = suffix(&mut cluster, &anchored);
    assert_eq!(expected.snapshot, anchored.snapshot);
    assert_eq!(expected.commit_index, 3);
    assert_eq!(expected.entries.len(), 1);
    assert_eq!(expected.entries[0].index, 3);
    assert_eq!(expected.ballot_origin, anchored.ballot_origin);
    cluster.close();
    cluster.open(NativeOpenMode::Recover);
    let recovered = cluster.claim(3, 9103, false);
    assert_eq!(recovered.image().parent, expected);
    assert_eq!(recovered.image().pins, vec![anchored.snapshot.unwrap()]);
    let parent = Parent::with_snapshots(&recovered);
    assert_eq!(parent.apps[&setup::mapping().parent].applied_index(), 3);
    assert_eq!(
        parent.apps[&setup::mapping().parent]
            .read_at(3, ())
            .unwrap(),
        7
    );
    assert!(parent.is_drained());
    super::recovery::refusal_is_atomic(&recovered, &parent.apps[&setup::mapping().parent]);
    assert_eq!(parent.close(), expected);
    cluster.finish();
}
#[test]
fn tcp_original_parent_checkpoint_publication_compaction_and_reconcile_recover_full_bytes() {
    history(NativePeerProtocol::TcpTls);
}
#[cfg(feature = "quic")]
#[test]
fn quic_original_parent_checkpoint_publication_compaction_and_reconcile_recover_full_bytes() {
    history(NativePeerProtocol::Quic);
}
