// SPDX-License-Identifier: RPL-1.5
// Copyright (c) 2026 Karl Livesey
use super::{cluster::*, setup, support};
use std::time::{Duration, Instant};
use voteboat::{
    application::StateMachine,
    identity::OperationId,
    logical_voter::*,
    native::{connect::NativePeerProtocol, startup::NativeOpenMode},
    runtime::NodePollBudget,
    snapshot_worker::SnapshotWorker,
};
#[test]
fn original_backing_checkpoint_admission_preserves_scope_and_waits_for_durability() {
    let _guard = serial();
    let clock = Instant::now();
    let mut cluster = Cluster::new(NativePeerProtocol::TcpTls, 0, clock);
    {
        let mut bridge = LogicalChildBridge::new(
            &mut cluster.nodes,
            setup::lower_mapping(0),
            support::node(1),
        )
        .unwrap();
        let before = inspect(&bridge);
        assert_eq!(
            bridge.checkpoint_backing(3),
            Err(LogicalBridgeError::InvalidAssembly)
        );
        assert_eq!(inspect(&bridge), before);
        bridge
            .begin_claim(OperationId::new(400000).unwrap())
            .unwrap();
        let usage = bridge.usage();
        bridge.checkpoint_backing(0).unwrap();
        assert_eq!(bridge.usage(), usage);
        assert_eq!(inspect(&bridge).0, before.0);
        assert!(bridge.take_image().is_none());
        let frontier = bridge
            .inspect_backing(0, |p| {
                p.owner
                    .core(setup::bottom(0).group)
                    .unwrap()
                    .state()
                    .commit_index
            })
            .unwrap();
        let mut image = None;
        let end = Instant::now() + Duration::from_secs(15);
        loop {
            advance(&mut bridge, clock);
            if image.is_none() {
                image = bridge.take_image();
            }
            let completed = bridge
                .inspect_backing(0, |p| {
                    let snapshots = p.snapshots.as_ref().unwrap();
                    p.owner
                        .core(setup::bottom(0).group)
                        .unwrap()
                        .state()
                        .base_index()
                        >= frontier
                        && snapshots.router.is_drained()
                        && snapshots.worker.is_drained()
                })
                .unwrap();
            if completed && image.is_some() {
                break;
            }
            assert!(
                Instant::now() < end,
                "original physical checkpoint/claim timeout"
            );
            std::thread::park_timeout(Duration::from_millis(1));
        }
        assert!(inspect(&bridge).0 > before.0);
        bridge.close();
        let closed = inspect(&bridge);
        assert_eq!(
            bridge.checkpoint_backing(0),
            Err(LogicalBridgeError::Closed)
        );
        assert_eq!(inspect(&bridge), closed);
        assert!(bridge.is_drained());
    }
    cluster.finish();
}

#[test]
fn recovered_backing_checkpoint_covers_new_original_claim_before_join() {
    let _guard = serial();
    let clock = Instant::now();
    let mut cluster = Cluster::new(NativePeerProtocol::TcpTls, 0, clock);
    let original = claim(&mut cluster, 400001);
    cluster.checkpoint();
    let group = setup::lower_mapping(0).child;
    let old_bases = cluster
        .all()
        .map(|node| node.local().owner.core(group).unwrap().state().base_index())
        .collect::<Vec<_>>();
    assert!(old_bases.iter().all(|&base| base > 0));
    cluster.close();
    cluster.open(NativeOpenMode::Recover);
    let recovered = claim(&mut cluster, 400002);
    assert_eq!(recovered.binding().identity, original.binding().identity);
    assert!(recovered.binding().session > original.binding().session);
    let end = Instant::now() + Duration::from_secs(15);
    while cluster
        .all()
        .zip(&old_bases)
        .any(|(node, &old)| node.local().applications[&group].applied_index() <= old)
    {
        cluster.poll();
        assert!(
            Instant::now() < end,
            "original recovered Claim application timeout"
        );
    }
    let frontiers = cluster
        .all()
        .map(|node| node.local().applications[&group].applied_index())
        .collect::<Vec<_>>();
    // Every old recovered snapshot is insufficient for this actual new prefix.
    assert!(cluster.all().zip(&frontiers).all(|(node, &frontier)| {
        let state = node.local().owner.core(group).unwrap().state();
        state.base_index() < frontier && state.snapshot.unwrap().index < frontier
    }));
    cluster.checkpoint();
    for (node, frontier) in cluster.all().zip(frontiers) {
        let local = node.local();
        let state = local.owner.core(group).unwrap().state();
        let snapshots = local.snapshots.as_ref().unwrap();
        assert!(state.base_index() >= frontier);
        assert!(state.snapshot.unwrap().index >= frontier);
        assert!(snapshots.router.is_drained() && snapshots.worker.is_drained());
    }
    drop((original, recovered));
    cluster.finish();
}

fn claim(cluster: &mut Cluster, operation: u128) -> VerifiedLogicalImage {
    let clock = cluster.clock;
    let mut bridge = LogicalChildBridge::new(
        &mut cluster.nodes,
        setup::lower_mapping(cluster.slot),
        cluster.representative,
    )
    .unwrap();
    bridge
        .begin_claim(OperationId::new(operation).unwrap())
        .unwrap();
    let end = Instant::now() + Duration::from_secs(15);
    let image = loop {
        advance(&mut bridge, clock);
        if let Some(image) = bridge.take_image() {
            break image;
        }
        assert!(Instant::now() < end, "original maintenance Claim timeout");
        std::thread::park_timeout(Duration::from_millis(1));
    };
    bridge.close();
    // Hand back only this consumer; the same Cluster drains all child work.
    assert!(bridge.observations_are_drained());
    image
}

fn advance(bridge: &mut super::aliases::Bridge<'_>, clock: Instant) {
    bridge
        .advance(
            voteboat::runtime::MonoTime(clock.elapsed().as_millis() as u64),
            LogicalBridgeBudget {
                node_polls: 3,
                completions: 8,
                transitions: 1,
                node: NodePollBudget::default(),
            },
        )
        .unwrap();
}

fn inspect(
    bridge: &super::aliases::Bridge<'_>,
) -> (u64, voteboat::runtime::EffectOwnerUsage, LogicalBridgeUsage) {
    bridge
        .inspect_backing(0, |p| {
            (
                p.owner
                    .core(setup::bottom(0).group)
                    .unwrap()
                    .state()
                    .base_index(),
                p.owner.usage(),
                bridge.usage(),
            )
        })
        .unwrap()
}
