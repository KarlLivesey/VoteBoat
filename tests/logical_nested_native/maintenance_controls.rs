// SPDX-License-Identifier: RPL-1.5
// Copyright (c) 2026 Karl Livesey
use super::{cluster::*, setup, support};
use std::time::{Duration, Instant};
use voteboat::{
    identity::OperationId, logical_voter::*, native::connect::NativePeerProtocol,
    runtime::NodePollBudget, snapshot_worker::SnapshotWorker,
};
#[test]
fn original_backing_checkpoint_admission_preserves_scope_and_waits_for_durability() {
    let _guard = HISTORY.lock().unwrap();
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
