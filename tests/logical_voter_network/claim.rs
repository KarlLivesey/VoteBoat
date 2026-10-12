// SPDX-License-Identifier: RPL-1.5
// Copyright (c) 2026 Karl Livesey
use super::{aliases::Bridge, cluster::Cluster, setup, support};
use std::time::{Duration, Instant};
use voteboat::{identity::*, logical_voter::*, runtime::*};
pub fn claim(bridge: &mut Bridge<'_>, clock: Instant, operation: u128) -> VerifiedLogicalImage {
    claim_with_peers(bridge, &mut [], clock, operation)
}
pub fn claim_with_peers(
    bridge: &mut Bridge<'_>,
    peers: &mut [super::cluster::ChildNode],
    clock: Instant,
    operation: u128,
) -> VerifiedLogicalImage {
    let operation = OperationId::new(operation).unwrap();
    bridge.begin_claim(operation).unwrap();
    let end = Instant::now() + Duration::from_secs(15);
    loop {
        for node in &mut *peers {
            node.poll(
                MonoTime(clock.elapsed().as_millis() as u64),
                NodePollBudget::default(),
            )
            .unwrap();
        }
        bridge
            .advance(
                MonoTime(clock.elapsed().as_millis() as u64),
                LogicalBridgeBudget {
                    node_polls: 3,
                    completions: 4,
                    transitions: 1,
                    node: NodePollBudget::default(),
                },
            )
            .unwrap();
        if let Some(image) = bridge.take_image() {
            assert_eq!(image.claim().operation, operation);
            assert!(bridge.is_drained());
            return image;
        }
        assert!(Instant::now() < end, "actual three-child claim timeout");
        std::thread::park_timeout(Duration::from_millis(1));
    }
}
pub fn representative(slot: usize) -> NodeId {
    support::node(*setup::physical_ids(slot).nth(slot).as_ref().unwrap())
}
pub fn bridge(cluster: &mut Cluster) -> Bridge<'_> {
    LogicalChildBridge::new(
        &mut cluster.nodes,
        setup::mapping(cluster.slot),
        cluster.representative,
    )
    .unwrap()
}
