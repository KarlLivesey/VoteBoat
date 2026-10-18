// SPDX-License-Identifier: RPL-1.5
// Copyright (c) 2026 Karl Livesey
use super::{
    cluster::Cluster,
    harness::{self, FixtureClock},
    setup, support,
};
use std::time::{Duration, Instant};
use voteboat::{identity::OperationId, logical_voter::*, runtime::*};
impl<C: FixtureClock> Cluster<C> {
    pub fn claim(
        &mut self,
        representative: u64,
        operation: u128,
        withhold: bool,
    ) -> VerifiedLogicalImage {
        let clock = &self.clock;
        let mut bridge = LogicalChildBridge::new(
            &mut self.nodes,
            setup::mapping(),
            support::node(representative),
        )
        .unwrap();
        let image = claim_in(&mut bridge, clock, operation, withhold);
        bridge.close();
        assert!(bridge.is_drained());
        image
    }
    pub fn read(&mut self, query: LogicalQuery) -> LogicalRead {
        let end = harness::hang_guard();
        loop {
            self.wait(|host| host.leader().is_some());
            let slot = self.leader().unwrap();
            let ticket = self.nodes[slot]
                .read(setup::mapping().child, query)
                .unwrap();
            loop {
                self.poll();
                if let Some(output) = self.nodes[slot].poll_read() {
                    assert_eq!(output.ticket(), ticket);
                    match self.nodes[slot].complete_read(output).unwrap() {
                        ReadOutcome::Read {
                            barrier,
                            result: Ok(result),
                        } => {
                            assert_eq!(barrier.group(), setup::mapping().child);
                            assert_eq!(barrier.request(), ticket.request);
                            return result;
                        }
                        ReadOutcome::Read {
                            result: Err(error), ..
                        } => panic!("actual logical read: {error:?}"),
                        ReadOutcome::NotRead(_) | ReadOutcome::Unavailable(_) => break,
                    }
                }
                assert!(Instant::now() < end, "actual logical quorum read timeout");
            }
        }
    }
}

pub fn claim_in(
    bridge: &mut super::aliases::Bridge<'_>,
    clock: impl FixtureClock,
    operation: u128,
    withhold: bool,
) -> VerifiedLogicalImage {
    let operation = OperationId::new(operation).unwrap();
    bridge.begin_claim(operation).unwrap();
    assert_eq!(bridge.begin_claim(operation), Err(LogicalBridgeError::Busy));
    assert!(bridge.take_image().is_none());
    let end = harness::hang_guard();
    let mut held = false;
    loop {
        let now = clock.round(harness::bridge_settled(bridge));
        let usage = bridge
            .advance(
                now,
                LogicalBridgeBudget {
                    node_polls: 3,
                    completions: 4,
                    transitions: 1,
                    node: NodePollBudget::default(),
                },
            )
            .unwrap();
        if withhold && !held && usage.command_bytes > 0 && usage.child_attempts == 1 {
            for _ in 0..32 {
                let usage = bridge
                    .advance(
                        clock.round(harness::bridge_settled(bridge)),
                        LogicalBridgeBudget {
                            node_polls: 3,
                            completions: 0,
                            transitions: 0,
                            node: NodePollBudget::default(),
                        },
                    )
                    .unwrap();
                assert_eq!(usage.child_attempts, 1);
                assert!(!usage.image_ready);
                assert!(!bridge.is_drained());
                assert!(bridge.take_image().is_none());
                std::thread::park_timeout(Duration::from_millis(1));
            }
            held = true;
        }
        if let Some(image) = bridge.take_image() {
            assert_eq!(image.claim().operation, operation);
            assert!(bridge.is_drained());
            assert!(!withhold || held);
            return image;
        }
        assert!(Instant::now() < end, "actual child claim timeout");
        std::thread::park_timeout(Duration::from_millis(1));
    }
}
