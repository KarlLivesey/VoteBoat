// SPDX-License-Identifier: RPL-1.5
// Copyright (c) 2026 Karl Livesey
use super::{
    aliases::{Bridge, Middle, UpperBridge},
    cluster::Cluster,
    setup,
};
use std::time::{Duration, Instant};
use voteboat::{identity::OperationId, logical_voter::*, outbound::OutboundQueue, runtime::*};
pub struct Lower<'a> {
    pub bridges: Vec<Bridge<'a>>,
    pub clock: Instant,
    pub held: bool,
}
impl<'a> Lower<'a> {
    pub fn new(clusters: &'a mut [Cluster], clock: Instant) -> Self {
        let bridges = clusters
            .iter_mut()
            .map(|c| {
                LogicalChildBridge::new(
                    &mut c.nodes,
                    setup::lower_mapping(c.slot),
                    c.representative,
                )
                .unwrap()
            })
            .collect();
        Self {
            bridges,
            clock,
            held: false,
        }
    }
    pub fn now(&self) -> MonoTime {
        MonoTime(self.clock.elapsed().as_millis() as u64)
    }
    pub fn advance(&mut self) {
        let now = self.now();
        for b in &mut self.bridges {
            b.advance(
                now,
                LogicalBridgeBudget {
                    node_polls: 3,
                    completions: if self.held { 0 } else { 8 },
                    transitions: 1,
                    node: NodePollBudget::default(),
                },
            )
            .unwrap();
        }
    }
    pub fn claims(&mut self, generation: u128) -> Vec<VerifiedLogicalImage> {
        for (i, b) in self.bridges.iter_mut().enumerate() {
            b.begin_claim(OperationId::new(generation + i as u128).unwrap())
                .unwrap();
        }
        let end = Instant::now() + Duration::from_secs(15);
        let mut images = (0..9).map(|_| None).collect::<Vec<_>>();
        while images.iter().any(Option::is_none) {
            self.advance();
            for (i, b) in self.bridges.iter_mut().enumerate() {
                if images[i].is_none() {
                    images[i] = b.take_image();
                }
            }
            assert!(Instant::now() < end, "bottom original claim timeout");
            std::thread::park_timeout(Duration::from_millis(1));
        }
        images.into_iter().map(Option::unwrap).collect()
    }
    pub fn pump_middle(&mut self, middle: &mut [Vec<Middle>]) {
        for (i, cohort) in middle.iter_mut().enumerate() {
            for (j, node) in cohort.iter_mut().enumerate() {
                let bridge = &mut self.bridges[i * 3 + j];
                node.pump_backing(|p| {
                    bridge.pump_persistence(p.persistence).unwrap();
                    bridge.pump_release(p.releases).unwrap();
                    if let Some(s) = p.snapshots {
                        bridge.pump_snapshots(s, p.persistence).unwrap();
                    }
                });
            }
        }
    }
    pub fn pump_upper(&mut self, upper: &mut [UpperBridge<'_>]) {
        for (i, u) in upper.iter_mut().enumerate() {
            for j in 0..3 {
                let bridge = &mut self.bridges[i * 3 + j];
                u.pump_backing(j, |p| {
                    let p = p.unwrap();
                    bridge.pump_persistence(p.persistence).unwrap();
                    bridge.pump_release(p.releases).unwrap();
                    if let Some(s) = p.snapshots {
                        bridge.pump_snapshots(s, p.persistence).unwrap();
                    }
                })
                .unwrap();
            }
        }
    }
    pub fn poll_middle(&mut self, middle: &mut [Vec<Middle>]) {
        self.pump_middle(middle);
        self.advance();
        self.pump_middle(middle);
        let now = self.now();
        for node in middle.iter_mut().flatten() {
            node.poll(now, LogicalNodePollBudget::default()).unwrap();
        }
        std::thread::park_timeout(Duration::from_millis(1));
    }
    pub fn drained(&self) -> bool {
        self.bridges.iter().all(Bridge::is_drained)
    }
    /// Called only after the host closes the intake of every original middle
    /// owner. All original lower persistence/release/snapshot pumps continue.
    pub fn shutdown_middle(&mut self, middle: &mut [Vec<Middle>]) {
        self.pump_middle(middle);
        self.advance();
        self.pump_middle(middle);
        let now = self.now();
        for (cohort, nodes) in middle.iter_mut().enumerate() {
            for node in nodes {
                super::shutdown::record(
                    node.poll_shutdown(now, LogicalNodePollBudget::default()),
                    setup::middle(cohort).group,
                    node.local().outbound.binding(),
                    &setup::middle(cohort).voter_stores,
                );
            }
        }
        std::thread::park_timeout(Duration::from_millis(1));
    }
}
