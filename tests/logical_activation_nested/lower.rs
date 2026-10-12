// SPDX-License-Identifier: RPL-1.5
// Copyright (c) 2026 Karl Livesey
//! Same actual physical backing owners are pumped while middle borrows move.
use super::{cluster::Cluster, repeated::Session, setup, Middle, UpperBridge};
use std::time::{Duration, Instant};
use voteboat::{
    identity::OperationId, logical_voter::*, runtime::*, transport::TransportPollBudget,
};
type Bridge<'a> = voteboat::logical_voter::LogicalBackingBridge<'a, super::cluster::ChildNode>;
struct Retired {
    session: Session,
    authorization: RuntimeAuthorization,
}
pub struct Lower<'a> {
    pub bridges: Vec<Bridge<'a>>,
    pub clock: Instant,
    retired: Vec<Retired>,
}
impl<'a> Lower<'a> {
    pub fn new(clusters: &'a mut [Cluster], clock: Instant) -> Self {
        Self {
            bridges: clusters
                .iter_mut()
                .map(|c| {
                    LogicalBackingBridge::new(
                        &mut c.nodes,
                        setup::lower_mapping(c.slot),
                        c.representative,
                    )
                    .unwrap()
                })
                .collect(),
            clock,
            retired: Vec::new(),
        }
    }
    pub fn now(&self) -> MonoTime {
        MonoTime(self.clock.elapsed().as_millis() as u64)
    }
    pub fn budget() -> LogicalBridgeBudget {
        LogicalBridgeBudget {
            node_polls: 3,
            completions: 8,
            transitions: 1,
            node: NodePollBudget::default(),
        }
    }
    pub fn advance(&mut self) {
        let now = self.now();
        for owned in &mut self.retired {
            owned
                .session
                .poll(now, TransportPollBudget::default())
                .unwrap();
        }
        for bridge in &mut self.bridges {
            bridge.advance(now, Self::budget()).unwrap();
        }
    }
    pub fn retain_retired(&mut self, session: Session, authorization: RuntimeAuthorization) {
        assert!(self.retired.len() < 3, "three original obsolete channels");
        assert!(session.work_is_drained());
        self.retired.push(Retired {
            session,
            authorization,
        });
    }
    pub fn take_retired_for_close(&mut self) -> Vec<Session> {
        self.retired
            .drain(..)
            .map(|mut owned| {
                assert!(
                    owned.session.take_send().is_none(),
                    "no retired send output"
                );
                let _ = owned.session.take_received(owned.authorization).unwrap();
                owned.session.close();
                owned.session
            })
            .collect()
    }
    pub fn claim(&mut self, slot: usize, operation: u128) -> VerifiedLogicalImage {
        self.bridges[slot]
            .begin_claim(OperationId::new(operation).unwrap())
            .unwrap();
        let end = Instant::now() + Duration::from_secs(15);
        loop {
            self.advance();
            if let Some(image) = self.bridges[slot].take_image() {
                return image;
            }
            assert!(Instant::now() < end, "original lower Claim {slot}");
            std::thread::park_timeout(Duration::from_millis(1));
        }
    }
    pub fn pump(&mut self, nodes: &mut [Middle]) {
        for (slot, node) in nodes.iter_mut().enumerate() {
            node.pump_backing(|p| {
                let b = &mut self.bridges[slot];
                b.pump_persistence(p.persistence).unwrap();
                b.pump_release(p.releases).unwrap();
                if let Some(s) = p.snapshots {
                    b.pump_snapshots(s, p.persistence).unwrap();
                }
            });
        }
    }
    pub fn pump_bridge(&mut self, bridge: &mut UpperBridge<'_>, count: usize) {
        for slot in 0..count {
            bridge
                .pump_backing(slot, |p| {
                    let p = p.unwrap();
                    let b = &mut self.bridges[slot];
                    b.pump_persistence(p.persistence).unwrap();
                    b.pump_release(p.releases).unwrap();
                    if let Some(s) = p.snapshots {
                        b.pump_snapshots(s, p.persistence).unwrap();
                    }
                })
                .unwrap();
        }
    }
    pub fn middle(&mut self, nodes: &mut [Middle]) {
        self.pump(nodes);
        self.advance();
        self.pump(nodes);
        let now = self.now();
        for node in nodes {
            node.poll(now, LogicalNodePollBudget::default()).unwrap();
        }
        std::thread::park_timeout(Duration::from_millis(1));
    }
    pub fn close(self) {
        assert!(self.retired.is_empty(), "original channels handed to close");
        for mut bridge in self.bridges {
            bridge.close();
            assert!(bridge.is_drained(), "actual lower observations drain");
        }
    }
}
