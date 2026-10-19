// SPDX-License-Identifier: RPL-1.5
// Copyright (c) 2026 Karl Livesey
//! Same actual physical backing owners are pumped while middle borrows move.
use super::{
    cluster::Cluster,
    harness::{self, FixtureClock, Observed, SimClock},
    repeated::Session,
    setup, Middle, UpperBridge,
};
use std::time::Duration;
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
    pub clock: SimClock,
    retired: Vec<Retired>,
}
impl<'a> Lower<'a> {
    pub fn new(clusters: &'a mut [Cluster], clock: &SimClock) -> Self {
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
            clock: clock.clone(),
            retired: Vec::new(),
        }
    }
    pub fn now(&self) -> MonoTime {
        self.clock.now()
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
        self.advance_with(Observed::default());
    }
    /// Start one complete fixture round: decide its protocol time from what
    /// the previous round left (every lower bridge's native children, the
    /// retired sessions, and `upper`, the caller's other participants), then
    /// poll the lower layer. Each fixture loop calls this exactly once per
    /// round; the rest of the round reuses [`Lower::now`].
    pub fn advance_with(&mut self, upper: Observed) {
        let retired = self
            .retired
            .iter()
            .map(|owned| owned.session.work_is_drained())
            .collect::<Vec<_>>();
        let observed = self
            .bridges
            .iter()
            .fold(upper, Observed::bridge)
            .progress(retired);
        let now = self.clock.round(observed);
        for owned in &mut self.retired {
            owned
                .session
                .poll(now, TransportPollBudget::default())
                .unwrap();
            // This obsolete route never admits frames to the current owner.
            let _ = owned.session.take_received(owned.authorization).unwrap();
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
        let end = harness::Wait::new(&self.clock, harness::LIVENESS_MS);
        loop {
            self.advance();
            if let Some(image) = self.bridges[slot].take_image() {
                return image;
            }
            assert!(end.within(&self.clock), "original lower Claim {slot}");
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
        self.middle_with(nodes, Observed::default());
    }
    /// One round over the lower layer and these middle nodes; `other` adds
    /// any separately borrowed bridge the caller polls in the same round.
    pub fn middle_with(&mut self, nodes: &mut [Middle], other: Observed) {
        self.pump(nodes);
        self.advance_with(Self::observed(nodes, other));
        self.pump(nodes);
        let now = self.now();
        for node in nodes {
            node.poll(now, LogicalNodePollBudget::default()).unwrap();
        }
        std::thread::park_timeout(Duration::from_millis(1));
    }
    /// Replica state of middle nodes the caller owns directly.
    pub fn observed(nodes: &[Middle], other: Observed) -> Observed {
        nodes.iter().fold(other, |o, node| o.local(node.local()))
    }
    pub fn close(self) {
        assert!(self.retired.is_empty(), "original channels handed to close");
        for mut bridge in self.bridges {
            bridge.close();
            assert!(bridge.is_drained(), "actual lower observations drain");
        }
    }
}
