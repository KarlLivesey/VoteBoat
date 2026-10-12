// SPDX-License-Identifier: RPL-1.5
// Copyright (c) 2026 Karl Livesey
use super::{aliases::Bridge, channels::Channels, cluster::Cluster, parent::Parent, setup};
use std::time::{Duration, Instant};
use voteboat::{
    application::StateMachine, log::GroupLog, logical_voter::*,
    native::connect::NativePeerProtocol, raft::*, runtime::*,
};
pub struct Host<'a> {
    pub parents: Vec<Parent>,
    pub representatives: Vec<RepresentativeBinding>,
    pub paused_child: Option<usize>,
    parked: Vec<&'a mut Vec<super::cluster::ChildNode>>,
    pub bridges: Vec<Bridge<'a>>,
    clocks: Vec<Instant>,
    channels: Channels,
    clock: Instant,
}
impl<'a> Host<'a> {
    pub fn new(
        cohorts: &'a mut [Cluster; 3],
        protocol: NativePeerProtocol,
        generation: u128,
    ) -> Self {
        super::bootstrap::prepare(cohorts, generation);
        let clocks = cohorts.iter().map(|c| c.clock).collect::<Vec<_>>();
        for cohort in &mut *cohorts {
            cohort.park_quorum_peers();
        }
        let (mut bridges, mut parked): (Vec<_>, Vec<_>) = cohorts
            .iter_mut()
            .map(|c| {
                let bridge =
                    LogicalChildBridge::new(&mut c.nodes, setup::mapping(c.slot), c.representative)
                        .unwrap();
                (bridge, &mut c.parked)
            })
            .unzip();
        let images = super::bootstrap::claims(&mut bridges, &mut parked, &clocks, generation);
        let descriptors = images
            .iter()
            .enumerate()
            .map(|(i, v)| LogicalPeerDescriptor::from_verified(v, &setup::app(i)).unwrap())
            .collect::<Vec<_>>();
        let parents = images.iter().map(Parent::new).collect::<Vec<_>>();
        let representatives = images
            .iter()
            .map(VerifiedLogicalImage::representative)
            .collect();
        let clock = Instant::now();
        let channels = Channels::new(&descriptors, &parents, protocol, MonoTime(0));
        Self {
            parents,
            representatives,
            paused_child: None,
            parked,
            bridges,
            clocks,
            channels,
            clock,
        }
    }
    pub fn poll(&mut self) {
        self.poll_with_completions(4);
    }
    /// Withhold only consumption of original leader-child outputs. Provider and
    /// authenticated parent network progress continue with their existing budgets.
    pub fn poll_with_completions(&mut self, leader_completions: usize) -> Vec<OwnerStep> {
        let mut steps = Vec::new();
        for (i, peers) in self.parked.iter_mut().enumerate() {
            if self.paused_child != Some(i) {
                for node in peers.iter_mut() {
                    node.poll(
                        MonoTime(self.clocks[i].elapsed().as_millis() as u64),
                        NodePollBudget::default(),
                    )
                    .unwrap();
                }
            }
        }
        for (i, (bridge, parent)) in self.bridges.iter_mut().zip(&mut self.parents).enumerate() {
            bridge.pump_persistence(&mut parent.port).unwrap();
            bridge.pump_release(parent.driver.releases()).unwrap();
            if let Some(s) = &mut parent.snapshots {
                bridge.pump_snapshots(&mut s.port, &parent.port).unwrap();
            }
            bridge
                .advance(
                    MonoTime(self.clocks[i].elapsed().as_millis() as u64),
                    LogicalBridgeBudget {
                        node_polls: 3,
                        completions: if i == 0 { leader_completions } else { 4 },
                        transitions: 1,
                        node: NodePollBudget::default(),
                    },
                )
                .unwrap();
            bridge.pump_persistence(&mut parent.port).unwrap();
            bridge.pump_release(parent.driver.releases()).unwrap();
            if let Some(s) = &mut parent.snapshots {
                bridge.pump_snapshots(&mut s.port, &parent.port).unwrap();
            }
            steps.extend(parent.poll().steps);
        }
        self.channels.poll(
            &mut self.parents,
            MonoTime(self.clock.elapsed().as_millis() as u64),
        );
        std::thread::park_timeout(Duration::from_millis(1));
        steps
    }
    pub fn until(&mut self, mut done: impl FnMut(&Self) -> bool) {
        let end = Instant::now() + Duration::from_secs(15);
        while !done(self) {
            self.poll();
            assert!(
                Instant::now() < end,
                "actual nine-child parent progress timeout"
            );
        }
    }
    pub fn is_drained(&self) -> bool {
        self.parents.iter().all(Parent::is_drained)
            && self.bridges.iter().all(Bridge::is_drained)
            && self.channels.is_quiescent()
    }
    pub fn elect(&mut self, slot: usize) {
        let previous = self.parents[slot]
            .owner
            .core(setup::mapping(slot).parent)
            .unwrap()
            .state()
            .hard_state
            .term;
        self.parents[slot]
            .owner
            .admit(setup::mapping(slot).parent, Event::Campaign)
            .unwrap();
        self.until(|h| {
            let core = h.parents[slot]
                .owner
                .core(setup::mapping(slot).parent)
                .unwrap();
            core.role() == Role::Leader
                && core.state().hard_state.term > previous
                && core.state().term_at(core.state().commit_index)
                    == Some(core.state().hard_state.term)
                && h.parents
                    .iter()
                    .all(|p| p.apps[&p.mapping.parent].applied_index() >= core.state().commit_index)
                && h.is_drained()
        });
    }
    pub fn close(mut self) -> Vec<GroupLog> {
        self.until(Self::is_drained);
        for bridge in &mut self.bridges {
            bridge.close();
            assert!(bridge.is_drained());
        }
        self.channels.close(
            &mut self.parents,
            MonoTime(self.clock.elapsed().as_millis() as u64),
        );
        self.parents.into_iter().map(Parent::close).collect()
    }
}
