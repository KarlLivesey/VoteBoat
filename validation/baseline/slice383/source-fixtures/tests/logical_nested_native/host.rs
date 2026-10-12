// SPDX-License-Identifier: RPL-1.5
// Copyright (c) 2026 Karl Livesey
use super::{
    aliases::{Middle, Top, UpperBridge},
    application::App,
    channels::Channels,
    lower::Lower,
    parts, setup,
};
use std::time::{Duration, Instant};
use voteboat::{
    application::StateMachine,
    identity::OperationId,
    logical_voter::*,
    native::connect::NativePeerProtocol,
    outbound::OutboundQueue,
    raft::Role,
    runtime::{ClientTicket, OwnerStep, ReadInvocationTicket},
};
pub struct Host<'b, 'm> {
    pub nodes: Vec<Top>,
    pub lower: Lower<'b>,
    pub upper: Vec<UpperBridge<'m>>,
    pub phase: &'static str,
    pub last_proposal: [Option<OwnerStep>; 3],
    pub last_read: [Option<OwnerStep>; 3],
    pub accepted_clients: Vec<ClientTicket>,
    pub accepted_reads: Vec<ReadInvocationTicket>,
    pub writes: Vec<super::convergence::WriteProgress>,
    pub last_step: [Option<String>; 3],
    maintenance: super::maintenance::Maintenance,
    middle_transport: Vec<Vec<super::observed::Observation>>,
    channels: Channels,
}
impl<'b, 'm> Host<'b, 'm> {
    pub fn new(
        mut lower: Lower<'b>,
        middle: &'m mut [Vec<Middle>],
        protocol: NativePeerProtocol,
        generation: u128,
        middle_channels: &[Channels],
    ) -> Self {
        super::maintenance::check_scope(middle);
        let mut upper = middle
            .iter_mut()
            .enumerate()
            .map(|(i, cohort)| {
                LogicalBackingBridge::new(
                    cohort,
                    setup::upper_mapping(i),
                    super::support::node(201 + i as u64 * 3),
                )
                .unwrap()
            })
            .collect::<Vec<_>>();
        for (i, b) in upper.iter_mut().enumerate() {
            b.begin_claim(OperationId::new(generation + i as u128).unwrap())
                .unwrap();
        }
        let mut images = (0..3).map(|_| None).collect::<Vec<_>>();
        let end = Instant::now() + Duration::from_secs(15);
        while images.iter().any(Option::is_none) {
            advance(&mut lower, &mut upper);
            for (i, b) in upper.iter_mut().enumerate() {
                if images[i].is_none() {
                    images[i] = b.take_image();
                }
            }
            assert!(
                Instant::now() < end,
                "upper original nested claim timeout: {:?}",
                upper.iter().map(|b| b.usage()).collect::<Vec<_>>()
            );
        }
        let images = images.into_iter().map(Option::unwrap).collect::<Vec<_>>();
        for image in &images {
            assert_eq!(image.endpoint_chain().depth(), 2);
            assert!(image.endpoint_chain().leaf().node.get() <= 27);
        }
        let descriptors = images
            .iter()
            .enumerate()
            .map(|(i, v)| LogicalPeerDescriptor::from_verified(v, &setup::upper_app(i)).unwrap())
            .collect::<Vec<_>>();
        let originals = images
            .iter()
            .map(|image| parts::build(image, App::new(), setup::upper_limits(), lower.now()))
            .collect::<Vec<_>>();
        let bindings = originals
            .iter()
            .map(|p| (p.local.owner.authorization(), p.local.outbound.binding()))
            .collect::<Vec<_>>();
        let mut channels = Channels::new(&descriptors, &bindings, protocol, lower.now());
        let nodes = originals
            .into_iter()
            .enumerate()
            .map(|(i, mut p)| {
                p.sessions = channels.take_sessions(i, &descriptors);
                LogicalNode::from_parts(
                    &images[i],
                    p,
                    parts::node_limits(setup::upper_limits()),
                    lower.now(),
                )
                .unwrap_or_else(|e| panic!("upper original assembly: {:?}", e.reason))
            })
            .collect();
        Self {
            nodes,
            lower,
            upper,
            channels,
            phase: "assembled",
            last_proposal: std::array::from_fn(|_| None),
            last_read: std::array::from_fn(|_| None),
            accepted_clients: Vec::with_capacity(super::requests::ATTEMPTS * 2),
            accepted_reads: Vec::with_capacity(super::requests::ATTEMPTS),
            writes: Vec::with_capacity(2),
            last_step: std::array::from_fn(|_| None),
            maintenance: super::maintenance::Maintenance::default(),
            middle_transport: middle_channels.iter().map(Channels::observations).collect(),
        }
    }
    pub fn poll(&mut self) {
        self.maintenance
            .poll(&mut self.upper, self.phase != "close");
        for (i, node) in self.nodes.iter_mut().enumerate() {
            pump(&mut self.upper[i], node);
        }
        advance(&mut self.lower, &mut self.upper);
        let now = self.lower.now();
        for (i, node) in self.nodes.iter_mut().enumerate() {
            pump(&mut self.upper[i], node);
            let progress = if self.phase == "close" {
                super::shutdown::record(
                    node.poll_shutdown(now, LogicalNodePollBudget::default()),
                    setup::top().group,
                    node.local().outbound.binding(),
                    &setup::top().voter_stores,
                )
            } else {
                node.poll(now, LogicalNodePollBudget::default()).unwrap()
            };
            assert!(
                progress.steps.iter().all(|s| super::requests::valid_step(
                    s,
                    &self.accepted_clients,
                    &self.accepted_reads
                )),
                "original upper steps: phase={}; {:?}",
                self.phase,
                progress.steps
            );
            for step in progress.steps {
                self.last_step[i] = Some(format!("{step:?}"));
                if step.proposed.is_some() {
                    eprintln!("nested original proposed now={now:?}: {step:?}");
                    self.last_proposal[i] = Some(step);
                } else if step.read.is_some() {
                    eprintln!("nested original read now={now:?}: {step:?}");
                    self.last_read[i] = Some(step);
                }
            }
        }
        let mut writes = std::mem::take(&mut self.writes);
        for write in &mut writes {
            write.observe(self);
        }
        self.writes = writes;
        assert!(
            self.writes
                .iter()
                .all(super::convergence::WriteProgress::valid),
            "original write/convergence functional timeout: {}",
            super::diagnostic::retained(self)
        );
    }
    pub fn transport(&self) -> String {
        let middle = self
            .middle_transport
            .iter()
            .map(|xs| {
                xs.iter()
                    .map(|(key, c)| (*key, c.get()))
                    .collect::<Vec<_>>()
            })
            .collect::<Vec<_>>();
        format!("middle={middle:?},upper={:?}", self.channels.counts())
    }
    pub fn until(&mut self, mut done: impl FnMut(&Self) -> bool) {
        let end = Instant::now() + Duration::from_secs(15);
        while !done(self) {
            self.poll();
            assert!(
                Instant::now() < end,
                "two-level actual progress timeout: {:?}; {}",
                self.nodes
                    .iter()
                    .map(|n| {
                        let c = n.local().owner.core(setup::top().group).unwrap();
                        (
                            c.role(),
                            c.state().hard_state.term,
                            c.state().last_index(),
                            c.state().commit_index,
                            n.work_is_drained(),
                        )
                    })
                    .collect::<Vec<_>>(),
                super::diagnostic::retained(self)
            );
        }
    }
    pub fn drained(&self) -> bool {
        self.nodes.iter().all(Top::work_is_drained)
            && self.upper.iter().all(UpperBridge::is_drained)
            && self.lower.drained()
    }
    pub fn elect(&mut self) {
        self.phase = "upper_election";
        let original = super::watchdog::Election::new(self);
        let mut watch = super::watchdog::Watchdog::new();
        self.nodes[0].campaign().unwrap();
        while !self.election_ready() {
            self.poll();
            watch.election(self, &original);
            assert!(
                watch.valid(),
                "original initial-election functional timeout: {}",
                super::diagnostic::retained(self)
            );
        }
    }
    fn election_ready(&self) -> bool {
        let c = self.nodes[0]
            .local()
            .owner
            .core(setup::top().group)
            .unwrap();
        c.role() == Role::Leader
            && !c.has_pending_dependency()
            && c.state().term_at(c.state().commit_index) == Some(c.state().hard_state.term)
            && self.nodes.iter().all(|n| {
                n.local().applications[&setup::top().group].applied_index()
                    >= c.state().commit_index
            })
    }
    pub fn close(mut self) -> Lower<'b> {
        self.phase = "close";
        for node in &mut self.nodes {
            node.close_intake().unwrap();
        }
        self.until(|h| h.nodes.iter().all(Top::is_drained));
        self.maintenance.poll(&mut self.upper, false);
        assert!(
            self.maintenance.completed > 0,
            "original middle maintenance must complete"
        );
        self.reclaim_closed()
    }
    /// Return an already closed top cohort's borrow without destroying the
    /// original middle Nodes or canceling their independently owned lower work.
    pub fn reclaim_closed(mut self) -> Lower<'b> {
        assert!(self.nodes.iter().all(Top::is_drained));
        assert!(
            self.upper.iter().all(UpperBridge::observations_are_drained),
            "original upper observations still owned: {}",
            super::diagnostic::retained(&self)
        );
        for b in &mut self.upper {
            b.close();
        }
        let sessions = self
            .nodes
            .into_iter()
            .flat_map(|n| {
                n.into_parts()
                    .unwrap_or_else(|_| panic!("upper original drain"))
                    .sessions
                    .into_values()
            })
            .collect::<Vec<_>>();
        self.channels.close(sessions, self.lower.now());
        self.lower
    }
}
fn pump(bridge: &mut UpperBridge<'_>, node: &mut Top) {
    node.pump_backing(|p| {
        bridge.pump_persistence(p.persistence).unwrap();
        bridge.pump_release(p.releases).unwrap();
        if let Some(s) = p.snapshots {
            bridge.pump_snapshots(s, p.persistence).unwrap();
        }
    });
}
fn advance(lower: &mut Lower<'_>, upper: &mut [UpperBridge<'_>]) {
    lower.pump_upper(upper);
    lower.advance();
    lower.pump_upper(upper);
    let now = lower.now();
    for u in upper {
        u.advance(
            now,
            LogicalBridgeBudget {
                node_polls: 3,
                completions: 8,
                transitions: 1,
                node: LogicalNodePollBudget::default(),
            },
        )
        .unwrap();
    }
    std::thread::park_timeout(Duration::from_millis(1));
}
