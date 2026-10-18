// SPDX-License-Identifier: RPL-1.5
// Copyright (c) 2026 Karl Livesey
use super::{
    cleanup,
    harness::{self, FixtureClock, Observed, PortLease, SimClock},
    setup, support,
};
use std::{
    net::SocketAddr,
    path::PathBuf,
    sync::{
        atomic::{AtomicU64, Ordering},
        Arc, Mutex,
    },
    time::{Duration, Instant},
};
use voteboat::{
    application::StateMachine,
    logical_voter::*,
    native::{
        connect::{NativePeerProtocol, NativeServiceConnector},
        node::NativeNode,
        startup::*,
        worker::ThreadWake,
    },
    raft::Role,
    runtime::*,
};
pub type ChildNode = NativeNode<LogicalApplication, NativeServiceConnector>;
pub static HISTORY: Mutex<()> = Mutex::new(());
pub fn serial() -> std::sync::MutexGuard<'static, ()> {
    support::serial::serial(&HISTORY)
}
static NEXT: AtomicU64 = AtomicU64::new(1);
/// Three native child voters. `C` supplies protocol time: wall-clock
/// `Instant` by default, or a deterministic [`SimClock`].
pub struct Cluster<C: FixtureClock = Instant> {
    pub nodes: Vec<ChildNode>,
    pub clock: C,
    root: PathBuf,
    addresses: [SocketAddr; 3],
    // Owned for the whole test, across close and reopen.
    leases: [PortLease; 3],
    protocol: NativePeerProtocol,
    application: LogicalApplication,
}
impl Cluster {
    pub fn new(protocol: NativePeerProtocol) -> Self {
        Self::new_with_application(protocol, setup::app())
    }
    pub fn new_with_application(
        protocol: NativePeerProtocol,
        application: LogicalApplication,
    ) -> Self {
        Self::build(protocol, application, Instant::now())
    }
}
impl Cluster<SimClock> {
    pub fn simulated(protocol: NativePeerProtocol, application: LogicalApplication) -> Self {
        Self::build(protocol, application, SimClock::default())
    }
}
impl<C: FixtureClock> Cluster<C> {
    fn build(protocol: NativePeerProtocol, application: LogicalApplication, clock: C) -> Self {
        let root = std::env::temp_dir().join(format!(
            "voteboat-logical-child-{}-{}",
            std::process::id(),
            NEXT.fetch_add(1, Ordering::Relaxed)
        ));
        std::fs::create_dir(&root).unwrap();
        let leases: [PortLease; 3] = std::array::from_fn(|_| PortLease::acquire(protocol));
        let mut host = Self {
            nodes: Vec::new(),
            clock,
            root,
            addresses: leases.each_ref().map(PortLease::address),
            leases,
            protocol,
            application,
        };
        host.open(NativeOpenMode::Create);
        host
    }
    pub fn now(&self) -> MonoTime {
        self.clock.now()
    }
    fn config(&self, id: u64, mode: NativeOpenMode) -> NativeStartup {
        NativeStartup {
            directory: self.root.join(id.to_string()),
            mode,
            node: support::node(id),
            store: support::identity(id.into()),
            bootstrap: setup::child(),
            listen: self.addresses[id as usize - 1],
            peers: (1..=3)
                .filter(|peer| *peer != id)
                .map(|peer| {
                    (
                        support::node(peer),
                        NativeStartupPeer {
                            address: self.addresses[peer as usize - 1],
                            certificate: setup::certificate(peer).to_vec(),
                            server_name: format!("node{peer}.voteboat.test"),
                        },
                    )
                })
                .collect(),
            tls: support::tls::configuration(id)
                .with_wire_version(8)
                .unwrap(),
            entropy_seed: id * 17,
            limits: NodeLimits::default(),
            ready_visit_items: 16,
        }
    }
    pub fn open(&mut self, mode: NativeOpenMode) {
        assert!(self.nodes.is_empty());
        self.clock.reopened();
        for id in 1..=3 {
            self.leases[id as usize - 1].handoff();
            let node = self
                .config(id, mode)
                .open_with_protocol_and_timers(
                    self.protocol,
                    NativeTimingProfile::Throughput.timers(),
                    self.application.clone(),
                    Arc::new(ThreadWake::current()),
                    self.now(),
                )
                .unwrap_or_else(|error| panic!("actual logical child startup: {:?}", error.reason));
            self.nodes.push(node);
        }
        self.nodes[0]
            .control(setup::mapping().child, NodeControl::Campaign)
            .unwrap();
        self.wait(|host| host.leader().is_some());
    }
    pub fn leader(&self) -> Option<usize> {
        self.nodes.iter().position(|node| {
            let core = node.local().owner.core(setup::mapping().child).unwrap();
            core.role() == Role::Leader
                && !core.has_pending_dependency()
                && core.state().term_at(core.state().commit_index)
                    == Some(core.state().hard_state.term)
                && node.local().applications[&setup::mapping().child].applied_index()
                    >= core.state().commit_index
        })
    }
    pub fn poll(&mut self) {
        let now = self.clock.round(Observed::nodes(&self.nodes));
        for node in &mut self.nodes {
            node.poll(now, NodePollBudget::default()).unwrap();
        }
        std::thread::park_timeout(Duration::from_millis(1));
    }
    pub fn wait(&mut self, mut done: impl FnMut(&Self) -> bool) {
        let end = harness::Wait::new(&self.clock, harness::LIVENESS_MS);
        while !done(self) {
            self.poll();
            assert!(
                end.within(&self.clock),
                "native logical child timeout: {}",
                self.root.display()
            );
        }
    }
    pub fn catch_up(&mut self) {
        self.wait(|host| {
            host.leader().is_some_and(|leader| {
                let commit = host.nodes[leader]
                    .local()
                    .owner
                    .core(setup::mapping().child)
                    .unwrap()
                    .state()
                    .commit_index;
                host.nodes.iter().all(|node| {
                    node.local().applications[&setup::mapping().child].applied_index() >= commit
                })
            })
        });
    }
    pub fn checkpoint(&mut self) {
        self.catch_up();
        for node in &mut self.nodes {
            node.control(setup::mapping().child, NodeControl::Checkpoint)
                .unwrap();
        }
        self.wait(|host| {
            host.nodes.iter().all(|node| {
                node.checkpoints().pending.is_empty()
                    && node
                        .local()
                        .owner
                        .core(setup::mapping().child)
                        .unwrap()
                        .state()
                        .base_index()
                        > 0
                    && node.local().snapshots.as_ref().unwrap().router.is_drained()
            })
        });
    }
    pub fn close(&mut self) {
        self.catch_up();
        for node in &mut self.nodes {
            node.begin_shutdown();
        }
        self.wait(|host| host.nodes.iter().all(ChildNode::is_drained));
        // Rebinding each address as soon as its own node has joined proves
        // the node released it, then holds it for a later reopen.
        for (node, lease) in self.nodes.drain(..).zip(&mut self.leases) {
            cleanup::join(node);
            lease.reclaim();
        }
    }
    pub fn finish(mut self) {
        self.close();
        std::fs::remove_dir_all(self.root).unwrap();
    }
}
