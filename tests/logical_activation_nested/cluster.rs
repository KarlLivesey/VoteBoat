// SPDX-License-Identifier: RPL-1.5
// Copyright (c) 2026 Karl Livesey
use super::{
    cleanup,
    harness::{self, FixtureClock, Observed, SimClock},
    setup, support,
};
use std::{
    net::{SocketAddr, TcpListener, UdpSocket},
    path::PathBuf,
    sync::{
        atomic::{AtomicU64, Ordering},
        Arc, Mutex,
    },
    time::Duration,
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
#[derive(Clone, Copy, Debug)]
enum NativeWaitStage {
    Election,
    CheckpointCatchUp,
    CheckpointPublished,
    CloseCatchUp,
    CloseDrained,
}
pub static HISTORY: Mutex<()> = Mutex::new(());
pub fn serial() -> std::sync::MutexGuard<'static, ()> {
    support::serial::serial(&HISTORY)
}
static NEXT: AtomicU64 = AtomicU64::new(1);
pub struct Cluster {
    pub nodes: Vec<ChildNode>,
    pub slot: usize,
    pub representative: voteboat::identity::NodeId,
    pub clock: SimClock,
    root: PathBuf,
    addresses: [SocketAddr; 3],
    ports: support::ports::PortLease,
    protocol: NativePeerProtocol,
}
impl Cluster {
    pub fn new(protocol: NativePeerProtocol, slot: usize, clock: &SimClock) -> Self {
        let root = std::env::temp_dir().join(format!(
            "voteboat-logical-nested-child-{}-{}",
            std::process::id(),
            NEXT.fetch_add(1, Ordering::Relaxed)
        ));
        std::fs::create_dir(&root).unwrap();
        let ports = support::ports::PortLease::acquire(3);
        let mut host = Self {
            nodes: Vec::new(),
            slot,
            representative: support::node(slot as u64 * 3 + 1),
            clock: clock.clone(),
            root,
            addresses: ports.addresses(),
            ports,
            protocol,
        };
        host.ports.handoff();
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
            store: support::identity(u128::from(id) + 1000),
            bootstrap: setup::bottom(self.slot),
            listen: self.addresses[setup::certificate_slot(id) as usize - 1],
            peers: setup::physical_ids(self.slot)
                .filter(|peer| *peer != id)
                .map(|peer| {
                    (
                        support::node(peer),
                        NativeStartupPeer {
                            address: self.addresses[setup::certificate_slot(peer) as usize - 1],
                            certificate: setup::certificate(setup::certificate_slot(peer)).to_vec(),
                            server_name: format!(
                                "node{}.voteboat.test",
                                setup::certificate_slot(peer)
                            ),
                        },
                    )
                })
                .collect(),
            tls: support::tls::configuration(setup::certificate_slot(id))
                .with_wire_version(8)
                .unwrap(),
            entropy_seed: id * 17,
            limits: NodeLimits::default(),
            ready_visit_items: 16,
        }
    }
    pub fn open(&mut self, mode: NativeOpenMode) {
        Self::open_all(std::slice::from_mut(self), mode);
    }
    pub fn open_before_election(&mut self, mode: NativeOpenMode) {
        assert!(self.nodes.is_empty());

        self.nodes = setup::physical_ids(self.slot)
            .map(|id| {
                let mut parts = self
                    .config(id, mode)
                    .prepare_for_discovery_with_log_limits(
                        self.protocol,
                        NativeTimingProfile::Throughput.timers(),
                        voteboat::log::LogLimits {
                            max_command_bytes: setup::lower_limits().max_command_bytes,
                            ..Default::default()
                        },
                        setup::lower_app(self.slot),
                        Arc::new(ThreadWake::current()),
                        self.now(),
                    )
                    .unwrap_or_else(|error| {
                        panic!("actual logical child startup: {:?}", error.reason)
                    });
                let authorization = parts.local.owner.authorization();
                parts.local.clients = ClientRouter::new(
                    parts.local.clients.binding(),
                    ClientRouterLimits {
                        command_bytes: setup::lower_limits().max_command_bytes,
                        ..Default::default()
                    },
                )
                .unwrap();
                parts.local.reads = ReadRequests::new(
                    parts.local.reads.binding(),
                    setup::read_limits(),
                    ReadRouter::new(
                        ReadRouterBinding::new(
                            authorization,
                            voteboat::identity::ReadRouterGeneration::new(1).unwrap(),
                        ),
                        ReadRouterLimits {
                            result_bytes: 16 * 1024 * 1024,
                            bytes: 64 * 1024 * 1024,
                            ..Default::default()
                        },
                    )
                    .unwrap(),
                )
                .unwrap();
                super::physical::select(&mut parts, id, self.now());
                Node::from_parts(parts, NodeLimits::default(), self.now())
                    .unwrap_or_else(|e| panic!("actual selected native parts {:?}", e.reason))
            })
            .collect();
    }
    pub fn elect(&mut self) {
        Self::elect_all(std::slice::from_mut(self));
    }
    /// Campaign in every cluster, then wait for every leader in lockstep.
    pub fn elect_all(clusters: &mut [Self]) {
        for host in &mut *clusters {
            let selected = host
                .nodes
                .iter()
                .position(|n| {
                    n.local().owner.identity().store.identity
                        == support::identity(1000 + u128::from(host.representative.get()))
                })
                .unwrap();
            host.nodes[selected]
                .control(setup::lower_mapping(host.slot).child, NodeControl::Campaign)
                .unwrap();
        }
        Self::wait_all(clusters, NativeWaitStage::Election, |host| {
            host.leader().is_some()
        });
    }
    /// Reopen every cluster, then elect them together.
    pub fn open_all(clusters: &mut [Self], mode: NativeOpenMode) {
        for host in &mut *clusters {
            host.open_before_election(mode);
        }
        Self::elect_all(clusters);
    }
    pub fn all(&self) -> impl Iterator<Item = &ChildNode> {
        self.nodes.iter()
    }
    pub fn leader(&self) -> Option<usize> {
        self.nodes.iter().position(|node| {
            let core = node
                .local()
                .owner
                .core(setup::lower_mapping(self.slot).child)
                .unwrap();
            core.role() == Role::Leader
                && !core.has_pending_dependency()
                && core.state().term_at(core.state().commit_index)
                    == Some(core.state().hard_state.term)
                && node.local().applications[&setup::lower_mapping(self.slot).child].applied_index()
                    >= core.state().commit_index
        })
    }
    pub fn poll(&mut self) {
        self.poll_with_budget(NodePollBudget::default());
    }
    pub fn poll_with_budget(&mut self, budget: NodePollBudget) {
        Self::poll_all(std::slice::from_mut(self), budget);
    }
    /// One complete round over every node of these clusters; protocol time
    /// moves only if the previous round left them all idle (TR1a's hold rule).
    fn poll_all(clusters: &mut [Self], budget: NodePollBudget) {
        let observed = clusters.iter().fold(Observed::default(), |o, host| {
            host.nodes.iter().fold(o, Observed::node)
        });
        let now = clusters[0].clock.round(observed);
        for node in clusters.iter_mut().flat_map(|host| host.nodes.iter_mut()) {
            node.poll(now, budget).unwrap();
        }
        std::thread::park_timeout(Duration::from_millis(1));
    }
    /// The original 15 s (90 s for checkpoint publication) bounds, now in
    /// protocol time, for each of these clusters. They are polled together:
    /// a cluster left unpolled while shared protocol time advances would see
    /// that gap as one QUIC round-trip sample, and QUIC's close period is three
    /// probe timeouts derived from it, so a sequential close made the last
    /// cluster's drain depend on how long the others took. A close drains by
    /// polling to quiescence: the transports' own close timers then fire after
    /// the same protocol interval on any host.
    fn wait_all(
        clusters: &mut [Self],
        stage: NativeWaitStage,
        mut done: impl FnMut(&Self) -> bool,
    ) {
        let clock = clusters[0].clock.clone();
        let started = clock.now();
        let end = harness::Wait::new(
            &clock,
            if matches!(stage, NativeWaitStage::CheckpointPublished) {
                90_000
            } else {
                harness::LIVENESS_MS
            },
        );
        while let Some(pending) = clusters.iter().position(|host| !done(host)) {
            Self::poll_all(clusters, NodePollBudget::default());
            let within = end.within(&clock);
            let peers = if within {
                Vec::new()
            } else {
                clusters[pending].peer_drain()
            };
            let host = &clusters[pending];
            assert!(
                within,
                "native logical child timeout: {}; stage={stage:?}; slot={}; group={:?}; now={:?}; elapsed_ms={}; clock={}; original_nodes={:?}; peer_drain={peers:?}",
                host.root.display(),
                host.slot,
                setup::bottom(host.slot).group,
                host.now(),
                host.now().0 - started.0,
                clock.describe(),
                super::cluster_inspection::retained(host.slot, &host.nodes),
            );
        }
    }
    /// Failure-only: each node's per-peer connection drain state.
    fn peer_drain(&mut self) -> Vec<String> {
        let ids = setup::physical_ids(self.slot).collect::<Vec<_>>();
        self.nodes
            .iter_mut()
            .map(|node| {
                let statuses = ids
                    .iter()
                    .filter_map(|id| node.peer_drain_status(support::node(*id)))
                    .collect::<Vec<_>>();
                format!("{statuses:?}")
            })
            .collect()
    }
    fn catch_up_all(clusters: &mut [Self], stage: NativeWaitStage) {
        Self::wait_all(clusters, stage, |host| {
            let group = setup::lower_mapping(host.slot).child;
            host.leader().is_some_and(|leader| {
                let commit = host.nodes[leader]
                    .local()
                    .owner
                    .core(group)
                    .unwrap()
                    .state()
                    .commit_index;
                host.all()
                    .all(|node| node.local().applications[&group].applied_index() >= commit)
            })
        });
    }
    pub fn checkpoint(&mut self) {
        Self::checkpoint_all(std::slice::from_mut(self));
    }
    pub fn checkpoint_all(clusters: &mut [Self]) {
        Self::catch_up_all(clusters, NativeWaitStage::CheckpointCatchUp);
        for host in &mut *clusters {
            let group = setup::lower_mapping(host.slot).child;
            for node in host.nodes.iter_mut() {
                node.control(group, NodeControl::Checkpoint).unwrap();
            }
        }
        Self::wait_all(clusters, NativeWaitStage::CheckpointPublished, |host| {
            let group = setup::lower_mapping(host.slot).child;
            host.all().all(|node| {
                node.checkpoints().pending.is_empty()
                    && node.local().owner.core(group).unwrap().state().base_index() > 0
                    && node.local().snapshots.as_ref().unwrap().router.is_drained()
            })
        });
    }
    pub fn close(&mut self) {
        Self::close_all(std::slice::from_mut(self));
    }
    pub fn close_all(clusters: &mut [Self]) {
        Self::catch_up_all(clusters, NativeWaitStage::CloseCatchUp);
        for node in clusters.iter_mut().flat_map(|host| host.nodes.iter_mut()) {
            node.begin_shutdown();
        }
        Self::wait_all(clusters, NativeWaitStage::CloseDrained, |host| {
            host.all().all(ChildNode::is_drained)
        });
        for host in clusters {
            for node in host.nodes.drain(..) {
                cleanup::join(node);
            }
            host.rebind();
        }
    }
    pub fn rebind(&self) {
        for address in self.addresses {
            drop((
                TcpListener::bind(address).unwrap(),
                UdpSocket::bind(address).unwrap(),
            ));
        }
    }
    pub fn finish_all(mut clusters: Vec<Self>) {
        Self::close_all(&mut clusters);
        for host in clusters {
            std::fs::remove_dir_all(host.root).unwrap();
        }
    }
}
