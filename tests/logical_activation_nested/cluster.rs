// SPDX-License-Identifier: RPL-1.5
// Copyright (c) 2026 Karl Livesey
use super::{cleanup, setup, support};
use std::{
    net::{SocketAddr, TcpListener, UdpSocket},
    path::PathBuf,
    sync::{
        atomic::{AtomicU64, AtomicUsize, Ordering},
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
#[derive(Clone, Copy, Debug)]
enum NativeWaitStage {
    Election,
    CheckpointCatchUp,
    CheckpointPublished,
    CloseCatchUp,
    CloseDrained,
}
pub static HISTORY: Mutex<()> = Mutex::new(());
static NEXT: AtomicU64 = AtomicU64::new(1);
static LISTENER: AtomicUsize = AtomicUsize::new(0);
pub struct Cluster {
    pub nodes: Vec<ChildNode>,
    pub slot: usize,
    pub representative: voteboat::identity::NodeId,
    pub clock: Instant,
    root: PathBuf,
    addresses: [SocketAddr; 3],
    protocol: NativePeerProtocol,
}
impl Cluster {
    pub fn new(protocol: NativePeerProtocol, slot: usize, clock: Instant) -> Self {
        let root = std::env::temp_dir().join(format!(
            "voteboat-logical-nested-child-{}-{}",
            std::process::id(),
            NEXT.fetch_add(1, Ordering::Relaxed)
        ));
        std::fs::create_dir(&root).unwrap();
        let mut host = Self {
            nodes: Vec::new(),
            slot,
            representative: support::node(slot as u64 * 3 + 1),
            clock,
            root,
            addresses: addresses(),
            protocol,
        };
        host.open(NativeOpenMode::Create);
        host
    }
    pub fn now(&self) -> MonoTime {
        MonoTime(self.clock.elapsed().as_millis() as u64)
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
            append_window: None,
        }
    }
    pub fn open(&mut self, mode: NativeOpenMode) {
        self.open_before_election(mode);
        self.elect();
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
        let selected = self
            .nodes
            .iter()
            .position(|n| {
                n.local().owner.identity().store.identity
                    == support::identity(1000 + u128::from(self.representative.get()))
            })
            .unwrap();
        self.nodes[selected]
            .control(setup::lower_mapping(self.slot).child, NodeControl::Campaign)
            .unwrap();
        self.wait(NativeWaitStage::Election, |host| host.leader().is_some());
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
        let now = self.now();
        for node in self.nodes.iter_mut() {
            node.poll(now, budget).unwrap();
        }
        std::thread::park_timeout(Duration::from_millis(1));
    }
    fn wait(&mut self, stage: NativeWaitStage, mut done: impl FnMut(&Self) -> bool) {
        let started = Instant::now();
        let end = started
            + Duration::from_secs(if matches!(stage, NativeWaitStage::CheckpointPublished) {
                90
            } else {
                15
            });
        while !done(self) {
            self.poll();
            assert!(
                Instant::now() < end,
                "native logical child timeout: {}; stage={stage:?}; slot={}; group={:?}; now={:?}; elapsed={:?}; original_nodes={:?}",
                self.root.display(),
                self.slot,
                setup::bottom(self.slot).group,
                self.now(),
                started.elapsed(),
                super::cluster_inspection::retained(self.slot, &self.nodes),
            );
        }
    }
    fn catch_up(&mut self, stage: NativeWaitStage) {
        let group = setup::lower_mapping(self.slot).child;
        self.wait(stage, |host| {
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
        let group = setup::lower_mapping(self.slot).child;
        self.catch_up(NativeWaitStage::CheckpointCatchUp);
        for node in self.nodes.iter_mut() {
            node.control(group, NodeControl::Checkpoint).unwrap();
        }
        self.wait(NativeWaitStage::CheckpointPublished, |host| {
            host.all().all(|node| {
                node.checkpoints().pending.is_empty()
                    && node.local().owner.core(group).unwrap().state().base_index() > 0
                    && node.local().snapshots.as_ref().unwrap().router.is_drained()
            })
        });
    }
    pub fn close(&mut self) {
        self.catch_up(NativeWaitStage::CloseCatchUp);
        for node in self.nodes.iter_mut() {
            node.begin_shutdown();
        }
        self.wait(NativeWaitStage::CloseDrained, |host| {
            host.all().all(ChildNode::is_drained)
        });
        for node in self.nodes.drain(..) {
            cleanup::join(node);
        }
        self.rebind();
    }
    pub fn rebind(&self) {
        for address in self.addresses {
            drop((
                TcpListener::bind(address).unwrap(),
                UdpSocket::bind(address).unwrap(),
            ));
        }
    }
    pub fn finish(mut self) {
        self.close();
        std::fs::remove_dir_all(self.root).unwrap();
    }
}
fn addresses() -> [SocketAddr; 3] {
    let held = (0..3)
        .map(|_| {
            (0..32)
                .find_map(|_| {
                    let port = 20000 + LISTENER.fetch_add(1, Ordering::Relaxed) % 10000;
                    let tcp =
                        TcpListener::bind((std::net::Ipv4Addr::LOCALHOST, port as u16)).ok()?;
                    let udp = UdpSocket::bind(tcp.local_addr().ok()?).ok()?;
                    Some((tcp, udp))
                })
                .expect("owned TCP and UDP listeners")
        })
        .collect::<Vec<_>>();
    held.iter()
        .map(|(tcp, _)| tcp.local_addr().unwrap())
        .collect::<Vec<_>>()
        .try_into()
        .unwrap()
}
