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
    snapshot_worker::SnapshotWorker,
};
pub type ChildNode = NativeNode<LogicalApplication, NativeServiceConnector>;
#[derive(Clone, Copy, Debug)]
enum NativeWaitStage {
    Election,
    CheckpointCatchUp,
    CheckpointPublished,
    CloseCatchUp,
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
        assert!(self.nodes.is_empty());

        self.nodes = setup::physical_ids(self.slot)
            .map(|id| self.open_node(id, mode))
            .collect();
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
    fn open_node(&self, id: u64, mode: NativeOpenMode) -> ChildNode {
        let config = self.config(id, mode);
        let limits = config.limits;
        let now = self.now();
        let mut parts = config
            .prepare_for_discovery_with_log_limits(
                self.protocol,
                NativeTimingProfile::Throughput.timers(),
                setup::physical_log_limits(),
                setup::lower_app(self.slot),
                Arc::new(ThreadWake::current()),
                now,
            )
            .unwrap_or_else(|error| panic!("actual logical child startup: {:?}", error.reason));
        assert!(parts.local.clients.is_drained() && parts.local.reads.is_drained());
        parts.local.clients = ClientRouter::new(
            ClientRouterBinding::new(
                parts.local.owner.authorization(),
                voteboat::identity::ClientRouterGeneration::new(2).unwrap(),
            ),
            setup::client_limits(setup::lower_limits()),
        )
        .unwrap();
        let (invocations, execution) = setup::read_limits(setup::lower_limits());
        let reads = ReadRouter::new(
            ReadRouterBinding::new(
                parts.local.owner.authorization(),
                voteboat::identity::ReadRouterGeneration::new(2).unwrap(),
            ),
            execution,
        )
        .unwrap();
        parts.local.reads = ReadRequests::new(
            ReadInvocationBinding::new(
                parts.local.owner.authorization(),
                voteboat::identity::ReadInvocationGeneration::new(2).unwrap(),
            ),
            invocations,
            reads,
        )
        .map_err(|e| e.reason)
        .unwrap();
        self.physical_peers(&mut parts, id, now);
        NativeNode::from_parts(parts, limits, now)
            .unwrap_or_else(|error| panic!("actual logical child assembly: {:?}", error.reason))
    }
    fn physical_peers(
        &self,
        parts: &mut voteboat::native::node::NativeNodeParts<
            LogicalApplication,
            NativeServiceConnector,
        >,
        id: u64,
        now: MonoTime,
    ) {
        use voteboat::{
            outbound::OutboundQueue,
            secure::LocalIdentity,
            transport::{PeerRoster, PeerRosterConfig, PeerRosterLimits},
        };
        let transport = setup::physical_transport_limits();
        let owner = parts.local.owner.identity();
        let first = owner.store.session.get().checked_mul(10000).unwrap();
        assert!(parts.local.outbound.is_drained());
        let mut outbound = parts.local.outbound.binding();
        outbound.generation = voteboat::identity::OutboundGeneration::new(2).unwrap();
        parts.local.outbound = voteboat::native::outbound::NativeOutbound::new(
            outbound,
            setup::physical_outbound_limits(),
        )
        .unwrap();
        let peers = parts.peers.as_mut().unwrap();
        peers.ingress = IngressRouter::new(
            IngressBinding {
                owner,
                local: LocalIdentity {
                    node: support::node(id),
                    store: owner.store,
                },
                generation: voteboat::identity::IngressGeneration::new(2).unwrap(),
            },
            setup::physical_ingress_limits(),
        )
        .unwrap();
        assert_eq!(peers.roster.usage().connections, 0);
        peers.factory = voteboat::native::transport::NativeTransportFactory::new(
            setup::physical_wire_codec(),
            transport,
        )
        .unwrap();
        peers.roster = PeerRoster::new(
            PeerRosterConfig {
                local: LocalIdentity {
                    node: support::node(id),
                    store: owner.store,
                },
                outbound: parts.local.outbound.binding(),
                first_generation: voteboat::identity::SecureSessionGeneration::new(first).unwrap(),
                last_generation: voteboat::identity::SecureSessionGeneration::new(
                    first.checked_add(9999).unwrap(),
                )
                .unwrap(),
                wire_version: 8,
                limits: PeerRosterLimits::default(),
                transport_limits: transport,
            },
            setup::bottom(self.slot)
                .voter_stores
                .into_iter()
                .filter(|(node, _)| *node != support::node(id))
                .collect(),
            now,
        )
        .unwrap();
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
        let now = self.now();
        for node in self.nodes.iter_mut() {
            node.poll(now, NodePollBudget::default()).unwrap();
        }
        std::thread::park_timeout(Duration::from_millis(1));
    }
    fn wait(&mut self, stage: NativeWaitStage, mut done: impl FnMut(&Self) -> bool) {
        let started = Instant::now();
        // Publication checks durability using the same bounded watchdog as
        // the original upper checkpoint history.
        let allowance = match stage {
            NativeWaitStage::CheckpointPublished => Duration::from_secs(90),
            _ => Duration::from_secs(15),
        };
        let end = started + allowance;
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
        let frontiers = self
            .all()
            .map(|node| node.local().applications[&group].applied_index())
            .collect::<Vec<_>>();
        for node in self.nodes.iter_mut() {
            node.control(group, NodeControl::Checkpoint).unwrap();
        }
        self.wait(NativeWaitStage::CheckpointPublished, |host| {
            host.all().zip(&frontiers).all(|(node, &frontier)| {
                let state = node.local().owner.core(group).unwrap().state();
                node.checkpoints().pending.is_empty()
                    && state.base_index() >= frontier
                    && state
                        .snapshot
                        .is_some_and(|snapshot| snapshot.index >= frontier)
                    && node.local().snapshots.as_ref().unwrap().router.is_drained()
                    && node.local().snapshots.as_ref().unwrap().worker.is_drained()
            })
        });
    }
    pub fn close(&mut self) {
        self.catch_up(NativeWaitStage::CloseCatchUp);
        for node in self.nodes.iter_mut() {
            node.begin_shutdown();
        }
        let slot = self.slot;
        super::protocol_close::drive(
            &mut self.nodes,
            &self.clock,
            |nodes, clock| {
                let now = MonoTime(clock.elapsed().as_millis() as u64);
                for node in nodes {
                    node.poll(now, NodePollBudget::default()).unwrap();
                }
            },
            |nodes, _| {
                eprintln!(
                    "native logical child close slot={slot}; original_nodes={:?}",
                    super::cluster_inspection::retained(slot, nodes),
                );
            },
        );
        for node in self.nodes.drain(..) {
            cleanup::join(node);
        }
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
