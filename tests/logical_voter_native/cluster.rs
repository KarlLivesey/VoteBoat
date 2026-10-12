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
pub static HISTORY: Mutex<()> = Mutex::new(());
static NEXT: AtomicU64 = AtomicU64::new(1);
static LISTENER: AtomicUsize = AtomicUsize::new(0);
pub struct Cluster {
    pub nodes: Vec<ChildNode>,
    pub clock: Instant,
    root: PathBuf,
    addresses: [SocketAddr; 3],
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
        let root = std::env::temp_dir().join(format!(
            "voteboat-logical-child-{}-{}",
            std::process::id(),
            NEXT.fetch_add(1, Ordering::Relaxed)
        ));
        std::fs::create_dir(&root).unwrap();
        let mut host = Self {
            nodes: Vec::new(),
            clock: Instant::now(),
            root,
            addresses: addresses(),
            protocol,
            application,
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
        }
    }
    pub fn open(&mut self, mode: NativeOpenMode) {
        assert!(self.nodes.is_empty());
        self.clock = Instant::now();
        self.nodes = (1..=3)
            .map(|id| {
                self.config(id, mode)
                    .open_with_protocol_and_timers(
                        self.protocol,
                        NativeTimingProfile::Throughput.timers(),
                        self.application.clone(),
                        Arc::new(ThreadWake::current()),
                        self.now(),
                    )
                    .unwrap_or_else(|error| {
                        panic!("actual logical child startup: {:?}", error.reason)
                    })
            })
            .collect();
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
        let now = self.now();
        for node in &mut self.nodes {
            node.poll(now, NodePollBudget::default()).unwrap();
        }
        std::thread::park_timeout(Duration::from_millis(1));
    }
    pub fn wait(&mut self, mut done: impl FnMut(&Self) -> bool) {
        let end = Instant::now() + Duration::from_secs(15);
        while !done(self) {
            self.poll();
            assert!(
                Instant::now() < end,
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
