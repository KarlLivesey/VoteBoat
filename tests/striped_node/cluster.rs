// SPDX-License-Identifier: RPL-1.5
use super::{
    cleanup,
    construction::{self, Boat},
    support,
};
use std::{
    net::{SocketAddr, TcpListener, UdpSocket},
    path::PathBuf,
    sync::{
        atomic::{AtomicU64, Ordering},
        Mutex,
    },
    time::{Duration, Instant},
};
use voteboat::{
    application::StateMachine,
    identity::GroupIdentity,
    native::{connect::NativePeerProtocol, log_store::striped::FileStripedIo},
    raft::Role,
    runtime::*,
};
pub static HISTORY: Mutex<()> = Mutex::new(());
pub fn serial() -> std::sync::MutexGuard<'static, ()> {
    support::serial::serial(&HISTORY)
}
static NEXT: AtomicU64 = AtomicU64::new(1);
pub struct Cluster {
    pub nodes: Vec<Boat>,
    pub root: PathBuf,
    pub group: GroupIdentity,
    pub addresses: [SocketAddr; 3],
    ports: support::ports::PortLease,
    pub protocol: NativePeerProtocol,
    clock: Instant,
}
impl Cluster {
    pub fn new(protocol: NativePeerProtocol) -> Self {
        let root = std::env::temp_dir().join(format!(
            "voteboat-striped-node-{}-{}",
            std::process::id(),
            NEXT.fetch_add(1, Ordering::Relaxed)
        ));
        std::fs::create_dir(&root).unwrap();
        for id in 1..=3 {
            std::fs::create_dir(root.join(id.to_string())).unwrap();
        }
        let ports = support::ports::PortLease::acquire(3);
        let mut host = Self {
            nodes: Vec::new(),
            root,
            group: support::group(318),
            addresses: ports.addresses(),
            ports,
            protocol,
            clock: Instant::now(),
        };
        host.ports.handoff();
        host.open(false);
        host
    }
    pub fn lanes(&self, id: u64) -> Vec<PathBuf> {
        (0..3)
            .map(|lane| self.root.join(id.to_string()).join(format!("lane{lane}")))
            .collect()
    }
    pub fn now(&self) -> MonoTime {
        MonoTime(self.clock.elapsed().as_millis() as u64)
    }
    pub fn open(&mut self, recover: bool) {
        assert!(self.nodes.is_empty());
        self.clock = Instant::now();
        self.nodes = (1..=3)
            .map(|id| construction::open(self, id, recover))
            .collect();
        for id in 1..=3 {
            assert!(
                FileStripedIo::open(self.lanes(id)).is_err(),
                "worker holds lane locks"
            );
        }
    }
    pub fn campaign(&mut self) {
        self.nodes[0]
            .control(self.group, NodeControl::Campaign)
            .unwrap();
        self.wait(|h| h.leader().is_some());
    }
    pub fn leader(&self) -> Option<usize> {
        self.nodes.iter().position(|n| {
            let core = n.local().owner.core(self.group).unwrap();
            core.role() == Role::Leader
                && !core.has_pending_dependency()
                && core.state().term_at(core.state().commit_index)
                    == Some(core.state().hard_state.term)
                && n.local().applications[&self.group].applied_index() >= core.state().commit_index
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
                "striped native history timed out at {}",
                self.root.display()
            );
        }
    }
    pub fn converge(&mut self, value: i64) {
        self.wait(|h| {
            h.nodes.iter().all(|n| {
                let app = &n.local().applications[&h.group];
                app.read_applied(app.applied_index()).unwrap() == value
                    && app.applied_index()
                        == n.local().owner.core(h.group).unwrap().state().commit_index
            })
        });
    }
    pub fn checkpoint(&mut self) -> Vec<u64> {
        for n in &mut self.nodes {
            n.control(self.group, NodeControl::Checkpoint).unwrap();
        }
        self.wait(|h| {
            h.nodes.iter().all(|n| {
                n.checkpoints().pending.is_empty()
                    && n.local().owner.core(h.group).unwrap().state().base_index() > 0
                    && n.local().snapshots.as_ref().unwrap().router.is_drained()
            })
        });
        self.nodes
            .iter()
            .map(|n| {
                n.local()
                    .owner
                    .core(self.group)
                    .unwrap()
                    .state()
                    .base_index()
            })
            .collect()
    }
    pub fn close(&mut self) {
        for n in &mut self.nodes {
            n.begin_shutdown();
        }
        self.wait(|h| h.nodes.iter().all(Boat::is_drained));
        for node in self.nodes.drain(..) {
            cleanup::join(node, self.group);
        }
        for id in 1..=3 {
            drop(FileStripedIo::open(self.lanes(id)).unwrap());
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
