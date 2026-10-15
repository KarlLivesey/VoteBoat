// SPDX-License-Identifier: RPL-1.5
// Copyright (c) 2026 Karl Livesey
use super::support;
use std::{
    collections::BTreeMap,
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
    identity::*,
    native::{connect::*, node::NativeNode, startup::*, worker::ThreadWake},
    raft::Role,
    runtime::*,
    transaction::*,
};

pub type TransferNode = NativeNode<TransferApplication, NativeServiceConnector>;
pub static HISTORY: Mutex<()> = Mutex::new(());
static NEXT: AtomicU64 = AtomicU64::new(1);
static LISTENER: AtomicUsize = AtomicUsize::new(0);

pub fn groups() -> [GroupIdentity; 3] {
    [77, 78, 79].map(|id| GroupIdentity {
        id: GroupId::new(id).unwrap(),
        incarnation: GroupIncarnation::new(4).unwrap(),
    })
}
pub fn profile() -> TransferProfile {
    let [coordinator, left, right] = groups();
    TransferProfile::new(coordinator, [left, right]).unwrap()
}
pub fn account(slot: usize) -> AccountId {
    AccountId::new(10 + slot as u64).unwrap()
}
pub fn spec(id: u128, amount: i64, reverse: bool) -> TransferSpec {
    let phase = |offset| OperationId::new(id * 16 + offset).unwrap();
    let ids = PhaseIds::new(
        [phase(1), phase(2), phase(3), phase(4)],
        [
            [phase(5), phase(6), phase(7)],
            [phase(8), phase(9), phase(10)],
        ],
    )
    .unwrap();
    let refs = [0, 1].map(|i| AccountRef::new(profile().participants()[i], account(i)));
    let (debit, credit) = if reverse {
        (refs[1], refs[0])
    } else {
        (refs[0], refs[1])
    };
    TransferSpec::new(
        TransactionId::new(id).unwrap(),
        profile(),
        debit,
        credit,
        amount,
        ids,
    )
    .unwrap()
}
fn certificate(id: u64) -> &'static [u8] {
    match id {
        1 => include_bytes!("../fixtures/tls/node1.der"),
        2 => include_bytes!("../fixtures/tls/node2.der"),
        3 => include_bytes!("../fixtures/tls/node3.der"),
        _ => unreachable!(),
    }
}

pub struct Cluster {
    pub nodes: Vec<TransferNode>,
    pub clock: Instant,
    root: PathBuf,
    addresses: [SocketAddr; 3],
    protocol: NativePeerProtocol,
    capacity: usize,
    opening: [i64; 2],
}
impl Cluster {
    pub fn new(protocol: NativePeerProtocol, capacity: usize, opening: [i64; 2]) -> Self {
        let root = std::env::temp_dir().join(format!(
            "voteboat-transfer-driver-{}-{}",
            std::process::id(),
            NEXT.fetch_add(1, Ordering::Relaxed),
        ));
        std::fs::create_dir(&root).unwrap();
        // Keep all listener candidates outside both hosts' ephemeral dial pool.
        let reservations = (0..3)
            .map(|_| {
                (0..32)
                    .find_map(|_| {
                        let port = 20000 + LISTENER.fetch_add(1, Ordering::Relaxed) % 10000;
                        let tcp =
                            TcpListener::bind((std::net::Ipv4Addr::LOCALHOST, port as u16)).ok()?;
                        let udp = UdpSocket::bind(tcp.local_addr().ok()?).ok()?;
                        Some((tcp, udp))
                    })
                    .expect("owned native TCP/UDP listener")
            })
            .collect::<Vec<_>>();
        let addresses = reservations
            .iter()
            .map(|(tcp, _)| tcp.local_addr().unwrap())
            .collect::<Vec<_>>()
            .try_into()
            .unwrap();
        drop(reservations);
        let mut host = Self {
            nodes: Vec::new(),
            clock: Instant::now(),
            root,
            addresses,
            protocol,
            capacity,
            opening,
        };
        host.open(NativeOpenMode::Create);
        host
    }
    fn config(&self, id: u64, mode: NativeOpenMode) -> NativeMultiStartup {
        let bootstraps = groups().map(|group| {
            let mut b = support::bootstrap(group.id.get(), 3);
            b.group = group;
            b
        });
        NativeMultiStartup {
            startup: NativeStartup {
                directory: self.root.join(id.to_string()),
                mode,
                node: support::node(id),
                store: support::identity(id.into()),
                bootstrap: bootstraps[0].clone(),
                listen: self.addresses[id as usize - 1],
                peers: (1..=3)
                    .filter(|peer| *peer != id)
                    .map(|peer| {
                        (
                            support::node(peer),
                            NativeStartupPeer {
                                address: self.addresses[peer as usize - 1],
                                certificate: certificate(peer).to_vec(),
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
                append_window: None,
            },
            additional_groups: bootstraps[1..].to_vec(),
            provisioned_stores: bootstraps[0].voter_stores.clone(),
        }
    }
    fn applications(&self) -> BTreeMap<GroupIdentity, TransferApplication> {
        let limits = TransferLimits::new(self.capacity, 1).unwrap();
        let p = profile();
        let mut apps = BTreeMap::from([(
            p.coordinator(),
            TransferApplication::coordinator(p.coordinator(), p, limits).unwrap(),
        )]);
        for (slot, group) in p.participants().into_iter().enumerate() {
            apps.insert(
                group,
                TransferApplication::participant(
                    group,
                    p,
                    &[(account(slot), self.opening[slot])],
                    limits,
                )
                .unwrap(),
            );
        }
        apps
    }
    fn open_node(&self, id: u64, mode: NativeOpenMode) -> TransferNode {
        self.config(id, mode)
            .open(
                self.protocol,
                NativeTimingProfile::Throughput.timers(),
                self.applications(),
                Arc::new(ThreadWake::current()),
                self.now(),
            )
            .unwrap()
    }
    pub fn open(&mut self, mode: NativeOpenMode) {
        assert!(self.nodes.is_empty());
        self.clock = Instant::now();
        self.nodes = (1..=3).map(|id| self.open_node(id, mode)).collect();
        // Distinct leaders ensure every phase uses its actual scoped group path.
        for (slot, group) in groups().into_iter().enumerate() {
            self.nodes[slot]
                .control(group, NodeControl::Campaign)
                .unwrap();
        }
        self.wait(|h| groups().into_iter().all(|g| h.leader(g).is_some()));
    }
    pub fn now(&self) -> MonoTime {
        MonoTime(self.clock.elapsed().as_millis() as u64)
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
                "native transfer fixture timed out at {}",
                self.root.display()
            );
        }
    }
    pub fn leader(&self, group: GroupIdentity) -> Option<usize> {
        self.nodes.iter().position(|n| {
            let c = n.local().owner.core(group).unwrap();
            c.role() == Role::Leader
                && !c.has_pending_dependency()
                && c.state().term_at(c.state().commit_index) == Some(c.state().hard_state.term)
                && n.local().applications[&group].applied_index() >= c.state().commit_index
        })
    }
    pub fn read(&mut self, group: GroupIdentity, query: TransferQuery) -> TransferRead {
        let end = Instant::now() + Duration::from_secs(15);
        loop {
            self.wait(|h| h.leader(group).is_some());
            let index = self.leader(group).unwrap();
            let ticket = self.nodes[index].read(group, query).unwrap();
            loop {
                self.poll();
                if let Some(output) = self.nodes[index].poll_read() {
                    assert_eq!(output.ticket(), ticket);
                    match self.nodes[index].complete_read(output).unwrap() {
                        ReadOutcome::Read {
                            barrier,
                            result: Ok(result),
                        } => {
                            assert_eq!(barrier.group(), group);
                            assert_eq!(barrier.request(), ticket.request);
                            return result;
                        }
                        ReadOutcome::Read { result: Err(e), .. } => {
                            panic!("actual transfer query: {e:?}")
                        }
                        ReadOutcome::NotRead(_) | ReadOutcome::Unavailable(_) => break,
                    }
                }
                assert!(
                    Instant::now() < end,
                    "native quorum transfer read timed out"
                );
            }
        }
    }
    pub fn status(&mut self, group: GroupIdentity, id: TransactionId) -> Option<TransferStatus> {
        match self.read(group, TransferQuery::Status(id)) {
            TransferRead::Status(status) => status.map(|s| *s),
            TransferRead::Account(_) => panic!("original status query result mismatch"),
        }
    }
    pub fn balance(&mut self, slot: usize) -> AccountRead {
        match self.read(
            profile().participants()[slot],
            TransferQuery::Account(account(slot)),
        ) {
            TransferRead::Account(value) => value,
            TransferRead::Status(_) => panic!("original account query result mismatch"),
        }
    }
    pub fn checkpoint(&mut self) {
        self.wait(|h| {
            h.nodes.iter().all(|node| {
                groups().into_iter().all(|g| {
                    h.leader(g).is_some_and(|leader| {
                        let commit = h.nodes[leader]
                            .local()
                            .owner
                            .core(g)
                            .unwrap()
                            .state()
                            .commit_index;
                        node.local().applications[&g].applied_index() >= commit
                    })
                })
            })
        });
        for node in &mut self.nodes {
            for group in groups() {
                node.control(group, NodeControl::Checkpoint).unwrap();
            }
        }
        self.wait(|h| {
            h.nodes.iter().all(|n| {
                n.checkpoints().pending.is_empty()
                    && groups()
                        .into_iter()
                        .all(|g| n.local().owner.core(g).unwrap().state().base_index() > 0)
                    && n.local().snapshots.as_ref().unwrap().router.is_drained()
            })
        });
    }
    pub fn replace_coordinator_leader(&mut self) {
        let old = self.leader(profile().coordinator()).unwrap();
        let id = self.nodes[old]
            .local()
            .owner
            .core(profile().coordinator())
            .unwrap()
            .local_node()
            .get();
        let mut retired = self.nodes.remove(old);
        retired.begin_shutdown();
        let end = Instant::now() + Duration::from_secs(15);
        while !retired.is_drained() {
            retired.poll(self.now(), NodePollBudget::default()).unwrap();
            self.poll();
            assert!(Instant::now() < end, "original leader must actually drain");
        }
        join(retired);
        self.nodes[0]
            .control(profile().coordinator(), NodeControl::Campaign)
            .unwrap();
        self.wait(|h| h.leader(profile().coordinator()).is_some());
        assert_ne!(
            self.nodes[self.leader(profile().coordinator()).unwrap()]
                .local()
                .owner
                .core(profile().coordinator())
                .unwrap()
                .local_node()
                .get(),
            id
        );
        let replacement = self.open_node(id, NativeOpenMode::Recover);
        self.nodes.push(replacement);
        self.nodes.sort_by_key(|n| {
            n.local()
                .owner
                .core(profile().coordinator())
                .unwrap()
                .local_node()
        });
        self.wait(|h| {
            h.nodes
                .iter()
                .all(|n| n.local().owner.groups().count() == 3)
        });
    }
    pub fn close(&mut self) {
        for node in &mut self.nodes {
            node.begin_shutdown();
        }
        self.wait(|h| h.nodes.iter().all(TransferNode::is_drained));
        for node in self.nodes.drain(..) {
            join(node);
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
fn join(node: TransferNode) {
    let mut parts = node
        .into_parts()
        .unwrap_or_else(|_| panic!("actual native Node not drained"));
    let mut dialer = parts
        .peers
        .take()
        .unwrap()
        .connector
        .into_dialer()
        .unwrap_or_else(|_| panic!("actual connector not drained"));
    let mut snapshots = parts.local.snapshots.take().unwrap();
    let end = Instant::now() + Duration::from_secs(10);
    let (mut wal, mut snapshot) = (false, false);
    loop {
        let dial = dialer.as_mut().is_none_or(|d| d.try_finish().unwrap());
        if !wal {
            wal = parts.local.persistence.try_reclaim().unwrap().is_some();
        }
        if !snapshot {
            snapshot = snapshots.worker.try_reclaim().unwrap().is_some();
        }
        if dial && wal && snapshot {
            return;
        }
        assert!(
            Instant::now() < end,
            "actual native transaction workers must join"
        );
        std::thread::park_timeout(Duration::from_millis(1));
    }
}
