// SPDX-License-Identifier: RPL-1.5
// Copyright (c) 2026 Karl Livesey
use super::*;
use voteboat::{native::connect::*, raft::Role};
#[path = "multi/peer_rotation.rs"]
mod peer_rotation;
type MultiNode = NativeNode<HostApplication, NativeServiceConnector>;
fn groups() -> Vec<Bootstrap> {
    let stores = (1..=3)
        .map(|n| {
            (
                NodeId::new(n).unwrap(),
                StoreIdentity {
                    id: StoreId::new(n.into()).unwrap(),
                    incarnation: StoreIncarnation::new(1).unwrap(),
                },
            )
        })
        .collect::<BTreeMap<_, _>>();
    (77..80)
        .map(|id| Bootstrap {
            group: GroupIdentity {
                id: GroupId::new(id).unwrap(),
                incarnation: GroupIncarnation::new(4).unwrap(),
            },
            configuration: ConfigurationId::new(9).unwrap(),
            policy: Policy::new(
                Tree::Majority(stores.keys().copied().map(Tree::Voter).collect()),
                Limits::default(),
            )
            .unwrap(),
            voter_stores: stores.clone(),
        })
        .collect()
}
fn apps(groups: &[Bootstrap]) -> BTreeMap<GroupIdentity, HostApplication> {
    groups.iter().map(|b| (b.group, app())).collect()
}
fn selected(
    root: &std::path::Path,
    addresses: &[std::net::SocketAddr],
    n: usize,
    mode: NativeOpenMode,
) -> NativeMultiStartup {
    let groups = groups();
    NativeMultiStartup {
        startup: selected_wire_config(root, addresses, &groups[0], n, mode, 8),
        additional_groups: groups[1..].to_vec(),
        provisioned_stores: groups[0].voter_stores.clone(),
    }
}
fn timers() -> TimerConfig {
    TimerConfig {
        heartbeat_ms: 50,
        election_min_ms: 500,
        election_spread_ms: 500,
        expirations_per_poll: 32,
    }
}
struct History {
    root: PathBuf,
    addresses: Vec<std::net::SocketAddr>,
    nodes: Vec<MultiNode>,
    clock: Instant,
    protocol: NativePeerProtocol,
}
impl History {
    fn new(protocol: NativePeerProtocol) -> Self {
        let root = root();
        std::fs::create_dir(&root).unwrap();
        let sockets = (0..3)
            .map(|_| {
                let tcp = std::net::TcpListener::bind("127.0.0.1:0").unwrap();
                let udp = std::net::UdpSocket::bind(tcp.local_addr().unwrap()).unwrap();
                (tcp, udp)
            })
            .collect::<Vec<_>>();
        let addresses = sockets
            .iter()
            .map(|(s, _)| s.local_addr().unwrap())
            .collect();
        drop(sockets);
        let mut h = Self {
            root,
            addresses,
            nodes: vec![],
            clock: Instant::now(),
            protocol,
        };
        h.open(NativeOpenMode::Create);
        h
    }
    fn open(&mut self, mode: NativeOpenMode) {
        self.clock = Instant::now();
        self.nodes = (1..=3)
            .map(|n| {
                selected(&self.root, &self.addresses, n, mode)
                    .open(
                        self.protocol,
                        timers(),
                        apps(&groups()),
                        Arc::new(ThreadWake::current()),
                        MonoTime(0),
                    )
                    .unwrap()
            })
            .collect();
        for node in &self.nodes {
            assert_eq!(node.local().owner.groups().count(), 3);
            for bootstrap in groups() {
                assert_eq!(
                    node.local()
                        .owner
                        .core(bootstrap.group)
                        .unwrap()
                        .storage_binding(),
                    node.local().owner.identity().store
                );
            }
        }
    }
    fn poll(&mut self) {
        let now = MonoTime(self.clock.elapsed().as_millis() as u64);
        for node in &mut self.nodes {
            node.poll(now, NodePollBudget::default()).unwrap();
        }
        std::thread::park_timeout(Duration::from_millis(1));
    }
    fn wait(&mut self, predicate: impl Fn(&Self) -> bool) {
        let start = Instant::now();
        while !predicate(self) {
            self.poll();
            assert!(
                start.elapsed() < Duration::from_secs(15),
                "multi startup timeout: {}",
                self.root.display()
            );
        }
    }
    fn leader(&self, group: GroupIdentity) -> Option<usize> {
        self.nodes.iter().position(|n| {
            let c = n.local().owner.core(group).unwrap();
            c.role() == Role::Leader
                && c.state().term_at(c.state().commit_index) == Some(c.state().hard_state.term)
        })
    }
    fn write(
        &mut self,
        group: GroupIdentity,
        operation: u128,
        delta: i64,
        expected: i64,
        duplicate: bool,
    ) {
        self.wait(|h| h.leader(group).is_some());
        let leader = self.leader(group).unwrap();
        let ticket = self.nodes[leader]
            .propose(ClientRequest {
                group,
                operation: OperationId::new(operation).unwrap(),
                bytes: delta.to_le_bytes().to_vec(),
            })
            .unwrap();
        let start = Instant::now();
        let position = loop {
            self.poll();
            if let Some(result) = self.nodes[leader].poll_client() {
                assert_eq!(result.ticket(), ticket);
                let ClientOutcome::Applied { position, receipt } =
                    self.nodes[leader].complete_client(result).unwrap()
                else {
                    panic!("write lost leadership");
                };
                assert_eq!(receipt.operation.get(), operation);
                assert_eq!(receipt.outcome, CounterOutcome::Value(expected));
                assert_eq!(receipt.duplicate, duplicate);
                break position;
            }
            assert!(start.elapsed() < Duration::from_secs(10));
        };
        self.wait(|h| {
            h.nodes
                .iter()
                .all(|n| n.local().applications[&group].applied_index() >= position.index)
        });
    }
    fn checkpoint(&mut self) {
        for node in &mut self.nodes {
            for b in groups() {
                node.control(b.group, NodeControl::Checkpoint).unwrap();
            }
        }
        self.wait(|h| {
            h.nodes.iter().all(|n| {
                groups()
                    .iter()
                    .all(|b| n.local().owner.core(b.group).unwrap().state().base_index() > 0)
                    && n.local().snapshots.as_ref().unwrap().router.is_drained()
            })
        });
    }
    fn close(&mut self) {
        drain_wire_nodes(std::mem::take(&mut self.nodes));
    }
    fn rejected_recovery(&self, changed: bool) {
        let mut config = selected(&self.root, &self.addresses, 1, NativeOpenMode::Recover);
        let mut applications = apps(&groups());
        if changed {
            config.additional_groups[0].configuration = ConfigurationId::new(10).unwrap();
        } else {
            applications.remove(&config.additional_groups.pop().unwrap().group);
        }
        let mut rejected = config
            .open(
                self.protocol,
                timers(),
                applications,
                Arc::new(ThreadWake::current()),
                MonoTime(0),
            )
            .err()
            .unwrap();
        assert_eq!(rejected.reason.stage, "recovery");
        assert!(rejected
            .application
            .as_ref()
            .is_some_and(|a| a.len() == if changed { 3 } else { 2 }));
        if changed {
            assert!(rejected.application.as_ref().unwrap()[&groups()[0].group].applied_index() > 0);
        }
        assert!(rejected.try_cleanup().unwrap());
    }
}
fn history(protocol: NativePeerProtocol) {
    let mut h = History::new(protocol);
    for (index, b) in groups().iter().enumerate() {
        h.write(
            b.group,
            index as u128 + 1,
            index as i64 + 3,
            index as i64 + 3,
            false,
        );
    }
    h.checkpoint();
    h.close();
    h.rejected_recovery(false);
    h.rejected_recovery(true);
    h.open(NativeOpenMode::Recover);
    for (index, b) in groups().iter().enumerate() {
        h.write(
            b.group,
            index as u128 + 1,
            index as i64 + 3,
            index as i64 + 3,
            true,
        );
        h.write(b.group, index as u128 + 10, 10, index as i64 + 13, false);
    }
    h.close();
    std::fs::remove_dir_all(h.root).unwrap();
}
#[test]
fn native_multi_startup_tcp_shares_one_store_and_recovers_every_group() {
    history(NativePeerProtocol::TcpTls);
}
#[cfg(feature = "quic")]
#[test]
fn native_multi_startup_quic_shares_one_store_and_recovers_every_group() {
    history(NativePeerProtocol::Quic);
}

fn local_multi(path: PathBuf) -> NativeMultiStartup {
    let mut c = config(path, NativeOpenMode::Create);
    c.tls = c.tls.with_wire_version(8).unwrap();
    let mut b = c.bootstrap.clone();
    b.group.id = GroupId::new(78).unwrap();
    NativeMultiStartup {
        provisioned_stores: c.bootstrap.voter_stores.clone(),
        startup: c,
        additional_groups: vec![b],
    }
}
fn local_apps(c: &NativeMultiStartup) -> BTreeMap<GroupIdentity, HostApplication> {
    [
        (c.startup.bootstrap.group, app()),
        (c.additional_groups[0].group, app()),
    ]
    .into()
}
#[test]
fn multi_startup_rejects_invalid_inventory_before_creating_files() {
    for case in 0..5 {
        let root = root();
        let mut c = local_multi(root.clone());
        let mut applications = local_apps(&c);
        match case {
            0 => c.additional_groups[0].group = c.startup.bootstrap.group,
            1 => {
                applications.remove(&c.startup.bootstrap.group);
            }
            2 => c.startup.tls = c.startup.tls.with_wire_version(1).unwrap(),
            3 => c.additional_groups[0].voter_stores.clear(),
            _ => {
                c.additional_groups =
                    vec![c.additional_groups[0].clone(); MAX_NATIVE_STARTUP_GROUPS]
            }
        }
        let mut error = c
            .open(
                NativePeerProtocol::TcpTls,
                timers(),
                applications,
                Arc::new(ThreadWake::current()),
                MonoTime(0),
            )
            .err()
            .unwrap();
        assert!(error.try_cleanup().unwrap());
        assert!(!root.exists());
    }
}
fn failed_cleanup(protocol: NativePeerProtocol) {
    let root = root();
    let tcp = std::net::TcpListener::bind("127.0.0.1:0").unwrap();
    let address = tcp.local_addr().unwrap();
    let udp = std::net::UdpSocket::bind(address).unwrap();
    drop((tcp, udp));
    let mut c = local_multi(root.clone());
    c.startup.listen = address;
    c.startup.limits.replica.leases = 0;
    let applications = local_apps(&c);
    let mut rejected = c
        .open(
            protocol,
            timers(),
            applications,
            Arc::new(ThreadWake::current()),
            MonoTime(0),
        )
        .err()
        .unwrap();
    assert_eq!(rejected.application.as_ref().unwrap().len(), 2);
    let start = Instant::now();
    while !rejected.try_cleanup().unwrap() {
        assert!(start.elapsed() < Duration::from_secs(10));
        std::thread::park_timeout(Duration::from_millis(1));
    }
    let tcp = std::net::TcpListener::bind(address).unwrap();
    let udp = std::net::UdpSocket::bind(address).unwrap();
    let mut c = local_multi(root.clone());
    c.startup.mode = NativeOpenMode::Recover;
    let node = c
        .open(
            protocol,
            timers(),
            local_apps(&local_multi(root.clone())),
            Arc::new(ThreadWake::current()),
            MonoTime(0),
        )
        .unwrap();
    drain_wire_nodes(vec![node]);
    drop((tcp, udp));
    std::fs::remove_dir_all(root).unwrap();
}
#[test]
fn multi_startup_tcp_late_failure_returns_all_apps_and_joins_workers() {
    failed_cleanup(NativePeerProtocol::TcpTls);
}
#[cfg(feature = "quic")]
#[test]
fn multi_startup_quic_late_failure_returns_all_apps_and_joins_workers() {
    failed_cleanup(NativePeerProtocol::Quic);
}
