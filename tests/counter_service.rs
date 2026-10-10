// SPDX-License-Identifier: RPL-1.5
// Copyright (c) 2026 Karl Livesey
// Unless explicitly acquired and licensed from Licensor under another license,
// the contents of this file are subject to the Reciprocal Public License
// ("RPL") Version 1.5, or subsequent versions as allowed by the RPL, and You may
// not copy or use this file in either source code or executable form, except
// in compliance with the terms and conditions of the RPL.
//
// All software distributed under the RPL is provided strictly on an "AS IS"
// basis, WITHOUT WARRANTY OF ANY KIND, EITHER EXPRESS OR IMPLIED, AND LICENSOR
// HEREBY DISCLAIMS ALL SUCH WARRANTIES, INCLUDING WITHOUT LIMITATION, ANY
// WARRANTIES OF MERCHANTABILITY, FITNESS FOR A PARTICULAR PURPOSE, QUIET
// ENJOYMENT, OR NON-INFRINGEMENT. See the RPL for specific language governing
// rights and limitations under the RPL.
//! Actual executable processes, TCP/TLS, abrupt leader loss and disk recovery.
#![cfg(feature = "tls")]
use std::{
    collections::{BTreeMap, BTreeSet},
    fs,
    net::{Ipv4Addr, TcpListener, UdpSocket},
    path::PathBuf,
    process::{Child, Command, Output, Stdio},
    sync::{
        atomic::{AtomicU64, Ordering},
        Mutex,
    },
    time::{Duration, Instant},
};
const BIN: &str = env!("CARGO_BIN_EXE_voteboat-counter");
static PORT_BLOCKS: Mutex<BTreeSet<u16>> = Mutex::new(BTreeSet::new());
static DIRECTORIES: AtomicU64 = AtomicU64::new(0);
// Coordinate only parent-held native store handles and process creation. A
// concurrent fork can briefly inherit an exclusive lock until exec closes it.
// Child execution and waiting stay outside this gate and remain parallel.
static STORE_SPAWN: Mutex<()> = Mutex::new(());
#[path = "counter_service/assignments.rs"]
mod assignments;
#[path = "counter_service/command_discovery.rs"]
mod command_discovery;
#[path = "counter_service/command_endpoints.rs"]
mod command_endpoints;
#[path = "counter_service/configuration_pending.rs"]
mod configuration_pending;
#[path = "counter_service/credential_reload.rs"]
mod credential_reload;
#[path = "counter_service/drain.rs"]
mod drain;
#[path = "counter_service/drain_replacement.rs"]
mod drain_replacement;
#[path = "counter_service/drain_retirement.rs"]
mod drain_retirement;
#[path = "counter_service/drain_runner.rs"]
mod drain_runner;
#[path = "counter_service/events.rs"]
mod events;
#[path = "counter_service/failure_diagnostics.rs"]
mod failure_diagnostics;
#[path = "counter_service/group_admin.rs"]
mod group_admin;
#[path = "counter_service/group_drain.rs"]
mod group_drain;
#[path = "counter_service/group_drain_bounds.rs"]
mod group_drain_bounds;
#[path = "counter_service/group_drain_runner.rs"]
mod group_drain_runner;
#[path = "counter_service/group_leadership.rs"]
mod group_leadership;
#[path = "counter_service/groups.rs"]
mod groups;
#[path = "counter_service/history.rs"]
mod history;
#[path = "support/history.rs"]
mod history_checker;
#[path = "counter_service/joint_retirement.rs"]
mod joint_retirement;
#[path = "counter_service/leadership.rs"]
mod leadership;
#[path = "counter_service/maintenance.rs"]
mod maintenance;
#[path = "counter_service/membership_drain.rs"]
mod membership_drain;
#[path = "counter_service/new_voter.rs"]
mod new_voter;
#[path = "counter_service/peer_credentials.rs"]
mod peer_credentials;
#[path = "counter_service/peer_discovery.rs"]
mod peer_discovery;
#[path = "counter_service/placement.rs"]
mod placement;
#[path = "counter_service/quorum.rs"]
mod quorum;
#[path = "counter_service/read_failover.rs"]
mod read_failover;
#[path = "counter_service/timing.rs"]
mod timing;
#[path = "counter_service/write_recovery.rs"]
mod write_recovery;

fn fixture_gate() -> std::sync::MutexGuard<'static, ()> {
    STORE_SPAWN.lock().unwrap_or_else(|e| e.into_inner())
}
fn run(command: &mut Command) -> Output {
    let child = {
        let _gate = fixture_gate();
        command
            .stdout(Stdio::piped())
            .stderr(Stdio::piped())
            .spawn()
            .unwrap()
    };
    child.wait_with_output().unwrap()
}
struct Cluster {
    root: PathBuf,
    base: u16,
    children: Vec<Option<Child>>,
    service_logs: BTreeMap<usize, PathBuf>,
    endpoints: Option<PathBuf>,
    deployment: Option<PathBuf>,
    admin_plan: Option<PathBuf>,
    remote_admin: bool,
    targets_admin: bool,
    command_access: Option<PathBuf>,
    peer_credentials: Option<PathBuf>,
    command_principal: Option<u64>,
    command_peers: Option<PathBuf>,
    remote_commands: bool,
    discovery_peers: Option<PathBuf>,
    peer_discovery: Option<PathBuf>,
    discover_via: Option<u64>,
    tls: Option<PathBuf>,
    listeners: BTreeMap<u16, TcpListener>,
    udp_sockets: Vec<UdpSocket>,
    quic: bool,
    wal_reclaim_ms: Option<u64>,
    checkpoint_entries: Option<u64>,
    leadership_maintenance: bool,
    node_drain: bool,
    membership_drain: Option<(usize, PathBuf)>,
    groups: Option<PathBuf>,
    group_admin_plans: Option<PathBuf>,
    group_drain_plan: Option<(usize, PathBuf)>,
}
impl Cluster {
    fn new() -> Self {
        let root = std::env::temp_dir().join(format!(
            "voteboat-service-{}-{}-{}",
            std::process::id(),
            std::time::SystemTime::now()
                .duration_since(std::time::UNIX_EPOCH)
                .unwrap()
                .as_nanos(),
            DIRECTORIES.fetch_add(1, Ordering::Relaxed)
        ));
        fs::create_dir(&root).unwrap();
        // Never recycle a fixture's block within this test process: accepted
        // TCP sockets can still be closing after its listeners/children drop.
        // The growing suite has 305 candidate blocks. Unavailable blocks are
        // skipped by binding every reserved TCP/UDP endpoint.
        let (base, listeners, udp_sockets) = {
            let mut blocks = PORT_BLOCKS.lock().unwrap_or_else(|e| e.into_inner());
            let (base, listeners, udp_sockets) = (10000u16..49000)
                .step_by(128)
                .find_map(|base| {
                    if blocks.contains(&base) {
                        return None;
                    }
                    [1, 2, 3, 4, 11, 12, 13, 101, 102, 103, 104, 111, 112, 113]
                        .into_iter()
                        .map(|offset| {
                            TcpListener::bind((Ipv4Addr::LOCALHOST, base + offset))
                                .map(|listener| (offset, listener))
                        })
                        .collect::<Result<BTreeMap<_, _>, _>>()
                        .ok()
                        .and_then(|listeners| {
                            [1, 2, 3, 4, 11, 12, 13]
                                .into_iter()
                                .map(|n| UdpSocket::bind((Ipv4Addr::LOCALHOST, base + n)))
                                .collect::<Result<Vec<_>, _>>()
                                .ok()
                                .map(|sockets| (base, listeners, sockets))
                        })
                })
                .expect("available independent port block");
            blocks.insert(base);
            (base, listeners, udp_sockets)
        };
        Self {
            root,
            base,
            children: (0..3).map(|_| None).collect(),
            service_logs: BTreeMap::new(),
            endpoints: None,
            deployment: None,
            admin_plan: None,
            remote_admin: false,
            targets_admin: false,
            command_access: None,
            peer_credentials: None,
            command_principal: None,
            command_peers: None,
            remote_commands: false,
            discovery_peers: None,
            peer_discovery: None,
            discover_via: None,
            tls: None,
            listeners,
            udp_sockets,
            quic: false,
            wal_reclaim_ms: None,
            checkpoint_entries: None,
            leadership_maintenance: false,
            node_drain: false,
            membership_drain: None,
            groups: None,
            group_admin_plans: None,
            group_drain_plan: None,
        }
    }
    fn take_listener(&mut self, offset: u16) -> TcpListener {
        self.listeners
            .remove(&offset)
            .expect("reserved fixture listener")
    }
    fn start(&mut self, id: usize, mode: &str) {
        // Native child processes bind their own listeners. Fake peers instead
        // take the reserved listener directly, with no probe/rebind interval.
        // Release every child port before starting the first child. Otherwise
        // early peers can connect to a placeholder listener for a later child.
        self.listeners.clear();
        self.udp_sockets.clear();
        let log_path = self.root.join(format!("{id}-{mode}.log"));
        let log = fs::File::create(&log_path).unwrap();
        self.service_logs.insert(id, log_path);
        let tls = self.tls.clone().unwrap_or_else(|| {
            PathBuf::from(env!("CARGO_MANIFEST_DIR")).join("tests/fixtures/tls")
        });
        let mut command = Command::new(BIN);
        command
            .args(["serve", mode])
            .arg(self.root.join(id.to_string()))
            .arg(id.to_string())
            .arg(self.base.to_string())
            .arg(tls);
        if let Some(path) = &self.endpoints {
            command.arg(path);
        }
        if let Some(path) = &self.groups {
            command.arg("--groups").arg(path);
        }
        if let Some(path) = &self.group_admin_plans {
            command.arg("--group-admin-plans").arg(path);
        }
        if let Some((source, path)) = &self.group_drain_plan {
            if *source == id {
                command.arg("--group-drain-plan").arg(path);
            }
        }
        if let Some(path) = &self.deployment {
            command.arg("--deployment").arg(path);
        }
        if self.quic {
            command.args(["--transport", "quic"]);
        }
        if let Some(interval) = self.wal_reclaim_ms {
            command.arg("--wal-reclaim-ms").arg(interval.to_string());
        }
        if self.leadership_maintenance {
            command.args(["--leadership-maintenance", "enabled"]);
        }
        if self.node_drain {
            command.args(["--node-drain", "enabled"]);
        }
        if let Some((owner, path)) = &self.membership_drain {
            if *owner == id {
                command.arg("--membership-drain").arg(path);
            }
        }
        if let Some(entries) = self.checkpoint_entries {
            command.arg("--checkpoint-entries").arg(entries.to_string());
        }
        if let Some(path) = &self.admin_plan {
            command
                .arg(if self.targets_admin {
                    "--remote-admin-policy"
                } else if self.remote_admin {
                    "--remote-admin-plan"
                } else {
                    "--admin-plan"
                })
                .arg(path);
        }
        if let Some(path) = &self.peer_credentials {
            command.arg("--peer-credentials").arg(path);
        }
        if let Some(path) = &self.command_access {
            command.arg("--service-access").arg(path);
        }
        if let Some(path) = &self.discovery_peers {
            command.arg("--discovery-peers").arg(path);
        }
        if let Some(path) = &self.peer_discovery {
            command.arg("--peer-discovery").arg(path);
        }
        if self.remote_commands {
            command
                .arg("--command-listen")
                .arg(format!("0.0.0.0:{}", self.base + 110 + id as u16));
        }
        let _gate = fixture_gate();
        self.children[id - 1] = Some(
            command
                .stdout(log.try_clone().unwrap())
                .stderr(log)
                .spawn()
                .unwrap(),
        );
    }

    fn request(&self, id: usize, args: &[&str]) -> std::process::Output {
        self.target(&id.to_string(), args)
    }
    fn enroll(
        &self,
        mode: &str,
        id: u64,
        target: &std::path::Path,
        source: u64,
    ) -> std::process::Output {
        let mut command = Command::new(BIN);
        command
            .args(["enroll", mode])
            .arg(target)
            .arg(id.to_string())
            .arg(self.base.to_string())
            .arg(self.tls.clone().unwrap_or_else(|| {
                PathBuf::from(env!("CARGO_MANIFEST_DIR")).join("tests/fixtures/tls")
            }))
            .arg(self.root.join(source.to_string()))
            .arg(source.to_string());
        if let Some(path) = &self.deployment {
            command.arg("--deployment").arg(path);
        }
        if self.leadership_maintenance {
            command.args(["--leadership-maintenance", "enabled"]);
        }
        run(&mut command)
    }
    fn client(&self, target: &str, args: &[&str]) -> Command {
        let mut command = Command::new(BIN);
        command
            .args(["client", &self.base.to_string(), target])
            .args(args);
        if let Some(path) = &self.command_peers {
            command.arg("--command-peers").arg(path);
        }
        if let Some(principal) = self.command_principal {
            command
                .arg("--service-tls")
                .arg(self.tls.clone().unwrap_or_else(|| {
                    PathBuf::from(env!("CARGO_MANIFEST_DIR")).join("tests/fixtures/tls")
                }))
                .arg("--principal")
                .arg(principal.to_string());
        }
        if let Some(source) = self.discover_via {
            command.arg("--discover-via").arg(source.to_string());
        }
        command
    }
    fn target(&self, target: &str, args: &[&str]) -> std::process::Output {
        run(&mut self.client(target, args))
    }
    fn ok(&self, id: usize, args: &[&str]) -> String {
        let output = self.request(id, args);
        assert!(
            output.status.success(),
            "node {id} {args:?}: {} {}\nservice log: {}\n{}",
            String::from_utf8_lossy(&output.stdout),
            String::from_utf8_lossy(&output.stderr),
            self.service_log(id),
            failure_diagnostics::snapshot(self, args)
        );
        String::from_utf8(output.stdout).unwrap()
    }
    fn wait_configuration_status(&self, id: usize, operation: &str) -> String {
        let deadline = Instant::now() + Duration::from_secs(10);
        loop {
            let output = self.request(id, &["configuration-status", operation]);
            if output.status.success() {
                return String::from_utf8(output.stdout).unwrap();
            }
            assert!(
                Instant::now() < deadline,
                "node {id} configuration status unavailable"
            );
            std::thread::sleep(Duration::from_millis(10));
        }
    }
    fn routed(&self, args: &[&str]) -> String {
        let output = self.target("auto", args);
        let diagnostics = if output.status.success() {
            String::new()
        } else {
            failure_diagnostics::snapshot(self, args)
        };
        assert!(
            output.status.success(),
            "{args:?}: {} {}\n{diagnostics}",
            String::from_utf8_lossy(&output.stdout),
            String::from_utf8_lossy(&output.stderr)
        );
        String::from_utf8(output.stdout).unwrap()
    }
    fn leader(&mut self) -> usize {
        let deadline = Instant::now() + Duration::from_secs(20);
        loop {
            for id in 1..=self.children.len() {
                if let Some(child) = &mut self.children[id - 1] {
                    assert!(
                        child.try_wait().unwrap().is_none(),
                        "node {id} exited: {}",
                        self.service_log(id)
                    );
                    let output = self.request(id, &["status"]);
                    if output.status.success()
                        && String::from_utf8_lossy(&output.stdout).contains("role=Leader")
                    {
                        return id;
                    }
                }
            }
            assert!(
                Instant::now() < deadline,
                "no leader; logs at {:?}",
                self.root
            );
            std::thread::sleep(Duration::from_millis(20));
        }
    }
    fn stop(&mut self) {
        // Keep the selected discovery source available until other commands finish.
        let mut nodes = (1..=self.children.len()).collect::<Vec<_>>();
        nodes.sort_by_key(|id| Some(*id as u64) == self.discover_via);
        // Intake closes independently; each node must drain and join its workers.
        for id in nodes {
            if self.children[id - 1].is_some() {
                self.ok(id, &["quit"]);
            }
        }
        let deadline = Instant::now() + Duration::from_secs(15);
        for i in 0..self.children.len() {
            if self.children[i].is_some() {
                loop {
                    if let Some(status) = self.children[i].as_mut().unwrap().try_wait().unwrap() {
                        assert!(
                            status.success(),
                            "node {} exit {status}: {}",
                            i + 1,
                            self.service_log(i + 1)
                        );
                        break;
                    }
                    assert!(
                        Instant::now() < deadline,
                        "node {} shutdown timeout: {}",
                        i + 1,
                        self.service_log(i + 1)
                    );
                    std::thread::sleep(Duration::from_millis(10));
                }
                self.children[i] = None;
            }
        }
    }
    fn service_log(&self, id: usize) -> String {
        self.service_logs.get(&id).map_or_else(
            || "no service log".into(),
            |path| fs::read_to_string(path).unwrap_or_else(|e| format!("{}: {e}", path.display())),
        )
    }
}
impl Drop for Cluster {
    fn drop(&mut self) {
        for c in self.children.iter_mut().flatten() {
            let _ = c.kill();
            let _ = c.wait();
        }
    }
}
#[test]
fn three_service_processes_retry_replace_leader_and_recover_native_files() {
    replicated_history(false);
}
#[cfg(feature = "quic")]
#[test]
fn three_quic_service_processes_retry_replace_leader_and_recover_native_files() {
    replicated_history(true);
}
/// Prepared durable assignment tests service recovery, not online proposal delivery.
fn seed_member_service(root: &std::path::Path, final_view: bool, include_learner: bool) {
    let _gate = fixture_gate();
    use voteboat::{
        contracts::HardState,
        identity::*,
        log::*,
        membership::*,
        native::{log_store::*, snapshot_store::*},
        quorum::*,
        snapshot::*,
    };
    let node = |n| NodeId::new(n).unwrap();
    let store = |n: u64| StoreIdentity {
        id: StoreId::new(n as u128).unwrap(),
        incarnation: StoreIncarnation::new(1).unwrap(),
    };
    let cid = |n| ConfigurationId::new(n).unwrap();
    let group = GroupIdentity {
        id: GroupId::new(1).unwrap(),
        incarnation: GroupIncarnation::new(1).unwrap(),
    };
    let policy = |ids: &[u64]| {
        Policy::new(
            Tree::Majority(ids.iter().map(|&n| Tree::Voter(node(n))).collect()),
            Limits::default(),
        )
        .unwrap()
    };
    let bootstrap = Bootstrap {
        group,
        configuration: cid(1),
        policy: policy(&[1, 2, 3]),
        voter_stores: (1..=3).map(|n| (node(n), store(n))).collect(),
    };
    let next = Configuration::new(
        cid(3),
        policy(&[1, 2]),
        (1..=2).map(|n| (node(n), store(n))).collect(),
        [(node(3), store(3))].into(),
    )
    .unwrap();
    let records = [
        ConfigurationRecord {
            operation: OperationId::new(500).unwrap(),
            expected: cid(1),
            change: ConfigurationChange::Joint { id: cid(2), next },
        },
        ConfigurationRecord {
            operation: OperationId::new(500).unwrap(),
            expected: cid(2),
            change: ConfigurationChange::Final { id: cid(3) },
        },
    ];
    for local in 1..=if include_learner { 3 } else { 2 } {
        let path = root.join(local.to_string());
        let mut log = NativeLogStore::create(
            FileLogIo::create(&path).unwrap(),
            store(local),
            LogLimits::default(),
        )
        .unwrap();
        let tickets = log
            .append_batch(vec![LogMutation::Create(bootstrap.clone())])
            .unwrap();
        log.barrier(&tickets).unwrap();
        let state = log.state(group).unwrap();
        let entries = records
            .iter()
            .take(if final_view { 2 } else { 1 })
            .enumerate()
            .map(|(i, r)| LogEntry {
                index: i as u64 + 1,
                term: 1,
                payload: EntryPayload::Configuration(Box::new(r.clone())),
            })
            .collect::<Vec<_>>();
        let tickets = log
            .append_batch(vec![LogMutation::Update(LogUpdate {
                group,
                expected_revision: state.revision,
                hard_state: HardState {
                    term: 1,
                    voted_for: None,
                },
                commit_index: entries.len() as u64,
                suffix: Some(Suffix { from: 1, entries }),
                snapshot: None,
                snapshot_membership: None,
            })])
            .unwrap();
        log.barrier(&tickets).unwrap();
        NativeSnapshotStore::create(
            FileSnapshotIo::create(path.join("snapshots")).unwrap(),
            SnapshotIdentity {
                group,
                store: store(local),
            },
            SnapshotLimits::default(),
        )
        .unwrap();
    }
}
fn trusted_administration_history(quic: bool) {
    let mut cluster = Cluster::new();
    cluster.quic = quic;
    seed_member_service(&cluster.root, true, true);
    let path = cluster.root.join("administration.plan");
    let policy = if quic {
        "w:2 2 m:2 v:1 v:2 2 v:3"
    } else {
        "m:3 v:1 v:2 v:3"
    };
    fs::write(&path, format!("voteboat-counter-admin-v1\nplacement 2 false\nreplica 1 1\nreplica 2 2\nreplica 3 3\njoint 800 3 4 5 - {policy}\nfinal 800 4 5\n")).unwrap();
    cluster.admin_plan = Some(path);
    for restart in [false, true] {
        for id in 1..=3 {
            cluster.start(id, "recover-member");
        }
        cluster.leader();
        // A real application write establishes this leader's current-term
        // commitment. Administration never manufactures an application write.
        assert!(cluster.routed(&["add", "900", "1"]).contains("Value(1)"));
        let deadline = Instant::now() + Duration::from_secs(25);
        for id in 1..=3 {
            loop {
                let status = cluster.wait_configuration_status(id, "800");
                if status.contains("action=completed") {
                    break;
                }
                assert!(
                    Instant::now() < deadline,
                    "administration did not finish: {status}; logs at {:?}",
                    cluster.root
                );
                std::thread::sleep(Duration::from_millis(30));
            }
        }
        assert_eq!(cluster.routed(&["read"]), "OK value=1\n");
        let leader = cluster.leader();
        // Startup scope does not add configuration mutation to the command port.
        assert!(!cluster
            .request(leader, &["configure", "801"])
            .status
            .success());
        cluster.stop();
        let _gate = fixture_gate();
        use voteboat::{identity::*, log::*, membership::*, native::log_store::*};
        let group = GroupIdentity {
            id: GroupId::new(1).unwrap(),
            incarnation: GroupIncarnation::new(1).unwrap(),
        };
        for id in 1..=3 {
            let log = NativeLogStore::recover(
                FileLogIo::open(cluster.root.join(id.to_string())).unwrap(),
                StoreIdentity {
                    id: StoreId::new(id as u128).unwrap(),
                    incarnation: StoreIncarnation::new(1).unwrap(),
                },
                LogLimits::default(),
            )
            .unwrap();
            let state = log.state(group).unwrap();
            let membership = state.membership_at(state.commit_index).unwrap();
            assert_eq!(membership.id().get(), 5);
            assert_eq!(membership.stable().voter_stores().len(), 3);
            assert!(membership.stable().learners().is_empty());
            let records = state
                .entries
                .iter()
                .filter_map(|entry| match &entry.payload {
                    EntryPayload::Configuration(record) if record.operation.get() == 800 => {
                        Some(&record.change)
                    }
                    _ => None,
                })
                .collect::<Vec<_>>();
            assert_eq!(
                records.len(),
                2,
                "restart={restart} must not duplicate original intent"
            );
            assert!(matches!(records[0], ConfigurationChange::Joint { .. }));
            assert!(matches!(records[1], ConfigurationChange::Final { .. }));
        }
    }
}
#[test]
fn trusted_startup_plan_promotes_and_restarts_tcp() {
    trusted_administration_history(false);
}
#[cfg(feature = "quic")]
#[test]
fn trusted_startup_plan_promotes_and_restarts_quic() {
    trusted_administration_history(true);
}
#[test]
fn invalid_administration_is_rejected_before_store_open() {
    let cluster = Cluster::new();
    let prefix =
        "voteboat-counter-admin-v1\nplacement 1 false\nreplica 1 1\nreplica 2 2\nreplica 3 3\n";
    let valid = "joint 800 1 2 3 - m:3 v:1 v:2 v:3\nfinal 800 2 3\n";
    let cases = [
        "wrong header\n".to_owned(),
        "x".repeat(65537),
        prefix.to_owned(),
        format!("{prefix}replica 1 2\n{valid}"),
        format!("{prefix}replica 4 4\n{valid}"),
        format!("{prefix}joint 800 1 2 3 - m:2 v:1 v:1\n"),
        format!("{prefix}joint 800 1 2 3 - w:2 0 v:1 1 v:2\n"),
        format!("{prefix}joint 800 1 2 3 - m:999999 v:1\n"),
        format!("{prefix}joint 800 1 2 3 - {}v:1\n", "m:1 ".repeat(34)),
        format!("{prefix}learners 800 1 2 3,3 m:2 v:1 v:2\n"),
        format!("{prefix}final 0 2 3\n"),
        format!("{prefix}final 800 2 3 extra\n"),
        format!("{prefix}{valid}final 800 2 3\n"),
        format!(
            "{prefix}{}",
            (1..=65)
                .map(|n| format!("final {n} 2 3\n"))
                .collect::<String>()
        ),
    ];
    let path = cluster.root.join("invalid.plan");
    let tls = PathBuf::from(env!("CARGO_MANIFEST_DIR")).join("tests/fixtures/tls");
    for (i, contents) in cases.iter().enumerate() {
        fs::write(&path, contents).unwrap();
        let target = cluster.root.join(format!("invalid-{i}"));
        let output = run(Command::new(BIN)
            .args(["serve", "recover-member"])
            .arg(&target)
            .arg("1")
            .arg(cluster.base.to_string())
            .arg(&tls)
            .arg("--admin-plan")
            .arg(&path));
        assert!(!output.status.success());
        assert!(!target.exists());
        assert!(
            !String::from_utf8_lossy(&output.stderr).contains("No such file"),
            "parser should reject before missing WAL: {i}"
        );
    }
    fs::write(&path, format!("{prefix}{valid}")).unwrap();
    for mode in ["create", "recover"] {
        let target = cluster.root.join(mode);
        let output = run(Command::new(BIN)
            .args(["serve", mode])
            .arg(&target)
            .arg("1")
            .arg(cluster.base.to_string())
            .arg(&tls)
            .arg("--admin-plan")
            .arg(&path));
        assert!(!output.status.success());
        assert!(String::from_utf8_lossy(&output.stderr)
            .contains("administration plan requires recover-member"));
        assert!(!target.exists());
    }
}
fn member_service_history(quic: bool) {
    for final_view in [false, true] {
        let mut cluster = Cluster::new();
        cluster.quic = quic;
        seed_member_service(&cluster.root, final_view, true);
        // Selecting ordinary recovery must not silently opt into a dynamic view.
        cluster.listeners.clear();
        cluster.udp_sockets.clear();
        let tls = PathBuf::from(env!("CARGO_MANIFEST_DIR")).join("tests/fixtures/tls");
        let refused = run(Command::new(BIN)
            .args(["serve", "recover"])
            .arg(cluster.root.join("1"))
            .arg("1")
            .arg(cluster.base.to_string())
            .arg(tls));
        assert!(
            !refused.status.success(),
            "static recovery must refuse the journal"
        );
        for id in 1..=3 {
            cluster.start(id, "recover-member");
        }
        let leader = cluster.leader();
        if final_view {
            assert_ne!(leader, 3, "demoted learner must not lead");
        }
        let expected = if final_view {
            "action=completed"
        } else {
            "action=finalize_requires_authorization"
        };
        for id in 1..=3 {
            assert!(cluster
                .wait_configuration_status(id, "500")
                .contains(expected));
        }
        assert!(leader_request(&mut cluster, &["add", "700", "42"]).contains("Value(42)"));
        assert_eq!(leader_request(&mut cluster, &["read"]), "OK value=42\n");
        // Configuration intake remains closed even in explicit member mode.
        assert!(!cluster
            .request(leader, &["configure", "501"])
            .status
            .success());
        cluster.ok(leader, &["checkpoint"]);
        cluster.stop();
        {
            let _gate = fixture_gate();
            use voteboat::{identity::*, log::*, native::log_store::*};
            let log = NativeLogStore::recover(
                FileLogIo::open(cluster.root.join(leader.to_string())).unwrap(),
                StoreIdentity {
                    id: StoreId::new(leader as u128).unwrap(),
                    incarnation: StoreIncarnation::new(1).unwrap(),
                },
                LogLimits::default(),
            )
            .unwrap();
            let group = GroupIdentity {
                id: GroupId::new(1).unwrap(),
                incarnation: GroupIncarnation::new(1).unwrap(),
            };
            assert!(
                log.state(group).unwrap().base_index() > 0,
                "member checkpoint must drain before restart"
            );
        }
        for id in 1..=3 {
            cluster.start(id, "recover-member");
        }
        cluster.leader();
        assert!(leader_request(&mut cluster, &["add", "700", "42"]).contains("duplicate=true"));
        assert_eq!(leader_request(&mut cluster, &["read"]), "OK value=42\n");
        for id in 1..=3 {
            assert!(cluster
                .wait_configuration_status(id, "500")
                .contains(expected));
        }
        if final_view {
            assert!(cluster.ok(3, &["status"]).contains("role=Follower"));
            assert!(!cluster.request(3, &["add", "701", "1"]).status.success());
        }
        cluster.stop();
        for id in 1..=3 {
            assert!(
                fs::read_to_string(cluster.root.join(format!("{id}-recover-member.log")))
                    .unwrap()
                    .contains("workers_joined=true")
            );
        }
        fs::remove_dir_all(&cluster.root).unwrap();
    }
}
#[test]
fn member_service_recovers_joint_and_final_histories_and_checkpoint_retries() {
    member_service_history(false);
}
#[cfg(feature = "quic")]
#[test]
fn quic_member_service_recovers_joint_and_final_histories_and_checkpoint_retries() {
    member_service_history(true);
}
fn checkpoint_source(root: &std::path::Path, id: u64, operation: u128, delta: i64) {
    let _gate = fixture_gate();
    use voteboat::{
        application::*,
        identity::*,
        log::*,
        native::{log_store::*, snapshot_store::*},
        snapshot::*,
    };
    let identity = StoreIdentity {
        id: StoreId::new(id as u128).unwrap(),
        incarnation: StoreIncarnation::new(1).unwrap(),
    };
    let group = GroupIdentity {
        id: GroupId::new(1).unwrap(),
        incarnation: GroupIncarnation::new(1).unwrap(),
    };
    let path = root.join(id.to_string());
    let mut log = NativeLogStore::recover(
        FileLogIo::open(&path).unwrap(),
        identity,
        LogLimits::default(),
    )
    .unwrap();
    let state = log.state(group).unwrap();
    let index = state.last_index() + 1;
    let tickets = log
        .append_batch(vec![LogMutation::Update(LogUpdate {
            group,
            expected_revision: state.revision,
            hard_state: state.hard_state,
            commit_index: index,
            suffix: Some(Suffix {
                from: index,
                entries: vec![LogEntry {
                    index,
                    term: state.hard_state.term,
                    payload: EntryPayload::Command {
                        operation: OperationId::new(operation).unwrap(),
                        bytes: delta.to_le_bytes().to_vec(),
                    },
                }],
            }),
            snapshot: None,
            snapshot_membership: None,
        })])
        .unwrap();
    log.barrier(&tickets).unwrap();
    let mut snapshots = NativeSnapshotStore::recover(
        FileSnapshotIo::open(path.join("snapshots")).unwrap(),
        SnapshotIdentity {
            store: identity,
            group,
        },
        SnapshotLimits::default(),
    )
    .unwrap();
    let mut app = Counter::new(10000).unwrap();
    let (mut core, _) = recover_member_replica(
        NodeId::new(id).unwrap(),
        group,
        &log,
        &mut snapshots,
        &mut app,
    )
    .unwrap();
    let receipt = checkpoint_application(&core, &app, &mut snapshots).unwrap();
    compact_replica(
        &mut core,
        &mut log,
        &mut snapshots,
        &app,
        receipt.reference(),
    )
    .unwrap();
}
fn enrolled_state(root: &std::path::Path) -> voteboat::log::GroupLog {
    use voteboat::identity::*;
    enrolled_state_for(
        root,
        3,
        StoreIdentity {
            id: StoreId::new(3).unwrap(),
            incarnation: StoreIncarnation::new(1).unwrap(),
        },
    )
}
fn enrolled_state_for(
    root: &std::path::Path,
    local: u64,
    identity: voteboat::identity::StoreIdentity,
) -> voteboat::log::GroupLog {
    recovered_state_for(root, local, identity, 42, (700, 42))
}
fn recovered_state_for(
    root: &std::path::Path,
    local: u64,
    identity: voteboat::identity::StoreIdentity,
    expected: i64,
    retry_command: (u128, i64),
) -> voteboat::log::GroupLog {
    let _gate = fixture_gate();
    use voteboat::{
        application::*,
        identity::*,
        log::*,
        native::{log_store::*, snapshot_store::*},
        snapshot::*,
    };
    let log = NativeLogStore::recover(
        FileLogIo::open(root).unwrap(),
        identity,
        LogLimits::default(),
    )
    .unwrap();
    let group = GroupIdentity {
        id: GroupId::new(1).unwrap(),
        incarnation: GroupIncarnation::new(1).unwrap(),
    };
    let mut snapshots = NativeSnapshotStore::recover(
        FileSnapshotIo::open(root.join("snapshots")).unwrap(),
        SnapshotIdentity {
            group,
            store: identity,
        },
        SnapshotLimits::default(),
    )
    .unwrap();
    let mut app = Counter::new(10000).unwrap();
    let (core, _) = recover_member_replica(
        NodeId::new(local).unwrap(),
        group,
        &log,
        &mut snapshots,
        &mut app,
    )
    .unwrap();
    assert_eq!(
        app.read_applied(core.state().commit_index).unwrap(),
        expected
    );
    // Inspect the imported application's retry state directly, without treating
    // this local diagnostic as a distributed read or proposing to a learner.
    let retry = app
        .apply_batch(&[LogEntry {
            index: app.applied_index() + 1,
            term: 1,
            payload: EntryPayload::Command {
                operation: OperationId::new(retry_command.0).unwrap(),
                bytes: retry_command.1.to_le_bytes().to_vec(),
            },
        }])
        .unwrap();
    assert!(retry[0].duplicate);
    assert_eq!(retry[0].outcome, CounterOutcome::Value(retry_command.1));
    log.state(group).unwrap()
}
fn enrolled_service_history(quic: bool) {
    let mut cluster = Cluster::new();
    cluster.quic = quic;
    seed_member_service(&cluster.root, true, false);
    checkpoint_source(&cluster.root, 1, 700, 42);
    let destination = cluster.root.join("3");
    let created = cluster.enroll("create", 3, &destination, 1);
    assert!(
        created.status.success(),
        "{}",
        String::from_utf8_lossy(&created.stderr)
    );
    assert!(String::from_utf8_lossy(&created.stdout).contains("checkpoint=3 term=1"));
    let before = enrolled_state(&destination);
    assert_eq!(before.commit_index, 3);
    assert!(before.entries.is_empty());
    for _ in 0..2 {
        assert!(cluster
            .enroll("recover", 3, &destination, 1)
            .status
            .success());
        assert_eq!(
            enrolled_state(&destination),
            before,
            "lost-reply retry must not rewrite history"
        );
    }
    assert!(!cluster
        .enroll("create", 3, &destination, 1)
        .status
        .success());
    assert_eq!(enrolled_state(&destination), before);
    for id in 1..=3 {
        cluster.start(id, "recover-member");
    }
    let leader = cluster.leader();
    assert_ne!(leader, 3);
    assert!(cluster
        .ok(leader, &["add", "700", "42"])
        .contains("duplicate=true"));
    assert_eq!(cluster.ok(leader, &["read"]), "OK value=42\n");
    assert!(cluster
        .ok(leader, &["add", "701", "1"])
        .contains("Value(43)"));
    cluster.ok(leader, &["checkpoint"]);
    cluster.stop();
    for id in 1..=3 {
        cluster.start(id, "recover-member");
    }
    let leader = cluster.leader();
    assert!(cluster
        .ok(leader, &["add", "701", "1"])
        .contains("duplicate=true"));
    assert_eq!(cluster.ok(leader, &["read"]), "OK value=43\n");
    cluster.stop();
    fs::remove_dir_all(&cluster.root).unwrap();
}
#[test]
fn offline_enrollment_retries_then_runs_and_recovers_tcp_service() {
    enrolled_service_history(false);
}
#[cfg(feature = "quic")]
#[test]
fn offline_enrollment_retries_then_runs_and_recovers_quic_service() {
    enrolled_service_history(true);
}
#[test]
fn enrollment_refuses_missing_checkpoint_voter_import_and_joint_image_before_creation() {
    for joint in [false, true] {
        let cluster = Cluster::new();
        seed_member_service(&cluster.root, !joint, false);
        let target = cluster.root.join("3");
        assert!(!cluster.enroll("create", 3, &target, 1).status.success());
        assert!(
            !target.exists(),
            "no pinned source must not create destination"
        );
        checkpoint_source(&cluster.root, 1, 700, 42);
        let voter = cluster.root.join("voter-import");
        assert!(!cluster.enroll("create", 2, &voter, 1).status.success());
        assert!(!voter.exists());
        assert!(!cluster.enroll("recover", 3, &target, 1).status.success());
        assert!(
            !target.exists(),
            "recovery must never provision missing files"
        );
        if joint {
            assert!(!cluster.enroll("create", 3, &target, 1).status.success());
            assert!(!target.exists(), "joint image must not create destination");
        } else {
            assert!(cluster.enroll("create", 3, &target, 1).status.success());
            let before = enrolled_state(&target);
            checkpoint_source(&cluster.root, 1, 701, 1);
            assert!(!cluster.enroll("recover", 3, &target, 1).status.success());
            assert_eq!(
                enrolled_state(&target),
                before,
                "different image must not replace enrollment"
            );
            // Keep the old checkpoint but commit removal of its learner.
            let gate = fixture_gate();
            use voteboat::{identity::*, log::*, membership::*, native::log_store::*};
            let mut log = NativeLogStore::recover(
                FileLogIo::open(cluster.root.join("1")).unwrap(),
                StoreIdentity {
                    id: StoreId::new(1).unwrap(),
                    incarnation: StoreIncarnation::new(1).unwrap(),
                },
                LogLimits::default(),
            )
            .unwrap();
            let group = GroupIdentity {
                id: GroupId::new(1).unwrap(),
                incarnation: GroupIncarnation::new(1).unwrap(),
            };
            let state = log.state(group).unwrap();
            let membership = state.membership_at(state.commit_index).unwrap();
            let next = Configuration::new(
                ConfigurationId::new(4).unwrap(),
                membership.stable().policy().clone(),
                membership.stable().voter_stores().clone(),
                BTreeMap::new(),
            )
            .unwrap();
            let index = state.last_index() + 1;
            let tickets = log
                .append_batch(vec![LogMutation::Update(LogUpdate {
                    group,
                    expected_revision: state.revision,
                    hard_state: state.hard_state,
                    commit_index: index,
                    suffix: Some(Suffix {
                        from: index,
                        entries: vec![LogEntry {
                            index,
                            term: state.hard_state.term,
                            payload: EntryPayload::Configuration(Box::new(ConfigurationRecord {
                                operation: OperationId::new(502).unwrap(),
                                expected: ConfigurationId::new(3).unwrap(),
                                change: ConfigurationChange::Learners(next),
                            })),
                        }],
                    }),
                    snapshot: None,
                    snapshot_membership: None,
                })])
                .unwrap();
            log.barrier(&tickets).unwrap();
            drop(log);
            drop(gate);
            let stale = cluster.root.join("stale-target");
            let rejected = cluster.enroll("create", 3, &stale, 1);
            assert!(!rejected.status.success());
            assert!(String::from_utf8_lossy(&rejected.stderr).contains("membership is stale"));
            assert!(!stale.exists());
            assert!(!cluster
                .enroll("create", 3, &cluster.root.join("1"), 1)
                .status
                .success());
        }
        fs::remove_dir_all(&cluster.root).unwrap();
    }
}
fn deployment_history(quic: bool) {
    use voteboat::identity::*;
    let mut cluster = Cluster::new();
    cluster.quic = quic;
    cluster.children.push(None); // Nodes 1, 2 and 4; node 3 has retired.
    seed_member_service(&cluster.root, true, false);
    let fourth = StoreIdentity {
        id: StoreId::new(404).unwrap(),
        incarnation: StoreIncarnation::new(7).unwrap(),
    };
    seed_fourth_learner(&cluster, fourth);
    checkpoint_source(&cluster.root, 1, 700, 42);
    configure_fourth_tls(&mut cluster);
    let declaration = format!("voteboat-deployment-v1\n1 1 1 127.0.0.1:{} node1.voteboat.test\n2 2 1 127.0.0.1:{} node2.voteboat.test\n4 404 7 127.0.0.1:{} node3.voteboat.test\n",
        cluster.base + 1, cluster.base + 2, cluster.base + 4);
    let path = cluster.root.join("deployment.txt");
    fs::write(&path, &declaration).unwrap();
    cluster.deployment = Some(path.clone());
    let destination = cluster.root.join("4");
    cluster.listeners.clear();
    cluster.udp_sockets.clear();
    refuse_unassigned_deployment(&cluster, &path);
    let created = cluster.enroll("create", 4, &destination, 1);
    assert!(
        created.status.success(),
        "{}",
        String::from_utf8_lossy(&created.stderr)
    );
    let before = enrolled_state_for(&destination, 4, fourth);
    assert_eq!(before.commit_index, 4);
    assert!(cluster
        .enroll("recover", 4, &destination, 1)
        .status
        .success());
    assert_eq!(enrolled_state_for(&destination, 4, fourth), before);
    fs::write(&path, declaration.replace("4 404 7", "4 404 8")).unwrap();
    assert!(!cluster
        .enroll("recover", 4, &destination, 1)
        .status
        .success());
    assert_eq!(enrolled_state_for(&destination, 4, fourth), before);
    fs::write(&path, declaration).unwrap();
    for id in [1, 2, 4] {
        cluster.start(id, "recover-member");
    }
    let leader = cluster.leader();
    assert!(leader == 1 || leader == 2);
    assert!(cluster
        .ok(leader, &["add", "700", "42"])
        .contains("duplicate=true"));
    assert!(cluster
        .ok(leader, &["add", "701", "1"])
        .contains("Value(43)"));
    // Observe node 4 after the new write, rather than counting a provisioned route
    // as evidence of catch-up. Its exact store must durably receive replication.
    let boundary = cluster
        .ok(leader, &["status"])
        .trim()
        .split("committed=")
        .nth(1)
        .unwrap()
        .parse::<u64>()
        .unwrap();
    let deadline = Instant::now() + Duration::from_secs(10);
    loop {
        let status = cluster.ok(4, &["status"]);
        if status
            .trim()
            .split("committed=")
            .nth(1)
            .unwrap()
            .parse::<u64>()
            .unwrap()
            >= boundary
        {
            break;
        }
        assert!(Instant::now() < deadline, "new learner did not catch up");
        std::thread::sleep(Duration::from_millis(10));
    }
    assert!(!cluster.request(4, &["add", "702", "1"]).status.success());
    cluster.ok(leader, &["checkpoint"]);
    cluster.stop();
    for id in [1, 2, 4] {
        cluster.start(id, "recover-member");
    }
    let leader = cluster.leader();
    assert!(cluster
        .ok(leader, &["add", "701", "1"])
        .contains("duplicate=true"));
    assert_eq!(cluster.ok(leader, &["read"]), "OK value=43\n");
    assert!(cluster.ok(4, &["status"]).contains("role=Follower"));
    cluster.stop();
    fs::remove_dir_all(&cluster.root).unwrap();
}
#[test]
fn explicit_deployment_enrolls_fourth_exact_store_and_restarts_tcp() {
    deployment_history(false);
}
#[cfg(feature = "quic")]
#[test]
fn explicit_deployment_enrolls_fourth_exact_store_and_restarts_quic() {
    deployment_history(true);
}
/// Drive only the exact caller-owned zero-delta retry while leadership changes.
/// One successful write per leader/term establishes the core administration
/// prerequisite without continuously invalidating promotion readiness.
fn finish_lifecycle_operation(cluster: &Cluster, ids: &[usize], operation: &str, write: &str) {
    let deadline = Instant::now() + Duration::from_secs(30);
    let mut established = BTreeSet::new();
    loop {
        let mut complete = true;
        for &id in ids {
            let output = cluster.request(id, &["configuration-status", operation]);
            complete &= output.status.success()
                && String::from_utf8_lossy(&output.stdout).contains("action=completed");
            let status = cluster.request(id, &["status"]);
            let status = String::from_utf8_lossy(&status.stdout);
            if status.contains("role=Leader") {
                let term = status
                    .split("term=")
                    .nth(1)
                    .unwrap()
                    .split_whitespace()
                    .next()
                    .unwrap()
                    .parse::<u64>()
                    .unwrap();
                if !established.contains(&(id, term)) {
                    let output = cluster.request(id, &["add", write, "0"]);
                    let text = String::from_utf8_lossy(&output.stdout);
                    if output.status.success() {
                        assert!(text.contains("Value(42)"));
                        established.insert((id, term));
                    } else {
                        assert!(
                            text.contains("UNKNOWN")
                                || text.contains("NOT_LEADER")
                                || text.contains("Busy"),
                            "unexpected application rejection: {text} {}",
                            String::from_utf8_lossy(&output.stderr)
                        );
                    }
                }
            }
        }
        if complete && !established.is_empty() {
            return;
        }
        assert!(
            Instant::now() < deadline,
            "operation {operation} did not complete; logs at {:?}",
            cluster.root
        );
        std::thread::sleep(Duration::from_millis(25));
    }
}
fn complete_executable_membership_history(quic: bool) {
    let mut cluster = Cluster::new();
    cluster.quic = quic;
    cluster.children.push(None);
    // All configuration history below comes from real executable proposals.
    // Start only the original public bootstrap, then select member recovery.
    for id in 1..=3 {
        cluster.start(id, "create");
    }
    cluster.leader();
    assert!(cluster.routed(&["add", "700", "42"]).contains("Value(42)"));
    cluster.stop();
    let plan = cluster.root.join("lifecycle.plan");
    let legacy =
        "voteboat-counter-admin-v1\nplacement 2 false\nreplica 1 1\nreplica 2 2\nreplica 3 3\n";
    fs::write(&plan, format!("{legacy}joint 1000 1 2 3 3 m:2 v:1 v:2\nfinal 1000 2 3\nlearners 1001 3 4 - m:2 v:1 v:2\n")).unwrap();
    cluster.admin_plan = Some(plan.clone());
    for id in 1..=3 {
        cluster.start(id, "recover-member");
    }
    finish_lifecycle_operation(&cluster, &[1, 2], "1001", "701");
    cluster.stop();
    inspect_lifecycle(&cluster, &[1, 2], 4, &[1, 2], &[], &[1000, 1001], 42);

    // Node 3 is retired before its public test credential is assigned to node 4.
    // No two live peers share that credential. Store 4 has a distinct incarnation.
    configure_fourth_tls(&mut cluster);
    let deployment = cluster.root.join("lifecycle.deployment");
    let declaration = format!("voteboat-deployment-v1\n1 1 1 127.0.0.1:{} node1.voteboat.test\n2 2 1 127.0.0.1:{} node2.voteboat.test\n4 404 7 127.0.0.1:{} node3.voteboat.test\n", cluster.base+1, cluster.base+2, cluster.base+4);
    fs::write(&deployment, &declaration).unwrap();
    cluster.deployment = Some(deployment.clone());
    let provisioned =
        "voteboat-counter-admin-v1\nplacement 2 false\nreplica 1 1\nreplica 2 2\nreplica 4 4\n";
    plan_fourth_learner(&cluster, &plan, provisioned);
    for id in [1, 2] {
        cluster.start(id, "recover-member");
    }
    finish_lifecycle_operation(&cluster, &[1, 2], "1002", "702");
    for id in [1, 2] {
        cluster.ok(id, &["checkpoint"]);
    }
    cluster.stop();
    inspect_lifecycle(&cluster, &[1, 2], 5, &[1, 2], &[4], &[1000, 1001, 1002], 42);
    enroll_checkpointed_fourth(&cluster);

    promote_fourth(&mut cluster, &plan, provisioned, quic);
    inspect_lifecycle(
        &cluster,
        &[2, 4],
        7,
        &[1, 2, 4],
        &[],
        &[1000, 1001, 1002, 1003],
        42,
    );

    fs::write(&plan, format!("{provisioned}joint 1004 7 8 9 1 m:2 v:2 v:4\nfinal 1004 8 9\nlearners 1005 9 10 - m:2 v:2 v:4\n")).unwrap();
    for id in [2, 4] {
        cluster.start(id, "recover-member");
    }
    finish_lifecycle_operation(&cluster, &[2, 4], "1005", "705");
    for id in [2, 4] {
        cluster.ok(id, &["checkpoint"]);
    }
    cluster.stop();
    inspect_lifecycle(
        &cluster,
        &[2, 4],
        10,
        &[2, 4],
        &[],
        &[1000, 1001, 1002, 1003, 1004, 1005],
        42,
    );
    // The survivors can now omit both retired original routes. Reopen actual
    // compacted state, preserve retries, and serve a fresh nonzero mutation.
    cluster.admin_plan = None;
    fs::write(
        &deployment,
        declaration
            .lines()
            .filter(|line| !line.starts_with("1 "))
            .collect::<Vec<_>>()
            .join("\n")
            + "\n",
    )
    .unwrap();
    for id in [2, 4] {
        cluster.start(id, "recover-member");
    }
    let leader = cluster.leader();
    assert!(cluster
        .ok(leader, &["add", "700", "42"])
        .contains("duplicate=true"));
    assert!(cluster
        .ok(leader, &["add", "706", "1"])
        .contains("Value(43)"));
    assert_eq!(cluster.ok(leader, &["read"]), "OK value=43\n");
    cluster.stop();
    inspect_lifecycle(
        &cluster,
        &[2, 4],
        10,
        &[2, 4],
        &[],
        &[1000, 1001, 1002, 1003, 1004, 1005],
        43,
    );
    fs::remove_dir_all(&cluster.root).unwrap();
}
#[test]
fn executable_membership_add_enroll_promote_retire_and_restart_tcp() {
    complete_executable_membership_history(false);
}
#[cfg(feature = "quic")]
#[test]
fn executable_membership_add_enroll_promote_retire_and_restart_quic() {
    complete_executable_membership_history(true);
}
#[test]
fn invalid_deployment_is_rejected_before_store_creation() {
    let mut cluster = Cluster::new();
    cluster.listeners.clear();
    cluster.udp_sockets.clear();
    let tls = PathBuf::from(env!("CARGO_MANIFEST_DIR")).join("tests/fixtures/tls");
    let path = cluster.root.join("deployment.txt");
    let line = format!(
        "4 404 7 127.0.0.1:{} node4.voteboat.test\n",
        cluster.base + 4
    );
    let cases = [
        format!("unknown-version\n{line}"),
        format!("voteboat-deployment-v1\n{line}{line}"),
        format!(
            "voteboat-deployment-v1\n{line}1 404 7 127.0.0.1:{} node1.voteboat.test\n",
            cluster.base + 1
        ),
        format!(
            "voteboat-deployment-v1\n{line}1 1 1 127.0.0.1:{} node1.voteboat.test\n",
            cluster.base + 4
        ),
        format!(
            "voteboat-deployment-v1\n{}",
            line.replace("4 404 7", "0 404 7")
        ),
        format!(
            "voteboat-deployment-v1\n{}",
            line.replace("4 404 7", "4097 404 7")
        ),
        format!(
            "voteboat-deployment-v1\n{}",
            line.replace("4 404 7", "4 0 7")
        ),
        format!(
            "voteboat-deployment-v1\n{}",
            line.replace("4 404 7", "4 404 0")
        ),
        "voteboat-deployment-v1\n4 404 7 0.0.0.0:0 invalid/name\n".to_owned(),
        format!(
            "voteboat-deployment-v1\n{}",
            line.replace("node4.voteboat.test", "invalid/name")
        ),
        "voteboat-deployment-v1\n4 404 7\n".to_owned(),
        "voteboat-deployment-v1\n1 1 1 127.0.0.1:1234 node1.voteboat.test\n".to_owned(),
        "x".repeat(65537),
        format!(
            "voteboat-deployment-v1\n{}",
            (1..=1025)
                .map(|n| format!("{n} {n} 1 127.0.0.1:{} node1.voteboat.test\n", n + 1000))
                .collect::<String>()
        ),
    ];
    for (i, declaration) in cases.iter().enumerate() {
        fs::write(&path, declaration).unwrap();
        let target = cluster.root.join(format!("bad-{i}"));
        let output = run(Command::new(BIN)
            .args(["serve", "recover-member"])
            .arg(&target)
            .arg("4")
            .arg(cluster.base.to_string())
            .arg(&tls)
            .arg("--deployment")
            .arg(&path));
        assert!(!output.status.success(), "invalid declaration {i}");
        assert!(!target.exists());
    }
    fs::write(&path, format!("voteboat-deployment-v1\n{line}")).unwrap();
    let target = cluster.root.join("no-assignment");
    let output = run(Command::new(BIN)
        .args(["serve", "create"])
        .arg(&target)
        .arg("4")
        .arg(cluster.base.to_string())
        .arg(&tls)
        .arg("--deployment")
        .arg(&path));
    assert!(!output.status.success());
    assert!(!target.exists());
    assert!(!cluster.target("4097", &["status"]).status.success());
    fs::remove_dir_all(&cluster.root).unwrap();
}
fn replicated_history(quic: bool) {
    let mut cluster = Cluster::new();
    cluster.quic = quic;
    if quic {
        let path = cluster.root.join("quic-peers.txt");
        let peers = (1..=3)
            .map(|n| {
                format!(
                    "{n} 127.0.0.1:{} node{n}.voteboat.test\n",
                    cluster.base + 10 + n
                )
            })
            .collect::<String>();
        fs::write(&path, peers).unwrap();
        cluster.endpoints = Some(path);
    }
    for id in 1..=3 {
        cluster.start(id, "create");
    }
    let leader = cluster.leader();
    let query = [
        "configuration-status",
        "340282366920938463463374607431768211455",
    ];
    for id in 1..=3 {
        let status = cluster.wait_configuration_status(id, query[1]);
        assert!(status.contains("evidence=local_durable"));
        assert!(status.contains("committed=NotFoundLocally accepted=NotFoundLocally"));
        assert!(status.contains("action=inconclusive_local_absence"));
        assert!(!cluster
            .request(id, &["configuration-status", "0"])
            .status
            .success());
    }
    assert!(cluster.routed(&["add", "1", "7"]).contains("Value(7)"));
    let retry = cluster.routed(&["add", "1", "7"]);
    assert!(retry.contains("Value(7)") && retry.contains("duplicate=true"));
    assert_eq!(cluster.routed(&["read"]), "OK value=7\n");
    // A local applied value cannot make a follower's read linearizable.
    let follower = leader % 3 + 1;
    let denied = cluster.request(follower, &["read"]);
    assert!(!denied.status.success());
    assert_eq!(
        String::from_utf8(denied.stdout).unwrap(),
        "ERR NOT_LEADER\n"
    );
    let mut killed = cluster.children[leader - 1].take().unwrap();
    killed.kill().unwrap();
    killed.wait().unwrap();
    assert!(cluster.routed(&["add", "2", "3"]).contains("Value(10)"));
    let replacement = cluster.leader();
    assert_ne!(replacement, leader);
    assert_eq!(cluster.routed(&["read"]), "OK value=10\n");
    cluster.start(leader, "recover");
    assert!(cluster
        .wait_configuration_status(leader, query[1])
        .contains("action=inconclusive_local_absence"));
    if quic {
        wait_recovered_quic(&cluster, leader, replacement);
    }
    // Checkpoint admission is asynchronous; shutdown drains admitted work.
    cluster.ok(replacement, &["checkpoint"]);
    cluster.stop();
    check_drained_checkpoint(&cluster, replacement);
    let tls = PathBuf::from(env!("CARGO_MANIFEST_DIR")).join("tests/fixtures/tls");
    let refused = run(Command::new(BIN)
        .args(["serve", "create"])
        .arg(cluster.root.join("1"))
        .arg("1")
        .arg(cluster.base.to_string())
        .arg(tls));
    assert!(
        !refused.status.success(),
        "create must not reset existing state"
    );
    for id in 1..=3 {
        cluster.start(id, "recover");
    }
    assert!(cluster
        .routed(&["add", "2", "3"])
        .contains("duplicate=true"));
    assert_eq!(cluster.routed(&["read"]), "OK value=10\n");
    cluster.stop();
    for id in 1..=3 {
        assert!(
            fs::read_to_string(cluster.root.join(format!("{id}-recover.log")))
                .unwrap()
                .contains("workers_joined=true")
        );
    }
    fs::remove_dir_all(&cluster.root).unwrap();
}
#[test]
fn missing_recovery_and_invalid_configuration_do_not_create_a_store() {
    let mut cluster = Cluster::new();
    // These children are launched directly rather than through start().
    cluster.listeners.remove(&1);
    cluster.listeners.remove(&101);
    let root = cluster.root.join("missing");
    let tls = PathBuf::from(env!("CARGO_MANIFEST_DIR")).join("tests/fixtures/tls");
    for (mode, id) in [("recover", "1"), ("recover-member", "1"), ("create", "4")] {
        let output = run(Command::new(BIN)
            .args(["serve", mode])
            .arg(&root)
            .arg(id)
            .arg(cluster.base.to_string())
            .arg(&tls));
        assert!(!output.status.success());
        assert!(!root.exists());
    }
    let oversized = cluster.root.join("oversized-credentials");
    fs::create_dir(&oversized).unwrap();
    fs::write(oversized.join("ca.der"), vec![0; 65537]).unwrap();
    let output = run(Command::new(BIN)
        .args(["serve", "create"])
        .arg(&root)
        .arg("1")
        .arg(cluster.base.to_string())
        .arg(oversized));
    assert!(!output.status.success());
    assert!(String::from_utf8_lossy(&output.stderr).contains("invalid TLS material size"));
    assert!(!root.exists());
    fs::remove_dir_all(&cluster.root).unwrap();
}

#[cfg(not(feature = "quic"))]
#[test]
fn unavailable_quic_selection_rejects_before_creating_a_store() {
    let cluster = Cluster::new();
    let directory = cluster.root.join("unsupported-quic");
    let output = run(Command::new(BIN)
        .args(["serve", "create"])
        .arg(&directory)
        .arg("1")
        .arg(cluster.base.to_string())
        .arg(PathBuf::from(env!("CARGO_MANIFEST_DIR")).join("tests/fixtures/tls"))
        .args(["--transport", "quic"]));
    assert!(!output.status.success());
    assert!(String::from_utf8_lossy(&output.stderr).contains("--features quic"));
    assert!(!directory.exists());
    fs::remove_dir_all(&cluster.root).unwrap();
}

#[test]
fn bounded_commands_and_quorum_loss_preserve_retry_identity() {
    use std::io::{Read, Write};
    let mut cluster = Cluster::new();
    let path = cluster.root.join("peers.txt");
    fs::write(
        &path,
        (1..=3)
            .map(|id| {
                format!(
                    "{id} 127.0.0.1:{} node{id}.voteboat.test\n",
                    cluster.base + 10 + id
                )
            })
            .collect::<String>(),
    )
    .unwrap();
    cluster.endpoints = Some(path);
    for id in 1..=3 {
        cluster.start(id, "create");
    }
    let leader = cluster.leader();
    let endpoint = (Ipv4Addr::LOCALHOST, cluster.base + 100 + leader as u16);
    let mut stream = std::net::TcpStream::connect(endpoint).unwrap();
    stream
        .set_read_timeout(Some(Duration::from_secs(2)))
        .unwrap();
    stream.write_all(&[b'x'; 256]).unwrap();
    let mut response = String::new();
    stream.take(4096).read_to_string(&mut response).unwrap();
    assert_eq!(response, "ERR command too long\n");
    assert!(!cluster.request(leader, &["add", "0", "7"]).status.success());
    assert!(!cluster
        .request(leader, &["add", "1", "not-a-number"])
        .status
        .success());
    assert!(leader_request(&mut cluster, &["add", "1", "7"]).contains("Value(7)"));
    for id in 1..=3 {
        if id != leader {
            let mut child = cluster.children[id - 1].take().unwrap();
            child.kill().unwrap();
            child.wait().unwrap();
        }
    }
    // The local process and applied counter are alive, but fresh reads need quorum.
    assert!(!cluster.request(leader, &["read"]).status.success());
    let unknown = cluster.request(leader, &["add", "2", "3"]);
    assert!(!unknown.status.success());
    assert!(!String::from_utf8_lossy(&unknown.stdout).contains("OK outcome="));
    for id in 1..=3 {
        if id != leader {
            cluster.start(id, "recover");
        }
    }
    // A sampled role can change during reconnect/election. Model an explicit
    // caller retry of the original intent; the CLI must still stop on Unknown.
    let deadline = Instant::now() + Duration::from_secs(30);
    let leader = loop {
        let candidate = cluster.leader();
        let output = cluster.request(candidate, &["add", "2", "3"]);
        let reply = String::from_utf8(output.stdout).unwrap();
        if output.status.success() {
            assert!(reply.contains("Value(10)"));
            break candidate;
        }
        assert!(
            reply.starts_with("UNKNOWN ") || reply == "ERR NOT_LEADER\n",
            "{reply}"
        );
        assert!(
            Instant::now() < deadline,
            "explicit retry did not resolve: {reply}"
        );
        std::thread::sleep(Duration::from_millis(20));
    };
    // A receipt does not lease this leader for the next observation.
    drain::kill(&mut cluster, leader);
    assert_eq!(leader_request(&mut cluster, &["read"]), "OK value=10\n");
    assert_eq!(
        leader_request(&mut cluster, &["add", "2", "3"]),
        "OK outcome=Value(10) duplicate=true\n"
    );
    cluster.start(leader, "recover");
    cluster.stop();
    for id in 1..=3 {
        cluster.start(id, "recover");
    }
    for (operation, delta, value) in [("1", "7", "7"), ("2", "3", "10")] {
        assert_eq!(
            leader_request(&mut cluster, &["add", operation, delta]),
            format!("OK outcome=Value({value}) duplicate=true\n")
        );
    }
    assert_eq!(leader_request(&mut cluster, &["read"]), "OK value=10\n");
    cluster.stop();
    fs::remove_dir_all(&cluster.root).unwrap();
}

// Fault peers exercise the CLI's routing boundary; actual Raft histories are above.
fn reply_peer(
    listener: TcpListener,
    reply: Option<&'static [u8]>,
) -> std::thread::JoinHandle<Vec<u8>> {
    std::thread::spawn(move || {
        use std::io::{Read, Write};
        listener.set_nonblocking(true).unwrap();
        let deadline = Instant::now() + Duration::from_secs(5);
        let mut stream = loop {
            match listener.accept() {
                Ok((s, _)) => break s,
                Err(e) if e.kind() == std::io::ErrorKind::WouldBlock => (),
                Err(e) => panic!("{e}"),
            }
            assert!(Instant::now() < deadline, "expected a client connection");
            std::thread::sleep(Duration::from_millis(1));
        };
        // BSD sockets can inherit O_NONBLOCK from the listening socket.
        stream.set_nonblocking(false).unwrap();
        stream
            .set_read_timeout(Some(Duration::from_secs(2)))
            .unwrap();
        let mut bytes = [0u8; 256];
        let mut used = 0;
        loop {
            let n = stream.read(&mut bytes[used..]).unwrap();
            assert_ne!(n, 0);
            used += n;
            if bytes[..used].ends_with(b"\n") {
                break;
            }
            assert!(used < bytes.len());
        }
        if let Some(reply) = reply {
            stream.write_all(reply).unwrap();
        }
        bytes[..used].to_vec()
    })
}
#[test]
fn automatic_client_reuses_original_command_after_only_proven_non_acceptance() {
    for first_available in [false, true] {
        let mut cluster = Cluster::new();
        if !first_available {
            cluster.listeners.remove(&101);
        }
        let first = first_available
            .then(|| reply_peer(cluster.take_listener(101), Some(b"ERR NOT_LEADER\n")));
        let second = reply_peer(
            cluster.take_listener(102),
            Some(b"OK outcome=Value(7) duplicate=false\n"),
        );
        let third = cluster.take_listener(103);
        third.set_nonblocking(true).unwrap();
        let reply = cluster.routed(&["add", "42", "7"]);
        assert!(reply.contains("Value(7)"));
        if let Some(first) = first {
            assert_eq!(first.join().unwrap(), b"add 42 7\n");
        }
        assert_eq!(second.join().unwrap(), b"add 42 7\n");
        assert_eq!(
            third.accept().unwrap_err().kind(),
            std::io::ErrorKind::WouldBlock
        );
        std::fs::remove_dir_all(&cluster.root).unwrap();
    }
}
#[test]
fn automatic_client_never_reroutes_uncertain_writes_or_other_errors() {
    for reply in [
        None,
        Some(b"OK outcome=Value(7)".as_slice()),
        Some(b"UNKNOWN LeadershipChanged\n".as_slice()),
        Some(b"ERR Overloaded\n".as_slice()),
        Some(b"ERR NotRead(ReadNotReady)\n".as_slice()),
        Some(b"ERR Unavailable(LeadershipChanged)\n".as_slice()),
        Some(b"ERR Draining\n".as_slice()),
    ] {
        let mut cluster = Cluster::new();
        let first = reply_peer(cluster.take_listener(101), Some(b"ERR NOT_LEADER\n"));
        let second = reply_peer(cluster.take_listener(102), reply);
        let third = cluster.take_listener(103);
        third.set_nonblocking(true).unwrap();
        let result = cluster.target("auto", &["add", "42", "7"]);
        assert!(!result.status.success());
        let stdout = String::from_utf8(result.stdout).unwrap();
        if reply.is_none()
            || reply.is_some_and(|r| !r.ends_with(b"\n") || r.starts_with(b"UNKNOWN"))
        {
            assert!(stdout.starts_with("UNKNOWN "), "{stdout}");
        }
        assert_eq!(first.join().unwrap(), b"add 42 7\n");
        assert_eq!(second.join().unwrap(), b"add 42 7\n");
        assert_eq!(
            third.accept().unwrap_err().kind(),
            std::io::ErrorKind::WouldBlock
        );
        let rejected = cluster.target("auto", &["quit"]);
        assert!(!rejected.status.success());
        assert_eq!(
            third.accept().unwrap_err().kind(),
            std::io::ErrorKind::WouldBlock
        );
        std::fs::remove_dir_all(&cluster.root).unwrap();
    }
}
#[test]
fn automatic_read_retries_only_exact_unavailable_replica_replies() {
    for reply in [
        b"ERR NotRead(ReadNotReady)\n".as_slice(),
        b"ERR Unavailable(LeadershipChanged)\n",
        b"ERR Draining\n",
        b"ERR Draining trailing\n",
        b"ERR NotRead(StaleRead)\n",
        b"ERR Unavailable(OwnerFailed)\n",
        b"ERR Unavailable(Cancelled)\n",
        b"ERR Unavailable(Aborted)\n",
        b"UNKNOWN LeadershipChanged\n",
        b"ERR Unavailable(LeadershipChanged) trailing\n",
        b"ERR Unavailable(LeadershipChanged)",
    ] {
        let mut cluster = Cluster::new();
        let first = reply_peer(cluster.take_listener(101), Some(reply));
        let second = cluster.take_listener(102);
        if matches!(
            reply,
            b"ERR NotRead(ReadNotReady)\n"
                | b"ERR Unavailable(LeadershipChanged)\n"
                | b"ERR Draining\n"
        ) {
            let second = reply_peer(second, Some(b"OK value=7\n"));
            assert_eq!(cluster.routed(&["read"]), "OK value=7\n");
            assert_eq!(second.join().unwrap(), b"read\n");
        } else {
            second.set_nonblocking(true).unwrap();
            assert!(!cluster.target("auto", &["read"]).status.success());
            assert_eq!(
                second.accept().unwrap_err().kind(),
                std::io::ErrorKind::WouldBlock
            );
        }
        assert_eq!(first.join().unwrap(), b"read\n");
        fs::remove_dir_all(&cluster.root).unwrap();
    }
    for reply in [
        b"ERR NotRead(ReadNotReady)\n".as_slice(),
        b"ERR Unavailable(LeadershipChanged)\n",
        b"ERR Draining\n",
    ] {
        let mut cluster = Cluster::new();
        let first = reply_peer(cluster.take_listener(101), Some(reply));
        assert!(!cluster.request(1, &["read"]).status.success());
        assert_eq!(first.join().unwrap(), b"read\n");
        fs::remove_dir_all(&cluster.root).unwrap();
    }
}

#[test]
fn trickled_reply_cannot_extend_the_clients_absolute_routing_deadline() {
    use std::io::{Read, Write};
    let mut cluster = Cluster::new();
    let first = reply_peer(cluster.take_listener(101), Some(b"ERR NOT_LEADER\n"));
    let second = cluster.take_listener(102);
    second.set_nonblocking(true).unwrap();
    let third = cluster.take_listener(103);
    third.set_nonblocking(true).unwrap();
    let slow = std::thread::spawn(move || {
        let deadline = Instant::now() + Duration::from_secs(20);
        let mut stream = loop {
            match second.accept() {
                Ok((s, _)) => break s,
                Err(e) if e.kind() == std::io::ErrorKind::WouldBlock => (),
                Err(e) => panic!("{e}"),
            }
            assert!(Instant::now() < deadline);
            std::thread::sleep(Duration::from_millis(1));
        };
        stream
            .set_read_timeout(Some(Duration::from_secs(2)))
            .unwrap();
        let mut request = [0; 256];
        let mut used = 0;
        loop {
            let n = stream.read(&mut request[used..]).unwrap();
            assert_ne!(n, 0);
            used += n;
            if request[..used].ends_with(b"\n") {
                break;
            }
        }
        stream.set_nonblocking(true).unwrap();
        // Continual incomplete progress must not reset the client's deadline.
        while Instant::now() < deadline {
            match stream.write(b"x") {
                Ok(1) => (),
                Err(e) if e.kind() == std::io::ErrorKind::WouldBlock => (),
                _ => break,
            }
            std::thread::sleep(Duration::from_millis(20));
        }
        request[..used].to_vec()
    });
    let start = Instant::now();
    let output = cluster.target("auto", &["add", "42", "7"]);
    assert!(
        start.elapsed() < Duration::from_millis(11500),
        "routing deadline was extended"
    );
    assert!(!output.status.success());
    assert!(String::from_utf8_lossy(&output.stdout).starts_with("UNKNOWN "));
    assert_eq!(first.join().unwrap(), b"add 42 7\n");
    assert_eq!(slow.join().unwrap(), b"add 42 7\n");
    assert_eq!(
        third.accept().unwrap_err().kind(),
        std::io::ErrorKind::WouldBlock
    );
    std::fs::remove_dir_all(&cluster.root).unwrap();
}

fn metrics_history(quic: bool) {
    fn metrics(cluster: &Cluster, id: usize) -> BTreeMap<String, u64> {
        let deadline = Instant::now() + Duration::from_secs(10);
        let reply = loop {
            let output = cluster.request(id, &["metrics"]);
            if output.status.success() {
                break String::from_utf8(output.stdout).unwrap();
            }
            assert!(
                Instant::now() < deadline,
                "metrics endpoint did not become ready"
            );
            std::thread::sleep(Duration::from_millis(10));
        };
        assert!(reply.starts_with("OK evidence=local_volatile "), "{reply}");
        assert!(reply.len() < 4096);
        reply
            .split_whitespace()
            .skip(2)
            .map(|field| {
                let (key, value) = field.split_once('=').unwrap();
                (key.to_string(), value.parse().unwrap())
            })
            .collect()
    }
    let mut cluster = Cluster::new();
    cluster.quic = quic;
    for id in 1..=3 {
        cluster.start(id, "create");
    }
    let leader = cluster.leader();
    let before = metrics(&cluster, leader);
    assert!(before["polls"] > 0);
    assert!(authenticated_write(&cluster, &["add", "1", "7"]).contains("Value(7)"));
    assert!(authenticated_write(&cluster, &["add", "1", "7"]).contains("duplicate=true"));
    assert!(cluster.routed(&["read"]).contains("value=7"));
    // The sampled peer may have lost leadership. Wait for its actual replay/
    // replication counters; another peer's metrics cannot satisfy this check.
    let deadline = Instant::now() + Duration::from_secs(10);
    let after = loop {
        let after = metrics(&cluster, leader);
        if ["applications", "persistence_batches", "peer_received"]
            .iter()
            .all(|key| after[*key] > before[*key])
        {
            break after;
        }
        assert!(
            Instant::now() < deadline,
            "sampled peer did not apply: {after:?}"
        );
        std::thread::park_timeout(Duration::from_millis(10));
    };
    for (key, value) in &before {
        if !key.starts_with("outbound_")
            && (!key.starts_with("peer_") || matches!(key.as_str(), "peer_sends" | "peer_received"))
        {
            assert!(after[key] >= *value, "{key}");
        }
    }
    failure_diagnostics::check_peer_metrics(&after);
    assert!(after["applications"] > before["applications"]);
    assert!(after["persistence_batches"] > before["persistence_batches"]);
    assert!(after["peer_received"] > before["peer_received"]);
    let old_first = metrics(&cluster, 1);
    cluster.stop();
    // With no quorum, this restarted process cannot apply a new term's no-op.
    // Recovery replay is outside poll diagnostics, so the new collector starts
    // with zero application deliveries while durable application state survives.
    cluster.start(1, "recover");
    let fresh = metrics(&cluster, 1);
    assert!(fresh["store_session"] > old_first["store_session"]);
    assert_eq!(fresh["applications"], 0);
    for id in 2..=3 {
        cluster.start(id, "recover");
    }
    cluster.leader();
    assert!(authenticated_write(&cluster, &["add", "1", "7"]).contains("duplicate=true"));
    assert!(cluster.routed(&["read"]).contains("value=7"));
    assert!(authenticated_write(&cluster, &["add", "2", "3"]).contains("Value(10)"));
    cluster.stop();
    fs::remove_dir_all(&cluster.root).unwrap();
}
#[test]
fn native_tcp_metrics_are_volatile_and_preserve_recovery_and_retries() {
    metrics_history(false);
}
#[cfg(feature = "quic")]
#[test]
fn native_quic_metrics_are_volatile_and_preserve_recovery_and_retries() {
    metrics_history(true);
}

// Explicit caller recovery keeps the original operation ID and payload. The CLI
// itself still stops on uncertainty; only an actual success returns a receipt.
fn authenticated_write(cluster: &Cluster, args: &[&str]) -> String {
    write_recovery::invoke(cluster, args, Duration::from_secs(10))
}
fn authenticated_command_history(quic: bool) {
    let mut cluster = Cluster::new();
    cluster.quic = quic;
    let access = cluster.root.join("service-access.txt");
    fs::write(
        &access,
        "voteboat-service-access-v1 1\n1 reader 1 1\n2 writer 1 1\n3 admin 1 1\n",
    )
    .unwrap();
    cluster.command_access = Some(access.clone());
    cluster.command_principal = Some(3);
    for id in 1..=3 {
        cluster.start(id, "create");
    }
    let leader = cluster.leader();
    cluster.command_principal = Some(1);
    assert_eq!(cluster.routed(&["read"]), "OK value=0\n");
    for args in [vec!["add", "11001", "7"], vec!["checkpoint"], vec!["quit"]] {
        let denied = cluster.request(leader, &args);
        assert!(!denied.status.success());
        assert_eq!(
            String::from_utf8(denied.stdout).unwrap(),
            "ERR AUTHORIZATION\n"
        );
    }
    cluster.command_principal = Some(2);
    assert!(authenticated_write(&cluster, &["add", "11001", "7"]).contains("Value(7)"));
    assert!(authenticated_write(&cluster, &["add", "11001", "7"]).contains("duplicate=true"));
    assert_eq!(cluster.routed(&["read"]), "OK value=7\n");
    assert_eq!(
        String::from_utf8(cluster.request(leader, &["quit"]).stdout).unwrap(),
        "ERR AUTHORIZATION\n"
    );
    // Plain commands cannot use the TLS-only access mode or change state.
    cluster.command_principal = None;
    assert!(!cluster
        .request(leader, &["add", "11002", "100"])
        .status
        .success());
    refuse_spoofed_reader(&cluster);
    cluster.command_principal = Some(3);
    assert_eq!(cluster.routed(&["read"]), "OK value=7\n");
    cluster.ok(leader, &["checkpoint"]);
    cluster.stop();
    let state = recovered_state_for(
        &cluster.root.join(leader.to_string()),
        leader as u64,
        voteboat::identity::StoreIdentity {
            id: voteboat::identity::StoreId::new(leader as u128).unwrap(),
            incarnation: voteboat::identity::StoreIncarnation::new(1).unwrap(),
        },
        7,
        (11001, 7),
    );
    assert!(state.base_index() > 0);
    // Restart selects a fresh access generation and revokes the former writer.
    fs::write(
        &access,
        "voteboat-service-access-v1 2\n1 reader 1 1\n2 reader 1 1\n3 admin 1 1\n",
    )
    .unwrap();
    for id in 1..=3 {
        cluster.start(id, "recover");
    }
    let leader = cluster.leader();
    cluster.command_principal = Some(2);
    assert_eq!(
        String::from_utf8(cluster.request(leader, &["add", "11001", "7"]).stdout).unwrap(),
        "ERR AUTHORIZATION\n"
    );
    assert_eq!(cluster.routed(&["read"]), "OK value=7\n");
    cluster.command_principal = Some(3);
    assert!(authenticated_write(&cluster, &["add", "11001", "7"]).contains("duplicate=true"));
    assert!(authenticated_write(&cluster, &["add", "11003", "3"]).contains("Value(10)"));
    cluster.stop();
    for id in 1..=3 {
        assert!(
            fs::read_to_string(cluster.root.join(format!("{id}-recover.log")))
                .unwrap()
                .contains("workers_joined=true")
        );
    }
    fs::remove_dir_all(&cluster.root).unwrap();
}
#[test]
fn authenticated_service_principals_scope_commands_and_recover_tcp() {
    authenticated_command_history(false);
}
#[cfg(feature = "quic")]
#[test]
fn authenticated_service_principals_scope_commands_and_recover_quic() {
    authenticated_command_history(true);
}
#[test]
fn invalid_service_access_fails_before_store_creation_or_listener_ownership() {
    let cluster = Cluster::new();
    let access = cluster.root.join("invalid-access.txt");
    fs::write(
        &access,
        "voteboat-service-access-v1 1\n1 reader 1 1\n1 admin 1 1\n",
    )
    .unwrap();
    let directory = cluster.root.join("must-not-exist");
    let fixtures = PathBuf::from(env!("CARGO_MANIFEST_DIR")).join("tests/fixtures/tls");
    let output = run(Command::new(BIN)
        .args(["serve", "create"])
        .arg(&directory)
        .args(["1", &cluster.base.to_string()])
        .arg(fixtures)
        .arg("--service-access")
        .arg(access));
    assert!(!output.status.success());
    assert!(!directory.exists());
}

fn remote_plan_command(cluster: &mut Cluster, operation: &str, success: bool) -> String {
    let deadline = Instant::now() + Duration::from_secs(10);
    loop {
        let leader = cluster.leader();
        let output = cluster.request(leader, &["configure", operation]);
        let text = String::from_utf8(output.stdout).unwrap();
        // A leader can change after discovery or during an admitted mutation.
        // Retry only this documented uncertainty with the identical operation;
        // an expected deterministic refusal must never be retried as success.
        let leadership_changed = success
            && text == "UNKNOWN LeadershipChanged; retry the same configuration operation ID and record\n";
        if text != "ERR NOT_LEADER\n" && !leadership_changed {
            assert_eq!(
                output.status.success(),
                success,
                "operation={operation}: {text} {}",
                String::from_utf8_lossy(&output.stderr)
            );
            return text;
        }
        assert!(
            Instant::now() < deadline,
            "configuration leader did not settle: {text}"
        );
    }
}
fn remote_configuration_history(quic: bool) {
    let mut cluster = Cluster::new();
    cluster.quic = quic;
    let access = cluster.root.join("remote-access.txt");
    fs::write(
        &access,
        "voteboat-service-access-v1 1\n1 reader 1 1\n1 admin 1 99\n2 writer 1 1\n2 admin 99 1\n3 admin 1 1\n",
    )
    .unwrap();
    cluster.command_access = Some(access);
    cluster.command_principal = Some(3);
    for id in 1..=3 {
        cluster.start(id, "create");
    }
    cluster.leader();
    assert!(authenticated_write(&cluster, &["add", "15000", "42"]).contains("Value(42)"));
    cluster.stop();
    let plan = cluster.root.join("remote-plan.txt");
    fs::write(&plan, "voteboat-counter-admin-v1\nplacement 2 false\nreplica 1 1\nreplica 2 2\nreplica 3 3\nlearners 15001 1 2 - m:3 v:1 v:2 v:3\njoint 15003 2 3 4 - m:3 v:1 v:2 v:3\nfinal 15003 3 4\n").unwrap();
    cluster.admin_plan = Some(plan);
    cluster.remote_admin = true;
    for id in 1..=3 {
        cluster.start(id, "recover-member");
    }
    let leader = cluster.leader();
    let follower = (1..=3).find(|id| *id != leader).unwrap();
    let refused = cluster.request(follower, &["configure", "15001"]);
    assert_eq!(
        String::from_utf8(refused.stdout).unwrap(),
        "ERR NOT_LEADER\n"
    );
    assert!(cluster
        .ok(leader, &["configuration-status", "15001"])
        .contains("inconclusive_local_absence"));
    for principal in [1, 2] {
        cluster.command_principal = Some(principal);
        let denied = cluster.request(leader, &["configure", "15001"]);
        assert!(!denied.status.success());
        assert_eq!(
            String::from_utf8(denied.stdout).unwrap(),
            "ERR AUTHORIZATION\n"
        );
    }
    cluster.command_principal = Some(3);
    let missing = remote_plan_command(&mut cluster, "15002", false);
    assert!(
        missing.contains("absent from provisioned plan"),
        "{missing}"
    );
    assert!(cluster
        .ok(leader, &["configuration-status", "15001"])
        .contains("inconclusive_local_absence"));
    let applied = remote_plan_command(&mut cluster, "15001", true);
    assert!(applied.contains("committed_index="), "{applied}");
    assert!(remote_plan_command(&mut cluster, "15001", true).contains("action=completed"));
    assert!(remote_plan_command(&mut cluster, "15003", true).contains("committed_index="));
    assert!(remote_plan_command(&mut cluster, "15003", true).contains("committed_index="));
    assert!(remote_plan_command(&mut cluster, "15003", true).contains("action=completed"));
    assert!(authenticated_write(&cluster, &["add", "15000", "42"]).contains("duplicate=true"));
    assert_eq!(cluster.routed(&["read"]), "OK value=42\n");
    for id in 1..=3 {
        cluster.ok(id, &["checkpoint"]);
    }
    cluster.stop();
    for id in 1..=3 {
        cluster.start(id, "recover-member");
    }
    cluster.leader();
    assert!(remote_plan_command(&mut cluster, "15001", true).contains("action=completed"));
    assert!(authenticated_write(&cluster, &["add", "15000", "42"]).contains("duplicate=true"));
    assert_eq!(cluster.routed(&["read"]), "OK value=42\n");
    cluster.stop();
    fs::remove_dir_all(&cluster.root).unwrap();
}
#[test]
fn remote_configuration_requires_admin_and_preserves_checkpoint_retries_tcp() {
    remote_configuration_history(false);
}
#[cfg(feature = "quic")]
#[test]
fn remote_configuration_requires_admin_and_preserves_checkpoint_retries_quic() {
    remote_configuration_history(true);
}

#[test]
fn remote_configuration_requires_access_before_opening_files_or_listeners() {
    let cluster = Cluster::new();
    let missing = cluster.root.join("not-created");
    let tls = PathBuf::from(env!("CARGO_MANIFEST_DIR")).join("tests/fixtures/tls");
    let result = run(Command::new(BIN)
        .args(["serve", "recover-member"])
        .arg(&missing)
        .arg("1")
        .arg(cluster.base.to_string())
        .arg(tls)
        .arg("--remote-admin-plan")
        .arg(cluster.root.join("missing-plan")));
    assert!(!result.status.success());
    assert!(String::from_utf8_lossy(&result.stderr)
        .contains("remote administration requires service access"));
    assert!(!missing.exists());
    fs::remove_dir_all(&cluster.root).unwrap();
}

#[test]
fn interrupted_configuration_reply_is_unknown_and_preserves_original_operation() {
    let mut cluster = Cluster::new();
    let peer = reply_peer(cluster.take_listener(101), None);
    let result = cluster.request(1, &["configure", "15001"]);
    assert!(!result.status.success());
    assert!(String::from_utf8_lossy(&result.stdout).starts_with("UNKNOWN "));
    assert!(String::from_utf8_lossy(&result.stdout).contains("same configuration operation ID"));
    assert_eq!(peer.join().unwrap(), b"configure 15001\n");
    fs::remove_dir_all(&cluster.root).unwrap();
}

// Select a current leader; preserve exact arguments on documented uncertainty.
// Administration has no CLI auto mode.
fn retryable_leader_response(args: &[&str], text: &str) -> bool {
    match args {
        ["read"] => matches!(text, "ERR NOT_LEADER\n" | "ERR NotRead(ReadNotReady)\n" | "ERR Unavailable(LeadershipChanged)\n"),
        ["resume-leadership" | "leadership-status", _] => matches!(text, "ERR NOT_LEADER\n" | "ERR NotRead(ReadNotReady)\n" | "ERR Unavailable(LeadershipChanged)\n"),
        ["add", _, _] => matches!(text, "ERR NOT_LEADER\n" | "UNKNOWN LeadershipChanged; retry the same operation ID and delta\n"),
        ["configure-record", _] => matches!(text, "ERR NOT_LEADER\n" | "UNKNOWN LeadershipChanged; retry the same configuration operation ID and record\n" | "UNKNOWN exact record locally durable but not committed; preserve original record\n"),
        ["cancel-leadership", _] => matches!(text, "ERR NOT_LEADER\n" | "UNKNOWN LeadershipChanged; retry the same operation ID and delta\n" | "UNKNOWN LeadershipChanged; retry the same administrative operation ID and record\n" | "ERR Unavailable(LeadershipChanged)\n"),
        _ => false,
    }
}
fn leader_request(cluster: &mut Cluster, args: &[&str]) -> String {
    let deadline = Instant::now() + Duration::from_secs(10);
    loop {
        let leader = cluster.leader();
        let output = cluster.request(leader, args);
        let text = String::from_utf8(output.stdout).unwrap();
        if output.status.success() {
            return text;
        }
        assert!(
            retryable_leader_response(args, &text),
            "{args:?}: {text} {}",
            String::from_utf8_lossy(&output.stderr)
        );
        assert!(
            Instant::now() < deadline,
            "configuration did not settle: {text}"
        );
        std::thread::park_timeout(Duration::from_millis(10));
    }
}
fn client_supplied_configuration_history(quic: bool) {
    let mut cluster = Cluster::new();
    cluster.quic = quic;
    let access = cluster.root.join("target-access.txt");
    fs::write(
        &access,
        "voteboat-service-access-v1 1\n1 reader 1 1\n2 writer 1 1\n3 admin 1 1\n",
    )
    .unwrap();
    cluster.command_access = Some(access);
    cluster.command_principal = Some(3);
    for id in 1..=3 {
        cluster.start(id, "create");
    }
    cluster.leader();
    assert!(authenticated_write(&cluster, &["add", "16000", "42"]).contains("Value(42)"));
    cluster.stop();
    let policy = cluster.root.join("target-policy.txt");
    // No predeclared operation identity or target configuration.
    fs::write(
        &policy,
        "voteboat-counter-admin-v1\nplacement 2 false\nreplica 1 1\nreplica 2 2\nreplica 3 3\n",
    )
    .unwrap();
    cluster.admin_plan = Some(policy);
    cluster.targets_admin = true;
    for id in 1..=3 {
        cluster.start(id, "recover-member");
    }
    let leader = cluster.leader();
    refuse_invalid_configurations(&mut cluster, leader);
    let record = "learners 16001 1 2 - m:3 v:1 v:2 v:3";
    assert!(
        leader_request(&mut cluster, &["configure-record", record]).contains("committed_index=")
    );
    assert!(leader_request(&mut cluster, &["configure-record", record]).contains("duplicate=true"));
    let leader = cluster.leader();
    let conflict = cluster.request(
        leader,
        &["configure-record", "learners 16001 1 99 - m:3 v:1 v:2 v:3"],
    );
    assert!(String::from_utf8(conflict.stdout)
        .unwrap()
        .contains("conflicts with retained record"));
    let joint = "joint 16003 2 3 4 - w:3 1 v:1 1 v:2 1 v:3";
    assert!(leader_request(&mut cluster, &["configure-record", joint]).contains("committed_index="));
    assert!(leader_request(&mut cluster, &["configure-record", joint]).contains("duplicate=true"));
    let final_record = "final 16003 3 4";
    assert!(
        leader_request(&mut cluster, &["configure-record", final_record])
            .contains("committed_index=")
    );
    assert!(
        leader_request(&mut cluster, &["configure-record", final_record])
            .contains("duplicate=true")
    );
    for id in 1..=3 {
        cluster.ok(id, &["checkpoint"]);
    }
    cluster.stop();
    for id in 1..=3 {
        cluster.start(id, "recover-member");
    }
    let leader = cluster.leader();
    let old = cluster.request(leader, &["configure-record", record]);
    assert!(!old.status.success());
    assert!(String::from_utf8(old.stdout)
        .unwrap()
        .contains("comparison history unavailable"));
    assert!(cluster
        .ok(leader, &["configuration-status", "16001"])
        .contains("action=completed"));
    assert!(leader_request(
        &mut cluster,
        &[
            "configure-record",
            "learners 16004 4 5 - w:3 1 v:1 1 v:2 1 v:3"
        ]
    )
    .contains("committed_index="));
    assert!(authenticated_write(&cluster, &["add", "16000", "42"]).contains("duplicate=true"));
    assert_eq!(cluster.routed(&["read"]), "OK value=42\n");
    let oversized = "x".repeat(257);
    let refused = cluster.request(leader, &["configure-record", &oversized]);
    assert!(!refused.status.success());
    assert!(String::from_utf8_lossy(&refused.stderr).contains("command too long"));
    cluster.stop();
    fs::remove_dir_all(&cluster.root).unwrap();
}
#[test]
fn client_supplied_configuration_targets_are_checked_and_recovered_tcp() {
    client_supplied_configuration_history(false);
}
#[cfg(feature = "quic")]
#[test]
fn client_supplied_configuration_targets_are_checked_and_recovered_quic() {
    client_supplied_configuration_history(true);
}

/// A real authenticated command channel whose application reply is never read.
/// TLS polling can buffer ciphertext/plaintext; that is not client observation.
struct UnobservedCommand {
    session: voteboat::native::tls::NativeTlsSession<std::net::TcpStream>,
    socket: std::net::TcpStream,
    start: Instant,
}
impl UnobservedCommand {
    fn send(cluster: &Cluster, target: usize, command: &str) -> Self {
        use std::io::Write;
        use voteboat::{identity::*, native::tls::*, runtime::MonoTime, secure::*};
        let fixtures = cluster.tls.clone().unwrap_or_else(|| {
            PathBuf::from(env!("CARGO_MANIFEST_DIR")).join("tests/fixtures/tls")
        });
        let config = NativeTlsConfig::new(TlsCredentials {
            roots: vec![fs::read(fixtures.join("ca.der")).unwrap()],
            certificate_chain: vec![fs::read(fixtures.join("node3.der")).unwrap()],
            private_key: fs::read(fixtures.join("node3-key.der")).unwrap(),
        })
        .unwrap();
        let identity = |number: u64, client: bool| {
            let namespace = if client { 1u64 << 63 } else { 1u64 << 62 };
            PeerIdentity {
                node: NodeId::new(namespace | number).unwrap(),
                store: StoreIdentity {
                    id: StoreId::new((1u128 << 127) | u128::from(namespace | number)).unwrap(),
                    incarnation: StoreIncarnation::new(1).unwrap(),
                },
            }
        };
        let mut stream =
            std::net::TcpStream::connect((Ipv4Addr::LOCALHOST, cluster.base + 100 + target as u16))
                .unwrap();
        stream.write_all(b"3\n").unwrap();
        stream.set_nonblocking(true).unwrap();
        let socket = stream.try_clone().unwrap();
        let local = identity(3, true);
        let start = Instant::now();
        let mut session = NativeTlsSession::client(
            stream,
            &config,
            LocalIdentity {
                node: local.node,
                store: StoreBinding {
                    identity: local.store,
                    session: StoreSession::new(1).unwrap(),
                },
            },
            TlsPeer {
                identity: identity(target as u64, false),
                certificate: fs::read(fixtures.join(format!("node{target}.der"))).unwrap(),
                server_name: format!("node{target}.voteboat.test"),
            },
            SecureSessionGeneration::new(1).unwrap(),
            SessionLimits::default(),
            MonoTime(0),
        )
        .unwrap();
        let bytes = format!("{command}\n").into_bytes();
        assert!(bytes.len() <= 256);
        let mut sent = 0;
        loop {
            assert!(
                start.elapsed() < Duration::from_secs(5),
                "command send timed out"
            );
            session
                .poll(
                    MonoTime(start.elapsed().as_millis() as u64),
                    SessionPollBudget::default(),
                )
                .unwrap();
            if session.state() == SessionState::Ready && sent < bytes.len() {
                match session.write_plaintext(&bytes[sent..]) {
                    Ok(n) => sent += n,
                    Err(SessionError::WouldBlock) => (),
                    other => panic!("command send failed: {other:?}"),
                }
            }
            if sent == bytes.len() && session.is_flushed() {
                break;
            }
            std::thread::park_timeout(Duration::from_millis(1));
        }
        Self {
            session,
            socket,
            start,
        }
    }
    fn close_notify(&mut self) {
        use voteboat::{runtime::MonoTime, secure::*};
        self.session.close();
        let deadline = Instant::now() + Duration::from_secs(2);
        while !self.session.is_flushed() {
            assert!(Instant::now() < deadline, "TLS close did not flush");
            let result = self.session.poll(
                MonoTime(self.start.elapsed().as_millis() as u64),
                SessionPollBudget::default(),
            );
            assert!(result.is_ok() || self.session.is_flushed(), "{result:?}");
            std::thread::park_timeout(Duration::from_millis(1));
        }
        // Keep the TCP handle open: server cancellation must observe TLS close,
        // rather than a TCP EOF or the original command deadline.
    }
    fn disconnect(self) {
        let _ = self.socket.shutdown(std::net::Shutdown::Both);
    }
}
fn wait_administration_event(cluster: &Cluster, id: usize, event: &str) {
    let deadline = Instant::now() + Duration::from_secs(12);
    let path = cluster.root.join(format!("{id}-recover-member.log"));
    loop {
        let text = fs::read_to_string(&path).unwrap();
        if text.contains(event) {
            return;
        }
        assert!(
            Instant::now() < deadline,
            "missing {event}; logs at {path:?}: {text}\n{}",
            failure_diagnostics::snapshot(cluster, &["status"])
        );
        std::thread::park_timeout(Duration::from_millis(5));
    }
}
fn retry_configuration_record(cluster: &mut Cluster, record: &str) {
    let deadline = Instant::now() + Duration::from_secs(10);
    loop {
        let leader = cluster.leader();
        let output = cluster.request(leader, &["configure-record", record]);
        if output.status.success() {
            return;
        }
        let text = String::from_utf8(output.stdout).unwrap();
        assert!(
            retryable_leader_response(&["configure-record", record], &text),
            "configuration retry failed: {text} {}",
            String::from_utf8_lossy(&output.stderr)
        );
        assert!(
            Instant::now() < deadline,
            "leadership failed to settle: {text}"
        );
        std::thread::park_timeout(Duration::from_millis(10));
    }
}
fn interrupted_native_configuration_history(quic: bool) {
    let mut cluster = Cluster::new();
    cluster.quic = quic;
    let access = cluster.root.join("interruption-access.txt");
    fs::write(&access, "voteboat-service-access-v1 1\n3 admin 1 1\n").unwrap();
    cluster.command_access = Some(access);
    cluster.command_principal = Some(3);
    for id in 1..=3 {
        cluster.start(id, "create");
    }
    cluster.leader();
    assert!(authenticated_write(&cluster, &["add", "17000", "42"]).contains("Value(42)"));
    cluster.stop();
    let policy = cluster.root.join("interruption-policy.txt");
    fs::write(
        &policy,
        "voteboat-counter-admin-v1\nplacement 2 false\nreplica 1 1\nreplica 2 2\nreplica 3 3\n",
    )
    .unwrap();
    cluster.admin_plan = Some(policy);
    cluster.targets_admin = true;
    for id in 1..=3 {
        cluster.start(id, "recover-member");
    }
    retry_configuration_record(&mut cluster, "joint 17010 1 2 3 3 m:2 v:1 v:2");
    retry_configuration_record(&mut cluster, "final 17010 2 3");
    let mut learner = cluster.children[2].take().unwrap();
    learner.kill().unwrap();
    learner.wait().unwrap();
    let leader = cluster.leader();
    cancel_closed_and_expired(&cluster, leader);
    let abandoned = UnobservedCommand::send(
        &cluster,
        leader,
        "configure-record joint 17015 3 4 5 - m:3 v:1 v:2 v:3",
    );
    wait_administration_event(
        &cluster,
        leader,
        "administration operation=17015 preparing_learner=3",
    );
    // The prepared target has no live learner proof and cannot be proposed.
    // Reopen the killed serving voter before asking for election: this current
    // two-voter policy requires both voters, independent of the future target.
    let mut preparing = cluster.children[leader - 1].take().unwrap();
    preparing.kill().unwrap();
    preparing.wait().unwrap();
    abandoned.disconnect();
    cluster.start(leader, "recover-member");
    let leader = cluster.leader();
    assert!(cluster
        .ok(leader, &["configuration-status", "17015"])
        .contains("inconclusive_local_absence"));
    cluster.start(3, "recover-member");
    // A new live learner cannot resurrect a canceled target without a new request.
    assert!(cluster
        .ok(leader, &["configuration-status", "17011"])
        .contains("inconclusive_local_absence"));
    assert!(cluster
        .ok(leader, &["configuration-status", "17012"])
        .contains("inconclusive_local_absence"));
    assert!(cluster
        .ok(leader, &["configuration-status", "17015"])
        .contains("inconclusive_local_absence"));
    assert!(leader_request(
        &mut cluster,
        &["configure-record", "joint 17012 3 4 5 - m:3 v:1 v:2 v:3"]
    )
    .contains("committed_index="));
    assert!(
        leader_request(&mut cluster, &["configure-record", "final 17012 4 5"])
            .contains("committed_index=")
    );
    let leader = cluster.leader();
    let record = "learners 17013 5 6 - m:3 v:1 v:2 v:3";
    let lost = UnobservedCommand::send(&cluster, leader, &format!("configure-record {record}"));
    wait_administration_event(
        &cluster,
        leader,
        "administration operation=17013 outcome=Committed(",
    );
    // The original client has never read plaintext. Kill after observed durable
    // commitment, then resolve the identical record through a different leader.
    let mut failed = cluster.children[leader - 1].take().unwrap();
    failed.kill().unwrap();
    failed.wait().unwrap();
    lost.disconnect();
    let replacement = cluster.leader();
    assert_ne!(replacement, leader);
    assert!(authenticated_write(&cluster, &["add", "17014", "0"]).contains("Value(42)"));
    assert!(leader_request(&mut cluster, &["configure-record", record]).contains("duplicate=true"));
    check_interrupted_record_conflict(&cluster, replacement);
    cluster.start(leader, "recover-member");
    assert!(authenticated_write(&cluster, &["add", "17000", "42"]).contains("duplicate=true"));
    assert_eq!(cluster.routed(&["read"]), "OK value=42\n");
    cluster.stop();
    check_interrupted_membership(&cluster);
    fs::remove_dir_all(&cluster.root).unwrap();
}
#[test]
fn native_configuration_close_deadline_and_unread_commit_recover_tcp() {
    interrupted_native_configuration_history(false);
}
#[cfg(feature = "quic")]
#[test]
fn native_configuration_close_deadline_and_unread_commit_recover_quic() {
    interrupted_native_configuration_history(true);
}

fn seed_fourth_learner(cluster: &Cluster, fourth: voteboat::identity::StoreIdentity) {
    use voteboat::{identity::*, log::*, membership::*, native::log_store::*};
    {
        let _gate = fixture_gate();
        for local in 1..=2 {
            let mut log = NativeLogStore::recover(
                FileLogIo::open(cluster.root.join(local.to_string())).unwrap(),
                StoreIdentity {
                    id: StoreId::new(local).unwrap(),
                    incarnation: StoreIncarnation::new(1).unwrap(),
                },
                LogLimits::default(),
            )
            .unwrap();
            let group = GroupIdentity {
                id: GroupId::new(1).unwrap(),
                incarnation: GroupIncarnation::new(1).unwrap(),
            };
            let state = log.state(group).unwrap();
            let membership = state.membership_at(state.commit_index).unwrap();
            let next = Configuration::new(
                ConfigurationId::new(4).unwrap(),
                membership.stable().policy().clone(),
                membership.stable().voter_stores().clone(),
                [(NodeId::new(4).unwrap(), fourth)].into(),
            )
            .unwrap();
            let tickets = log
                .append_batch(vec![LogMutation::Update(LogUpdate {
                    group,
                    expected_revision: state.revision,
                    hard_state: state.hard_state,
                    commit_index: 3,
                    suffix: Some(Suffix {
                        from: 3,
                        entries: vec![LogEntry {
                            index: 3,
                            term: 1,
                            payload: EntryPayload::Configuration(Box::new(ConfigurationRecord {
                                operation: OperationId::new(502).unwrap(),
                                expected: ConfigurationId::new(3).unwrap(),
                                change: ConfigurationChange::Learners(next),
                            })),
                        }],
                    }),
                    snapshot: None,
                    snapshot_membership: None,
                })])
                .unwrap();
            log.barrier(&tickets).unwrap();
        }
    }
}

fn wait_recovered_quic(cluster: &Cluster, leader: usize, replacement: usize) {
    // Wait for the surviving peers' idle detection and fresh session lease,
    // then prove the recovered former leader receives newly committed work.
    assert!(cluster.routed(&["add", "3", "0"]).contains("Value(10)"));
    let deadline = Instant::now() + Duration::from_secs(20);
    loop {
        let recovered = cluster.request(leader, &["status"]);
        let active = cluster.request(replacement, &["status"]);
        let committed = |bytes: &[u8]| {
            String::from_utf8_lossy(bytes)
                .split("committed=")
                .nth(1)
                .and_then(|s| s.trim().parse::<u64>().ok())
        };
        if recovered.status.success()
            && active.status.success()
            && committed(&recovered.stdout)
                .is_some_and(|i| i > 0 && Some(i) == committed(&active.stdout))
        {
            break;
        }
        assert!(
            Instant::now() < deadline,
            "QUIC recovered peer failed to catch up; logs at {:?}",
            cluster.root
        );
        std::thread::sleep(Duration::from_millis(10));
    }
}

fn refuse_spoofed_reader(cluster: &Cluster) {
    // A valid CA certificate with the wrong selected pin cannot impersonate reader1.
    let fixtures = PathBuf::from(env!("CARGO_MANIFEST_DIR")).join("tests/fixtures/tls");
    let spoof = cluster.root.join("spoof");
    fs::create_dir(&spoof).unwrap();
    for (from, to) in [
        ("ca.der", "ca.der"),
        ("node3.der", "node1.der"),
        ("node3-key.der", "node1-key.der"),
        ("node2.der", "node2.der"),
    ] {
        fs::copy(fixtures.join(from), spoof.join(to)).unwrap();
    }
    let denied = run(Command::new(BIN)
        .args([
            "client",
            &cluster.base.to_string(),
            "2",
            "read",
            "--service-tls",
        ])
        .arg(&spoof)
        .args(["--principal", "1"]));
    assert!(!denied.status.success());
    assert!(String::from_utf8_lossy(&denied.stdout).contains("authentication"));
}

fn refuse_invalid_configurations(cluster: &mut Cluster, leader: usize) {
    for principal in [1, 2] {
        cluster.command_principal = Some(principal);
        let denied = cluster.request(
            leader,
            &["configure-record", "learners 16001 1 2 - m:3 v:1 v:2 v:3"],
        );
        assert_eq!(
            String::from_utf8(denied.stdout).unwrap(),
            "ERR AUTHORIZATION\n"
        );
    }
    cluster.command_principal = Some(3);
    for malformed in [
        "",
        "learners",
        "learners 0 1 2 - m:3 v:1 v:2 v:3",
        "learners 16099 1 2 - m:16383 v:1",
        "learners 16099 1 2 - m:3 v:1 v:1 v:3",
        "learners 16099 1 2 - m:3 v:1 v:2 v:4",
        "learners 16099 1 2 1,1 m:2 v:2 v:3",
        "learners 16099 1 2 - m:3 v:1 v:2 v:3 trailing",
        "joint 16099 1 2 3 - m:1 v:1", // Valid tree, violates placement.
    ] {
        let refused = cluster.request(leader, &["configure-record", malformed]);
        let text = String::from_utf8(refused.stdout).unwrap();
        assert!(!refused.status.success(), "accepted {malformed}: {text}");
        assert!(text.starts_with("ERR "), "{text}");
        assert!(cluster
            .ok(leader, &["configuration-status", "16099"])
            .contains("inconclusive_local_absence"));
    }
}

fn cancel_closed_and_expired(cluster: &Cluster, leader: usize) {
    let mut closed = UnobservedCommand::send(
        cluster,
        leader,
        "configure-record joint 17011 3 4 5 - m:3 v:1 v:2 v:3",
    );
    wait_administration_event(
        cluster,
        leader,
        "administration operation=17011 preparing_learner=3",
    );
    closed.close_notify();
    wait_administration_event(
        cluster,
        leader,
        "administration observation_cancelled operation=17011 phase=preparing reason=channel",
    );
    closed.disconnect();
    assert!(cluster
        .ok(leader, &["configuration-status", "17011"])
        .contains("inconclusive_local_absence"));
    let expired = UnobservedCommand::send(
        cluster,
        leader,
        "configure-record joint 17012 3 4 5 - m:3 v:1 v:2 v:3",
    );
    wait_administration_event(
        cluster,
        leader,
        "administration operation=17012 preparing_learner=3",
    );
    wait_administration_event(
        cluster,
        leader,
        "administration observation_cancelled operation=17012 phase=preparing reason=deadline",
    );
    expired.disconnect();
    assert!(cluster
        .ok(leader, &["configuration-status", "17012"])
        .contains("inconclusive_local_absence"));
}

fn check_interrupted_membership(cluster: &Cluster) {
    use voteboat::identity::*;
    for id in 1..=3 {
        let state = recovered_state_for(
            &cluster.root.join(id.to_string()),
            id as u64,
            StoreIdentity {
                id: StoreId::new(id as u128).unwrap(),
                incarnation: StoreIncarnation::new(1).unwrap(),
            },
            42,
            (17000, 42),
        );
        let membership = state.membership_at(state.commit_index).unwrap();
        assert_eq!(membership.id().get(), 6);
        assert!(membership.joint().is_none());
        assert_eq!(membership.stable().voter_stores().len(), 3);
        assert!(membership.stable().learners().is_empty());
        for operation in [17010, 17012, 17013] {
            assert!(membership
                .operations()
                .contains(&OperationId::new(operation).unwrap()));
        }
        assert!(!membership
            .operations()
            .contains(&OperationId::new(17011).unwrap()));
        assert!(!membership
            .operations()
            .contains(&OperationId::new(17015).unwrap()));
    }
}

fn inspect_lifecycle(
    cluster: &Cluster,
    ids: &[usize],
    configuration: u64,
    voters: &[u64],
    learners: &[u64],
    operations: &[u128],
    value: i64,
) {
    use voteboat::identity::*;

    for &id in ids {
        let state = recovered_state_for(
            &cluster.root.join(id.to_string()),
            id as u64,
            lifecycle_store(id),
            value,
            (700, 42),
        );
        let membership = state.membership_at(state.commit_index).unwrap();
        assert_eq!(membership.id().get(), configuration);
        assert!(membership.joint().is_none());
        assert_eq!(
            membership
                .stable()
                .voter_stores()
                .keys()
                .map(|n| n.get())
                .collect::<Vec<_>>(),
            voters
        );
        assert_eq!(
            membership
                .stable()
                .learners()
                .keys()
                .map(|n| n.get())
                .collect::<Vec<_>>(),
            learners
        );
        for &operation in operations {
            assert!(membership
                .operations()
                .contains(&OperationId::new(operation).unwrap()));
        }
        if matches!(configuration, 5 | 10) {
            assert!(state.snapshot.is_some() && state.base_index() > 0);
            let checkpoint_membership = state.membership_at(state.base_index()).unwrap();
            assert_eq!(checkpoint_membership.id().get(), configuration);
            for &operation in operations {
                assert!(checkpoint_membership
                    .operations()
                    .contains(&OperationId::new(operation).unwrap()));
            }
        }
    }
}

fn plan_fourth_learner(cluster: &Cluster, plan: &std::path::Path, provisioned: &str) {
    use voteboat::identity::*;
    // Plan from actual recovered membership, then carry the ordinary record
    // through the same trusted executable administration path as a Rust host.
    let state = recovered_state_for(
        &cluster.root.join("1"),
        1,
        lifecycle_store(1),
        42,
        (700, 42),
    );
    let current = state.membership_at(state.commit_index).unwrap();
    let candidates = [1usize, 2, 4].map(|n| voteboat::placement::PlacementCandidate {
        node: NodeId::new(n as u64).unwrap(),
        placement: voteboat::placement::ReplicaPlacement {
            store: lifecycle_store(n),
            domain: FailureDomainId::new(n as u64).unwrap(),
        },
        enabled: true,
        free_bytes: 1024 * 1024,
        free_replica_slots: 1,
        load_permille: 100,
    });
    let authorizer = voteboat::native::placement::NativePlacementAuthorizer::new(
        GroupIdentity {
            id: GroupId::new(1).unwrap(),
            incarnation: GroupIncarnation::new(1).unwrap(),
        },
        candidates.iter().map(|c| (c.node, c.placement)).collect(),
        voteboat::placement::PlacementRequirements {
            minimum_voting_domains: 2,
            survive_any_single_domain_loss: false,
        },
    )
    .unwrap();
    let planned = voteboat::placement::plan_learner(
        &voteboat::native::placement_planning::NativePlacementPlanner,
        &authorizer,
        voteboat::placement::PlacementRequest {
            snapshot: voteboat::placement::PlacementSnapshot {
                group: authorizer.group(),
                configuration: current.id(),
                generation: voteboat::placement::PlacementSampleGeneration::new(1).unwrap(),
                observed_at: voteboat::runtime::MonoTime(0),
                expires_at: voteboat::runtime::MonoTime(100),
                candidates: &candidates,
            },
            current: &current,
            now: voteboat::runtime::MonoTime(1),
            minimum_free_bytes: 1024,
        },
        OperationId::new(1002).unwrap(),
    )
    .unwrap();
    assert_eq!(planned.recommendation.node, NodeId::new(4).unwrap());
    let voteboat::membership::ConfigurationChange::Learners(next) = &planned.record.change else {
        panic!("planner changed voters");
    };
    assert_eq!(next.policy(), current.stable().policy());
    assert_eq!(next.voter_stores(), current.stable().voter_stores());
    let learners = next
        .learners()
        .keys()
        .map(|n| n.get().to_string())
        .collect::<Vec<_>>()
        .join(",");
    fs::write(
        plan,
        format!(
            "{provisioned}learners {} {} {} {learners} m:2 v:1 v:2\n",
            planned.record.operation.get(),
            planned.record.expected.get(),
            next.id().get()
        ),
    )
    .unwrap();
}

fn enroll_checkpointed_fourth(cluster: &Cluster) {
    let destination = cluster.root.join("4");
    let enrolled = cluster.enroll("create", 4, &destination, 1);
    assert!(
        enrolled.status.success(),
        "{}",
        String::from_utf8_lossy(&enrolled.stderr)
    );
    let before = enrolled_state_for(&destination, 4, lifecycle_store(4));
    assert!(before.snapshot.is_some());
    assert!(cluster
        .enroll("recover", 4, &destination, 1)
        .status
        .success());
    assert_eq!(
        enrolled_state_for(&destination, 4, lifecycle_store(4)),
        before,
        "lost enrollment receipt must not rewrite state"
    );
}

fn promote_fourth(cluster: &mut Cluster, plan: &std::path::Path, provisioned: &str, quic: bool) {
    let policy = if quic {
        "m:1 w:3 2 v:1 1 v:2 2 v:4"
    } else {
        "m:3 v:1 v:2 v:4"
    };
    fs::write(
        plan,
        format!("{provisioned}joint 1003 5 6 7 - {policy}\nfinal 1003 6 7\n"),
    )
    .unwrap();
    for id in [1, 2] {
        cluster.start(id, "recover-member");
    }
    let leader = cluster.leader();
    assert!(cluster
        .ok(leader, &["add", "703", "0"])
        .contains("Value(42)"));
    // Enrollment alone is not live promotion readiness. With 4 offline, the
    // old voters can still write, but must not accept the promotion intent.
    // This also leaves a real post-import command for learner catch-up.
    let unavailable_until = Instant::now() + Duration::from_secs(1);
    while Instant::now() < unavailable_until {
        for id in [1, 2] {
            assert!(cluster
                .wait_configuration_status(id, "1003")
                .contains("action=inconclusive_local_absence"));
        }
        std::thread::sleep(Duration::from_millis(25));
    }
    cluster.start(4, "recover-member");
    finish_lifecycle_operation(cluster, &[1, 2, 4], "1003", "703");
    // Lose original voter 1 abruptly, then keep it absent through retirement.
    let mut failed = cluster.children[0].take().unwrap();
    failed.kill().unwrap();
    failed.wait().unwrap();
    finish_lifecycle_operation(cluster, &[2, 4], "1003", "704");
    cluster.stop();
}

fn lifecycle_store(n: usize) -> voteboat::identity::StoreIdentity {
    use voteboat::identity::*;
    StoreIdentity {
        id: StoreId::new(if n == 4 { 404 } else { n as u128 }).unwrap(),
        incarnation: StoreIncarnation::new(if n == 4 { 7 } else { 1 }).unwrap(),
    }
}

fn configure_fourth_tls(cluster: &mut Cluster) {
    let tls = cluster.root.join("tls");
    fs::create_dir(&tls).unwrap();
    let fixtures = PathBuf::from(env!("CARGO_MANIFEST_DIR")).join("tests/fixtures/tls");
    for name in [
        "ca.der",
        "node1.der",
        "node1-key.der",
        "node2.der",
        "node2-key.der",
    ] {
        fs::copy(fixtures.join(name), tls.join(name)).unwrap();
    }
    // This deployment assigns the public alternative certificate to node 4 only;
    // node 3 is absent. DNS names and Raft node IDs are independently checked.
    fs::copy(fixtures.join("node3.der"), tls.join("node4.der")).unwrap();
    fs::copy(fixtures.join("node3-key.der"), tls.join("node4-key.der")).unwrap();
    cluster.tls = Some(tls);
}

fn check_drained_checkpoint(cluster: &Cluster, replacement: usize) {
    {
        let _gate = fixture_gate();
        use voteboat::{identity::*, log::*, native::log_store::*};
        let store = NativeLogStore::recover(
            FileLogIo::open(cluster.root.join(replacement.to_string())).unwrap(),
            StoreIdentity {
                id: StoreId::new(replacement as u128).unwrap(),
                incarnation: StoreIncarnation::new(1).unwrap(),
            },
            LogLimits::default(),
        )
        .unwrap();
        let group = GroupIdentity {
            id: GroupId::new(1).unwrap(),
            incarnation: GroupIncarnation::new(1).unwrap(),
        };
        assert!(
            store.state(group).unwrap().base_index() > 0,
            "admitted checkpoint must drain durably"
        );
    }
}

fn check_interrupted_record_conflict(cluster: &Cluster, replacement: usize) {
    let conflict = cluster.request(
        replacement,
        &["configure-record", "learners 17013 5 99 - m:3 v:1 v:2 v:3"],
    );
    assert!(String::from_utf8(conflict.stdout)
        .unwrap()
        .contains("conflicts with retained record"));
}

fn refuse_unassigned_deployment(cluster: &Cluster, path: &std::path::Path) {
    let unassigned = cluster.root.join("unassigned-4");
    let refused = run(Command::new(BIN)
        .args(["serve", "recover-member"])
        .arg(&unassigned)
        .arg("4")
        .arg(cluster.base.to_string())
        .arg(cluster.tls.as_ref().unwrap())
        .arg("--deployment")
        .arg(path));
    assert!(
        !refused.status.success(),
        "provisioning must not create member history"
    );
    assert!(!unassigned.exists());
}
