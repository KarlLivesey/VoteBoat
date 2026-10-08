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
    endpoints: Option<PathBuf>,
    deployment: Option<PathBuf>,
    admin_plan: Option<PathBuf>,
    tls: Option<PathBuf>,
    listeners: BTreeMap<u16, TcpListener>,
    udp_sockets: Vec<UdpSocket>,
    quic: bool,
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
        // The finite suite uses fewer than 40 of the available 157 blocks.
        let (base, listeners, udp_sockets) = {
            let mut blocks = PORT_BLOCKS.lock().unwrap_or_else(|e| e.into_inner());
            let (base, listeners, udp_sockets) = (10000u16..30000)
                .step_by(128)
                .find_map(|base| {
                    if blocks.contains(&base) {
                        return None;
                    }
                    [1, 2, 3, 4, 11, 12, 13, 101, 102, 103, 104]
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
            endpoints: None,
            deployment: None,
            admin_plan: None,
            tls: None,
            listeners,
            udp_sockets,
            quic: false,
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
        let log = fs::File::create(self.root.join(format!("{id}-{mode}.log"))).unwrap();
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
        if let Some(path) = &self.deployment {
            command.arg("--deployment").arg(path);
        }
        if self.quic {
            command.args(["--transport", "quic"]);
        }
        if let Some(path) = &self.admin_plan {
            command.arg("--admin-plan").arg(path);
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
        run(&mut command)
    }
    fn target(&self, target: &str, args: &[&str]) -> std::process::Output {
        run(Command::new(BIN)
            .args(["client", &self.base.to_string(), target])
            .args(args))
    }
    fn ok(&self, id: usize, args: &[&str]) -> String {
        let output = self.request(id, args);
        assert!(
            output.status.success(),
            "{args:?}: {} {}",
            String::from_utf8_lossy(&output.stdout),
            String::from_utf8_lossy(&output.stderr)
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
        assert!(
            output.status.success(),
            "{args:?}: {} {}",
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
                        fs::read_to_string(self.root.join(format!("{id}-create.log")))
                            .unwrap_or_default()
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
        // Intake closes independently; each node must drain and join its workers.
        for id in 1..=self.children.len() {
            if self.children[id - 1].is_some() {
                self.ok(id, &["quit"]);
            }
        }
        let deadline = Instant::now() + Duration::from_secs(15);
        for child in &mut self.children {
            if let Some(c) = child.as_mut() {
                loop {
                    if let Some(status) = c.try_wait().unwrap() {
                        assert!(status.success());
                        break;
                    }
                    assert!(Instant::now() < deadline, "shutdown timeout");
                    std::thread::sleep(Duration::from_millis(10));
                }
                *child = None;
            }
        }
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
        assert!(cluster
            .ok(leader, &["add", "700", "42"])
            .contains("Value(42)"));
        assert_eq!(cluster.ok(leader, &["read"]), "OK value=42\n");
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
        let leader = cluster.leader();
        assert!(cluster
            .ok(leader, &["add", "700", "42"])
            .contains("duplicate=true"));
        assert_eq!(cluster.ok(leader, &["read"]), "OK value=42\n");
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
    recovered_state_for(root, local, identity, 42)
}
fn recovered_state_for(
    root: &std::path::Path,
    local: u64,
    identity: voteboat::identity::StoreIdentity,
    expected: i64,
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
                operation: OperationId::new(700).unwrap(),
                bytes: 42i64.to_le_bytes().to_vec(),
            },
        }])
        .unwrap();
    assert!(retry[0].duplicate);
    assert_eq!(retry[0].outcome, CounterOutcome::Value(42));
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
    use voteboat::{identity::*, log::*, membership::*, native::log_store::*};
    let mut cluster = Cluster::new();
    cluster.quic = quic;
    cluster.children.push(None); // Nodes 1, 2 and 4; node 3 has retired.
    seed_member_service(&cluster.root, true, false);
    let fourth = StoreIdentity {
        id: StoreId::new(404).unwrap(),
        incarnation: StoreIncarnation::new(7).unwrap(),
    };
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
    checkpoint_source(&cluster.root, 1, 700, 42);
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
    let declaration = format!("voteboat-deployment-v1\n1 1 1 127.0.0.1:{} node1.voteboat.test\n2 2 1 127.0.0.1:{} node2.voteboat.test\n4 404 7 127.0.0.1:{} node3.voteboat.test\n",
        cluster.base + 1, cluster.base + 2, cluster.base + 4);
    let path = cluster.root.join("deployment.txt");
    fs::write(&path, &declaration).unwrap();
    cluster.deployment = Some(path.clone());
    let destination = cluster.root.join("4");
    cluster.listeners.clear();
    cluster.udp_sockets.clear();
    let unassigned = cluster.root.join("unassigned-4");
    let refused = run(Command::new(BIN)
        .args(["serve", "recover-member"])
        .arg(&unassigned)
        .arg("4")
        .arg(cluster.base.to_string())
        .arg(cluster.tls.as_ref().unwrap())
        .arg("--deployment")
        .arg(&path));
    assert!(
        !refused.status.success(),
        "provisioning must not create member history"
    );
    assert!(!unassigned.exists());
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
    use voteboat::identity::*;
    let mut cluster = Cluster::new();
    cluster.quic = quic;
    cluster.children.push(None);
    let store = |n: usize| StoreIdentity {
        id: StoreId::new(if n == 4 { 404 } else { n as u128 }).unwrap(),
        incarnation: StoreIncarnation::new(if n == 4 { 7 } else { 1 }).unwrap(),
    };
    let inspect = |cluster: &Cluster,
                   ids: &[usize],
                   configuration: u64,
                   voters: &[u64],
                   learners: &[u64],
                   operations: &[u128],
                   value: i64| {
        for &id in ids {
            let state = recovered_state_for(
                &cluster.root.join(id.to_string()),
                id as u64,
                store(id),
                value,
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
    };
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
    inspect(&cluster, &[1, 2], 4, &[1, 2], &[], &[1000, 1001], 42);

    // Node 3 is retired before its public test credential is assigned to node 4.
    // No two live peers share that credential. Store 4 has a distinct incarnation.
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
    fs::copy(fixtures.join("node3.der"), tls.join("node4.der")).unwrap();
    fs::copy(fixtures.join("node3-key.der"), tls.join("node4-key.der")).unwrap();
    cluster.tls = Some(tls);
    let deployment = cluster.root.join("lifecycle.deployment");
    let declaration = format!("voteboat-deployment-v1\n1 1 1 127.0.0.1:{} node1.voteboat.test\n2 2 1 127.0.0.1:{} node2.voteboat.test\n4 404 7 127.0.0.1:{} node3.voteboat.test\n", cluster.base+1, cluster.base+2, cluster.base+4);
    fs::write(&deployment, &declaration).unwrap();
    cluster.deployment = Some(deployment.clone());
    let provisioned =
        "voteboat-counter-admin-v1\nplacement 2 false\nreplica 1 1\nreplica 2 2\nreplica 4 4\n";
    fs::write(
        &plan,
        format!("{provisioned}learners 1002 4 5 4 m:2 v:1 v:2\n"),
    )
    .unwrap();
    for id in [1, 2] {
        cluster.start(id, "recover-member");
    }
    finish_lifecycle_operation(&cluster, &[1, 2], "1002", "702");
    for id in [1, 2] {
        cluster.ok(id, &["checkpoint"]);
    }
    cluster.stop();
    inspect(&cluster, &[1, 2], 5, &[1, 2], &[4], &[1000, 1001, 1002], 42);
    let destination = cluster.root.join("4");
    let enrolled = cluster.enroll("create", 4, &destination, 1);
    assert!(
        enrolled.status.success(),
        "{}",
        String::from_utf8_lossy(&enrolled.stderr)
    );
    let before = enrolled_state_for(&destination, 4, store(4));
    assert!(before.snapshot.is_some());
    assert!(cluster
        .enroll("recover", 4, &destination, 1)
        .status
        .success());
    assert_eq!(
        enrolled_state_for(&destination, 4, store(4)),
        before,
        "lost enrollment receipt must not rewrite state"
    );

    let policy = if quic {
        "m:1 w:3 2 v:1 1 v:2 2 v:4"
    } else {
        "m:3 v:1 v:2 v:4"
    };
    fs::write(
        &plan,
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
    finish_lifecycle_operation(&cluster, &[1, 2, 4], "1003", "703");
    // Lose original voter 1 abruptly, then keep it absent through retirement.
    let mut failed = cluster.children[0].take().unwrap();
    failed.kill().unwrap();
    failed.wait().unwrap();
    finish_lifecycle_operation(&cluster, &[2, 4], "1003", "704");
    cluster.stop();
    inspect(
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
    inspect(
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
    inspect(
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
    // Checkpoint admission is asynchronous; shutdown drains admitted work.
    cluster.ok(replacement, &["checkpoint"]);
    cluster.stop();
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
    assert!(cluster.ok(leader, &["add", "1", "7"]).contains("Value(7)"));
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
    assert_eq!(cluster.ok(leader, &["read"]), "OK value=10\n");
    assert!(cluster
        .ok(leader, &["add", "2", "3"])
        .contains("duplicate=true"));
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
