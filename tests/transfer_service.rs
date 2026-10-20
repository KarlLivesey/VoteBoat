// SPDX-License-Identifier: RPL-1.5
// Copyright (c) 2026 Karl Livesey
#![cfg(feature = "tls")]
#[path = "support/ports.rs"]
#[allow(dead_code)] // This service uses the shared fixture's offset layout.
mod ports;
#[path = "transfer_source/fixtures.rs"]
mod source_fixture;
use std::{
    fs,
    path::PathBuf,
    process::{Child, Command, Output, Stdio},
    sync::{Mutex, MutexGuard},
    time::{Duration, Instant},
};
#[path = "transfer_service/cuts.rs"]
mod cuts;
#[path = "transfer_service/initialization.rs"]
mod initialization;
#[path = "transfer_service/interrupt.rs"]
mod interrupt;
#[path = "transfer_service/merge.rs"]
mod merge;
#[path = "transfer_service/peer_credentials.rs"]
mod peer_credentials;
#[path = "transfer_service/profiles.rs"]
mod profiles;
#[path = "transfer_service/retirement.rs"]
mod retirement;
#[path = "transfer_service/retirement_retry.rs"]
mod retirement_retry;
#[path = "transfer_service/revocation.rs"]
mod revocation;
#[path = "transfer_service/startup_data.rs"]
mod startup_data;
#[path = "transfer_service/startup_discovery.rs"]
mod startup_discovery;
#[cfg(unix)]
#[path = "transfer_service/startup_recovery.rs"]
mod startup_recovery;
const BIN: &str = env!("CARGO_BIN_EXE_voteboat-transfer");
// A concurrent fork can briefly inherit another fixture's reserved listeners.
// Coordinate reservations and process creation, never child execution/waiting.
static RESERVATION_SPAWN: Mutex<()> = Mutex::new(());
fn fixture_gate() -> MutexGuard<'static, ()> {
    RESERVATION_SPAWN
        .lock()
        .unwrap_or_else(|error| error.into_inner())
}
fn spawn(command: &mut Command) -> Child {
    let _gate = fixture_gate();
    command.spawn().unwrap()
}
fn run(command: &mut Command) -> Output {
    let child = spawn(command.stdout(Stdio::piped()).stderr(Stdio::piped()));
    child.wait_with_output().unwrap()
}
const GROUPS: [u128; 4] = [1, 20, 21, 22];
struct Cluster {
    root: PathBuf,
    base: u16,
    quic: bool,
    admin: u64,
    peer_credentials: bool,
    retirement: bool,
    children: Vec<(u128, u16, Child)>,
    _ports: ports::PortLease,
}
fn listener_offsets() -> Vec<u16> {
    (0..4)
        .flat_map(|slot| (1..=3).flat_map(move |node| [slot * 128 + node, slot * 128 + 100 + node]))
        .collect()
}

#[test]
fn listener_layout_rejects_the_exhausted_allocator_boundary_without_wrapping() {
    let offsets = listener_offsets();
    assert_eq!(offsets.len(), 24);
    let max_base = u16::MAX - offsets.iter().max().unwrap();
    let last = ports::offset_ports(max_base, &offsets).unwrap();
    assert_eq!(last.iter().max(), Some(&u16::MAX));
    assert!(ports::offset_ports(max_base + 1, &offsets).is_none());
    // The old NEXT ladder reaches this base after fifty 1024-port advances.
    assert!(ports::offset_ports(65200, &offsets).is_none());
}
impl Cluster {
    fn new(quic: bool) -> Self {
        Self::with_retirement(quic, false)
    }
    fn with_retirement(quic: bool, retirement: bool) -> Self {
        Self::with_profile(quic, retirement, false)
    }
    fn with_profile(quic: bool, retirement: bool, merge: bool) -> Self {
        Self::with_options(quic, retirement, merge, false)
    }
    fn with_options(quic: bool, retirement: bool, merge: bool, peer_credentials: bool) -> Self {
        let reservation = fixture_gate();
        let (base, mut ports) = ports::PortLease::acquire_offsets(&listener_offsets());
        let root = std::env::temp_dir().join(format!(
            "voteboat-transfer-service-{}-{base}",
            std::process::id()
        ));
        fs::create_dir_all(&root).unwrap();
        for group in GROUPS {
            fs::create_dir_all(root.join(group.to_string())).unwrap();
        }
        let mut plan = Command::new(BIN);
        plan.args([
            if merge { "plan-merge" } else { "plan" },
            "1",
            "20",
            "21",
            "22",
            "10",
            "128",
            "200",
            "201",
        ]);
        if retirement && !merge {
            plan.arg("--retirement");
        }
        // This offline child finishes under the reservation gate, so no reserved
        // listener can outlive the parent reservation through an unrelated fork.
        let plan = plan.output().unwrap();
        assert!(
            plan.status.success(),
            "plan: {}",
            String::from_utf8_lossy(&plan.stderr)
        );
        fs::write(root.join("profile"), plan.stdout).unwrap();
        let mut access = "voteboat-service-access-v1 1\n".to_owned();
        for g in GROUPS {
            access.push_str(&format!("3 admin {g} 1\n2 reader {g} 1\n"));
        }
        access.push_str("1 admin 999 1\n");
        fs::write(root.join("access"), access).unwrap();
        for select in 0..=3 {
            let mut endpoints = "voteboat-transfer-endpoints-v1\n".to_owned();
            for (slot, g) in GROUPS.into_iter().enumerate() {
                for node in 1..=3 {
                    if select == 0 || select == node {
                        endpoints.push_str(&format!(
                            "{g} 1 {node} 127.0.0.1:{}\n",
                            base + slot as u16 * 128 + 100 + node
                        ));
                    }
                }
            }
            fs::write(root.join(format!("endpoints{select}")), endpoints).unwrap();
        }
        ports.handoff();
        drop(reservation);
        let mut rig = Self {
            root,
            base,
            quic,
            admin: 3,
            peer_credentials,
            retirement,
            children: Vec::new(),
            _ports: ports,
        };
        if peer_credentials {
            peer_credentials::select(&rig, 1, &rig.tls());
        }
        rig.start("create");
        rig
    }
    fn tls(&self) -> PathBuf {
        PathBuf::from(env!("CARGO_MANIFEST_DIR")).join("tests/fixtures/tls")
    }
    fn start_one(&mut self, g: u128, slot: usize, node: u16, mode: &str) {
        let log = fs::File::create(self.root.join(format!("{g}-{node}.log"))).unwrap();
        let mut command = Command::new(BIN);
        command
            .args(["serve", mode])
            .arg(self.root.join(format!("{g}/{node}")))
            .arg(node.to_string())
            .arg((self.base + slot as u16 * 128).to_string())
            .arg(self.tls())
            .arg(self.root.join("profile"))
            .arg(g.to_string())
            .arg(self.root.join("access"))
            .arg(if self.quic { "quic" } else { "tcp" })
            .stdout(log.try_clone().unwrap())
            .stderr(log);
        if self.peer_credentials {
            command
                .arg("--peer-credentials")
                .arg(self.root.join("peer-credentials"));
        }
        let child = spawn(&mut command);
        self.children.push((g, node, child));
    }
    fn start(&mut self, mode: &str) {
        for (slot, g) in GROUPS.into_iter().enumerate() {
            for node in 1..=3 {
                self.start_one(g, slot, node, mode);
            }
        }
        let deadline = Instant::now() + Duration::from_secs(30);
        loop {
            let mut ready = 0;
            for (g, n, child) in &mut self.children {
                let text = fs::read_to_string(self.root.join(format!("{g}-{n}.log"))).unwrap();
                assert!(
                    child.try_wait().unwrap().is_none(),
                    "server {g}/{n}: {text}"
                );
                ready += usize::from(text.contains("ready transfer"));
            }
            if ready == 12 {
                return;
            }
            assert!(
                Instant::now() < deadline,
                "server startup timeout; retained {:?}",
                self.root
            );
            std::thread::sleep(Duration::from_millis(10));
        }
    }
    fn client(&self, select: u16, principal: u64, verb: &str) -> Command {
        let mut command = Command::new(BIN);
        command
            .arg(verb)
            .arg(self.root.join("profile"))
            .arg(self.root.join(format!("endpoints{select}")))
            .arg(self.tls())
            .arg(principal.to_string());
        command
    }
    fn request(&self, select: u16, principal: u64, g: u128, words: &[&str]) -> Output {
        run(self
            .client(select, principal, "command")
            .arg(g.to_string())
            .args(words))
    }
    fn ok(&self, g: u128, words: &[&str]) -> String {
        let out = self.request(0, self.admin, g, words);
        assert!(
            out.status.success(),
            "{g} {words:?}: {} {}",
            String::from_utf8_lossy(&out.stdout),
            String::from_utf8_lossy(&out.stderr)
        );
        String::from_utf8(out.stdout).unwrap()
    }
    fn operate(&self, verb: &str) -> String {
        let out = run(self.client(0, self.admin, "client").arg(verb));
        assert!(
            out.status.success(),
            "{verb}: {} {}",
            String::from_utf8_lossy(&out.stdout),
            String::from_utf8_lossy(&out.stderr)
        );
        String::from_utf8(out.stdout).unwrap()
    }
    fn crash(&mut self) {
        for (_, _, child) in &mut self.children {
            child.kill().unwrap();
            child.wait().unwrap();
        }
        self.children.clear();
    }
    fn stop_group(&mut self, g: u128) {
        for node in 1..=3 {
            let out = self.request(node, self.admin, g, &["quit"]);
            assert!(
                out.status.success(),
                "shutdown: {}",
                String::from_utf8_lossy(&out.stderr)
            );
            let (_, _, child) = self
                .children
                .iter_mut()
                .find(|(id, n, _)| *id == g && *n == node)
                .unwrap();
            let deadline = Instant::now() + Duration::from_secs(15);
            while child.try_wait().unwrap().is_none() {
                assert!(Instant::now() < deadline, "shutdown join timeout");
                std::thread::sleep(Duration::from_millis(10));
            }
            let log = fs::read_to_string(self.root.join(format!("{g}-{node}.log"))).unwrap();
            assert!(log.contains("workers_joined=true"), "{log}");
        }
    }
}
impl Drop for Cluster {
    fn drop(&mut self) {
        for (_, _, child) in &mut self.children {
            let _ = child.kill();
            let _ = child.wait();
        }
    }
}
fn history(quic: bool) {
    let mut rig = Cluster::new(quic);
    initialize(&rig);
    interrupt::read(&mut rig);
    assert!(rig.operate("status").contains("RecordIntent"));
    assert!(!rig
        .request(0, 2, 1, &["transfer-step", "intent"])
        .status
        .success());
    assert!(!rig.request(0, 1, 20, &["status"]).status.success());
    if quic {
        for _ in 0..5 {
            rig.operate("step");
        }
        rig.crash();
        rig.start("recover");
        assert!(rig.operate("resume").contains("OK complete"));
    } else {
        assert!(rig.operate("start").contains("OK complete"));
    }
    finish(rig);
}
fn initialize(rig: &Cluster) {
    // ST2: every transfer group checkpoints and reclaims by default.
    let text = rig.ok(1, &["maintenance"]);
    for field in [
        "retention=maintained",
        "enabled=true",
        "checkpoint_entries=8192",
        "high_water=false",
        "wal_parked=0",
        // ST2 round 3 (I7).
        "retained_budget=16384",
        "retained_capacity=16640",
        "entry_high_water=false",
        "checkpoint_failures=0",
        "wal_refused_proposals=0",
        "wal_held_ms=0",
        "step_downs=0",
        "last_step_down=none",
        // ST2 round 4 (R4-1).
        "wal_noop_refused=0",
        "noop_refusals=0",
        "wal_noop_starved=false",
    ] {
        assert!(text.contains(field), "{field}: {text}");
    }
    let log = fs::read_to_string(rig.root.join("20-1.log")).unwrap();
    assert!(
        log.contains("maintenance retention=maintained checkpoint_entries=8192"),
        "{log}"
    );
    initialization::command(rig, 1, "initialize");
    initialization::command(rig, 1, "grant");
    initialization::command(rig, 20, "initialize");
    startup_data::add(rig, "1", "1", "7", 7);
    startup_data::add(rig, "2", "200", "11", 11);
}
fn finish(mut rig: Cluster) {
    assert!(rig.operate("status").contains("Complete"));
    let refused = rig.request(0, 3, 20, &["read", "1"]);
    assert!(!refused.status.success());
    assert!(String::from_utf8_lossy(&refused.stderr).contains("Fenced"));
    assert!(!rig
        .request(0, 3, 20, &["add", "300", "1", "1"])
        .status
        .success());
    rig.stop_group(1);
    rig.stop_group(20);
    for (g, key, old, value) in [(21, "1", "1", "7"), (22, "200", "2", "11")] {
        assert!(rig
            .ok(g, &["read", key])
            .contains(&format!("value={value}")));
        assert!(rig
            .ok(g, &["add", old, key, value])
            .contains("duplicate: true"));
        rig.ok(g, &["add", "301", key, "2"]);
        let updated = value.parse::<i64>().unwrap() + 2;
        assert!(rig
            .ok(g, &["read", key])
            .contains(&format!("value={updated}")));
        rig.stop_group(g);
    }
    let root = rig.root.clone();
    drop(rig);
    fs::remove_dir_all(root).unwrap();
}
/// ST2 / U5: a maintenance selection that would be refused or ignored once
/// serving is refused before any file exists.
#[test]
fn maintenance_selection_is_validated_before_storage() {
    let root = std::env::temp_dir().join(format!(
        "voteboat-transfer-maintenance-{}",
        std::process::id()
    ));
    let ack = "I-accept-a-bounded-log";
    for (args, message) in [
        (vec!["--no-maintenance", "yes"], "Pass --no-maintenance"),
        (
            vec!["--no-maintenance", ack, "--wal-reclaim-ms", "5"],
            "cannot be combined",
        ),
        (vec!["--checkpoint-entries", "8193"], "at most 8192"),
        // ST2 round 3 (I6).
        (vec!["--wal-reclaim-ms", "86400001"], "at most 86400000"),
        (
            vec!["--no-maintenance", ack, "--wal-reclaim-min-bytes", "0"],
            "cannot be combined",
        ),
    ] {
        let out = run(Command::new(BIN)
            .args(["serve", "create"])
            .arg(&root)
            .args([
                "1",
                "20000",
                "missing-tls",
                "missing-profile",
                "1",
                "missing-access",
                "tcp",
            ])
            .args(&args));
        assert!(!out.status.success(), "{args:?}");
        let stderr = String::from_utf8_lossy(&out.stderr);
        assert!(stderr.contains(message), "{args:?}: {stderr}");
        assert!(!root.exists(), "{args:?}");
    }
}
#[test]
fn tcp_authenticated_operator_starts_a_split_and_children_survive_parent_shutdown() {
    history(false);
}
#[cfg(feature = "quic")]
#[test]
fn quic_authenticated_operator_resumes_original_split_after_process_crash() {
    history(true);
}
