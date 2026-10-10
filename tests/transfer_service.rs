// SPDX-License-Identifier: RPL-1.5
// Copyright (c) 2026 Karl Livesey
#![cfg(feature = "tls")]
#[path = "transfer_source/fixtures.rs"]
mod source_fixture;
use std::{
    fs,
    net::{TcpListener, UdpSocket},
    path::PathBuf,
    process::{Child, Command, Output, Stdio},
    sync::atomic::{AtomicU16, Ordering},
    time::{Duration, Instant},
};
#[path = "transfer_service/cuts.rs"]
mod cuts;
#[path = "transfer_service/interrupt.rs"]
mod interrupt;
#[path = "transfer_service/profiles.rs"]
mod profiles;
const BIN: &str = env!("CARGO_BIN_EXE_voteboat-transfer");
static NEXT: AtomicU16 = AtomicU16::new(14000);
const GROUPS: [u128; 4] = [1, 20, 21, 22];
struct Cluster {
    root: PathBuf,
    base: u16,
    quic: bool,
    children: Vec<(u128, u16, Child)>,
}
impl Cluster {
    fn new(quic: bool) -> Self {
        let (base, held, udp) = loop {
            let base = NEXT.fetch_add(1024, Ordering::Relaxed);
            let mut held = Vec::new();
            let mut udp = Vec::new();
            let mut valid = true;
            for slot in 0..4 {
                for node in 1..=3 {
                    for port in [base + slot * 128 + node, base + slot * 128 + 100 + node] {
                        match (
                            TcpListener::bind(("127.0.0.1", port)),
                            UdpSocket::bind(("127.0.0.1", port)),
                        ) {
                            (Ok(t), Ok(u)) => {
                                held.push(t);
                                udp.push(u);
                            }
                            _ => valid = false,
                        }
                    }
                }
            }
            if valid {
                break (base, held, udp);
            }
        };
        let root = std::env::temp_dir().join(format!(
            "voteboat-transfer-service-{}-{base}",
            std::process::id()
        ));
        fs::create_dir_all(&root).unwrap();
        for group in GROUPS {
            fs::create_dir_all(root.join(group.to_string())).unwrap();
        }
        let plan = Command::new(BIN)
            .args(["plan", "1", "20", "21", "22", "10", "128", "200", "201"])
            .output()
            .unwrap();
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
        drop(held);
        drop(udp);
        let mut rig = Self {
            root,
            base,
            quic,
            children: Vec::new(),
        };
        rig.start("create");
        rig
    }
    fn tls(&self) -> PathBuf {
        PathBuf::from(env!("CARGO_MANIFEST_DIR")).join("tests/fixtures/tls")
    }
    fn start_one(&mut self, g: u128, slot: usize, node: u16, mode: &str) {
        let log = fs::File::create(self.root.join(format!("{g}-{node}.log"))).unwrap();
        let child = Command::new(BIN)
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
            .stderr(log)
            .spawn()
            .unwrap();
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
        self.client(select, principal, "command")
            .arg(g.to_string())
            .args(words)
            .output()
            .unwrap()
    }
    fn ok(&self, g: u128, words: &[&str]) -> String {
        let out = self.request(0, 3, g, words);
        assert!(
            out.status.success(),
            "{g} {words:?}: {} {}",
            String::from_utf8_lossy(&out.stdout),
            String::from_utf8_lossy(&out.stderr)
        );
        String::from_utf8(out.stdout).unwrap()
    }
    fn operate(&self, verb: &str) -> String {
        let out = self.client(0, 3, "client").arg(verb).output().unwrap();
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
            let out = self.request(node, 3, g, &["quit"]);
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
    rig.ok(1, &["initialize"]);
    rig.ok(1, &["grant"]);
    rig.ok(20, &["initialize"]);
    rig.ok(20, &["add", "1", "1", "7"]);
    rig.ok(20, &["add", "2", "200", "11"]);
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
#[test]
fn tcp_authenticated_operator_starts_a_split_and_children_survive_parent_shutdown() {
    history(false);
}
#[cfg(feature = "quic")]
#[test]
fn quic_authenticated_operator_resumes_original_split_after_process_crash() {
    history(true);
}
