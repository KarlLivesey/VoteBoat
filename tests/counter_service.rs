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
    fs,
    net::{Ipv4Addr, TcpListener},
    path::PathBuf,
    process::{Child, Command},
    time::{Duration, Instant},
};
const BIN: &str = env!("CARGO_BIN_EXE_voteboat-counter");
struct Cluster {
    root: PathBuf,
    base: u16,
    children: Vec<Option<Child>>,
}
impl Cluster {
    fn new() -> Self {
        let root = std::env::temp_dir().join(format!(
            "voteboat-service-{}-{}",
            std::process::id(),
            std::time::SystemTime::now()
                .duration_since(std::time::UNIX_EPOCH)
                .unwrap()
                .as_nanos()
        ));
        fs::create_dir(&root).unwrap();
        let base = (0..100)
            .find_map(|_| {
                let listener = TcpListener::bind((Ipv4Addr::LOCALHOST, 0)).unwrap();
                let base = listener.local_addr().unwrap().port();
                if base > 65432 {
                    return None;
                }
                let reservations = [1, 2, 3, 101, 102, 103]
                    .into_iter()
                    .map(|offset| TcpListener::bind((Ipv4Addr::LOCALHOST, base + offset)))
                    .collect::<Result<Vec<_>, _>>()
                    .ok()?;
                drop(reservations);
                Some(base)
            })
            .expect("available port range");
        Self {
            root,
            base,
            children: (0..3).map(|_| None).collect(),
        }
    }
    fn start(&mut self, id: usize, mode: &str) {
        let log = fs::File::create(self.root.join(format!("{id}-{mode}.log"))).unwrap();
        let tls = PathBuf::from(env!("CARGO_MANIFEST_DIR")).join("tests/fixtures/tls");
        self.children[id - 1] = Some(
            Command::new(BIN)
                .args(["serve", mode])
                .arg(self.root.join(id.to_string()))
                .arg(id.to_string())
                .arg(self.base.to_string())
                .arg(tls)
                .stdout(log.try_clone().unwrap())
                .stderr(log)
                .spawn()
                .unwrap(),
        );
    }
    fn request(&self, id: usize, args: &[&str]) -> std::process::Output {
        Command::new(BIN)
            .args(["client", &self.base.to_string(), &id.to_string()])
            .args(args)
            .output()
            .unwrap()
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
    fn leader(&mut self) -> usize {
        let deadline = Instant::now() + Duration::from_secs(20);
        loop {
            for id in 1..=3 {
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
        for id in 1..=3 {
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
    let mut cluster = Cluster::new();
    for id in 1..=3 {
        cluster.start(id, "create");
    }
    let leader = cluster.leader();
    assert!(cluster.ok(leader, &["add", "1", "7"]).contains("Value(7)"));
    let retry = cluster.ok(leader, &["add", "1", "7"]);
    assert!(retry.contains("Value(7)") && retry.contains("duplicate=true"));
    assert_eq!(cluster.ok(leader, &["read"]), "OK value=7\n");
    // A local applied value cannot make a follower's read linearizable.
    let follower = leader % 3 + 1;
    assert!(!cluster.request(follower, &["read"]).status.success());
    let mut killed = cluster.children[leader - 1].take().unwrap();
    killed.kill().unwrap();
    killed.wait().unwrap();
    let replacement = cluster.leader();
    assert_ne!(replacement, leader);
    assert!(cluster
        .ok(replacement, &["add", "2", "3"])
        .contains("Value(10)"));
    assert_eq!(cluster.ok(replacement, &["read"]), "OK value=10\n");
    cluster.start(leader, "recover");
    // Checkpoint admission is asynchronous; shutdown drains admitted work.
    cluster.ok(replacement, &["checkpoint"]);
    cluster.stop();
    {
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
    let refused = Command::new(BIN)
        .args(["serve", "create"])
        .arg(cluster.root.join("1"))
        .arg("1")
        .arg(cluster.base.to_string())
        .arg(tls)
        .output()
        .unwrap();
    assert!(
        !refused.status.success(),
        "create must not reset existing state"
    );
    for id in 1..=3 {
        cluster.start(id, "recover");
    }
    let leader = cluster.leader();
    assert!(cluster
        .ok(leader, &["add", "2", "3"])
        .contains("duplicate=true"));
    assert_eq!(cluster.ok(leader, &["read"]), "OK value=10\n");
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
    let cluster = Cluster::new();
    let root = cluster.root.join("missing");
    let tls = PathBuf::from(env!("CARGO_MANIFEST_DIR")).join("tests/fixtures/tls");
    for (mode, id) in [("recover", "1"), ("create", "4")] {
        let output = Command::new(BIN)
            .args(["serve", mode])
            .arg(&root)
            .arg(id)
            .arg(cluster.base.to_string())
            .arg(&tls)
            .output()
            .unwrap();
        assert!(!output.status.success());
        assert!(!root.exists());
    }
    let oversized = cluster.root.join("oversized-credentials");
    fs::create_dir(&oversized).unwrap();
    fs::write(oversized.join("ca.der"), vec![0; 65537]).unwrap();
    let output = Command::new(BIN)
        .args(["serve", "create"])
        .arg(&root)
        .arg("1")
        .arg(cluster.base.to_string())
        .arg(oversized)
        .output()
        .unwrap();
    assert!(!output.status.success());
    assert!(String::from_utf8_lossy(&output.stderr).contains("invalid TLS material size"));
    assert!(!root.exists());
    fs::remove_dir_all(&cluster.root).unwrap();
}

#[test]
fn bounded_commands_and_quorum_loss_preserve_retry_identity() {
    use std::io::{Read, Write};
    let mut cluster = Cluster::new();
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
    let leader = cluster.leader();
    assert!(cluster.ok(leader, &["add", "2", "3"]).contains("Value(10)"));
    assert_eq!(cluster.ok(leader, &["read"]), "OK value=10\n");
    assert!(cluster
        .ok(leader, &["add", "2", "3"])
        .contains("duplicate=true"));
    cluster.stop();
    fs::remove_dir_all(&cluster.root).unwrap();
}
