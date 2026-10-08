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
    collections::BTreeSet,
    fs,
    net::{Ipv4Addr, TcpListener},
    path::PathBuf,
    process::{Child, Command},
    sync::Mutex,
    time::{Duration, Instant},
};
const BIN: &str = env!("CARGO_BIN_EXE_voteboat-counter");
static PORT_BLOCKS: Mutex<BTreeSet<u16>> = Mutex::new(BTreeSet::new());
struct Cluster {
    root: PathBuf,
    base: u16,
    children: Vec<Option<Child>>,
    endpoints: Option<PathBuf>,
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
        // Keep non-overlapping blocks for each live fixture, rather than reusing
        // a released ephemeral listener as a base during parallel socket tests.
        let base = {
            let mut blocks = PORT_BLOCKS.lock().unwrap_or_else(|e| e.into_inner());
            let base = (10000u16..30000)
                .step_by(128)
                .find(|base| {
                    if blocks.contains(base) {
                        return false;
                    }
                    [1, 2, 3, 11, 12, 13, 101, 102, 103]
                        .into_iter()
                        .map(|offset| TcpListener::bind((Ipv4Addr::LOCALHOST, *base + offset)))
                        .collect::<Result<Vec<_>, _>>()
                        .is_ok()
                })
                .expect("available independent port block");
            blocks.insert(base);
            base
        };
        Self {
            root,
            base,
            children: (0..3).map(|_| None).collect(),
            endpoints: None,
        }
    }
    fn start(&mut self, id: usize, mode: &str) {
        let log = fs::File::create(self.root.join(format!("{id}-{mode}.log"))).unwrap();
        let tls = PathBuf::from(env!("CARGO_MANIFEST_DIR")).join("tests/fixtures/tls");
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
    fn target(&self, target: &str, args: &[&str]) -> std::process::Output {
        Command::new(BIN)
            .args(["client", &self.base.to_string(), target])
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
        PORT_BLOCKS
            .lock()
            .unwrap_or_else(|e| e.into_inner())
            .remove(&self.base);
    }
}
#[test]
fn three_service_processes_retry_replace_leader_and_recover_native_files() {
    let mut cluster = Cluster::new();
    for id in 1..=3 {
        cluster.start(id, "create");
    }
    let leader = cluster.leader();
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
    let leader = cluster.leader();
    assert!(cluster.ok(leader, &["add", "2", "3"]).contains("Value(10)"));
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
        let cluster = Cluster::new();
        let first = first_available.then(|| {
            reply_peer(
                TcpListener::bind((Ipv4Addr::LOCALHOST, cluster.base + 101)).unwrap(),
                Some(b"ERR NOT_LEADER\n"),
            )
        });
        let second = reply_peer(
            TcpListener::bind((Ipv4Addr::LOCALHOST, cluster.base + 102)).unwrap(),
            Some(b"OK outcome=Value(7) duplicate=false\n"),
        );
        let third = TcpListener::bind((Ipv4Addr::LOCALHOST, cluster.base + 103)).unwrap();
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
        let cluster = Cluster::new();
        let first = reply_peer(
            TcpListener::bind((Ipv4Addr::LOCALHOST, cluster.base + 101)).unwrap(),
            Some(b"ERR NOT_LEADER\n"),
        );
        let second = reply_peer(
            TcpListener::bind((Ipv4Addr::LOCALHOST, cluster.base + 102)).unwrap(),
            reply,
        );
        let third = TcpListener::bind((Ipv4Addr::LOCALHOST, cluster.base + 103)).unwrap();
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
    let cluster = Cluster::new();
    let first = reply_peer(
        TcpListener::bind((Ipv4Addr::LOCALHOST, cluster.base + 101)).unwrap(),
        Some(b"ERR NOT_LEADER\n"),
    );
    let second = TcpListener::bind((Ipv4Addr::LOCALHOST, cluster.base + 102)).unwrap();
    second.set_nonblocking(true).unwrap();
    let third = TcpListener::bind((Ipv4Addr::LOCALHOST, cluster.base + 103)).unwrap();
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
