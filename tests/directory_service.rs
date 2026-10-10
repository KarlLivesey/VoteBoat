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
//! Real metadata executable, publication receipts and remote quorum reads.
#![cfg(feature = "tls")]
use std::{
    fs,
    net::{TcpListener, UdpSocket},
    path::PathBuf,
    process::{Child, Command, Output, Stdio},
    sync::atomic::{AtomicU64, Ordering},
    time::{Duration, Instant},
};
use voteboat::{
    directory::DirectoryCommand, identity::*, placement::PlacementRequirements, routing::*,
};
const BIN: &str = env!("CARGO_BIN_EXE_voteboat-directory");
static NEXT: AtomicU64 = AtomicU64::new(32000);
fn group(n: u128) -> GroupIdentity {
    GroupIdentity {
        id: GroupId::new(n).unwrap(),
        incarnation: GroupIncarnation::new(1).unwrap(),
    }
}
fn plan(target: u128) -> String {
    let manifest = ResponsibilityManifest::new(ManifestInput {
        responsibility: ResponsibilityIdentity {
            id: ResponsibilityId::new(10).unwrap(),
            incarnation: ResponsibilityIncarnation::new(1).unwrap(),
        },
        parent: None,
        authority: group(42),
        application: ApplicationAdapter {
            id: ApplicationAdapterId::new(1).unwrap(),
            version: 1,
        },
        scheme: PartitionScheme {
            id: RoutingSchemeId::new(1).unwrap(),
            version: 1,
        },
        scope: BucketRange::new(0, 256).unwrap(),
        epoch: OwnershipEpoch::new(1).unwrap(),
        generation: RouteGeneration::new(1).unwrap(),
        placement: PlacementRequirements {
            minimum_voting_domains: 3,
            survive_any_single_domain_loss: true,
        },
        state: ResponsibilityState::Active,
        execution: ExecutionMode::Single(group(target)),
    })
    .unwrap();
    let bytes = DirectoryCommand {
        expected: None,
        manifest,
    }
    .encode(32768)
    .unwrap();
    let hex = bytes.iter().map(|b| format!("{b:02x}")).collect::<String>();
    format!("voteboat-directory-plan-v1 42 1 100\n101 {hex}\n")
}
struct Cluster {
    root: PathBuf,
    base: u16,
    children: Vec<Option<Child>>,
    held: Vec<TcpListener>,
    udp: Vec<UdpSocket>,
    quic: bool,
}
impl Cluster {
    fn new(quic: bool) -> Self {
        let (base, held, udp) = loop {
            let base = NEXT.fetch_add(128, Ordering::Relaxed) as u16;
            let ports = [
                base + 1,
                base + 2,
                base + 3,
                base + 101,
                base + 102,
                base + 103,
            ];
            let held = ports
                .into_iter()
                .map(|p| TcpListener::bind(("127.0.0.1", p)))
                .collect::<Result<Vec<_>, _>>();
            let udp = [base + 1, base + 2, base + 3]
                .into_iter()
                .map(|p| UdpSocket::bind(("127.0.0.1", p)))
                .collect::<Result<Vec<_>, _>>();
            if let (Ok(held), Ok(udp)) = (held, udp) {
                break (base, held, udp);
            }
        };
        let root =
            std::env::temp_dir().join(format!("voteboat-directory-{}-{base}", std::process::id()));
        fs::create_dir(&root).unwrap();
        let generated = Command::new(BIN)
            .args(["plan", "42", "1", "10", "1", "100", "1"])
            .output()
            .unwrap();
        assert!(generated.status.success());
        assert_eq!(String::from_utf8_lossy(&generated.stdout), plan(100));
        fs::write(root.join("plan"), generated.stdout).unwrap();
        fs::write(
            root.join("access"),
            "voteboat-service-access-v1 1\n1 reader 42 1\n2 writer 42 1\n3 admin 42 1\n",
        )
        .unwrap();
        Self {
            root,
            base,
            children: (0..3).map(|_| None).collect(),
            held,
            udp,
            quic,
        }
    }
    fn tls(&self) -> PathBuf {
        PathBuf::from(env!("CARGO_MANIFEST_DIR")).join("tests/fixtures/tls")
    }
    fn server(&self, id: usize, mode: &str) -> Command {
        let mut c = Command::new(BIN);
        c.args(["serve", mode])
            .arg(self.root.join(id.to_string()))
            .arg(id.to_string())
            .arg(self.base.to_string())
            .arg(self.tls())
            .arg(self.root.join("plan"))
            .arg(self.root.join("access"));
        if self.quic {
            c.args(["--transport", "quic"]);
        }
        c
    }
    fn start(&mut self, id: usize, mode: &str) {
        self.held.clear();
        self.udp.clear();
        let log = fs::File::create(self.root.join(format!("{id}.log"))).unwrap();
        self.children[id - 1] = Some(
            self.server(id, mode)
                .stdout(log.try_clone().unwrap())
                .stderr(log)
                .spawn()
                .unwrap(),
        );
    }
    fn client(&self, id: usize, principal: u64, args: &[&str]) -> Command {
        let mut c = Command::new(BIN);
        c.args(["client", &self.base.to_string(), &id.to_string()])
            .arg(self.tls())
            .arg(principal.to_string())
            .args(args);
        c
    }
    fn request(&self, id: usize, principal: u64, args: &[&str]) -> Output {
        self.client(id, principal, args).output().unwrap()
    }
    fn ok(&self, id: usize, args: &[&str]) -> String {
        let r = self.request(id, 3, args);
        assert!(
            r.status.success(),
            "{args:?}: {} {}",
            String::from_utf8_lossy(&r.stdout),
            String::from_utf8_lossy(&r.stderr)
        );
        String::from_utf8(r.stdout).unwrap()
    }
    fn lookup(&self, id: usize) -> Command {
        let mut c = Command::new(BIN);
        c.args(["lookup", &self.base.to_string(), &id.to_string()])
            .arg(self.tls())
            .args(["1", "42", "1", "10", "1"]);
        c
    }
    fn leader(&mut self) -> usize {
        let until = Instant::now() + Duration::from_secs(20);
        loop {
            for id in 1..=3 {
                if let Some(child) = self.children[id - 1].as_mut() {
                    assert!(
                        child.try_wait().unwrap().is_none(),
                        "{}",
                        fs::read_to_string(self.root.join(format!("{id}.log"))).unwrap()
                    );
                    let r = self.request(id, 3, &["status"]);
                    if r.status.success()
                        && String::from_utf8_lossy(&r.stdout).contains("role=Leader")
                    {
                        return id;
                    }
                }
            }
            assert!(Instant::now() < until, "no leader: {:?}", self.root);
            std::thread::sleep(Duration::from_millis(10));
        }
    }
    fn stop(&mut self) {
        for id in 1..=3 {
            if self.children[id - 1].is_some() {
                self.ok(id, &["quit"]);
            }
        }
        for child in &mut self.children {
            if child.is_some() {
                let deadline = Instant::now() + Duration::from_secs(10);
                loop {
                    if let Some(status) = child.as_mut().unwrap().try_wait().unwrap() {
                        assert!(status.success());
                        break;
                    }
                    assert!(Instant::now() < deadline, "worker drain stalled");
                    std::thread::sleep(Duration::from_millis(1));
                }
                *child = None;
            }
        }
    }

    fn kill(&mut self, id: usize) {
        let mut c = self.children[id - 1].take().unwrap();
        c.kill().unwrap();
        c.wait().unwrap();
    }
}
impl Drop for Cluster {
    fn drop(&mut self) {
        for c in self.children.iter_mut().flatten() {
            let _ = c.kill();
            let _ = c.wait();
        }
        let _ = fs::remove_dir_all(&self.root);
    }
}
fn initialize(c: &mut Cluster) -> usize {
    for id in 1..=3 {
        c.start(id, "create");
    }
    let leader = c.leader();
    let denied = c.request(leader, 2, &["initialize"]);
    assert!(!denied.status.success());
    assert!(String::from_utf8_lossy(&denied.stdout).contains("AUTHORIZATION"));
    assert!(c.ok(leader, &["initialize"]).contains("Initialized"));
    let missing = c.lookup(leader).output().unwrap();
    assert!(!missing.status.success());
    assert!(
        String::from_utf8_lossy(&missing.stderr).contains("Missing"),
        "unexpected unpublished lookup failure: {}",
        String::from_utf8_lossy(&missing.stderr)
    );
    assert!(c.ok(leader, &["publish", "101"]).contains("Published"));
    leader
}
fn history(quic: bool) {
    let mut c = Cluster::new(quic);
    let leader = initialize(&mut c);
    let r = c.lookup(leader).output().unwrap();
    assert!(r.status.success(), "{}", String::from_utf8_lossy(&r.stderr));
    assert!(String::from_utf8_lossy(&r.stdout).contains("authority=42"));
    c.ok(leader, &["checkpoint"]);
    let deadline = Instant::now() + Duration::from_secs(10);
    while c.ok(leader, &["status"]).contains("checkpoint_index=0 ") {
        assert!(Instant::now() < deadline);
        std::thread::sleep(Duration::from_millis(5));
    }
    c.stop();
    for id in 1..=3 {
        c.start(id, "recover");
    }
    let leader = c.leader();
    assert!(c.ok(leader, &["initialize"]).contains("duplicate=true"));
    assert!(c.ok(leader, &["publish", "101"]).contains("duplicate=true"));
    let r = c.lookup(leader).output().unwrap();
    assert!(r.status.success(), "{}", String::from_utf8_lossy(&r.stderr));
    c.stop();
    fs::write(c.root.join("plan"), plan(101)).unwrap();
    let out = c.server(1, "recover").output().unwrap();
    assert!(!out.status.success(), "changed durable plan accepted");
    let error = String::from_utf8_lossy(&out.stderr);
    assert!(
        error.contains("InvalidCheckpoint") || error.contains("InvalidCommand"),
        "wrong refusal: {error}"
    );
}
#[test]
fn directory_publication_remote_reads_and_checkpoint_recovery_tcp() {
    history(false);
}
#[cfg(feature = "quic")]
#[test]
fn directory_publication_remote_reads_and_checkpoint_recovery_quic() {
    history(true);
}
#[test]
fn disconnected_manifest_read_retains_and_drains_node_ownership() {
    let mut c = Cluster::new(false);
    let leader = initialize(&mut c);
    for id in 1..=3 {
        if id != leader {
            c.kill(id);
        }
    }
    let before = fs::read_to_string(c.root.join(format!("{leader}.log")))
        .unwrap()
        .len();
    let mut client = c
        .lookup(leader)
        .stdout(Stdio::null())
        .stderr(Stdio::null())
        .spawn()
        .unwrap();
    let deadline = Instant::now() + Duration::from_secs(3);
    loop {
        let log = fs::read_to_string(c.root.join(format!("{leader}.log"))).unwrap();
        if log[before..].contains("manifest read accepted") {
            break;
        }
        assert!(Instant::now() < deadline, "lookup never accepted: {log}");
        std::thread::sleep(Duration::from_millis(1));
    }
    assert!(
        client.try_wait().unwrap().is_none(),
        "lookup completed without quorum before cancellation"
    );
    client.kill().unwrap();
    client.wait().unwrap();
    assert!(c.ok(leader, &["status"]).contains("pending_reads=0"));
    for id in 1..=3 {
        if id != leader {
            c.start(id, "recover");
        }
    }
    let leader = c.leader();
    assert!(c.lookup(leader).output().unwrap().status.success());
    c.stop();
}
#[test]
fn invalid_metadata_plan_is_rejected_before_storage_or_tls() {
    let c = Cluster::new(false);
    for (text, reason) in [
        (
            "voteboat-directory-plan-v1 42 1 100\n101 zz\n".to_owned(),
            "invalid command hex",
        ),
        ("x".repeat(128 * 1024 + 1), "exceeds128KiB"),
    ] {
        fs::write(c.root.join("plan"), text).unwrap();
        let out = c.server(1, "create").output().unwrap();
        assert!(!out.status.success());
        assert!(String::from_utf8_lossy(&out.stderr).contains(reason));
        assert!(!c.root.join("1").exists());
    }
}

#[path = "directory_service/routes.rs"]
mod routes;
