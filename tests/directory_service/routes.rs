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
//! Executable recursive routing against three independent replicated authorities.
use super::*;
fn responsibility(n: u128) -> ResponsibilityIdentity {
    ResponsibilityIdentity {
        id: ResponsibilityId::new(n).unwrap(),
        incarnation: ResponsibilityIncarnation::new(1).unwrap(),
    }
}
fn parent(n: u128, g: u128) -> ParentAuthority {
    ParentAuthority {
        responsibility: responsibility(n),
        group: group(g),
    }
}
fn child(n: u128, g: u128, end: u16) -> RouteEntry {
    RouteEntry {
        scope: BucketRange::new(0, end).unwrap(),
        target: RouteTarget::Child(ChildAuthority {
            responsibility: responsibility(n),
            group: group(g),
            epoch: OwnershipEpoch::new(1).unwrap(),
        }),
    }
}
fn manifest(
    g: u128,
    n: u128,
    p: Option<ParentAuthority>,
    end: u16,
    execution: ExecutionMode,
) -> ManifestInput {
    ManifestInput {
        responsibility: responsibility(n),
        parent: p,
        authority: group(g),
        application: ApplicationAdapter {
            id: ApplicationAdapterId::new(1).unwrap(),
            version: 1,
        },
        scheme: PartitionScheme {
            id: RoutingSchemeId::new(1).unwrap(),
            version: 1,
        },
        scope: BucketRange::new(0, end).unwrap(),
        epoch: OwnershipEpoch::new(1).unwrap(),
        generation: RouteGeneration::new(1).unwrap(),
        placement: PlacementRequirements {
            minimum_voting_domains: 3,
            survive_any_single_domain_loss: true,
        },
        state: ResponsibilityState::Active,
        execution,
    }
}
fn provision(c: &Cluster, input: ManifestInput) {
    let g = input.authority.id.get();
    let bytes = DirectoryCommand {
        expected: None,
        manifest: ResponsibilityManifest::new(input).unwrap(),
    }
    .encode(32768)
    .unwrap();
    let hex = bytes.iter().map(|b| format!("{b:02x}")).collect::<String>();
    fs::write(
        c.root.join("plan"),
        format!("voteboat-directory-plan-v1 {g} 1 100\n101 {hex}\n"),
    )
    .unwrap();
    fs::write(
        c.root.join("access"),
        format!("voteboat-service-access-v1 1\n1 reader {g} 1\n3 admin {g} 1\n"),
    )
    .unwrap();
}
fn publish(c: &mut Cluster) -> usize {
    for id in 1..=3 {
        c.start(id, "create");
    }
    let leader = c.leader();
    c.ok(leader, &["initialize"]);
    c.ok(leader, &["publish", "101"]);
    leader
}
struct Tree {
    root: Cluster,
    middle: Cluster,
    leaf: Cluster,
    map: PathBuf,
    leaf_leader: usize,
}
impl Tree {
    fn new(quic: bool, change: impl FnOnce(&mut ManifestInput)) -> Self {
        let mut root = Cluster::new(quic);
        let mut middle = Cluster::new(quic);
        let mut leaf = Cluster::new(quic);
        let unused = RouteEntry {
            scope: BucketRange::new(128, 256).unwrap(),
            target: RouteTarget::Child(ChildAuthority {
                responsibility: responsibility(40),
                group: group(45),
                epoch: OwnershipEpoch::new(1).unwrap(),
            }),
        };
        provision(
            &root,
            manifest(
                42,
                10,
                None,
                256,
                ExecutionMode::Delegated(vec![child(20, 43, 128), unused]),
            ),
        );
        provision(
            &middle,
            manifest(
                43,
                20,
                Some(parent(10, 42)),
                128,
                ExecutionMode::Delegated(vec![child(30, 44, 128)]),
            ),
        );
        let mut input = manifest(
            44,
            30,
            Some(parent(20, 43)),
            128,
            ExecutionMode::Single(group(100)),
        );
        change(&mut input);
        provision(&leaf, input);
        publish(&mut root);
        publish(&mut middle);
        let leaf_leader = publish(&mut leaf);
        let map = root.root.join("authorities");
        let mut rows = "voteboat-authorities-v1\n".to_owned();
        for (g, c) in [(42, &root), (43, &middle), (44, &leaf)] {
            for node in 1..=3 {
                rows.push_str(&format!(
                    "{g} 1 {node} 127.0.0.1:{} node{node}.voteboat.test\n",
                    c.base + 100 + node
                ));
            }
        }
        fs::write(&map, rows).unwrap();
        Self {
            root,
            middle,
            leaf,
            map,
            leaf_leader,
        }
    }
    fn command(
        &self,
        authority: &str,
        responsibility: &str,
        key: &str,
        options: &[&str],
    ) -> Command {
        let mut c = Command::new(BIN);
        c.arg("route")
            .arg(self.root.tls())
            .args(["1", authority, "1", responsibility, "1", key])
            .arg(&self.map)
            .args(options);
        c
    }
    fn success(&self, authority: &str, responsibility: &str) {
        let out = self
            .command(authority, responsibility, "10", &[])
            .output()
            .unwrap();
        assert!(
            out.status.success(),
            "{}",
            String::from_utf8_lossy(&out.stderr)
        );
        let text = String::from_utf8(out.stdout).unwrap();
        assert!(text.contains("responsibility=30 incarnation=1 execution_group=100 execution_incarnation=1 scope=0..128 bucket=10 epoch=1 generation=1 hint_only=true"),"{text}");
    }
    fn refused(&self, options: &[&str], reason: &str) {
        let out = self.command("42", "10", "10", options).output().unwrap();
        refused(out, reason);
    }
    fn stop(&mut self) {
        self.root.stop();
        self.middle.stop();
        self.leaf.stop();
    }
}
fn refused(out: Output, reason: &str) {
    assert!(
        !out.status.success(),
        "unexpected route: {}",
        String::from_utf8_lossy(&out.stdout)
    );
    assert!(!String::from_utf8_lossy(&out.stdout).contains("hint_only=true"));
    assert!(
        String::from_utf8_lossy(&out.stderr).contains(reason),
        "expected {reason}: {}",
        String::from_utf8_lossy(&out.stderr)
    );
}
fn history(quic: bool) {
    let mut t = Tree::new(quic, |_| {});
    t.success("42", "10");
    t.refused(&["--max-hops", "2"], "HopLimit");
    refused(
        t.command("42", "10", "200", &[]).output().unwrap(),
        "WrongAuthority",
    );
    t.root.stop();
    t.middle.stop();
    t.success("44", "30");
    t.leaf.stop();
}
#[test]
fn recursive_route_tcp_and_known_child_without_ancestors() {
    history(false);
}
#[cfg(feature = "quic")]
#[test]
fn recursive_route_quic_and_known_child_without_ancestors() {
    history(true);
}
#[test]
fn recursive_route_rejects_wrong_parent_epoch_and_scope() {
    for (change, reason) in [
        (0, "WrongParent"),
        (1, "WrongChild"),
        (2, "WrongChild"),
        (3, "UnsupportedSchema"),
    ] {
        let mut t = Tree::new(false, |m| match change {
            0 => m.parent = Some(parent(99, 43)),
            1 => m.epoch = OwnershipEpoch::new(2).unwrap(),
            2 => m.scope = BucketRange::new(0, 64).unwrap(),
            _ => m.scheme.id = RoutingSchemeId::new(2).unwrap(),
        });
        t.refused(&[], reason);
        t.stop();
    }
}
#[test]
fn recursive_route_rejects_wrong_authority_and_tls_identity() {
    let mut t = Tree::new(false, |_| {});
    let original = fs::read_to_string(&t.map).unwrap();
    let wrong = original
        .lines()
        .map(|line| {
            if line.starts_with("42 1 ") {
                let words = line.split_whitespace().collect::<Vec<_>>();
                let node = words[2].parse::<u16>().unwrap();
                format!(
                    "42 1 {node} 127.0.0.1:{} node{node}.voteboat.test",
                    t.middle.base + 100 + node
                )
            } else {
                line.to_owned()
            }
        })
        .collect::<Vec<_>>()
        .join("\n");
    fs::write(&t.map, wrong).unwrap();
    t.refused(&[], "Session(Truncated)");
    fs::write(
        &t.map,
        original
            .replace("node1", "wrong.example")
            .replace("node2", "wrong.example")
            .replace("node3", "wrong.example"),
    )
    .unwrap();
    t.refused(&[], "authentication/connection failed");
    t.stop();
}
#[test]
fn recursive_route_missing_leaf_is_not_a_partial_success() {
    let mut t = Tree::new(false, |m| m.responsibility = responsibility(31));
    t.refused(&[], "Missing");
    t.stop();
}
#[test]
fn recursive_route_interrupted_leaf_never_returns_a_hint() {
    let mut t = Tree::new(false, |_| {});
    for id in 1..=3 {
        if id != t.leaf_leader {
            t.leaf.kill(id);
        }
    }
    let log = t.leaf.root.join(format!("{}.log", t.leaf_leader));
    let before = fs::read_to_string(&log).unwrap().len();
    let output = t.root.root.join("route.stdout");
    let error = t.root.root.join("route.stderr");
    let mut client = t
        .command("42", "10", "10", &[])
        .stdout(fs::File::create(&output).unwrap())
        .stderr(fs::File::create(&error).unwrap())
        .spawn()
        .unwrap();
    let accepted = Instant::now() + Duration::from_secs(8);
    loop {
        if fs::read_to_string(&log).unwrap()[before..].contains("manifest read accepted") {
            break;
        }
        assert!(
            client.try_wait().unwrap().is_none(),
            "{}",
            fs::read_to_string(&error).unwrap()
        );
        assert!(Instant::now() < accepted, "leaf lookup not accepted");
        std::thread::sleep(Duration::from_millis(1));
    }
    t.leaf.kill(t.leaf_leader);
    let deadline = Instant::now() + Duration::from_secs(12);
    let status = loop {
        if let Some(s) = client.try_wait().unwrap() {
            break s;
        }
        assert!(Instant::now() < deadline, "route did not terminate");
        std::thread::sleep(Duration::from_millis(1));
    };
    assert!(!status.success());
    assert!(!fs::read_to_string(&output)
        .unwrap()
        .contains("hint_only=true"));
    assert!(!fs::read_to_string(&error).unwrap().is_empty());
    t.root.stop();
    t.middle.stop();
}
#[test]
fn recursive_route_rejects_invalid_maps_and_options_before_connections() {
    let c = Cluster::new(false);
    let map = c.root.join("map");
    let valid = "voteboat-authorities-v1\n42 1 1 127.0.0.1:1 node1\n";
    for (content, reason) in [
        ("x".repeat(32769), "exceeds32KiB"),
        ("voteboat-authorities-v1\n".into(), "empty authority map"),
        (
            format!("{valid}42 1 1 127.0.0.1:2 node1\n"),
            "duplicate authority",
        ),
        (
            format!("{valid}43 1 1 127.0.0.1:2 node2\n"),
            "conflicting TLS name",
        ),
        (
            "voteboat-authorities-v1\n42 1 1 0.0.0.0:10 node1\n".into(),
            "invalid or duplicate",
        ),
    ] {
        fs::write(&map, content).unwrap();
        let out = Command::new(BIN)
            .arg("route")
            .args(["/missing-tls", "1", "42", "1", "10", "1", "10"])
            .arg(&map)
            .output()
            .unwrap();
        refused(out, reason);
    }
    fs::write(&map, valid).unwrap();
    for args in [
        vec!["--max-hops", "0"],
        vec!["--max-hops", "33"],
        vec!["--min-epoch", "0"],
        vec!["--min-generation", "0"],
        vec!["--max-hops", "2", "--max-hops", "3"],
    ] {
        let out = Command::new(BIN)
            .arg("route")
            .args(["/missing-tls", "1", "42", "1", "10", "1", "10"])
            .arg(&map)
            .args(args)
            .output()
            .unwrap();
        assert!(!out.status.success());
        assert!(!String::from_utf8_lossy(&out.stderr).contains("No such file"));
    }
}

#[test]
fn recursive_route_enforces_root_observation_floors() {
    let mut t = Tree::new(false, |_| {});
    for flag in ["--min-epoch", "--min-generation"] {
        let out = t.command("42", "10", "10", &[flag, "2"]).output().unwrap();
        let error = String::from_utf8_lossy(&out.stderr);
        assert!(
            error.contains("deadline expired") || error.contains("connection budget exhausted"),
            "unexpected stale-floor failure: {error}"
        );
        refused(out, "recursive lookup");
    }
    t.success("42", "10");
    t.stop();
}

#[test]
fn recursive_route_rotates_unavailable_endpoints_after_leader_loss() {
    let mut t = Tree::new(false, |_| {});
    let leader = t.root.leader();
    t.root.kill(leader);
    t.root.leader();
    t.success("42", "10");
    t.stop();
}
