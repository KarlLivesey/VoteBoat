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
use super::*;

fn routes(cluster: &Cluster, stale: bool) -> String {
    let mut text = String::from("voteboat-command-peers-v1\n");
    for id in 1..=3 {
        let offset = if stale && id != 1 { 100 } else { 110 };
        text.push_str(&format!(
            "{id} 127.0.0.1:{} node{id}.voteboat.test\n",
            cluster.base + offset + id
        ));
    }
    text
}
fn prepared(quic: bool) -> Cluster {
    let mut c = Cluster::new();
    c.quic = quic;
    c.remote_commands = true;
    let access = c.root.join("access.txt");
    fs::write(
        &access,
        "voteboat-service-access-v1 1\n1 reader 1 1\n2 writer 1 1\n3 admin 1 1\n",
    )
    .unwrap();
    let advertised = c.root.join("advertised.txt");
    fs::write(&advertised, routes(&c, false)).unwrap();
    let bootstrap = c.root.join("bootstrap.txt");
    fs::write(&bootstrap, routes(&c, true)).unwrap();
    c.command_access = Some(access);
    c.command_principal = Some(3);
    c.command_peers = Some(bootstrap);
    c.discovery_peers = Some(advertised);
    c.discover_via = Some(1);
    c
}
// Readiness is a direct authenticated status exchange, independent of the
// discovery behavior whose refusal the following tests intend to exercise.
fn wait_for_source(c: &mut Cluster) {
    let source = c.discover_via.take();
    let deadline = Instant::now() + Duration::from_secs(20);
    loop {
        let out = c.request(1, &["status"]);
        if out.status.success() {
            break;
        }
        assert!(
            Instant::now() < deadline,
            "source not ready: {}\n{}",
            String::from_utf8_lossy(&out.stderr),
            c.service_log(1)
        );
        std::thread::sleep(Duration::from_millis(10));
    }
    c.discover_via = source;
}
fn history(quic: bool) {
    let mut c = prepared(quic);
    for id in 1..=3 {
        c.start(id, "create");
    }
    wait_for_source(&mut c);
    let leader = c.leader();
    for id in 1..=3 {
        assert!(c.ok(id, &["status"]).contains("OK role="));
    }
    c.command_principal = Some(1);
    assert_eq!(c.routed(&["read"]), "OK value=0\n");
    let denied = c.request(leader, &["add", "18801", "9"]);
    assert_eq!(
        String::from_utf8(denied.stdout).unwrap(),
        "ERR AUTHORIZATION\n"
    );
    c.command_principal = Some(2);
    assert!(authenticated_write(&c, &["add", "18801", "9"]).contains("Value(9)"));
    c.command_principal = Some(3);
    c.ok(leader, &["checkpoint"]);
    c.stop();
    for id in 1..=3 {
        c.start(id, "recover");
    }
    c.leader();
    assert!(authenticated_write(&c, &["add", "18801", "9"]).contains("duplicate=true"));
    assert_eq!(c.routed(&["read"]), "OK value=9\n");
    c.stop();
    fs::remove_dir_all(&c.root).unwrap();
}
#[test]
fn discovered_command_endpoints_preserve_retries_and_recovery_tcp() {
    history(false);
}
#[cfg(feature = "quic")]
#[test]
fn discovered_command_endpoints_preserve_retries_and_recovery_quic() {
    history(true);
}

#[test]
fn discovery_failure_never_submits_a_command_or_uses_stale_addresses() {
    let mut c = prepared(false);
    // A valid source can explicitly have no mapping for the requested target.
    fs::write(
        c.discovery_peers.as_ref().unwrap(),
        routes(&c, false)
            .lines()
            .take(2)
            .collect::<Vec<_>>()
            .join("\n")
            + "\n",
    )
    .unwrap();
    for id in 1..=3 {
        c.start(id, "create");
    }
    wait_for_source(&mut c);
    c.ok(1, &["status"]);
    let out = c.request(2, &["add", "18802", "99"]);
    assert!(!out.status.success());
    assert!(String::from_utf8_lossy(&out.stderr).contains("Missing"));
    assert!(!String::from_utf8_lossy(&out.stdout).contains("UNKNOWN"));
    // Discovery cannot change independent source certificate-name verification.
    let bootstrap = c.command_peers.as_ref().unwrap();
    let original = fs::read_to_string(bootstrap).unwrap();
    fs::write(
        bootstrap,
        original.replace("node1.voteboat.test", "wrong.voteboat.test"),
    )
    .unwrap();
    let out = c.request(1, &["add", "18803", "99"]);
    assert!(!out.status.success());
    assert!(String::from_utf8_lossy(&out.stderr)
        .contains("authentication failed before command submission"));
    fs::write(bootstrap, original).unwrap();
    c.discover_via = None;
    fs::write(bootstrap, routes(&c, false)).unwrap();
    assert_eq!(c.routed(&["read"]), "OK value=0\n");
    c.stop();
    fs::remove_dir_all(&c.root).unwrap();
}

#[test]
fn discovery_is_opt_in_and_requires_authentication_before_opening_storage() {
    let mut c = prepared(false);
    c.discovery_peers = None;
    for id in 1..=3 {
        c.start(id, "create");
    }
    wait_for_source(&mut c);
    let out = c.request(1, &["add", "18804", "99"]);
    assert!(!out.status.success());
    assert!(String::from_utf8_lossy(&out.stderr).contains("discovery upgrade refused"));
    c.discover_via = None;
    fs::write(c.command_peers.as_ref().unwrap(), routes(&c, false)).unwrap();
    assert_eq!(c.routed(&["read"]), "OK value=0\n");
    c.stop();
    let root = c.root.join("never-created");
    let out = run(Command::new(BIN)
        .args(["serve", "create"])
        .arg(&root)
        .args([
            "1",
            &c.base.to_string(),
            "/missing-tls",
            "--discovery-peers",
            "/missing-peers",
        ]));
    assert!(!out.status.success());
    assert!(String::from_utf8_lossy(&out.stderr).contains("requires --service-access"));
    assert!(!root.exists());
    fs::remove_dir_all(&c.root).unwrap();
}

#[test]
fn discovery_scope_and_startup_validation_fail_before_submission() {
    let mut c = prepared(false);
    fs::write(
        c.command_access.as_ref().unwrap(),
        "voteboat-service-access-v1 1\n1 reader 2 1\n2 writer 1 1\n3 admin 1 1\n",
    )
    .unwrap();
    for id in 1..=3 {
        c.start(id, "create");
    }
    wait_for_source(&mut c);
    c.command_principal = Some(1);
    let out = c.request(1, &["add", "18805", "99"]);
    assert!(!out.status.success());
    assert!(String::from_utf8_lossy(&out.stderr).contains("discovery upgrade refused"));
    c.command_principal = Some(3);
    assert_eq!(c.routed(&["read"]), "OK value=0\n");
    c.stop();
    let source = c.discovery_peers.as_ref().unwrap();
    fs::write(source, "invalid header\n").unwrap();
    let root = c.root.join("never-created");
    let out = run(Command::new(BIN)
        .args(["serve", "create"])
        .arg(&root)
        .args([
            "1",
            &c.base.to_string(),
            "/missing-tls",
            "--service-access",
            "/missing-access",
            "--discovery-peers",
        ])
        .arg(source));
    assert!(!out.status.success());
    assert!(String::from_utf8_lossy(&out.stderr).contains("expected voteboat-command-peers-v1"));
    assert!(!root.exists());
    fs::remove_dir_all(&c.root).unwrap();
}
