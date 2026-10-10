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
fn configuration(c: &Cluster, generation: u64, stale: bool) -> String {
    let mut text = routes(c, false).replacen(
        "voteboat-command-peers-v1",
        &format!("voteboat-discovery-peers-v2 {generation}"),
        1,
    );
    if stale {
        text = text.replace(
            &format!("2 127.0.0.1:{}", c.base + 112),
            &format!("2 127.0.0.1:{}", c.base + 102),
        );
    }
    text
}
fn check_update(c: &mut Cluster, endpoint: &str) {
    c.command_principal = Some(1);
    assert!(c.ok(1, &["discovery-status"]).contains("generation=10"));
    let denied = c.request(1, &["discovery-update", "10", "11", "2", endpoint]);
    assert_eq!(
        String::from_utf8(denied.stdout).unwrap(),
        "ERR AUTHORIZATION\n"
    );
    c.command_principal = Some(3);
    assert_eq!(
        c.ok(1, &["discovery-update", "10", "11", "2", endpoint]),
        "OK generation=11 duplicate=false durable=false\n"
    );
    assert_eq!(
        c.ok(1, &["discovery-update", "10", "11", "2", endpoint]),
        "OK generation=11 duplicate=true durable=false\n"
    );
    let conflict = c.request(1, &["discovery-update", "10", "11", "2", "127.0.0.1:1"]);
    assert!(String::from_utf8_lossy(&conflict.stdout)
        .contains("ERR stale or invalid endpoint generation"));
    assert!(c.ok(1, &["discovery-status"]).contains("generation=11"));
}
fn history(quic: bool) {
    let mut c = prepared(quic);
    fs::write(
        c.discovery_peers.as_ref().unwrap(),
        configuration(&c, 10, true),
    )
    .unwrap();
    for id in 1..=3 {
        c.start(id, "create");
    }
    wait_for_source(&mut c);
    let unavailable = c.request(2, &["add", "20901", "9"]);
    assert!(!unavailable.status.success());
    assert!(!String::from_utf8_lossy(&unavailable.stdout).contains("UNKNOWN"));
    // Node2 may be leader while its discovery hint is deliberately stale.
    // Check the unchanged state independently, using known authenticated addresses.
    let source = c.discover_via.take();
    let bootstrap = c.command_peers.as_ref().unwrap().clone();
    let original = fs::read_to_string(&bootstrap).unwrap();
    fs::write(&bootstrap, routes(&c, false)).unwrap();
    assert_eq!(c.routed(&["read"]), "OK value=0\n");
    fs::write(bootstrap, original).unwrap();
    c.discover_via = source;
    let endpoint = format!("127.0.0.1:{}", c.base + 112);
    check_update(&mut c, &endpoint);
    assert!(c.ok(2, &["status"]).contains("OK role="));
    assert!(authenticated_write(&c, &["add", "20901", "9"]).contains("Value(9)"));
    assert_eq!(c.routed(&["read"]), "OK value=9\n");
    // Publishing never silently rewrites administrator-owned startup files.
    assert!(fs::read_to_string(c.discovery_peers.as_ref().unwrap())
        .unwrap()
        .starts_with("voteboat-discovery-peers-v2 10\n"));
    let leader = c.leader();
    c.ok(leader, &["checkpoint"]);
    c.stop();
    // The administrator explicitly persists the accepted endpoint snapshot.
    fs::write(
        c.discovery_peers.as_ref().unwrap(),
        configuration(&c, 11, false),
    )
    .unwrap();
    for id in 1..=3 {
        c.start(id, "recover");
    }
    wait_for_source(&mut c);
    assert!(c.ok(1, &["discovery-status"]).contains("generation=11"));
    assert!(c.ok(2, &["status"]).contains("OK role="));
    assert!(authenticated_write(&c, &["add", "20901", "9"]).contains("duplicate=true"));
    assert_eq!(c.routed(&["read"]), "OK value=9\n");
    c.stop();
    fs::remove_dir_all(&c.root).unwrap();
}
#[test]
fn versioned_discovery_update_is_authorized_retryable_and_restart_explicit_tcp() {
    history(false);
}
#[cfg(feature = "quic")]
#[test]
fn versioned_discovery_update_is_authorized_retryable_and_restart_explicit_quic() {
    history(true);
}

#[test]
fn versioned_source_rejects_zero_generation_before_storage_is_opened() {
    let c = prepared(false);
    fs::write(
        c.discovery_peers.as_ref().unwrap(),
        configuration(&c, 0, false),
    )
    .unwrap();
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
        .arg(c.discovery_peers.as_ref().unwrap()));
    assert!(!out.status.success());
    assert!(String::from_utf8_lossy(&out.stderr).contains("invalid hint generation"));
    assert!(!root.exists());
    fs::remove_dir_all(&c.root).unwrap();
}
