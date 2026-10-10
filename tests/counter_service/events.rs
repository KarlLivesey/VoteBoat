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
fn page(cluster: &Cluster, node: usize, session: u64, after: u64, count: usize) -> String {
    let args = [
        "events".to_string(),
        session.to_string(),
        after.to_string(),
        count.to_string(),
    ];
    let args = args.iter().map(String::as_str).collect::<Vec<_>>();
    let deadline = Instant::now() + Duration::from_secs(10);
    let reply = loop {
        let output = cluster.request(node, &args);
        if output.status.success() {
            break String::from_utf8(output.stdout).unwrap();
        }
        assert!(
            output.stdout.is_empty(),
            "unexpected event rejection: {output:?}"
        );
        assert!(
            Instant::now() < deadline,
            "event listener unavailable: {output:?}"
        );
        std::thread::park_timeout(Duration::from_millis(10));
    };
    assert!(reply.starts_with("OK evidence=local_volatile "), "{reply}");
    assert!(reply.len() < 4096);
    reply
}
fn field(reply: &str, name: &str) -> u64 {
    reply
        .split_whitespace()
        .filter_map(|f| f.split_once('='))
        .find(|(key, _)| *key == name)
        .unwrap()
        .1
        .parse()
        .unwrap()
}
fn history(quic: bool) {
    let mut cluster = Cluster::new();
    cluster.quic = quic;
    let access = cluster.root.join("events-access.txt");
    fs::write(&access, "voteboat-service-access-v1 1\n3 admin 1 1\n").unwrap();
    cluster.command_access = Some(access);
    cluster.command_principal = Some(3);
    for id in 1..=3 {
        cluster.start(id, "create");
    }
    let leader = cluster.leader();
    let first = page(&cluster, leader, 0, 0, 1);
    assert!(first.contains(":state:None:Running"));
    let session = field(&first, "store_session");
    assert_eq!(field(&first, "event_generation"), 1);
    assert_eq!(field(&first, "next"), 1);
    assert!(authenticated_write(&cluster, &["add", "19001", "7"]).contains("Value(7)"));
    // Local checkpoint submission is independent of leadership and returns admission only.
    cluster.ok(leader, &["checkpoint"]);
    let deadline = Instant::now() + Duration::from_secs(10);
    let mut after = 0;
    loop {
        let reply = page(&cluster, leader, session, after, 16);
        let next = field(&reply, "next");
        assert!(next >= after);
        assert_eq!(field(&reply, "missed"), 0);
        if reply.contains(":snapshots:") {
            break;
        }
        after = next;
        assert!(
            Instant::now() < deadline,
            "checkpoint diagnostic absent: {reply}"
        );
        std::thread::park_timeout(Duration::from_millis(5));
    }
    for args in [
        ["events", "0", "0", "17"],
        ["events", "0", "0", "0"],
        ["events", "0", "1", "1"],
    ] {
        assert!(!cluster.request(leader, &args).status.success());
    }
    let future = cluster.request(
        leader,
        &["events", &session.to_string(), &u64::MAX.to_string(), "1"],
    );
    assert!(!future.status.success());
    assert!(String::from_utf8(future.stdout)
        .unwrap()
        .contains("InvalidCursor"));
    cluster.stop();
    // A new local store session replaces this volatile stream; it cannot reuse old cursors.
    cluster.start(leader, "recover");
    let fresh = page(&cluster, leader, 0, 0, 1);
    assert!(field(&fresh, "store_session") > session);
    assert_eq!(field(&fresh, "oldest"), 1);
    let stale = cluster.request(leader, &["events", &session.to_string(), "1", "1"]);
    assert!(!stale.status.success());
    assert!(String::from_utf8(stale.stdout)
        .unwrap()
        .contains("stale event session"));
    for id in 1..=3 {
        if id != leader {
            cluster.start(id, "recover");
        }
    }
    cluster.leader();
    assert!(authenticated_write(&cluster, &["add", "19001", "7"]).contains("duplicate=true"));
    assert_eq!(cluster.routed(&["read"]), "OK value=7\n");
    cluster.stop();
    fs::remove_dir_all(&cluster.root).unwrap();
}
#[test]
fn tcp_bounded_events_checkpoint_and_restart_preserve_original_operations() {
    history(false);
}
#[cfg(feature = "quic")]
#[test]
fn quic_bounded_events_checkpoint_and_restart_preserve_original_operations() {
    history(true);
}
