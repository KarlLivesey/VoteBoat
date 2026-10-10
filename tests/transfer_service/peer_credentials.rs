// SPDX-License-Identifier: RPL-1.5
// Copyright (c) 2026 Karl Livesey
use super::*;
#[path = "../support/credential_commands.rs"]
mod commands;
pub(super) fn select(c: &Cluster, generation: u64, directory: &std::path::Path) {
    fs::write(
        c.root.join("peer-credentials"),
        format!(
            "voteboat-peer-credentials-v1 {generation}\ntls {}\n",
            directory.display()
        ),
    )
    .unwrap();
}
fn wait(c: &Cluster, group: u128, node: u16, request: &str, generation: u64, state: &str) {
    let end = Instant::now() + Duration::from_secs(10);
    loop {
        let out = c.request(node, 3, group, &["peer-credential-status", request]);
        let text = String::from_utf8(out.stdout).unwrap();
        if out.status.success() && text.contains(&format!("state={state}")) {
            assert!(
                text.starts_with(&format!("OK generation={generation} ")),
                "{text}"
            );
            return;
        }
        assert!(Instant::now() < end, "{group}/{node}: {text}");
        std::thread::park_timeout(Duration::from_millis(5));
    }
}
fn rotate(c: &mut Cluster) {
    let next = PathBuf::from(env!("CARGO_MANIFEST_DIR")).join("tests/fixtures/four-node-tls");
    select(c, 2, &next);
    for (slot, group) in GROUPS.into_iter().enumerate() {
        let denied = c.request(1, 2, group, &["reload-peers", "1", "1", "2"]);
        assert!(!denied.status.success());
        assert!(String::from_utf8_lossy(&denied.stderr).contains("AUTHORIZATION"));
        let unread = commands::send(
            c.base + slot as u16 * 128,
            1,
            &c.tls(),
            "reload-peers 1 1 2",
        );
        commands::record(&c.root.join(format!("{group}/1/PEER-CREDENTIAL-RELOAD")));
        drop(unread);
        let index = c
            .children
            .iter()
            .position(|(g, n, _)| *g == group && *n == 1)
            .unwrap();
        let (_, _, mut child) = c.children.remove(index);
        child.kill().unwrap();
        child.wait().unwrap();
        c.start_one(group, slot, 1, "recover");
        wait(c, group, 1, "1", 2, "recorded");
        for node in 2..=3 {
            let out = c.request(node, 3, group, &["reload-peers", "1", "1", "2"]);
            assert!(
                out.status.success(),
                "{}",
                String::from_utf8_lossy(&out.stderr)
            );
            wait(c, group, node, "1", 2, "recorded");
        }
    }
}
fn restart(c: &mut Cluster) {
    let next = PathBuf::from(env!("CARGO_MANIFEST_DIR")).join("tests/fixtures/four-node-tls");
    select(c, 3, &c.root.join("missing-tls"));
    assert!(c
        .request(1, 3, 1, &["reload-peers", "2", "2", "3"])
        .status
        .success());
    wait(c, 1, 1, "2", 2, "failed");
    c.crash();
    select(c, 1, &c.tls());
    refused(c, "credential files conflict with durable reload record");
    select(c, 2, &c.tls());
    refused(c, "credential files conflict with durable reload record");
    select(c, 2, &next);
    c.peer_credentials = false;
    refused(
        c,
        "existing peer credential journal requires --peer-credentials",
    );
    c.peer_credentials = true;
    c.start("recover");
    for group in GROUPS {
        wait(c, group, 1, "1", 2, "recorded");
        let out = c.request(1, 3, group, &["reload-peers", "1", "1", "2"]);
        assert!(out.status.success());
        assert_eq!(out.stdout, b"OK reload=1 already_recorded=true\n");
        for node in 2..=3 {
            wait(c, group, node, "1", 2, "recorded");
        }
    }
}
fn refused(c: &mut Cluster, message: &str) {
    for (slot, group) in GROUPS.into_iter().enumerate() {
        c.start_one(group, slot, 1, "recover");
        let (_, _, mut child) = c.children.pop().unwrap();
        let end = Instant::now() + Duration::from_secs(5);
        loop {
            if let Some(status) = child.try_wait().unwrap() {
                assert!(!status.success());
                break;
            }
            if Instant::now() >= end {
                child.kill().unwrap();
                child.wait().unwrap();
                panic!("accepted invalid credentials: {group}");
            }
            std::thread::park_timeout(Duration::from_millis(1));
        }
        let log = fs::read_to_string(c.root.join(format!("{group}-1.log"))).unwrap();
        assert!(log.contains(message), "{log}");
        assert!(!log.contains("ready transfer"));
    }
}
fn history(quic: bool) {
    let mut c = Cluster::with_options(quic, true, false, true);
    initialize(&c);
    for _ in 0..3 {
        c.operate("step");
    }
    let before = c.operate("status");
    assert!(before.contains("Fence("), "{before}");
    rotate(&mut c);
    assert_eq!(c.operate("status"), before);
    assert!(c.operate("resume").contains("OK complete"));
    if quic {
        cuts::support::checkpoint(&c);
    }
    restart(&mut c);
    assert!(c.operate("resume").contains("OK complete"));
    finish(c);
}
#[test]
fn peer_rotation_preserves_split_roles_and_restart_tcp() {
    history(false);
}
#[cfg(feature = "quic")]
#[test]
fn peer_rotation_preserves_split_roles_and_restart_quic() {
    history(true);
}
