// SPDX-License-Identifier: RPL-1.5
// Copyright (c) 2026 Karl Livesey
use super::*;
#[path = "../support/credential_commands.rs"]
mod commands;
fn select(c: &Cluster, generation: u64, directory: &std::path::Path) {
    fs::write(
        c.root.join("peer-credentials"),
        format!(
            "voteboat-peer-credentials-v1 {generation}\ntls {}\n",
            directory.display()
        ),
    )
    .unwrap();
}
fn wait(c: &Cluster, node: usize, verb: &str, generation: u64, state: &str) {
    let end = Instant::now() + Duration::from_secs(10);
    loop {
        let out = c.request(node, 3, &[verb, "1"]);
        let text = String::from_utf8(out.stdout).unwrap();
        if out.status.success() && text.contains(&format!("state={state}")) {
            assert!(
                text.starts_with(&format!("OK generation={generation} ")),
                "{text}"
            );
            return;
        }
        assert!(Instant::now() < end, "{node}: {text}");
        std::thread::park_timeout(Duration::from_millis(5));
    }
}
fn lookup(c: &mut Cluster) -> Vec<u8> {
    let leader = c.leader();
    let out = c.lookup(leader).fixture_output().unwrap();
    assert!(
        out.status.success(),
        "{}",
        String::from_utf8_lossy(&out.stderr)
    );
    out.stdout
}
fn rotate(c: &mut Cluster) {
    let next = PathBuf::from(env!("CARGO_MANIFEST_DIR")).join("tests/fixtures/four-node-tls");
    select(c, 2, &next);
    for principal in [1, 2] {
        let denied = c.request(1, principal, &["reload-peers", "1", "1", "2"]);
        assert!(!denied.status.success());
        assert!(String::from_utf8_lossy(&denied.stdout).contains("AUTHORIZATION"));
    }
    let unread = commands::send(c.base, 1, &c.tls(), "reload-peers 1 1 2");
    commands::record(&c.root.join("1/PEER-CREDENTIAL-RELOAD"));
    drop(unread);
    c.kill(1);
    c.start(1, "recover");
    wait(c, 1, "peer-credential-status", 2, "recorded");
    for node in 2..=3 {
        assert!(c
            .ok(node, &["reload-peers", "1", "1", "2"])
            .contains("queued=true"));
        wait(c, node, "peer-credential-status", 2, "recorded");
    }
    assert!(c
        .ok(1, &["reload-peers", "1", "1", "2"])
        .contains("already_recorded=true"));
}
fn refused(c: &mut Cluster, message: &str) {
    c.start(1, "recover");
    let end = Instant::now() + Duration::from_secs(5);
    loop {
        if let Some(status) = c.children[0].as_mut().unwrap().try_wait().unwrap() {
            assert!(!status.success());
            break;
        }
        assert!(Instant::now() < end, "accepted invalid peer selection");
        std::thread::park_timeout(Duration::from_millis(1));
    }
    c.children[0] = None;
    let log = fs::read_to_string(c.root.join("1.log")).unwrap();
    assert!(log.contains(message), "{log}");
    assert!(!log.contains("ready directory"));
}
fn restart(c: &mut Cluster) {
    c.stop();
    select(c, 1, &c.tls());
    refused(c, "credential files conflict with durable reload record");
    select(c, 2, &c.tls());
    refused(c, "credential files conflict with durable reload record");
    let next = PathBuf::from(env!("CARGO_MANIFEST_DIR")).join("tests/fixtures/four-node-tls");
    select(c, 2, &next);
    c.peer_credentials = false;
    refused(
        c,
        "existing peer credential journal requires --peer-credentials",
    );
    c.peer_credentials = true;
    for node in 1..=3 {
        c.start(node, "recover");
    }
}
fn access(c: &Cluster) {
    fs::write(
        c.root.join("access"),
        "voteboat-service-access-v1 2\n1 reader 42 1\n2 admin 42 1\n3 admin 42 1\n",
    )
    .unwrap();
    for node in 1..=3 {
        // Access publication can close its own reply channel.
        let _ = c.request(node, 3, &["reload-access", "1", "1", "2"]);
        wait(c, node, "credential-status", 2, "recorded");
        assert!(c
            .request(node, 2, &["peer-credential-status", "1"])
            .status
            .success());
        assert!(!c
            .request(node, 1, &["reload-access", "2", "2", "3"])
            .status
            .success());
    }
}
fn checkpoint(c: &Cluster, leader: usize) {
    let field = |text: String, name: &str| {
        text.split_whitespace()
            .find_map(|w| w.strip_prefix(name))
            .unwrap()
            .parse::<u64>()
            .unwrap()
    };
    let committed = field(c.ok(leader, &["status"]), "committed=");
    c.ok(leader, &["checkpoint"]);
    let end = Instant::now() + Duration::from_secs(10);
    while field(c.ok(leader, &["status"]), "checkpoint_index=") < committed {
        assert!(
            Instant::now() < end,
            "checkpoint did not reach committed prefix"
        );
        std::thread::park_timeout(Duration::from_millis(5));
    }
}
fn history(quic: bool) {
    let mut c = Cluster::new(quic);
    c.peer_credentials = true;
    select(&c, 1, &c.tls());
    initialize(&mut c);
    let before = lookup(&mut c);
    rotate(&mut c);
    assert!(initialization::command(&mut c, &["initialize"])
        .1
        .contains("duplicate=true"));
    assert!(initialization::command(&mut c, &["publish", "101"])
        .1
        .contains("duplicate=true"));
    assert_eq!(lookup(&mut c), before);
    if quic {
        let leader = c.leader();
        checkpoint(&c, leader);
    }
    restart(&mut c);
    assert_eq!(lookup(&mut c), before);
    for node in 1..=3 {
        wait(&c, node, "peer-credential-status", 2, "recorded");
    }
    access(&c);
    c.stop();
    for node in 1..=3 {
        c.start(node, "recover");
        wait(&c, node, "credential-status", 2, "recorded");
    }
    assert_eq!(lookup(&mut c), before);
    c.stop();
}
#[test]
fn peer_rotation_preserves_committed_directory_and_command_access_tcp() {
    history(false);
}
#[cfg(feature = "quic")]
#[test]
fn peer_rotation_preserves_committed_directory_and_command_access_quic() {
    history(true);
}
