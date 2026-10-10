// SPDX-License-Identifier: RPL-1.5
// Copyright (c) 2026 Karl Livesey
use super::*;
fn fixture(name: &str) -> PathBuf {
    PathBuf::from(env!("CARGO_MANIFEST_DIR"))
        .join("tests/fixtures")
        .join(name)
}
fn select(path: &std::path::Path, generation: u64, keys: &std::path::Path) {
    fs::write(
        path,
        format!(
            "voteboat-peer-credentials-v1 {generation}\ntls {}\n",
            keys.display()
        ),
    )
    .unwrap();
}
fn setup(quic: bool, multi: bool, member: bool) -> Cluster {
    let mut c = if multi {
        groups::setup(quic)
    } else {
        Cluster::new()
    };
    c.quic = quic;
    if member {
        let deployment = c.root.join("deployment");
        let rows = (1..=3)
            .map(|n| {
                format!(
                    "{n} {n} 1 127.0.0.1:{} node{n}.voteboat.test\n",
                    c.base + n as u16
                )
            })
            .collect::<String>();
        fs::write(&deployment, format!("voteboat-deployment-v1\n{rows}")).unwrap();
        c.deployment = Some(deployment);
    }
    let access = c.root.join("command-access");
    let grants = if multi {
        "2 admin 1 1\n3 admin 1 1\n3 admin 7 3\n3 admin 8 2\n"
    } else {
        "2 writer 1 1\n3 admin 1 1\n"
    };
    fs::write(&access, format!("voteboat-service-access-v1 1\n{grants}")).unwrap();
    c.command_access = Some(access);
    c.command_principal = Some(3);
    let path = c.root.join("peer-credentials");
    select(&path, 1, &fixture("tls"));
    c.peer_credentials = Some(path);
    let deployment = c.deployment.take();
    for id in 1..=3 {
        c.start(id, "create");
    }
    if member {
        // Explicit deployment opens existing membership; it cannot create it.
        c.leader();
        c.stop();
        c.deployment = deployment;
        for id in 1..=3 {
            c.start(id, "recover-member");
        }
    }
    c
}
fn wait(c: &Cluster, id: usize, sequence: &str, state: &str, generation: u64) {
    let deadline = Instant::now() + Duration::from_secs(10);
    loop {
        let output = c.request(id, &["peer-credential-status", sequence]);
        let text = String::from_utf8(output.stdout).unwrap();
        if output.status.success() && text.contains(&format!("state={state}")) {
            assert!(
                text.starts_with(&format!("OK generation={generation} ")),
                "{text}"
            );
            return;
        }
        assert!(Instant::now() < deadline, "{text}; {}", c.service_log(id));
        std::thread::park_timeout(Duration::from_millis(5));
    }
}
fn data(c: &Cluster, operation: &str, delta: &str, value: &str, duplicate: bool) {
    let groups = if c.groups.is_some() {
        vec![("1", "1"), ("7", "3"), ("8", "2")]
    } else {
        vec![("1", "1")]
    };
    for (group, incarnation) in groups {
        let (text, retried) = write(c, &["group", group, incarnation, "add", operation, delta]);
        assert!(text.contains(&format!("Value({value})")), "{text}");
        if duplicate || !retried {
            assert!(text.contains(&format!("duplicate={duplicate}")), "{text}");
        } else {
            assert!(
                text.contains("duplicate=true") || text.contains("duplicate=false"),
                "{text}"
            );
        }
        assert_eq!(
            c.routed(&["group", group, incarnation, "read"]),
            format!("OK value={value}\n")
        );
    }
}
fn write(c: &Cluster, args: &[&str]) -> (String, bool) {
    let end = Instant::now() + Duration::from_secs(10);
    let mut retried = false;
    loop {
        let output = c.target("auto", args);
        let text = String::from_utf8(output.stdout).unwrap();
        if output.status.success() {
            return (text, retried);
        }
        assert!(text.starts_with("UNKNOWN LeadershipChanged;"), "{text}");
        assert!(
            Instant::now() < end,
            "original write did not settle: {text}"
        );
        retried = true;
        std::thread::park_timeout(Duration::from_millis(10));
    }
}
fn observe_lost_reply(c: &mut Cluster) {
    let unread = UnobservedCommand::send(c, 1, "reload-peers 1 1 2");
    let record = c.root.join("1/PEER-CREDENTIAL-RELOAD");
    let deadline = Instant::now() + Duration::from_secs(10);
    while !record.exists() {
        assert!(Instant::now() < deadline, "{}", c.service_log(1));
        std::thread::park_timeout(Duration::from_millis(1));
    }
    unread.disconnect();
    let mut child = c.children[0].take().unwrap();
    child.kill().unwrap();
    child.wait().unwrap();
    c.start(1, recovery(c));
    wait(c, 1, "1", "recorded", 2);
}
fn failures(c: &mut Cluster) {
    assert!(c
        .ok(1, &["reload-peers", "1", "1", "2"])
        .contains("already_recorded=true"));
    for args in [
        ["reload-peers", "1", "2", "3"],
        ["reload-peers", "2", "1", "3"],
    ] {
        assert!(!c.request(1, &args).status.success());
    }
    let path = c.peer_credentials.as_ref().unwrap();
    select(path, 3, &c.root.join("absent-keys"));
    assert!(c
        .ok(1, &["reload-peers", "2", "2", "3"])
        .contains("queued=true"));
    wait(c, 1, "2", "failed", 2);
    select(path, 2, &fixture("four-node-tls"));
}
fn refused_start(c: &mut Cluster, expected: &str) {
    c.start(1, recovery(c));
    let deadline = Instant::now() + Duration::from_secs(10);
    loop {
        if let Some(status) = c.children[0].as_mut().unwrap().try_wait().unwrap() {
            assert!(!status.success());
            let log = c.service_log(1);
            assert!(log.contains(expected), "{log}");
            assert!(!log.contains("ready node="), "{log}");
            c.children[0] = None;
            break;
        }
        assert!(Instant::now() < deadline);
        std::thread::park_timeout(Duration::from_millis(5));
    }
}
fn startup_refusals(c: &mut Cluster) {
    let path = c.peer_credentials.as_ref().unwrap().clone();
    for generation in [1, 2] {
        select(&path, generation, &fixture("tls"));
        refused_start(c, "credential files conflict with durable reload record");
    }
    select(&path, 3, &fixture("four-node-tls"));
    refused_start(c, "peer credential generation differs from durable record");
    c.peer_credentials = None;
    refused_start(
        c,
        "existing peer credential journal requires --peer-credentials",
    );
    select(&path, 2, &fixture("four-node-tls"));
    c.peer_credentials = Some(path);
}
fn history(quic: bool, multi: bool, member: bool) {
    let mut c = setup(quic, multi, member);
    data(&c, "501", "7", "7", false);
    c.command_principal = Some(2);
    let denied = c.request(1, &["reload-peers", "1", "1", "2"]);
    assert_eq!(
        String::from_utf8(denied.stdout).unwrap(),
        "ERR AUTHORIZATION\n"
    );
    c.command_principal = Some(3);
    select(
        c.peer_credentials.as_ref().unwrap(),
        2,
        &fixture("four-node-tls"),
    );
    observe_lost_reply(&mut c);
    for id in 2..=3 {
        assert!(c
            .ok(id, &["reload-peers", "1", "1", "2"])
            .contains("queued=true"));
        wait(&c, id, "1", "recorded", 2);
    }
    data(&c, "501", "7", "7", true);
    data(&c, "502", "3", "10", false);
    assert_eq!(c.routed(&["read"]), "OK value=10\n");
    failures(&mut c);
    c.stop();
    startup_refusals(&mut c);
    for id in 1..=3 {
        c.start(id, recovery(&c));
        wait(&c, id, "1", "recorded", 2);
    }
    data(&c, "502", "3", "10", true);
    assert_eq!(c.routed(&["read"]), "OK value=10\n");
    c.stop();
    fs::remove_dir_all(&c.root).unwrap();
}
#[test]
fn tcp_peer_key_rollout_recovers_unobserved_request_and_data() {
    history(false, false, false);
}
#[test]
fn tcp_multigroup_peer_rollout_requires_all_scope_permissions() {
    history(false, true, false);
}
#[cfg(feature = "quic")]
#[test]
fn quic_peer_key_rollout_recovers_unobserved_request_and_data() {
    history(true, false, false);
}
#[cfg(feature = "quic")]
#[test]
fn quic_multigroup_peer_rollout_requires_all_scope_permissions() {
    history(true, true, false);
}

#[test]
fn peer_administration_uses_actual_local_scopes_without_requiring_group_one() {
    let mut c = groups::setup(false);
    fs::write(
        c.groups.as_ref().unwrap(),
        "voteboat-counter-groups-v1\ngroup 7 3 9 m:3 v:1 v:2 v:3\ngroup 8 2 11 m:3 v:1 v:2 v:3\n",
    )
    .unwrap();
    fs::write(c.command_access.as_ref().unwrap(), "voteboat-service-access-v1 1\n2 admin 7 3\n2 admin 8 2\n3 admin 1 1\n3 admin 7 3\n3 admin 8 2\n").unwrap();
    let path = c.root.join("peer-credentials");
    select(&path, 1, &fixture("tls"));
    c.peer_credentials = Some(path);
    for id in 1..=3 {
        c.start(id, "create");
    }
    c.command_principal = Some(2);
    wait(&c, 1, "1", "unknown", 1);
    assert!(c
        .ok(1, &["reload-peers", "1", "1", "2"])
        .contains("queued=true"));
    wait(&c, 1, "1", "failed", 1); // staged manifest still selects generation1
    c.command_principal = Some(3);
    c.stop();
    fs::remove_dir_all(&c.root).unwrap();
}

fn recovery(c: &Cluster) -> &'static str {
    if c.deployment.is_some() {
        "recover-member"
    } else {
        "recover"
    }
}
#[test]
fn tcp_member_peer_rollout_retains_wire_profile_and_durable_generation() {
    history(false, false, true);
}
#[cfg(feature = "quic")]
#[test]
fn quic_member_peer_rollout_retains_wire_profile_and_durable_generation() {
    history(true, false, true);
}
