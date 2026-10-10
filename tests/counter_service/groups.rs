// SPDX-License-Identifier: RPL-1.5
// Copyright (c) 2026 Karl Livesey
use super::*;

const GROUPS: &str = "voteboat-counter-groups-v1\ngroup 1 1 1 m:3 v:1 v:2 v:3\ngroup 7 3 9 m:3 v:1 v:2 v:3\ngroup 8 2 11 w:3 1 v:1 1 v:2 1 v:3\n";
pub(super) fn setup(quic: bool) -> Cluster {
    let mut c = Cluster::new();
    c.quic = quic;
    let groups = c.root.join("groups.txt");
    fs::write(&groups, GROUPS).unwrap();
    c.groups = Some(groups);
    let access = c.root.join("access.txt");
    fs::write(&access, "voteboat-service-access-v1 1\n1 reader 7 3\n2 writer 7 3\n3 admin 1 1\n3 admin 7 3\n3 admin 8 2\n3 admin 99 1\n").unwrap();
    c.command_access = Some(access);
    c.command_principal = Some(3);
    c
}
pub(super) fn leader(c: &mut Cluster, group: &str, incarnation: &str) -> usize {
    let end = Instant::now() + Duration::from_secs(20);
    loop {
        for id in 1..=3 {
            let output = c.request(id, &["group", group, incarnation, "status"]);
            if output.status.success()
                && String::from_utf8_lossy(&output.stdout).contains("role=Leader")
            {
                return id;
            }
            assert!(
                c.children[id - 1]
                    .as_mut()
                    .unwrap()
                    .try_wait()
                    .unwrap()
                    .is_none(),
                "{}",
                c.service_log(id)
            );
        }
        assert!(Instant::now() < end, "group {group}: {}", c.service_log(1));
        std::thread::sleep(Duration::from_millis(10));
    }
}
fn data(c: &mut Cluster, duplicate: bool) {
    for (group, incarnation, value) in [("1", "1", "3"), ("7", "3", "5"), ("8", "2", "9")] {
        let text = c.routed(&["group", group, incarnation, "add", "42", value]);
        assert!(text.contains(&format!("Value({value})")), "{text}");
        assert!(text.contains(&format!("duplicate={duplicate}")), "{text}");
        assert_eq!(
            c.routed(&["group", group, incarnation, "read"]),
            format!("OK value={value}\n")
        );
        let leader = leader(c, group, incarnation);
        assert!(c
            .ok(leader, &["group", group, incarnation, "checkpoint"])
            .contains("checkpoint_admitted"));
    }
}
fn authorization(c: &mut Cluster) {
    let n = leader(c, "7", "3");
    c.command_principal = Some(1);
    assert_eq!(c.ok(n, &["group", "7", "3", "read"]), "OK value=5\n");
    for args in [
        vec!["group", "7", "3", "add", "44", "1"],
        vec!["group", "1", "1", "read"],
        vec!["group", "7", "4", "read"],
    ] {
        let output = c.request(n, &args);
        assert!(!output.status.success());
        assert!(String::from_utf8_lossy(&output.stdout).contains("AUTHORIZATION"));
    }
    c.command_principal = Some(2);
    assert!(c
        .routed(&["group", "7", "3", "add", "43", "0"])
        .contains("Value(5)"));
    c.command_principal = Some(3);
    let missing = c.request(n, &["group", "99", "1", "status"]);
    assert!(!missing.status.success());
    assert!(String::from_utf8_lossy(&missing.stdout).contains("unknown group or incarnation"));
    let shutdown = c.request(n, &["group", "7", "3", "quit"]);
    assert!(!shutdown.status.success());
    assert!(c.children[n - 1]
        .as_mut()
        .unwrap()
        .try_wait()
        .unwrap()
        .is_none());
}
fn refused(c: &mut Cluster, expected: &str) {
    c.start(1, "recover");
    let start = Instant::now();
    loop {
        if let Some(status) = c.children[0].as_mut().unwrap().try_wait().unwrap() {
            assert!(!status.success());
            assert!(c.service_log(1).contains(expected), "{}", c.service_log(1));
            c.children[0] = None;
            return;
        }
        assert!(
            !c.service_log(1).contains("ready node="),
            "unexpected startup: {}",
            c.service_log(1)
        );
        assert!(start.elapsed() < Duration::from_secs(10));
        std::thread::sleep(Duration::from_millis(10));
    }
}
fn history(quic: bool) {
    let mut c = setup(quic);
    for n in 1..=3 {
        c.start(n, "create");
    }
    data(&mut c, false);
    authorization(&mut c);
    let lost = leader(&mut c, "7", "3");
    let mut child = c.children[lost - 1].take().unwrap();
    child.kill().unwrap();
    child.wait().unwrap();
    assert!(c
        .routed(&["group", "7", "3", "add", "45", "0"])
        .contains("Value(5)"));
    c.start(lost, "recover");
    leader(&mut c, "7", "3");
    c.stop();
    let path = c.groups.take().unwrap();
    refused(&mut c, "durable group inventory differs");
    c.groups = Some(path);
    fs::write(
        c.groups.as_ref().unwrap(),
        GROUPS.lines().take(3).collect::<Vec<_>>().join("\n"),
    )
    .unwrap();
    refused(&mut c, "durable group inventory differs");
    fs::write(
        c.groups.as_ref().unwrap(),
        GROUPS.replace("group 7 3 9", "group 7 3 10"),
    )
    .unwrap();
    refused(&mut c, "original bootstrap differs");
    fs::write(c.groups.as_ref().unwrap(), GROUPS).unwrap();
    for n in 1..=3 {
        c.start(n, "recover");
    }
    data(&mut c, true);
    c.stop();
}
#[test]
fn authenticated_group_commands_tcp_recover_independent_histories() {
    history(false);
}
#[test]
#[cfg(feature = "quic")]
fn authenticated_group_commands_quic_recover_independent_histories() {
    history(true);
}

#[test]
fn malformed_group_manifests_and_incompatible_profiles_create_no_store() {
    let mut c = setup(false);
    let mut cases = [
        "bad",
        "voteboat-counter-groups-v1\n",
        "voteboat-counter-groups-v1\ngroup 7 3 9 m:1 v:4\n",
        "voteboat-counter-groups-v1\ngroup 1 1 1 m:3 v:1 v:2 v:3\ngroup 1 2 2 m:3 v:1 v:2 v:3\n",
    ]
    .map(str::to_owned)
    .to_vec();
    cases.push("x".repeat(65537));
    cases.push(format!(
        "voteboat-counter-groups-v1\n{}",
        (1..=257)
            .map(|n| format!("group {n} 1 1 m:3 v:1 v:2 v:3\n"))
            .collect::<String>()
    ));
    for text in cases {
        fs::write(c.groups.as_ref().unwrap(), text).unwrap();
        refused(&mut c, "Error:");
        assert!(!c.root.join("1").exists());
    }
    fs::write(c.groups.as_ref().unwrap(), GROUPS).unwrap();
    c.leadership_maintenance = true;
    refused(&mut c, "multi-group administration");
    assert!(!c.root.join("1").exists());
}

#[test]
fn group_routing_preserves_exact_request_and_stops_after_uncertainty() {
    for reply in [
        Some(b"OK outcome=Value(7) duplicate=false\n".as_slice()),
        None,
        Some(b"UNKNOWN lost reply\n".as_slice()),
    ] {
        let mut c = Cluster::new();
        let first = reply_peer(c.take_listener(101), Some(b"ERR NOT_LEADER\n"));
        let second = reply_peer(c.take_listener(102), reply);
        let third = c.take_listener(103);
        third.set_nonblocking(true).unwrap();
        let output = c.target("auto", &["group", "7", "3", "add", "42", "7"]);
        assert_eq!(
            output.status.success(),
            reply.is_some_and(|r| r.starts_with(b"OK"))
        );
        assert_eq!(first.join().unwrap(), b"group 7 3 add 42 7\n");
        assert_eq!(second.join().unwrap(), b"group 7 3 add 42 7\n");
        if !output.status.success() {
            assert!(String::from_utf8_lossy(&output.stdout).starts_with("UNKNOWN"));
        }
        assert_eq!(
            third.accept().unwrap_err().kind(),
            std::io::ErrorKind::WouldBlock
        );
    }
}

#[test]
fn invalid_group_commands_are_rejected_before_connecting() {
    let mut c = Cluster::new();
    let listener = c.take_listener(101);
    listener.set_nonblocking(true).unwrap();
    for command in [
        vec!["group"],
        vec!["group", "0", "1", "read"],
        vec!["group", "7", "0", "read"],
        vec!["group", "7", "3", "quit"],
        vec!["group", "7", "3", "group", "1", "1", "read"],
    ] {
        assert!(!c.request(1, &command).status.success());
        assert_eq!(
            listener.accept().unwrap_err().kind(),
            std::io::ErrorKind::WouldBlock
        );
    }
}
