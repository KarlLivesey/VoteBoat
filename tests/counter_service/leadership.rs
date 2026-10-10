// SPDX-License-Identifier: RPL-1.5
// Copyright (c) 2026 Karl Livesey
use super::*;

#[path = "leadership/cancellation.rs"]
mod cancellation;

fn cluster(quic: bool) -> Cluster {
    let mut c = Cluster::new();
    c.quic = quic;
    c.leadership_maintenance = true;
    let access = c.root.join("access.txt");
    fs::write(
        &access,
        "voteboat-service-access-v1 1\n1 reader 1 1\n2 writer 1 1\n3 admin 1 1\n",
    )
    .unwrap();
    c.command_access = Some(access);
    c.command_principal = Some(3);
    for id in 1..=3 {
        c.start(id, "create");
    }
    c.leader();
    assert!(authenticated_write(&c, &["add", "96000", "7"]).contains("Value(7)"));
    c
}
fn stop_one(c: &mut Cluster, id: usize) {
    let mut child = c.children[id - 1].take().unwrap();
    child.kill().unwrap();
    child.wait().unwrap();
}
fn begin(c: &Cluster, source: usize, target: usize, op: &str) -> String {
    c.ok(
        source,
        &[
            "move-leader",
            op,
            "1",
            &target.to_string(),
            &target.to_string(),
            "1",
        ],
    )
}
pub(super) fn status(c: &mut Cluster, op: &str, phase: &str) -> (usize, String) {
    let end = Instant::now() + Duration::from_secs(15);
    loop {
        let leader = c.leader();
        let output = c.request(leader, &["leadership-status", op]);
        let text = String::from_utf8(output.stdout).unwrap();
        if output.status.success() && text.contains(phase) {
            assert!(text.contains("evidence=quorum_read"), "{text}");
            return (leader, text);
        }
        assert!(
            Instant::now() < end,
            "status {phase}: {text}; {}",
            c.service_log(leader)
        );
        std::thread::sleep(Duration::from_millis(20));
    }
}
fn check_identity(text: &str, op: &str, source: usize, target: usize) {
    for field in [
        format!("operation={op} "),
        format!("source={source} "),
        format!("target={target} "),
        "configuration=1 ".into(),
    ] {
        assert!(text.contains(&field), "missing {field}: {text}");
    }
}
#[test]
fn authenticated_handoff_and_pending_restart_tcp() {
    history(false);
}
#[cfg(feature = "quic")]
#[test]
fn authenticated_handoff_and_pending_checkpoint_restart_quic() {
    history(true);
}
fn history(quic: bool) {
    let mut c = cluster(quic);
    let source = c.leader();
    let target = source % 3 + 1;
    // No target process can campaign until the original Pending record recovers.
    stop_one(&mut c, target);
    let op = "340282366920938463463374607431768211455";
    let first = begin(&c, source, target, op);
    check_identity(&first, op, source, target);
    assert!(first.contains("phase=Pending"), "{first}");
    let (_, pending) = status(&mut c, op, "phase=Pending");
    assert!(pending.contains("intent_index="));
    if quic {
        for id in 1..=3 {
            if c.children[id - 1].is_some() {
                c.ok(id, &["checkpoint"]);
            }
        }
        // Observe a published snapshot before process death.
        let end = Instant::now() + Duration::from_secs(10);
        while !(1..=3)
            .filter(|id| c.children[*id - 1].is_some())
            .all(|id| {
                c.ok(id, &["maintenance"])
                    .split("checkpoint_base=")
                    .nth(1)
                    .unwrap()
                    .trim()
                    .parse::<u64>()
                    .unwrap()
                    > 0
            })
        {
            assert!(Instant::now() < end);
            std::thread::sleep(Duration::from_millis(10));
        }
    }
    for id in 1..=3 {
        if c.children[id - 1].is_some() {
            stop_one(&mut c, id);
        }
    }
    for id in 1..=3 {
        c.start(id, "recover");
    }
    let (leader, completed) = status(&mut c, op, "phase=Completed");
    check_identity(&completed, op, source, target);
    assert!(completed.contains("historical=true"));
    let retry = begin(&c, leader, target, op);
    assert_eq!(retry.trim(), completed.split(" evidence=").next().unwrap());
    assert!(c
        .ok(leader, &["resume-leadership", op])
        .contains("phase=Completed"));
    assert!(authenticated_write(&c, &["add", "96000", "7"]).contains("duplicate=true"));
    assert!(authenticated_write(&c, &["add", "96001", "3"]).contains("Value(10)"));
    assert_eq!(c.routed(&["read"]), "OK value=10\n");
    for id in 1..=3 {
        c.ok(id, &["checkpoint"]);
    }
    c.stop();
    for id in 1..=3 {
        c.start(id, "recover");
    }
    let (_, recovered) = status(&mut c, op, "phase=Completed");
    assert_eq!(recovered, completed);
    c.stop();
    fs::remove_dir_all(&c.root).unwrap();
}

#[test]
fn authenticated_cancel_deadline_and_permission_boundaries() {
    let mut c = cluster(false);
    let source = c.leader();
    let target = source % 3 + 1;
    for principal in [1, 2] {
        c.command_principal = Some(principal);
        for command in [
            vec!["move-leader", "96100", "1", "2", "2", "1"],
            vec!["resume-leadership", "96100"],
            vec!["cancel-leadership", "96100"],
        ] {
            let output = c.request(source, &command);
            assert_eq!(
                String::from_utf8(output.stdout).unwrap(),
                "ERR AUTHORIZATION\n"
            );
        }
        assert!(c
            .ok(source, &["leadership-status", "96100"])
            .contains("phase=Absent"));
    }
    c.command_principal = Some(3);
    stop_one(&mut c, target);
    assert!(begin(&c, source, target, "96100").contains("phase=Pending"));
    let end = Instant::now() + Duration::from_secs(10);
    while !c.service_log(source).contains("local attempt expired") {
        assert!(
            Instant::now() < end,
            "attempt did not expire: {}",
            c.service_log(source)
        );
        std::thread::sleep(Duration::from_millis(10));
    }
    assert!(authenticated_write(&c, &["add", "96101", "1"]).contains("Value(8)"));
    assert!(c
        .ok(source, &["resume-leadership", "96100"])
        .contains("phase=Pending"));
    let cancelled = c.ok(source, &["cancel-leadership", "96100"]);
    assert!(cancelled.contains("phase=Cancelled"), "{cancelled}");
    assert_eq!(
        c.ok(source, &["cancel-leadership", "96100"])
            .split(" evidence=")
            .next()
            .unwrap(),
        cancelled.trim()
    );
    assert!(begin(&c, source, target, "96102").contains("phase=Pending"));
    // Retrying an old terminal cancellation cannot cancel the new handoff.
    assert_eq!(
        c.ok(source, &["cancel-leadership", "96100"])
            .split(" evidence=")
            .next()
            .unwrap(),
        cancelled.trim()
    );
    c.start(target, "recover");
    status(&mut c, "96102", "phase=Completed");
    let (_, status) = status(&mut c, "96100", "phase=Cancelled");
    check_identity(&status, "96100", source, target);
    c.stop();
    fs::remove_dir_all(&c.root).unwrap();
}

#[test]
fn lost_begin_reply_retains_exact_intent_and_data() {
    let mut c = cluster(false);
    let source = c.leader();
    let target = source % 3 + 1;
    let command = format!("move-leader 96200 1 {target} {target} 1");
    let lost = UnobservedCommand::send(&c, source, &command);
    // Never consume the application reply. Closing the socket releases the wait;
    // accepted work may still commit, so the exact original command is retried.
    lost.disconnect();
    let end = Instant::now() + Duration::from_secs(15);
    loop {
        let leader = c.leader();
        let reply = c.request(
            leader,
            &[
                "move-leader",
                "96200",
                "1",
                &target.to_string(),
                &target.to_string(),
                "1",
            ],
        );
        if reply.status.success() {
            break;
        }
        let text = String::from_utf8(reply.stdout).unwrap();
        assert!(Instant::now() < end, "retry: {text}");
    }
    let (_, completed) = status(&mut c, "96200", "phase=Completed");
    check_identity(&completed, "96200", source, target);
    assert!(authenticated_write(&c, &["add", "96000", "7"]).contains("duplicate=true"));
    c.stop();
    fs::remove_dir_all(&c.root).unwrap();
}

#[test]
fn disconnected_and_expired_status_waits_release_original_tickets() {
    let mut c = cluster(false);
    let source = c.leader();
    for id in 1..=3 {
        if id != source {
            stop_one(&mut c, id);
        }
    }
    let mut closed = UnobservedCommand::send(&c, source, "leadership-status 96300");
    wait_event(&c, source, "leadership status admitted operation=96300");
    closed.close_notify();
    wait_event(&c, source, "reason=channel");
    closed.disconnect();
    let expired = UnobservedCommand::send(&c, source, "leadership-status 96301");
    wait_event(&c, source, "leadership status admitted operation=96301");
    wait_event(&c, source, "reason=deadline");
    expired.disconnect();
    // The same single-connection owner remains usable after each lost wait.
    assert!(c.ok(source, &["metrics"]).contains("failed_polls=0"));
    for id in 1..=3 {
        if id != source {
            c.start(id, "recover");
        }
    }
    status(&mut c, "96300", "phase=Absent");
    c.stop();
    fs::remove_dir_all(&c.root).unwrap();
}

#[test]
fn maintenance_profile_refuses_existing_plain_counter_data() {
    let mut c = Cluster::new();
    for id in 1..=3 {
        c.start(id, "create");
    }
    // Reopen the replica that applied the acknowledged command. A follower
    // stopped immediately after the reply may not have learned that commit.
    let node = c.leader();
    assert!(c.ok(node, &["add", "96310", "7"]).contains("Value(7)"));
    c.stop();
    c.leadership_maintenance = true;
    let access = c.root.join("access.txt");
    fs::write(&access, "voteboat-service-access-v1 1\n3 admin 1 1\n").unwrap();
    c.command_access = Some(access);
    c.command_principal = Some(3);
    c.start(node, "recover");
    let end = Instant::now() + Duration::from_secs(10);
    loop {
        if let Some(code) = c.children[node - 1].as_mut().unwrap().try_wait().unwrap() {
            assert!(!code.success());
            assert!(
                c.service_log(node).contains("InvalidCommand"),
                "{}",
                c.service_log(node)
            );
            c.children[node - 1] = None;
            break;
        }
        assert!(Instant::now() < end, "incompatible profile was not refused");
        std::thread::sleep(Duration::from_millis(10));
    }
    c.leadership_maintenance = false;
    c.command_access = None;
    c.command_principal = None;
    for id in 1..=3 {
        c.start(id, "recover");
    }
    c.leader();
    assert_eq!(c.routed(&["read"]), "OK value=7\n");
    c.stop();
    fs::remove_dir_all(&c.root).unwrap();
}

fn wait_event(c: &Cluster, id: usize, event: &str) {
    let end = Instant::now() + Duration::from_secs(12);
    loop {
        let text = c.service_log(id);
        if text.contains(event) {
            return;
        }
        assert!(Instant::now() < end, "missing {event}: {text}");
        std::thread::sleep(Duration::from_millis(5));
    }
}
