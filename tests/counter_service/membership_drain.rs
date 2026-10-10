// SPDX-License-Identifier: RPL-1.5
// Copyright (c) 2026 Karl Livesey
use super::*;

fn prepare(quic: bool) -> (Cluster, usize, usize) {
    let mut c = drain::cluster(quic);
    let source = c.leader();
    let target = source % 3 + 1;
    let voters = (1..=3)
        .filter(|n| *n != source)
        .map(|n| format!("v:{n}"))
        .collect::<Vec<_>>()
        .join(" ");
    let records = format!("joint 19751 1 2 3 {source} m:2 {voters}\nfinal 19751 2 3\n");
    let admin = c.root.join("membership.admin");
    fs::write(&admin, format!("voteboat-counter-admin-v1\nplacement 2 false\nreplica 1 1 1 1\nreplica 2 2 2 1\nreplica 3 3 3 1\n{records}")).unwrap();
    let plan = c.root.join("membership.drain");
    fs::write(&plan, format!("voteboat-counter-drain-v1\noperation 19701\nsource {source} {source} 1\nhandoff {target} {target} 1\noriginal 1 - m:3 v:1 v:2 v:3\n{records}")).unwrap();
    c.stop();
    c.admin_plan = Some(admin);
    c.remote_admin = true;
    c.membership_drain = Some((source, plan));
    reopen(&mut c);
    let leader = c.leader();
    if leader != source {
        c.ok(
            leader,
            &[
                "move-leader",
                "19750",
                "1",
                &source.to_string(),
                &source.to_string(),
                "1",
            ],
        );
    }
    let end = Instant::now() + Duration::from_secs(15);
    while c.leader() != source {
        assert!(Instant::now() < end, "could not prepare source leader");
        std::thread::sleep(Duration::from_millis(20));
    }
    (c, source, target)
}
fn reopen(c: &mut Cluster) {
    for id in 1..=3 {
        c.start(id, "recover-member");
    }
}
fn configuration(c: &Cluster, id: usize, expected: &str) {
    let end = Instant::now() + Duration::from_secs(15);
    loop {
        let text = c.wait_configuration_status(id, "19751");
        if text.contains(expected) {
            return;
        }
        assert!(Instant::now() < end, "expected {expected}: {text}");
        std::thread::sleep(Duration::from_millis(20));
    }
}
fn configure(c: &mut Cluster) {
    let leader = c.leader();
    let text = c.ok(leader, &["configure", "19751"]);
    assert!(text.contains("operation=19751"), "{text}");
}
fn checkpoint(c: &Cluster) {
    for id in 1..=3 {
        c.ok(id, &["checkpoint"]);
        drain::wait_manual_checkpoint(c, id);
    }
}
fn history(quic: bool) {
    let (mut c, source, _) = prepare(quic);
    let unread = UnobservedCommand::send(&c, source, "drain-node 1 19701");
    let reply = drain::wait_status(&c, source, "phase=Active");
    unread.disconnect();
    assert!(reply.contains("membership_change=true"), "{reply}");
    assert!(!c
        .request(source, &["drain-stop", "1", "19701"])
        .status
        .success());
    assert!(!c
        .request(source, &["drain-node", "1", "999"])
        .status
        .success());
    // Administrative configuration needs an active authenticated admin session.
    c.command_principal = Some(1);
    let leader = c.leader();
    assert!(!c.request(leader, &["configure", "19751"]).status.success());
    c.command_principal = Some(3);
    let leader = c.leader();
    let unread = UnobservedCommand::send(&c, leader, "configure 19751");
    for id in 1..=3 {
        configuration(&c, id, "committed=Joint");
    }
    unread.disconnect();
    assert!(!c
        .request(source, &["drain-stop", "1", "19701"])
        .status
        .success());
    if quic {
        checkpoint(&c);
    }
    c.stop();
    reopen(&mut c);
    drain::wait_status(&c, source, "phase=Active");
    assert!(c
        .ok(source, &["resume-drain", "1", "19701"])
        .contains("ready=false"));
    configure(&mut c);
    for id in 1..=3 {
        configuration(&c, id, "action=completed");
    }
    drain::wait_status(&c, source, "ready=true");
    if quic {
        checkpoint(&c);
    }
    c.stop();
    reopen(&mut c);
    drain::wait_status(&c, source, "ready=true");
    assert!(c
        .ok(source, &["drain-node", "1", "19701"])
        .contains("ready=true"));
    assert!(!c.request(source, &["add", "19752", "3"]).status.success());
    c.ok(source, &["drain-stop", "1", "19701"]);
    drain::joined(&mut c, source);
    assert!(authenticated_write(&c, &["add", "19752", "3"]).contains("Value(10)"));
    assert!(authenticated_write(&c, &["add", "19700", "7"]).contains("duplicate=true"));
    c.stop();
}
#[test]
fn executable_membership_drain_resumes_joint_and_final_tcp() {
    history(false);
}
#[cfg(feature = "quic")]
#[test]
fn executable_membership_drain_resumes_joint_and_final_quic_checkpoint() {
    history(true);
}

fn refused(c: &mut Cluster, source: usize, expected: &str) {
    c.start(source, "recover-member");
    let end = Instant::now() + Duration::from_secs(10);
    loop {
        if let Some(status) = c.children[source - 1].as_mut().unwrap().try_wait().unwrap() {
            assert!(!status.success());
            assert!(
                c.service_log(source).contains(expected),
                "{}",
                c.service_log(source)
            );
            c.children[source - 1] = None;
            return;
        }
        assert!(
            Instant::now() < end,
            "expected startup refusal: {}",
            c.service_log(source)
        );
        std::thread::sleep(Duration::from_millis(10));
    }
}
#[test]
fn membership_drain_recovery_refuses_changed_or_omitted_original_plan() {
    let (mut c, source, target) = prepare(false);
    c.ok(source, &["drain-node", "1", "19701"]);
    drain::kill(&mut c, source);
    let (_, path) = c.membership_drain.clone().unwrap();
    let original = fs::read_to_string(&path).unwrap();
    let other = (1..=3).find(|id| *id != source && *id != target).unwrap();
    fs::write(
        &path,
        original.replace(
            &format!("handoff {target} {target} 1"),
            &format!("handoff {other} {other} 1"),
        ),
    )
    .unwrap();
    refused(
        &mut c,
        source,
        "drain plan differs from original durable intent",
    );
    fs::write(&path, original).unwrap();
    c.membership_drain = None;
    refused(
        &mut c,
        source,
        "membership drain journal requires its original host plan",
    );
    c.membership_drain = Some((source, path));
    c.start(source, "recover-member");
    drain::wait_status(&c, source, "phase=Active");
    c.ok(source, &["cancel-drain", "1", "19701"]);
    drain::wait_status(&c, source, "resuming=false");
    c.stop();
}

fn store_files(path: &std::path::Path) -> BTreeMap<PathBuf, Vec<u8>> {
    fs::read_dir(path)
        .unwrap()
        .map(|entry| entry.unwrap().path())
        .filter(|path| path.is_file())
        .map(|path| (path.clone(), fs::read(path).unwrap()))
        .collect()
}
#[test]
fn membership_drain_rejects_invalid_plan_files_before_opening_store() {
    let (mut c, source, _) = prepare(false);
    c.stop();
    let (_, path) = c.membership_drain.clone().unwrap();
    let original = fs::read_to_string(&path).unwrap();
    let before = store_files(&c.root.join(source.to_string()));
    for (bad, expected) in [
        ("x".repeat(65537), "drain plan exceeds64KiB"),
        (
            original.replace("voteboat-counter-drain-v1", "wrong-header"),
            "expected voteboat-counter-drain-v1",
        ),
        (
            original.replace("operation 19701", "operation 0"),
            "invalid drain operation",
        ),
        (
            original.replace(
                &format!("source {source} {source} 1"),
                &format!("source {source} 999 1"),
            ),
            "drain store differs from deployment",
        ),
        (
            original.replace("joint 19751", "joint 19752"),
            "exactly match the provisioned administration plan",
        ),
        (
            format!("{original}extra\n"),
            "exactly match the provisioned administration plan",
        ),
    ] {
        fs::write(&path, bad).unwrap();
        refused(&mut c, source, expected);
        assert_eq!(store_files(&c.root.join(source.to_string())), before);
    }
    fs::write(&path, original).unwrap();
    c.remote_admin = false;
    refused(&mut c, source, "explicitly requested --remote-admin-plan");
    assert_eq!(store_files(&c.root.join(source.to_string())), before);
}
