// SPDX-License-Identifier: RPL-1.5
// Copyright (c) 2026 Karl Livesey
use super::*;

const SOURCE: usize = 3;
const OP: &str = "19701";
pub(super) fn setup(quic: bool) -> Cluster {
    let mut c = group_admin::setup(quic);
    c.leadership_maintenance = true;
    c.node_drain = true;
    let access = c.command_access.as_ref().unwrap();
    fs::write(
        access,
        fs::read_to_string(access)
            .unwrap()
            .replace("1 reader 7 3", "1 admin 1 1"),
    )
    .unwrap();
    fs::write(c.root.join("one.plan"), "voteboat-counter-admin-v1\nplacement 2 false\nreplica 1 1\nreplica 2 2\nreplica 3 3\njoint 7001 1 2 3 3 m:2 v:1 v:2\nfinal 7001 2 3\n").unwrap();
    fs::write(c.group_admin_plans.as_ref().unwrap(), "voteboat-counter-group-admin-v1\ngroup 1 1 one.plan\ngroup 7 3 seven.plan\ngroup 8 2 eight.plan\n").unwrap();
    for n in 1..=3 {
        c.start(n, "create");
    }
    for (g, inc, delta) in [("1", "1", "3"), ("7", "3", "5"), ("8", "2", "8")] {
        authenticated_write(&c, &["group", g, inc, "add", "42", delta]);
    }
    group_admin::configure(&mut c, "1", "1");
    group_admin::configure(&mut c, "1", "1");
    // The source is now a learner; a leader's receipt does not prove that this
    // replica has learned the final commit before the fixture stops the peers.
    configured(&c, "1", "1", "committed=Final");
    c.stop();
    for (name, config, target, original) in [
        ("seven", 9, 1, "m:3 v:1 v:2 v:3"),
        ("eight", 11, 2, "w:3 1 v:1 1 v:2 1 v:3"),
    ] {
        fs::write(c.root.join(format!("{name}.drain")), format!("voteboat-counter-drain-v1\noperation {OP}\nsource 3 3 1\nhandoff {target} {target} 1\noriginal {config} - {original}\njoint 7001 {config} {} {} 3 m:2 v:1 v:2\nfinal 7001 {} {}\n", config+1, config+2, config+1, config+2)).unwrap();
    }
    let path = c.root.join("group.drain");
    fs::write(&path, format!("voteboat-counter-group-drain-v1\noperation {OP}\nsource 3 3 1\nretained 1 1 3 3 m:2 v:1 v:2\nvoter 7 3 seven.drain\nvoter 8 2 eight.drain\n")).unwrap();
    c.group_drain_plan = Some((SOURCE, path));
    for n in 1..=3 {
        c.start(n, "recover");
    }
    let deadline = Instant::now() + Duration::from_secs(10);
    for n in 1..=3 {
        while !c.request(n, &["status"]).status.success() {
            assert!(
                c.children[n - 1]
                    .as_mut()
                    .unwrap()
                    .try_wait()
                    .unwrap()
                    .is_none(),
                "{}",
                c.service_log(n)
            );
            assert!(Instant::now() < deadline, "{}", c.service_log(n));
            std::thread::sleep(Duration::from_millis(10));
        }
    }
    configured(&c, "1", "1", "committed=Final");
    c
}
pub(super) fn status(c: &Cluster, expected: &str) -> String {
    let deadline = Instant::now() + Duration::from_secs(15);
    loop {
        let output = c.request(SOURCE, &["drain-status", "1", OP]);
        let text = String::from_utf8_lossy(&output.stdout);
        if output.status.success() && text.contains(expected) {
            return text.into_owned();
        }
        assert!(
            Instant::now() < deadline,
            "expected {expected}: {text}; {}",
            c.service_log(SOURCE)
        );
        std::thread::sleep(Duration::from_millis(10));
    }
}
fn handoff(c: &mut Cluster, group: &str, incarnation: &str, config: &str, target: usize) {
    let leader = groups::leader(c, group, incarnation);
    if leader != target {
        let n = target.to_string();
        group_leadership::command(
            c,
            (group, incarnation),
            &["move-leader", OP, config, &n, &n, "1"],
        );
    }
    let deadline = Instant::now() + Duration::from_secs(15);
    while groups::leader(c, group, incarnation) != target {
        assert!(Instant::now() < deadline);
        std::thread::sleep(Duration::from_millis(10));
    }
}
fn denied(c: &Cluster, command: &[&str]) {
    assert!(!c.request(SOURCE, command).status.success(), "{command:?}");
}
fn configured(c: &Cluster, group: &str, inc: &str, phase: &str) {
    let deadline = Instant::now() + Duration::from_secs(15);
    loop {
        let text = c.ok(
            SOURCE,
            &["group", group, inc, "configuration-status", "7001"],
        );
        if text.contains(phase) {
            return;
        }
        assert!(Instant::now() < deadline, "{text}");
        std::thread::sleep(Duration::from_millis(10));
    }
}
fn history(quic: bool) {
    let mut c = setup(quic);
    c.command_principal = Some(1); // Group1 authority alone cannot drain every assignment.
    for verb in ["drain-node", "drain-status", "cancel-drain", "drain-stop"] {
        let output = c.request(SOURCE, &[verb, "1", OP]);
        assert!(String::from_utf8_lossy(&output.stdout).contains("AUTHORIZATION"));
    }
    c.command_principal = Some(3);
    let lost = UnobservedCommand::send(&c, SOURCE, "drain-node 1 19701");
    let before = status(&c, "phase=Active");
    lost.disconnect();
    assert!(before.contains("groups=3"));
    denied(&c, &["drain-stop", "1", OP]);
    denied(&c, &["group", "7", "3", "add", "50", "1"]);
    for offset in ["0", "1", "2"] {
        assert!(
            c.ok(SOURCE, &["drain-group", "1", OP, offset])
                .contains("done=false")
                || offset == "0"
        );
    }
    denied(&c, &["drain-group", "1", OP, "3"]);
    handoff(&mut c, "7", "3", "9", 1);
    group_admin::joint(&mut c, "7", "3");
    configured(&c, "7", "3", "committed=Joint");
    denied(&c, &["drain-stop", "1", OP]);
    if quic {
        for n in 1..=3 {
            for (g, inc) in [("1", "1"), ("7", "3"), ("8", "2")] {
                c.ok(n, &["group", g, inc, "checkpoint"]);
            }
        }
        c.stop();
    } else {
        for n in 1..=3 {
            drain::kill(&mut c, n);
        }
    }
    for n in 1..=3 {
        c.start(n, "recover");
    }
    assert_eq!(status(&c, "phase=Active"), before);
    c.ok(SOURCE, &["resume-drain", "1", OP]);
    denied(&c, &["drain-stop", "1", OP]);
    configured(&c, "7", "3", "committed=Joint");
    group_admin::configure(&mut c, "7", "3");
    handoff(&mut c, "8", "2", "11", 2);
    group_admin::configure(&mut c, "8", "2");
    group_admin::configure(&mut c, "8", "2");
    status(&c, "ready=true");
    for offset in ["0", "1", "2"] {
        assert!(c
            .ok(SOURCE, &["drain-group", "1", OP, offset])
            .contains("done=true"));
    }
    c.ok(SOURCE, &["drain-stop", "1", OP]);
    drain::joined(&mut c, SOURCE);
    assert!(
        authenticated_write(&c, &["group", "7", "3", "add", "42", "5"]).contains("duplicate=true")
    );
    assert!(authenticated_write(&c, &["group", "8", "2", "add", "43", "1"]).contains("Value(9)"));
    c.stop();
}
#[test]
fn multi_group_drain_tcp_recovers_partial_progress_and_stops_only_when_ready() {
    history(false);
}
#[test]
#[cfg(feature = "quic")]
fn multi_group_drain_quic_recovers_partial_checkpoints_and_stops_only_when_ready() {
    history(true);
}

fn refused(c: &mut Cluster, expected: &str) {
    c.start(SOURCE, "recover");
    let deadline = Instant::now() + Duration::from_secs(10);
    loop {
        if let Some(code) = c.children[SOURCE - 1].as_mut().unwrap().try_wait().unwrap() {
            assert!(!code.success());
            assert!(
                c.service_log(SOURCE).contains(expected),
                "{}",
                c.service_log(SOURCE)
            );
            c.children[SOURCE - 1] = None;
            return;
        }
        assert!(Instant::now() < deadline, "{}", c.service_log(SOURCE));
        std::thread::sleep(Duration::from_millis(10));
    }
}
#[test]
fn multi_group_drain_preserves_original_plan_and_cancelled_gate_on_restart() {
    let mut c = setup(false);
    c.ok(SOURCE, &["drain-node", "1", OP]);
    status(&c, "phase=Active");
    drain::kill(&mut c, SOURCE);
    let selected = c.group_drain_plan.take().unwrap();
    refused(&mut c, "requires the original group drain plan");
    c.group_drain_plan = Some(selected);
    let path = c.root.join("seven.drain");
    let original = fs::read_to_string(&path).unwrap();
    fs::write(&path, original.replace("handoff 1 1 1", "handoff 2 2 1")).unwrap();
    refused(&mut c, "differs from original durable plan");
    fs::write(&path, original).unwrap();
    c.start(SOURCE, "recover");
    status(&c, "phase=Active");
    c.ok(SOURCE, &["cancel-drain", "1", OP]);
    status(&c, "phase=Cancelled");
    drain::kill(&mut c, SOURCE);
    c.start(SOURCE, "recover");
    status(&c, "phase=Cancelled");
    let deadline = Instant::now() + Duration::from_secs(10);
    loop {
        let output = c.request(SOURCE, &["group", "7", "3", "read"]);
        let text = String::from_utf8_lossy(&output.stdout);
        if output.status.success() || text.contains("NOT_LEADER") {
            break;
        }
        assert!(
            text.contains("Draining") || text.contains("ReadNotReady"),
            "{text}"
        );
        assert!(Instant::now() < deadline, "{text}");
        std::thread::sleep(Duration::from_millis(10));
    }
    assert_eq!(c.routed(&["group", "7", "3", "read"]), "OK value=5\n");
    c.stop();
}

#[test]
fn multi_group_drain_failed_publication_stops_service_without_receipt() {
    let mut c = setup(false);
    let directory = c.root.join(SOURCE.to_string());
    let original = fs::read(directory.join("drain.record")).unwrap();
    let staging = directory.join("drain.drain-staging");
    fs::create_dir(&staging).unwrap();
    assert!(!c.request(SOURCE, &["drain-node", "1", OP]).status.success());
    let deadline = Instant::now() + Duration::from_secs(10);
    loop {
        if let Some(code) = c.children[SOURCE - 1].as_mut().unwrap().try_wait().unwrap() {
            assert!(!code.success());
            c.children[SOURCE - 1] = None;
            break;
        }
        assert!(Instant::now() < deadline, "{}", c.service_log(SOURCE));
        std::thread::sleep(Duration::from_millis(10));
    }
    assert!(c.service_log(SOURCE).contains("uncertain: true"));
    assert_eq!(fs::read(directory.join("drain.record")).unwrap(), original);
    fs::remove_dir(staging).unwrap();
    c.start(SOURCE, "recover");
    let deadline = Instant::now() + Duration::from_secs(10);
    while !c.request(SOURCE, &["status"]).status.success() {
        assert!(Instant::now() < deadline, "{}", c.service_log(SOURCE));
        std::thread::sleep(Duration::from_millis(10));
    }
    denied(&c, &["drain-status", "1", OP]);
    c.ok(SOURCE, &["drain-node", "1", OP]);
    status(&c, "phase=Active");
    c.ok(SOURCE, &["cancel-drain", "1", OP]);
    status(&c, "phase=Cancelled");
    c.stop();
}
