// SPDX-License-Identifier: RPL-1.5
// Copyright (c) 2026 Karl Livesey
use super::*;

const OP: &str = "40001";
const NEXT: &str = "40002";
fn status(c: &mut Cluster, scope: (&str, &str), op: &str, phase: &str) -> (usize, String) {
    let deadline = Instant::now() + Duration::from_secs(20);
    loop {
        let leader = groups::leader(c, scope.0, scope.1);
        let output = c.request(
            leader,
            &["group", scope.0, scope.1, "leadership-status", op],
        );
        let text = String::from_utf8(output.stdout).unwrap();
        if output.status.success() && text.contains(phase) {
            assert!(text.contains("evidence=quorum_read"), "{text}");
            return (leader, text);
        }
        assert!(
            Instant::now() < deadline,
            "{text}; {}",
            c.service_log(leader)
        );
        std::thread::sleep(Duration::from_millis(10));
    }
}
fn begin(c: &mut Cluster, scope: (&str, &str, &str), op: &str, target: usize) -> usize {
    let leader = groups::leader(c, scope.0, scope.1);
    let target = target.to_string();
    let output = c.ok(
        leader,
        &[
            "group",
            scope.0,
            scope.1,
            "move-leader",
            op,
            scope.2,
            &target,
            &target,
            "1",
        ],
    );
    assert!(output.contains("phase=Pending"), "{output}");
    leader
}
fn permissions(c: &mut Cluster, leader: usize) {
    c.command_principal = Some(2); // Administrator for group7 only.
    for words in [
        vec!["group", "8", "2", "cancel-leadership", OP],
        vec!["group", "7", "4", "resume-leadership", OP],
    ] {
        let output = c.request(leader, &words);
        assert!(!output.status.success());
        assert!(String::from_utf8_lossy(&output.stdout).contains("AUTHORIZATION"));
    }
    c.command_principal = Some(3);
    let invalid = c.request(
        leader,
        &["group", "7", "3", "move-leader", OP, "9", "2", "999", "1"],
    );
    assert!(!invalid.status.success());
}
fn durable_pending(c: &Cluster, missing: usize, checkpoint: bool) {
    use voteboat::{
        application::Counter,
        identity::*,
        log::*,
        maintenance::*,
        native::{log_store::*, snapshot_store::*},
        snapshot::*,
    };
    let _guard = fixture_gate();
    for node in (1..=3).filter(|node| *node != missing) {
        let root = c.root.join(node.to_string());
        let store = StoreIdentity {
            id: StoreId::new(node as u128).unwrap(),
            incarnation: StoreIncarnation::new(1).unwrap(),
        };
        let log =
            NativeLogStore::recover(FileLogIo::open(&root).unwrap(), store, LogLimits::default())
                .unwrap();
        for (id, incarnation, op) in [(7, 3, NEXT), (8, 2, OP)] {
            let group = GroupIdentity {
                id: GroupId::new(id).unwrap(),
                incarnation: GroupIncarnation::new(incarnation).unwrap(),
            };
            let mut snapshots = NativeSnapshotStore::recover(
                FileSnapshotIo::open(root.join(format!("snapshots-{id}-{incarnation}"))).unwrap(),
                SnapshotIdentity { group, store },
                SnapshotLimits::default(),
            )
            .unwrap();
            let mut app = Maintenance::new(group, 2, 64, Counter::new(10000).unwrap()).unwrap();
            let (core, _) = recover_member_replica(
                NodeId::new(node as u64).unwrap(),
                group,
                &log,
                &mut snapshots,
                &mut app,
            )
            .unwrap();
            let record = app
                .record(OperationId::new(op.parse().unwrap()).unwrap())
                .unwrap();
            assert_eq!(record.phase, LeadershipPhase::Pending);
            if checkpoint {
                assert!(core.state().base_index() >= record.index);
            } else {
                assert!(core.state().entry_at(record.index).is_some());
            }
        }
    }
}
fn await_commits(c: &mut Cluster, missing: usize) {
    for (group, incarnation) in [("7", "3"), ("8", "2")] {
        let leader = groups::leader(c, group, incarnation);
        let committed = |node| {
            c.ok(node, &["group", group, incarnation, "status"])
                .split("committed=")
                .nth(1)
                .unwrap()
                .trim()
                .parse::<u64>()
                .unwrap()
        };
        let through = committed(leader);
        let deadline = Instant::now() + Duration::from_secs(10);
        while !(1..=3)
            .filter(|node| *node != missing)
            .all(|node| committed(node) >= through)
        {
            assert!(
                Instant::now() < deadline,
                "group {group} commit did not reach both surviving replicas"
            );
            std::thread::sleep(Duration::from_millis(5));
        }
    }
}
fn checkpoint_and_stop(c: &mut Cluster, missing: usize, checkpoint: bool) {
    await_commits(c, missing);
    if checkpoint {
        for node in (1..=3).filter(|node| *node != missing) {
            for (group, incarnation) in [("7", "3"), ("8", "2")] {
                c.ok(node, &["group", group, incarnation, "checkpoint"]);
            }
        }
        c.stop();
    } else {
        for node in (1..=3).filter(|node| *node != missing) {
            drain::kill(c, node);
        }
    }
    durable_pending(c, missing, checkpoint);
}
fn history(quic: bool) {
    let mut c = group_admin::setup(quic);
    c.leadership_maintenance = true;
    for node in 1..=3 {
        c.start(node, "create");
    }
    assert!(c
        .routed(&["group", "7", "3", "add", "42", "5"])
        .contains("Value(5)"));
    assert!(c
        .routed(&["group", "8", "2", "add", "42", "8"])
        .contains("Value(8)"));
    let leader = groups::leader(&mut c, "7", "3");
    permissions(&mut c, leader);
    let target = leader % 3 + 1;
    drain::kill(&mut c, target);
    let source = begin(&mut c, ("7", "3", "9"), OP, target);
    begin(&mut c, ("8", "2", "11"), OP, target);
    status(&mut c, ("8", "2"), OP, "phase=Pending");
    assert!(c
        .ok(source, &["group", "7", "3", "cancel-leadership", OP])
        .contains("phase=Cancelled"));
    status(&mut c, ("8", "2"), OP, "phase=Pending");
    begin(&mut c, ("7", "3", "9"), NEXT, target);
    checkpoint_and_stop(&mut c, target, quic);
    for node in 1..=3 {
        c.start(node, "recover");
    }
    let mut completed = Vec::new();
    for (group, incarnation, config, op) in [("7", "3", "9", NEXT), ("8", "2", "11", OP)] {
        let (leader, text) = status(&mut c, (group, incarnation), op, "phase=Completed");
        assert_eq!(leader, target);
        assert!(text.contains(&format!("target={target} ")));
        assert!(c
            .ok(
                leader,
                &["group", group, incarnation, "resume-leadership", op]
            )
            .contains("phase=Completed"));
        let target = target.to_string();
        assert!(c
            .ok(
                leader,
                &[
                    "group",
                    group,
                    incarnation,
                    "move-leader",
                    op,
                    config,
                    &target,
                    &target,
                    "1"
                ]
            )
            .contains("phase=Completed"));
        completed.push(text);
    }
    status(&mut c, ("7", "3"), OP, "phase=Cancelled");
    status(&mut c, ("1", "1"), OP, "phase=Absent");
    assert!(c
        .routed(&["group", "7", "3", "add", "42", "5"])
        .contains("duplicate=true"));
    assert_eq!(c.routed(&["group", "8", "2", "read"]), "OK value=8\n");
    group_admin::configure(&mut c, "7", "3");
    group_admin::configure(&mut c, "7", "3");
    assert!(c
        .routed(&["group", "7", "3", "add", "43", "1"])
        .contains("Value(6)"));
    c.stop();
    for node in 1..=3 {
        c.start(node, "recover");
    }
    for ((group, incarnation, op), expected) in [("7", "3", NEXT), ("8", "2", OP)]
        .into_iter()
        .zip(completed)
    {
        assert_eq!(
            status(&mut c, (group, incarnation), op, "phase=Completed").1,
            expected
        );
    }
    c.stop();
}
#[test]
fn group_leadership_tcp_recovers_independent_pending_and_cancelled_intents() {
    history(false);
}
#[test]
#[cfg(feature = "quic")]
fn group_leadership_quic_recovers_independent_pending_and_cancelled_checkpoints() {
    history(true);
}

#[test]
fn group_maintenance_schema_cannot_reinterpret_existing_counter_checkpoints() {
    for original in [false, true] {
        let mut c = groups::setup(false);
        c.leadership_maintenance = original;
        for node in 1..=3 {
            c.start(node, "create");
        }
        assert!(c
            .routed(&["group", "7", "3", "add", "42", "5"])
            .contains("Value(5)"));
        let leader = groups::leader(&mut c, "7", "3");
        c.ok(leader, &["group", "7", "3", "checkpoint"]);
        c.stop();
        c.leadership_maintenance = !original;
        c.start(leader, "recover");
        let deadline = Instant::now() + Duration::from_secs(10);
        loop {
            if let Some(code) = c.children[leader - 1].as_mut().unwrap().try_wait().unwrap() {
                assert!(!code.success());
                let log = c.service_log(leader);
                assert!(!log.contains("ready node="), "{log}");
                assert!(log.contains("Error:"), "{log}");
                c.children[leader - 1] = None;
                break;
            }
            assert!(Instant::now() < deadline, "{}", c.service_log(leader));
            std::thread::sleep(Duration::from_millis(10));
        }
        c.leadership_maintenance = original;
        for node in 1..=3 {
            c.start(node, "recover");
        }
        assert_eq!(c.routed(&["group", "7", "3", "read"]), "OK value=5\n");
        c.stop();
    }
}
