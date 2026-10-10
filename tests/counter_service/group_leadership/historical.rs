// SPDX-License-Identifier: RPL-1.5
// Copyright (c) 2026 Karl Livesey
use super::*;

#[test]
fn completed_group_handoff_remains_historical_after_target_loss_tcp() {
    history(false);
}

#[cfg(feature = "quic")]
#[test]
fn completed_group_handoff_remains_historical_after_target_loss_quic() {
    history(true);
}

fn history(quic: bool) {
    let mut c = group_admin::setup(quic);
    c.leadership_maintenance = true;
    for node in 1..=3 {
        c.start(node, "create");
    }
    assert!(authenticated_write(&c, &["group", "7", "3", "add", "42", "5"]).contains("Value(5)"));
    assert!(authenticated_write(&c, &["group", "8", "2", "add", "42", "8"]).contains("Value(8)"));
    let source = groups::leader(&mut c, "7", "3");
    let target = source % 3 + 1;
    begin(&mut c, ("7", "3", "9"), OP, target);
    let (_, completed) = status(&mut c, ("7", "3"), OP, "phase=Completed");
    for field in [
        format!("operation={OP} "),
        format!("source={source} "),
        format!("target={target} "),
        "configuration=9 ".into(),
        "historical=true".into(),
    ] {
        assert!(completed.contains(&field), "missing {field}: {completed}");
    }

    drain::kill(&mut c, target);
    let (leader, recovered) = status(&mut c, ("7", "3"), OP, "phase=Completed");
    assert_ne!(leader, target, "the handoff target has no live process");
    assert_eq!(recovered, completed);
    let target_text = target.to_string();
    for words in [
        vec!["resume-leadership", OP],
        vec!["cancel-leadership", OP],
        vec!["move-leader", OP, "9", &target_text, &target_text, "1"],
    ] {
        let reply = command(&mut c, ("7", "3"), &words);
        assert_eq!(
            reply.trim().split(" evidence=").next(),
            completed.trim().split(" evidence=").next()
        );
    }
    status(&mut c, ("8", "2"), OP, "phase=Absent");
    assert!(
        authenticated_write(&c, &["group", "7", "3", "add", "42", "5"]).contains("duplicate=true")
    );
    assert!(authenticated_write(&c, &["group", "7", "3", "add", "43", "1"]).contains("Value(6)"));
    assert_eq!(c.routed(&["group", "8", "2", "read"]), "OK value=8\n");
    let checkpoint = quic.then(|| checkpoint(&mut c));
    c.stop();
    if let Some((node, through)) = checkpoint {
        verify_checkpoint(&c, node, through);
    }
    for node in 1..=3 {
        c.start(node, "recover");
    }
    assert_eq!(
        status(&mut c, ("7", "3"), OP, "phase=Completed").1,
        completed
    );
    status(&mut c, ("8", "2"), OP, "phase=Absent");
    assert_eq!(c.routed(&["group", "7", "3", "read"]), "OK value=6\n");
    assert_eq!(c.routed(&["group", "8", "2", "read"]), "OK value=8\n");
    c.stop();
}

fn checkpoint(c: &mut Cluster) -> (usize, u64) {
    let leader = groups::leader(c, "7", "3");
    let through = field(&c.ok(leader, &["group", "7", "3", "status"]), "committed=");
    c.ok(leader, &["group", "7", "3", "checkpoint"]);
    (leader, through)
}

fn verify_checkpoint(c: &Cluster, node: usize, through: u64) {
    use voteboat::{
        application::Counter,
        identity::*,
        log::*,
        maintenance::Maintenance,
        native::{log_store::*, snapshot_store::*},
        snapshot::*,
    };
    let _guard = fixture_gate();
    let root = c.root.join(node.to_string());
    let store = StoreIdentity {
        id: StoreId::new(node as u128).unwrap(),
        incarnation: StoreIncarnation::new(1).unwrap(),
    };
    let group = GroupIdentity {
        id: GroupId::new(7).unwrap(),
        incarnation: GroupIncarnation::new(3).unwrap(),
    };
    let log = NativeLogStore::recover(FileLogIo::open(&root).unwrap(), store, LogLimits::default())
        .unwrap();
    let mut snapshots = NativeSnapshotStore::recover(
        FileSnapshotIo::open(root.join("snapshots-7-3")).unwrap(),
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
    assert!(core.state().base_index() >= through);
}

fn field(text: &str, prefix: &str) -> u64 {
    text.split_whitespace()
        .find_map(|word| word.strip_prefix(prefix))
        .unwrap()
        .parse()
        .unwrap()
}
