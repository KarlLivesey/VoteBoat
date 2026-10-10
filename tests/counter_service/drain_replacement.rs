// SPDX-License-Identifier: RPL-1.5
// Copyright (c) 2026 Karl Livesey
use super::*;
use voteboat::{identity::*, log::*, native::log_store::*};

#[path = "drain_replacement/setup_recovery.rs"]
mod setup_recovery;

fn stored(c: &Cluster, id: usize) -> GroupLog {
    let _gate = fixture_gate();
    NativeLogStore::recover(
        FileLogIo::open(c.root.join(id.to_string())).unwrap(),
        store(id),
        LogLimits::default(),
    )
    .unwrap()
    .state(GroupIdentity {
        id: GroupId::new(1).unwrap(),
        incarnation: GroupIncarnation::new(1).unwrap(),
    })
    .unwrap()
}
fn store(id: usize) -> StoreIdentity {
    StoreIdentity {
        id: StoreId::new(if id == 4 { 404 } else { id as u128 }).unwrap(),
        incarnation: StoreIncarnation::new(if id == 4 { 7 } else { 1 }).unwrap(),
    }
}
fn configure(c: &mut Cluster, operation: &str) {
    let leader = c.leader();
    c.ok(leader, &["configure", operation]);
    let until = Instant::now() + Duration::from_secs(15);
    loop {
        if (1..=3).all(|id| {
            c.wait_configuration_status(id, operation)
                .contains("action=completed")
        }) {
            return;
        }
        assert!(Instant::now() < until, "configuration {operation}");
        std::thread::park_timeout(Duration::from_millis(10));
    }
}
fn enroll(c: &mut Cluster) {
    let destination = c.root.join("4");
    c.leadership_maintenance = false;
    assert!(!c.enroll("create", 4, &destination, 1).status.success());
    assert!(
        !destination.exists(),
        "wrong schema created a learner store"
    );
    c.leadership_maintenance = true;
    let imported = c.enroll("create", 4, &destination, 1);
    assert!(
        imported.status.success(),
        "{}",
        String::from_utf8_lossy(&imported.stderr)
    );
    let original = stored(c, 4);
    assert_eq!(original.snapshot.unwrap().application_schema, 2);
    c.leadership_maintenance = false;
    assert!(!c.enroll("recover", 4, &destination, 1).status.success());
    assert_eq!(
        stored(c, 4),
        original,
        "wrong profile changed imported state"
    );
    c.leadership_maintenance = true;
    assert!(c.enroll("recover", 4, &destination, 1).status.success());
    assert_eq!(
        stored(c, 4),
        original,
        "lost import receipt changed original image"
    );
    assert!(!destination.join("drain.record").exists());
    verify_maintenance_checkpoint(c, &original);
}
fn verify_maintenance_checkpoint(c: &Cluster, original: &GroupLog) {
    use voteboat::{application::*, maintenance::*, native::snapshot_store::*, snapshot::*};
    let _gate = fixture_gate();
    let mut snapshots = NativeSnapshotStore::recover(
        FileSnapshotIo::open(c.root.join("4/snapshots")).unwrap(),
        SnapshotIdentity {
            store: store(4),
            group: original.bootstrap.group,
        },
        SnapshotLimits::default(),
    )
    .unwrap();
    let image = snapshots.load_pinned(original.snapshot.unwrap()).unwrap();
    let mut app = Maintenance::new(
        original.bootstrap.group,
        2,
        64,
        Counter::new(10000).unwrap(),
    )
    .unwrap();
    app.restore_checkpoint(
        image.metadata.application_schema,
        image.metadata.index,
        &image.application,
    )
    .unwrap();
    assert!(matches!(
        app.record(OperationId::new(19769).unwrap()).unwrap().phase,
        LeadershipPhase::Completed { .. }
    ));
}
fn checkpoint_after(c: &Cluster, id: usize, minimum: u64) {
    let end = Instant::now() + Duration::from_secs(10);
    loop {
        let text = c.ok(id, &["status"]);
        if field(&text, "committed=") >= minimum {
            break;
        }
        assert!(Instant::now() < end, "handoff prefix on {id}: {text}");
        std::thread::park_timeout(Duration::from_millis(10));
    }
    c.ok(id, &["checkpoint"]);
    loop {
        let text = c.ok(id, &["maintenance"]);
        if field(&text, "checkpoint_base=") >= minimum {
            return;
        }
        assert!(Instant::now() < end, "handoff checkpoint on {id}: {text}");
        std::thread::park_timeout(Duration::from_millis(10));
    }
}
fn field(text: &str, prefix: &str) -> u64 {
    text.split_whitespace()
        .find_map(|word| word.strip_prefix(prefix))
        .unwrap()
        .parse()
        .unwrap()
}
fn prepare(quic: bool, lost_reply: bool) -> (Cluster, Option<String>) {
    let tls = PathBuf::from(env!("CARGO_MANIFEST_DIR")).join("tests/fixtures/four-node-tls");
    let mut c = drain::cluster_with_tls(quic, Some(tls));
    c.children.push(None);
    c.stop();
    let deployment = c.root.join("four.deployment");
    let peers = c.root.join("four.commands");
    let mut routes = String::from("voteboat-deployment-v1\n");
    let mut commands = String::from("voteboat-command-peers-v1\n");
    for id in 1..=4 {
        let identity = store(id);
        routes.push_str(&format!(
            "{id} {} {} 127.0.0.1:{} node{id}.voteboat.test\n",
            identity.id.get(),
            identity.incarnation.get(),
            c.base + id as u16
        ));
        commands.push_str(&format!(
            "{id} 127.0.0.1:{} node{id}.voteboat.test\n",
            c.base + 100 + id as u16
        ));
    }
    fs::write(&deployment, routes).unwrap();
    fs::write(&peers, commands).unwrap();
    c.deployment = Some(deployment);
    c.command_peers = Some(peers);
    let admin = c.root.join("replacement.admin");
    let header = "voteboat-counter-admin-v1\nplacement 2 false\nreplica 1 1 1 1\nreplica 2 2 2 1\nreplica 3 3 3 1\nreplica 4 4 404 7\n";
    fs::write(
        &admin,
        format!("{header}learners 19770 1 2 4 m:3 v:1 v:2 v:3\n"),
    )
    .unwrap();
    c.admin_plan = Some(admin.clone());
    c.remote_admin = true;
    for id in 1..=3 {
        c.start(id, "recover-member");
    }
    configure(&mut c, "19770");
    // An older image is real setup state, not proof a later checkpoint finished.
    c.ok(1, &["checkpoint"]);
    drain::wait_manual_checkpoint(&c, 1);
    let leader = c.leader();
    let target = leader % 3 + 1;
    leader_request(
        &mut c,
        &[
            "move-leader",
            "19769",
            "2",
            &target.to_string(),
            &target.to_string(),
            "1",
        ],
    );
    let (_, completed) = leadership::status(&mut c, "19769", "phase=Completed");
    let minimum = completed
        .split_once("phase=Completed { index: ")
        .unwrap()
        .1
        .split(',')
        .next()
        .unwrap()
        .parse::<u64>()
        .unwrap();
    assert!(minimum > 0);
    for id in 1..=3 {
        checkpoint_after(&c, id, minimum);
    }
    c.stop();
    enroll(&mut c);
    let records = "joint 19751 2 3 4 1 m:3 v:2 v:3 v:4\nfinal 19751 3 4\n";
    fs::write(&admin, format!("{header}{records}")).unwrap();
    let plan = c.root.join("replacement.drain");
    fs::write(&plan, format!("voteboat-counter-drain-v1\noperation 19701\nsource 1 1 1\nhandoff 2 2 1\noriginal 2 4 m:3 v:1 v:2 v:3\n{records}")).unwrap();
    c.membership_drain = Some((1, plan));
    for id in 1..=3 {
        c.start(id, "recover-member");
    }
    let receipt = setup_recovery::handoff(&mut c, lost_reply);
    (c, receipt)
}

fn replacement_history(quic: bool, lost_reply: bool) {
    let (mut c, receipt) = prepare(quic, lost_reply);
    let log = fs::File::create(c.root.join("replacement-runner.log")).unwrap();
    let mut runner = {
        let _gate = fixture_gate();
        drain_runner::command(&c, 1, "19701", 3)
            .stdout(log.try_clone().unwrap())
            .stderr(log)
            .spawn()
            .unwrap()
    };
    wait_replacement_preparation(&c);
    assert!(drain::wait_status(&c, 1, "ready=false").contains("phase=Active"));
    for id in 1..=3 {
        assert!(c
            .wait_configuration_status(id, "19751")
            .contains("inconclusive_local_absence"));
    }
    runner.kill().unwrap();
    runner.wait().unwrap();
    // The imported learner needs the maintenance schema, but has no local
    // drain journal: do not claim enrollment copied the source's local gate.
    c.node_drain = false;
    c.start(4, "recover-member");
    c.node_drain = true;
    let result = run(&mut drain_runner::command(&c, 1, "19701", 3));
    assert!(
        result.status.success(),
        "{} {}",
        String::from_utf8_lossy(&result.stdout),
        String::from_utf8_lossy(&result.stderr)
    );
    drain::joined(&mut c, 1);
    // Losing another original voter now requires the new exact store's vote.
    drain::kill(&mut c, 3);
    assert!(authenticated_write(&c, &["add", "19772", "3"]).contains("Value(10)"));
    assert!(authenticated_write(&c, &["add", "19700", "7"]).contains("duplicate=true"));
    c.stop();
    for id in [2, 4] {
        let saved = stored(&c, id);
        let membership = saved.membership_at(saved.commit_index).unwrap();
        assert_eq!(membership.id().get(), 4);
        assert_eq!(
            membership.stable().voter_stores()[&NodeId::new(4).unwrap()],
            store(4)
        );
        assert_eq!(
            membership.stable().learners()[&NodeId::new(1).unwrap()],
            store(1)
        );
        assert!(membership.joint().is_none());
        c.node_drain = id != 4;
        c.start(id, "recover-member");
    }
    assert!(authenticated_write(&c, &["add", "19772", "3"]).contains("duplicate=true"));
    assert!(authenticated_write(&c, &["add", "19773", "1"]).contains("Value(11)"));
    if let Some(receipt) = receipt {
        let (_, recovered) = leadership::status(&mut c, "19750", "phase=Completed");
        assert_eq!(recovered, receipt);
    }
    c.stop();
}

fn wait_replacement_preparation(c: &Cluster) {
    // Readiness may start on the old leader before its handoff completes.
    // The original operation and absent learner are the test's invariant.
    let deadline = Instant::now() + Duration::from_secs(12);
    loop {
        let logs = (1..=3).map(|id| c.service_log(id)).collect::<Vec<_>>();
        if logs
            .iter()
            .any(|log| log.contains("administration operation=19751 preparing_learner=4"))
        {
            return;
        }
        assert!(
            Instant::now() < deadline,
            "missing replacement readiness: {logs:?}"
        );
        std::thread::park_timeout(Duration::from_millis(5));
    }
}
#[test]
fn maintenance_replacement_drain_waits_for_exact_learner_tcp() {
    replacement_history(false, false);
}
#[cfg(feature = "quic")]
#[test]
fn maintenance_replacement_drain_waits_for_exact_learner_quic() {
    replacement_history(true, false);
}
