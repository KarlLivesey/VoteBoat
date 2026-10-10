// SPDX-License-Identifier: RPL-1.5
// Copyright (c) 2026 Karl Livesey
use super::*;
const OP: &str = "7001";
fn plan(config: u64) -> String {
    format!("voteboat-counter-admin-v1\nplacement 2 false\nreplica 1 1\nreplica 2 2\nreplica 3 3\njoint {OP} {config} {} {} 3 m:2 v:1 v:2\nfinal {OP} {} {}\n", config + 1, config + 2, config + 1, config + 2)
}
fn setup(quic: bool) -> Cluster {
    let mut c = groups::setup(quic);
    fs::write(c.root.join("seven.plan"), plan(9)).unwrap();
    fs::write(c.root.join("eight.plan"), plan(11)).unwrap();
    let path = c.root.join("group-admin.txt");
    fs::write(
        &path,
        "voteboat-counter-group-admin-v1\ngroup 7 3 seven.plan\ngroup 8 2 eight.plan\n",
    )
    .unwrap();
    c.group_admin_plans = Some(path);
    let access = c.command_access.as_ref().unwrap();
    let permissions = fs::read_to_string(access)
        .unwrap()
        .replace("2 writer", "2 admin");
    fs::write(access, permissions).unwrap();
    c
}
fn authorization(c: &mut Cluster, leader: usize) {
    for (principal, group, incarnation) in [(1, "7", "3"), (2, "8", "2"), (2, "7", "4")] {
        c.command_principal = Some(principal);
        let denied = c.request(leader, &["group", group, incarnation, "configure", OP]);
        assert!(!denied.status.success());
        assert!(String::from_utf8_lossy(&denied.stdout).contains("AUTHORIZATION"));
    }
    c.command_principal = Some(3);
    let leader = groups::leader(c, "1", "1");
    let disabled = c.request(leader, &["group", "1", "1", "configure", OP]);
    assert!(!disabled.status.success());
    assert!(String::from_utf8_lossy(&disabled.stdout).contains("administration disabled"));
}
fn refused(c: &mut Cluster, text: &str, expected: &str) {
    fs::write(c.group_admin_plans.as_ref().unwrap(), text).unwrap();
    c.start(1, "create");
    let deadline = Instant::now() + Duration::from_secs(10);
    while c.children[0]
        .as_mut()
        .unwrap()
        .try_wait()
        .unwrap()
        .is_none()
    {
        assert!(Instant::now() < deadline, "{}", c.service_log(1));
        std::thread::sleep(Duration::from_millis(5));
    }
    assert!(!c.children[0].take().unwrap().wait().unwrap().success());
    assert!(c.service_log(1).contains(expected), "{}", c.service_log(1));
    assert!(!c.root.join("1").exists());
}
fn status(c: &Cluster, id: usize, group: &str, incarnation: &str, expected: &str) {
    let deadline = Instant::now() + Duration::from_secs(15);
    loop {
        let output = c.request(
            id,
            &["group", group, incarnation, "configuration-status", OP],
        );
        let text = String::from_utf8_lossy(&output.stdout);
        if output.status.success() && text.contains(expected) {
            return;
        }
        assert!(
            Instant::now() < deadline,
            "group={group} node={id} expected={expected} {text}\n{}",
            c.service_log(id)
        );
        std::thread::sleep(Duration::from_millis(10));
    }
}
fn configure(c: &mut Cluster, group: &str, incarnation: &str) {
    let deadline = Instant::now() + Duration::from_secs(15);
    loop {
        let leader = groups::leader(c, group, incarnation);
        let output = c.request(leader, &["group", group, incarnation, "configure", OP]);
        if output.status.success() {
            return;
        }
        let text = String::from_utf8_lossy(&output.stdout);
        assert!(
            text.starts_with("UNKNOWN") || text.contains("NOT_LEADER"),
            "{text}"
        );
        assert!(Instant::now() < deadline, "{text}");
    }
}
fn durable_joint(c: &Cluster, checkpoint: bool) {
    use voteboat::{identity::*, log::*, native::log_store::*};
    let _guard = fixture_gate();
    for node in 1..=3 {
        let log = NativeLogStore::recover(
            FileLogIo::open(c.root.join(node.to_string())).unwrap(),
            StoreIdentity {
                id: StoreId::new(node).unwrap(),
                incarnation: StoreIncarnation::new(1).unwrap(),
            },
            LogLimits::default(),
        )
        .unwrap();
        for (group, incarnation) in [(7, 3), (8, 2)] {
            let state = log
                .state(GroupIdentity {
                    id: GroupId::new(group).unwrap(),
                    incarnation: GroupIncarnation::new(incarnation).unwrap(),
                })
                .unwrap();
            let membership = state.membership_at(state.commit_index).unwrap();
            let joint = membership.joint().unwrap();
            assert_eq!(joint.operation.get(), 7001);
            if checkpoint {
                assert!(state.base_index() >= joint.index);
                assert!(state
                    .membership_at(state.base_index())
                    .unwrap()
                    .joint()
                    .is_some());
            } else {
                assert!(state.entry_at(joint.index).is_some());
            }
        }
    }
}
fn histories(quic: bool) {
    let mut c = setup(quic);
    for n in 1..=3 {
        c.start(n, "create");
    }
    assert!(c
        .routed(&["group", "1", "1", "add", "42", "3"])
        .contains("Value(3)"));
    assert!(c
        .routed(&["group", "7", "3", "add", "42", "5"])
        .contains("Value(5)"));
    let leader = groups::leader(&mut c, "7", "3");
    authorization(&mut c, leader);
    let unread = UnobservedCommand::send(&c, leader, "group 7 3 configure 7001");
    for n in 1..=3 {
        status(&c, n, "7", "3", "committed=Joint");
    }
    unread.disconnect();
    status(&c, 1, "8", "2", "action=inconclusive_local_absence");
    configure(&mut c, "8", "2");
    for n in 1..=3 {
        status(&c, n, "8", "2", "committed=Joint");
    }
    status(&c, 1, "1", "1", "action=inconclusive_local_absence");
    if quic {
        for n in 1..=3 {
            for (g, inc) in [("7", "3"), ("8", "2")] {
                c.ok(n, &["group", g, inc, "checkpoint"]);
            }
        }
        c.stop();
    } else {
        for n in 1..=3 {
            drain::kill(&mut c, n);
        }
    }
    durable_joint(&c, quic);
    for n in 1..=3 {
        c.start(n, "recover");
    }
    for (g, inc) in [("7", "3"), ("8", "2")] {
        configure(&mut c, g, inc);
        for n in 1..=3 {
            status(&c, n, g, inc, "committed=Final");
        }
        configure(&mut c, g, inc);
    }
    assert!(c
        .routed(&["group", "7", "3", "add", "42", "5"])
        .contains("duplicate=true"));
    assert!(c
        .routed(&["group", "7", "3", "add", "43", "2"])
        .contains("Value(7)"));
    assert_eq!(c.routed(&["group", "1", "1", "read"]), "OK value=3\n");
    status(&c, 1, "1", "1", "action=inconclusive_local_absence");
    c.stop();
}
#[test]
fn group_membership_tcp_reopens_joint_records_and_preserves_scope() {
    histories(false);
}
#[cfg(feature = "quic")]
#[test]
fn group_membership_quic_reopens_joint_checkpoints_and_preserves_scope() {
    histories(true);
}

#[test]
fn group_plan_manifest_rejects_wrong_scopes_and_bounds_before_store_creation() {
    let mut c = setup(false);
    let cases = [
        "bad",
        "voteboat-counter-group-admin-v1\n",
        "voteboat-counter-group-admin-v1\ngroup 7 3 seven.plan\ngroup 7 3 seven.plan\n",
        "voteboat-counter-group-admin-v1\ngroup 99 1 seven.plan\n",
        "voteboat-counter-group-admin-v1\ngroup 7 4 seven.plan\n",
        "voteboat-counter-group-admin-v1\ngroup 7 3 absent.plan\n",
    ]
    .map(str::to_owned);
    for text in cases.into_iter().chain(std::iter::once("x".repeat(65537))) {
        refused(&mut c, &text, "Error:");
    }
}

#[test]
fn group_plan_count_and_aggregate_bytes_are_bounded_before_store_creation() {
    let mut c = setup(false);
    let manifest = |count| {
        format!(
            "voteboat-counter-group-admin-v1\n{}",
            (1..=count)
                .map(|n| format!("group {n} 1 seven.plan\n"))
                .collect::<String>()
        )
    };
    refused(&mut c, &manifest(257), "exceeds 256 groups");
    let large = plan(9).replace("placement 2", &format!("placement{}2", " ".repeat(60000)));
    fs::write(c.root.join("seven.plan"), large).unwrap();
    refused(&mut c, &manifest(20), "aggregate plan file byte limit");
}
