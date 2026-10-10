// SPDX-License-Identifier: RPL-1.5
// Copyright (c) 2026 Karl Livesey
use super::*;

const OP: &str = "21101";
const RECORD: &str = "learners 21101 1 2 - m:3 v:1 v:2 v:3";
const UNKNOWN: &str =
    "UNKNOWN exact record locally durable but not committed; preserve original record\n";

fn setup(quic: bool) -> Cluster {
    let mut c = Cluster::new();
    c.quic = quic;
    let access = c.root.join("pending-access.txt");
    fs::write(&access, "voteboat-service-access-v1 1\n3 admin 1 1\n").unwrap();
    c.command_access = Some(access);
    c.command_principal = Some(3);
    for node in 1..=3 {
        c.start(node, "create");
    }
    assert!(authenticated_write(&c, &["add", "21100", "42"]).contains("Value(42)"));
    c.stop();
    let policy = c.root.join("pending-policy.txt");
    fs::write(
        &policy,
        "voteboat-counter-admin-v1\nplacement 2 false\nreplica 1 1\nreplica 2 2\nreplica 3 3\n",
    )
    .unwrap();
    c.admin_plan = Some(policy);
    c.targets_admin = true;
    for node in 1..=3 {
        c.start(node, "recover-member");
    }
    c
}

fn observe_pending(c: &Cluster, leader: usize) -> String {
    let pending = c.ok(leader, &["configuration-status", OP]);
    assert!(pending.contains("evidence=local_durable"), "{pending}");
    assert!(pending.contains("committed=NotFoundLocally"), "{pending}");
    assert!(pending.contains("accepted=Learners"), "{pending}");
    assert!(pending.contains("action=wait_for_commit"), "{pending}");
    let replay = c.request(leader, &["configure-record", RECORD]);
    assert!(!replay.status.success());
    let unknown = String::from_utf8(replay.stdout).unwrap();
    assert_eq!(unknown, UNKNOWN);
    assert!(retryable_leader_response(
        &["configure-record", RECORD],
        &unknown
    ));
    pending
}

fn history(quic: bool) {
    let mut c = setup(quic);
    let leader = c.leader();
    for node in (1..=3).filter(|node| *node != leader) {
        drain::kill(&mut c, node);
    }
    let first = c.request(leader, &["configure-record", RECORD]);
    assert!(!first.status.success());
    assert!(String::from_utf8_lossy(&first.stdout).starts_with("UNKNOWN "));
    wait_administration_event(&c, leader, "phase=configuration_queued");
    let pending = observe_pending(&c, leader);
    let conflict = c.request(
        leader,
        &["configure-record", "learners 21101 1 99 - m:3 v:1 v:2 v:3"],
    );
    assert!(!conflict.status.success());
    let refused = String::from_utf8(conflict.stdout).unwrap();
    assert!(
        refused.contains("conflicts with retained record"),
        "{refused}"
    );
    assert!(!retryable_leader_response(
        &["configure-record", RECORD],
        &refused
    ));
    assert_eq!(c.ok(leader, &["configuration-status", OP]), pending);
    for node in (1..=3).filter(|node| *node != leader) {
        c.start(node, "recover-member");
    }
    let committed = leader_request(&mut c, &["configure-record", RECORD]);
    assert!(committed.contains("committed_index="), "{committed}");
    assert!(committed.contains("operation=21101"), "{committed}");
    let duplicate = leader_request(&mut c, &["configure-record", RECORD]);
    assert!(duplicate.contains("duplicate=true"), "{duplicate}");
    assert!(authenticated_write(&c, &["add", "21100", "42"]).contains("duplicate=true"));
    assert_eq!(c.routed(&["read"]), "OK value=42\n");
    c.stop();
    for node in 1..=3 {
        c.start(node, "recover-member");
    }
    let leader = c.leader();
    assert!(c
        .ok(leader, &["configuration-status", OP])
        .contains("action=completed"));
    assert_eq!(
        leader_request(&mut c, &["configure-record", RECORD]),
        duplicate
    );
    assert!(authenticated_write(&c, &["add", "21100", "42"]).contains("duplicate=true"));
    assert_eq!(c.routed(&["read"]), "OK value=42\n");
    c.stop();
}

#[test]
fn pending_original_configuration_recovers_after_quorum_loss_tcp() {
    history(false);
}

#[test]
#[cfg(feature = "quic")]
fn pending_original_configuration_recovers_after_quorum_loss_quic() {
    history(true);
}
