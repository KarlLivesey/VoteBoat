// SPDX-License-Identifier: RPL-1.5
// Copyright (c) 2026 Karl Livesey
// Unless explicitly acquired and licensed from Licensor under another license,
// the contents of this file are subject to the Reciprocal Public License
// ("RPL") Version 1.5, or subsequent versions as allowed by the RPL, and You may
// not copy or use this file in either source code or executable form, except
// in compliance with the terms and conditions of the RPL.
//
// All software distributed under the RPL is provided strictly on an "AS IS"
// basis, WITHOUT WARRANTY OF ANY KIND, EITHER EXPRESS OR IMPLIED, AND LICENSOR
// HEREBY DISCLAIMS ALL SUCH WARRANTIES, INCLUDING WITHOUT LIMITATION, ANY
// WARRANTIES OF MERCHANTABILITY, FITNESS FOR A PARTICULAR PURPOSE, QUIET
// ENJOYMENT, OR NON-INFRINGEMENT. See the RPL for specific language governing
// rights and limitations under the RPL.
//! Authenticated promotion of a newly enrolled store across two interruptions.
use super::*;
use voteboat::identity::NodeId;

const OPERATION: &str = "19001";
const JOINT: &str = "joint 19001 5 6 7 1 m:2 v:2 v:4";
const FINAL: &str = "final 19001 6 7";
const MEMBERS: &[usize] = &[1, 2, 4];

fn setup(quic: bool) -> Cluster {
    let mut cluster = Cluster::new();
    cluster.quic = quic;
    cluster.tls =
        Some(PathBuf::from(env!("CARGO_MANIFEST_DIR")).join("tests/fixtures/four-node-tls"));
    cluster.children.push(None);
    for id in 1..=3 {
        cluster.start(id, "create");
    }
    cluster.leader();
    assert!(leader_write(&mut cluster, &["add", "700", "42"]).contains("Value(42)"));
    cluster.stop();
    let plan = cluster.root.join("new-voter.plan");
    let original =
        "voteboat-counter-admin-v1\nplacement 2 false\nreplica 1 1\nreplica 2 2\nreplica 3 3\n";
    fs::write(&plan, format!("{original}joint 1000 1 2 3 3 m:2 v:1 v:2\nfinal 1000 2 3\nlearners 1001 3 4 - m:2 v:1 v:2\n")).unwrap();
    cluster.admin_plan = Some(plan.clone());
    for id in 1..=3 {
        cluster.start(id, "recover-member");
    }
    finish_lifecycle_operation(&cluster, &[1, 2], "1001", "701");
    cluster.stop();
    deployment(&mut cluster);
    let provisioned =
        "voteboat-counter-admin-v1\nplacement 2 false\nreplica 1 1\nreplica 2 2\nreplica 4 4\n";
    plan_fourth_learner(&cluster, &plan, provisioned);
    for id in [1, 2] {
        cluster.start(id, "recover-member");
    }
    finish_lifecycle_operation(&cluster, &[1, 2], "1002", "702");
    for id in [1, 2] {
        cluster.ok(id, &["checkpoint"]);
    }
    cluster.stop();
    inspect_lifecycle(&cluster, &[1, 2], 5, &[1, 2], &[4], &[1000, 1001, 1002], 42);
    enroll_checkpointed_fourth(&cluster);
    fs::write(&plan, provisioned).unwrap();
    let access = cluster.root.join("new-voter-access.txt");
    fs::write(&access, "voteboat-service-access-v1 1\n3 admin 1 1\n").unwrap();
    cluster.command_access = Some(access);
    cluster.command_principal = Some(3);
    cluster.targets_admin = true;
    cluster
}

fn deployment(cluster: &mut Cluster) {
    let path = cluster.root.join("new-voter.deployment");
    fs::write(&path, format!("voteboat-deployment-v1\n1 1 1 127.0.0.1:{} node1.voteboat.test\n2 2 1 127.0.0.1:{} node2.voteboat.test\n4 404 7 127.0.0.1:{} node4.voteboat.test\n", cluster.base+1, cluster.base+2, cluster.base+4)).unwrap();
    cluster.deployment = Some(path);
}

fn stop_member(cluster: &mut Cluster, id: usize, checkpoint: bool) {
    if checkpoint {
        cluster.ok(id, &["checkpoint"]);
        cluster.ok(id, &["quit"]);
    }
    let mut child = cluster.children[id - 1].take().unwrap();
    if !checkpoint {
        child.kill().unwrap();
    }
    let deadline = Instant::now() + Duration::from_secs(15);
    loop {
        if let Some(status) = child.try_wait().unwrap() {
            assert!(!checkpoint || status.success());
            return;
        }
        assert!(Instant::now() < deadline, "node {id} did not stop");
        std::thread::park_timeout(Duration::from_millis(5));
    }
}

fn committed(cluster: &Cluster, id: usize) -> u64 {
    cluster
        .ok(id, &["status"])
        .split("committed=")
        .nth(1)
        .unwrap()
        .trim()
        .parse()
        .unwrap()
}

fn catch_up(cluster: &mut Cluster) {
    let leader = cluster.leader();
    let through = committed(cluster, leader);
    let deadline = Instant::now() + Duration::from_secs(20);
    while !MEMBERS.iter().all(|&id| committed(cluster, id) >= through) {
        assert!(
            Instant::now() < deadline,
            "new store did not catch up: {:?}",
            cluster.root
        );
        std::thread::park_timeout(Duration::from_millis(10));
    }
}

fn phase(cluster: &Cluster, ids: &[usize], expected: &str) {
    let deadline = Instant::now() + Duration::from_secs(20);
    loop {
        let states = ids
            .iter()
            .map(|&id| cluster.ok(id, &["configuration-status", OPERATION]))
            .collect::<Vec<_>>();
        if states.iter().all(|s| s.contains(expected)) {
            return;
        }
        assert!(
            Instant::now() < deadline,
            "missing {expected}: {states:?}; {:?}",
            cluster.root
        );
        std::thread::park_timeout(Duration::from_millis(5));
    }
}

fn interrupted_preparation(cluster: &mut Cluster) {
    for id in [1, 2] {
        cluster.start(id, "recover-member");
    }
    cluster.leader();
    assert!(leader_write(cluster, &["add", "19000", "0"]).contains("Value(42)"));
    let leader = cluster.leader();
    let unread = UnobservedCommand::send(cluster, leader, &format!("configure-record {JOINT}"));
    wait_administration_event(
        cluster,
        leader,
        "administration operation=19001 preparing_learner=4",
    );
    stop_member(cluster, leader, false);
    unread.disconnect();
    let saved = enrolled_state_for(
        &cluster.root.join(leader.to_string()),
        leader as u64,
        lifecycle_store(leader),
    );
    assert!(!saved
        .membership()
        .unwrap()
        .operations()
        .contains(&voteboat::identity::OperationId::new(19001).unwrap()));
    cluster.start(leader, "recover-member");
    cluster.leader();
    phase(cluster, &[1, 2], "inconclusive_local_absence");
    cluster.start(4, "recover-member");
    assert!(leader_write(cluster, &["add", "19000", "0"]).contains("duplicate=true"));
    catch_up(cluster);
    phase(cluster, MEMBERS, "inconclusive_local_absence");
}

fn interrupted_joint(cluster: &mut Cluster, checkpoint: bool) {
    assert!(leader_write(cluster, &["configure-record", JOINT]).contains("committed_index="));
    phase(cluster, MEMBERS, "action=finalize_requires_authorization");
    assert!(leader_write(cluster, &["configure-record", JOINT]).contains("duplicate=true"));
    stop_member(cluster, 4, checkpoint);
    let saved = enrolled_state_for(&cluster.root.join("4"), 4, lifecycle_store(4));
    let membership = saved.membership_at(saved.commit_index).unwrap();
    assert_eq!(membership.id().get(), 6);
    let joint = membership.joint().unwrap();
    assert_eq!(joint.operation.get(), 19001);
    assert_eq!(
        joint.next.voter_stores()[&NodeId::new(4).unwrap()],
        lifecycle_store(4)
    );
    if checkpoint {
        assert!(saved.base_index() >= joint.index);
        assert!(saved
            .membership_at(saved.base_index())
            .unwrap()
            .joint()
            .is_some());
    } else {
        assert!(saved.base_index() < joint.index);
        assert!(saved.entry_at(joint.index).is_some());
    }
    let leader = cluster.leader();
    let unread = UnobservedCommand::send(cluster, leader, &format!("configure-record {FINAL}"));
    // Actual accepted/durable final state, while commitment remains joint.
    phase(cluster, &[leader], "accepted=Final");
    phase(cluster, &[leader], "committed=Joint");
    phase(cluster, &[leader], "action=wait_for_commit");
    cluster.start(4, "recover-member");
    finish_lifecycle_operation(cluster, MEMBERS, OPERATION, "19002");
    unread.disconnect();
    assert!(leader_write(cluster, &["configure-record", FINAL]).contains("duplicate=true"));
}

fn history(quic: bool, checkpoint: bool) {
    let mut cluster = setup(quic);
    eprintln!("new-voter quic={quic} checkpoint={checkpoint} preparing");
    interrupted_preparation(&mut cluster);
    eprintln!("new-voter quic={quic} checkpoint={checkpoint} joint");
    interrupted_joint(&mut cluster, checkpoint);
    assert!(leader_write(&mut cluster, &["add", "19003", "1"]).contains("Value(43)"));
    assert_eq!(leader_write(&mut cluster, &["read"]), "OK value=43\n");
    catch_up(&mut cluster);
    cluster.stop();
    inspect_lifecycle(
        &cluster,
        MEMBERS,
        7,
        &[2, 4],
        &[1],
        &[1000, 1001, 1002, 19001],
        43,
    );
    for &id in MEMBERS {
        cluster.start(id, "recover-member");
    }
    assert!(matches!(cluster.leader(), 2 | 4));
    assert!(leader_write(&mut cluster, &["configure-record", FINAL]).contains("duplicate=true"));
    let original = leader_write(&mut cluster, &["add", "700", "42"]);
    assert!(original.contains("duplicate=true") && original.contains("Value(42)"));
    assert!(leader_write(&mut cluster, &["add", "19003", "1"]).contains("duplicate=true"));
    assert_eq!(leader_write(&mut cluster, &["read"]), "OK value=43\n");
    cluster.stop();
    inspect_lifecycle(
        &cluster,
        MEMBERS,
        7,
        &[2, 4],
        &[1],
        &[1000, 1001, 1002, 19001],
        43,
    );
    fs::remove_dir_all(&cluster.root).unwrap();
}

#[test]
fn public_new_voter_tcp_recovers_readiness_and_joint_wal_and_checkpoint() {
    for checkpoint in [false, true] {
        history(false, checkpoint);
    }
}
#[cfg(feature = "quic")]
#[test]
fn public_new_voter_quic_recovers_readiness_and_joint_wal_and_checkpoint() {
    for checkpoint in [false, true] {
        history(true, checkpoint);
    }
}
