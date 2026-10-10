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
//! A public joint command outlives the leader it demotes and its unread reply.
use super::*;
use voteboat::{identity::*, log::GroupLog};

fn setup(quic: bool) -> Cluster {
    let mut cluster = Cluster::new();
    cluster.quic = quic;
    let access = cluster.root.join("joint-access.txt");
    fs::write(&access, "voteboat-service-access-v1 1\n3 admin 1 1\n").unwrap();
    cluster.command_access = Some(access);
    cluster.command_principal = Some(3);
    for id in 1..=3 {
        cluster.start(id, "create");
    }
    let leader = cluster.leader();
    assert!(cluster
        .ok(leader, &["add", "18000", "42"])
        .contains("Value(42)"));
    cluster.stop();
    let policy = cluster.root.join("joint-policy.txt");
    fs::write(
        &policy,
        "voteboat-counter-admin-v1\nplacement 2 false\nreplica 1 1\nreplica 2 2\nreplica 3 3\n",
    )
    .unwrap();
    cluster.admin_plan = Some(policy);
    cluster.targets_admin = true;
    for id in 1..=3 {
        cluster.start(id, "recover-member");
    }
    cluster
}

fn state(cluster: &Cluster, id: usize, value: i64) -> GroupLog {
    recovered_state_for(
        &cluster.root.join(id.to_string()),
        id as u64,
        lifecycle_store(id),
        value,
        (18000, 42),
    )
}

fn cut_leader(cluster: &mut Cluster, leader: usize, checkpoint: bool) {
    if checkpoint {
        cluster.ok(leader, &["checkpoint"]);
        cluster.ok(leader, &["quit"]);
    }
    let mut child = cluster.children[leader - 1].take().unwrap();
    if !checkpoint {
        child.kill().unwrap();
    }
    let deadline = Instant::now() + Duration::from_secs(15);
    loop {
        if let Some(status) = child.try_wait().unwrap() {
            if checkpoint {
                assert!(status.success());
            }
            break;
        }
        assert!(Instant::now() < deadline, "leader did not stop");
        std::thread::park_timeout(Duration::from_millis(5));
    }
    let saved = state(cluster, leader, 42);
    let membership = saved.membership_at(saved.commit_index).unwrap();
    assert_eq!(membership.id().get(), 2);
    let joint = membership.joint().unwrap();
    assert_eq!(joint.operation.get(), 18001);
    if checkpoint {
        assert!(saved.base_index() >= joint.index);
        assert!(saved
            .membership_at(saved.base_index())
            .unwrap()
            .joint()
            .is_some());
    } else {
        assert_eq!(saved.base_index(), 0);
        assert!(saved.entry_at(joint.index).is_some());
    }
}

fn finish(cluster: &mut Cluster, leader: usize, joint: &str) {
    assert!(cluster
        .ok(leader, &["configuration-status", "18001"])
        .contains("action=finalize_requires_authorization"));
    assert!(leader_write(cluster, &["configure-record", joint]).contains("duplicate=true"));
    let conflict = joint.replacen("1 2 3", "1 2 99", 1);
    let refused = cluster.request(leader, &["configure-record", &conflict]);
    assert!(!refused.status.success());
    assert!(String::from_utf8_lossy(&refused.stdout).contains("conflicts with retained record"));
    assert!(
        leader_write(cluster, &["configure-record", "final 18001 2 3"])
            .contains("committed_index=")
    );
    let deadline = Instant::now() + Duration::from_secs(15);
    loop {
        let complete = (1..=3).all(|id| {
            cluster
                .ok(id, &["configuration-status", "18001"])
                .contains("action=completed")
        });
        if complete {
            break;
        }
        assert!(Instant::now() < deadline, "final record did not replicate");
        std::thread::park_timeout(Duration::from_millis(5));
    }
    assert!(authenticated_write(cluster, &["add", "18003", "1"]).contains("Value(43)"));
    let original = authenticated_write(cluster, &["add", "18000", "42"]);
    assert!(original.contains("duplicate=true") && original.contains("Value(42)"));
    assert_eq!(cluster.routed(&["read"]), "OK value=43\n");
    wait_committed(cluster, leader);
}

fn wait_committed(cluster: &Cluster, leader: usize) {
    let committed = |id| {
        cluster
            .ok(id, &["status"])
            .split("committed=")
            .nth(1)
            .unwrap()
            .trim()
            .parse::<u64>()
            .unwrap()
    };
    let boundary = committed(leader);
    let deadline = Instant::now() + Duration::from_secs(15);
    while !(1..=3).all(|id| committed(id) >= boundary) {
        assert!(Instant::now() < deadline, "learner did not catch up");
        std::thread::park_timeout(Duration::from_millis(5));
    }
}

fn verify_files(cluster: &Cluster, retired: usize) {
    let voters: BTreeSet<_> = (1..=3)
        .filter(|id| *id != retired)
        .map(|id| NodeId::new(id as u64).unwrap())
        .collect();
    for id in 1..=3 {
        let saved = state(cluster, id, 43);
        let membership = saved.membership_at(saved.commit_index).unwrap();
        assert_eq!(membership.id().get(), 3);
        assert!(membership.joint().is_none());
        assert_eq!(
            membership
                .stable()
                .voter_stores()
                .keys()
                .copied()
                .collect::<BTreeSet<_>>(),
            voters
        );
        assert_eq!(
            membership.stable().learners(),
            &[(
                NodeId::new(retired as u64).unwrap(),
                lifecycle_store(retired)
            )]
            .into()
        );
        assert!(membership
            .operations()
            .contains(&OperationId::new(18001).unwrap()));
    }
}

fn current_term(cluster: &Cluster, node: usize) -> u64 {
    cluster
        .ok(node, &["status"])
        .split("term=")
        .nth(1)
        .unwrap()
        .split_whitespace()
        .next()
        .unwrap()
        .parse()
        .unwrap()
}
fn unread_joint(cluster: &mut Cluster, original: usize, joint: &str) {
    let command = format!("configure-record {joint}");
    let mut attempted = Some((original, current_term(cluster, original)));
    let mut pending = vec![UnobservedCommand::send(cluster, original, &command)];
    let deadline = Instant::now() + Duration::from_secs(15);
    loop {
        let status = cluster.request(original, &["configuration-status", "18001"]);
        let text = String::from_utf8(status.stdout).unwrap();
        if status.status.success() && text.contains("action=finalize_requires_authorization") {
            break;
        }
        let leader = cluster.leader();
        let term = current_term(cluster, leader);
        if attempted != Some((leader, term)) {
            assert!(pending.len() < 8, "too many unread joint attempts");
            pending.push(UnobservedCommand::send(cluster, leader, &command));
            attempted = Some((leader, term));
        }
        assert!(
            Instant::now() < deadline,
            "joint did not commit locally: {text}; files at {:?}",
            cluster.root
        );
        std::thread::park_timeout(Duration::from_millis(10));
    }
    for channel in pending {
        channel.disconnect();
    }
}

fn history(quic: bool, checkpoint: bool) {
    let mut cluster = setup(quic);
    let leader = cluster.leader();
    assert!(cluster
        .ok(leader, &["add", "18002", "0"])
        .contains("Value(42)"));
    let voters = (1..=3)
        .filter(|id| *id != leader)
        .map(|id| format!("v:{id}"))
        .collect::<Vec<_>>()
        .join(" ");
    let joint = format!("joint 18001 1 2 3 {leader} m:2 {voters}");
    unread_joint(&mut cluster, leader, &joint);
    cut_leader(&mut cluster, leader, checkpoint);
    let replacement = cluster.leader();
    assert_ne!(replacement, leader);
    cluster.start(leader, "recover-member");
    cluster.wait_configuration_status(leader, "18001");
    finish(&mut cluster, replacement, &joint);
    cluster.stop();
    verify_files(&cluster, leader);
    for id in 1..=3 {
        cluster.start(id, "recover-member");
    }
    let replacement = cluster.leader();
    assert_ne!(
        replacement, leader,
        "demoted learner cannot win an election"
    );
    assert!(authenticated_write(&cluster, &["add", "18003", "1"]).contains("duplicate=true"));
    assert_eq!(cluster.routed(&["read"]), "OK value=43\n");
    cluster.stop();
    verify_files(&cluster, leader);
    fs::remove_dir_all(&cluster.root).unwrap();
}

#[test]
fn public_joint_survives_demoted_leader_loss_tcp() {
    for checkpoint in [false, true] {
        history(false, checkpoint);
    }
}

#[cfg(feature = "quic")]
#[test]
fn public_joint_survives_demoted_leader_loss_quic() {
    for checkpoint in [false, true] {
        history(true, checkpoint);
    }
}
