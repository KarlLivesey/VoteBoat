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

fn setup(quic: bool) -> (Cluster, voteboat::membership::Membership) {
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
    let saved = state(&cluster, 1, 42);
    let membership = saved.membership_at(saved.commit_index).unwrap();
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
    (cluster, membership)
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

fn finish(cluster: &mut Cluster, leader: usize, joint: &str, finalize: &str) {
    assert!(cluster
        .ok(leader, &["configuration-status", "18001"])
        .contains("action=finalize_requires_authorization"));
    assert!(leader_request(cluster, &["configure-record", joint]).contains("duplicate=true"));
    let conflict = joint.replacen("1 2 3", "1 2 99", 1);
    let refused = cluster.request(leader, &["configure-record", &conflict]);
    assert!(!refused.status.success());
    assert!(String::from_utf8_lossy(&refused.stdout).contains("conflicts with retained record"));
    assert!(leader_request(cluster, &["configure-record", finalize]).contains("committed_index="));
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
    let (mut cluster, membership) = setup(quic);
    let leader = cluster.leader();
    assert!(cluster
        .ok(leader, &["add", "18002", "0"])
        .contains("Value(42)"));
    let (joint, finalize) = plan_removal(&membership, leader);
    unread_joint(&mut cluster, leader, &joint);
    cut_leader(&mut cluster, leader, checkpoint);
    let replacement = cluster.leader();
    assert_ne!(replacement, leader);
    cluster.start(leader, "recover-member");
    cluster.wait_configuration_status(leader, "18001");
    finish(&mut cluster, replacement, &joint, &finalize);
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

fn plan_removal(current: &voteboat::membership::Membership, retiring: usize) -> (String, String) {
    use voteboat::{
        membership::ConfigurationChange, native::placement::NativePlacementAuthorizer,
        placement::*, quorum::*,
    };
    let candidates = [1usize, 2, 3].map(|n| PlacementCandidate {
        node: NodeId::new(n as u64).unwrap(),
        placement: ReplicaPlacement {
            store: lifecycle_store(n),
            domain: FailureDomainId::new(n as u64).unwrap(),
        },
        enabled: true,
        free_bytes: 0,
        free_replica_slots: 0,
        load_permille: 0,
    });
    let group = GroupIdentity {
        id: GroupId::new(1).unwrap(),
        incarnation: GroupIncarnation::new(1).unwrap(),
    };
    let authorizer = NativePlacementAuthorizer::new(
        group,
        candidates.iter().map(|c| (c.node, c.placement)).collect(),
        PlacementRequirements {
            minimum_voting_domains: 2,
            survive_any_single_domain_loss: false,
        },
    )
    .unwrap();
    let policy = Policy::new(
        Tree::Majority(
            candidates
                .iter()
                .filter(|c| c.node.get() != retiring as u64)
                .map(|c| Tree::Voter(c.node))
                .collect(),
        ),
        Limits::default(),
    )
    .unwrap();
    let plan = plan_voter_change(
        &authorizer,
        PlacementRequest {
            snapshot: PlacementSnapshot {
                group,
                configuration: current.id(),
                generation: PlacementSampleGeneration::new(1).unwrap(),
                observed_at: voteboat::runtime::MonoTime(0),
                expires_at: voteboat::runtime::MonoTime(100),
                candidates: &candidates,
            },
            current,
            now: voteboat::runtime::MonoTime(1),
            minimum_free_bytes: 0,
        },
        policy,
        RemovedVoters::RetainAsLearners,
        OperationId::new(18001).unwrap(),
    )
    .unwrap();
    let ConfigurationChange::Joint { id, next } = &plan.joint.change else {
        panic!("not joint");
    };
    let voters = next
        .policy()
        .voters()
        .iter()
        .map(|n| format!("v:{}", n.get()))
        .collect::<Vec<_>>()
        .join(" ");
    let learners = next
        .learners()
        .keys()
        .map(|n| n.get().to_string())
        .collect::<Vec<_>>()
        .join(",");
    assert_eq!(learners, retiring.to_string());
    let ConfigurationChange::Final { id: final_id } = plan.finalize.change else {
        panic!("not final");
    };
    (
        format!(
            "joint {} {} {} {} {learners} m:2 {voters}",
            plan.joint.operation.get(),
            plan.joint.expected.get(),
            id.get(),
            next.id().get()
        ),
        format!(
            "final {} {} {}",
            plan.finalize.operation.get(),
            plan.finalize.expected.get(),
            final_id.get()
        ),
    )
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
