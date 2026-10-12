// SPDX-License-Identifier: RPL-1.5
// Copyright (c) 2026 Karl Livesey
use super::{
    aliases::Bridge,
    cluster::{ChildNode, Cluster},
    setup,
};
use std::time::{Duration, Instant};
use voteboat::{application::StateMachine, identity::OperationId, logical_voter::*, runtime::*};

pub fn prepare(cohorts: &mut [Cluster; 3], generation: u128) {
    let mut requested = [None; 3];
    let end = Instant::now() + Duration::from_secs(15);
    loop {
        for cohort in &mut *cohorts {
            cohort.poll();
        }
        if cohorts.iter().all(selected_ready) {
            return;
        }
        for (slot, cohort) in cohorts.iter_mut().enumerate() {
            transfer(cohort, generation, &mut requested[slot]);
        }
        assert!(
            Instant::now() < end,
            "actual selected child leadership timeout"
        );
    }
}

fn selected_ready(cohort: &Cluster) -> bool {
    cohort.leader().is_some_and(|index| {
        let leader = cohort.nodes[index]
            .local()
            .owner
            .core(setup::mapping(cohort.slot).child)
            .unwrap();
        leader.local_node() == cohort.representative
            && cohort.all().all(|node| {
                let core = node
                    .local()
                    .owner
                    .core(setup::mapping(cohort.slot).child)
                    .unwrap();
                core.state().hard_state.term == leader.state().hard_state.term
                    && !core.has_pending_dependency()
                    && core.state().commit_index >= leader.state().commit_index
                    && node.local().applications[&setup::mapping(cohort.slot).child].applied_index()
                        >= leader.state().commit_index
                    && (core.local_node() == cohort.representative
                        || core.role() == voteboat::raft::Role::Follower)
            })
    })
}

fn transfer(cohort: &mut Cluster, generation: u128, requested: &mut Option<(u64, u64)>) {
    let group = setup::mapping(cohort.slot).child;
    let leader = cohort.all().find_map(|node| {
        let core = node.local().owner.core(group).unwrap();
        (core.role() == voteboat::raft::Role::Leader && !core.has_pending_dependency()).then_some((
            core.local_node(),
            core.state().hard_state.term,
            core.membership().id(),
        ))
    });
    let Some((node, term, configuration)) = leader else {
        return;
    };
    if node == cohort.representative || *requested == Some((node.get(), term)) {
        return;
    }
    let request = voteboat::raft::LeadershipTransferRequest {
        operation: OperationId::new(generation + 100 + cohort.slot as u128).unwrap(),
        target: voteboat::secure::PeerIdentity {
            node: cohort.representative,
            store: super::support::identity(1000 + u128::from(cohort.representative.get())),
        },
        configuration,
    };
    cohort
        .nodes
        .iter_mut()
        .chain(&mut cohort.parked)
        .find(|n| n.local().owner.core(group).unwrap().local_node() == node)
        .unwrap()
        .control(group, NodeControl::TransferLeadership(request))
        .unwrap();
    *requested = Some((node.get(), term));
}

pub fn claims(
    bridges: &mut [Bridge<'_>],
    peers: &mut [&mut Vec<ChildNode>],
    clocks: &[Instant],
    generation: u128,
) -> Vec<VerifiedLogicalImage> {
    for (i, bridge) in bridges.iter_mut().enumerate() {
        bridge
            .begin_claim(OperationId::new(generation + i as u128).unwrap())
            .unwrap();
    }
    let mut images = (0..bridges.len()).map(|_| None).collect::<Vec<_>>();
    let end = Instant::now() + Duration::from_secs(15);
    while images.iter().any(Option::is_none) {
        for (i, bridge) in bridges.iter_mut().enumerate() {
            let now = MonoTime(clocks[i].elapsed().as_millis() as u64);
            for node in peers[i].iter_mut() {
                node.poll(now, NodePollBudget::default()).unwrap();
            }
            bridge
                .advance(
                    now,
                    LogicalBridgeBudget {
                        node_polls: 3,
                        completions: 4,
                        transitions: 1,
                        node: NodePollBudget::default(),
                    },
                )
                .unwrap();
            if images[i].is_none() {
                images[i] = bridge.take_image();
            }
        }
        assert!(
            Instant::now() < end,
            "{}",
            diagnostic(bridges, peers, generation)
        );
        std::thread::park_timeout(Duration::from_millis(1));
    }
    images.into_iter().map(Option::unwrap).collect()
}

fn diagnostic(bridges: &[Bridge<'_>], peers: &[&mut Vec<ChildNode>], generation: u128) -> String {
    let progress = bridges
        .iter()
        .map(|b| (b.mapping(), b.usage()))
        .collect::<Vec<_>>();
    let actual_peers = peers
        .iter()
        .enumerate()
        .map(|(i, nodes)| {
            nodes
                .iter()
                .map(|n| {
                    let core = n.local().owner.core(bridges[i].mapping().child).unwrap();
                    (
                        core.local_node(),
                        core.role(),
                        core.state().hard_state.term,
                        core.state().commit_index,
                    )
                })
                .collect::<Vec<_>>()
        })
        .collect::<Vec<_>>();
    format!(
        "actual original concurrent claims {generation}: {progress:?}; parked peers {actual_peers:?}"
    )
}
