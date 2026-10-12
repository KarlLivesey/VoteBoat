// SPDX-License-Identifier: RPL-1.5
// Copyright (c) 2026 Karl Livesey
use super::{aliases::Middle, channels::Channels, lower::Lower, parts, setup};
use std::time::{Duration, Instant};
use voteboat::{
    application::StateMachine, logical_voter::*, native::connect::NativePeerProtocol,
    outbound::OutboundQueue, raft::Role,
};
pub fn build(
    images: &[VerifiedLogicalImage],
    protocol: NativePeerProtocol,
    lower: &mut Lower<'_>,
) -> (Vec<Vec<Middle>>, Vec<Channels>) {
    let mut cohorts = Vec::new();
    let mut channels = Vec::new();
    for slot in 0..3 {
        let selected = &images[slot * 3..slot * 3 + 3];
        let descriptors = selected
            .iter()
            .enumerate()
            .map(|(i, image)| {
                LogicalPeerDescriptor::from_verified(image, &setup::lower_app(slot * 3 + i))
                    .unwrap()
            })
            .collect::<Vec<_>>();
        let mut originals = selected
            .iter()
            .map(|image| {
                parts::build(
                    image,
                    setup::upper_app(slot),
                    setup::lower_limits(),
                    lower.now(),
                )
            })
            .collect::<Vec<_>>();
        let bindings = originals
            .iter()
            .map(|p| (p.local.owner.authorization(), p.local.outbound.binding()))
            .collect::<Vec<_>>();
        let mut transport = Channels::new(&descriptors, &bindings, protocol, lower.now());
        let nodes = originals
            .drain(..)
            .enumerate()
            .map(|(i, mut p)| {
                p.sessions = transport.take_sessions(i, &descriptors);
                LogicalNode::from_parts(
                    &selected[i],
                    p,
                    parts::node_limits(setup::lower_limits()),
                    lower.now(),
                )
                .unwrap_or_else(|e| panic!("middle original assembly: {:?}", e.reason))
            })
            .collect();
        cohorts.push(nodes);
        channels.push(transport);
    }
    (cohorts, channels)
}
pub fn elect(lower: &mut Lower<'_>, middle: &mut [Vec<Middle>]) {
    for cohort in &mut *middle {
        cohort[0].campaign().unwrap();
    }
    let end = Instant::now() + Duration::from_secs(15);
    while !ready(middle) {
        lower.poll_middle(middle);
        assert!(
            Instant::now() < end,
            "middle three logical quorums timeout: {:?}; lower {:?}",
            middle
                .iter()
                .enumerate()
                .map(|(i, ns)| ns
                    .iter()
                    .map(|n| {
                        let c = n.local().owner.core(setup::middle(i).group).unwrap();
                        (
                            c.local_node(),
                            c.role(),
                            c.state().hard_state.term,
                            c.state().last_index(),
                            c.state().commit_index,
                            n.local().applications[&setup::middle(i).group].applied_index(),
                            c.has_pending_dependency(),
                            n.work_is_drained(),
                        )
                    })
                    .collect::<Vec<_>>())
                .collect::<Vec<_>>(),
            lower.bridges.iter().map(|b| b.usage()).collect::<Vec<_>>()
        );
    }
}
fn ready(middle: &[Vec<Middle>]) -> bool {
    middle.iter().enumerate().all(|(i, cohort)| {
        let g = setup::upper_mapping(i).child;
        let c = cohort[0].local().owner.core(g).unwrap();
        c.role() == Role::Leader
            && !c.has_pending_dependency()
            && c.state().term_at(c.state().commit_index) == Some(c.state().hard_state.term)
            && cohort
                .iter()
                .all(|n| n.local().applications[&g].applied_index() >= c.state().commit_index)
    })
}
pub fn close(
    lower: &mut Lower<'_>,
    middle: Vec<Vec<Middle>>,
    channels: Vec<Channels>,
) -> Vec<voteboat::log::GroupLog> {
    let mut middle = middle;
    for n in middle.iter_mut().flatten() {
        n.close_intake().unwrap();
    }
    let end = Instant::now() + Duration::from_secs(15);
    while !middle.iter().flatten().all(Middle::is_drained) {
        lower.shutdown_middle(&mut middle);
        assert!(Instant::now() < end, "middle actual close timeout");
    }
    let mut states = Vec::new();
    for (index, (cohort, channels)) in middle.into_iter().zip(channels).enumerate() {
        let sessions = cohort
            .into_iter()
            .flat_map(|n| {
                states.push(
                    n.local()
                        .owner
                        .core(setup::middle(index).group)
                        .unwrap()
                        .state()
                        .clone(),
                );
                n.into_parts()
                    .unwrap_or_else(|_| panic!("middle original drain"))
                    .sessions
                    .into_values()
            })
            .collect::<Vec<_>>();
        channels.close(sessions, lower.now());
    }
    assert!(lower.drained());
    states
}
