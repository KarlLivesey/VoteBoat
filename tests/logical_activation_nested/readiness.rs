// SPDX-License-Identifier: RPL-1.5
// Copyright (c) 2026 Karl Livesey
//! Actual logical admission and original execution, without forged readiness.
use super::harness::{self, Observed};
use super::{lower::Lower, setup, support, Middle};
use std::time::Duration;
use voteboat::{logical_voter::*, raft::*, runtime::*};
pub fn cancellation(lower: &mut Lower<'_>, nodes: &mut [Middle]) {
    let wrong = setup::top().group;
    assert_eq!(
        nodes[1].cancel_learner_readiness_tracked(wrong),
        Err(LogicalNodeError::WrongBinding)
    );
    let core = nodes[1].local().owner.core(setup::middle().group).unwrap();
    let previous = core.pending_learner_readiness();
    let ticket = nodes[1]
        .request_learner_readiness_tracked(
            setup::middle().group,
            support::node(201),
            ReadinessRequirements {
                application_schema: 1,
                inner_profile: voteboat::raft::InnerProfile::LEAF,
                command_bytes: 1,
                snapshot_bytes: 1,
            },
        )
        .unwrap();
    assert_eq!(ticket.owner, nodes[1].local().owner.identity());
    assert_eq!(
        nodes[1]
            .local()
            .owner
            .core(ticket.group)
            .unwrap()
            .pending_learner_readiness(),
        previous
    );
    let step = original_step(lower, nodes, ticket);
    assert_eq!(step.admission, Some(ticket));
    assert_eq!(step.error, Some(RaftError::NotLeader));
    let cancel = nodes[1]
        .cancel_learner_readiness_tracked(ticket.group)
        .unwrap();
    assert_ne!(cancel, ticket);
    assert_eq!(cancel.owner, ticket.owner);
    assert!(nodes[1]
        .local()
        .owner
        .core(ticket.group)
        .unwrap()
        .pending_learner_readiness()
        .is_none());
    let step = original_step(lower, nodes, cancel);
    assert_eq!(step.admission, Some(cancel));
    assert_eq!(step.error, None);
}
fn original_step(
    lower: &mut Lower<'_>,
    nodes: &mut [Middle],
    ticket: AdmissionTicket,
) -> OwnerStep {
    let end = harness::Wait::new(&lower.clock, harness::LIVENESS_MS);
    loop {
        lower.pump(nodes);
        lower.advance_with(Lower::observed(nodes, Observed::default()));
        lower.pump(nodes);
        let mut observed = None;
        for (slot, node) in nodes.iter_mut().enumerate() {
            let progress = node
                .poll(lower.now(), LogicalNodePollBudget::default())
                .unwrap();
            if slot == 1 {
                observed = progress
                    .steps
                    .into_iter()
                    .find(|step| step.admission == Some(ticket));
            }
        }
        if let Some(step) = observed {
            return step;
        }
        assert!(
            end.within(&lower.clock),
            "same logical tracked admission must execute"
        );
        std::thread::park_timeout(Duration::from_millis(1));
    }
}
pub fn closed(nodes: &mut [Middle]) {
    for node in nodes {
        assert_eq!(
            node.cancel_learner_readiness_tracked(setup::middle().group),
            Err(LogicalNodeError::Closed)
        );
        assert_eq!(
            node.request_learner_readiness_tracked(
                setup::middle().group,
                support::node(201),
                ReadinessRequirements {
                    application_schema: 1,
                    inner_profile: voteboat::raft::InnerProfile::LEAF,
                    command_bytes: 1,
                    snapshot_bytes: 1,
                }
            ),
            Err(LogicalNodeError::Closed)
        );
    }
}
