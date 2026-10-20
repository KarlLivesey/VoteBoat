// SPDX-License-Identifier: RPL-1.5
// Copyright (c) 2026 Karl Livesey
//! Current authenticated origin, not helper/catalog, binds original tracked work.
use super::*;
use voteboat::{
    membership::*,
    raft::{ConfigurationProposal, ConfigurationProposalError, RaftError, ReadinessRequirements},
};
pub(super) fn source_and_original_controls(c: &mut cluster::Cluster) {
    let end = Instant::now() + Duration::from_secs(15);
    while c.boats[0]
        .activation_peer_binding(support::node(2))
        .is_none()
        || c.boats[1]
            .activation_peer_binding(support::node(1))
            .is_none()
    {
        c.poll();
        assert!(
            Instant::now() < end,
            "actual current relay source views become available"
        );
    }
    let source = c.boats[0]
        .peers()
        .unwrap()
        .source_view(support::node(2))
        .unwrap();
    assert_eq!(source.source().node, support::node(2));
    assert_eq!(source.receiving_access().peer.node, support::node(9));
    assert!(source.direct_access().is_none());
    assert_eq!(
        c.boats[0].activation_peer_binding(support::node(2)),
        Some(source.source().store)
    );
    assert!(c.boats[0]
        .activation_peer_binding(support::node(9))
        .is_none());
    assert!(c.boats[0]
        .activation_readiness(setup::child().group, support::node(2))
        .is_none());
    let original = c.boats[1].local().owner.usage();
    assert_eq!(
        c.boats[1].cancel_activation_readiness(setup::mapping().parent),
        Err(LogicalBackingPollError::Node(Box::new(NodeError::Owner(
            EffectOwnerError::Runtime(RuntimeError::UnknownGroup)
        ))))
    );
    assert_eq!(c.boats[1].local().owner.usage(), original);
    let before = c.boats[1]
        .local()
        .owner
        .core(setup::child().group)
        .unwrap()
        .pending_learner_readiness();
    let ticket = c.boats[1]
        .request_activation_readiness(
            setup::child().group,
            support::node(1),
            ReadinessRequirements {
                application_schema: voteboat::logical_voter::LOGICAL_APPLICATION_SCHEMA,
                identity: voteboat::raft::LayerIdentity::family(
                    voteboat::raft::ApplicationFamily::LOGICAL_VOTER,
                ),
                inner_profile: voteboat::raft::InnerProfile::LEAF,
                command_bytes: 1,
                snapshot_bytes: 1,
            },
        )
        .unwrap();
    assert_eq!(ticket.owner, c.boats[1].local().owner.identity());
    assert_eq!(
        c.boats[1]
            .local()
            .owner
            .core(setup::child().group)
            .unwrap()
            .pending_learner_readiness(),
        before
    );
    let step = tracked(c, 1, ticket);
    assert_eq!(step.error, Some(RaftError::NotLeader));
    let cancel = c.boats[1]
        .cancel_activation_readiness(setup::child().group)
        .unwrap();
    assert_eq!(cancel.owner, ticket.owner);
    assert_ne!(cancel, ticket);
    assert_eq!(tracked(c, 1, cancel).error, None);
    configure(c);
    assert!(
        !c.root.join("9").exists(),
        "helper owns no ballot/WAL/application store"
    );
    // The physical group remains exactly the same three ordinary voters.
    assert_eq!(
        c.boats[0]
            .local()
            .owner
            .core(setup::child().group)
            .unwrap()
            .membership()
            .stable()
            .voter_stores()
            .keys()
            .copied()
            .collect::<Vec<_>>(),
        [1, 2, 3].map(support::node)
    );
}
fn tracked(c: &mut cluster::Cluster, slot: usize, ticket: AdmissionTicket) -> OwnerStep {
    let end = Instant::now() + Duration::from_secs(15);
    loop {
        c.helper();
        let now = c.now();
        for (i, node) in c.boats.iter_mut().enumerate() {
            let progress = node.poll(now, NodePollBudget::default()).unwrap();
            if i == slot {
                if let Some(step) = progress
                    .replica
                    .unwrap()
                    .steps
                    .into_iter()
                    .find(|s| s.admission == Some(ticket))
                {
                    return step;
                }
            }
        }
        assert!(
            Instant::now() < end,
            "same original relay-backed tracked control executes"
        );
        std::thread::park_timeout(Duration::from_millis(1));
    }
}
fn configure(c: &mut cluster::Cluster) {
    let old = c.boats[0]
        .local()
        .owner
        .core(setup::child().group)
        .unwrap()
        .membership()
        .stable()
        .clone();
    let next = Configuration::new(
        ConfigurationId::new(2).unwrap(),
        old.policy().clone(),
        old.voter_stores().clone(),
        old.learners().clone(),
    )
    .unwrap();
    let ticket = c.boats[0]
        .configure_activation(ConfigurationRequest {
            group: setup::child().group,
            proposal: ConfigurationProposal {
                record: ConfigurationRecord {
                    operation: OperationId::new(371002).unwrap(),
                    expected: old.id(),
                    change: ConfigurationChange::Learners(next),
                },
                readiness: Vec::new(),
                requirements: ReadinessRequirements {
                    application_schema: voteboat::logical_voter::LOGICAL_APPLICATION_SCHEMA,
                    identity: voteboat::raft::LayerIdentity::family(
                        voteboat::raft::ApplicationFamily::LOGICAL_VOTER,
                    ),
                    inner_profile: voteboat::raft::InnerProfile::LEAF,
                    command_bytes: setup::limits().max_command_bytes,
                    snapshot_bytes: setup::limits().max_child_checkpoint_bytes,
                },
            },
        })
        .unwrap();
    let end = Instant::now() + Duration::from_secs(15);
    loop {
        c.poll();
        if let Some(output) = c.boats[0].poll_activation_configuration() {
            assert_eq!(output.ticket, ticket);
            assert_eq!(
                output.outcome,
                ConfigurationOutcome::NotProposed(
                    ConfigurationProposalError::AuthenticationRequired.into()
                )
            );
            let core = c.boats[0].local().owner.core(setup::child().group).unwrap();
            assert_eq!(core.membership().stable(), &old);
            assert!(core.state().entries.iter().all(|entry| !matches!(
                &entry.payload,
                voteboat::log::EntryPayload::Configuration(record)
                    if record.operation == OperationId::new(371002).unwrap()
            )));
            break;
        }
        assert!(
            Instant::now() < end,
            "original selected-network configuration observer"
        );
    }
}
