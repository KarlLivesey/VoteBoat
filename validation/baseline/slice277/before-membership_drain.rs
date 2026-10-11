// SPDX-License-Identifier: RPL-1.5
// Copyright (c) 2026 Karl Livesey
use super::*;
use voteboat::{
    drain::*,
    membership::*,
    native::{administration::*, drain_journal::*, placement::*},
    placement::*,
};
fn plan(h: &History) -> MembershipDrainPlan {
    let original = h.core(0).membership().stable().clone();
    let target = Configuration::new(
        ConfigurationId::new(11).unwrap(),
        Policy::new(
            Tree::Majority(vec![
                Tree::Voter(NodeId::new(2).unwrap()),
                Tree::Voter(NodeId::new(3).unwrap()),
            ]),
            Limits::default(),
        )
        .unwrap(),
        original
            .voter_stores()
            .iter()
            .filter(|(node, _)| node.get() != 1)
            .map(|(n, s)| (*n, *s))
            .collect(),
        [(
            NodeId::new(1).unwrap(),
            original.voter_stores()[&NodeId::new(1).unwrap()],
        )]
        .into(),
    )
    .unwrap();
    let operation = OperationId::new(19720).unwrap();
    MembershipDrainPlan::new(
        PeerIdentity {
            node: NodeId::new(1).unwrap(),
            store: original.voter_stores()[&NodeId::new(1).unwrap()],
        },
        OperationId::new(19721).unwrap(),
        vec![DrainMembershipGroup {
            group: group(),
            handoff: PeerIdentity {
                node: NodeId::new(2).unwrap(),
                store: original.voter_stores()[&NodeId::new(2).unwrap()],
            },
            original,
            change: PlannedVoterChange {
                joint: ConfigurationRecord {
                    operation,
                    expected: ConfigurationId::new(9).unwrap(),
                    change: ConfigurationChange::Joint {
                        id: ConfigurationId::new(10).unwrap(),
                        next: target,
                    },
                },
                finalize: ConfigurationRecord {
                    operation,
                    expected: ConfigurationId::new(10).unwrap(),
                    change: ConfigurationChange::Final {
                        id: ConfigurationId::new(11).unwrap(),
                    },
                },
            },
        }],
    )
    .unwrap()
}
fn authorize(h: &mut History, plan: &MembershipDrainPlan) {
    let placement = NativePlacementAuthorizer::new(
        group(),
        h.bootstrap
            .voter_stores
            .iter()
            .map(|(node, store)| {
                (
                    *node,
                    ReplicaPlacement {
                        store: *store,
                        domain: FailureDomainId::new(node.get()).unwrap(),
                    },
                )
            })
            .collect(),
        PlacementRequirements {
            minimum_voting_domains: 2,
            survive_any_single_domain_loss: false,
        },
    )
    .ok()
    .unwrap();
    h.administration = Some(
        NativeAdministrationPlan::new(
            group(),
            placement,
            h.app(0).deployment_requirements().unwrap(),
            vec![
                plan.groups()[0].change.joint.clone(),
                plan.groups()[0].change.finalize.clone(),
            ],
        )
        .unwrap(),
    );
}
fn journal(h: &History, plan: &MembershipDrainPlan, create: bool) -> NativeDrainJournal {
    let io = FileDrainRecord::new(h.directory.join("MEMBERSHIP_DRAIN"));
    let owner = plan.record(1).unwrap().owner;
    if create {
        NativeDrainJournal::initialize(io, owner)
    } else {
        NativeDrainJournal::recover(io, owner)
    }
    .ok()
    .unwrap()
}
fn propose(
    h: &mut History,
    plan: &MembershipDrainPlan,
    journal: &NativeDrainJournal,
    lose_reply: bool,
) {
    let MembershipDrainAction::Configure(record) = plan.next(journal, h.core(1)).unwrap() else {
        panic!("expected original configuration action");
    };
    let requirements = h.app(1).deployment_requirements().unwrap();
    let ticket = h.nodes[1]
        .configure(ConfigurationRequest {
            group: group(),
            proposal: ConfigurationProposal {
                record,
                readiness: vec![],
                requirements,
            },
        })
        .unwrap();
    if lose_reply {
        h.nodes[1].cancel_configuration(ticket).unwrap();
    }
    let mut outcome = None;
    let deadline = Instant::now() + Duration::from_secs(10);
    while outcome.is_none() {
        h.poll();
        outcome = h.nodes[1].poll_configuration();
        assert!(Instant::now() < deadline);
    }
    let outcome = outcome.unwrap();
    assert_eq!(outcome.ticket, ticket);
    if lose_reply {
        assert!(matches!(
            outcome.outcome,
            ConfigurationOutcome::Unknown(ConfigurationUnknown::CancelledWait)
        ));
    } else {
        assert!(
            matches!(outcome.outcome, ConfigurationOutcome::Committed(_)),
            "{outcome:?}"
        );
    }
}
fn history(protocol: NativePeerProtocol, checkpoint: bool) {
    let mut h = History::new_mode(protocol, 8, true);
    h.elect(0);
    assert!(matches!(
        h.submit(
            0,
            OperationId::new(19722).unwrap(),
            Maintenance::<HostApplication>::data(&7i64.to_le_bytes(), 100).unwrap()
        ),
        ClientOutcome::Applied { .. }
    ));
    let plan = plan(&h);
    authorize(&mut h, &plan);
    let mut journal = journal(&h, &plan, true);
    journal.publish(plan.record(1).unwrap()).unwrap();
    assert_eq!(
        h.nodes[0].membership_drain_ready(&plan, &journal),
        Err(DrainPlanError::Unrestored)
    );
    h.nodes[0].restore_drain(&journal).unwrap();
    let MembershipDrainAction::Transfer(request) = plan.next(&journal, h.core(0)).unwrap() else {
        panic!("expected handoff");
    };
    h.nodes[0]
        .control(group(), NodeControl::TransferLeadership(request))
        .unwrap();
    h.wait("target current-term leader", |h| {
        h.core(1).role() == Role::Leader
            && h.core(1).state().term_at(h.core(1).state().commit_index)
                == Some(h.core(1).state().hard_state.term)
    });
    propose(&mut h, &plan, &journal, true);
    h.wait("joint committed everywhere", |h| {
        (0..3).all(|i| {
            h.core(i).membership().joint().is_some()
                && h.core(i).membership().last_configuration_index()
                    <= h.core(i).state().commit_index
        })
    });
    assert!(!h.nodes[0].membership_drain_ready(&plan, &journal).unwrap());
    h.reopen(checkpoint);
    journal = self::journal(&h, &plan, false);
    h.nodes[0].restore_drain(&journal).unwrap();
    h.elect(1);
    propose(&mut h, &plan, &journal, false);
    h.wait("final committed everywhere", |h| {
        (0..3).all(|i| {
            h.core(i).membership().id().get() == 11
                && h.core(i).membership().last_configuration_index()
                    <= h.core(i).state().commit_index
        })
    });
    h.wait("evacuated source ready", |h| {
        h.nodes[0].membership_drain_ready(&plan, &journal).unwrap()
    });
    h.reopen(checkpoint);
    journal = self::journal(&h, &plan, false);
    h.nodes[0].restore_drain(&journal).unwrap();
    h.wait("recovered evacuation ready", |h| {
        h.nodes[0].membership_drain_ready(&plan, &journal).unwrap()
    });
    assert!(!h.core(0).local_voter());
    assert_eq!(
        h.nodes[0]
            .read(group(), MaintenanceQuery::Data(()))
            .unwrap_err()
            .reason,
        ReadInvocationError::Draining
    );
    h.elect(1);
    let source = h.nodes.remove(0);
    drain_wire_nodes(vec![source], h.clock);
    assert!(matches!(
        h.submit(
            0,
            OperationId::new(19723).unwrap(),
            Maintenance::<HostApplication>::data(&3i64.to_le_bytes(), 100).unwrap()
        ),
        ClientOutcome::Applied { .. }
    ));
    assert!(matches!(
        h.submit(
            0,
            OperationId::new(19722).unwrap(),
            Maintenance::<HostApplication>::data(&7i64.to_le_bytes(), 100).unwrap()
        ),
        ClientOutcome::Applied {
            receipt: MaintenanceReceipt::Data(CounterReceipt {
                duplicate: true,
                ..
            }),
            ..
        }
    ));
    h.close();
}
#[test]
fn membership_drain_resumes_original_joint_and_final_over_tcp() {
    history(NativePeerProtocol::TcpTls, false);
}
#[cfg(feature = "quic")]
#[test]
fn membership_drain_resumes_original_joint_and_final_over_quic_checkpoints() {
    history(NativePeerProtocol::Quic, true);
}
