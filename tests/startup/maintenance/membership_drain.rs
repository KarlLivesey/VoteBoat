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
#[derive(Clone, Copy)]
enum Observation {
    Cancel,
    Observe,
    Campaign,
    Reconcile,
}
fn retained_joint_refusal(
    h: &History,
    plan: &MembershipDrainPlan,
    outcome: &ConfigurationOutcome,
) -> bool {
    let ConfigurationOutcome::NotProposed(RaftError::Configuration(error)) = outcome else {
        return false;
    };
    if !matches!(
        error.as_ref(),
        ConfigurationProposalError::TransportCapacity(voteboat::transport::TransportError::Wire(
            voteboat::wire::WireError::InvalidMessage("configuration admission journal")
        ))
    ) {
        return false;
    }
    let original = &plan.groups()[0].change.joint;
    let core = h.core(1);
    if !core
        .membership()
        .joint()
        .is_some_and(|joint| joint.operation == original.operation)
    {
        return false;
    }
    let Some(entry) = core.state().entries.iter().find(|entry|
        matches!(&entry.payload, EntryPayload::Configuration(record) if record.as_ref() == original))
    else {
        return false;
    };
    eprintln!(
        "reconciliation refused duplicate original joint {:?} at {} term {}, commit {}",
        original.operation,
        entry.index,
        entry.term,
        core.state().commit_index
    );
    true
}
fn interrupt_final(h: &mut History, operation: OperationId) {
    let deadline = Instant::now() + Duration::from_secs(10);
    loop {
        let now = MonoTime(h.clock.elapsed().as_millis() as u64);
        let administration = h.administration.as_ref().unwrap();
        h.nodes[1]
            .poll_with_configuration_authorization(now, NodePollBudget::default(), |core, p| {
                administration.authorize(group(), core.membership(), p)
            })
            .unwrap();
        let core = h.core(1);
        let status = core.configuration_status(operation).unwrap();
        if matches!(status.accepted, ConfigurationProgress::Final { configuration, index, .. }
            if configuration.get() == 11 && index > status.committed_index)
            && !core.has_pending_dependency()
        {
            assert!(
                (0..3).all(|i| h.core(i).membership().last_configuration_index()
                    > h.core(i).state().commit_index
                    || h.core(i).membership().id().get() != 11)
            );
            h.nodes[1].control(group(), NodeControl::Campaign).unwrap();
            return;
        }
        assert!(
            Instant::now() < deadline,
            "original final must become durable before campaign"
        );
        std::thread::park_timeout(Duration::from_millis(1));
    }
}
fn propose(
    h: &mut History,
    plan: &MembershipDrainPlan,
    journal: &NativeDrainJournal,
    observation: Observation,
) {
    let MembershipDrainAction::Configure(record) = plan.next(journal, h.core(1)).unwrap() else {
        panic!("expected original configuration action");
    };
    let operation = record.operation;
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
    match observation {
        Observation::Cancel => h.nodes[1].cancel_configuration(ticket).unwrap(),
        Observation::Campaign => interrupt_final(h, operation),
        Observation::Observe | Observation::Reconcile => {}
    }
    let mut outcome = None;
    let mut executed = !matches!(observation, Observation::Cancel);
    let deadline = Instant::now() + Duration::from_secs(10);
    while outcome.is_none() || !executed {
        h.poll_observed(|step| {
            if step.admission == Some(ticket.admission()) {
                assert_eq!(step.operation, Some(ticket.operation()));
                assert!(step.proposed.is_some() || step.error.is_some());
                executed = true;
            }
        });
        if outcome.is_none() {
            outcome = h.nodes[1].poll_configuration();
        }
        assert!(Instant::now() < deadline);
    }
    let outcome = outcome.unwrap();
    assert_eq!(outcome.ticket, ticket);
    let valid = match observation {
        Observation::Cancel => matches!(
            outcome.outcome,
            ConfigurationOutcome::Unknown(ConfigurationUnknown::CancelledWait)
        ),
        Observation::Campaign => matches!(
            outcome.outcome,
            ConfigurationOutcome::Unknown(ConfigurationUnknown::LeadershipChanged)
        ),
        Observation::Observe | Observation::Reconcile => {
            matches!(
                outcome.outcome,
                ConfigurationOutcome::Committed(_)
                    | ConfigurationOutcome::Unknown(ConfigurationUnknown::LeadershipChanged)
            ) || (matches!(observation, Observation::Reconcile)
                && retained_joint_refusal(h, plan, &outcome.outcome))
        }
    };
    assert!(valid, "{outcome:?}");
}
fn final_committed(h: &History, compacted: bool) -> bool {
    (0..3).all(|i| {
        let core = h.core(i);
        let status = core
            .configuration_status(OperationId::new(19720).unwrap())
            .unwrap();
        core.membership().id().get() == 11
            && core.membership().last_configuration_index() <= core.state().commit_index
            && match status.committed {
                ConfigurationProgress::Final { configuration, .. } => configuration.get() == 11,
                ConfigurationProgress::CompactedCompleted { .. } => compacted,
                _ => false,
            }
    })
}
fn reconcile_joint(h: &mut History, plan: &MembershipDrainPlan, journal: &NativeDrainJournal) {
    let deadline = Instant::now() + Duration::from_secs(10);
    let original = &plan.groups()[0].change.joint;
    let mut actions = 0;
    loop {
        assert!(
            Instant::now() < deadline,
            "original joint reconciliation timed out"
        );
        if (0..3).all(|i| {
            matches!(h.core(i).configuration_status(original.operation).unwrap().committed,
                ConfigurationProgress::Joint { configuration, target, .. }
                if configuration.get() == 10 && target.get() == 11)
                && h.core(i)
                    .membership()
                    .joint()
                    .is_some_and(|joint| joint.operation == original.operation)
        }) {
            return;
        }
        if h.core(1).role() != Role::Leader {
            actions += 1;
            assert!(actions <= 4, "original joint repeatedly lost leadership");
            h.elect(1);
        }
        if let MembershipDrainAction::Configure(record) = plan.next(journal, h.core(1)).unwrap() {
            if &record == original {
                actions += 1;
                assert!(actions <= 4, "original joint repeatedly lost observation");
                propose(h, plan, journal, Observation::Reconcile);
            } else {
                assert_eq!(record, plan.groups()[0].change.finalize);
            }
        }
        h.poll();
    }
}
fn history(protocol: NativePeerProtocol, checkpoint: bool, final_observation: Observation) {
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
    propose(&mut h, &plan, &journal, Observation::Cancel);
    reconcile_joint(&mut h, &plan, &journal);
    assert!(!h.nodes[0].membership_drain_ready(&plan, &journal).unwrap());
    h.reopen(checkpoint);
    journal = self::journal(&h, &plan, false);
    h.nodes[0].restore_drain(&journal).unwrap();
    h.elect(1);
    propose(&mut h, &plan, &journal, final_observation);
    h.wait("original final committed everywhere", |h| {
        final_committed(h, false)
    });
    h.wait("evacuated source ready", |h| {
        h.nodes[0].membership_drain_ready(&plan, &journal).unwrap()
    });
    h.reopen(checkpoint);
    journal = self::journal(&h, &plan, false);
    h.nodes[0].restore_drain(&journal).unwrap();
    h.wait("recovered original final everywhere", |h| {
        final_committed(h, true)
    });
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
    history(NativePeerProtocol::TcpTls, false, Observation::Observe);
}
#[cfg(feature = "quic")]
#[test]
fn membership_drain_resumes_original_joint_and_final_over_quic_checkpoints() {
    history(NativePeerProtocol::Quic, true, Observation::Observe);
}
#[test]
fn final_leadership_uncertainty_keeps_original_tcp_evacuating_operation() {
    history(NativePeerProtocol::TcpTls, false, Observation::Campaign);
}
#[cfg(feature = "quic")]
#[test]
fn final_leadership_uncertainty_keeps_original_quic_checkpoint_operation() {
    history(NativePeerProtocol::Quic, true, Observation::Campaign);
}
