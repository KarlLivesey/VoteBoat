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

use super::*;
use super::{
    peer_driver::{parts_for, Connector, Factory},
    snapshot_routes::Worker,
};
use support::outbound::HostOutbound;
use voteboat::outbound::*;
type Parts = NodeParts<
    Ready,
    Timers,
    Entropy,
    Counter,
    HostWorker,
    HostOutbound,
    Worker,
    Connector,
    Factory,
>;
type Boat = voteboat::runtime::Node<
    Ready,
    Timers,
    Entropy,
    Counter,
    HostWorker,
    HostOutbound,
    Worker,
    Connector,
    Factory,
>;
fn parts(voters: u64, networking: bool) -> Parts {
    let local = super::replica_driver::local_parts(voters);
    let peers = networking.then(|| parts_for(local.owner.identity(), local.outbound.binding()));
    NodeParts { local, peers }
}
fn boat(parts: Parts) -> Boat {
    Boat::from_parts(parts, NodeLimits::default(), MonoTime(0))
        .unwrap_or_else(|r| panic!("{:?}", r.reason))
}
fn settle(n: &mut Boat) {
    for _ in 0..100 {
        n.poll(MonoTime(0), NodePollBudget::default()).unwrap();
        if n.local().owner.is_drained()
            && n.local().persistence.is_drained()
            && n.local().outbound.is_drained()
            && n.replica_usage().leases() == 0
        {
            return;
        }
    }
    panic!("not settled");
}
fn elected() -> Boat {
    let mut n = boat(parts(1, false));
    n.control(group(1), NodeControl::Campaign).unwrap();
    settle(&mut n);
    n
}
fn admin_request(group_id: u128, operation: u128, finalizing: bool) -> ConfigurationRequest {
    use voteboat::membership::*;
    let b = bootstrap(group_id, 1);
    ConfigurationRequest {
        group: group(group_id),
        proposal: ConfigurationProposal {
            record: ConfigurationRecord {
                operation: OperationId::new(operation).unwrap(),
                expected: ConfigurationId::new(if finalizing { 2 } else { 1 }).unwrap(),
                change: if finalizing {
                    ConfigurationChange::Final {
                        id: ConfigurationId::new(3).unwrap(),
                    }
                } else {
                    ConfigurationChange::Joint {
                        id: ConfigurationId::new(2).unwrap(),
                        next: Configuration::new(
                            ConfigurationId::new(3).unwrap(),
                            b.policy,
                            b.voter_stores,
                            Default::default(),
                        )
                        .unwrap(),
                    }
                },
            },
            readiness: vec![],
            requirements: ReadinessRequirements {
                application_schema: 1,
                command_bytes: 8,
                snapshot_bytes: 4096,
            },
        },
    }
}
fn admin_settle(n: &mut Boat) {
    for _ in 0..100 {
        let p = n
            .poll_with_configuration_authorization(
                MonoTime(0),
                NodePollBudget::default(),
                |_, _| Ok(()),
            )
            .unwrap();
        for step in p.replica.unwrap().steps {
            assert!(step.error.is_none(), "{:?}", step.error);
        }
        if n.local().owner.is_drained()
            && n.local().persistence.is_drained()
            && n.replica_usage().leases() == 0
        {
            return;
        }
    }
    panic!("administration did not settle");
}
#[test]
fn configuration_receipts_wait_for_durability_and_joint_then_final_commit() {
    let mut n = elected();
    let ticket = n.configure(admin_request(1, 100, false)).unwrap();
    let p = n
        .poll_with_configuration_authorization(
            MonoTime(0),
            NodePollBudget {
                replica: ReplicaPollBudget {
                    effects: 1,
                    ..Default::default()
                },
                ..Default::default()
            },
            |_, _| Ok(()),
        )
        .unwrap();
    let step = p
        .replica
        .unwrap()
        .steps
        .into_iter()
        .find(|s| s.admission == Some(ticket.admission()))
        .unwrap();
    assert_eq!(step.operation, Some(ticket.operation()));
    assert!(step.proposed.is_some());
    assert!(n.poll_configuration().is_none());
    assert_eq!(
        n.local().owner.core(group(1)).unwrap().state().commit_index,
        1
    );
    assert_eq!(
        n.configure(admin_request(1, 100, true)).unwrap_err().reason,
        ConfigurationRequestError::Overloaded
    );
    admin_settle(&mut n);
    let committed = n.poll_configuration().unwrap();
    assert_eq!(committed.ticket, ticket);
    assert_eq!(
        committed.outcome,
        ConfigurationOutcome::Committed(ProposalPosition { index: 2, term: 1 })
    );
    let ticket = n.configure(admin_request(1, 100, true)).unwrap();
    admin_settle(&mut n);
    assert_eq!(n.poll_configuration().unwrap().ticket, ticket);
    assert_eq!(
        n.local().owner.core(group(1)).unwrap().membership().id(),
        ConfigurationId::new(3).unwrap()
    );
    // Administration uses the same polling path as application admission.
    n.propose(request(500)).unwrap();
    admin_settle(&mut n);
    let reply = n.poll_client().unwrap();
    assert!(matches!(
        n.complete_client(reply).unwrap(),
        ClientOutcome::Applied { .. }
    ));
    shutdown(&mut n);
}
#[test]
fn queued_configuration_rechecks_authorization_and_default_poll_denies_it() {
    for default_poll in [false, true] {
        let mut n = elected();
        let before = n.local().owner.core(group(1)).unwrap().state().clone();
        let ticket = n.configure(admin_request(1, 100, false)).unwrap();
        let mut checks = 0;
        if default_poll {
            n.poll(MonoTime(0), NodePollBudget::default()).unwrap();
        } else {
            n.poll_with_configuration_authorization(
                MonoTime(0),
                NodePollBudget::default(),
                |core, proposal| {
                    checks += 1;
                    assert_eq!(core.state(), &before);
                    assert_eq!(proposal.record.operation, ticket.operation());
                    Err(ConfigurationProposalError::AuthenticationRequired)
                },
            )
            .unwrap();
        }
        assert_eq!(checks, usize::from(!default_poll));
        let result = n.poll_configuration().unwrap();
        assert_eq!(result.ticket, ticket);
        assert_eq!(
            result.outcome,
            ConfigurationOutcome::NotProposed(
                ConfigurationProposalError::AuthenticationRequired.into()
            )
        );
        assert_eq!(n.local().owner.core(group(1)).unwrap().state(), &before);
        assert_eq!(n.state(), NodeState::Running);
        shutdown(&mut n);
    }
}
#[test]
fn configuration_cancellation_is_unknown_and_does_not_undo_queued_work() {
    let mut n = elected();
    let ticket = n.configure(admin_request(1, 100, false)).unwrap();
    n.cancel_configuration(ticket).unwrap();
    assert_eq!(
        n.poll_configuration().unwrap().outcome,
        ConfigurationOutcome::Unknown(ConfigurationUnknown::CancelledWait)
    );
    assert_eq!(
        n.cancel_configuration(ticket),
        Err(ConfigurationRequestError::StaleTicket)
    );
    admin_settle(&mut n);
    assert_eq!(
        n.local().owner.core(group(1)).unwrap().state().commit_index,
        2
    );
    assert!(n.poll_configuration().is_none());
    shutdown(&mut n);
}
#[test]
fn configuration_shutdown_and_abort_preserve_unknown_outputs_and_ownership() {
    for abort in [false, true] {
        let mut n = elected();
        let ticket = n.configure(admin_request(1, 100, false)).unwrap();
        if abort {
            n.abort();
        } else {
            n.begin_shutdown();
        }
        assert_eq!(
            n.configure(admin_request(2, 101, false))
                .unwrap_err()
                .reason,
            ConfigurationRequestError::Closed
        );
        let result = n.poll_configuration().unwrap();
        assert_eq!(result.ticket, ticket);
        assert_eq!(
            result.outcome,
            ConfigurationOutcome::Unknown(if abort {
                ConfigurationUnknown::OwnerFailed
            } else {
                ConfigurationUnknown::Shutdown
            })
        );
        if abort {
            let recovered = n.into_recovery().unwrap_or_else(|_| panic!());
            assert!(recovered.configuration.is_drained());
        } else {
            shutdown(&mut n);
        }
    }
}
#[test]
fn configuration_limits_reject_with_original_request_and_no_core_mutation() {
    let p = parts(1, false);
    let mut n = Boat::from_parts(
        p,
        NodeLimits {
            configuration: ConfigurationRequestLimits {
                requests: 1,
                bytes: 1,
            },
            ..Default::default()
        },
        MonoTime(0),
    )
    .unwrap_or_else(|r| panic!("{:?}", r.reason));
    let before = n.local().owner.core(group(1)).unwrap().state().clone();
    let request = admin_request(1, 100, false);
    let record = request.proposal.record.clone();
    let rejection = n.configure(request).unwrap_err();
    assert_eq!(rejection.reason, ConfigurationRequestError::Overloaded);
    assert_eq!(rejection.request.proposal.record, record);
    assert_eq!(n.local().owner.core(group(1)).unwrap().state(), &before);
    shutdown(&mut n);
}
#[test]
fn configuration_written_storage_failure_retains_unknown_receipt_for_recovery() {
    let mut p = parts(1, false);
    p.local.owner.admit(group(1), Event::Campaign).unwrap();
    pump(
        &mut p.local.owner,
        &mut p.local.persistence,
        &mut p.local.applications,
        MonoTime(0),
    );
    p.local.persistence.fail = true;
    let mut n = boat(p);
    let ticket = n.configure(admin_request(1, 100, false)).unwrap();
    let mut saw_written = false;
    for _ in 0..100 {
        match n.poll_with_configuration_authorization(
            MonoTime(0),
            NodePollBudget::default(),
            |_, _| Ok(()),
        ) {
            Ok(p) => {
                saw_written |= p.replica.unwrap().worker_events > 0;
                assert!(n.poll_configuration().is_none());
            }
            Err(_) => break,
        }
    }
    assert!(saw_written);
    assert_eq!(n.state(), NodeState::RecoveryRequired);
    assert_eq!(
        n.configuration_status(group(1), ticket.operation()),
        Err(NodeError::RecoveryRequired)
    );
    let mut recovery = n.into_recovery().unwrap_or_else(|_| panic!());
    let result = recovery.configuration.poll().unwrap();
    assert_eq!(result.ticket, ticket);
    assert_eq!(
        result.outcome,
        ConfigurationOutcome::Unknown(ConfigurationUnknown::OwnerFailed)
    );
    assert_eq!(
        recovery
            .local
            .persistence
            .store
            .state(group(1))
            .unwrap()
            .commit_index,
        1
    );
}
#[test]
fn configuration_committed_output_survives_shutdown_and_request_count_stays_bounded() {
    let mut n = elected();
    let ticket = n.configure(admin_request(1, 100, false)).unwrap();
    admin_settle(&mut n);
    assert_eq!(
        n.configure(admin_request(1, 100, true)).unwrap_err().reason,
        ConfigurationRequestError::Overloaded
    );
    n.begin_shutdown();
    n.poll(MonoTime(0), NodePollBudget::default()).unwrap();
    assert_eq!(n.state(), NodeState::Quiescing);
    let result = n.poll_configuration().unwrap();
    assert_eq!(result.ticket, ticket);
    assert!(matches!(result.outcome, ConfigurationOutcome::Committed(_)));
    shutdown(&mut n);
}
#[test]
fn configuration_rejects_static_wire_before_persistence_even_with_host_authorization() {
    let mut n = boat(parts(3, true));
    let before = n.local().owner.core(group(1)).unwrap().state().clone();
    let ticket = n.configure(admin_request(1, 100, false)).unwrap();
    n.poll_with_configuration_authorization(MonoTime(0), NodePollBudget::default(), |_, _| Ok(()))
        .unwrap();
    let result = n.poll_configuration().unwrap();
    assert_eq!(result.ticket, ticket);
    assert_eq!(
        result.outcome,
        ConfigurationOutcome::NotProposed(
            ConfigurationProposalError::UnsupportedWireVersion(1).into()
        )
    );
    assert_eq!(n.local().owner.core(group(1)).unwrap().state(), &before);
    shutdown(&mut n);
}
#[test]
fn configuration_queued_behind_campaign_uses_its_actual_proposal_term() {
    let mut n = boat(parts(1, false));
    n.control(group(1), NodeControl::Campaign).unwrap();
    let ticket = n.configure(admin_request(1, 100, false)).unwrap();
    admin_settle(&mut n);
    let result = n.poll_configuration().unwrap();
    assert_eq!(result.ticket, ticket);
    assert_eq!(
        result.outcome,
        ConfigurationOutcome::Committed(ProposalPosition { index: 2, term: 1 })
    );
    shutdown(&mut n);
}
#[test]
fn configuration_cannot_expand_a_local_only_node_without_peer_providers() {
    use voteboat::membership::*;
    let mut n = elected();
    let before = n.local().owner.core(group(1)).unwrap().state().clone();
    let mut request = admin_request(1, 100, false);
    let b = bootstrap(1, 1);
    request.proposal.record.change = ConfigurationChange::Learners(
        Configuration::new(
            ConfigurationId::new(2).unwrap(),
            b.policy,
            b.voter_stores,
            [(node(2), identity(2))].into(),
        )
        .unwrap(),
    );
    n.configure(request).unwrap();
    n.poll_with_configuration_authorization(MonoTime(0), NodePollBudget::default(), |_, _| Ok(()))
        .unwrap();
    assert_eq!(
        n.poll_configuration().unwrap().outcome,
        ConfigurationOutcome::NotProposed(ConfigurationProposalError::MissingPeerTransport.into())
    );
    assert_eq!(n.local().owner.core(group(1)).unwrap().state(), &before);
    shutdown(&mut n);
}
#[test]
fn configuration_status_resumes_a_lost_joint_reply_through_fresh_authorization() {
    use voteboat::membership::*;
    let mut n = elected();
    let request = admin_request(1, 100, false);
    let operation = request.proposal.record.operation;
    let requirements = request.proposal.requirements;
    let ticket = n.configure(request).unwrap();
    n.cancel_configuration(ticket).unwrap();
    assert!(matches!(
        n.poll_configuration().unwrap().outcome,
        ConfigurationOutcome::Unknown(_)
    ));
    admin_settle(&mut n);
    let status = n.configuration_status(group(1), operation).unwrap();
    assert_eq!(
        status.committed,
        ConfigurationProgress::Joint {
            configuration: ConfigurationId::new(2).unwrap(),
            target: ConfigurationId::new(3).unwrap(),
            index: 2,
            term: Some(1)
        }
    );
    let ConfigurationResumption::Submitted(final_ticket) = n
        .resume_configuration(group(1), operation, requirements)
        .unwrap()
    else {
        panic!()
    };
    n.poll(MonoTime(0), NodePollBudget::default()).unwrap();
    let denied = n.poll_configuration().unwrap();
    assert_eq!(denied.ticket, final_ticket);
    assert!(matches!(
        denied.outcome,
        ConfigurationOutcome::NotProposed(RaftError::Configuration(_))
    ));
    assert_eq!(n.configuration_status(group(1), operation).unwrap(), status);
    let ConfigurationResumption::Submitted(final_ticket) = n
        .resume_configuration(group(1), operation, requirements)
        .unwrap()
    else {
        panic!()
    };
    assert_ne!(ticket.admission(), final_ticket.admission());
    assert_eq!(ticket.operation(), final_ticket.operation());
    admin_settle(&mut n);
    assert!(matches!(
        n.poll_configuration().unwrap().outcome,
        ConfigurationOutcome::Committed(_)
    ));
    let before = n.local().owner.core(group(1)).unwrap().state().clone();
    assert_eq!(
        n.resume_configuration(group(1), operation, requirements)
            .unwrap(),
        ConfigurationResumption::Completed
    );
    assert_eq!(
        n.resume_configuration(group(1), OperationId::new(999).unwrap(), requirements)
            .unwrap(),
        ConfigurationResumption::NotFoundLocally
    );
    assert_eq!(n.local().owner.core(group(1)).unwrap().state(), &before);
    n.begin_shutdown();
    assert_eq!(
        n.resume_configuration(group(1), operation, requirements),
        Err(NodeError::Closed)
    );
    shutdown(&mut n);
}
fn request(op: u128) -> ClientRequest {
    ClientRequest {
        group: group(1),
        operation: OperationId::new(op).unwrap(),
        bytes: 7i64.to_le_bytes().to_vec(),
    }
}
fn shutdown(n: &mut Boat) {
    n.begin_shutdown();
    for _ in 0..100 {
        n.poll(MonoTime(0), NodePollBudget::default()).unwrap();
        if n.is_drained() {
            return;
        }
    }
    panic!("not shut down");
}
#[test]
fn membership_routes_are_preflighted_and_rejections_return_owned_routes() {
    use voteboat::{connect::ConnectDirection, runtime::PeerDriverError};
    let mut n = boat(parts(3, true));
    let current = [
        (node(2), ConnectDirection::Accept),
        (node(3), ConnectDirection::Dial(())),
    ]
    .into();
    assert_eq!(
        n.preflight_peer_event(group(1), &Event::Heartbeat, &current, MonoTime(100)),
        Ok(())
    );
    assert_eq!(
        n.preflight_peer_event(group(8), &Event::Heartbeat, &current, MonoTime(0)),
        Err(PeerDriverError::Assignments(
            voteboat::transport::PeerAssignmentsError::UnknownGroup
        ))
    );
    let rejected = n
        .reconcile_membership(Default::default(), MonoTime(0))
        .err()
        .unwrap();
    assert_eq!(rejected.reason, PeerDriverError::WrongBinding);
    assert!(rejected.routes.is_empty());
    let wrong = [
        (node(2), ConnectDirection::Accept),
        (node(4), ConnectDirection::Dial(())),
    ]
    .into_iter()
    .collect();
    let rejected = n.reconcile_membership(wrong, MonoTime(0)).err().unwrap();
    assert_eq!(rejected.reason, PeerDriverError::WrongBinding);
    assert_eq!(rejected.routes.len(), 2);
    let routes = [
        (node(2), ConnectDirection::Accept),
        (node(3), ConnectDirection::Dial(())),
    ]
    .into_iter()
    .collect();
    n.reconcile_membership(routes, MonoTime(0))
        .unwrap_or_else(|r| panic!("{:?}", r.reason));
    assert!(!n.local().owner.is_failed());
    n.begin_shutdown();
    assert_eq!(
        n.preflight_peer_event(group(1), &Event::Heartbeat, &current, MonoTime(0)),
        Err(PeerDriverError::NotQuiescent)
    );
    assert_eq!(
        n.reconcile_membership(Default::default(), MonoTime(0))
            .err()
            .unwrap()
            .reason,
        PeerDriverError::NotQuiescent
    );
    shutdown(&mut n);
}
#[test]
fn facade_exact_client_and_read_outputs_hold_shutdown_until_consumer_completion() {
    let mut n = elected();
    let ticket = n.propose(request(1)).unwrap();
    settle(&mut n);
    let write = n.poll_client().unwrap();
    assert_eq!(write.ticket(), ticket);
    let read_ticket = n.read(group(1), ()).unwrap();
    settle(&mut n);
    let read = n.poll_read().unwrap();
    assert_eq!(read.ticket(), read_ticket);
    n.begin_shutdown();
    n.poll(MonoTime(0), NodePollBudget::default()).unwrap();
    assert_eq!(n.state(), NodeState::Quiescing);
    assert_eq!(
        n.propose(request(2)).unwrap_err().reason,
        ClientError::Closed
    );
    assert_eq!(
        n.read(group(1), ()).unwrap_err().reason,
        ReadInvocationError::Closed
    );
    assert!(matches!(
        n.complete_client(write).unwrap(),
        ClientOutcome::Applied { .. }
    ));
    n.poll(MonoTime(0), NodePollBudget::default()).unwrap();
    assert_eq!(n.state(), NodeState::Quiescing);
    assert!(matches!(
        n.complete_read(read).unwrap(),
        ReadOutcome::Read { result: Ok(7), .. }
    ));
    shutdown(&mut n);
    assert!(n.local().persistence.closed);
    assert!(n.local().owner.deadline(group(1)).is_none());
    let p = n.into_parts().unwrap_or_else(|_| panic!("not reclaimable"));
    assert_eq!(p.local.applications[&group(1)].read_applied(2), Ok(7));
}
#[test]
fn constructor_requires_voter_store_authorization_and_returns_original_parts_before_poll() {
    let rejected = match Boat::from_parts(parts(3, false), NodeLimits::default(), MonoTime(0)) {
        Err(r) => r,
        Ok(_) => panic!("missing network"),
    };
    assert_eq!(rejected.reason, NodeError::MissingPeers);
    assert!(rejected.parts.local.owner.connection_budget().is_none());
    assert!(!rejected.parts.local.persistence.closed);
    let mut p = *rejected.parts;
    p.peers = Some(parts_for(
        p.local.owner.identity(),
        p.local.outbound.binding(),
    ));
    let n = boat(p);
    assert_eq!(n.state(), NodeState::Running);
    assert_eq!(
        n.local().owner.connection_budget().unwrap().limit(),
        n.peers().unwrap().roster().limits().peers
    );
    assert_eq!(
        n.local().owner.reserved_connection_peers().unwrap(),
        Some(2)
    );
    assert_eq!(n.peers().unwrap().usage().attempts, 0);
    let mut p = parts(3, true);
    let net = p.peers.as_mut().unwrap();
    use voteboat::transport::*;
    net.roster = PeerRoster::new(
        PeerRosterConfig {
            local: net.roster.local(),
            outbound: net.roster.outbound_binding(),
            first_generation: SecureSessionGeneration::new(1).unwrap(),
            last_generation: SecureSessionGeneration::new(100).unwrap(),
            wire_version: 1,
            limits: PeerRosterLimits {
                connect_timeout_ms: 10,
                ..Default::default()
            },
            transport_limits: TransportLimits::default(),
        },
        [(node(2), identity(99)), (node(3), identity(3))].into(),
        MonoTime(0),
    )
    .unwrap();
    let rejected = match Boat::from_parts(p, NodeLimits::default(), MonoTime(0)) {
        Err(r) => r,
        Ok(_) => panic!("wrong voter store"),
    };
    assert_eq!(rejected.reason, NodeError::WrongPeerStore);
    assert!(rejected.parts.local.owner.is_drained());
}
#[test]
fn constructor_requires_connections_for_rollback_reachable_recovered_learners() {
    use voteboat::membership::*;
    let mut p = parts(1, false);
    let id = p.local.owner.identity();
    let mut shard = Shard::new(
        id,
        ShardLimits {
            max_groups: 100,
            ..ShardLimits::default()
        },
        Ready(VecDeque::new()),
    )
    .unwrap();
    for g in [group(1), group(2)] {
        let mut state = p.local.owner.core(g).unwrap().state().clone();
        let config = |number, learners| {
            Configuration::new(
                ConfigurationId::new(number).unwrap(),
                state.bootstrap.policy.clone(),
                state.bootstrap.voter_stores.clone(),
                learners,
            )
            .unwrap()
        };
        state.entries = vec![
            LogEntry {
                index: 1,
                term: 1,
                payload: EntryPayload::Configuration(Box::new(ConfigurationRecord {
                    operation: OperationId::new(900).unwrap(),
                    expected: ConfigurationId::new(1).unwrap(),
                    change: ConfigurationChange::Learners(config(
                        2,
                        [(node(2), identity(2))].into(),
                    )),
                })),
            },
            LogEntry {
                index: 2,
                term: 1,
                payload: EntryPayload::Configuration(Box::new(ConfigurationRecord {
                    operation: OperationId::new(901).unwrap(),
                    expected: ConfigurationId::new(2).unwrap(),
                    change: ConfigurationChange::Learners(config(3, Default::default())),
                })),
            },
        ];
        state.hard_state.term = 1;
        state.commit_index = 1;
        let core = Raft::recover_member(node(1), id.store, state, LogLimits::default()).unwrap();
        assert_eq!(core.membership().replica_store(node(2)), None);
        assert!(core.connection_replicas(3).unwrap().contains_key(&node(2)));
        shard.register(core).unwrap();
    }
    let runtime = TimedShard::new(
        shard,
        Timers {
            owner: id,
            sequence: 0,
            entries: BTreeMap::new(),
        },
        Entropy(17),
        TimerConfig::default(),
        MonoTime(0),
    )
    .unwrap();
    p.local.owner = EffectOwner::new(
        runtime,
        p.local.persistence.binding(),
        EffectOwnerLimits::default(),
    )
    .unwrap();
    let rejected = Boat::from_parts(p, NodeLimits::default(), MonoTime(0))
        .err()
        .unwrap();
    assert_eq!(rejected.reason, NodeError::MissingPeers);
    assert!(rejected.parts.local.owner.is_drained());
    assert!(!rejected.parts.local.persistence.closed);
}

#[test]
fn learner_repair_requires_selected_wire_before_node_service_or_persistence() {
    for snapshot in [false, true] {
        for networking in [false, true] {
            let mut p = parts(1, networking);
            let id = p.local.owner.identity();
            let mut shard = Shard::new(
                id,
                ShardLimits {
                    max_groups: 100,
                    ..ShardLimits::default()
                },
                Ready(VecDeque::new()),
            )
            .unwrap();
            for g in [group(1), group(2)] {
                let state = p.local.owner.core(g).unwrap().state().clone();
                let core = Raft::recover(node(1), id.store, state, LogLimits::default()).unwrap();
                let core = if snapshot {
                    core.with_snapshot_joint_repair()
                } else {
                    core.with_batched_joint_repair()
                };
                shard.register(core).unwrap();
            }
            let runtime = TimedShard::new(
                shard,
                Timers {
                    owner: id,
                    sequence: 0,
                    entries: BTreeMap::new(),
                },
                Entropy(17),
                TimerConfig::default(),
                MonoTime(0),
            )
            .unwrap();
            p.local.owner = EffectOwner::new(
                runtime,
                p.local.persistence.binding(),
                EffectOwnerLimits::default(),
            )
            .unwrap();
            let rejected = Boat::from_parts(p, NodeLimits::default(), MonoTime(0))
                .err()
                .unwrap();
            assert_eq!(rejected.reason, NodeError::IncompatiblePeerProtocol);
            assert!(rejected.parts.local.owner.is_drained());
            assert!(!rejected.parts.local.persistence.closed);
        }
    }
}
#[test]
fn unsupported_snapshot_and_invalid_driver_limits_reject_before_any_service_or_io() {
    let mut p = parts(3, true);
    p.local.snapshots = None;
    assert!(matches!(
        Boat::from_parts(p, NodeLimits::default(), MonoTime(0)),
        Err(NodeRejected {
            reason: NodeError::MissingSnapshots,
            ..
        })
    ));
    let mut limits = NodeLimits::default();
    limits.replica.leases = 0;
    let rejected = match Boat::from_parts(parts(1, false), limits, MonoTime(0)) {
        Err(r) => r,
        Ok(_) => panic!("invalid limits"),
    };
    assert!(matches!(
        rejected.reason,
        NodeError::Replica(ReplicaError::InvalidLimits)
    ));
    assert!(!rejected.parts.local.persistence.closed);
    let mut p = *rejected.parts;
    p.local.snapshots = None;
    let mut n = boat(p);
    assert_eq!(
        n.control(group(1), NodeControl::Checkpoint),
        Err(NodeError::MissingSnapshots)
    );
    assert!(n.local().owner.is_drained());
    shutdown(&mut n);
}
#[test]
fn all_poll_budgets_and_time_validate_before_either_driver_performs_work() {
    let mut n = boat(parts(3, true));
    let b = NodePollBudget {
        replica: ReplicaPollBudget {
            effects: 0,
            ..Default::default()
        },
        ..Default::default()
    };
    assert_eq!(
        n.poll(MonoTime(0), b).unwrap_err(),
        NodeError::InvalidLimits
    );
    let b = NodePollBudget {
        peers: PeerDriverBudget {
            peer_visits: 4097,
            ..Default::default()
        },
        ..Default::default()
    };
    assert!(matches!(
        n.poll(MonoTime(0), b),
        Err(NodeError::Peer(PeerDriverError::InvalidLimits))
    ));
    assert_eq!(n.peers().unwrap().usage().attempts, 0);
    assert_eq!(n.local().persistence.usage().requests, 0);
    n.poll(MonoTime(1), NodePollBudget::default()).unwrap();
    assert_eq!(
        n.poll(MonoTime(0), NodePollBudget::default()).unwrap_err(),
        NodeError::TimeWentBack
    );
    assert_eq!(n.state(), NodeState::Running);
    n.abort();
}
#[test]
fn abort_after_written_returns_unknown_and_durable_worker_work_without_rollback_or_false_applied() {
    let mut n = elected();
    let ticket = n.propose(request(1)).unwrap();
    n.poll(MonoTime(0), NodePollBudget::default()).unwrap();
    n.poll(MonoTime(0), NodePollBudget::default()).unwrap();
    assert!(n.local().persistence.written.is_some());
    assert!(n.poll_client().is_none());
    n.abort();
    assert_eq!(n.state(), NodeState::RecoveryRequired);
    assert!(!n.is_drained());
    assert_eq!(
        n.poll(MonoTime(0), NodePollBudget::default()).unwrap_err(),
        NodeError::RecoveryRequired
    );
    let output = n.poll_client().unwrap();
    assert_eq!(output.ticket(), ticket);
    assert!(matches!(
        n.complete_client(output).unwrap(),
        ClientOutcome::Unknown(ClientUnknown::Aborted)
    ));
    let mut r = n
        .into_recovery()
        .unwrap_or_else(|_| panic!("missing recovery"));
    assert_eq!(r.reason, NodeError::Aborted);
    assert!(r.local.owner.is_failed());
    assert_eq!(r.local.applications[&group(1)].read_applied(1), Ok(0));
    let events = r.local.persistence.poll(1);
    assert!(matches!(events.as_slice(), [WorkerEvent::Durable { .. }]));
    assert!(r.local.persistence.is_drained());
    assert!(r.local.persistence.store.state(group(1)).unwrap().entries.iter().any(
        |e| matches!(e.payload,EntryPayload::Command {operation,..} if operation==ticket.operation)
    ));
}
#[test]
fn stalled_accepted_write_keeps_quiescing_until_abort_and_returns_retained_original_lease() {
    let mut p = parts(1, false);
    pump(
        &mut p.local.owner,
        &mut p.local.persistence,
        &mut p.local.applications,
        MonoTime(0),
    );
    // Explicit campaign first; pump at zero alone does not elect.
    p.local.owner.admit(group(1), Event::Campaign).unwrap();
    pump(
        &mut p.local.owner,
        &mut p.local.persistence,
        &mut p.local.applications,
        MonoTime(0),
    );
    p.local.persistence.reject = true;
    let mut n = boat(p);
    n.propose(request(1)).unwrap();
    n.poll(MonoTime(0), NodePollBudget::default()).unwrap();
    n.begin_shutdown();
    for _ in 0..3 {
        n.poll(MonoTime(0), NodePollBudget::default()).unwrap();
        assert_eq!(n.state(), NodeState::Quiescing);
    }
    assert_eq!(n.replica_usage().leases(), 1);
    n.abort();
    let output = n.poll_client().unwrap();
    assert!(matches!(
        n.complete_client(output).unwrap(),
        ClientOutcome::Unknown(_)
    ));
    let mut r = n
        .into_recovery()
        .unwrap_or_else(|_| panic!("missing recovery"));
    assert_eq!(r.replica.usage().leases(), 1);
    r.replica.discard_failed(&mut r.local.as_parts()).unwrap();
    assert_eq!(r.local.owner.usage().reserved_bytes, 0);
}
#[test]
fn provider_failure_fences_node_and_returns_unknown_with_original_selected_resources() {
    let mut p = parts(1, false);
    p.local.owner.admit(group(1), Event::Campaign).unwrap();
    pump(
        &mut p.local.owner,
        &mut p.local.persistence,
        &mut p.local.applications,
        MonoTime(0),
    );
    p.local.persistence.fail = true;
    let mut n = boat(p);
    n.propose(request(1)).unwrap();
    let mut failed = false;
    for _ in 0..10 {
        if n.poll(MonoTime(0), NodePollBudget::default()).is_err() {
            failed = true;
            break;
        }
    }
    assert!(failed);
    assert_eq!(n.state(), NodeState::RecoveryRequired);
    assert!(n.local().owner.is_failed());
    let output = n.poll_client().unwrap();
    assert!(matches!(
        n.complete_client(output).unwrap(),
        ClientOutcome::Unknown(_)
    ));
    let r = n
        .into_recovery()
        .unwrap_or_else(|_| panic!("missing recovery"));
    assert!(matches!(r.reason, NodeError::Replica(_)));
    assert!(r.local.persistence.closed);
    assert_eq!(r.local.applications[&group(1)].read_applied(1), Ok(0));
}
#[test]
fn abort_preserves_earlier_success_and_network_remains_explicitly_drainable_without_core_access() {
    let mut n = boat(parts(1, true));
    n.control(group(1), NodeControl::Campaign).unwrap();
    settle(&mut n);
    n.propose(request(1)).unwrap();
    settle(&mut n);
    let success = n.poll_client().unwrap();
    n.propose(request(2)).unwrap();
    n.abort();
    assert!(matches!(
        n.complete_client(success).unwrap(),
        ClientOutcome::Applied { .. }
    ));
    let output = n.poll_client().unwrap();
    assert!(matches!(
        n.complete_client(output).unwrap(),
        ClientOutcome::Unknown(_)
    ));
    let mut r = n
        .into_recovery()
        .unwrap_or_else(|_| panic!("missing recovery"));
    let peer = r.peers.as_mut().unwrap();
    for _ in 0..10 {
        peer.drain(
            &mut r.local.outbound,
            MonoTime(0),
            PeerDriverBudget::default(),
        )
        .unwrap();
        if peer.is_drained() {
            break;
        }
    }
    assert!(peer.is_drained());
    assert!(r.local.owner.is_failed());
    r.replica.discard_failed(&mut r.local.as_parts()).unwrap();
}
#[test]
fn unknown_and_closed_admission_return_original_command_allocation_and_instances_shutdown_independently(
) {
    let mut n = elected();
    let mut other = elected();
    let mut q = request(1);
    q.group = group(999);
    let ptr = q.bytes.as_ptr();
    let r = n.propose(q).unwrap_err();
    assert_eq!(r.reason, ClientError::UnknownGroup);
    assert_eq!(r.request.bytes.as_ptr(), ptr);
    shutdown(&mut n);
    other.propose(request(1)).unwrap();
    settle(&mut other);
    let output = other.poll_client().unwrap();
    assert!(matches!(
        other.complete_client(output).unwrap(),
        ClientOutcome::Applied { .. }
    ));
    assert_eq!(other.state(), NodeState::Running);
    shutdown(&mut other);
}

#[test]
fn maintenance_result_is_scoped_retained_and_part_of_shutdown() {
    let mut p = parts(1, false);
    p.local.persistence.reclaim_supported = true;
    let mut n = boat(p);
    let ticket = n.reclaim(1024).unwrap();
    assert_eq!(
        n.reclaim(1024),
        Err(NodeError::Replica(ReplicaError::Worker(
            WorkerError::Overloaded
        )))
    );
    n.poll(MonoTime(0), NodePollBudget::default()).unwrap();
    assert_eq!(n.replica_usage().reclaims, 1);
    n.begin_shutdown();
    n.poll(MonoTime(0), NodePollBudget::default()).unwrap();
    assert!(!n.is_drained());
    let event = n.poll_reclaim().unwrap();
    assert_eq!(event.request, ticket);
    assert_eq!(
        event.result.unwrap(),
        LogReclaimed {
            before_bytes: 100,
            after_bytes: 50
        }
    );
    shutdown(&mut n);
}
#[test]
fn unsupported_and_rejected_maintenance_preserve_running_service() {
    let mut n = boat(parts(1, false));
    assert_eq!(
        n.reclaim(1024),
        Err(NodeError::Replica(ReplicaError::Worker(
            WorkerError::Unsupported
        )))
    );
    assert_eq!(n.state(), NodeState::Running);
    shutdown(&mut n);
    let mut p = parts(1, false);
    p.local.persistence.reclaim_supported = true;
    p.local.persistence.reclaim_error = Some(voteboat::contracts::StorageError::Rejected(
        "maintenance budget",
    ));
    let mut n = boat(p);
    n.reclaim(1024).unwrap();
    n.poll(MonoTime(0), NodePollBudget::default()).unwrap();
    assert!(matches!(
        n.poll_reclaim().unwrap().result,
        Err(voteboat::contracts::StorageError::Rejected(_))
    ));
    n.control(group(1), NodeControl::Campaign).unwrap();
    settle(&mut n);
    n.propose(request(1)).unwrap();
    settle(&mut n);
    let output = n.poll_client().unwrap();
    assert!(matches!(
        n.complete_client(output).unwrap(),
        ClientOutcome::Applied { .. }
    ));
    let ticket = n.reclaim(1).unwrap();
    n.poll(MonoTime(0), NodePollBudget::default()).unwrap();
    let result = n.poll_reclaim().unwrap();
    assert_eq!(result.request, ticket);
    assert_eq!(result.result.unwrap().after_bytes, 1);
    shutdown(&mut n);
}
#[test]
fn wrong_maintenance_receipt_fences_without_losing_original_ticket() {
    let mut p = parts(1, false);
    p.local.persistence.reclaim_supported = true;
    p.local.persistence.reclaim_wrong = true;
    let mut n = boat(p);
    let ticket = n.reclaim(1024).unwrap();
    assert_eq!(
        n.poll(MonoTime(0), NodePollBudget::default()).unwrap_err(),
        NodeError::Replica(ReplicaError::ProviderContract)
    );
    assert_eq!(n.state(), NodeState::RecoveryRequired);
    let recovered = n.into_recovery().unwrap_or_else(|_| panic!("not failed"));
    assert!(recovered.local.owner.is_failed());
    assert_eq!(recovered.replica.usage().reclaims, 2);
    assert_ne!(recovered.replica.failed_reclaim().unwrap().request, ticket);
}
#[test]
fn uncertain_maintenance_preserves_earlier_reply_and_failed_result() {
    let mut p = parts(1, false);
    p.local.persistence.reclaim_supported = true;
    p.local.persistence.reclaim_error = Some(voteboat::contracts::StorageError::Uncertain(
        "lost replacement receipt".into(),
    ));
    let mut n = boat(p);
    n.control(group(1), NodeControl::Campaign).unwrap();
    settle(&mut n);
    n.propose(request(1)).unwrap();
    settle(&mut n);
    let earlier = n.poll_client().unwrap();
    let ticket = n.reclaim(1024).unwrap();
    n.propose(request(2)).unwrap();
    assert!(n.poll(MonoTime(0), NodePollBudget::default()).is_err());
    assert_eq!(n.state(), NodeState::RecoveryRequired);
    assert!(matches!(
        n.complete_client(earlier).unwrap(),
        ClientOutcome::Applied { .. }
    ));
    assert_eq!(n.poll_reclaim().unwrap().request, ticket);
    let pending = n.poll_client().unwrap();
    assert!(matches!(
        n.complete_client(pending).unwrap(),
        ClientOutcome::Unknown(_)
    ));
    assert!(n.poll_reclaim().is_none());
}

fn current_route_plan() -> BTreeMap<NodeId, PeerRoute<()>> {
    use voteboat::connect::ConnectDirection;
    [2, 3]
        .into_iter()
        .map(|n| {
            (
                node(n),
                PeerRoute {
                    store: identity(n as u128),
                    direction: ConnectDirection::Dial(()),
                },
            )
        })
        .collect()
}
#[test]
fn node_retains_pin_checked_routes_across_live_poll_and_drain_and_rejects_withdrawal() {
    let mut n = boat(parts(3, true));
    n.poll(MonoTime(0), NodePollBudget::default()).unwrap();
    n.poll(MonoTime(0), NodePollBudget::default()).unwrap();
    let before = n.peers().unwrap().roster().binding(node(2));
    assert!(before.is_some());
    let mut missing = current_route_plan();
    missing.remove(&node(2));
    let rejected = n.set_admission_routes(missing, MonoTime(0)).err().unwrap();
    assert_eq!(rejected.reason, PeerDriverError::WrongBinding);
    assert_eq!(rejected.routes.len(), 1);
    let mut unsupported = current_route_plan();
    unsupported.insert(
        node(4),
        PeerRoute {
            store: identity(4),
            direction: voteboat::connect::ConnectDirection::Dial(()),
        },
    );
    let rejected = n
        .set_admission_routes(unsupported, MonoTime(0))
        .err()
        .unwrap();
    assert_eq!(rejected.reason, PeerDriverError::WrongBinding);
    assert_eq!(rejected.routes.len(), 3);
    n.set_admission_routes(current_route_plan(), MonoTime(0))
        .unwrap_or_else(|r| panic!("{:?}", r.reason));
    n.poll(MonoTime(0), NodePollBudget::default()).unwrap();
    assert_eq!(n.peers().unwrap().roster().binding(node(2)), before);
    assert_eq!(
        n.local()
            .owner
            .connection_budget()
            .unwrap()
            .provisioned_peers()
            .unwrap()
            .count(),
        2
    );
    n.begin_shutdown();
    assert_eq!(
        n.set_admission_routes(current_route_plan(), MonoTime(0))
            .err()
            .unwrap()
            .reason,
        PeerDriverError::NotQuiescent
    );
    shutdown(&mut n);
    let p = n.into_parts().unwrap_or_else(|_| panic!("not drained"));
    assert_eq!(p.peers.unwrap().admission_routes.unwrap().len(), 2);
}
#[test]
fn route_plan_constructor_failure_returns_hints_and_leaves_owner_policy_uninstalled() {
    let mut p = parts(3, true);
    let mut plan = current_route_plan();
    plan.insert(
        node(4),
        PeerRoute {
            store: identity(4),
            direction: voteboat::connect::ConnectDirection::Dial(()),
        },
    );
    p.peers.as_mut().unwrap().admission_routes = Some(plan);
    let rejected = Boat::from_parts(p, NodeLimits::default(), MonoTime(0))
        .err()
        .unwrap();
    assert_eq!(
        rejected.reason,
        NodeError::Peer(PeerDriverError::WrongBinding)
    );
    assert!(rejected.parts.local.owner.connection_budget().is_none());
    assert_eq!(
        rejected
            .parts
            .peers
            .unwrap()
            .admission_routes
            .unwrap()
            .len(),
        3
    );
    let mut p = parts(3, true);
    p.peers.as_mut().unwrap().admission_routes = Some(current_route_plan());
    let rejected = Boat::from_parts(
        p,
        NodeLimits {
            peers: PeerDriverLimits {
                metadata_bytes: 1,
                ..PeerDriverLimits::default()
            },
            ..NodeLimits::default()
        },
        MonoTime(0),
    )
    .err()
    .unwrap();
    assert_eq!(
        rejected.reason,
        NodeError::Peer(PeerDriverError::InvalidLimits)
    );
    assert!(rejected.parts.local.owner.connection_budget().is_none());
    assert_eq!(
        rejected
            .parts
            .peers
            .unwrap()
            .admission_routes
            .unwrap()
            .len(),
        2
    );
}

#[test]
fn selected_host_placement_is_rechecked_at_execution_and_preserves_core_gates() {
    use voteboat::{membership::*, placement::*};
    struct HostPlacement {
        allow: bool,
    }
    impl PlacementAuthorizer for HostPlacement {
        fn authorize(
            &self,
            selected: GroupIdentity,
            current: &Membership,
            record: &ConfigurationRecord,
        ) -> Result<(), PlacementError> {
            assert_eq!(selected, group(1));
            assert_eq!(current.id(), record.expected);
            if self.allow {
                Ok(())
            } else {
                Err(PlacementError::UnknownReplica(node(1)))
            }
        }
    }
    let mut n = elected();
    let before = n.local().owner.core(group(1)).unwrap().state().clone();
    let mut policy = HostPlacement { allow: true };
    let request = admin_request(1, 100, false);
    policy
        .authorize(
            group(1),
            n.local().owner.core(group(1)).unwrap().membership(),
            &request.proposal.record,
        )
        .unwrap();
    let ticket = n.configure(request).unwrap();
    policy.allow = false;
    n.poll_with_placement_authorizer(
        MonoTime(0),
        NodePollBudget::default(),
        &policy as &dyn PlacementAuthorizer,
    )
    .unwrap();
    let rejected = n.poll_configuration().unwrap();
    assert_eq!(rejected.ticket, ticket);
    assert_eq!(
        rejected.outcome,
        ConfigurationOutcome::NotProposed(
            ConfigurationProposalError::Placement(PlacementError::UnknownReplica(node(1))).into()
        )
    );
    assert_eq!(n.local().owner.core(group(1)).unwrap().state(), &before);
    assert_eq!(n.state(), NodeState::Running);
    policy.allow = true;
    n.configure(admin_request(1, 100, false)).unwrap();
    for _ in 0..100 {
        n.poll_with_placement_authorizer(MonoTime(0), NodePollBudget::default(), &policy)
            .unwrap();
        if let Some(result) = n.poll_configuration() {
            assert!(matches!(result.outcome, ConfigurationOutcome::Committed(_)));
            shutdown(&mut n);
            return;
        }
    }
    panic!("placement-authorized request did not commit");
}

#[test]
fn selected_transport_capacity_rechecks_version_and_roster_budgets_before_persistence() {
    use voteboat::transport::*;
    use voteboat::wire::*;
    let footprint = WireFootprint {
        frame_bytes: 1024,
        decoded_bytes: 1024,
    };
    for (capacity, expected) in [
        (
            None,
            Some(TransportError::UnsupportedConfigurationAdmission),
        ),
        (
            Some(ConfigurationWireCapacity {
                wire_version: 3,
                append: footprint,
                command: footprint,
                snapshot: footprint,
            }),
            Some(TransportError::IncompatibleCodec),
        ),
        (
            Some(ConfigurationWireCapacity {
                wire_version: 4,
                append: footprint,
                command: footprint,
                snapshot: WireFootprint {
                    frame_bytes: 4096,
                    ..footprint
                },
            }),
            Some(TransportError::IncompatibleCodec),
        ),
        (
            Some(ConfigurationWireCapacity {
                wire_version: 4,
                append: WireFootprint {
                    frame_bytes: 0,
                    ..footprint
                },
                command: footprint,
                snapshot: footprint,
            }),
            Some(TransportError::ProviderViolation),
        ),
        (
            Some(ConfigurationWireCapacity {
                wire_version: 4,
                append: footprint,
                command: footprint,
                snapshot: footprint,
            }),
            None,
        ),
    ] {
        let mut p = parts(1, true);
        let net = p.peers.as_mut().unwrap();
        net.factory.capacity = capacity;
        net.roster = PeerRoster::new(
            PeerRosterConfig {
                local: net.roster.local(),
                outbound: net.roster.outbound_binding(),
                first_generation: SecureSessionGeneration::new(1).unwrap(),
                last_generation: SecureSessionGeneration::new(100).unwrap(),
                wire_version: 4,
                limits: net.roster.limits(),
                transport_limits: TransportLimits {
                    send_frame_bytes: 2048,
                    receive_frame_bytes: 2048,
                    decoded_bytes: 4096,
                },
            },
            BTreeMap::new(),
            MonoTime(0),
        )
        .unwrap();
        net.routes.clear();
        let mut n = boat(p);
        n.control(group(1), NodeControl::Campaign).unwrap();
        settle(&mut n);
        let before = n.local().owner.core(group(1)).unwrap().state().clone();
        let ticket = n.configure(admin_request(1, 100, false)).unwrap();
        n.poll_with_configuration_authorization(MonoTime(0), NodePollBudget::default(), |_, _| {
            Ok(())
        })
        .unwrap();
        if let Some(error) = expected {
            let result = n.poll_configuration().unwrap();
            assert_eq!(result.ticket, ticket);
            assert_eq!(
                result.outcome,
                ConfigurationOutcome::NotProposed(
                    ConfigurationProposalError::TransportCapacity(error).into()
                )
            );
            assert_eq!(n.local().owner.core(group(1)).unwrap().state(), &before);
            assert_eq!(n.state(), NodeState::Running);
        } else {
            admin_settle(&mut n);
            assert!(matches!(
                n.poll_configuration().unwrap().outcome,
                ConfigurationOutcome::Committed(_)
            ));
        }
        shutdown(&mut n);
    }
}
