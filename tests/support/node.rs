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
