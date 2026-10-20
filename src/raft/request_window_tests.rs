// SPDX-License-Identifier: RPL-1.5
// Copyright (c) 2026 Karl Livesey
//! Genuine original effects and actual request retirement; no caller Ready flag.
use super::*;
use crate::raft::request_window::{OriginalScope, RequestWindow};
#[path = "request_window_tests/native_output.rs"]
mod native_output;
#[path = "request_window_tests/snapshot.rs"]
mod snapshots;

fn window(core: &Raft, m: &Message) -> Box<RequestWindow> {
    let OriginalScope::Request(window) = core.request_window(m) else {
        panic!("missing original request");
    };
    window
}
fn sent(effects: Vec<Effect>, to: u64) -> Message {
    effects
        .into_iter()
        .find_map(|e| match e {
            Effect::Send(m) if m.to == node(to) => Some(m),
            _ => None,
        })
        .unwrap()
}
fn response(original: &Message, rpc: Rpc) -> Message {
    Message {
        from: original.to,
        sender: StoreBinding {
            identity: store(original.to.get()),
            session: StoreSession::new(1).unwrap(),
        },
        to: original.from,
        rpc,
        ..original.clone()
    }
}

#[test]
fn relay_vote_scope_exists_only_after_original_sync_and_retires_on_leadership() {
    let mut core = core();
    let effects = core.step(Event::Campaign).unwrap();
    assert!(effects.iter().all(|e| !matches!(e, Effect::Send(_))));
    let votes = durable(&mut core, effects);
    let vote = sent(votes, 2);
    let scope = window(&core, &vote);
    let mut changed = vote.clone();
    changed.context.sequence += 1;
    assert!(matches!(
        core.request_window(&changed),
        OriginalScope::Retired
    ));
    assert!(scope.current(&core));
    let effects = core
        .step(Event::Receive(response(
            &vote,
            Rpc::Voted { granted: true },
        )))
        .unwrap();
    assert_eq!(core.role(), Role::Leader);
    assert!(!scope.current(&core));
    assert!(matches!(core.request_window(&vote), OriginalScope::Retired));
    durable(&mut core, effects);
}

#[test]
fn relay_replication_is_per_recipient_and_heartbeats_preserve_original_scope() {
    let mut core = core();
    let two = sent(core.step(Event::Heartbeat).unwrap(), 2);
    let three = sent(core.step(Event::Heartbeat).unwrap(), 3);
    let scope2 = window(&core, &two);
    let scope3 = window(&core, &three);
    let retry = sent(core.step(Event::Heartbeat).unwrap(), 2);
    assert_eq!(retry.context, two.context);
    core.step(Event::Receive(response(
        &two,
        Rpc::Appended {
            success: true,
            matching_index: 1,
        },
    )))
    .unwrap();
    assert!(!scope2.current(&core));
    assert!(scope3.current(&core));
    let next = sent(core.step(Event::Heartbeat).unwrap(), 2);
    assert_ne!(next.context, two.context);
    assert!(window(&core, &next).current(&core));
    assert!(
        scope3.current(&core),
        "unavailable voter3 is not a global barrier"
    );
    let mut changed = next.clone();
    if let Rpc::Append { previous_term, .. } = &mut changed.rpc {
        *previous_term += 1;
    }
    assert!(matches!(
        core.request_window(&changed),
        OriginalScope::Retired
    ));
}

#[test]
fn relay_read_result_retention_does_not_keep_an_ack_collector() {
    let mut core = core();
    let effects = core
        .step(Event::Read {
            request: ReadRequestId::new(1).unwrap(),
        })
        .unwrap();
    let three = effects
        .iter()
        .find_map(|e| match e {
            Effect::Send(m) if m.to == node(3) => Some(m.clone()),
            _ => None,
        })
        .unwrap();
    let two = sent(effects, 2);
    assert!(matches!(three.rpc, Rpc::ReadProbe));
    let scope = window(&core, &three);
    core.step(Event::Receive(response(&two, Rpc::ReadAck)))
        .unwrap();
    assert!(core.ready_read.is_some());
    assert!(core.read.is_none());
    assert!(!scope.current(&core));
    let ready = core.ready_read.unwrap();
    core.finish_read(&ready, ready.index()).unwrap();
    let next = sent(
        core.step(Event::Read {
            request: ReadRequestId::new(2).unwrap(),
        })
        .unwrap(),
        2,
    );
    assert_ne!(next.context, two.context);
    assert!(window(&core, &next).current(&core));
}

#[test]
fn relay_readiness_owns_the_exact_session_and_cancel_retires_it() {
    let mut core = staged();
    let original = sent(
        core.step(Event::CheckLearnerReadiness {
            learner: PeerIdentity {
                node: node(4),
                store: store(4),
            },
            session: StoreSession::new(7).unwrap(),
            requirements: ReadinessRequirements {
                application_schema: 1,
                inner_profile: crate::raft::InnerProfile::LEAF,
                command_bytes: 128,
                snapshot_bytes: 256,
            },
        })
        .unwrap(),
        4,
    );
    let scope = window(&core, &original);
    let mut changed = original.clone();
    if let Rpc::LearnerReadinessRequest(request) = &mut changed.rpc {
        request.session = StoreSession::new(8).unwrap();
    }
    assert!(matches!(
        core.request_window(&changed),
        OriginalScope::Retired
    ));
    core.step(Event::CancelLearnerReadiness).unwrap();
    assert!(!scope.current(&core));
}

#[test]
fn relay_authority_query_is_not_leader_only_and_a_permit_is_not_pending() {
    let mut core = core();
    core.role = Role::Follower;
    let original = sent(
        core.step(Event::AuthorizeReplication {
            witness: PeerIdentity {
                node: node(2),
                store: store(2),
            },
            candidate: PeerIdentity {
                node: node(4),
                store: store(4),
            },
            configuration: cid(4),
        })
        .unwrap(),
        2,
    );
    let scope = window(&core, &original);
    core.step(Event::Receive(response(
        &original,
        Rpc::AuthorityReply {
            candidate: PeerIdentity {
                node: node(4),
                store: store(4),
            },
            configuration: cid(4),
            committed_index: 2,
            committed_term: 1,
            granted: true,
        },
    )))
    .unwrap();
    assert!(matches!(
        core.replication_authorization_status(),
        ReplicationAuthorizationStatus::Granted { .. }
    ));
    assert!(!scope.current(&core));
}

#[test]
fn relay_batched_candidate_repair_and_one_way_joint_output_are_distinct() {
    for batched in [false, true] {
        let mut core = joint(false);
        if batched {
            core = core.with_batched_joint_repair();
        }
        let campaign = core.step(Event::Campaign).unwrap();
        let original = sent(durable(&mut core, campaign), 4);
        assert_eq!(core.role(), Role::Candidate);
        if batched {
            assert!(matches!(original.rpc, Rpc::LearnerRepair { .. }));
            let scope = window(&core, &original);
            assert!(scope.current(&core));
            core.repair_requests.remove(&node(4));
            assert!(!scope.current(&core));
        } else {
            assert!(matches!(
                original.rpc,
                Rpc::Append {
                    leader_commit: 0,
                    ..
                }
            ));
            assert!(matches!(
                core.request_window(&original),
                OriginalScope::OneWay
            ));
            assert!(!core.requests.contains_key(&node(4)));
        }
    }
}

#[test]
fn ordinary_startup_envelope_does_not_reduce_the_existing_repair_window() {
    for max_replication_entries in [1, 64, 1024] {
        let (mut core, _) = retained_repair_pair(7, 0);
        core.configure_batch_limits(BatchLimits {
            max_replication_entries,
            ..BatchLimits::default()
        })
        .unwrap();
        core = core.with_batched_joint_repair();
        let campaign = core.step(Event::Campaign).unwrap();
        let original = sent(durable(&mut core, campaign), 4);
        let Rpc::LearnerRepair { entries, .. } = &original.rpc else {
            panic!("bounded repair required");
        };
        assert!(entries.len() > 1 && entries.len() <= 64);
        assert!(window(&core, &original).current(&core));
    }
}

#[test]
fn relay_retiring_leader_notice_survives_the_original_role_transition() {
    let mut core = retiring_leader(true);
    acknowledge(&mut core, 2);
    let commit = acknowledge(&mut core, 3);
    let notice = sent(durable(&mut core, commit), 4);
    assert_eq!(core.role(), Role::Follower);
    assert!(!core.local_voter());
    assert!(matches!(
        core.request_window(&notice),
        OriginalScope::OneWay
    ));
    let mut wrong = notice;
    if let Rpc::Append { leader_commit, .. } = &mut wrong.rpc {
        *leader_commit += 1;
    }
    assert!(matches!(
        core.request_window(&wrong),
        OriginalScope::Retired
    ));
}

#[test]
fn relay_timeout_now_is_the_original_one_way_signal_and_cancel_retires_it() {
    let mut core = core();
    core.step(Event::TransferLeadership(LeadershipTransferRequest {
        operation: operation(800),
        target: PeerIdentity {
            node: node(2),
            store: store(2),
        },
        configuration: cid(1),
    }))
    .unwrap();
    let signal = sent(acknowledge(&mut core, 2), 2);
    assert!(matches!(signal.rpc, Rpc::TimeoutNow { .. }));
    assert!(core.leadership_transfer().unwrap().signal_sent);
    assert!(matches!(
        core.request_window(&signal),
        OriginalScope::OneWay
    ));
    core.step(Event::CancelLeadershipTransfer {
        context: signal.context,
    })
    .unwrap();
    assert!(matches!(
        core.request_window(&signal),
        OriginalScope::Retired
    ));
}

#[test]
fn relay_term_zero_authority_uses_positive_wire_term_and_exact_pending_query() {
    let initial = core();
    let bootstrap = initial.state().bootstrap.clone();
    let mut states = BTreeMap::new();
    apply_batch(
        &mut states,
        &[LogMutation::Create(bootstrap.clone())],
        initial.limits,
    )
    .unwrap();
    let mut core = Raft::recover(
        node(1),
        initial.binding,
        states.remove(&bootstrap.group).unwrap(),
        initial.limits,
    )
    .unwrap();
    let original = sent(
        core.step(Event::AuthorizeReplication {
            witness: PeerIdentity {
                node: node(2),
                store: store(2),
            },
            candidate: PeerIdentity {
                node: node(4),
                store: store(4),
            },
            configuration: cid(4),
        })
        .unwrap(),
        2,
    );
    assert_eq!(core.state().hard_state.term, 0);
    assert_eq!(original.term, 1);
    let scope = window(&core, &original);
    assert!(scope.current(&core));
    core.step(Event::CancelReplicationAuthorization).unwrap();
    assert!(!scope.current(&core));
}

#[test]
fn relay_reply_correlation_checks_original_context_and_physical_store() {
    let mut core = core();
    let original = sent(core.step(Event::Heartbeat).unwrap(), 2);
    let scope = window(&core, &original);
    let reply = response(
        &original,
        Rpc::Appended {
            success: true,
            matching_index: 1,
        },
    );
    let source = crate::secure::LocalIdentity {
        node: reply.from,
        store: reply.sender,
    };
    assert!(scope.accepts_reply(&core, source, &reply));
    let mut wrong = reply.clone();
    wrong.context.sequence += 1;
    assert!(!scope.accepts_reply(&core, source, &wrong));
    wrong = reply.clone();
    wrong.rpc = Rpc::ReadAck;
    assert!(!scope.accepts_reply(&core, source, &wrong));
    let other = crate::secure::LocalIdentity {
        store: StoreBinding {
            session: StoreSession::new(2).unwrap(),
            ..source.store
        },
        ..source
    };
    assert!(!scope.accepts_reply(&core, other, &reply));
    let mut higher = reply;
    higher.term += 1;
    assert!(
        scope.accepts_reply(&core, source, &higher),
        "higher durable term goes to the independent real core"
    );
}
