// SPDX-License-Identifier: RPL-1.5
// Copyright (c) 2026 Karl Livesey
use super::*;

#[test]
fn lost_initial_request_and_reply_are_repaired_by_exact_heartbeat_retry() {
    let mut leader = core();
    let mut follower = Raft::recover(
        node(3),
        StoreBinding {
            identity: leader.membership().replica_store(node(3)).unwrap(),
            session: StoreSession::new(1).unwrap(),
        },
        leader.state().clone(),
        LogLimits::default(),
    )
    .unwrap();
    let appended = append(&mut leader, 4);
    let three = message(&appended, 3);
    let retry = message(&leader.step(Event::Heartbeat).unwrap(), 3);
    assert_eq!(retry.context, three.context);
    let effects = follower.step(Event::Receive(retry)).unwrap();
    let applied = durable(&mut follower, effects);
    let lost_reply = message(&applied, 1);
    assert_eq!(follower.state().last_index(), 5);
    assert_eq!(leader.progress[&node(3)], 0);
    let retry = message(&leader.step(Event::Heartbeat).unwrap(), 3);
    assert_eq!(retry.context, three.context);
    let reply = message(&follower.step(Event::Receive(retry)).unwrap(), 1);
    assert_eq!(reply, lost_reply);
    let effects = leader.step(Event::Receive(reply)).unwrap();
    let committed = durable(&mut leader, effects);
    assert_eq!(super::committed(&committed).len(), 4);
    assert_eq!(leader.state().commit_index, 5);
}

#[test]
fn checkpoint_history_keeps_progress_and_owed_notification_contracts() {
    let mut leader = core();
    let reference = SnapshotRef {
        store: leader.binding.identity,
        group: leader.state().bootstrap.group,
        configuration: leader.membership().id(),
        generation: SnapshotGeneration::new(1).unwrap(),
        index: 1,
        term: 1,
        application_schema: 1,
        file_bytes: 128,
        checksum: 7,
    };
    let effects = leader.begin_compact(reference).unwrap();
    let effects = durable(&mut leader, effects);
    for peer in [2, 3] {
        leader
            .step(Event::Receive(ack(&leader, &message(&effects, peer))))
            .unwrap();
    }
    assert_eq!(leader.state().base_index(), 1);
    assert_eq!(leader.state().snapshot, Some(reference));
    let appended = append(&mut leader, 2);
    let effects = leader
        .step(Event::Receive(ack(&leader, &message(&appended, 2))))
        .unwrap();
    let effects = durable(&mut leader, effects);
    assert_eq!(committed(&effects).len(), 2);
    assert!(effects
        .iter()
        .all(|effect| !matches!(effect, Effect::Send(m) if m.to == node(3))));
    let effects = leader
        .step(Event::Receive(ack(&leader, &message(&appended, 3))))
        .unwrap();
    let fresh = message(&effects, 3);
    notification(&fresh, 3, 3);
    assert!(leader
        .step(Event::Receive(ack(&leader, &fresh)))
        .unwrap()
        .is_empty());
}

#[test]
fn higher_term_cancels_original_progress_debt_before_stale_ack() {
    let mut leader = core();
    let appended = append(&mut leader, 2);
    let original = message(&appended, 2);
    let mut higher = ack(&leader, &original);
    higher.term += 1;
    let effects = leader.step(Event::Receive(higher)).unwrap();
    durable(&mut leader, effects);
    assert_eq!(leader.role(), Role::Follower);
    assert!(leader.requests.is_empty());
    assert!(leader
        .step(Event::Receive(ack(&leader, &original)))
        .unwrap()
        .is_empty());
    assert!(leader.step(Event::Heartbeat).unwrap().is_empty());
}

#[test]
fn pending_snapshot_keeps_original_progress_retry_context() {
    let mut leader = core();
    let reference = SnapshotRef {
        store: leader.binding.identity,
        group: leader.state().bootstrap.group,
        configuration: leader.membership().id(),
        generation: SnapshotGeneration::new(1).unwrap(),
        index: 1,
        term: 1,
        application_schema: 1,
        file_bytes: 128,
        checksum: 7,
    };
    let effects = leader.begin_compact(reference).unwrap();
    durable(&mut leader, effects);
    leader.clear_replication_requests();
    leader.next_index.insert(node(2), 1);
    let effects = leader.broadcast_commit_progress().unwrap();
    let original = leader.requests[&node(2)];
    assert_eq!(original.snapshot, Some(reference));
    assert!(effects.iter().any(|effect| matches!(effect,
        Effect::SnapshotRequired { to, context, .. }
            if *to == node(2) && *context == original.context)));
    let repeated = leader.broadcast_commit_progress().unwrap();
    assert!(repeated.iter().any(|effect| matches!(effect,
        Effect::SnapshotRequired { to, context, .. }
            if *to == node(2) && *context == original.context)));
}

#[test]
fn leadership_transfer_keeps_legacy_progress_retry_context() {
    let mut leader = core();
    let request = LeadershipTransferRequest {
        operation: OperationId::new(99).unwrap(),
        target: PeerIdentity {
            node: node(2),
            store: leader.membership().voter_store(node(2)).unwrap(),
        },
        configuration: leader.membership().id(),
    };
    let effects = leader.step(Event::TransferLeadership(request)).unwrap();
    let held = message(&effects, 3);
    assert!(!leader.progress_sends_enabled());
    let repeated = message(&leader.broadcast_commit_progress().unwrap(), 3);
    assert_eq!(repeated.context, held.context);
    assert_eq!(repeated.rpc, held.rpc);
}
