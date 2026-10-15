// SPDX-License-Identifier: RPL-1.5
// Copyright (c) 2026 Karl Livesey
//! Actual Core controls with explicitly host-asserted exact durable completions.
use super::*;
use crate::application::{Counter, CounterOutcome, StateMachine};
use crate::raft::membership_tests::{core, durable};
mod histories;

fn node(id: u64) -> NodeId {
    NodeId::new(id).unwrap()
}
fn proposals(count: u128) -> Vec<Proposal> {
    (1..=count)
        .map(|id| Proposal {
            operation: OperationId::new(id).unwrap(),
            bytes: 1_i64.to_le_bytes().to_vec(),
        })
        .collect()
}
fn message(effects: &[Effect], peer: u64) -> Message {
    effects
        .iter()
        .find_map(|effect| match effect {
            Effect::Send(message) if message.to == node(peer) => Some(message.clone()),
            _ => None,
        })
        .expect("expected peer message")
}
fn ack(core: &Raft, request: &Message) -> Message {
    let Rpc::Append {
        previous_index,
        entries,
        ..
    } = &request.rpc
    else {
        panic!("expected append")
    };
    Message {
        group: request.group,
        configuration: request.configuration,
        from: request.to,
        sender: StoreBinding {
            identity: core.membership().replica_store(request.to).unwrap(),
            session: StoreSession::new(1).unwrap(),
        },
        to: request.from,
        term: request.term,
        context: request.context,
        rpc: Rpc::Appended {
            success: true,
            matching_index: entries.last().map_or(*previous_index, |entry| entry.index),
        },
    }
}
fn notification(request: &Message, end: u64, commit: u64) {
    let Rpc::Append {
        previous_index,
        entries,
        leader_commit,
        ..
    } = &request.rpc
    else {
        panic!("expected append")
    };
    assert_eq!(*previous_index, end);
    assert!(entries.is_empty());
    assert_eq!(*leader_commit, commit);
}
fn append(core: &mut Raft, count: u128) -> Vec<Effect> {
    let effects = core.propose_batch(proposals(count)).unwrap();
    durable(core, effects)
}
fn committed(effects: &[Effect]) -> Vec<LogEntry> {
    effects
        .iter()
        .flat_map(|effect| match effect {
            Effect::Committed(entries) => entries.clone(),
            _ => Vec::new(),
        })
        .collect()
}

#[test]
fn progress_skips_delayed_peer_and_each_commit_notification_terminates() {
    let mut leader = core();
    let appended = append(&mut leader, 4);
    let two = message(&appended, 2);
    let three = message(&appended, 3);
    let response = ack(&leader, &two);
    let effects = leader.step(Event::Receive(response.clone())).unwrap();
    assert!(committed(&effects).is_empty());
    assert_eq!(leader.state().commit_index, 1);
    assert_eq!(
        leader.complete(&DurableLog { tickets: vec![] }),
        Err(RaftError::WrongCompletion)
    );
    let effects = durable(&mut leader, effects);
    assert_eq!(committed(&effects).len(), 4);
    assert!(effects
        .iter()
        .all(|effect| !matches!(effect, Effect::Send(m) if m.to == node(3))));
    assert_eq!(leader.requests[&node(3)].context, three.context);
    let notify_two = message(&effects, 2);
    notification(&notify_two, 5, 5);
    assert_ne!(notify_two.context, two.context);
    assert!(leader.step(Event::Receive(response)).unwrap().is_empty());
    assert!(leader
        .step(Event::Receive(ack(&leader, &notify_two)))
        .unwrap()
        .is_empty());
    let effects = leader.step(Event::Receive(ack(&leader, &three))).unwrap();
    let notify_three = message(&effects, 3);
    notification(&notify_three, 5, 5);
    assert_ne!(notify_three.context, three.context);
    assert!(leader
        .step(Event::Receive(ack(&leader, &notify_three)))
        .unwrap()
        .is_empty());
    assert!(leader.requests.is_empty());
}

#[test]
fn old_ack_after_newer_heartbeat_retry_still_owes_a_fresh_notification() {
    let mut leader = core();
    let appended = append(&mut leader, 4);
    let two = message(&appended, 2);
    let three = message(&appended, 3);
    let effects = leader.step(Event::Receive(ack(&leader, &two))).unwrap();
    durable(&mut leader, effects);
    let retries = leader.step(Event::Heartbeat).unwrap();
    let retry = message(&retries, 3);
    assert_eq!(retry.context, three.context);
    let Rpc::Append {
        entries,
        leader_commit,
        ..
    } = &retry.rpc
    else {
        panic!()
    };
    assert_eq!(entries.len(), 4);
    assert_eq!(*leader_commit, 5);
    assert_eq!(leader.requests[&node(3)].commit_at_start, 1);
    let effects = leader.step(Event::Receive(ack(&leader, &three))).unwrap();
    let fresh = message(&effects, 3);
    notification(&fresh, 5, 5);
    assert_ne!(fresh.context, retry.context);
    assert!(leader
        .step(Event::Receive(ack(&leader, &fresh)))
        .unwrap()
        .is_empty());
}

#[test]
fn partial_prefixes_commit_all_originals_without_replaying_other_peer() {
    let mut leader = core();
    leader
        .configure_batch_limits(BatchLimits {
            max_proposal_entries: 64,
            max_replication_entries: 2,
        })
        .unwrap();
    let mut counter = Counter::new(6).unwrap();
    counter.apply_batch(leader.replay_committed()).unwrap();
    let appended = append(&mut leader, 6);
    let delayed = message(&appended, 3);
    let mut request = message(&appended, 2);
    for end in [3, 5, 7] {
        let Rpc::Append { entries, .. } = &request.rpc else {
            panic!()
        };
        assert_eq!(entries.len(), 2);
        assert_eq!(entries.last().unwrap().index, end);
        let effects = leader.step(Event::Receive(ack(&leader, &request))).unwrap();
        let effects = durable(&mut leader, effects);
        assert!(effects
            .iter()
            .all(|effect| !matches!(effect, Effect::Send(m) if m.to == node(3))));
        let receipts = counter.apply_batch(&committed(&effects)).unwrap();
        assert_eq!(receipts.len(), 2);
        for receipt in receipts {
            assert_eq!(receipt.operation.get(), u128::from(receipt.index - 1));
            assert_eq!(
                receipt.outcome,
                CounterOutcome::Value((receipt.index - 1) as i64)
            );
        }
        request = message(&effects, 2);
    }
    notification(&request, 7, 7);
    assert_eq!(counter.read_applied(7), Ok(6));
    assert_eq!(leader.requests[&node(3)].context, delayed.context);
}
