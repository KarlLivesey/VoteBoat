// SPDX-License-Identifier: RPL-1.5
// Copyright (c) 2026 Karl Livesey
//! Actual ordered Core histories; fixture tickets assert host durability only.
use super::*;
use crate::application::{Counter, StateMachine};
use crate::quorum::{Limits, Policy, Tree, WeightedChild};
use crate::raft::membership_tests::{core, durable};
#[path = "gates.rs"]
mod gates;
#[cfg(feature = "native")]
#[path = "native.rs"]
mod native;

fn node(id: u64) -> NodeId {
    NodeId::new(id).unwrap()
}
fn proposal(id: u128) -> Proposal {
    Proposal {
        operation: OperationId::new(id).unwrap(),
        bytes: 1i64.to_le_bytes().to_vec(),
    }
}
fn update(effects: Vec<Effect>) -> LogUpdate {
    let [Effect::Persist(update)] = effects.as_slice() else {
        panic!("expected sole persistence effect")
    };
    update.clone()
}
fn sends(effects: Vec<Effect>) -> Vec<Message> {
    effects
        .into_iter()
        .filter_map(|effect| match effect {
            Effect::Send(message) => Some(message),
            _ => None,
        })
        .collect()
}
fn ack(core: &Raft, request: &Message) -> Message {
    let Rpc::Append {
        previous_index,
        entries,
        ..
    } = &request.rpc
    else {
        panic!("expected ordinary append")
    };
    Message {
        group: request.group,
        configuration: request.configuration,
        from: request.to,
        to: request.from,
        sender: StoreBinding {
            identity: core.membership().replica_store(request.to).unwrap(),
            session: StoreSession::new(1).unwrap(),
        },
        term: request.term,
        context: request.context,
        rpc: Rpc::Appended {
            success: true,
            matching_index: entries.last().map_or(*previous_index, |entry| entry.index),
        },
    }
}
fn ticket(core: &Raft) -> LogTicket {
    let metadata = core.pending.as_ref().unwrap().next.metadata();
    LogTicket {
        binding: core.binding,
        batch: core.last_batch + 1,
        group: metadata.group,
        revision: metadata.revision,
        generation: metadata.generation,
        last_index: metadata.last_index,
        term: metadata.hard_state.term,
    }
}
fn policy_core(kind: usize) -> Raft {
    let mut state = core().state().clone();
    let count = if kind == 1 { 5 } else { 3 };
    let stores = (1..=count)
        .map(|n| {
            (
                node(n),
                StoreIdentity {
                    id: StoreId::new(n as u128).unwrap(),
                    incarnation: StoreIncarnation::new(1).unwrap(),
                },
            )
        })
        .collect();
    let tree = if kind == 2 {
        Tree::Weighted(
            [2, 3, 1]
                .into_iter()
                .enumerate()
                .map(|(offset, weight)| WeightedChild {
                    weight,
                    node: Tree::Voter(node(offset as u64 + 1)),
                })
                .collect(),
        )
    } else {
        Tree::Majority((1..=count).map(|n| Tree::Voter(node(n))).collect())
    };
    state.bootstrap.policy = Policy::new(tree, Limits::default()).unwrap();
    state.bootstrap.voter_stores = stores;
    let binding = core().binding;
    let mut core = Raft::recover(node(1), binding, state, LogLimits::default()).unwrap();
    core.role = Role::Leader;
    core.initialize_replication().unwrap();
    core
}
fn ready(kind: usize) -> (Raft, Vec<Message>) {
    let mut core = policy_core(kind);
    let append = core
        .propose_batch(vec![proposal(10), proposal(11)])
        .unwrap();
    let requests = sends(durable(&mut core, append));
    if kind == 1 {
        let response = ack(&core, requests.iter().find(|m| m.to == node(2)).unwrap());
        assert!(core.step(Event::Receive(response)).unwrap().is_empty());
    }
    let peers = if kind == 1 { [3, 4] } else { [2, 3] };
    let replies = peers
        .into_iter()
        .map(|peer| ack(&core, requests.iter().find(|m| m.to == node(peer)).unwrap()))
        .collect();
    (core, replies)
}
fn apply(counter: &mut Counter, effects: &[Effect]) -> Vec<crate::application::CounterReceipt> {
    let mut receipts = Vec::new();
    for effect in effects {
        if let Effect::Committed(entries) = effect {
            receipts.extend(counter.apply_batch(entries).unwrap());
        }
    }
    receipts
}

#[test]
fn three_five_and_weighted_histories_match_serialized_receipts_and_uncommitted_tail() {
    for kind in 0..3 {
        let (mut serial, replies) = ready(kind);
        let (mut joined, joined_replies) = ready(kind);
        let mut serial_app = Counter::new(16).unwrap();
        let mut joined_app = serial_app.clone();
        serial_app.apply_batch(serial.replay_committed()).unwrap();
        joined_app.apply_batch(joined.replay_committed()).unwrap();
        let commit = serial.step(Event::Receive(replies[0].clone())).unwrap();
        let old_receipts = apply(&mut serial_app, &durable(&mut serial, commit));
        assert!(
            sends(serial.step(Event::Receive(replies[1].clone())).unwrap())
                .iter()
                .all(|m| matches!(
                    &m.rpc,
                    Rpc::Append {
                        leader_commit: 3,
                        ..
                    }
                ))
        );
        let append = serial
            .propose_batch(vec![proposal(12), proposal(13)])
            .unwrap();
        assert!(apply(&mut serial_app, &durable(&mut serial, append)).is_empty());
        let before = joined.state().clone();
        let planned = joined
            .propose_after_ready_replies(&joined_replies, vec![proposal(12), proposal(13)])
            .unwrap();
        assert_eq!(joined.state(), &before);
        let combined = update(planned);
        assert_eq!(combined.commit_index, 3);
        assert_eq!(combined.suffix.as_ref().unwrap().from, 4);
        let effects = durable(&mut joined, vec![Effect::Persist(combined)]);
        assert!(matches!(effects.first(), Some(Effect::Committed(entries)) if entries.len() == 2));
        assert_eq!(apply(&mut joined_app, &effects), old_receipts);
        assert_eq!(joined_app.applied_index(), 3);
        assert_eq!(joined.state().entries, serial.state().entries);
        assert_eq!(joined.state().hard_state, serial.state().hard_state);
        assert_eq!(joined.state().commit_index, 3);
        assert_eq!(joined.state().last_index(), 5);
        assert_eq!(
            joined.state().revision.get() + 1,
            serial.state().revision.get()
        );
        assert!(sends(effects).iter().all(|m| {
            matches!(
                m.rpc,
                Rpc::Append {
                    leader_commit: 3,
                    ..
                }
            )
        }));
    }
}

fn assert_durable_notifications(core: &Raft, early: &[Message], notifications: &[Message]) {
    assert_eq!(notifications.len(), early.len());
    for notification in notifications {
        let original = early.iter().find(|m| m.to == notification.to).unwrap();
        assert_eq!(notification.context, original.context);
        assert!(
            matches!(&notification.rpc, Rpc::Append { leader_commit: 3, entries, .. }
            if entries.len() == 2)
        );
        assert_eq!(core.requests[&notification.to].commit_at_start, 1);
    }
}

#[test]
fn written_sends_old_commit_exact_durable_notifies_same_context_and_new_tail_needs_quorum() {
    let (mut core, replies) = ready(0);
    let planned = core
        .propose_after_ready_replies(&replies, vec![proposal(12), proposal(13)])
        .unwrap();
    assert_eq!(update(planned).commit_index, 3);
    let exact = ticket(&core);
    let before = core.state().clone();
    let early = sends(core.replicate_admitted(exact).unwrap());
    assert_eq!(early.len(), 2);
    for message in &early {
        assert!(
            matches!(&message.rpc, Rpc::Append { previous_index: 3, entries,
            leader_commit: 1, .. } if entries.len() == 2)
        );
        assert_eq!(core.progress[&message.to], 3);
        let response = ack(&core, message);
        assert_eq!(core.step(Event::Receive(response)), Err(RaftError::Busy));
    }
    let mut stale = exact;
    stale.batch += 1;
    assert_eq!(
        core.complete(&DurableLog {
            tickets: vec![stale]
        }),
        Err(RaftError::WrongCompletion)
    );
    assert_eq!(core.state(), &before);
    let durable_effects = core
        .complete(&DurableLog {
            tickets: vec![exact],
        })
        .unwrap();
    assert!(
        matches!(durable_effects.first(), Some(Effect::Committed(entries)) if entries.len() == 2)
    );
    let notifications = sends(durable_effects);
    assert_durable_notifications(&core, &early, &notifications);
    assert_eq!(core.state().commit_index, 3);
    let commit = core.step(Event::Receive(ack(&core, &early[0]))).unwrap();
    assert_eq!(core.state().commit_index, 3);
    let final_effects = durable(&mut core, commit);
    assert!(
        matches!(final_effects.first(), Some(Effect::Committed(entries)) if entries.len() == 2)
    );
    assert_eq!(core.state().commit_index, 5);
    assert_eq!(
        core.complete(&DurableLog {
            tickets: vec![exact]
        }),
        Err(RaftError::WrongCompletion)
    );
}

#[test]
fn an_unconsumed_old_primary_reply_remains_scoped_until_its_later_notification() {
    let (mut core, replies) = ready(0);
    let delayed = replies[1].clone();
    let planned = update(
        core.propose_after_ready_replies(&replies[..1], vec![proposal(12)])
            .unwrap(),
    );
    core.validate_persist_effect(&planned).unwrap();
    let exact = ticket(&core);
    let early = sends(core.replicate_admitted(exact).unwrap());
    assert_eq!(early.len(), 1);
    assert_eq!(early[0].to, replies[0].from);
    assert_eq!(
        core.step(Event::Receive(delayed.clone())),
        Err(RaftError::Busy)
    );
    assert_eq!(
        core.validate_persist_effect(&planned),
        Err(RaftError::WrongCompletion)
    );
    let pending = core.pending.as_ref().unwrap();
    assert!(pending.next.matches_update(&planned));
    assert_eq!(pending.ticket, Some(exact));
    let completed = core
        .complete(&DurableLog {
            tickets: vec![exact],
        })
        .unwrap();
    let notification = sends(completed)
        .into_iter()
        .find(|m| m.to == delayed.from)
        .unwrap();
    assert_eq!(notification.context, delayed.context);
    assert!(matches!(
        notification.rpc,
        Rpc::Append {
            leader_commit: 3,
            ..
        }
    ));
    let next = sends(core.step(Event::Receive(delayed)).unwrap());
    assert_eq!(next.len(), 1);
    assert!(
        matches!(&next[0].rpc, Rpc::Append { previous_index: 3, entries, leader_commit: 3, .. }
        if entries.len() == 1)
    );
    assert_eq!(core.state().commit_index, 3);
}
