// SPDX-License-Identifier: RPL-1.5
// Copyright (c) 2026 Karl Livesey
//! Exact Core transition controls; modeled host tokens are not filesystem proof.
use super::*;
use crate::raft::membership_tests::{core, durable};

fn node(id: u64) -> NodeId {
    NodeId::new(id).unwrap()
}
fn leader() -> Raft {
    let mut core = core();
    for id in 2..=3 {
        core.progress.insert(node(id), 1);
    }
    core
}
fn prepare(core: &mut Raft) -> LogTicket {
    core.propose_batch(
        (1..=4)
            .map(|id| Proposal {
                operation: OperationId::new(id).unwrap(),
                bytes: vec![id as u8],
            })
            .collect(),
    )
    .unwrap();
    let next = core.pending.as_ref().unwrap().next.metadata();
    LogTicket {
        binding: core.binding,
        batch: core.last_batch + 1,
        group: next.group,
        revision: next.revision,
        generation: next.generation,
        last_index: next.last_index,
        term: next.hard_state.term,
    }
}
fn sends(effects: Vec<Effect>) -> Vec<Message> {
    effects
        .into_iter()
        .map(|effect| {
            let Effect::Send(message) = effect else {
                panic!("expected send")
            };
            message
        })
        .collect()
}
fn ack(core: &Raft, request: &Message, success: bool) -> Message {
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
            success,
            matching_index: entries.last().map_or(*previous_index, |entry| entry.index),
        },
    }
}
fn complete(core: &mut Raft, ticket: LogTicket) -> Vec<Effect> {
    core.complete(&DurableLog {
        tickets: vec![ticket],
    })
    .unwrap()
}

#[test]
fn written_starts_follower_work_without_local_matching_commit_or_application() {
    let mut core = leader();
    let ticket = prepare(&mut core);
    let reserved = core.admitted_replication_reservation().unwrap();
    let early = core.replicate_admitted(ticket).unwrap();
    let actual = early
        .iter()
        .try_fold(early.capacity() * 4 * size_of::<Effect>(), |sum, effect| {
            let Effect::Send(message) = effect else {
                return None;
            };
            sum.checked_add(
                crate::outbound::message_cost(message, usize::MAX)
                    .unwrap()
                    .1,
            )
        })
        .unwrap();
    assert!(actual <= reserved);
    let early = sends(early);
    assert_eq!(early.len(), 2);
    assert_eq!(core.state().last_index(), 1);
    assert_eq!(core.state().commit_index, 1);
    assert_eq!(core.progress[&node(1)], 1);
    assert_eq!(
        core.step(Event::Receive(ack(&core, &early[0], true))),
        Err(RaftError::Busy)
    );
    assert!(complete(&mut core, ticket).is_empty());
    assert_eq!(core.state().last_index(), 5);
    assert_eq!(core.state().commit_index, 1);
    let effects = core
        .step(Event::Receive(ack(&core, &early[0], true)))
        .unwrap();
    assert!(matches!(effects.as_slice(), [Effect::Persist(_)]));
    assert_eq!(core.state().commit_index, 1);
    let applied = durable(&mut core, effects);
    assert!(applied
        .iter()
        .any(|effect| matches!(effect, Effect::Committed(entries) if entries.len() == 4)));
    assert_eq!(core.state().commit_index, 5);
}

#[test]
fn empty_commit_collector_alias_does_not_steal_new_acknowledgement() {
    for old_first in [true, false] {
        let mut core = leader();
        let old = sends(core.step(Event::Heartbeat).unwrap());
        let ticket = prepare(&mut core);
        assert!(core.admitted_replication_reservation().is_some());
        let fresh = sends(core.replicate_admitted(ticket).unwrap());
        assert_ne!(old[0].context, fresh[0].context);
        assert_eq!(core.empty_append_aliases.len(), 2);
        assert!(complete(&mut core, ticket).is_empty());
        if old_first {
            let response = ack(&core, &old[0], true);
            assert!(core.step(Event::Receive(response)).unwrap().is_empty());
            assert_eq!(core.requests[&fresh[0].to].context, fresh[0].context);
            assert_eq!(core.progress[&fresh[0].to], 1);
        }
        let response = ack(&core, &fresh[0], true);
        let effects = core.step(Event::Receive(response)).unwrap();
        assert!(!core.empty_append_aliases.contains_key(&fresh[0].to));
        assert!(!core.admitted_ranges.contains_key(&fresh[0].to));
        durable(&mut core, effects);
        if !old_first {
            let response = ack(&core, &old[0], true);
            assert!(core.step(Event::Receive(response)).unwrap().is_empty());
        }
        assert_eq!(core.progress[&fresh[0].to], 5);
    }
}

#[test]
fn exact_ticket_duplicate_failure_and_higher_term_retire_all_early_scopes() {
    let mut core = leader();
    let old = sends(core.step(Event::Heartbeat).unwrap());
    let ticket = prepare(&mut core);
    let mut wrong = ticket;
    wrong.binding.session = StoreSession::new(2).unwrap();
    assert_eq!(
        core.replicate_admitted(wrong),
        Err(RaftError::WrongCompletion)
    );
    assert!(core.admitted_ranges.is_empty());
    let fresh = sends(core.replicate_admitted(ticket).unwrap());
    assert_eq!(
        core.replicate_admitted(ticket),
        Err(RaftError::WrongCompletion)
    );
    complete(&mut core, ticket);
    let mut high = ack(&core, &old[0], true);
    high.term = 3;
    let effects = core.step(Event::Receive(high)).unwrap();
    assert!(matches!(effects.as_slice(), [Effect::Persist(_)]));
    assert_eq!(core.role(), Role::Follower);
    assert!(core.requests.is_empty());
    assert!(core.empty_append_aliases.is_empty());
    assert!(core.admitted_ranges.is_empty());
    durable(&mut core, effects);
    assert_eq!(core.state().hard_state.term, 3);
    let mut failed = leader();
    let ticket = prepare(&mut failed);
    failed.replicate_admitted(ticket).unwrap();
    failed.storage_failed();
    assert!(failed.requests.is_empty());
    assert!(failed.admitted_ranges.is_empty());
    assert_eq!(failed.replicate_admitted(ticket), Err(RaftError::Fenced));
    assert_eq!(fresh.len(), 2);
}

#[test]
fn bulk_snapshot_unknown_peer_and_changed_ballot_keep_original_serialized_path() {
    for reason in 0..4 {
        let mut core = leader();
        if reason == 0 {
            let context = core.context().unwrap();
            core.requests.insert(
                node(2),
                Replication {
                    context,
                    configuration: core.membership().id(),
                    start: 1,
                    end: 1,
                    snapshot: None,
                    commit_at_start: core.durable.commit_index,
                },
            );
            core.progress.insert(node(3), 0);
        } else if reason == 1 {
            let context = core.context().unwrap();
            let reference = SnapshotRef {
                store: core.binding.identity,
                group: core.state().bootstrap.group,
                generation: SnapshotGeneration::new(1).unwrap(),
                configuration: core.membership().id(),
                index: 1,
                term: 1,
                application_schema: 1,
                file_bytes: 64,
                checksum: 0,
            };
            core.requests.insert(
                node(2),
                Replication {
                    context,
                    configuration: core.membership().id(),
                    start: 0,
                    end: 1,
                    snapshot: Some(reference),
                    commit_at_start: core.durable.commit_index,
                },
            );
            core.progress.insert(node(3), 0);
        } else if reason == 2 {
            core.progress.insert(node(2), 0);
            core.progress.insert(node(3), 0);
        }
        let ticket = prepare(&mut core);
        if reason == 3 {
            core.pending.as_mut().unwrap().update.hard_state.term += 1;
        }
        assert!(core.admitted_replication_reservation().is_none());
        assert!(core.replicate_admitted(ticket).unwrap().is_empty());
        assert_eq!(core.state().last_index(), 1);
        assert!(core.admitted_ranges.is_empty());
    }
}

#[cfg(feature = "native")]
#[test]
fn relay_currentness_validates_pending_original_and_bounded_empty_alias_bytes() {
    use crate::raft::request_window::OriginalScope;
    let mut core = leader();
    let old = sends(core.step(Event::Heartbeat).unwrap());
    let ticket = prepare(&mut core);
    let fresh = sends(core.replicate_admitted(ticket).unwrap());
    let OriginalScope::Request(old_window) = core.request_window(&old[0]) else {
        panic!("old empty alias must remain current")
    };
    let OriginalScope::Request(new_window) = core.request_window(&fresh[0]) else {
        panic!("admitted original must be scoped")
    };
    assert!(old_window.current(&core));
    assert!(new_window.current(&core));
    let mut altered = fresh[0].clone();
    let Rpc::Append { entries, .. } = &mut altered.rpc else {
        unreachable!()
    };
    let EntryPayload::Command { bytes, .. } = &mut entries[0].payload else {
        unreachable!()
    };
    bytes.push(9);
    assert!(matches!(
        core.request_window(&altered),
        OriginalScope::Retired
    ));
    complete(&mut core, ticket);
    assert!(old_window.current(&core));
    assert!(new_window.current(&core));
    let response = ack(&core, &old[0], true);
    core.step(Event::Receive(response)).unwrap();
    assert!(!old_window.current(&core));
    assert!(new_window.current(&core));
    core.storage_failed();
    assert!(!new_window.current(&core));
}
