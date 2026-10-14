// SPDX-License-Identifier: RPL-1.5
// Copyright (c) 2026 Karl Livesey
//! Actual Core histories; fixture completion tokens do not model filesystem sync.
use super::*;
use crate::application::{Counter, StateMachine};
use crate::raft::membership_tests::{core, durable};
#[cfg(feature = "native")]
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
fn ack(core: &Raft, request: &Message) -> Event {
    let Rpc::Append {
        previous_index,
        entries,
        ..
    } = &request.rpc
    else {
        panic!("expected append")
    };
    Event::Receive(Message {
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
    })
}
fn commit_ready() -> (Raft, LogUpdate) {
    let mut core = core();
    let append = core
        .propose_batch(vec![proposal(10), proposal(11)])
        .unwrap();
    let requests = sends(durable(&mut core, append));
    let request = requests
        .iter()
        .find(|message| message.to == node(2))
        .unwrap();
    let response = ack(&core, request);
    let commit = update(core.step(response).unwrap());
    assert!(core.can_combine_ready_commit(&commit));
    (core, commit)
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
fn fused_durability_matches_serialized_old_receipts_and_uncommitted_new_tail() {
    let (mut serial, commit) = commit_ready();
    let (mut fused, fused_commit) = commit_ready();
    let mut serial_app = Counter::new(16).unwrap();
    let mut fused_app = serial_app.clone();
    serial_app
        .apply_batch(&serial.state().entries[..1])
        .unwrap();
    fused_app.apply_batch(&fused.state().entries[..1]).unwrap();
    let old_receipts = apply(
        &mut serial_app,
        &durable(&mut serial, vec![Effect::Persist(commit)]),
    );
    let append = serial
        .propose_batch(vec![proposal(12), proposal(13)])
        .unwrap();
    assert!(apply(&mut serial_app, &durable(&mut serial, append)).is_empty());
    let before = fused.state().clone();
    let effects = fused
        .combine_ready_commit(&fused_commit, vec![proposal(12), proposal(13)])
        .unwrap();
    assert_eq!(fused.state(), &before);
    let combined = update(effects);
    assert_eq!(combined.commit_index, 3);
    assert_eq!(combined.suffix.as_ref().unwrap().from, 4);
    let completion = durable(&mut fused, vec![Effect::Persist(combined)]);
    assert!(matches!(completion.first(), Some(Effect::Committed(entries)) if entries.len() == 2));
    assert_eq!(apply(&mut fused_app, &completion), old_receipts);
    assert_eq!(fused_app.applied_index(), 3);
    assert_eq!(fused.state().commit_index, 3);
    assert_eq!(fused.state().last_index(), 5);
    assert_eq!(fused.state().entries, serial.state().entries);
    assert_eq!(fused.state().hard_state, serial.state().hard_state);
    assert_eq!(fused.state().generation, serial.state().generation);
    assert_eq!(
        fused.state().revision.get() + 1,
        serial.state().revision.get()
    );
}

#[test]
fn rejected_inputs_preserve_exact_commit_dependency_and_every_exported_field() {
    let (mut core, commit) = commit_ready();
    let state = core.state().clone();
    let progress = core.progress.clone();
    for variant in 0..5 {
        let mut altered = commit.clone();
        match variant {
            0 => altered.commit_index -= 1,
            1 => altered.expected_revision = LogRevision::new(999).unwrap(),
            2 => altered.hard_state.term += 1,
            3 => {
                altered.suffix = Some(Suffix {
                    from: 4,
                    entries: Vec::new(),
                })
            }
            _ => altered.group.incarnation = GroupIncarnation::new(99).unwrap(),
        }
        assert!(!core.can_combine_ready_commit(&altered));
        assert_eq!(
            core.combine_ready_commit(&altered, vec![proposal(12)]),
            Err(RaftError::WrongCompletion)
        );
        assert_eq!(core.validate_persist_effect(&commit), Ok(()));
    }
    for input in [
        Vec::new(),
        vec![Proposal {
            operation: OperationId::new(12).unwrap(),
            bytes: vec![0; core.limits.max_command_bytes + 1],
        }],
    ] {
        assert!(core.combine_ready_commit(&commit, input).is_err());
        assert_eq!(core.validate_persist_effect(&commit), Ok(()));
    }
    assert_eq!(core.state(), &state);
    assert_eq!(core.progress, progress);
    assert_eq!(core.pending.as_ref().unwrap().next.update(), &commit);
    let exact = ticket(&core);
    core.admit_effect(&commit, exact).unwrap();
    assert!(!core.can_combine_ready_commit(&commit));
    assert_eq!(
        core.combine_ready_commit(&commit, vec![proposal(12)]),
        Err(RaftError::WrongCompletion)
    );
    assert_eq!(core.pending.as_ref().unwrap().ticket, Some(exact));
}

#[test]
fn role_quorum_full_tip_continuation_and_transfer_gates_keep_serial_commit_intact() {
    for gate in 0..5 {
        let (mut core, mut commit) = commit_ready();
        match gate {
            0 => core.role = Role::Follower,
            1 => {
                core.progress.insert(node(2), 1);
            }
            2 => {
                commit = update(
                    core.persist(core.durable.hard_state, 2, None, After::Commit, None)
                        .unwrap(),
                );
            }
            3 => core.pending.as_mut().unwrap().after = After::Reply,
            _ => {
                core.transfer = Some(LeadershipTransfer {
                    request: LeadershipTransferRequest {
                        operation: OperationId::new(99).unwrap(),
                        target: crate::secure::PeerIdentity {
                            node: node(2),
                            store: core.membership().replica_store(node(2)).unwrap(),
                        },
                        configuration: core.membership().id(),
                    },
                    context: RequestContext {
                        origin: core.binding,
                        sequence: 99,
                    },
                    term: core.durable.hard_state.term,
                    index: 3,
                    signal_sent: false,
                });
            }
        }
        let state = core.state().clone();
        let transfer = core.transfer;
        assert!(!core.can_combine_ready_commit(&commit));
        assert_eq!(
            core.combine_ready_commit(&commit, vec![proposal(12)]),
            Err(RaftError::WrongCompletion)
        );
        assert_eq!(core.state(), &state);
        assert_eq!(core.pending.as_ref().unwrap().next.update(), &commit);
        assert_eq!(core.transfer, transfer);
    }
}

#[test]
fn written_uses_old_commit_and_old_prefix_waits_for_exact_durable_completion() {
    let (mut core, commit) = commit_ready();
    let effects = core
        .combine_ready_commit(&commit, vec![proposal(12), proposal(13)])
        .unwrap();
    let combined = update(effects);
    assert_eq!(
        core.validate_persist_effect(&commit),
        Err(RaftError::WrongCompletion)
    );
    let exact = ticket(&core);
    let requests = sends(core.replicate_admitted(exact).unwrap());
    assert_eq!(requests.len(), 1);
    let request = &requests[0];
    assert!(
        matches!(&request.rpc, Rpc::Append { previous_index: 3, leader_commit: 1, entries, .. } if entries.len() == 2)
    );
    assert_eq!(core.state().commit_index, 1);
    assert_eq!(core.state().last_index(), 3);
    assert_eq!(core.progress[&node(1)], 3);
    let response = ack(&core, request);
    assert_eq!(core.step(response.clone()), Err(RaftError::Busy));
    let mut stale = exact;
    stale.last_index -= 1;
    assert_eq!(
        core.complete(&DurableLog {
            tickets: vec![stale]
        }),
        Err(RaftError::WrongCompletion)
    );
    let effects = core
        .complete(&DurableLog {
            tickets: vec![exact],
        })
        .unwrap();
    assert!(matches!(effects.first(), Some(Effect::Committed(entries)) if entries.len() == 2));
    assert_eq!(core.state().commit_index, combined.commit_index);
    assert_eq!(core.state().last_index(), 5);
    // A heartbeat retries the same early range with the now-durable old commit.
    let retry = sends(core.step(Event::Heartbeat).unwrap())
        .into_iter()
        .find(|message| message.to == request.to)
        .unwrap();
    assert_eq!(retry.context, request.context);
    assert!(matches!(
        retry.rpc,
        Rpc::Append {
            leader_commit: 3,
            ..
        }
    ));
    let commit_new = core.step(response).unwrap();
    assert_eq!(core.state().commit_index, 3);
    let committed = durable(&mut core, commit_new);
    assert!(
        matches!(committed.first(), Some(Effect::Committed(entries)) if entries.len() == 2 && entries[0].index == 4)
    );
    assert_eq!(core.state().commit_index, 5);
}

#[test]
fn failure_fences_without_exposing_new_state_and_recovery_keeps_new_tail_uncommitted() {
    let (mut core, commit) = commit_ready();
    let before = core.state().clone();
    let combined = update(
        core.combine_ready_commit(&commit, vec![proposal(12)])
            .unwrap(),
    );
    let exact = ticket(&core);
    core.replicate_admitted(exact).unwrap();
    core.storage_failed();
    assert_eq!(core.state(), &before);
    assert_eq!(
        core.complete(&DurableLog {
            tickets: vec![exact]
        }),
        Err(RaftError::Fenced)
    );
    for recovered in [
        before.clone(),
        stage_update(&before, &combined, core.limits).unwrap(),
    ] {
        let expected_commit = recovered.commit_index;
        let mut restarted = Raft::recover(core.node, core.binding, recovered, core.limits).unwrap();
        assert_eq!(restarted.state().commit_index, expected_commit);
        assert!(restarted.pending.is_none());
        assert_eq!(
            restarted.complete(&DurableLog {
                tickets: vec![exact]
            }),
            Err(RaftError::WrongCompletion)
        );
        if expected_commit == 3 {
            assert_eq!(restarted.state().last_index(), 4);
            let mut app = Counter::new(16).unwrap();
            let receipts = app.apply_batch(restarted.replay_committed()).unwrap();
            assert_eq!(receipts.len(), 2);
            assert_eq!(app.applied_index(), 3);
        }
    }
}

fn commit_with_empty_collector() -> (Raft, LogUpdate, Message) {
    use crate::quorum::{Limits, Policy, Tree};
    let original = core();
    let mut state = original.state().clone();
    state.bootstrap.voter_stores = (1..=5)
        .map(|id| {
            (
                node(id),
                StoreIdentity {
                    id: StoreId::new(u128::from(id)).unwrap(),
                    incarnation: StoreIncarnation::new(1).unwrap(),
                },
            )
        })
        .collect();
    state.bootstrap.policy = Policy::new(
        Tree::Majority((1..=5).map(|id| Tree::Voter(node(id))).collect()),
        Limits::default(),
    )
    .unwrap();
    let mut core = Raft::recover(node(1), original.binding, state, original.limits).unwrap();
    core.role = Role::Leader;
    core.initialize_replication().unwrap();
    let append = core
        .propose_batch(vec![proposal(10), proposal(11)])
        .unwrap();
    let requests = sends(durable(&mut core, append));
    let first = requests
        .iter()
        .find(|request| request.to == node(2))
        .unwrap();
    assert!(core.step(ack(&core, first)).unwrap().is_empty());
    let collector = sends(core.step(Event::Heartbeat).unwrap())
        .into_iter()
        .find(|request| request.to == node(2))
        .unwrap();
    assert!(
        matches!(&collector.rpc, Rpc::Append { entries, previous_index: 3, .. } if entries.is_empty())
    );
    let second = requests
        .iter()
        .find(|request| request.to == node(3))
        .unwrap();
    let commit = update(core.step(ack(&core, second)).unwrap());
    (core, commit, collector)
}

#[test]
fn combined_early_alias_and_old_commit_notification_preserve_later_quorum_progress() {
    for old_first in [true, false] {
        let (mut core, commit, collector) = commit_with_empty_collector();
        core.combine_ready_commit(&commit, vec![proposal(12), proposal(13)])
            .unwrap();
        let exact = ticket(&core);
        let early = sends(core.replicate_admitted(exact).unwrap());
        assert_eq!(early.len(), 2);
        let first = early.iter().find(|request| request.to == node(2)).unwrap();
        let second = early.iter().find(|request| request.to == node(3)).unwrap();
        assert_ne!(first.context, collector.context);
        assert!(core.empty_append_aliases.contains_key(&node(2)));
        core.complete(&DurableLog {
            tickets: vec![exact],
        })
        .unwrap();
        let retry = sends(core.step(Event::Heartbeat).unwrap())
            .into_iter()
            .find(|request| request.to == node(2))
            .unwrap();
        assert_eq!(retry.context, first.context);
        assert!(matches!(
            retry.rpc,
            Rpc::Append {
                leader_commit: 3,
                ..
            }
        ));
        if old_first {
            assert!(core.step(ack(&core, &collector)).unwrap().is_empty());
            assert_eq!(core.progress[&node(2)], 3);
            assert_eq!(core.requests[&node(2)].context, first.context);
        }
        let effects = core.step(ack(&core, first)).unwrap();
        assert!(!effects
            .iter()
            .any(|effect| matches!(effect, Effect::Persist(_))));
        assert_eq!(core.state().commit_index, 3);
        if !old_first {
            assert!(core.step(ack(&core, &collector)).unwrap().is_empty());
        }
        let effects = core.step(ack(&core, second)).unwrap();
        assert!(matches!(effects.as_slice(), [Effect::Persist(_)]));
        assert_eq!(core.state().commit_index, 3);
        let committed = durable(&mut core, effects);
        assert!(
            matches!(committed.first(), Some(Effect::Committed(entries)) if entries.len() == 2 && entries[0].index == 4)
        );
        assert_eq!(core.state().commit_index, 5);
    }
}
