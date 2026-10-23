// SPDX-License-Identifier: RPL-1.5
// Copyright (c) 2026 Karl Livesey
//! Real incoming ownership controls; fixture completions assert no disk durability.
use super::*;

fn command(index: u64, term: u64, id: u128, bytes: Vec<u8>) -> LogEntry {
    LogEntry {
        index,
        term,
        payload: EntryPayload::Command {
            operation: operation(id),
            bytes: bytes.into(),
        },
    }
}

fn plain_entries() -> Vec<LogEntry> {
    vec![
        command(2, 1, u128::MAX, Vec::new()),
        command(3, 1, 7, vec![9, 0, 255]),
        LogEntry {
            index: 4,
            term: 1,
            payload: EntryPayload::Noop,
        },
        command(5, 1, (1 << 127) | 17, vec![1]),
    ]
}

fn incoming(receiver: &Raft, entries: Vec<LogEntry>) -> Message {
    let sender = StoreBinding {
        identity: store(2),
        session: StoreSession::new(1).unwrap(),
    };
    let matching = entries.last().map_or(1, |entry| entry.index);
    Message {
        group: receiver.state().bootstrap.group,
        configuration: receiver.membership().id(),
        from: node(2),
        sender,
        to: receiver.node,
        term: entries.last().map_or(1, |entry| entry.term),
        context: RequestContext {
            origin: sender,
            sequence: 41,
        },
        rpc: Rpc::Append {
            previous_index: 1,
            previous_term: 1,
            entries: entries.into(),
            leader_commit: matching,
        },
    }
}

fn spare_outer(entries: Vec<LogEntry>) -> Vec<LogEntry> {
    let mut spare = Vec::with_capacity(entries.len() + 1);
    spare.extend(entries);
    assert!(spare.capacity() > spare.len());
    spare
}

fn update(effects: &[Effect]) -> &LogUpdate {
    let [Effect::Persist(update)] = effects else {
        panic!("expected one persistence dependency, no early reply/application")
    };
    update
}

fn suffix(effects: &[Effect]) -> &Suffix {
    update(effects).suffix.as_ref().unwrap()
}

fn bytes(entry: &LogEntry) -> &CommandBytes {
    let EntryPayload::Command { bytes, .. } = &entry.payload else {
        panic!("expected command")
    };
    bytes
}

fn ticket(receiver: &Raft) -> LogTicket {
    let next = receiver.pending.as_ref().unwrap().next.metadata();
    LogTicket {
        binding: receiver.binding,
        batch: receiver.last_batch + 1,
        group: next.group,
        revision: next.revision,
        generation: next.generation,
        last_index: next.last_index,
        term: next.hard_state.term,
    }
}

fn delayed_completion(receiver: &mut Raft, effects: &[Effect]) -> Vec<Effect> {
    let before = receiver.state().clone();
    receiver.validate_persist_effect(update(effects)).unwrap();
    assert_eq!(
        receiver.complete(&DurableLog { tickets: vec![] }),
        Err(RaftError::WrongCompletion)
    );
    let exact = ticket(receiver);
    let mut wrong = exact;
    wrong.revision = LogRevision::new(wrong.revision.get() + 1).unwrap();
    assert_eq!(receiver.admitted(wrong), Err(RaftError::WrongCompletion));
    receiver.admitted(exact).unwrap();
    assert_eq!(
        receiver.complete(&DurableLog {
            tickets: vec![wrong]
        }),
        Err(RaftError::WrongCompletion)
    );
    assert_eq!(receiver.state(), &before);
    receiver
        .complete(&DurableLog {
            tickets: vec![exact],
        })
        .unwrap()
}

fn check_independent_pending(receiver: &Raft, effects: &[Effect]) {
    let original = update(effects);
    let pending = &receiver.pending.as_ref().unwrap().next;
    assert_eq!(*original, pending.update());
    assert!(pending.matches_update(original));
    let raw = original.suffix.as_ref().unwrap();
    let owned = pending.suffix().unwrap();
    // A small batch is an owned value, so the store's input and the core's
    // plan are independent copies; a wide one would share its backing.
    assert_eq!(
        raw.entries.first_backing_ptr() == owned.entries.first_backing_ptr(),
        raw.entries.is_shared()
    );
    assert_eq!(
        bytes(&raw.entries[1]).as_ptr(),
        bytes(&owned.entries[1]).as_ptr()
    );
    for change_id in [false, true] {
        let mut wrong = original.clone();
        let suffix = wrong.suffix.as_mut().unwrap();
        let mut edited = suffix.entries.to_vec();
        let EntryPayload::Command { operation, bytes } = &mut edited[1].payload else {
            unreachable!()
        };
        if change_id {
            *operation = super::operation(1 << 126);
        } else {
            let mut replacement = bytes.to_vec();
            replacement[0] ^= 1;
            *bytes = replacement.into();
        }
        suffix.entries = edited.into();
        assert_eq!(
            receiver.validate_persist_effect(&wrong),
            Err(RaftError::WrongCompletion)
        );
        assert_eq!(receiver.validate_persist_effect(original), Ok(()));
    }
}

#[test]
fn tight_owned_append_matches_forced_clone_with_independent_delayed_completion() {
    let mut optimized = follower(core(), 1);
    let mut cloned = follower(core(), 1);
    let before = optimized.state().clone();
    let entries = plain_entries();
    assert_eq!(entries.capacity(), entries.len());
    let input_pointer = entries.as_ptr();
    let payload_pointer = bytes(&entries[1]).as_ptr();
    let forced = spare_outer(entries.clone());
    let effects = optimized
        .step(Event::Receive(incoming(&optimized, entries)))
        .unwrap();
    let reference = cloned
        .step(Event::Receive(incoming(&cloned, forced)))
        .unwrap();
    assert_eq!(effects, reference);
    assert_eq!(
        suffix(&effects).entries.first_backing_ptr(),
        Some(input_pointer)
    );
    assert_eq!(
        bytes(&suffix(&effects).entries[1]).as_ptr(),
        payload_pointer
    );
    assert_eq!(optimized.state(), &before);
    assert_eq!(cloned.state(), &before);
    check_independent_pending(&optimized, &effects);
    check_independent_pending(&cloned, &reference);
    let completed = delayed_completion(&mut optimized, &effects);
    let oracle = delayed_completion(&mut cloned, &reference);
    assert_eq!(completed, oracle);
    assert_eq!(optimized.state(), cloned.state());
    assert_eq!(optimized.state().commit_index, 5);
    assert!(
        matches!(&completed[..], [Effect::Committed(entries), Effect::Send(reply)]
        if entries == &plain_entries() && matches!(reply.rpc,
            Rpc::Appended { success: true, matching_index: 5 }))
    );
}

#[test]
fn spare_outer_is_kept_by_a_narrow_batch_and_command_input_capacity_is_normalized_at_capture() {
    for shape in 0..3 {
        let mut receiver = follower(core(), 1);
        let mut entries = plain_entries();
        if shape == 0 {
            entries = spare_outer(entries);
        } else if shape == 1 {
            let EntryPayload::Command { bytes, .. } = &mut entries[0].payload else {
                unreachable!()
            };
            let mut raw = bytes.to_vec();
            raw.reserve(64);
            *bytes = raw.into();
        } else {
            let EntryPayload::Command { bytes, .. } = &mut entries[1].payload else {
                unreachable!()
            };
            let mut raw = bytes.to_vec();
            raw.reserve(64);
            *bytes = raw.into();
        }
        let input_pointer = entries.as_ptr();
        let payload_pointer = bytes(&entries[1]).as_ptr();
        let expected = entries.clone();
        let effects = receiver
            .step(Event::Receive(incoming(&receiver, entries)))
            .unwrap();
        let retained = &suffix(&effects).entries;
        assert_eq!(retained, &expected);
        // A batch this narrow stays owned: the input allocation is kept,
        // spare outer capacity included (messages and pending plans charge
        // it; the installed log charges the visible entries). A wide input
        // is frozen, releasing spare capacity beyond 4 KiB.
        assert!(expected.len() < crate::log::SHARE_MIN);
        assert_eq!(retained.first_backing_ptr(), Some(input_pointer));
        assert_eq!(
            retained.allocated_entries() > retained.len(),
            shape == 0,
            "{shape}"
        );
        assert_eq!(bytes(&retained[1]).as_ptr(), payload_pointer);
        assert_eq!(retained.backing_count(), 1);
        assert_eq!(bytes(&retained[0]).retained_bytes(), 0);
        assert_eq!(
            bytes(&retained[1]).retained_bytes(),
            (bytes(&retained[1]).len() + 2 * std::mem::size_of::<std::sync::atomic::AtomicUsize>())
                .next_multiple_of(std::mem::align_of::<std::sync::atomic::AtomicUsize>().max(2))
        );
        delayed_completion(&mut receiver, &effects);
        assert_eq!(receiver.state().commit_index, 5);
    }
}

#[test]
fn configuration_payload_shares_its_input_and_preserves_full_membership() {
    let mut receiver = follower(core(), 1);
    let entry = LogEntry {
        index: 2,
        term: 1,
        payload: EntryPayload::Configuration(Box::new(ConfigurationRecord {
            operation: operation(u128::MAX),
            expected: cid(1),
            change: ConfigurationChange::Learners(configuration(2, &[1, 2, 3], &[4])),
        })),
    };
    let entries = vec![entry];
    let input_pointer = entries.as_ptr();
    let expected = entries.clone();
    let mut message = incoming(&receiver, entries);
    message.configuration = cid(2);
    // Public unadmitted configuration ingress remains closed. The internal
    // transition fixture separately exercises its existing payload fallback.
    assert_eq!(
        receiver.step(Event::Receive(message.clone())),
        Err(RaftError::InvalidMessage)
    );
    let effects = receiver.receive_inner(message).unwrap();
    assert_eq!(suffix(&effects).entries, expected);
    assert_eq!(
        suffix(&effects).entries.first_backing_ptr(),
        Some(input_pointer)
    );
    assert_eq!(receiver.state().membership().unwrap().id(), cid(1));
    delayed_completion(&mut receiver, &effects);
    assert_eq!(receiver.membership().id(), cid(2));
    assert_eq!(
        receiver.membership(),
        &receiver.state().membership().unwrap()
    );
    assert_eq!(receiver.membership().replica_store(node(4)), Some(store(4)));
    assert_eq!(receiver.membership().voter_store(node(4)), None);
}

#[test]
fn matching_prefix_shares_only_the_unmatched_suffix_and_replay_has_no_persist() {
    let mut receiver = follower(core(), 1);
    let tail = command(2, 1, (1 << 127) | 5, vec![4, 5]);
    let entries = vec![receiver.state().entries[0].clone(), tail.clone()];
    let payload_pointer = bytes(&entries[1]).as_ptr();
    let mut message = incoming(&receiver, entries);
    let Rpc::Append {
        previous_index,
        previous_term,
        ..
    } = &mut message.rpc
    else {
        unreachable!()
    };
    *previous_index = 0;
    *previous_term = 0;
    let replay = message.clone();
    let effects = receiver.step(Event::Receive(message)).unwrap();
    assert_eq!(suffix(&effects).from, 2);
    assert_eq!(suffix(&effects).entries, [tail]);
    assert_eq!(suffix(&effects).entries.len(), 1);
    assert_eq!(suffix(&effects).entries.backing_count(), 0);
    assert_eq!(suffix(&effects).entries.allocated_entries(), 0);
    assert_eq!(
        bytes(&suffix(&effects).entries[0]).as_ptr(),
        payload_pointer
    );
    delayed_completion(&mut receiver, &effects);
    let before = receiver.state().clone();
    let reply = one_reply(receiver.step(Event::Receive(replay)).unwrap());
    assert!(matches!(
        reply.rpc,
        Rpc::Appended {
            success: true,
            matching_index: 2
        }
    ));
    assert_eq!(receiver.state(), &before);
}

#[test]
fn uncommitted_rewrite_has_the_same_atomic_term_commit_and_reply_as_clone() {
    let mut optimized = follower(core(), 1);
    let initial = optimized
        .persist(
            optimized.state().hard_state,
            1,
            Some(Suffix {
                from: 2,
                entries: plain_entries().into(),
            }),
            After::Reply,
            None,
        )
        .unwrap();
    durable(&mut optimized, initial);
    let mut cloned = Raft::recover(
        optimized.node,
        optimized.binding,
        optimized.state().clone(),
        optimized.limits,
    )
    .unwrap();
    let before = optimized.state().clone();
    let entries = vec![
        command(2, 2, 1 << 125, vec![2]),
        command(3, 2, 11, Vec::new()),
    ];
    let input_pointer = entries.as_ptr();
    let forced = spare_outer(entries.clone());
    let effects = optimized
        .step(Event::Receive(incoming(&optimized, entries)))
        .unwrap();
    let oracle = cloned
        .step(Event::Receive(incoming(&cloned, forced)))
        .unwrap();
    assert_eq!(effects, oracle);
    assert_eq!(
        suffix(&effects).entries.first_backing_ptr(),
        Some(input_pointer)
    );
    assert_eq!(optimized.state(), &before);
    let completed = delayed_completion(&mut optimized, &effects);
    let expected = delayed_completion(&mut cloned, &oracle);
    assert_eq!(completed, expected);
    assert_eq!(optimized.state(), cloned.state());
    assert_eq!(optimized.state().hard_state.term, 2);
    assert_eq!(optimized.state().commit_index, 3);
    assert_eq!(optimized.state().last_index(), 3);
}

#[test]
fn failed_ownership_fences_without_install_and_restart_retries_original() {
    for force_clone in [false, true] {
        let mut receiver = follower(core(), 1);
        let before = receiver.state().clone();
        let entries = if force_clone {
            spare_outer(plain_entries())
        } else {
            plain_entries()
        };
        let message = incoming(&receiver, entries);
        let retry = message.clone();
        let effects = receiver.step(Event::Receive(message)).unwrap();
        let stale = ticket(&receiver);
        receiver.admitted(stale).unwrap();
        receiver.storage_failed();
        assert_eq!(receiver.state(), &before);
        assert!(!receiver.has_pending_dependency());
        assert_eq!(
            receiver.validate_persist_effect(update(&effects)),
            Err(RaftError::Fenced)
        );
        assert_eq!(
            receiver.complete(&DurableLog {
                tickets: vec![stale]
            }),
            Err(RaftError::Fenced)
        );
        let binding = StoreBinding {
            session: StoreSession::new(2).unwrap(),
            ..receiver.binding
        };
        let mut recovered = Raft::recover(receiver.node, binding, before, receiver.limits).unwrap();
        assert_eq!(
            recovered.complete(&DurableLog {
                tickets: vec![stale]
            }),
            Err(RaftError::WrongCompletion)
        );
        let effects = recovered.step(Event::Receive(retry)).unwrap();
        assert_eq!(recovered.admitted(stale), Err(RaftError::WrongCompletion));
        delayed_completion(&mut recovered, &effects);
        assert_eq!(recovered.state().commit_index, 5);
        assert_eq!(recovered.state().entries.to_vec()[1..], plain_entries());
    }
}
