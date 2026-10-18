// SPDX-License-Identifier: RPL-1.5
// Copyright (c) 2026 Karl Livesey
//! Volatile cores: the core's log is the only copy and completion is a token.
use super::*;
use membership_tests::{core, durable};

fn volatile() -> (Raft, VolatileLog) {
    let mut core = core();
    core.volatile = true;
    core.incarnation = Raft::next_volatile_incarnation().unwrap();
    let log = VolatileLog::new(core.binding, core.limits).unwrap();
    (core, log)
}

fn token(effects: &[Effect]) -> VolatilePersist {
    let [Effect::PersistVolatile(token)] = effects else {
        panic!("one payload-free persistence token, got {effects:?}")
    };
    *token
}

fn propose(core: &mut Raft, operation: u128, bytes: Vec<u8>) -> Vec<Effect> {
    core.step(Event::Propose {
        operation: OperationId::new(operation).unwrap(),
        bytes,
    })
    .unwrap()
}

fn command_bytes(entry: &LogEntry) -> *const u8 {
    let EntryPayload::Command { bytes, .. } = &entry.payload else {
        panic!("command entry")
    };
    bytes.as_ptr()
}

#[test]
fn volatile_persist_carries_no_entries_and_installs_the_exact_proposal() {
    let (mut core, log) = volatile();
    let mut twin = membership_tests::core();
    let before = core.state().clone();
    let effects = propose(&mut core, 9, vec![1, 2, 3]);
    let persist = token(&effects);
    assert_eq!(persist.revision().get(), before.revision.get() + 1);
    assert_eq!(persist.generation(), before.generation);
    // Nothing is installed before completion; the plan owns the only copy.
    assert_eq!(core.state(), &before);
    let pending = core.pending.as_ref().unwrap();
    let planned = &pending.next.update().suffix.as_ref().unwrap().entries;
    assert_eq!(planned.capacity(), planned.len());
    let planned_bytes = command_bytes(&planned[0]);
    let (committed, following) = persist_volatile(&mut core, &log, persist)
        .unwrap()
        .into_parts();
    assert!(committed.is_none());
    assert!(following.iter().all(|e| matches!(e, Effect::Send(_))));
    assert!(!following.is_empty());
    let installed = core.state().entries.last().unwrap();
    assert_eq!(installed.index, 2);
    assert_eq!(command_bytes(installed), planned_bytes);
    // The durable seam reaches the same canonical state and effects.
    let effects = propose(&mut twin, 9, vec![1, 2, 3]);
    let expected = durable(&mut twin, effects);
    assert_eq!(core.state(), twin.state());
    assert_eq!(following, expected);
}

#[test]
fn volatile_token_is_three_words_and_never_sizes_effect() {
    assert_eq!(std::mem::size_of::<VolatilePersist>(), 24);
    assert!(std::mem::size_of::<LogUpdate>() > std::mem::size_of::<VolatilePersist>());
}

#[test]
fn volatile_step_is_busy_until_persist_completes() {
    let (mut core, log) = volatile();
    let persist = token(&propose(&mut core, 9, vec![1]));
    assert!(core.has_pending_dependency());
    assert_eq!(core.step(Event::Heartbeat), Err(RaftError::Busy));
    assert_eq!(
        core.propose_batch(vec![Proposal {
            operation: OperationId::new(10).unwrap(),
            bytes: vec![2],
        }]),
        Err(RaftError::Busy)
    );
    assert!(!core.is_fenced());
    persist_volatile(&mut core, &log, persist).unwrap();
    assert!(!core.has_pending_dependency());
    assert!(core.step(Event::Heartbeat).is_ok());
}

#[test]
fn volatile_core_rejects_durable_seam_calls() {
    let (mut core, log) = volatile();
    let persist = token(&propose(&mut core, 9, vec![1]));
    let update = core.pending.as_ref().unwrap().next.update().clone();
    let next = core.pending.as_ref().unwrap().next.metadata();
    let ticket = LogTicket {
        binding: core.binding,
        batch: core.last_batch + 1,
        group: next.group,
        revision: next.revision,
        generation: next.generation,
        last_index: next.last_index,
        term: next.hard_state.term,
    };
    let completion = DurableLog {
        tickets: vec![ticket],
    };
    assert_eq!(
        core.validate_persist_effect(&update),
        Err(RaftError::WrongCompletion)
    );
    assert_eq!(
        core.admit_effect(&update, ticket),
        Err(RaftError::WrongCompletion)
    );
    assert_eq!(core.admitted(ticket), Err(RaftError::WrongCompletion));
    assert_eq!(core.complete(&completion), Err(RaftError::WrongCompletion));
    // None of these refusals admitted, installed or fenced anything.
    assert!(!core.is_fenced());
    assert!(core.pending.as_ref().unwrap().ticket.is_none());
    assert_eq!(core.last_batch, 0);
    persist_volatile(&mut core, &log, persist).unwrap();
    assert_eq!(core.state().last_index(), 2);
    // And a durable core refuses a volatile token, fencing like any failure.
    let mut durable_core = membership_tests::core();
    let effects = propose(&mut durable_core, 9, vec![1]);
    assert!(matches!(&effects[..], [Effect::Persist(_)]));
    assert!(matches!(
        persist_volatile(&mut durable_core, &log, persist),
        Err(RaftError::WrongCompletion)
    ));
    assert!(durable_core.is_fenced());
    assert!(!durable_core.has_pending_dependency());
}

#[test]
fn volatile_commit_only_round_borrows_canonical_interval() {
    let (mut core, log) = volatile();
    core.role = Role::Follower;
    let entries = vec![
        LogEntry {
            index: 2,
            term: 1,
            payload: EntryPayload::Command {
                operation: OperationId::new(9).unwrap(),
                bytes: vec![1, 2, 3].into(),
            },
        },
        LogEntry {
            index: 3,
            term: 1,
            payload: EntryPayload::Command {
                operation: OperationId::new(u128::MAX).unwrap(),
                bytes: Vec::new().into(),
            },
        },
    ];
    let effects = core
        .persist(
            core.state().hard_state,
            1,
            Some(Suffix { from: 2, entries }),
            After::Reply,
            None,
        )
        .unwrap();
    let result = persist_volatile(&mut core, &log, token(&effects)).unwrap();
    assert!(result.committed().is_none());
    assert!(result.following().is_empty());
    let canonical = core.state().entries[1..].as_ptr();
    let canonical_bytes = command_bytes(&core.state().entries[1]);
    let effects = core
        .persist(core.state().hard_state, 3, None, After::Commit, None)
        .unwrap();
    let result = persist_volatile(&mut core, &log, token(&effects)).unwrap();
    let committed = result.committed().unwrap();
    assert_eq!(committed.as_ptr(), canonical);
    assert_eq!(committed.len(), 2);
    assert_eq!(command_bytes(&committed[0]), canonical_bytes);
    assert!(result.following().is_empty());
    assert_eq!(core.state().commit_index, 3);
}

#[test]
fn volatile_storage_failure_drops_pending_and_steps_down() {
    let (mut core, log) = volatile();
    assert_eq!(core.role(), Role::Leader);
    let before = core.state().clone();
    let persist = token(&propose(&mut core, 9, vec![1]));
    let mut other = log.binding();
    other.session = StoreSession::new(2).unwrap();
    let wrong = VolatileLog::new(other, log.limits()).unwrap();
    assert!(matches!(
        persist_volatile(&mut core, &wrong, persist),
        Err(RaftError::WrongIdentity)
    ));
    assert!(core.is_fenced());
    assert_eq!(core.role(), Role::Follower);
    assert!(!core.has_pending_dependency());
    assert_eq!(core.state(), &before);
    assert!(matches!(
        persist_volatile(&mut core, &log, persist),
        Err(RaftError::Fenced)
    ));
    // A stale or mismatched token fences a live core in the same way.
    let (mut core, log) = volatile();
    let persist = token(&propose(&mut core, 9, vec![1]));
    persist_volatile(&mut core, &log, persist).unwrap();
    let next = token(&propose(&mut core, 10, vec![2]));
    assert_ne!(next, persist);
    let completed = core.state().clone();
    assert!(matches!(
        persist_volatile(&mut core, &log, persist),
        Err(RaftError::WrongCompletion)
    ));
    assert!(core.is_fenced());
    assert!(!core.has_pending_dependency());
    assert_eq!(core.state(), &completed);
}

#[test]
fn proposal_batches_collect_a_tight_suffix_before_owned_planning() {
    for width in [1, 2, 3, 7, 64] {
        let (mut core, log) = volatile();
        let proposals = (0..width)
            .map(|i| Proposal {
                operation: OperationId::new(100 + i as u128).unwrap(),
                bytes: vec![i as u8; i],
            })
            .collect::<Vec<_>>();
        // An exact-length source collects tight today, so owned planning does
        // not reallocate to normalize capacity. std does not promise this, so
        // it is checked here rather than asserted in `propose_batch`.
        let persist = token(&core.propose_batch(proposals).unwrap());
        let suffix = &core.pending.as_ref().unwrap().next.update().suffix;
        let entries = &suffix.as_ref().unwrap().entries;
        assert_eq!(entries.len(), width);
        assert_eq!(entries.capacity(), width);
        persist_volatile(&mut core, &log, persist).unwrap();
        assert_eq!(core.state().last_index(), 1 + width as u64);
    }
}

#[test]
fn volatile_plan_moves_the_input_suffix_allocation() {
    // `CommandBytes` is shared, so payload pointers survive an entry clone;
    // the suffix buffer itself does not. A copying plan fails this check.
    fn plan(core: &mut Raft) -> (Vec<Effect>, *const LogEntry, *const LogEntry) {
        let entries = (2..=3)
            .map(|index| LogEntry {
                index,
                term: 1,
                payload: EntryPayload::Command {
                    operation: OperationId::new(u128::from(index)).unwrap(),
                    bytes: vec![index as u8].into(),
                },
            })
            .collect::<Vec<_>>();
        assert_eq!(entries.capacity(), entries.len());
        let input = entries.as_ptr();
        core.role = Role::Follower;
        let effects = core
            .persist(
                core.state().hard_state,
                1,
                Some(Suffix { from: 2, entries }),
                After::Reply,
                None,
            )
            .unwrap();
        let pending = core.pending.as_ref().unwrap();
        let planned = &pending.next.update().suffix.as_ref().unwrap().entries;
        assert_eq!(planned.len(), 2);
        (effects, input, planned.as_ptr())
    }
    let (mut core, log) = volatile();
    let (effects, input, planned) = plan(&mut core);
    assert_eq!(planned, input);
    persist_volatile(&mut core, &log, token(&effects)).unwrap();
    assert_eq!(core.state().last_index(), 3);
    // Control: the durable seam keeps a copy for its store in another buffer.
    let mut durable_core = membership_tests::core();
    let (effects, input, planned) = plan(&mut durable_core);
    assert!(matches!(&effects[..], [Effect::Persist(_)]));
    assert_ne!(planned, input);
}

#[test]
fn entry_batches_reach_the_volatile_plan_in_their_own_allocation() {
    // A tight builder's buffer is the planned suffix: no proposal-to-entry
    // collect and no copy. A spare-capacity buffer is normalized as before.
    for (capacity, width) in [(4, 4), (8, 3)] {
        let (mut core, log) = volatile();
        let mut batch = core.proposal_batch(capacity).unwrap();
        for n in 0..width {
            batch
                .push(OperationId::new(200 + n as u128).unwrap(), vec![n as u8; n])
                .unwrap();
        }
        let (input, allocated) = batch.allocation();
        assert_eq!(allocated, capacity);
        let persist = token(&core.propose_entries(batch).unwrap());
        let suffix = &core.pending.as_ref().unwrap().next.update().suffix;
        let entries = &suffix.as_ref().unwrap().entries;
        assert_eq!(entries.len(), width);
        assert_eq!(entries.capacity(), width);
        if capacity == width {
            assert_eq!(entries.as_ptr(), input);
        }
        persist_volatile(&mut core, &log, persist).unwrap();
        assert_eq!(core.state().last_index(), 1 + width as u64);
    }
}
