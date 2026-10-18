// SPDX-License-Identifier: RPL-1.5
// Copyright (c) 2026 Karl Livesey
//! Commit-only rounds plan a scalar advance; durable stores get the same update.
use super::*;
use completion_tests::ModeledStore;
use membership_tests::core;
use std::mem::size_of;

fn command(index: u64, operation: u128) -> LogEntry {
    LogEntry {
        index,
        term: 1,
        payload: EntryPayload::Command {
            operation: OperationId::new(operation).unwrap(),
            bytes: vec![index as u8; 3].into(),
        },
    }
}

fn token(effects: &[Effect]) -> VolatilePersist {
    let [Effect::PersistVolatile(token)] = effects else {
        panic!("one payload-free persistence token, got {effects:?}")
    };
    *token
}

fn update(effects: Vec<Effect>) -> LogUpdate {
    let [Effect::Persist(update)]: [Effect; 1] = effects.try_into().unwrap() else {
        panic!("one durable persistence effect")
    };
    update
}

/// A follower holding uncommitted entries 2 and 3, with commit index 1.
fn follower(volatile: bool) -> (Raft, VolatileLog) {
    let mut core = core();
    if volatile {
        core.volatile = true;
        core.incarnation = Raft::next_volatile_incarnation().unwrap();
    }
    core.role = Role::Follower;
    let log = VolatileLog::new(core.binding, core.limits).unwrap();
    let effects = core
        .persist(
            core.state().hard_state,
            1,
            Some(Suffix {
                from: 2,
                entries: vec![command(2, 9), command(3, 10)],
            }),
            After::Reply,
            None,
        )
        .unwrap();
    assert!(!core.pending.as_ref().unwrap().next.is_commit());
    if volatile {
        persist_volatile(&mut core, &log, token(&effects)).unwrap();
    } else {
        membership_tests::durable(&mut core, effects);
    }
    assert_eq!(core.state().commit_index, 1);
    (core, log)
}

fn commit_only(core: &mut Raft, commit: u64) -> Vec<Effect> {
    core.persist(core.state().hard_state, commit, None, After::Commit, None)
        .unwrap()
}

/// The completed state a commit-only round must produce: only the revision and
/// commit index change.
fn advanced(before: &GroupLog, commit: u64) -> GroupLog {
    let mut expected = before.clone();
    expected.revision = LogRevision::new(before.revision.get() + 1).unwrap();
    expected.commit_index = commit;
    expected
}

#[test]
fn volatile_commit_only_round_is_scalar_and_installs_only_the_commit_index() {
    let (mut core, log) = follower(true);
    let before = core.state().clone();
    let entries = core.state().entries.as_ptr();
    let persist = token(&commit_only(&mut core, 3));
    assert_eq!(persist.revision().get(), before.revision.get() + 1);
    assert_eq!(persist.generation(), before.generation);
    let pending = core.pending.as_ref().unwrap();
    assert!(pending.next.is_commit());
    // No update, suffix or entry copy is retained by the plan.
    assert!(pending.next.suffix().is_none());
    assert!(pending.next.entry_at(2).is_none());
    assert!(pending.membership.is_none());
    assert_eq!(core.state(), &before);
    let result = persist_volatile(&mut core, &log, persist).unwrap();
    let committed = result.committed().unwrap();
    assert_eq!(committed.len(), 2);
    assert_eq!(committed.as_ptr(), entries.wrapping_add(1));
    assert!(result.following().is_empty());
    assert_eq!(core.state(), &advanced(&before, 3));
    assert_eq!(core.state().entries.as_ptr(), entries);
    assert_eq!(core.log_summary, LogSummary::from_validated(core.state()));
    assert!(!core.has_pending_dependency());
}

#[test]
fn durable_commit_only_round_emits_the_identical_update_and_completes_through_the_store() {
    let (mut core, _) = follower(false);
    let before = core.state().clone();
    let mut store = ModeledStore::new(&core);
    let effects = commit_only(&mut core, 3);
    let expected = LogUpdate {
        snapshot_membership: None,
        group: before.bootstrap.group,
        expected_revision: before.revision,
        hard_state: before.hard_state,
        commit_index: 3,
        suffix: None,
        snapshot: None,
    };
    assert_eq!(effects, vec![Effect::Persist(expected.clone())]);
    assert!(core.pending.as_ref().unwrap().next.is_commit());
    core.validate_persist_effect(&expected).unwrap();
    let mut wrong = vec![expected.clone(); 4];
    wrong[0].commit_index = 2;
    wrong[1].expected_revision = LogRevision::new(before.revision.get() + 1).unwrap();
    wrong[2].hard_state.term += 1;
    wrong[3].suffix = Some(Suffix {
        from: 4,
        entries: Vec::new(),
    });
    for update in &wrong {
        assert_eq!(
            core.validate_persist_effect(update),
            Err(RaftError::WrongCompletion)
        );
    }
    assert!(!core.is_fenced());
    // The store validates and applies the same update with its full path.
    let result = persist_effect_borrowed(&mut core, &mut store, update(effects)).unwrap();
    assert_eq!(result.committed().map(<[LogEntry]>::len), Some(2));
    assert!(result.following().is_empty());
    assert_eq!(core.state(), &advanced(&before, 3));
    assert_eq!(core.state(), &store.state(before.bootstrap.group).unwrap());
    assert_eq!(core.log_summary, LogSummary::from_validated(core.state()));
    assert_eq!(store.submissions, 1);
}

#[test]
fn incomplete_commit_only_round_fences_and_keeps_the_last_completed_state() {
    // Volatile: a wrong binding fences; the completed state recovers.
    let (mut core, log) = follower(true);
    let completed = core.state().clone();
    let persist = token(&commit_only(&mut core, 3));
    let mut other = log.binding();
    other.session = StoreSession::new(2).unwrap();
    let wrong = VolatileLog::new(other, log.limits()).unwrap();
    assert!(matches!(
        persist_volatile(&mut core, &wrong, persist),
        Err(RaftError::WrongIdentity)
    ));
    assert!(core.is_fenced());
    assert!(!core.has_pending_dependency());
    assert_eq!(core.state(), &completed);
    assert!(matches!(
        persist_volatile(&mut core, &log, persist),
        Err(RaftError::Fenced)
    ));
    let mut recovered = Raft::recover_volatile(core.node, &log, core.state().clone()).unwrap();
    recovered.role = Role::Follower;
    let persist = token(&commit_only(&mut recovered, 3));
    persist_volatile(&mut recovered, &log, persist).unwrap();
    assert_eq!(recovered.state(), &advanced(&completed, 3));
    // Durable: a failed barrier fences after submission, before install.
    let (mut core, _) = follower(false);
    let completed = core.state().clone();
    let mut store = ModeledStore::new(&core);
    store.fail_barrier = true;
    let effects = commit_only(&mut core, 3);
    assert!(matches!(
        persist_effect_borrowed(&mut core, &mut store, update(effects)),
        Err(RaftError::Storage(StorageError::Uncertain(_)))
    ));
    assert!(core.is_fenced());
    assert!(!core.has_pending_dependency());
    assert_eq!(core.state(), &completed);
    // A failed install fences too: the predecessor check precedes mutation.
    let (mut core, log) = follower(true);
    let persist = token(&commit_only(&mut core, 3));
    core.durable.hard_state.voted_for = Some(core.node);
    let moved = core.state().clone();
    assert!(matches!(
        persist_volatile(&mut core, &log, persist),
        Err(RaftError::Storage(StorageError::Rejected(_)))
    ));
    assert!(core.is_fenced());
    assert!(!core.has_pending_dependency());
    assert_eq!(core.state(), &moved);
}

#[test]
fn stale_or_forged_commit_only_token_is_refused() {
    let (mut core, log) = follower(true);
    let first = token(&commit_only(&mut core, 2));
    persist_volatile(&mut core, &log, first).unwrap();
    let completed = core.state().clone();
    let second = token(&commit_only(&mut core, 3));
    assert_ne!(first, second);
    assert!(matches!(
        persist_volatile(&mut core, &log, first),
        Err(RaftError::WrongCompletion)
    ));
    assert!(core.is_fenced());
    assert!(!core.has_pending_dependency());
    assert_eq!(core.state(), &completed);
    // The right revision with another generation is refused as well.
    let (mut core, log) = follower(true);
    let completed = core.state().clone();
    let exact = token(&commit_only(&mut core, 3));
    let forged = VolatilePersist {
        incarnation: exact.incarnation,
        revision: exact.revision,
        generation: LogGeneration::new(exact.generation.get() + 1).unwrap(),
    };
    assert!(matches!(
        persist_volatile(&mut core, &log, forged),
        Err(RaftError::WrongCompletion)
    ));
    assert!(core.is_fenced());
    assert_eq!(core.state(), &completed);
}

#[test]
fn ballot_entries_or_snapshot_rounds_do_not_take_the_scalar_path() {
    for volatile in [false, true] {
        // A new term.
        let (mut core, _) = follower(volatile);
        let hard = HardState {
            term: core.state().hard_state.term + 1,
            voted_for: None,
        };
        let effects = core.persist(hard, 3, None, After::Commit, None).unwrap();
        let next = &core.pending.as_ref().unwrap().next;
        assert!(!next.is_commit());
        assert_eq!(next.update().hard_state, hard);
        assert_eq!(next.metadata().hard_state, hard);
        assert_eq!(
            matches!(&effects[..], [Effect::PersistVolatile(_)]),
            volatile
        );
        // A new vote in the same term.
        let (mut core, _) = follower(volatile);
        let hard = HardState {
            voted_for: Some(NodeId::new(2).unwrap()),
            ..core.state().hard_state
        };
        core.persist(hard, 3, None, After::Commit, None).unwrap();
        assert!(!core.pending.as_ref().unwrap().next.is_commit());
        // New entries with an unchanged ballot.
        let (mut core, _) = follower(volatile);
        core.persist(
            core.state().hard_state,
            3,
            Some(Suffix {
                from: 4,
                entries: vec![command(4, 11)],
            }),
            After::Reply,
            None,
        )
        .unwrap();
        let next = &core.pending.as_ref().unwrap().next;
        assert!(next.is_append() && !next.is_commit());
        // Compaction keeps the ballot and commit index but carries a snapshot.
        let (mut core, log) = follower(volatile);
        let effects = commit_only(&mut core, 3);
        if volatile {
            persist_volatile(&mut core, &log, token(&effects)).unwrap();
        } else {
            membership_tests::durable(&mut core, effects);
        }
        let reference = SnapshotRef {
            store: core.binding.identity,
            group: core.state().bootstrap.group,
            configuration: core.membership().id(),
            generation: SnapshotGeneration::new(1).unwrap(),
            index: 3,
            term: 1,
            application_schema: 1,
            file_bytes: 128,
            checksum: 7,
        };
        let commit = core.state().commit_index;
        core.begin_compact(reference).unwrap();
        let next = &core.pending.as_ref().unwrap().next;
        assert!(!next.is_commit());
        assert_eq!(next.update().snapshot, Some(reference));
        assert_eq!(next.metadata().commit_index, commit);
    }
}

#[test]
fn pending_and_plan_sizes() {
    // Measured on x86_64 before V2: Pending 1,728 B, OwnedNextLog 912 B,
    // After 144 B. The bounds are the sizes after V2 on the same target.
    let pending = size_of::<Pending>();
    // The core holds `Option<Pending>`. Rust does not guarantee a niche, so
    // bound it by at most one aligned tag word rather than equality.
    assert!(size_of::<Option<Pending>>() <= PENDING_BYTES + align_of::<Pending>());
    let plan = size_of::<OwnedNextLog>();
    assert!(
        pending <= PENDING_BYTES && plan <= PLAN_BYTES,
        "Pending is {pending} B, OwnedNextLog is {plan} B"
    );
    assert!(size_of::<After>() <= 16);
    assert!(size_of::<Option<Box<Membership>>>() <= 8);
}
const PENDING_BYTES: usize = 1_088;
const PLAN_BYTES: usize = 672;
