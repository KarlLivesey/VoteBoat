// SPDX-License-Identifier: RPL-1.5
// Copyright (c) 2026 Karl Livesey
use super::*;

#[test]
fn persisted_ballot_and_wrong_recovery_identity_preserve_the_original_binding() {
    let root = Directory::new("ballot");
    let mut store = populated(&root);
    let old = store.state(group(1)).unwrap();
    let LogMutation::Update(mut voted) = update(&old, 2, 1, None) else {
        unreachable!()
    };
    voted.hard_state.voted_for = Some(node(1));
    append(&mut store, vec![LogMutation::Update(voted)]);
    let expected = store.state(group(1)).unwrap();
    assert_eq!(expected.hard_state.voted_for, Some(node(1)));
    drop(store);
    let original = pair(&root);
    let wrong = NativeLogStore::recover(
        WitnessLogIo::open(&root.0).unwrap(),
        identity(2),
        LogLimits::default(),
    );
    assert!(matches!(wrong, Err(StorageError::WrongIdentity)));
    assert_eq!(pair(&root), original);
    let reopened = recover(&root).unwrap();
    assert_eq!(reopened.state(group(1)).unwrap(), expected);
}

#[test]
fn actual_histories_keep_pending_visibility_suffix_outcomes_and_stale_sessions() {
    let root = Directory::new("history");
    let mut store = create(&root);
    let tickets = store
        .append_batch(vec![
            LogMutation::Create(bootstrap(1, 1)),
            LogMutation::Create(bootstrap(2, 1)),
        ])
        .unwrap();
    assert!(store.state(group(1)).is_err());
    assert_eq!(store.barrier(&tickets[..1]).unwrap().tickets, tickets[..1]);
    assert_eq!(store.state(group(2)).unwrap().bootstrap, bootstrap(2, 1));
    let initial = store.state(group(1)).unwrap();
    let pending = store
        .append_batch(vec![update(
            &initial,
            1,
            1,
            Some(Suffix {
                from: 1,
                entries: vec![entry(1, 1, 17), entry(2, 1, 29)].into(),
            }),
        )])
        .unwrap();
    assert_eq!(store.state(group(1)).unwrap(), initial);
    assert!(store.reclaim(store.limits().max_wal_bytes).is_err());
    assert_eq!(store.barrier(&pending).unwrap().tickets, pending);
    let committed = store.state(group(1)).unwrap();
    assert_eq!(committed.commit_index, 1);
    assert!(store
        .append_batch(vec![update(
            &committed,
            2,
            1,
            Some(Suffix {
                from: 1,
                entries: vec![entry(1, 2, 99)].into()
            })
        )])
        .is_err());
    assert_eq!(store.state(group(1)).unwrap(), committed);
    let replaced = append(
        &mut store,
        vec![update(
            &committed,
            2,
            2,
            Some(Suffix {
                from: 2,
                entries: vec![entry(2, 2, 63)].into(),
            }),
        )],
    );
    let expected = store.state(group(1)).unwrap();
    assert_eq!(expected.entries, vec![entry(1, 1, 17), entry(2, 2, 63)]);
    assert_eq!(
        expected.hard_state,
        HardState {
            term: 2,
            voted_for: None
        }
    );
    let binding = store.binding();
    drop(store);
    let mut reopened = recover(&root).unwrap();
    assert_eq!(reopened.binding().identity, binding.identity);
    assert_eq!(reopened.binding().session.get(), binding.session.get() + 1);
    assert_eq!(reopened.state(group(1)).unwrap(), expected);
    assert_eq!(reopened.barrier(&replaced), Err(StorageError::StaleTicket));
    assert_eq!(
        reopened
            .fetch_range(group(1), expected.generation, 1, 2, 1024)
            .unwrap(),
        expected.entries
    );
    assert_eq!(reopened.state(group(2)).unwrap().bootstrap, bootstrap(2, 1));
}

#[test]
fn snapshot_binding_and_three_reclamations_preserve_the_exact_state() {
    let root = Directory::new("reclaim");
    let mut store = populated(&root);
    let old = store.state(group(1)).unwrap();
    let reference = voteboat::snapshot::SnapshotRef {
        store: identity(1),
        group: group(1),
        configuration: old.bootstrap.configuration,
        generation: SnapshotGeneration::new(1).unwrap(),
        index: 1,
        term: 1,
        application_schema: 1,
        file_bytes: 100,
        checksum: 7,
    };
    let LogMutation::Update(mut compact) = update(&old, 1, 1, None) else {
        unreachable!()
    };
    compact.snapshot = Some(reference);
    let mut foreign = compact.clone();
    foreign.snapshot.as_mut().unwrap().store = identity(2);
    assert_eq!(
        store.append_batch(vec![LogMutation::Update(foreign)]),
        Err(StorageError::WrongIdentity)
    );
    append(&mut store, vec![LogMutation::Update(compact)]);
    assert_eq!(
        store.state(group(1)).unwrap().entries,
        vec![entry(2, 1, 29)]
    );
    for term in [2, 3, 4] {
        for _ in 0..4 {
            let state = store.state(group(1)).unwrap();
            append(&mut store, vec![update(&state, term, 1, None)]);
        }
        let expected = store.state(group(1)).unwrap();
        let report = store.reclaim(store.limits().max_wal_bytes).unwrap();
        assert!(report.after_bytes < report.before_bytes);
        drop(store);
        store = recover(&root).unwrap();
        assert_eq!(store.state(group(1)).unwrap(), expected);
        assert_eq!(store.state(group(1)).unwrap().snapshot, Some(reference));
    }
}

#[test]
fn public_timing_reader_observes_two_syncs_without_per_barrier_namespace_work() {
    let root = Directory::new("timing");
    let timing = JournalTimings::default();
    let io = WitnessLogIo::create(&root.0)
        .unwrap()
        .with_timings(timing.clone());
    let mut store = NativeLogStore::create(io, identity(1), LogLimits::default()).unwrap();
    let before = timing.snapshot();
    append(&mut store, vec![LogMutation::Create(bootstrap(1, 1))]);
    let after = timing.snapshot();
    assert_eq!(after.log_sync.calls - before.log_sync.calls, 1);
    assert_eq!(after.manifest.calls - before.manifest.calls, 1);
    assert_eq!(
        after.publication.file_sync.calls - before.publication.file_sync.calls,
        1
    );
    assert_eq!(after.publication.open, before.publication.open);
    assert_eq!(after.publication.rename, before.publication.rename);
    assert_eq!(
        after.publication.directory_sync,
        before.publication.directory_sync
    );
    assert_eq!(after.log_sync.errors + after.manifest.errors, 0);
}
