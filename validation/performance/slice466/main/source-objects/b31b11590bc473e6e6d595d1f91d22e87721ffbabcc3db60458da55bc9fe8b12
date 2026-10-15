// SPDX-License-Identifier: RPL-1.5
// Copyright (c) 2026 Karl Livesey
use super::*;
use std::collections::BTreeMap;

#[test]
fn borrowed_equality_reads_durable_not_pending_and_survives_subset_barrier_reopen() {
    let io = ModelIo::default();
    let mut store = NativeLogStore::create(io.clone(), identity(1), LogLimits::default()).unwrap();
    let create = store
        .append_batch(vec![LogMutation::Create(bootstrap(1, 3))])
        .unwrap();
    let mut expected = BTreeMap::new();
    apply_batch(
        &mut expected,
        &[LogMutation::Create(bootstrap(1, 3))],
        store.limits(),
    )
    .unwrap();
    assert_eq!(
        store.matches_state(group(1), &expected[&group(1)]),
        Err(StorageError::Rejected("group not durably bootstrapped"))
    );
    store.barrier(&create).unwrap();
    append(&mut store, vec![LogMutation::Create(bootstrap(2, 3))]);
    let old = [
        store.state(group(1)).unwrap(),
        store.state(group(2)).unwrap(),
    ];
    let mut expected = old
        .iter()
        .map(|s| (s.bootstrap.group, s.clone()))
        .collect::<BTreeMap<_, _>>();
    let mutations = old
        .iter()
        .map(|s| {
            update(
                s,
                1,
                0,
                Some(Suffix {
                    from: 1,
                    entries: vec![entry(1, 1, 7)].into(),
                }),
            )
        })
        .collect::<Vec<_>>();
    apply_batch(&mut expected, &mutations, store.limits()).unwrap();
    let tickets = store.append_batch(mutations).unwrap();
    for state in &old {
        assert_eq!(store.matches_state(state.bootstrap.group, state), Ok(true));
        assert_eq!(
            store.matches_state(state.bootstrap.group, &expected[&state.bootstrap.group]),
            Ok(false)
        );
    }
    // Native subset barriers flush all written groups but return only requested evidence.
    assert_eq!(store.barrier(&tickets[..1]).unwrap().tickets, tickets[..1]);
    for (&group, state) in &expected {
        assert_eq!(store.matches_state(group, state), Ok(true));
    }
    store.barrier(&tickets[1..]).unwrap();
    drop(store);
    io.0.borrow_mut().power_loss();
    let recovered = NativeLogStore::recover(io, identity(1), LogLimits::default()).unwrap();
    for (&group, state) in &expected {
        assert_eq!(recovered.matches_state(group, state), Ok(true));
    }
}
#[test]
fn equality_fencing_precedes_missing_and_recovery_retains_exact_crash_state() {
    for fault in [
        Fault::Append(10),
        Fault::Sync,
        Fault::PublishBefore,
        Fault::PublishAfter,
    ] {
        let io = ModelIo::default();
        let mut store =
            NativeLogStore::create(io.clone(), identity(1), LogLimits::default()).unwrap();
        append(&mut store, vec![LogMutation::Create(bootstrap(1, 3))]);
        let old = store.state(group(1)).unwrap();
        io.0.borrow_mut().fault = fault;
        let transition = update(
            &old,
            1,
            0,
            Some(Suffix {
                from: 1,
                entries: vec![entry(1, 1, 7)].into(),
            }),
        );
        let mut next = BTreeMap::from([(group(1), old.clone())]);
        apply_batch(&mut next, std::slice::from_ref(&transition), store.limits()).unwrap();
        match store.append_batch(vec![transition]) {
            Err(StorageError::Uncertain(_)) => (),
            Ok(tickets) => assert!(matches!(
                store.barrier(&tickets),
                Err(StorageError::Uncertain(_))
            )),
            other => panic!("unexpected {other:?}"),
        }
        for group in [group(1), group(99)] {
            assert_eq!(store.matches_state(group, &old), Err(StorageError::Fenced));
            assert_eq!(store.state(group), Err(StorageError::Fenced));
        }
        drop(store);
        io.0.borrow_mut().power_loss();
        let recovered = NativeLogStore::recover(io, identity(1), LogLimits::default()).unwrap();
        let actual = recovered.state(group(1)).unwrap();
        assert!(actual == old || actual == next[&group(1)]);
        assert_eq!(recovered.matches_state(group(1), &old), Ok(actual == old));
        assert_eq!(
            recovered.matches_state(group(1), &next[&group(1)]),
            Ok(actual == next[&group(1)])
        );
        assert_eq!(
            recovered.matches_state(group(99), &old),
            Err(StorageError::Rejected("group not durably bootstrapped"))
        );
    }
}
