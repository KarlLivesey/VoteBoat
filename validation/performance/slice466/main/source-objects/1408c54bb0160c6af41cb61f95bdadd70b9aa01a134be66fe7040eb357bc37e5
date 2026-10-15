// SPDX-License-Identifier: RPL-1.5
// Copyright (c) 2026 Karl Livesey
// Unless explicitly acquired and licensed from Licensor under another license,
// the contents of this file are subject to the Reciprocal Public License
// ("RPL") Version 1.5, or subsequent versions as allowed by the RPL, and You may
// not copy or use this file in either source code or executable form, except
// in compliance with the terms and conditions of the RPL.
//
// All software distributed under the RPL is provided strictly on an "AS IS"
// basis, WITHOUT WARRANTY OF ANY KIND, EITHER EXPRESS OR IMPLIED, AND LICENSOR
// HEREBY DISCLAIMS ALL SUCH WARRANTIES, INCLUDING WITHOUT LIMITATION, ANY
// WARRANTIES OF MERCHANTABILITY, FITNESS FOR A PARTICULAR PURPOSE, QUIET
// ENJOYMENT, OR NON-INFRINGEMENT. See the RPL for specific language governing
// rights and limitations under the RPL.
use super::*;
fn mutation(store: &impl LogStore) -> Vec<LogMutation> {
    let s = store.state(group(1)).unwrap();
    let mut e = entry(1, 1, 17);
    if let EntryPayload::Command { bytes, .. } = &mut e.payload {
        *bytes = vec![17; 1300].into();
    }
    vec![update(
        &s,
        1,
        1,
        Some(Suffix {
            from: 1,
            entries: vec![e].into(),
        }),
    )]
}
#[test]
fn every_append_sync_and_catalog_failure_fences_and_recovers_a_complete_original_batch() {
    let (io, store) = seeded();
    let old = store.state(group(1)).unwrap();
    drop(store);
    let probe = io.fork();
    let mut s = recover(probe.clone()).unwrap();
    probe.reset_steps();
    let t = s.append_batch(mutation(&s)).unwrap();
    s.barrier(&t).unwrap();
    let expected = s.state(group(1)).unwrap();
    let steps = probe.steps();
    drop(s);
    assert!(steps >= 9);
    for step in 1..=steps {
        for after in [false, true] {
            let d = io.fork();
            let mut s = recover(d.clone()).unwrap();
            d.fail(step, after);
            let result = s
                .append_batch(mutation(&s))
                .and_then(|tickets| s.barrier(&tickets));
            assert!(
                result.is_err(),
                "step{step}/after{after} must not acknowledge"
            );
            assert_eq!(s.state(group(1)), Err(StorageError::Fenced));
            drop(s);
            d.crash();
            let recovered = recover(d).unwrap().state(group(1)).unwrap();
            assert!(
                recovered == old || recovered == expected,
                "atomic selection step{step}/after{after}"
            );
        }
    }
}
fn fill(store: &mut impl LogStore) {
    for _ in 0..24 {
        let s = store.state(group(1)).unwrap();
        append(
            store,
            vec![update(&s, s.hard_state.term + 1, s.commit_index, None)],
        );
    }
}
#[test]
fn every_reclaim_primitive_recovers_exact_state_and_safe_next_slot_reuse() {
    let (io, mut store) = seeded();
    let t = add(&mut store, 10, true);
    store.barrier(&t).unwrap();
    fill(&mut store);
    let expected = store.state(group(1)).unwrap();
    drop(store);
    let probe = io.fork();
    let mut store = recover(probe.clone()).unwrap();
    probe.reset_steps();
    let reclaimed = store.reclaim(store.limits().max_wal_bytes).unwrap();
    assert!(reclaimed.after_bytes < reclaimed.before_bytes);
    let steps = probe.steps();
    drop(store);
    for step in 1..=steps {
        for after in [false, true] {
            let d = io.fork();
            let mut store = recover(d.clone()).unwrap();
            d.fail(step, after);
            assert!(store.reclaim(store.limits().max_wal_bytes).is_err());
            assert_eq!(store.state(group(1)), Err(StorageError::Fenced));
            drop(store);
            d.crash();
            let mut store = recover(d.clone()).unwrap();
            assert_eq!(
                store.state(group(1)).unwrap(),
                expected,
                "step{step}/after{after}"
            );
            fill(&mut store);
            store.reclaim(store.limits().max_wal_bytes).unwrap();
            let next = store.state(group(1)).unwrap();
            drop(store);
            d.crash();
            assert_eq!(recover(d).unwrap().state(group(1)).unwrap(), next);
        }
    }
}
#[test]
fn catalog_budget_refuses_before_payload_side_effect_and_keeps_previous_control_selection() {
    let io = model::Io::default();
    let mut store = StripedLogStore::create_striped(
        io.clone(),
        identity(1),
        LogLimits::default(),
        StripedLimits {
            max_catalog_bytes: 256,
            control_reserve_bytes: 0,
            ..stripes()
        },
    )
    .unwrap();
    assert!(store
        .append_batch(vec![LogMutation::Create(bootstrap(1, 3))])
        .is_err());
    assert_eq!(store.state(group(1)), Err(StorageError::Fenced));
    assert_eq!(io.payload_bytes(), [0, 0]);
    drop(store);
    io.crash();
    let control = StripedJournal::inspect_control(
        &mut io.clone(),
        identity(1),
        LogLimits::default(),
        stripes(),
    )
    .unwrap();
    assert!(control.groups.is_empty());
}
