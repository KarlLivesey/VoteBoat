// SPDX-License-Identifier: RPL-1.5
// Copyright (c) 2026 Karl Livesey
//! In-place recovery replay matches the atomic staged apply exactly.
use super::*;
use crate::log::replay_batch;

fn batches() -> Vec<Vec<LogMutation>> {
    let mut store = open(2);
    let mut out = vec![vec![
        LogMutation::Create(bootstrap(1)),
        LogMutation::Create(bootstrap(2)),
    ]];
    for round in 0..40u64 {
        let mut batch = Vec::new();
        for id in [1, 2] {
            let s = store.state(group(id)).unwrap();
            let next = s.last_index() + 1;
            let term = 1 + round / 10;
            let commit = s.last_index().min(s.commit_index + 2);
            let entries = (0..1 + round % 3).map(|i| entry(next + i, term)).collect();
            let mut update = mutation_raw(
                &s,
                commit,
                Some(Suffix {
                    from: next,
                    entries,
                }),
            );
            let LogMutation::Update(u) = &mut update else {
                unreachable!()
            };
            u.hard_state.term = term;
            batch.push(update);
        }
        commit(&mut store, batch.clone());
        out.push(batch);
    }
    out
}

#[test]
fn replay_matches_atomic_apply_for_valid_and_invalid_batches() {
    let limits = LogLimits::default();
    let mut staged = BTreeMap::new();
    let mut replayed = BTreeMap::new();
    for batch in batches() {
        apply_batch(&mut staged, &batch, limits).unwrap();
        replay_batch(&mut replayed, &batch, limits).unwrap();
        assert_eq!(staged, replayed);
    }
    let s = &staged[&group(1)];
    let duplicate = vec![
        mutation_raw(s, s.commit_index, None),
        mutation_raw(s, s.commit_index, None),
    ];
    let stale = {
        let mut m = mutation_raw(s, s.commit_index, None);
        let LogMutation::Update(u) = &mut m else {
            unreachable!()
        };
        u.expected_revision = LogRevision::new(1).unwrap();
        vec![m]
    };
    for invalid in [
        duplicate,
        stale,
        vec![LogMutation::Create(bootstrap(1))],
        Vec::new(),
    ] {
        let mut a = staged.clone();
        let mut b = staged.clone();
        assert_eq!(
            apply_batch(&mut a, &invalid, limits),
            replay_batch(&mut b, &invalid, limits)
        );
        assert!(replay_batch(&mut b, &invalid, limits).is_err());
    }
}
