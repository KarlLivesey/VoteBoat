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
//! Facts, validators, selection and containers against direct oracles.
use super::*;
use crate::identity::OperationId;
use crate::membership::{ConfigurationChange, ConfigurationRecord};

mod oracle;
use oracle::*;

/// Deterministic xorshift, so every run checks the same histories.
struct Rng(u64);
impl Rng {
    fn next(&mut self) -> u64 {
        self.0 ^= self.0 << 13;
        self.0 ^= self.0 >> 7;
        self.0 ^= self.0 << 17;
        self.0
    }
    fn below(&mut self, n: u64) -> u64 {
        self.next() % n.max(1)
    }
}

fn command(index: u64, term: u64, bytes: usize) -> LogEntry {
    LogEntry {
        index,
        term,
        payload: EntryPayload::Command {
            operation: OperationId::new(u128::from(index) + 1).unwrap(),
            bytes: vec![7; bytes].into(),
        },
    }
}
fn noop(index: u64, term: u64) -> LogEntry {
    LogEntry {
        index,
        term,
        payload: EntryPayload::Noop,
    }
}
fn configuration(index: u64, term: u64, id: u64) -> LogEntry {
    LogEntry {
        index,
        term,
        payload: EntryPayload::Configuration(Box::new(ConfigurationRecord {
            operation: OperationId::new(u128::from(index) + 1).unwrap(),
            expected: ConfigurationId::new(1).unwrap(),
            change: ConfigurationChange::Final {
                id: ConfigurationId::new(id).unwrap(),
            },
        })),
    }
}
/// A valid history from `first`: nondecreasing terms and mixed payloads.
fn history(rng: &mut Rng, first: u64, len: usize, uniform: bool) -> Vec<LogEntry> {
    let mut term = 1 + rng.below(3);
    (0..len as u64)
        .map(|offset| {
            let index = first + offset;
            if rng.below(5) == 0 {
                term += 1;
            }
            match (uniform, rng.below(6)) {
                (true, _) => command(index, term, 0),
                (false, 0) => noop(index, term),
                (false, 1) => configuration(index, term, 2 + rng.below(4)),
                (false, _) => command(index, term, rng.below(40) as usize),
            }
        })
        .collect()
}

#[test]
fn shared_and_owned_pieces_follow_the_width_threshold() {
    let mut rng = Rng(11);
    let small: EntryBatch = history(&mut rng, 1, SHARE_MIN - 1, true).into();
    let wide: EntryBatch = history(&mut rng, 1, SHARE_MIN, true).into();
    assert!(!small.is_shared());
    assert!(wide.is_shared());
    // A shared subrange keeps the backing; an owned subrange is a copy.
    let shared = wide.slice(2..5).unwrap();
    assert!(shared.is_shared());
    assert_eq!(shared.first_backing_ptr(), wide.first_backing_ptr());
    let copied = small.slice(2..5).unwrap();
    assert_ne!(copied.first_backing_ptr(), small.first_backing_ptr());
    assert_eq!(copied, small.to_vec()[2..5]);
}

#[test]
fn range_facts_match_a_full_scan_for_every_subrange() {
    for seed in 1..=6 {
        let mut rng = Rng(seed);
        let entries = history(&mut rng, 5, 40, seed % 2 == 0);
        let batch: EntryBatch = entries.clone().into();
        for start in 0..=entries.len() {
            for end in start..=entries.len() {
                let part = batch.slice(start..end).unwrap();
                check_facts(&part, &entries[start..end]);
            }
        }
    }
}

#[test]
fn joined_pieces_keep_exact_facts_and_refuse_broken_joins() {
    let mut rng = Rng(5);
    let entries = history(&mut rng, 1, 60, false);
    let mut log = EntryLog::default();
    for chunk in [0..3, 3..20, 20..21, 21..40, 40..60] {
        log.append(entries[chunk].to_vec().into());
    }
    for start in 0..60 {
        for end in start..=60 {
            let joined = log.shared_range(start..end).unwrap();
            assert_eq!(joined, entries[start..end]);
            check_facts(&joined, &entries[start..end]);
        }
    }
    // A gap or a term decrease across a join removes the facts; validators
    // then follow the original loop on the visible entries.
    let mut gap = EntryLog::from(entries[..20].to_vec());
    gap.append(entries[21..40].to_vec().into());
    let broken = gap.to_batch();
    assert!(broken.summary().is_none());
    check_validators(
        &broken,
        &[entries[..20].to_vec(), entries[21..40].to_vec()].concat(),
    );
}

#[test]
fn malformed_content_is_kept_and_validated_entry_by_entry() {
    let mut rng = Rng(9);
    for defect in 0..4 {
        let mut entries = history(&mut rng, 3, 30, defect % 2 == 0);
        match defect {
            0 => entries[10].index += 1,
            1 => entries[0].term = 0,
            2 => entries[12].term = 0,
            _ => entries[20].term = entries[19].term.saturating_sub(1),
        }
        let batch: EntryBatch = entries.clone().into();
        assert!(batch.summary().is_none(), "defect {defect}");
        check_validators(&batch, &entries);
        for start in [0, 5, 11, 21] {
            let part = batch.slice(start..entries.len()).unwrap();
            check_validators(&part, &entries[start..]);
        }
    }
}

#[test]
fn replication_selection_matches_the_original_loop() {
    let mut rng = Rng(21);
    for case in 0..12 {
        let mut log = EntryLog::default();
        let mut all = Vec::new();
        let mut next = 1;
        while all.len() < 80 {
            let width = 1 + rng.below(30) as usize;
            let batch = history(&mut rng, next, width, case % 3 == 0);
            next += width as u64;
            all.extend(batch.iter().cloned());
            log.append(batch.into());
        }
        for offset in 0..all.len() {
            for max_entries in [1, 3, 17, 1024] {
                for budget in [256, 292, 293, 330, 600, 2_000, 1 << 20] {
                    let selected = log.replication_range(offset, max_entries, budget);
                    let expected = old_selection(&all[offset..], max_entries, budget);
                    assert_eq!(selected, expected, "{case} {offset} {max_entries} {budget}");
                }
            }
        }
    }
}

#[test]
fn log_operations_match_a_vector_model() {
    let mut rng = Rng(33);
    let mut log = EntryLog::default();
    let mut model: Vec<LogEntry> = Vec::new();
    let mut next = 1;
    for step in 0..400 {
        match rng.below(10) {
            0..=4 => {
                let width = 1 + rng.below(24) as usize;
                let batch = history(&mut rng, next, width, step % 2 == 0);
                next += width as u64;
                model.extend(batch.iter().cloned());
                log.append(batch.into());
            }
            5 | 6 => {
                let count = rng.below(model.len() as u64 + 1) as usize / 3;
                model.drain(..count);
                log.trim_prefix(count);
            }
            7 => {
                let keep = rng.below(model.len() as u64 + 1) as usize;
                model.truncate(keep);
                log.truncate(keep);
                next = model.last().map_or(next, |entry| entry.index + 1);
            }
            8 => {
                let (a, b) = (model.len() / 3, model.len());
                let shared = log.shared_range(a..b).unwrap();
                let mut copy = log.clone();
                copy.truncate(a);
                copy.append(shared);
                assert_eq!(copy, log);
            }
            _ => assert_eq!(log.pop(), model.pop()),
        }
        check_log(&log, &model);
    }
}

#[test]
fn retained_charges_equal_a_fresh_recount() {
    let mut rng = Rng(44);
    let mut log = EntryLog::default();
    let mut next = 1;
    for step in 0..120 {
        let width = 1 + rng.below(40) as usize;
        let batch: EntryBatch = history(&mut rng, next, width, step % 2 == 0).into();
        next += width as u64;
        let before = log.retained_bytes().unwrap();
        let predicted = log.appended_charge(before, &batch);
        log.append(batch);
        assert_eq!(predicted, log.retained_bytes());
        // An adjoining shared range coalesces and is not charged twice.
        let len = log.len();
        let tail = log.shared_range(len.saturating_sub(3)..len).unwrap();
        let mut trimmed = log.clone();
        trimmed.truncate(len.saturating_sub(3));
        let before = trimmed.retained_bytes().unwrap();
        let predicted = trimmed.appended_charge(before, &tail);
        trimmed.append(tail);
        assert_eq!(predicted, trimmed.retained_bytes());
        for count in [0, 1, len / 2, len] {
            let mut copy = log.clone();
            copy.trim_prefix(count);
            assert_eq!(
                log.trimmed_charge(count),
                copy.retained_bytes(),
                "{step} {count}"
            );
        }
    }
}

#[test]
fn holders_keep_their_contents_while_the_log_changes() {
    let mut rng = Rng(55);
    let entries = history(&mut rng, 1, 64, false);
    let mut log = EntryLog::from(entries.clone());
    let sent = log.shared_range(10..50).unwrap();
    let whole = log.to_batch();
    log.truncate(20);
    log.trim_prefix(5);
    log.edit(|entries| entries[0].term += 9);
    assert_eq!(sent, entries[10..50]);
    assert_eq!(whole, entries);
    // An edited copy is a new value; the original batch is unchanged.
    let mut edited = sent.clone();
    edited.edit(|entries| entries[3].index += 1);
    assert_eq!(sent, entries[10..50]);
    assert_ne!(edited, sent);
    assert!(edited.summary().is_none());
}

#[test]
fn equality_is_logical_across_partitions() {
    let mut rng = Rng(66);
    let entries = history(&mut rng, 1, 50, false);
    let one: EntryBatch = entries.clone().into();
    let mut log = EntryLog::default();
    for chunk in [0..17, 17..18, 18..50] {
        log.append(entries[chunk].to_vec().into());
    }
    let many = log.to_batch();
    assert_eq!(one, many);
    assert_eq!(EntryLog::from(entries.clone()), log);
    let mut different = entries.clone();
    different[30].payload = EntryPayload::Noop;
    assert_ne!(EntryBatch::from(different), many);
}

#[test]
fn borrowed_intervals_iterate_and_borrow_exactly() {
    let mut rng = Rng(77);
    let entries = history(&mut rng, 1, 70, false);
    let mut log = EntryLog::default();
    for chunk in [0..20, 20..22, 22..50, 50..70] {
        log.append(entries[chunk].to_vec().into());
    }
    for start in 0..70 {
        for end in start..=70 {
            let range = log.borrowed(start..end).unwrap();
            let expected = &entries[start..end];
            assert_eq!(range, expected);
            assert_eq!(
                range.iter().rev().collect::<Vec<_>>(),
                expected.iter().rev().collect::<Vec<_>>()
            );
            assert_eq!(
                range.slices().flatten().collect::<Vec<_>>(),
                expected.iter().collect::<Vec<_>>()
            );
            assert_eq!(range.contiguous(), expected);
            assert_eq!(range.as_slice().is_some(), range.slices().count() <= 1);
            assert_eq!(range.first(), expected.first());
            assert_eq!(range.last(), expected.last());
            for offset in 0..expected.len() {
                assert_eq!(range.get(offset), expected.get(offset));
            }
        }
    }
    assert!(log.borrowed(Range { start: 5, end: 4 }).is_none());
    assert!(log.borrowed(0..71).is_none());
    assert!(log.shared_range(69..71).is_none());
}

#[test]
fn joined_overlay_charges_equal_the_materialized_log() {
    let mut rng = Rng(88);
    for case in 0..40 {
        let mut base = EntryLog::default();
        let mut next = 1;
        for _ in 0..1 + rng.below(4) {
            let width = 1 + rng.below(30) as usize;
            base.append(history(&mut rng, next, width, case % 2 == 0).into());
            next += width as u64;
        }
        // The tail may continue the base's last shared backing.
        let mut tail = EntryLog::default();
        let wide: Vec<LogEntry> = history(&mut rng, next, 40, true);
        base.append(wide[..20].to_vec().into());
        if case % 3 == 0 {
            let whole = EntryLog::from(wide.clone());
            base.truncate(base.len() - 20);
            base.append(whole.shared_range(0..20).unwrap());
            tail.append(whole.shared_range(20..40).unwrap());
        } else {
            tail.append(wide[20..].to_vec().into());
        }
        let skip = rng.below(base.len() as u64) as usize;
        let visible = base.len() - skip + tail.len();
        for count in [0, 1, visible / 2, base.len() - skip, visible] {
            let mut materialized = base.clone();
            materialized.trim_prefix(skip);
            materialized.append_log(tail.clone());
            materialized.trim_prefix(count);
            assert_eq!(
                base.joined_trimmed_charge(skip, &tail, count),
                materialized.retained_bytes(),
                "{case} {skip} {count}"
            );
        }
    }
}
