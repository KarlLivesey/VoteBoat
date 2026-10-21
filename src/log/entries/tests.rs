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

#[test]
fn small_owned_pieces_take_the_original_loops_exactly() {
    let mut rng = Rng(99);
    for width in 1..SHARE_MIN {
        for defect in 0..5 {
            let mut entries = history(&mut rng, 4, width, defect % 2 == 0);
            let last = width - 1;
            match defect {
                0 => {}
                1 => entries[last].index += 1,
                2 => entries[0].term = 0,
                3 => entries[last].term = 0,
                _ => entries[last].term = entries[last.saturating_sub(1)].term.saturating_sub(1),
            }
            let batch: EntryBatch = entries.clone().into();
            assert!(!batch.is_shared());
            if defect == 0 {
                check_facts(&batch, &entries);
            } else {
                check_validators(&batch, &entries);
            }
            // The one-walk message charge equals its two separate queries.
            assert_eq!(
                batch.retained_bytes_and_data(),
                Some((batch.retained_bytes().unwrap(), batch.has_data()))
            );
            assert_eq!((batch.len(), batch.is_empty()), (width, false));
            assert_eq!(
                (batch.first(), batch.last()),
                (entries.first(), entries.last())
            );
            for offset in 0..=width {
                assert_eq!(batch.get(offset), entries.get(offset));
            }
        }
    }
    let empty = EntryBatch::default();
    assert_eq!((empty.len(), empty.is_empty()), (0, true));
    assert_eq!(empty.retained_bytes_and_data(), Some((0, false)));
    let shared: EntryBatch = history(&mut rng, 1, 40, false).into();
    assert_eq!(
        shared.retained_bytes_and_data(),
        Some((shared.retained_bytes().unwrap(), shared.has_data()))
    );
}

#[test]
fn a_last_owned_piece_answers_like_every_other_piece() {
    let mut rng = Rng(101);
    // A shared piece, then small batches gathered into one owned last piece.
    let mut log = EntryLog::default();
    let mut all = history(&mut rng, 1, 30, false);
    log.append(all.clone().into());
    while all.len() < 60 {
        let width = 1 + rng.below(SHARE_MIN as u64 - 1) as usize;
        let batch = history(&mut rng, all.len() as u64 + 1, width, false);
        all.extend(batch.iter().cloned());
        log.append(batch.into());
        check_log(&log, &all);
    }
    let many = log.to_batch();
    assert_eq!(many.backing_count(), 2);
    assert_eq!((many.len(), many.is_empty()), (all.len(), false));
    for start in 0..=all.len() {
        for end in start..=all.len() {
            assert_eq!(log.borrowed(start..end).unwrap(), all[start..end]);
        }
        for max_entries in [0, 1, 2, 5, 1024] {
            for budget in [256, 292, 293, 330, 600, 1 << 20] {
                assert_eq!(
                    log.replication_range(start, max_entries, budget),
                    old_selection(all.get(start..).unwrap_or(&[]), max_entries, budget),
                    "{start} {max_entries} {budget}"
                );
            }
        }
    }
    assert!(log.borrowed(0..all.len() + 1).is_none());
}

/// What a plain vector of exactly `entries` is charged: records and retained
/// payloads, as main charged every Append.
fn plain_charge(entries: &[LogEntry]) -> usize {
    facts::charge_of(entries, entries.len()).unwrap()
}

#[test]
fn a_narrow_message_from_a_wide_backing_is_charged_like_a_plain_copy() {
    // A recovered log: one backing of 16,384 eight-byte commands, whose whole
    // charge exceeds the default 1 MiB outbound batch budget.
    let entries: Vec<_> = (1..=16_384).map(|index| command(index, 1, 8)).collect();
    let log = EntryLog::from(entries.clone());
    assert_eq!(log.to_batch().backing_count(), 1);
    assert!(log.retained_bytes().unwrap() > 1 << 20);
    for (offset, count) in [(0, 1), (8_000, 1), (16_383, 1), (100, 64), (0, 1024)] {
        let visible = &entries[offset..offset + count];
        let message = log.replication_range(offset, count, 1 << 20);
        assert_eq!(message, visible);
        assert_eq!(
            message.retained_bytes_and_data(),
            Some((plain_charge(visible), true)),
            "{offset} {count}"
        );
        // A retry of the same range is held the same way.
        let retry = log
            .shared_range(offset..offset + count)
            .unwrap()
            .for_message(1 << 20);
        assert_eq!(retry, visible);
        assert_eq!(retry.retained_bytes(), Some(plain_charge(visible)));
    }
    // Whole frozen proposals are still shared: small spare capacity is kept
    // (and charged), larger spare capacity was released when they froze.
    for (capacity, kept) in [(100, 36), (1_000, 0)] {
        let mut log = EntryLog::default();
        for first in (1..=256).step_by(64) {
            let mut batch = Vec::with_capacity(capacity);
            batch.extend((first..first + 64).map(|index| command(index, 1, 8)));
            log.append(batch.into());
        }
        let message = log.replication_range(64, 128, 1 << 20);
        assert!(message.is_shared());
        assert_eq!(message.backing_count(), 2);
        assert_eq!(message.allocated_entries(), 128 + 2 * kept);
        assert_eq!(
            message.retained_bytes(),
            Some(plain_charge(&log.to_vec()[64..192]) + 2 * kept * std::mem::size_of::<LogEntry>())
        );
    }
}

/// What the outbound queue charges one Append beyond its retained entries:
/// the message itself and the batch header.
const ENVELOPE: usize = std::mem::size_of::<crate::raft::Message>() + 256;

#[test]
fn recovered_backings_never_push_an_append_over_the_outbound_budget() {
    // A recovered log is one backing: a Noop, then uniform commands. Near the
    // byte limit, the hidden rest of the backing must not fill the selector's
    // unused wire allowance (131 × 7,900 B once cost 1,049,160 B).
    let budget = 1 << 20;
    let mut kept = 0;
    for size in (2_000..=16_000).step_by(100) {
        for extra in [0, 2, 4, 8, 16, 64] {
            let count = (budget - 256) / (37 + size) + extra;
            let mut entries = vec![noop(1, 1)];
            entries.extend((2..=count as u64 + 1).map(|index| command(index, 1, size)));
            let log = EntryLog::from(entries.clone());
            assert_eq!(log.to_batch().backing_count(), 1);
            for offset in [1, count.saturating_sub(64).max(1)] {
                let message = log.replication_range(offset, 64, budget);
                let visible = &entries[offset..offset + message.len()];
                assert_eq!(message, visible);
                let plain = plain_charge(visible);
                let charge = message.retained_bytes().unwrap();
                assert!(
                    charge == plain || ENVELOPE + charge <= budget,
                    "{count} × {size} from {offset}: {charge} (plain {plain})"
                );
                kept += usize::from(charge > plain);
            }
        }
    }
    // Sharing still happens where the headroom allows it.
    assert!(kept > 0);
}

#[test]
fn hidden_bytes_are_kept_exactly_up_to_the_outbound_envelope() {
    // One uniform backing of 40 × 1,000 B commands; the message shows 20.
    // A plain copy retains 20 × (64 + 1,016) = 21,600 B and the backing
    // hides as much again.
    let entries: Vec<_> = (1..=40).map(|index| command(index, 1, 1_000)).collect();
    let log = EntryLog::from(entries.clone());
    assert_eq!(log.to_batch().backing_count(), 1);
    let (start, end) = (10, 30);
    let visible = &entries[start..end];
    let plain = plain_charge(visible);
    let hidden = log.to_batch().retained_bytes().unwrap() - plain;
    assert_eq!((plain, hidden), (21_600, 21_600));
    // The selector's upper bound of a plain copy: a record per entry, and
    // each payload with at most 23 B beyond its length. Written out here, so
    // that dropping either envelope term or the 23 B moves the edge below.
    let copy_max = visible.len() * (std::mem::size_of::<LogEntry>() + 23) + 20_000;
    let edge = ENVELOPE + copy_max + hidden;
    let message = |budget| log.shared_range(start..end).unwrap().for_message(budget);
    // At the edge the backing is kept, and the Append sent alone still fits.
    let kept = message(edge);
    assert_eq!(kept, visible);
    assert!(kept.is_shared());
    let charge = kept.retained_bytes().unwrap();
    assert_eq!(charge, plain + hidden);
    assert!(ENVELOPE + charge <= edge);
    // Across the 512 B below the edge, no kept hidden byte pushes the
    // Append over the budget.
    for budget in edge - 512..=edge {
        let message = message(budget);
        assert_eq!(message, visible);
        let charge = message.retained_bytes().unwrap();
        assert!(
            charge == plain || ENVELOPE + charge <= budget,
            "{budget}: {charge} (plain {plain})"
        );
        // The replication path selects and holds the same message.
        let selected = log.replication_range(start, end - start, budget);
        assert_eq!(selected.retained_bytes(), Some(charge), "{budget}");
    }
    // One byte below the edge, the visible entries are copied.
    let copied = message(edge - 1);
    assert_eq!(copied, visible);
    assert_eq!(copied.retained_bytes(), Some(plain));
}

#[test]
fn messages_keep_at_most_the_bounded_hidden_charge() {
    let mut rng = Rng(55);
    for case in 0..8 {
        // Backings from 1 to 600 entries, uniform or mixed, some truncated.
        let mut log = EntryLog::default();
        let mut all = Vec::new();
        let mut next = 1;
        while all.len() < 1_500 {
            let width = 1 + rng.below(600) as usize;
            let mut batch = history(&mut rng, next, width, case % 2 == 0);
            if rng.below(4) == 0 {
                batch.truncate(1 + width / 2);
            }
            next += batch.len() as u64;
            all.extend(batch.iter().cloned());
            log.append(batch.into());
        }
        for offset in (0..all.len()).step_by(7) {
            for max_entries in [1, 17, 64, 1024] {
                // Each budget is both `max_batch_bytes` and the outbound
                // batch budget.
                for budget in [600, 2_000, 40_000, 1 << 20] {
                    let message = log.replication_range(offset, max_entries, budget);
                    let visible = &all[offset..offset + message.len()];
                    assert_eq!(message, visible);
                    let used = message.len() * 37 + message.payload_bytes();
                    let room = (budget - 256).saturating_sub(used);
                    let plain = plain_charge(visible);
                    let charge = message.retained_bytes().unwrap();
                    let hidden = charge - plain;
                    let allowance = room.min(MESSAGE_HIDDEN_MAX + 2 * plain);
                    assert!(hidden <= allowance, "{case} {offset}");
                    // Kept hidden bytes never push the message's batch charge
                    // over the outbound budget.
                    assert!(
                        hidden == 0 || ENVELOPE + charge <= budget,
                        "{case} {offset} {max_entries} {budget}: {hidden} {charge}"
                    );
                    let end = offset + message.len();
                    let retry = log.shared_range(offset..end).unwrap().for_message(budget);
                    assert_eq!(retry.retained_bytes(), message.retained_bytes());
                }
            }
        }
    }
}

#[test]
fn hidden_charges_are_exact_where_claimed_and_never_low() {
    for uniform in [true, false] {
        let mut rng = Rng(66);
        let entries = history(&mut rng, 1, 40, uniform);
        let batch: EntryBatch = entries.clone().into();
        let Spans::One(span @ Span::Shared { .. }) = &batch.spans else {
            panic!("a wide batch is shared")
        };
        let whole = span.log_charge().unwrap();
        for start in 0..=entries.len() {
            for end in start..=entries.len() {
                let part = span.part(start..end);
                let exact = whole - plain_charge(&entries[start..end]);
                let hidden = part.hidden().unwrap();
                if uniform || end - start == entries.len() {
                    assert_eq!(hidden, exact, "{uniform} {start} {end}");
                } else {
                    assert!(hidden >= exact, "{start} {end}");
                }
            }
        }
    }
}

#[test]
fn freezing_keeps_at_most_spare_max_and_owned_batches_keep_their_capacity() {
    let mut rng = Rng(77);
    let slot = std::mem::size_of::<LogEntry>();
    let most = facts::SPARE_MAX / slot;
    for width in [1, SHARE_MIN - 1, SHARE_MIN, 40] {
        for spare in [9, most, most + 1] {
            let mut entries = Vec::with_capacity(width + spare);
            entries.extend(history(&mut rng, 1, width, width % 2 == 0));
            let spare = entries.capacity() - width;
            let plain = plain_charge(&entries);
            let batch: EntryBatch = entries.into();
            if width < SHARE_MIN {
                // Owned: messages and plans charge the allocation, the log
                // the visible entries.
                assert_eq!(batch.allocated_entries(), width + spare);
                assert_eq!(batch.retained_bytes(), Some(plain + spare * slot));
                assert_eq!(batch.log_charge(), Some(plain));
            } else {
                let kept = if spare * slot > facts::SPARE_MAX {
                    0
                } else {
                    spare
                };
                assert_eq!(batch.allocated_entries(), width + kept, "{width} {spare}");
                assert_eq!(batch.retained_bytes(), batch.log_charge());
                assert!(batch.retained_bytes().unwrap() >= plain + kept * slot);
            }
        }
    }
}

#[test]
fn empty_slices_keep_no_backing_alive() {
    let mut rng = Rng(88);
    let batch: EntryBatch = history(&mut rng, 1, 40, true).into();
    for at in [0, 7, 40] {
        let empty = batch.slice(at..at).unwrap();
        assert!(empty.is_empty());
        assert_eq!(empty.backing_count(), 0);
        assert_eq!(empty.retained_bytes(), Some(0));
    }
}
