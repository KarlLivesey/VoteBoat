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
//! The original per-entry loops, kept verbatim as test oracles.
use super::*;

/// `validate_suffix_entries` before shared batches.
pub(super) fn old_suffix(
    entries: &[LogEntry],
    from: u64,
    mut previous_term: u64,
    hard_term: u64,
    max: usize,
) -> bool {
    for (offset, entry) in entries.iter().enumerate() {
        if from.checked_add(offset as u64) != Some(entry.index)
            || entry.term == 0
            || entry.term < previous_term
            || entry.term > hard_term
            || entry.payload_bytes() > max
        {
            return false;
        }
        previous_term = entry.term;
    }
    true
}
/// `validate_append_entries` before shared batches.
pub(super) fn old_append(
    entries: &[LogEntry],
    previous_index: u64,
    previous_term: u64,
    message_term: u64,
    configuration: ConfigurationId,
    max: usize,
) -> bool {
    let mut previous = previous_term;
    for (i, e) in entries.iter().enumerate() {
        if activated_configuration(e).is_some_and(|id| id > configuration) {
            return false;
        }
        if previous_index.checked_add(i as u64 + 1) != Some(e.index)
            || e.term == 0
            || e.term < previous
            || e.term > message_term
            || e.payload_bytes() > max
        {
            return false;
        }
        previous = e.term;
    }
    true
}
/// `append_for`'s selection loop before shared batches.
pub(super) fn old_selection(
    remaining: &[LogEntry],
    max_entries: usize,
    max: usize,
) -> Vec<LogEntry> {
    let mut count = 0usize;
    let mut bytes = 0usize;
    for entry in remaining.iter().take(max_entries) {
        let size = 37 + entry.payload_bytes();
        if size > max.saturating_sub(bytes + 256) {
            break;
        }
        count += 1;
        bytes += size;
    }
    remaining[..count].to_vec()
}

/// Facts of a valid visible range against a direct scan.
pub(super) fn check_facts(batch: &EntryBatch, expected: &[LogEntry]) {
    check_validators(batch, expected);
    let payload: usize = expected.iter().map(LogEntry::payload_bytes).sum();
    assert_eq!(batch.payload_bytes(), payload);
    let configured = expected
        .iter()
        .any(|e| matches!(e.payload, EntryPayload::Configuration(_)));
    assert_eq!(batch.has_configuration(), configured);
    let data = expected
        .iter()
        .any(|e| !matches!(e.payload, EntryPayload::Noop));
    assert_eq!(batch.has_data(), data);
    let Some(summary) = batch.summary() else {
        // Only a single small owned piece goes straight to the original loops.
        assert!(!batch.is_shared() && batch.slices().count() == 1);
        return;
    };
    assert_eq!(summary.count, expected.len());
    assert_eq!(
        summary.bounds,
        expected
            .first()
            .zip(expected.last())
            .map(|(a, b)| ((a.index, a.term), (b.index, b.term)))
    );
    assert_eq!(summary.payload, payload);
    let configurations = expected
        .iter()
        .filter(|e| matches!(e.payload, EntryPayload::Configuration(_)))
        .count();
    assert_eq!(summary.configurations, configurations);
    // Maxima are upper bounds, never below the visible maximum.
    let max_payload = expected
        .iter()
        .map(LogEntry::payload_bytes)
        .max()
        .unwrap_or(0);
    assert!(summary.max_payload >= max_payload);
    let max_configuration = expected.iter().filter_map(activated_configuration).max();
    assert!(summary.max_configuration >= max_configuration);
}

/// Validators against the original loops, across receiver contexts.
pub(super) fn check_validators(batch: &EntryBatch, expected: &[LogEntry]) {
    let first = expected.first().map_or(1, |e| e.index);
    let terms: Vec<u64> = expected.iter().map(|e| e.term).collect();
    let low = terms.iter().copied().min().unwrap_or(1);
    let high = terms.iter().copied().max().unwrap_or(1);
    let payload = expected
        .iter()
        .map(LogEntry::payload_bytes)
        .max()
        .unwrap_or(0);
    for from in [first.saturating_sub(1), first, first + 1] {
        for previous in [0, low.saturating_sub(1), low, low + 1] {
            for hard in [high.saturating_sub(1), high, high + 1] {
                for max in [payload.saturating_sub(1), payload, payload + 1] {
                    assert_eq!(
                        batch.valid_suffix(from, previous, hard, max),
                        old_suffix(expected, from, previous, hard, max),
                        "suffix {from} {previous} {hard} {max}"
                    );
                    for configuration in [2, 3, 4, 6] {
                        let configuration = ConfigurationId::new(configuration).unwrap();
                        let previous_index = from.saturating_sub(1);
                        assert_eq!(
                            batch.valid_append(previous_index, previous, hard, configuration, max),
                            old_append(
                                expected,
                                previous_index,
                                previous,
                                hard,
                                configuration,
                                max
                            ),
                            "append {previous_index} {previous} {hard} {max}"
                        );
                    }
                }
            }
        }
    }
}

/// Every read-only query of a log against its vector model.
pub(super) fn check_log(log: &EntryLog, model: &[LogEntry]) {
    assert_eq!(log.len(), model.len());
    assert_eq!(log.is_empty(), model.is_empty());
    assert_eq!(log, model);
    assert_eq!(log.to_vec(), model);
    assert_eq!(log.first(), model.first());
    assert_eq!(log.last(), model.last());
    assert_eq!(
        log.last_index_term(),
        model.last().map(|e| (e.index, e.term))
    );
    for (ordinal, entry) in model.iter().enumerate() {
        assert_eq!(log.get(ordinal), Some(entry));
    }
    assert_eq!(log.get(model.len()), None);
    assert_eq!(log.iter().len(), model.len());
    let mid = model.len() / 2;
    assert_eq!(log.shared_range(mid..model.len()).unwrap(), model[mid..]);
    assert_eq!(log.borrowed(0..mid).unwrap(), model[..mid]);
}
