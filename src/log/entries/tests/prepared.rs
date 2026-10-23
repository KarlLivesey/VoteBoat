// SPDX-License-Identifier: RPL-1.5
// Copyright (c) 2026 Karl Livesey
//! Independent raw-entry oracles for the prepared lifecycle.
use super::*;

#[test]
fn cpu6_span_size() {
    println!("CPU6_SIZE Span {}", std::mem::size_of::<Span>());
}

#[test]
fn prepared_wide_work_witness() {
    for width in [16, 231, 515] {
        for uniform in [true, false] {
            let (commands, raw) = originals(width, uniform);
            facts::work::take();
            let batch = commands.bind(7, 3).unwrap();
            let construction = facts::work::take();
            assert_eq!(batch, raw);
            assert_eq!(
                batch.payload_bytes(),
                raw.iter().map(LogEntry::payload_bytes).sum::<usize>()
            );
            assert!(batch.valid_suffix(7, 2, 3, 36));
            assert!(batch.valid_append(6, 2, 3, ConfigurationId::new(1).unwrap(), 36));
            let retained = batch.retained_bytes();
            assert!(retained.is_some());
            let sent = batch.for_message(1 << 20);
            assert_eq!(sent.retained_bytes(), retained);
            let log = EntryLog::from(sent);
            assert_eq!(log.replication_range(0, width, 1 << 20), raw);
            let downstream = facts::work::take();
            println!("CPU6_WORK width={width} uniform={uniform} construction={construction:?} downstream={downstream:?}");
        }
    }
}

#[test]
fn prepared_command_retained_charge_matches_independent_owner_ledger() {
    for width in [1, 2, 15, 16, 17, 231, 515, 1024] {
        for uniform in [false, true] {
            let (commands, raw) = originals(width, uniform);
            let capacity = commands.allocation().1;
            let batch = commands.bind(7, 3).unwrap();
            let payloads = raw
                .iter()
                .map(LogEntry::retained_payload_bytes)
                .sum::<usize>();
            let entry_storage = if width == 1 {
                0
            } else {
                capacity * std::mem::size_of::<LogEntry>()
            };
            let mixed_prefixes = if width >= SHARE_MIN && !uniform {
                (width + 1) * std::mem::size_of::<[usize; 3]>()
            } else {
                0
            };
            let expected = entry_storage + payloads + mixed_prefixes;
            assert_eq!(
                batch.retained_bytes(),
                Some(expected),
                "independent complete backing and payload ledger"
            );
            assert_eq!(batch.retained_bytes_and_data(), Some((expected, true)));
            let expected_log = if width == 1 {
                expected + std::mem::size_of::<LogEntry>()
            } else {
                expected
            };
            assert_eq!(batch.log_charge(), Some(expected_log));
        }
    }
}

fn originals(width: usize, uniform: bool) -> (CommandEntries, Vec<LogEntry>) {
    let mut commands = CommandEntries::with_capacity(width);
    let raw = (0..width)
        .map(|offset| {
            let operation = OperationId::new((1u128 << 127) + 13 * offset as u128 + 1).unwrap();
            let bytes = vec![(offset % 251) as u8; if uniform { 9 } else { offset % 37 }];
            commands.push(operation, bytes.clone().into());
            LogEntry {
                index: 7 + offset as u64,
                term: 3,
                payload: EntryPayload::Command {
                    operation,
                    bytes: bytes.into(),
                },
            }
        })
        .collect();
    (commands, raw)
}

fn check_prepared(batch: &EntryBatch, raw: &[LogEntry]) {
    assert_eq!(batch, raw);
    assert_eq!(batch.to_vec(), raw);
    assert_eq!(batch.first(), raw.first());
    assert_eq!(batch.last(), raw.last());
    assert_eq!(batch.iter().len(), raw.len());
    assert_eq!(batch.slices().len(), batch.slices().count());
    assert_eq!(
        batch.slices().flatten().collect::<Vec<_>>(),
        raw.iter().collect::<Vec<_>>()
    );
    let mut iter = batch.iter();
    let mut expected = raw.iter();
    while !expected.as_slice().is_empty() {
        assert_eq!(iter.next(), expected.next());
        assert_eq!(iter.next_back(), expected.next_back());
        assert_eq!(iter.len(), expected.len());
    }
    assert_eq!(iter.next(), None);
    assert_eq!(iter.next_back(), None);
    for offset in 0..=raw.len() {
        assert_eq!(batch.get(offset), raw.get(offset));
    }
    let payload = raw.iter().map(LogEntry::payload_bytes).sum::<usize>();
    assert_eq!(batch.payload_bytes(), payload);
    let expected_data = raw
        .iter()
        .any(|entry| !matches!(entry.payload, EntryPayload::Noop));
    assert_eq!(batch.has_data(), expected_data);
    assert_eq!(
        batch.retained_bytes_and_data(),
        Some((batch.retained_bytes().unwrap(), expected_data))
    );
    let generic: EntryBatch = raw.to_vec().into();
    assert_eq!(
        batch
            .totals()
            .map(|t| (t.payload, t.non_noops, t.configurations)),
        generic
            .totals()
            .map(|t| (t.payload, t.non_noops, t.configurations))
    );
    assert_eq!(batch.log_charge(), generic.log_charge());
    assert_eq!(
        batch.valid_suffix(7, 2, 3, 36),
        old_suffix(raw, 7, 2, 3, 36)
    );
    assert_eq!(
        batch.valid_append(6, 2, 3, ConfigurationId::new(1).unwrap(), 36),
        old_append(raw, 6, 2, 3, ConfigurationId::new(1).unwrap(), 36)
    );
}

#[test]
fn prepared_widths_and_bounded_subranges_match_raw_entries() {
    for width in [
        1,
        2,
        15,
        16,
        17,
        231,
        515,
        1024,
        crate::raft::BatchLimits::MAX_PROPOSAL_ENTRIES,
    ] {
        for uniform in [false, true] {
            let (commands, raw) = originals(width, uniform);
            let allocation = commands.allocation();
            let batch = commands.bind(7, 3).unwrap();
            check_prepared(&batch, &raw);
            if width == 1 {
                assert_eq!(batch.allocated_entries(), 0);
                assert_eq!(
                    batch.retained_bytes(),
                    Some(raw[0].retained_payload_bytes())
                );
            } else {
                assert_eq!(batch.first_backing_ptr(), Some(allocation.0));
            }
            let mut starts: Vec<_> = (0..=width.min(17)).collect();
            starts.extend([width / 2, width.saturating_sub(1), width]);
            starts.sort_unstable();
            starts.dedup();
            for start in starts {
                let ends: Vec<_> = if width <= 17 {
                    (start..=width).collect()
                } else {
                    [
                        start,
                        (start + 1).min(width),
                        (start + 15).min(width),
                        (start + 16).min(width),
                        width,
                    ]
                    .into()
                };
                for end in ends {
                    let part = batch.slice(start..end).unwrap();
                    let expected = &raw[start..end];
                    assert_eq!(part, expected);
                    assert_eq!(
                        part.payload_bytes(),
                        expected.iter().map(LogEntry::payload_bytes).sum::<usize>()
                    );
                    assert_eq!(part.has_data(), !expected.is_empty());
                    assert_eq!(
                        part.retained_bytes_and_data(),
                        Some((part.retained_bytes().unwrap(), !expected.is_empty()))
                    );
                    if expected.len() <= 1 {
                        assert_eq!(part.retained_bytes(), Some(visible_heap_charge(expected)));
                    }
                }
            }
        }
    }
    assert!(CommandEntries::with_capacity(0).bind(7, 3).is_none());
    assert!(originals(2, false).0.bind(u64::MAX, 3).is_none());
    assert!(originals(1, false).0.bind(7, 0).is_none());
    assert_eq!(
        originals(2, false)
            .0
            .bind(u64::MAX - 1, u64::MAX)
            .unwrap()
            .last()
            .unwrap()
            .index,
        u64::MAX
    );
}

#[test]
fn inline_payload_survives_holders_compaction_replacement_and_exact_drop() {
    let original = command(1, 1, 37);
    let EntryPayload::Command { bytes, .. } = &original.payload else {
        unreachable!()
    };
    let weak = bytes.weak().unwrap();
    let inline = EntryBatch::from_entry(original);
    let held = inline.slice(0..1).unwrap().for_message(1 << 20);
    let empty = inline.slice(1..1).unwrap();
    let mut log = EntryLog::default();
    log.append(inline);
    for index in 2..=100 {
        log.append(EntryBatch::from_entry(command(index, 1, 9)));
    }
    assert_eq!(log.to_batch().backing_count(), 1);
    assert_eq!(
        log.retained_bytes(),
        Some(
            log.iter()
                .map(|e| std::mem::size_of::<LogEntry>() + e.retained_payload_bytes())
                .sum()
        )
    );
    log.trim_prefix(80);
    log.truncate(1);
    log.edit(|entries| entries[0] = noop(81, 2));
    drop(log);
    assert!(weak.upgrade().is_some(), "held Inline must own its payload");
    assert_eq!(held[0], command(1, 1, 37));
    assert!(matches!(held.contiguous(), Cow::Borrowed(_)));
    assert_eq!(held.slices().next().unwrap()[0], command(1, 1, 37));
    drop(held);
    assert!(
        weak.upgrade().is_none(),
        "empty range cannot retain payload"
    );
    assert_eq!(empty.retained_bytes(), Some(0));
}

#[test]
fn inline_equality_and_edit_use_real_contents_and_generic_facts() {
    let mut inline = EntryBatch::from_entry(command(7, 3, 9));
    assert_ne!(inline, EntryBatch::from_entry(command(8, 3, 9)));
    assert_eq!(inline, EntryBatch::from(vec![command(7, 3, 9)]));
    inline.edit(|entries| entries[0].payload = EntryPayload::Noop);
    assert_eq!(inline, [noop(7, 3)]);
    check_facts(&inline, &[noop(7, 3)]);
    let (commands, raw) = originals(17, false);
    let mut batch = commands.bind(7, 3).unwrap();
    batch.edit(|entries| {
        entries[8].index += 1;
        entries[9].payload = EntryPayload::Noop;
    });
    let mut edited = raw;
    edited[8].index += 1;
    edited[9].payload = EntryPayload::Noop;
    assert_eq!(batch, edited);
    assert!(batch.summary().is_none());
    check_validators(&batch, &edited);
}
