// SPDX-License-Identifier: RPL-1.5
// Copyright (c) 2026 Karl Livesey
use super::*;
use std::{cell::Cell, rc::Rc};

fn log(entries: Vec<LogEntry>) -> EntryLog {
    let mut log = EntryLog::default();
    for chunk in entries.chunks(17) {
        log.append(chunk.to_vec().into());
    }
    log
}

#[test]
fn public_counter_into_matches_owned_and_reuses_warmed_capacity() {
    for width in [1, 231, 515] {
        let mut owned = Counter::new(4 * width).unwrap();
        let mut host = Counter::new(4 * width).unwrap();
        let mut output = Vec::with_capacity(width);
        let pointer = output.as_ptr();
        let capacity = output.capacity();
        for round in 0..3 {
            let entries: Vec<_> = (0..width)
                .map(|offset| {
                    let index = (round * width + offset + 1) as u64;
                    command(index, (1 << 127) + u128::from(index) * 7919, 1)
                })
                .collect();
            let expected = owned.apply_batch(&entries).unwrap();
            if round == 1 {
                let log = log(entries);
                host.apply_entries_into(log.borrowed(0..width).unwrap(), &mut output)
                    .unwrap();
            } else {
                host.apply_batch_into(&entries, &mut output).unwrap();
            }
            assert_eq!(output, expected);
            assert_eq!(
                output.as_ptr(),
                pointer,
                "caller allocation must remain live and reused"
            );
            assert_eq!(output.capacity(), capacity);
        }
        let snapshot = host.checkpoint(1_000_000).unwrap();
        let mut restored = Counter::new(4 * width).unwrap();
        restored
            .restore_checkpoint(1, host.applied_index(), &snapshot)
            .unwrap();
        let retry = command(host.applied_index() + 1, (1 << 127) + 7919, 1);
        restored.apply_batch_into(&[retry], &mut output).unwrap();
        assert!(output[0].duplicate);
        assert_eq!(output[0].outcome, CounterOutcome::Value(1));
        drop(host);
        let expected = owned.applied_index();
        owned.apply_batch_into(&[], &mut output).unwrap();
        assert!(output.is_empty());
        assert_eq!(owned.applied_index(), expected);
    }
}

#[test]
fn counter_late_errors_clear_output_and_preserve_snapshot_and_retry_novelty() {
    for mode in 0..3 {
        let mut counter = Counter::new(if mode == 2 { 1 } else { 4 }).unwrap();
        let before = counter.checkpoint(1024).unwrap();
        let mut entries = vec![command(1, u128::MAX, 3), command(2, 19, 4)];
        let error = match mode {
            0 => {
                entries[1].payload = EntryPayload::Command {
                    operation: OperationId::new(19).unwrap(),
                    bytes: vec![1].into(),
                };
                ApplicationError::InvalidCommand
            }
            1 => {
                entries[1].index = 3;
                ApplicationError::IndexGap
            }
            _ => ApplicationError::DedupCapacity,
        };
        let mut output = vec![CounterReceipt {
            index: 99,
            operation: OperationId::new(1).unwrap(),
            outcome: CounterOutcome::Overflow,
            duplicate: false,
        }];
        assert_eq!(counter.apply_batch_into(&entries, &mut output), Err(error));
        assert!(
            output.is_empty(),
            "late errors cannot expose prefix receipts"
        );
        assert_eq!(counter.checkpoint(1024).unwrap(), before);
        counter
            .apply_batch_into(&entries[..1], &mut output)
            .unwrap();
        assert!(!output[0].duplicate);
        assert_eq!(output[0].outcome, CounterOutcome::Value(3));
    }
}

#[test]
fn counter_into_preserves_conflict_overflow_and_noncommand_receipt_order() {
    let mut counter = Counter::new(3).unwrap();
    let mut entries = vec![
        command(1, u128::MAX, i64::MAX),
        command(2, 17, 1),
        command(3, u128::MAX, 2),
        command(4, 17, 1),
    ];
    entries.push(LogEntry {
        index: 5,
        term: 1,
        payload: EntryPayload::Noop,
    });
    let mut output = Vec::new();
    counter.apply_batch_into(&entries, &mut output).unwrap();
    assert_eq!(
        output.iter().map(|r| r.index).collect::<Vec<_>>(),
        vec![1, 2, 3, 4]
    );
    assert_eq!(output[1].outcome, CounterOutcome::Overflow);
    assert_eq!(output[2].outcome, CounterOutcome::OperationConflict);
    assert!(output[3].duplicate);
    assert_eq!(counter.applied_index(), 5);
}

struct Receipt(Rc<Cell<usize>>);
impl Drop for Receipt {
    fn drop(&mut self) {
        self.0.set(self.0.get() + 1);
    }
}
struct Legacy {
    drops: Rc<Cell<usize>>,
    batches: usize,
    ranges: usize,
    fail: bool,
}
impl StateMachine for Legacy {
    type Receipt = Receipt;
    fn applied_index(&self) -> u64 {
        0
    }
    fn apply_batch(&mut self, _: &[LogEntry]) -> Result<Vec<Receipt>, ApplicationError> {
        self.batches += 1;
        if self.fail {
            return Err(ApplicationError::InvalidCommand);
        }
        Ok(vec![Receipt(self.drops.clone())])
    }
    fn apply_entries(&mut self, _: EntryRange<'_>) -> Result<Vec<Receipt>, ApplicationError> {
        self.ranges += 1;
        if self.fail {
            return Err(ApplicationError::IndexGap);
        }
        Ok(vec![
            Receipt(self.drops.clone()),
            Receipt(self.drops.clone()),
        ])
    }
}
#[test]
fn default_adapters_honor_custom_ranges_nonclone_lifetimes_and_empty_errors() {
    let drops = Rc::new(Cell::new(0));
    let mut app = Legacy {
        drops: drops.clone(),
        batches: 0,
        ranges: 0,
        fail: false,
    };
    let log = log(vec![command(1, 1, 1)]);
    let mut output = Vec::new();
    app.apply_batch_into(&[], &mut output).unwrap();
    let retained = std::mem::take(&mut output);
    app.apply_entries_into(log.borrowed(0..1).unwrap(), &mut output)
        .unwrap();
    assert_eq!(
        (
            app.batches,
            app.ranges,
            retained.len(),
            output.len(),
            drops.get()
        ),
        (1, 1, 1, 2, 0)
    );
    app.fail = true;
    assert_eq!(
        app.apply_entries_into(log.borrowed(0..1).unwrap(), &mut output),
        Err(ApplicationError::IndexGap)
    );
    assert!(output.is_empty());
    assert_eq!((app.batches, app.ranges, drops.get()), (1, 2, 2));
    output.push(Receipt(drops.clone()));
    assert_eq!(
        app.apply_batch_into(&[], &mut output),
        Err(ApplicationError::InvalidCommand)
    );
    assert!(output.is_empty());
    assert_eq!((app.batches, app.ranges, drops.get()), (2, 2, 3));
    drop(retained);
    assert_eq!(drops.get(), 4);
}

struct OldOnly(Legacy);
impl StateMachine for OldOnly {
    type Receipt = Receipt;
    fn applied_index(&self) -> u64 {
        0
    }
    fn apply_batch(&mut self, entries: &[LogEntry]) -> Result<Vec<Receipt>, ApplicationError> {
        self.0.apply_batch(entries)
    }
}
#[test]
fn old_only_provider_default_range_and_batch_call_owned_method_once() {
    let drops = Rc::new(Cell::new(0));
    let mut app = OldOnly(Legacy {
        drops: drops.clone(),
        batches: 0,
        ranges: 0,
        fail: false,
    });
    let log = log((1..=40).map(|n| command(n, n as u128, 1)).collect());
    assert!(log.borrowed(0..40).unwrap().as_slice().is_none());
    let mut output = Vec::new();
    app.apply_entries_into(log.borrowed(0..40).unwrap(), &mut output)
        .unwrap();
    assert_eq!((app.0.batches, app.0.ranges, output.len()), (1, 0, 1));
    app.apply_batch_into(&[], &mut output).unwrap();
    assert_eq!(
        (app.0.batches, app.0.ranges, output.len(), drops.get()),
        (2, 0, 1, 1)
    );
    app.0.fail = true;
    assert_eq!(
        app.apply_entries_into(log.borrowed(0..40).unwrap(), &mut output),
        Err(ApplicationError::InvalidCommand)
    );
    assert!(output.is_empty());
    assert_eq!((app.0.batches, drops.get()), (3, 2));
}

#[test]
fn caller_counter_output_for_noop_configuration_only_and_late_piecewise_error() {
    use voteboat::{
        identity::ConfigurationId,
        membership::{ConfigurationChange, ConfigurationRecord},
    };
    for width in [1, 231, 515] {
        let entries: Vec<_> = (1..=width)
            .map(|index| LogEntry {
                index: index as u64,
                term: 1,
                payload: if index % 2 == 0 {
                    EntryPayload::Noop
                } else {
                    EntryPayload::Configuration(Box::new(ConfigurationRecord {
                        operation: OperationId::new(index as u128).unwrap(),
                        expected: ConfigurationId::new(1).unwrap(),
                        change: ConfigurationChange::Final {
                            id: ConfigurationId::new(2).unwrap(),
                        },
                    }))
                },
            })
            .collect();
        let mut counter = Counter::new(1).unwrap();
        let mut output = Vec::with_capacity(4);
        let pointer = output.as_ptr();
        let log = log(entries.clone());
        counter
            .apply_entries_into(log.borrowed(0..width).unwrap(), &mut output)
            .unwrap();
        assert!(output.is_empty());
        assert_eq!(counter.applied_index(), width as u64);
        assert_eq!(counter.remaining_operations(), 1);
        assert_eq!(output.as_ptr(), pointer);
        assert!(Counter::new(1)
            .unwrap()
            .apply_batch(&entries)
            .unwrap()
            .is_empty());
    }
    let mut counter = Counter::new(40).unwrap();
    let before = counter.checkpoint(4096).unwrap();
    let mut entries: Vec<_> = (1..=40)
        .map(|i| command(i, (1 << 127) + i as u128 * 7919, 1))
        .collect();
    entries[39].payload = EntryPayload::Command {
        operation: OperationId::new(u128::MAX).unwrap(),
        bytes: vec![1].into(),
    };
    let log = log(entries);
    assert!(log.borrowed(0..40).unwrap().as_slice().is_none());
    let mut output = Vec::with_capacity(40);
    assert_eq!(
        counter.apply_entries_into(log.borrowed(0..40).unwrap(), &mut output),
        Err(ApplicationError::InvalidCommand)
    );
    assert!(output.is_empty());
    assert_eq!(counter.checkpoint(4096).unwrap(), before);
}
