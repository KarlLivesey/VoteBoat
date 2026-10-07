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
use voteboat::{application::*, identity::OperationId, log::*};
fn command(index: u64, id: u128, delta: i64) -> LogEntry {
    LogEntry {
        index,
        term: 1,
        payload: EntryPayload::Command {
            operation: OperationId::new(id).unwrap(),
            bytes: delta.to_le_bytes().to_vec(),
        },
    }
}
#[test]
fn retries_return_the_original_result_without_reapplying() {
    let mut counter = Counter::new(10).unwrap();
    let receipts = counter
        .apply_batch(&[command(1, 1, 7), command(2, 2, 2), command(3, 1, 7)])
        .unwrap();
    assert_eq!(receipts[0].outcome, CounterOutcome::Value(7));
    assert_eq!(receipts[1].outcome, CounterOutcome::Value(9));
    assert_eq!(receipts[2].outcome, CounterOutcome::Value(7));
    assert!(receipts[2].duplicate);
    assert_eq!(counter.read_applied(3), Ok(9));
    assert_eq!(counter.read_applied(4), Err(ApplicationError::NotApplied));
}
#[test]
fn conflicting_operation_content_never_changes_the_original_effect() {
    let mut counter = Counter::new(10).unwrap();
    let receipts = counter
        .apply_batch(&[command(1, 1, 7), command(2, 1, 8), command(3, 1, 7)])
        .unwrap();
    assert_eq!(receipts[1].outcome, CounterOutcome::OperationConflict);
    assert_eq!(receipts[2].outcome, CounterOutcome::Value(7));
    assert_eq!(counter.read_applied(3), Ok(7));
}
#[test]
fn overflow_result_is_deduplicated_and_noops_advance_the_applied_boundary() {
    let mut counter = Counter::new(10).unwrap();
    let receipts = counter
        .apply_batch(&[
            command(1, 1, i64::MAX),
            command(2, 2, 1),
            command(3, 3, -1),
            command(4, 2, 1),
            LogEntry {
                index: 5,
                term: 1,
                payload: EntryPayload::Noop,
            },
        ])
        .unwrap();
    assert_eq!(receipts[1].outcome, CounterOutcome::Overflow);
    assert_eq!(receipts[3].outcome, CounterOutcome::Overflow);
    assert!(receipts[3].duplicate);
    assert_eq!(counter.read_applied(5), Ok(i64::MAX - 1));
}
#[test]
fn invalid_batches_and_capacity_do_not_partly_advance_state() {
    let mut counter = Counter::new(1).unwrap();
    assert_eq!(
        counter.apply_batch(&[command(1, 1, 7), command(2, 2, 2)]),
        Err(ApplicationError::DedupCapacity)
    );
    assert_eq!(counter.applied_index(), 0);
    assert_eq!(counter.read_applied(0), Ok(0));
    assert_eq!(
        counter.apply_batch(&[command(2, 1, 7)]),
        Err(ApplicationError::IndexGap)
    );
    let invalid = LogEntry {
        index: 2,
        term: 1,
        payload: EntryPayload::Command {
            operation: OperationId::new(2).unwrap(),
            bytes: vec![1],
        },
    };
    assert_eq!(
        counter.apply_batch(&[command(1, 1, 7), invalid]),
        Err(ApplicationError::InvalidCommand)
    );
    assert_eq!(counter.applied_index(), 0);
}

#[test]
fn host_application_can_supply_its_own_receipt_type_through_the_public_seam() {
    struct HostApplication {
        inner: Counter,
        observed_batches: usize,
    }
    impl StateMachine for HostApplication {
        type Receipt = String;
        fn applied_index(&self) -> u64 {
            self.inner.applied_index()
        }
        fn apply_batch(&mut self, entries: &[LogEntry]) -> Result<Vec<String>, ApplicationError> {
            let receipts = self.inner.apply_batch(entries)?;
            self.observed_batches += 1;
            Ok(receipts
                .into_iter()
                .map(|r| format!("{}:{:?}:{}", r.operation.get(), r.outcome, r.duplicate))
                .collect())
        }
    }
    fn consume<A: StateMachine>(
        application: &mut A,
        entries: &[LogEntry],
    ) -> Result<Vec<A::Receipt>, ApplicationError> {
        application.apply_batch(entries)
    }
    let entries = [command(1, 1, 7), command(2, 1, 7)];
    let mut native = Counter::new(10).unwrap();
    let mut host = HostApplication {
        inner: Counter::new(10).unwrap(),
        observed_batches: 0,
    };
    assert_eq!(
        consume(&mut native, &entries)
            .unwrap()
            .last()
            .unwrap()
            .outcome,
        CounterOutcome::Value(7)
    );
    assert_eq!(
        consume(&mut host, &entries).unwrap(),
        vec!["1:Value(7):false", "1:Value(7):true"]
    );
    assert_eq!(host.observed_batches, 1);
    assert_eq!(host.applied_index(), 2);
}

#[test]
fn checkpoint_restores_retry_content_original_outcomes_overflow_and_capacity() {
    let mut original = Counter::new(5).unwrap();
    original
        .apply_batch(&[command(1, 1, i64::MAX), command(2, 2, 1), command(3, 3, -1)])
        .unwrap();
    let bytes = original.checkpoint(1024).unwrap();
    let mut restored = Counter::new(5).unwrap();
    restored.restore_checkpoint(1, 3, &bytes).unwrap();
    assert_eq!(restored.checkpoint(1024).unwrap(), bytes);
    assert_eq!(restored.remaining_operations(), 2);
    let receipts = restored
        .apply_batch(&[
            command(4, 1, i64::MAX),
            command(5, 2, 1),
            command(6, 3, -2),
            command(7, 4, 1),
        ])
        .unwrap();
    assert_eq!(receipts[0].outcome, CounterOutcome::Value(i64::MAX));
    assert_eq!(receipts[1].outcome, CounterOutcome::Overflow);
    assert_eq!(receipts[2].outcome, CounterOutcome::OperationConflict);
    assert!(receipts[..3].iter().all(|r| r.duplicate));
    assert_eq!(restored.read_applied(7), Ok(i64::MAX));
    assert_eq!(restored.remaining_operations(), 1);
}

#[test]
fn malformed_or_incompatible_checkpoints_never_partly_restore_counter() {
    let mut original = Counter::new(10).unwrap();
    original
        .apply_batch(&[command(1, 1, 7), command(2, 2, 3)])
        .unwrap();
    let bytes = original.checkpoint(1024).unwrap();
    let mut target = Counter::new(10).unwrap();
    target.apply_batch(&[command(1, 9, 99)]).unwrap();
    let previous = target.checkpoint(1024).unwrap();
    for cut in 0..bytes.len() {
        assert!(target.restore_checkpoint(1, 2, &bytes[..cut]).is_err());
        assert_eq!(target.checkpoint(1024).unwrap(), previous);
    }
    for mutation in 0..6 {
        let mut invalid = bytes.clone();
        match mutation {
            0 => invalid[0] ^= 1,
            1 => invalid[8..16].copy_from_slice(&1u64.to_le_bytes()),
            2 => invalid[16..20].copy_from_slice(&11u32.to_le_bytes()),
            3 => invalid[28..32].copy_from_slice(&u32::MAX.to_le_bytes()),
            4 => {
                let first: Vec<_> = invalid[32..48].to_vec();
                invalid[65..81].copy_from_slice(&first);
            }
            5 => invalid[56] = 9,
            _ => unreachable!(),
        }
        assert!(target.restore_checkpoint(1, 2, &invalid).is_err());
        assert_eq!(target.checkpoint(1024).unwrap(), previous);
    }
    assert_eq!(
        target.restore_checkpoint(2, 2, &bytes),
        Err(ApplicationError::UnsupportedSchema)
    );
    assert!(original.checkpoint(bytes.len() - 1).is_err());
    let mut incompatible = Counter::new(9).unwrap();
    assert!(incompatible.restore_checkpoint(1, 2, &bytes).is_err());
    assert_eq!(incompatible.applied_index(), 0);
}
