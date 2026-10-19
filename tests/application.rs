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
//! Counter application: exactly-once retries under the session retention rule.
use voteboat::{
    application::*,
    identity::OperationId,
    log::*,
    retention::{SessionLimits, SessionRefusal},
};

#[path = "application/ready_admission.rs"]
mod ready_admission;
#[path = "application/sessions.rs"]
mod sessions;
#[path = "application/soak.rs"]
mod soak;
#[path = "application/window_bound.rs"]
mod window_bound;

/// Operation ID of `sequence` in `session`.
fn op(session: u64, sequence: u64) -> OperationId {
    Counter::operation(session, sequence).unwrap()
}
fn entry(index: u64, operation: OperationId, bytes: Vec<u8>) -> LogEntry {
    LogEntry {
        index,
        term: 1,
        payload: EntryPayload::Command {
            operation,
            bytes: bytes.into(),
        },
    }
}
/// Add `delta` as `sequence` of `session`, acknowledging through `ack`.
fn add(index: u64, session: u64, sequence: u64, ack: u64, delta: i64) -> LogEntry {
    entry(index, op(session, sequence), Counter::command(ack, delta))
}
/// Add `delta` as `sequence` of the built-in session, acknowledging nothing.
fn command(index: u64, sequence: u64, delta: i64) -> LogEntry {
    add(index, 1, sequence, 0, delta)
}
/// Register a session with `nonce`.
fn register(index: u64, nonce: u64) -> LogEntry {
    entry(
        index,
        OperationId::new(u128::from(nonce)).unwrap(),
        Vec::new(),
    )
}
fn noop(index: u64) -> LogEntry {
    LogEntry {
        index,
        term: 1,
        payload: EntryPayload::Noop,
    }
}

#[test]
fn counter_envelope_is_a_steady_state_bound_that_survives_restore() {
    let mut counter = Counter::new(3).unwrap();
    let required = counter.readiness_requirements();
    assert_eq!(required.application_schema, COUNTER_SCHEMA);
    assert_eq!(required.application_schema, 2);
    assert_eq!(required.command_bytes, 16);
    // Header 24, ledger header 28, one session 28 + 3 receipts of 25, one nonce 24.
    assert_eq!(required.snapshot_bytes, 24 + 28 + 28 + 3 * 25 + 24);
    assert_eq!(
        counter.retention(),
        Retention::Sessions(SessionLimits::single(3).unwrap())
    );
    counter
        .apply_batch(&[command(1, 1, i64::MAX), command(2, 2, 1), command(3, 3, -1)])
        .unwrap();
    assert_eq!(counter.readiness_requirements(), required);
    let image = counter.checkpoint(required.snapshot_bytes).unwrap();
    assert!(image.len() <= required.snapshot_bytes);
    assert!(counter.checkpoint(image.len() - 1).is_err());
    // A fourth unacknowledged outcome is refused at admission (retryable), and
    // acknowledging frees the window.
    assert_eq!(
        counter.validate_proposal(op(1, 4), &Counter::command(0, 1), std::iter::empty()),
        Err(ApplicationError::SessionWindow)
    );
    assert!(counter
        .validate_proposal(op(1, 4), &Counter::command(1, 1), std::iter::empty())
        .is_ok());
    assert!(counter
        .validate_proposal(op(1, 1), &Counter::command(0, i64::MAX), std::iter::empty())
        .is_ok());
    let mut restored = Counter::new(3).unwrap();
    restored
        .restore_checkpoint(required.application_schema, 3, &image)
        .unwrap();
    assert_eq!(restored.readiness_requirements(), required);
    assert_eq!(restored.retained_receipts(), 3);
    for invalid in [
        voteboat::raft::ReadinessRequirements {
            application_schema: 1,
            ..required
        },
        voteboat::raft::ReadinessRequirements {
            command_bytes: 15,
            ..required
        },
        voteboat::raft::ReadinessRequirements {
            snapshot_bytes: required.snapshot_bytes - 1,
            ..required
        },
    ] {
        assert!(restored.validate_readiness_requirements(invalid).is_err());
        assert!(restored.validate_deployment_requirements(invalid).is_err());
    }
    assert!(restored.validate_readiness_requirements(required).is_ok());
    assert!(restored.validate_deployment_requirements(required).is_ok());
    assert!(Counter::new(4)
        .unwrap()
        .restore_checkpoint(COUNTER_SCHEMA, 3, &image)
        .is_err());
}

#[test]
fn bounded_counter_receipts_cover_default_maximum_commit_batch_without_growth() {
    let count = LogLimits::default().max_entries_per_group;
    let limits = voteboat::runtime::ApplicationRouterLimits::default();
    let entries = (1..=count)
        .map(|index| command(index as u64, 1, 7))
        .collect::<Vec<_>>();
    let mut counter = Counter::new(1).unwrap();
    let bound = counter.receipt_bytes_bound(&entries).unwrap();
    assert!(count <= limits.batch_receipts);
    assert!(bound <= limits.batch_bytes);
    let receipts = counter.apply_batch(&entries).unwrap();
    assert_eq!(receipts.len(), count);
    assert!(receipts.capacity() * std::mem::size_of::<CounterReceipt>() <= bound);
    assert!(receipts
        .iter()
        .all(|r| r.outcome == CounterOutcome::Value(7)));
    assert!(receipts.iter().skip(1).all(|r| r.duplicate));
    assert_eq!(counter.applied_index(), count as u64);
    assert_eq!(counter.read_applied(count as u64), Ok(7));
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
            noop(5),
        ])
        .unwrap();
    assert_eq!(receipts[1].outcome, CounterOutcome::Overflow);
    assert_eq!(receipts[3].outcome, CounterOutcome::Overflow);
    assert!(receipts[3].duplicate);
    assert_eq!(counter.read_applied(5), Ok(i64::MAX - 1));
}

#[test]
fn invalid_batches_never_partly_advance_and_a_full_window_is_a_typed_outcome() {
    let mut counter = Counter::new(1).unwrap();
    let before = counter.checkpoint(4096).unwrap();
    assert_eq!(
        counter.apply_batch(&[command(2, 1, 7)]),
        Err(ApplicationError::IndexGap)
    );
    for malformed in [
        entry(2, op(1, 2), vec![1]),
        entry(2, op(1, 2), Counter::command(2, 1)),
        entry(2, op(1, 2), Counter::command(3, 1)),
        entry(
            2,
            OperationId::new(1 << 64).unwrap(),
            Counter::command(0, 1),
        ),
        entry(2, OperationId::new(9).unwrap(), Counter::command(0, 1)),
    ] {
        assert_eq!(
            counter.apply_batch(&[command(1, 1, 7), malformed]),
            Err(ApplicationError::InvalidCommand)
        );
        assert_eq!(counter.checkpoint(4096).unwrap(), before);
        assert_eq!(counter.applied_index(), 0);
    }
    // A committed command beyond the window is a typed refusal that executes
    // nothing but advances the applied index: replay can never crash-loop.
    let receipts = counter
        .apply_batch(&[command(1, 1, 7), command(2, 2, 2)])
        .unwrap();
    assert_eq!(receipts[0].outcome, CounterOutcome::Value(7));
    assert_eq!(
        receipts[1].outcome,
        CounterOutcome::Refused(SessionRefusal::Window)
    );
    assert!(!receipts[1].duplicate);
    assert_eq!(counter.applied_index(), 2);
    assert_eq!(counter.read_applied(2), Ok(7));
    assert_eq!(counter.lookup_operation(op(1, 2)), CounterLookup::Unknown);
    // The same sequence fits once the client acknowledges its first outcome.
    let receipts = counter.apply_batch(&[add(3, 1, 2, 1, 2)]).unwrap();
    assert_eq!(receipts[0].outcome, CounterOutcome::Value(9));
    assert_eq!(counter.retained_receipts(), 1);
}

#[test]
fn mixed_retry_sources_preserve_original_outcomes_at_exact_window() {
    let mut counter = Counter::new(4).unwrap();
    counter
        .apply_batch(&[command(1, 1, i64::MAX), command(2, 2, 1), command(3, 3, -1)])
        .unwrap();
    let entries = [
        command(4, 2, 1),
        command(5, 4, 1),
        command(6, 4, 2),
        command(7, 4, 1),
        command(8, 1, i64::MAX),
        command(9, 3, -2),
        noop(10),
    ];
    let receipts = counter.apply_batch(&entries).unwrap();
    assert_eq!(
        receipts
            .iter()
            .map(|receipt| (receipt.outcome, receipt.duplicate))
            .collect::<Vec<_>>(),
        [
            (CounterOutcome::Overflow, true),
            (CounterOutcome::Value(i64::MAX), false),
            (CounterOutcome::OperationConflict, true),
            (CounterOutcome::Value(i64::MAX), true),
            (CounterOutcome::Value(i64::MAX), true),
            (CounterOutcome::OperationConflict, true),
        ]
    );
    assert_eq!(
        receipts
            .iter()
            .map(|receipt| receipt.index)
            .collect::<Vec<_>>(),
        [4, 5, 6, 7, 8, 9]
    );
    assert_eq!(counter.read_applied(10), Ok(i64::MAX));
    assert_eq!(counter.retained_receipts(), 4);
    let image = counter.checkpoint(1024).unwrap();
    let mut restored = Counter::new(4).unwrap();
    restored
        .restore_checkpoint(COUNTER_SCHEMA, 10, &image)
        .unwrap();
    let retries = restored
        .apply_batch(&[command(11, 4, 1), command(12, 2, 1)])
        .unwrap();
    assert_eq!(retries[0].outcome, CounterOutcome::Value(i64::MAX));
    assert_eq!(retries[1].outcome, CounterOutcome::Overflow);
    assert!(retries.iter().all(|receipt| receipt.duplicate));
    assert_eq!(restored.retained_receipts(), 4);
    assert_eq!(restored.read_applied(12), Ok(i64::MAX));
    assert!(restored.apply_batch(&[]).unwrap().is_empty());
    assert_eq!(restored.applied_index(), 12);
}

#[test]
fn late_batch_failures_preserve_full_history_and_leave_rejected_operations_novel() {
    let mut counter = Counter::new(130).unwrap();
    counter
        .apply_batch(
            &(1..=128)
                .map(|index| command(index, index, 1))
                .collect::<Vec<_>>(),
        )
        .unwrap();
    let previous = counter.checkpoint(8192).unwrap();
    let malformed = entry(132, op(1, 130), vec![1]);
    let failed = [
        (
            vec![
                command(129, 129, 7),
                command(130, 129, 7),
                command(131, 1, 2),
                malformed,
            ],
            ApplicationError::InvalidCommand,
        ),
        (
            vec![command(129, 129, 7), command(131, 130, 2)],
            ApplicationError::IndexGap,
        ),
    ];
    for (batch, expected_error) in failed {
        assert_eq!(counter.apply_batch(&batch), Err(expected_error));
        assert_eq!(counter.checkpoint(8192).unwrap(), previous);
        assert_eq!(counter.read_applied(128), Ok(128));
        assert_eq!(counter.applied_index(), 128);
        assert_eq!(counter.retained_receipts(), 128);
    }
    let receipts = counter
        .apply_batch(&[
            command(129, 129, 7),
            command(130, 129, 9),
            command(131, 130, 2),
            command(132, 131, 1),
        ])
        .unwrap();
    assert_eq!(receipts[0].outcome, CounterOutcome::Value(135));
    assert!(!receipts[0].duplicate);
    assert_eq!(receipts[1].outcome, CounterOutcome::OperationConflict);
    assert!(receipts[1].duplicate);
    assert_eq!(receipts[2].outcome, CounterOutcome::Value(137));
    assert!(!receipts[2].duplicate);
    assert_eq!(
        receipts[3].outcome,
        CounterOutcome::Refused(SessionRefusal::Window)
    );
    assert_eq!(counter.retained_receipts(), 130);
    let image = counter.checkpoint(8192).unwrap();
    let mut restored = Counter::new(130).unwrap();
    restored
        .restore_checkpoint(COUNTER_SCHEMA, 132, &image)
        .unwrap();
    let historical = restored.apply_batch(&[command(133, 1, 1)]).unwrap();
    assert_eq!(historical[0].outcome, CounterOutcome::Value(1));
    assert!(historical[0].duplicate);
    assert_eq!(restored.read_applied(133), Ok(137));
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
                .map(|r| {
                    format!(
                        "{}:{:?}:{}",
                        r.operation.get() as u64,
                        r.outcome,
                        r.duplicate
                    )
                })
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
fn checkpoint_restores_retry_content_original_outcomes_overflow_and_window() {
    let mut original = Counter::new(5).unwrap();
    original
        .apply_batch(&[command(1, 1, i64::MAX), command(2, 2, 1), command(3, 3, -1)])
        .unwrap();
    let bytes = original.checkpoint(1024).unwrap();
    let mut restored = Counter::new(5).unwrap();
    restored
        .restore_checkpoint(COUNTER_SCHEMA, 3, &bytes)
        .unwrap();
    assert_eq!(restored.checkpoint(1024).unwrap(), bytes);
    assert_eq!(restored.retained_receipts(), 3);
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
    assert_eq!(restored.retained_receipts(), 4);
}

/// Byte offsets in a `Counter::new(10)` checkpoint holding the built-in
/// session with sequences 1 and 2.
mod layout {
    pub const APPLIED: usize = 8;
    pub const WINDOW: usize = 28;
    pub const NEXT_SESSION: usize = 36;
    pub const SESSION_COUNT: usize = 44;
    pub const SESSION_ID: usize = 52;
    pub const FLOOR: usize = 60;
    pub const LAST_INDEX: usize = 68;
    pub const FIRST_RECEIPT: usize = 80;
    pub const SECOND_RECEIPT: usize = 105;
    pub const LENGTH: usize = 130;
}

#[test]
fn malformed_or_incompatible_checkpoints_never_partly_restore_counter() {
    use layout::*;
    let mut original = Counter::new(10).unwrap();
    original
        .apply_batch(&[command(1, 1, 7), command(2, 2, 3)])
        .unwrap();
    let bytes = original.checkpoint(1024).unwrap();
    assert_eq!(bytes.len(), LENGTH);
    let mut target = Counter::new(10).unwrap();
    target.apply_batch(&[command(1, 9, 99)]).unwrap();
    let previous = target.checkpoint(1024).unwrap();
    for cut in 0..bytes.len() {
        assert!(target
            .restore_checkpoint(COUNTER_SCHEMA, 2, &bytes[..cut])
            .is_err());
        assert_eq!(target.checkpoint(1024).unwrap(), previous);
    }
    let set = |bytes: &mut Vec<u8>, at: usize, value: &[u8]| {
        bytes[at..at + value.len()].copy_from_slice(value);
    };
    type Mutation<'a> = &'a dyn Fn(&mut Vec<u8>);
    let mutations: [Mutation<'_>; 13] = [
        &|b| b[0] ^= 1,
        &|b| set(b, APPLIED, &1u64.to_le_bytes()),
        &|b| set(b, WINDOW, &11u32.to_le_bytes()),
        &|b| set(b, NEXT_SESSION, &1u64.to_le_bytes()),
        &|b| set(b, SESSION_COUNT, &u32::MAX.to_le_bytes()),
        &|b| set(b, SESSION_ID, &0u64.to_le_bytes()),
        &|b| set(b, SESSION_ID, &2u64.to_le_bytes()),
        &|b| set(b, FLOOR, &1u64.to_le_bytes()),
        &|b| set(b, LAST_INDEX, &3u64.to_le_bytes()),
        &|b| set(b, SECOND_RECEIPT, &1u64.to_le_bytes()),
        &|b| b[FIRST_RECEIPT + 16] = 9,
        &|b| set(b, FIRST_RECEIPT + 16, &[1, 7]),
        &|b| b.push(0),
    ];
    for mutate in mutations {
        let mut invalid = bytes.clone();
        mutate(&mut invalid);
        assert_eq!(
            target.restore_checkpoint(COUNTER_SCHEMA, 2, &invalid),
            Err(ApplicationError::InvalidCheckpoint)
        );
        assert_eq!(target.checkpoint(1024).unwrap(), previous);
    }
    assert_eq!(
        target.restore_checkpoint(1, 2, &bytes),
        Err(ApplicationError::UnsupportedSchema)
    );
    assert!(original.checkpoint(bytes.len() - 1).is_err());
    let mut incompatible = Counter::new(9).unwrap();
    assert!(incompatible
        .restore_checkpoint(COUNTER_SCHEMA, 2, &bytes)
        .is_err());
    assert_eq!(incompatible.applied_index(), 0);
    target
        .restore_checkpoint(COUNTER_SCHEMA, 2, &bytes)
        .unwrap();
    assert_eq!(target.checkpoint(1024).unwrap(), bytes);
}

#[test]
fn readiness_refuses_an_application_that_declares_no_retention_rule() {
    struct Host {
        envelope: Option<voteboat::raft::ReadinessRequirements>,
        retention: Retention,
    }
    impl StateMachine for Host {
        type Receipt = ();
        fn applied_index(&self) -> u64 {
            0
        }
        fn apply_batch(&mut self, _: &[LogEntry]) -> Result<Vec<()>, ApplicationError> {
            Ok(vec![])
        }
        fn deployment_requirements(&self) -> Option<voteboat::raft::ReadinessRequirements> {
            self.envelope
        }
        fn retention(&self) -> Retention {
            self.retention
        }
    }
    struct Legacy;
    impl StateMachine for Legacy {
        type Receipt = ();
        fn applied_index(&self) -> u64 {
            0
        }
        fn apply_batch(&mut self, _: &[LogEntry]) -> Result<Vec<()>, ApplicationError> {
            Ok(vec![])
        }
    }
    let actual = Counter::new(100).unwrap().readiness_requirements();
    assert_eq!(Legacy.deployment_requirements(), None);
    assert_eq!(Legacy.retention(), Retention::Undeclared);
    assert_eq!(
        Legacy.validate_deployment_requirements(actual),
        Err(ApplicationError::RetentionUndeclared)
    );
    // An envelope without a declared retention rule is refused too: the
    // envelope is meaningful only as a steady-state bound under that rule.
    let undeclared = Host {
        envelope: Some(actual),
        retention: Retention::Undeclared,
    };
    assert_eq!(
        undeclared.validate_deployment_requirements(actual),
        Err(ApplicationError::RetentionUndeclared)
    );
    for retention in [
        Retention::Stateless,
        Retention::Fenced,
        Retention::LifetimeBounded,
        Retention::Sessions(SessionLimits::default()),
    ] {
        let host = Host {
            envelope: Some(actual),
            retention,
        };
        host.validate_deployment_requirements(actual).unwrap();
        host.validate_deployment_requirements(voteboat::raft::ReadinessRequirements {
            command_bytes: actual.command_bytes + 10,
            snapshot_bytes: actual.snapshot_bytes + 100,
            ..actual
        })
        .unwrap();
        for invalid in [
            voteboat::raft::ReadinessRequirements {
                command_bytes: 0,
                ..actual
            },
            voteboat::raft::ReadinessRequirements {
                snapshot_bytes: 0,
                ..actual
            },
            voteboat::raft::ReadinessRequirements {
                application_schema: 0,
                ..actual
            },
        ] {
            assert!(Host {
                envelope: Some(invalid),
                retention,
            }
            .validate_deployment_requirements(actual)
            .is_err());
            assert!(host.validate_deployment_requirements(invalid).is_err());
        }
    }
    let host = Host {
        envelope: None,
        retention: Retention::Stateless,
    };
    assert_eq!(
        host.validate_deployment_requirements(actual),
        Err(ApplicationError::InvalidCheckpoint)
    );
}
