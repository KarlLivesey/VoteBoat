// SPDX-License-Identifier: RPL-1.5
// Copyright (c) 2026 Karl Livesey
use super::*;

#[test]
fn candidate_plain_claim_never_constructs_an_empty_parent() {
    let mut a = candidate();
    let outcome = apply(
        &mut a,
        &command(
            11,
            1,
            CommandBody::Claim {
                expected_epoch: None,
            },
        ),
    )
    .outcome;
    assert_eq!(
        outcome,
        LogicalOutcome::Refused(LogicalRefusal::MissingSnapshot)
    );
    assert!(a.state.parent.is_none());
    assert!(a.state.representative.is_none());
    assert!(a.state.enrollment.is_none());
    assert!(a.state.snapshots.is_empty());
    assert_eq!(
        a.read_at(1, LogicalQuery::Image).unwrap(),
        LogicalRead::Uninitialized
    );
}
#[test]
fn source_lineage_membership_and_conflicting_imports_are_refused_without_materialization() {
    for change in 0..6 {
        let mut a = candidate();
        let mut input = import(&a);
        match change {
            0 => input.plan.target.store = store(99),
            1 => input.origin.export.reference.store = store(99),
            2 => input.learners.term += 1,
            3 => input.origin.export.visit.group = group(99),
            4 => input.origin.export.request.binding.store.session = StoreSession::new(2).unwrap(),
            _ => {
                input.snapshot.metadata.application_schema = crate::application::COUNTER_SCHEMA + 1;
                input.origin.export.reference = SnapshotRef {
                    store: store(1),
                    ..a.snapshot_reference(&input.snapshot, SnapshotGeneration::new(9).unwrap())
                        .unwrap()
                };
            }
        }
        let receipt = apply(&mut a, &enrollment(input, 11));
        assert_eq!(
            receipt.outcome,
            LogicalOutcome::Refused(LogicalRefusal::SnapshotConflict)
        );
        assert!(a.state.parent.is_none());
        assert!(a.state.enrollment.is_none());
        assert!(a.state.representative.is_none());
        assert!(a.state.snapshots.is_empty());
    }
    let mut a = candidate();
    let original = enrollment(import(&a), 11);
    apply(&mut a, &original);
    let before = a.checkpoint(usize::MAX).unwrap();
    let mut other = import(&a);
    other.origin.export.reference.generation = SnapshotGeneration::new(10).unwrap();
    let conflict = entry(&a, &enrollment(other, 11), 2);
    assert_eq!(
        a.apply_batch(&[conflict]),
        Err(ApplicationError::OperationConflict)
    );
    assert_eq!(a.checkpoint(usize::MAX).unwrap(), before);
    let second = enrollment(import(&a), 12);
    assert_eq!(
        apply(&mut a, &second).outcome,
        LogicalOutcome::Refused(LogicalRefusal::SnapshotConflict)
    );
    assert_eq!(
        a.state.enrollment.as_ref().unwrap().operation,
        original.operation
    );
}
#[test]
fn selected_native_codec_refuses_empty_data_and_truncated_commands_are_atomic() {
    let mut a = candidate();
    let mut input = import(&a);
    input.snapshot.application.clear();
    assert_eq!(
        a.snapshot_bytes(&input.snapshot),
        Err(ApplicationError::InvalidCommand)
    );
    let c = enrollment(import(&a), 11);
    let bytes = a.encode_command(&c, a.limits.max_command_bytes).unwrap();
    for cut in [0, 7, 8, bytes.len() - 1] {
        let before = a.checkpoint(usize::MAX).unwrap();
        let e = LogEntry {
            index: 1,
            term: 1,
            payload: EntryPayload::Command {
                operation: c.operation,
                bytes: bytes[..cut].to_vec().into(),
            },
        };
        assert!(a.apply_batch(&[e]).is_err());
        assert_eq!(a.checkpoint(usize::MAX).unwrap(), before);
    }
    assert_eq!(apply(&mut a, &c).outcome, LogicalOutcome::Claimed);
    assert!(!a.state.snapshots[0].1.application.is_empty());
}
