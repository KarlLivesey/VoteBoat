// SPDX-License-Identifier: RPL-1.5
// Copyright (c) 2026 Karl Livesey
use super::*;
use crate::logical_voter::codec::Decoder;

// DR1 regenerated both v6 files: the parent is a Counter, whose schema (in the
// enrollment plan) now encodes its session limits (`0x0F00_0004` for one
// session × 16 × one nonce) and whose embedded checkpoints are `VBCTR002`
// with each retained receipt's acknowledgement.
const V6_EMPTY: &[u8] = include_bytes!("../formats/v6-candidate-empty.bin");
const V6_ENROLLED: &[u8] = include_bytes!("../formats/v6-candidate-enrolled.bin");
/// LV1 outputs (candidate checkpoint v3, schema 4), now legacy fixtures.
const V4_EMPTY: &[u8] = include_bytes!("../formats/v4-candidate-empty.bin");
const V4_ENROLLED: &[u8] = include_bytes!("../formats/v4-candidate-enrolled.bin");
/// Generated on `origin/main` 30a1f6c5 by the same fixture (applied 0 and 1).
const V2_EMPTY: &[u8] = include_bytes!("../formats/v2-candidate-empty.bin");
const V2_ENROLLED: &[u8] = include_bytes!("../formats/v2-candidate-enrolled.bin");
fn enrolled() -> LogicalApplication {
    let mut a = candidate();
    let c = enrollment(import(&a), 11);
    assert_eq!(apply(&mut a, &c).outcome, LogicalOutcome::Claimed);
    a
}
#[test]
fn candidate_v6_configuration_checkpoint_and_schema_are_explicit() {
    let a = candidate();
    let bytes = a.checkpoint(usize::MAX).unwrap();
    assert_eq!(&bytes[..8], b"VBLOGC06");
    assert_eq!(&a.configuration[..8], b"VBENRL02");
    assert_eq!(LOGICAL_ENROLLMENT_APPLICATION_SCHEMA, 6);
    assert_eq!(a.deployment_requirements().unwrap().application_schema, 6);
    assert_eq!(
        a.logical_response_schema(),
        LOGICAL_ENROLLMENT_RESPONSE_SCHEMA
    );
    let mut restored = candidate();
    restored
        .restore_checkpoint(LOGICAL_ENROLLMENT_APPLICATION_SCHEMA, 0, &bytes)
        .unwrap();
    assert_eq!(restored.state, a.state);
    let before = restored.checkpoint(usize::MAX).unwrap();
    for (schema, applied, old) in [
        (1, 0, &include_bytes!("v1-empty.bin")[..]),
        (2, 0, V2_EMPTY),
        (2, 1, V2_ENROLLED),
        (4, 0, V4_EMPTY),
        (4, 1, V4_ENROLLED),
    ] {
        assert_eq!(
            restored.restore_checkpoint(schema, applied, old),
            Err(ApplicationError::UnsupportedSchema)
        );
        assert_eq!(
            restored.restore_checkpoint(LOGICAL_ENROLLMENT_APPLICATION_SCHEMA, applied, old),
            Err(ApplicationError::InvalidCheckpoint)
        );
    }
    assert_eq!(restored.checkpoint(usize::MAX).unwrap(), before);
    let mut wrong = bytes.clone();
    wrong[8 + 8 + 8] ^= 1;
    assert!(restored
        .restore_checkpoint(LOGICAL_ENROLLMENT_APPLICATION_SCHEMA, 0, &wrong)
        .is_err());
    assert_eq!(restored.checkpoint(usize::MAX).unwrap(), before);
}
#[test]
fn candidate_v6_goldens_round_trip_exactly() {
    for (a, golden) in [(candidate(), V6_EMPTY), (enrolled(), V6_ENROLLED)] {
        let bytes = a.checkpoint(usize::MAX).unwrap();
        assert_eq!(bytes, golden);
        let mut b = candidate();
        b.restore_checkpoint(
            LOGICAL_ENROLLMENT_APPLICATION_SCHEMA,
            a.applied_index(),
            golden,
        )
        .unwrap();
        assert_eq!(b.state, a.state);
        assert_eq!(b.checkpoint(usize::MAX).unwrap(), golden);
    }
    let a = enrolled();
    assert_eq!(a.state.snapshot_generation, 1);
    assert!(a.state.enrollment.is_some());
    // An ordinary application refuses candidate bytes and vice versa.
    assert_eq!(
        app().restore_checkpoint(LOGICAL_APPLICATION_SCHEMA, 1, V6_ENROLLED),
        Err(ApplicationError::InvalidCheckpoint)
    );
}
/// A candidate's representative exists exactly when its enrollment does, and
/// the enrollment record is the result of its retained `EnrollmentClaim`.
#[test]
fn candidate_restore_checks_enrollment_against_its_claim() {
    let a = enrolled();
    let applied = a.applied_index();
    let edit = |f: fn(&mut LogicalEnrollmentRecord)| {
        let mut b = a.clone();
        f(b.state.enrollment.as_mut().unwrap());
        b
    };
    let mut cases = vec![
        (
            "enrollment operation",
            edit(|e| e.operation = OperationId::new(999).unwrap()),
        ),
        ("enrollment first index", edit(|e| e.first_index += 1)),
        ("enrollment first term", edit(|e| e.first_term += 1)),
        (
            "enrollment target",
            edit(|e| e.target_reference.checksum ^= 1),
        ),
        ("enrollment learners", edit(|e| e.learners.term += 1)),
    ];
    let mut b = a.clone();
    b.state.enrollment = None;
    cases.push(("representative without enrollment", b));
    // With the image and receipt epochs gone too, so only the pairing with
    // the enrollment is wrong.
    let mut b = a.clone();
    b.state.representative = None;
    b.state.parent = None;
    for r in b.state.records.values_mut() {
        r.receipt.epoch = None;
    }
    cases.push(("enrollment without representative", b));
    for (name, broken) in cases {
        let bytes = broken.checkpoint(usize::MAX).unwrap();
        let mut target = candidate();
        let before = target.checkpoint(usize::MAX).unwrap();
        assert_eq!(
            target.restore_checkpoint(LOGICAL_ENROLLMENT_APPLICATION_SCHEMA, applied, &bytes),
            Err(ApplicationError::InvalidCheckpoint),
            "{name}"
        );
        assert_eq!(target.checkpoint(usize::MAX).unwrap(), before, "{name}");
    }
    let mut target = candidate();
    target
        .restore_checkpoint(
            LOGICAL_ENROLLMENT_APPLICATION_SCHEMA,
            applied,
            &a.checkpoint(usize::MAX).unwrap(),
        )
        .unwrap();
    assert_eq!(target.state, a.state);
}
#[test]
fn exact_candidate_command_reserve_boundary_and_measured_envelope() {
    let mut limits = LogicalLimits::default();
    let minimum = limits.parent_snapshots.max_file_bytes()
        + limits.parent_log.max_command_bytes
        + crate::logical_voter::enrollment::ENROLLMENT_FIXED_ENVELOPE_ALLOWANCE;
    limits.max_command_bytes = minimum;
    let a = fixture::candidate_with_limits(limits).unwrap();
    let input = import(&a);
    let phase = a.enrollment_entry_bytes(&input.learners).unwrap();
    let snapshot = a.snapshot_bytes(&input.snapshot).unwrap();
    let bytes = a.encode_command(&enrollment(input, 11), minimum).unwrap();
    assert_eq!(&bytes[..8], b"VBLOGA04");
    assert!(
        bytes.len() - phase.len() - snapshot.len()
            <= crate::logical_voter::enrollment::ENROLLMENT_FIXED_ENVELOPE_ALLOWANCE
    );
    limits.max_command_bytes -= 1;
    assert!(fixture::candidate_with_limits(limits).is_err());
    let mut limits = LogicalLimits::default();
    limits.control_action_bytes = 2 * limits.max_command_bytes - 1;
    assert!(fixture::candidate_with_limits(limits).is_err());
}
#[test]
fn complete_v4_enrollment_response_retains_plan_entry_origin_and_target_floor() {
    let mut a = candidate();
    let c = enrollment(import(&a), 11);
    apply(&mut a, &c);
    let value = a
        .read_at(a.applied_index(), LogicalQuery::Enrollment)
        .unwrap();
    let LogicalRead::Enrollment(view) = &value else {
        unreachable!()
    };
    let bound = a.read_encoding_bound(&LogicalQuery::Enrollment).unwrap();
    let mut bytes = vec![0; bound];
    let used = a.encode_read(&value, &mut bytes).unwrap();
    bytes.truncate(used);
    assert_eq!(&bytes[..8], b"VBLOGR04");
    let mut d = Decoder::new(&bytes[8..]);
    assert_eq!(d.u8().unwrap(), 2);
    assert_eq!(d.u8().unwrap(), 6);
    assert_eq!(d.enrollment_plan().unwrap(), view.plan);
    assert!(d.flag().unwrap());
    let r = view.record.as_ref().unwrap();
    assert_eq!(d.enrollment_plan().unwrap(), r.plan);
    assert_eq!(d.enrollment_origin().unwrap(), r.origin);
    assert_eq!(
        d.blob(a.journal_limits().max_batch_bytes).unwrap(),
        a.enrollment_entry_bytes(&r.learners).unwrap()
    );
    assert_eq!(d.reference().unwrap(), r.target_reference);
    assert_eq!(d.u128().unwrap(), r.operation.get());
    assert_eq!(d.u64().unwrap(), r.first_index);
    assert_eq!(d.u64().unwrap(), r.first_term);
    d.finish().unwrap();
    let charge = a.read_result_bytes(&value, bound).unwrap();
    assert!(a.read_result_bytes(&value, charge - 1).is_err());
    let mut short = vec![0; used - 1];
    assert!(a.encode_read(&value, &mut short).is_err());
}
