// SPDX-License-Identifier: RPL-1.5
// Copyright (c) 2026 Karl Livesey
use super::*;
use crate::logical_voter::codec::Decoder;

#[test]
fn candidate_v2_configuration_checkpoint_and_schema_are_explicit() {
    let a = candidate();
    let bytes = a.checkpoint(usize::MAX).unwrap();
    assert_eq!(&bytes[..8], b"VBLOGC02");
    assert_eq!(&a.configuration[..8], b"VBENRL02");
    assert_eq!(a.deployment_requirements().unwrap().application_schema, 2);
    assert_eq!(
        a.logical_response_schema(),
        LOGICAL_ENROLLMENT_RESPONSE_SCHEMA
    );
    let mut restored = candidate();
    restored.restore_checkpoint(2, 0, &bytes).unwrap();
    assert_eq!(restored.state, a.state);
    let before = restored.checkpoint(usize::MAX).unwrap();
    assert_eq!(
        restored.restore_checkpoint(1, 0, include_bytes!("v1-empty.bin")),
        Err(ApplicationError::UnsupportedSchema)
    );
    assert_eq!(restored.checkpoint(usize::MAX).unwrap(), before);
    let mut wrong = bytes.clone();
    wrong[8 + 8 + 8] ^= 1;
    assert!(restored.restore_checkpoint(2, 0, &wrong).is_err());
    assert_eq!(restored.checkpoint(usize::MAX).unwrap(), before);
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
    assert_eq!(&bytes[..8], b"VBLOGA02");
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
fn complete_v2_enrollment_response_retains_plan_entry_origin_and_target_floor() {
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
    assert_eq!(&bytes[..8], b"VBLOGR02");
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
