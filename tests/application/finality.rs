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
//! Exactly-once guards found in review: the acknowledged floor and the
//! session allocation mark are validated against their evidence on restore,
//! replicas cannot disagree on the session limits, and a committed window
//! refusal is final for its sequence.
use super::*;

fn limits(sessions: u32, window: u32, registrations: u32) -> SessionLimits {
    SessionLimits::new(sessions, window, registrations).unwrap()
}
/// Offset of the built-in session's floor in a Counter checkpoint: the
/// 24-byte Counter header, the 44-byte ledger header, then the session ID.
const BUILT_IN_FLOOR: usize = 24 + 44 + 8;
/// Offset of the session allocation mark: the Counter header, then the
/// ledger's three 4-byte limits.
const NEXT_SESSION: usize = 24 + 12;

fn with_floor(image: &[u8], floor: u64) -> Vec<u8> {
    let mut image = image.to_vec();
    image[BUILT_IN_FLOOR..BUILT_IN_FLOOR + 8].copy_from_slice(&floor.to_le_bytes());
    image
}

/// Restore `image` into a fresh Counter; on refusal the target is unchanged.
fn restore(limits: SessionLimits, applied: u64, image: &[u8]) -> Result<Counter, ApplicationError> {
    let mut target = Counter::with_limits(limits).unwrap();
    let schema = target.schema_version();
    let before = target.checkpoint(usize::MAX).unwrap();
    match target.restore_checkpoint(schema, applied, image) {
        Ok(()) => Ok(target),
        Err(error) => {
            assert_eq!(target.checkpoint(usize::MAX).unwrap(), before);
            assert_eq!(target.applied_index(), 0);
            Err(error)
        }
    }
}

#[test]
fn a_lowered_acknowledgement_floor_is_refused_so_an_acknowledged_original_never_reexecutes() {
    // Reviewer reproducer: +7 as sequence 1, then +3 as sequence 2
    // acknowledging 1. Sequence 1 is retired and the value is 10.
    let limits = limits(1, 4, 1);
    let mut counter = Counter::with_limits(limits).unwrap();
    counter
        .apply_batch(&[add(1, 1, 1, 0, 7), add(2, 1, 2, 1, 3)])
        .unwrap();
    let image = counter.checkpoint(usize::MAX).unwrap();
    assert_eq!(
        &image[BUILT_IN_FLOOR..BUILT_IN_FLOOR + 8],
        1u64.to_le_bytes()
    );
    assert!(restore(limits, 2, &image).is_ok());
    // Lowering the floor to 0 would let a retry of sequence 1 execute again.
    assert_eq!(
        restore(limits, 2, &with_floor(&image, 0)).err(),
        Some(ApplicationError::InvalidCheckpoint)
    );
    // The intact image keeps the retry expired.
    let mut restored = restore(limits, 2, &image).unwrap();
    let retry = restored.apply_batch(&[add(3, 1, 1, 0, 7)]).unwrap();
    assert_eq!(
        retry[0].outcome,
        CounterOutcome::Refused(SessionRefusal::Expired)
    );
    assert_eq!(restored.read_applied(3), Ok(10));
}

#[test]
fn a_raised_acknowledgement_floor_is_refused() {
    // Sequence 1, then sequence 3 acknowledging 1: floor 1, sequence 3 kept.
    let limits = limits(1, 4, 1);
    let mut counter = Counter::with_limits(limits).unwrap();
    counter
        .apply_batch(&[add(1, 1, 1, 0, 7), add(2, 1, 3, 1, 3)])
        .unwrap();
    let image = counter.checkpoint(usize::MAX).unwrap();
    assert!(restore(limits, 2, &image).is_ok());
    // Floor 2 still lies below every retained sequence, but no applied
    // command acknowledged sequence 2.
    assert_eq!(
        restore(limits, 2, &with_floor(&image, 2)).err(),
        Some(ApplicationError::InvalidCheckpoint)
    );
}

#[test]
fn replicas_with_different_session_limits_refuse_the_same_declaration() {
    // Two replicas built with windows 32 and 64. The operator declares the
    // larger envelope; under ">= own envelope" both would accept it, and the
    // same committed entry would then be a window refusal on one replica and
    // an executed write on the other.
    let small = Counter::with_limits(limits(256, 32, 64)).unwrap();
    let large = Counter::with_limits(limits(256, 64, 64)).unwrap();
    let declared = large.deployment_requirements().unwrap();
    assert_eq!(large.validate_deployment_requirements(declared), Ok(()));
    assert_eq!(
        small.validate_deployment_requirements(declared),
        Err(ApplicationError::UnsupportedSchema)
    );
    let declared = small.deployment_requirements().unwrap();
    assert_eq!(small.validate_deployment_requirements(declared), Ok(()));
    assert!(large.validate_deployment_requirements(declared).is_err());
    // Sessions and registration limits are part of the same identity.
    for other in [limits(128, 32, 64), limits(256, 32, 32)] {
        let other = Counter::with_limits(other).unwrap();
        assert_eq!(
            other.validate_deployment_requirements(declared),
            Err(ApplicationError::UnsupportedSchema)
        );
        assert_ne!(other.schema_version(), small.schema_version());
    }
}

#[test]
fn a_committed_window_refusal_is_final_for_its_sequence() {
    let limits = limits(2, 4, 2);
    let mut counter = Counter::with_limits(limits).unwrap();
    // A leader that has not yet applied the registration of session 2 must
    // still refuse a command beyond its own acknowledgement plus the window.
    let beyond = Counter::command(0, 5);
    assert_eq!(
        counter.validate_proposal(op(2, 5), &beyond, std::iter::empty()),
        Err(ApplicationError::SessionWindow)
    );
    // If such a command is committed anyway, the refusal is final: a later
    // copy of the same sequence with a larger acknowledgement never executes.
    let receipts = counter
        .apply_batch(&[register(1, 77), add(2, 2, 5, 0, 5)])
        .unwrap();
    assert_eq!(receipts[0].outcome, CounterOutcome::Registered(2));
    assert_eq!(
        receipts[1].outcome,
        CounterOutcome::Refused(SessionRefusal::Window)
    );
    let retry = counter.apply_batch(&[add(3, 2, 5, 1, 5)]).unwrap();
    assert_ne!(retry[0].outcome, CounterOutcome::Value(5));
    assert!(matches!(retry[0].outcome, CounterOutcome::Refused(_)));
    assert_eq!(counter.read_applied(3), Ok(0));
    assert!(!matches!(
        counter.lookup_operation(op(2, 5)),
        CounterLookup::Retained(_)
    ));
}

#[test]
fn a_lowered_allocation_mark_after_closure_is_refused_so_a_retired_session_is_never_reallocated() {
    // Reviewer reproducer (round 2, P1): session 2 executes +7, then a
    // committed window refusal closes it, so session 2 is the newest
    // allocated session and is not live.
    let limits = limits(2, 4, 2);
    let mut counter = Counter::with_limits(limits).unwrap();
    let receipts = counter
        .apply_batch(&[register(1, 77), add(2, 2, 1, 0, 7), add(3, 2, 5, 0, 9)])
        .unwrap();
    assert_eq!(receipts[0].outcome, CounterOutcome::Registered(2));
    assert_eq!(receipts[1].outcome, CounterOutcome::Value(7));
    assert_eq!(
        receipts[2].outcome,
        CounterOutcome::Refused(SessionRefusal::Window)
    );
    assert_eq!(counter.lookup_operation(op(2, 1)), CounterLookup::Expired);
    let image = counter.checkpoint(usize::MAX).unwrap();
    assert_eq!(image[NEXT_SESSION..NEXT_SESSION + 8], 3u64.to_le_bytes());
    // Lowering only the mark 3 → 2 would hand session 2 to the next
    // registration, and the retry of (2, 1) would execute again (7 → 14).
    let mut lowered = image.clone();
    lowered[NEXT_SESSION..NEXT_SESSION + 8].copy_from_slice(&2u64.to_le_bytes());
    assert_eq!(
        restore(limits, 3, &lowered).err(),
        Some(ApplicationError::InvalidCheckpoint)
    );
    // The intact image: the next registration gets a new session and the
    // retry stays expired.
    let mut restored = restore(limits, 3, &image).unwrap();
    let after = restored
        .apply_batch(&[register(4, 88), add(5, 2, 1, 0, 7)])
        .unwrap();
    assert_eq!(after[0].outcome, CounterOutcome::Registered(3));
    assert_eq!(
        after[1].outcome,
        CounterOutcome::Refused(SessionRefusal::Expired)
    );
    assert!(!after[1].duplicate);
    assert_eq!(restored.read_applied(5), Ok(7));
}

/// Offset of the first live session's ID in a Counter checkpoint.
const FIRST_SESSION_ID: usize = 24 + 44;

#[test]
fn restore_counter_invalid_max_session_id_must_not_panic() {
    // Sol, round 3: a pristine checkpoint with only the first live session ID
    // replaced by u64::MAX is refused typed, never a panic.
    let limits = limits(2, 4, 2);
    let mut counter = Counter::with_limits(limits).unwrap();
    counter.apply_batch(&[add(1, 1, 1, 0, 7)]).unwrap();
    let image = counter.checkpoint(usize::MAX).unwrap();
    assert_eq!(
        u64::from_le_bytes(
            image[FIRST_SESSION_ID..FIRST_SESSION_ID + 8]
                .try_into()
                .unwrap()
        ),
        1
    );
    assert!(restore(limits, 1, &image).is_ok());
    let mut bad = image.clone();
    bad[FIRST_SESSION_ID..FIRST_SESSION_ID + 8].copy_from_slice(&u64::MAX.to_le_bytes());
    assert_eq!(
        restore(limits, 1, &bad).err(),
        Some(ApplicationError::InvalidCheckpoint)
    );
}
