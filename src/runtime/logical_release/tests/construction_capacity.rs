// SPDX-License-Identifier: RPL-1.5
// Copyright (c) 2026 Karl Livesey
//! Pure capability checks; these controls mint no logical authority or effect.
use super::*;

#[test]
fn static_construction_rejects_whole_batch_even_when_one_capture_fits() {
    let mut r = accounting();
    let one = r.capture_charge(1).unwrap();
    let whole = r.capture_charge(3).unwrap();
    r.limits.control_bytes = one;
    r.limits.bytes = one + whole - 1;
    r.limits.validate().unwrap();
    assert_eq!(r.check_capacity(one, false), Ok(()));
    assert_eq!(r.check_capacity(one, true), Ok(()));
    assert_eq!(
        r.check_static_reservation(3),
        Err(LogicalReleaseError::InvalidLimits)
    );
    assert_eq!(r.usage(), LogicalReleaseUsage::default());
    assert!(r.is_drained());
}

#[test]
fn static_exact_boundary_preserves_one_control_capture_and_original_usage() {
    let mut r = accounting();
    let one = r.capture_charge(1).unwrap();
    let whole = r.capture_charge(3).unwrap();
    r.limits.control_bytes = one;
    r.limits.bytes = one + whole;
    r.limits.validate().unwrap();
    assert_eq!(r.check_static_reservation(3), Ok(()));
    capacity::hold(&mut r, false, whole);
    assert_eq!(r.check_capacity(one, true), Ok(()));
    assert_eq!(
        r.check_capacity(one, false),
        Err(LogicalReleaseError::Overloaded)
    );
    // Static construction reserves one control capture, not every activation
    // window. The selected activation contract must still refuse this profile.
    assert_eq!(
        r.select_activation(wrapper().control_profile(), 3).err(),
        Some(LogicalReleaseError::InvalidLimits)
    );
}

#[test]
fn static_capability_rejects_missing_control_zero_count_and_overflow() {
    for field in 0..4 {
        let mut r = accounting();
        let mut count = 3;
        match field {
            0 => r.limits.control_bytes = r.capture_charge(1).unwrap() - 1,
            1 => count = 0,
            2 => count = usize::MAX,
            _ => r.limits.capsule_bytes = usize::MAX,
        }
        assert_eq!(
            r.check_static_reservation(count),
            Err(LogicalReleaseError::InvalidLimits)
        );
        assert_eq!(r.usage(), LogicalReleaseUsage::default());
        assert!(r.is_drained());
    }
}
