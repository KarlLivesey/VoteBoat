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
use super::*;
#[test]
fn every_declared_window_and_one_whole_ordinary_capture_must_fit() {
    let r = selected();
    let worst = r.capture_charge(3).unwrap();
    for field in 0..5 {
        let mut r = accounting();
        match field {
            0 => r.limits.control_pending = 1,
            1 => r.limits.control_bytes = 2 * worst - 1,
            2 => r.limits.bytes = r.limits.control_bytes + worst - 1,
            3 => {}
            _ => {}
        }
        let count = if field == 3 {
            0
        } else if field == 4 {
            usize::MAX
        } else {
            3
        };
        assert_eq!(
            r.select_activation(wrapper().control_profile(), count)
                .err(),
            Some(LogicalReleaseError::InvalidLimits)
        );
    }
    let mut r = accounting();
    r.limits.control_bytes = 2 * worst;
    r.limits.bytes = 3 * worst;
    let r = r.select_activation(wrapper().control_profile(), 3).unwrap();
    assert_eq!(r.usage(), LogicalReleaseUsage::default());
    assert!(r.is_drained());
}
pub(super) fn hold<R, V>(r: &mut LogicalReleaseCoordinator<R, V>, control: bool, charged: usize) {
    let sequence = r.pending.len() as u64 + 1;
    // Deliberately synthetic retained storage only; this is never submitted to
    // an original owner, published, or used to supply a child authorization.
    let ticket = EffectTicket {
        visit: VisitTicket {
            owner: owner(),
            group: group(1),
            sequence,
        },
        sequence,
    };
    r.pending.insert(
        sequence,
        Pending {
            payload: Payload::Send(Box::new(EffectLease {
                ticket,
                effect: Effect::Committed(vec![]),
            })),
            capsules: vec![],
            charged,
            control,
        },
    );
    r.usage.pending += 1;
    r.usage.bytes += charged;
}
#[test]
fn ordinary_count_and_bytes_leave_all_control_windows_available() {
    for field in 0..2 {
        let mut r = selected();
        let worst = r.capture_charge(3).unwrap();
        if field == 0 {
            for _ in 0..r.limits.pending - r.limits.control_pending {
                hold(&mut r, false, 1);
            }
        } else {
            let bulk = r.limits.bytes - r.limits.control_bytes;
            hold(&mut r, false, bulk);
        }
        assert_eq!(
            r.check_capacity(worst, false),
            Err(LogicalReleaseError::Overloaded)
        );
        for _ in 0..r.limits.control_pending {
            assert_eq!(r.check_capacity(worst, true), Ok(()));
            hold(&mut r, true, worst);
        }
    }
}
#[test]
fn control_capture_still_obeys_total_limits_and_sequence_exhaustion() {
    for field in 0..3 {
        let mut r = selected();
        let worst = r.capture_charge(3).unwrap();
        match field {
            0 => r.usage.pending = r.limits.pending,
            1 => r.usage.bytes = r.limits.bytes,
            _ => r.sequence = u64::MAX,
        }
        let expected = if field == 2 {
            LogicalReleaseError::Exhausted
        } else {
            LogicalReleaseError::Overloaded
        };
        assert_eq!(r.check_capacity(worst, true), Err(expected));
    }
}

fn original_nested_limits() -> LogicalReleaseCoordinator<LogicalReceipt, LogicalRead> {
    let base = accounting();
    // Accounting only: no logical owner, event or child authorization exists.
    LogicalReleaseCoordinator {
        authorization: base.authorization,
        control_profile: None,
        limits: LogicalReleaseLimits {
            pending: 32,
            bytes: 4 * 1_067_008,
            capsule_bytes: 1_067_008,
            control_pending: 8,
            control_bytes: 2 * 1_067_008,
        },
        pending: BTreeMap::new(),
        requests: VecDeque::new(),
        sequence: 0,
        usage: LogicalReleaseUsage::default(),
        closed: false,
        failed_domain: false,
    }
}
#[test]
fn capsule_only_profile_cannot_admit_its_first_typed_capture() {
    let r = original_nested_limits();
    let charged = r
        .limits
        .capture_reservation_bytes::<LogicalReceipt, LogicalRead>(1)
        .unwrap();
    assert!(charged > r.limits.bytes);
    assert_eq!(r.capture_charge(1).unwrap(), charged);
    assert_eq!(
        r.check_capacity(charged, true),
        Err(LogicalReleaseError::Overloaded)
    );
    assert_eq!(
        r.check_capacity(charged, false),
        Err(LogicalReleaseError::Overloaded)
    );
    assert!(r.is_drained());
}
#[test]
fn whole_static_batch_leaves_one_original_control_capture_reserved() {
    let mut r = original_nested_limits();
    let charged = r
        .limits
        .capture_reservation_bytes::<LogicalReceipt, LogicalRead>(32)
        .unwrap();
    r.limits.bytes = charged.checked_mul(2).unwrap();
    r.limits.control_bytes = charged;
    r.limits.validate().unwrap();
    assert!(r.limits.bytes <= 1024 * 1024 * 1024);
    assert_eq!(r.check_capacity(charged, false), Ok(()));
    hold(&mut r, false, charged);
    assert_eq!(
        r.check_capacity(charged, false),
        Err(LogicalReleaseError::Overloaded)
    );
    assert_eq!(r.check_capacity(charged, true), Ok(()));
    hold(&mut r, true, charged);
    assert_eq!(
        r.check_capacity(charged, true),
        Err(LogicalReleaseError::Overloaded)
    );
    assert_eq!(r.limits.pending, 32);
    assert_eq!(r.limits.control_pending, 8);
}
#[test]
fn public_capture_footprint_preserves_checked_overflow_refusal() {
    let r = original_nested_limits();
    assert_eq!(
        r.limits
            .capture_reservation_bytes::<LogicalReceipt, LogicalRead>(usize::MAX),
        Err(LogicalReleaseError::Overloaded)
    );
    let oversized = LogicalReleaseLimits {
        capsule_bytes: usize::MAX,
        ..r.limits
    };
    assert_eq!(
        oversized.capture_reservation_bytes::<CounterReceipt, i64>(1),
        Err(LogicalReleaseError::Overloaded)
    );
    assert_eq!(r.usage(), LogicalReleaseUsage::default());
    assert!(r.is_drained());
}
