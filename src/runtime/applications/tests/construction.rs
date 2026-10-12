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
fn selected_construction_reserves_every_complete_declared_control_window() {
    let r = selected();
    assert_eq!(r.control_receipts, 6);
    assert_eq!(r.control_profile, Some(wrapper().control_profile()));
    for field in 0..6 {
        let mut l = limits();
        let mut actual = 3;
        match field {
            0 => l.control_batches = 1,
            1 => l.control_bytes = 2 * l.batch_bytes,
            2 => l.receipts = 8,
            3 => actual = 4,
            4 => actual = 0,
            _ => l.batch_bytes = 3 * size_of::<CounterReceipt>() - 1,
        }
        let r = ApplicationRouter::<CounterReceipt>::new(binding(1), l).unwrap();
        assert!(matches!(
            r.select_activation(wrapper().control_profile(), actual),
            Err(ApplicationRouteError::InvalidLimits)
        ));
    }
}
#[test]
fn ordinary_defaults_stay_unchanged_and_cannot_claim_selected_full_batches() {
    let r =
        ApplicationRouter::<CounterReceipt>::new(binding(1), ApplicationRouterLimits::default())
            .unwrap();
    assert_eq!(r.control_receipts, 0);
    assert_eq!(r.logical_batch_reservation(), None);
    assert!(r.control_profile.is_none());
    assert!(matches!(
        r.select_activation(wrapper().control_profile(), 16384),
        Err(ApplicationRouteError::InvalidLimits)
    ));
    let r = ApplicationRouter::<CounterReceipt>::new(binding(1), limits()).unwrap();
    assert_eq!(
        r.select_activation(wrapper().control_profile(), usize::MAX)
            .err(),
        Some(ApplicationRouteError::InvalidLimits)
    );
}

#[test]
fn selected_logical_batch_tracks_parent_log_instead_of_router_ceiling() {
    let router = || ApplicationRouter::<CounterReceipt>::new(binding(1), limits()).unwrap();
    for count in [0, 4, usize::MAX] {
        assert_eq!(
            router().select_logical_batch(count).err(),
            Some(ApplicationRouteError::InvalidLimits)
        );
    }
    let r = router().select_logical_batch(2).unwrap();
    assert_eq!(r.logical_batch_reservation(), Some(2));
    assert_eq!(r.limits().batch_receipts, 3);
    assert_eq!(r.control_receipts, 0);
    assert!(r.control_profile.is_none());
    assert!(r.is_drained());
}
