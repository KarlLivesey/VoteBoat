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
fn complete_selected_invocation_defaults_and_each_short_window_are_checked() {
    let l = ReadInvocationLimits::default();
    let worst = Requests::window_bytes(l).unwrap();
    assert_eq!(
        worst,
        l.query_bytes + l.result_bytes + size_of::<Pending<Query, Read>>()
    );
    assert!(l.control_requests * worst <= l.control_bytes);
    assert!(l.bytes - l.control_bytes >= worst);
    assert_eq!(Requests::check_windows(l), Ok(()));
    for field in 0..8 {
        let mut l = l;
        match field {
            0 => l.control_requests = 1,
            1 => l.group_control_requests = 1,
            2 => l.control_requests = l.requests,
            3 => l.group_control_requests = l.group_requests,
            4 => l.group_control_requests = l.control_requests + 1,
            5 => l.control_bytes = 2 * worst - 1,
            6 => l.bytes = l.control_bytes + worst - 1,
            _ => l.query_bytes = usize::MAX,
        }
        assert_eq!(
            Requests::check_windows(l),
            Err(ReadInvocationError::InvalidLimits)
        );
    }
}
#[test]
fn execution_profile_count_and_nested_maxima_must_match_selected_invocations() {
    let r = selected();
    let p = app(group(1)).control_profile();
    assert!(r.execution.fits_activation::<Query>(&p, r.limits));
    assert!(!r
        .execution
        .fits_activation::<Query>(&app(group(2)).control_profile(), r.limits));
    for field in 0..4 {
        let mut limits = r.limits;
        match field {
            0 => limits.control_requests += 1,
            1 => limits.group_control_requests += 1,
            2 => limits.query_bytes += 1,
            _ => limits.result_bytes += 1,
        }
        assert!(!r.execution.fits_activation::<Query>(&p, limits));
    }
}
#[test]
fn physical_constructor_ignores_reserves_and_keeps_original_execution_on_refusal() {
    let execution = ReadRouter::<Read>::new(
        ReadRouterBinding::new(auth(), ReadRouterGeneration::new(7).unwrap()),
        ReadRouterLimits::default(),
    )
    .unwrap();
    let original = execution.binding();
    let l = ReadInvocationLimits {
        requests: 1,
        group_requests: 1,
        control_requests: usize::MAX,
        group_control_requests: usize::MAX,
        control_bytes: usize::MAX,
        ..ReadInvocationLimits::default()
    };
    let r = Requests::new(binding(1), l, execution)
        .unwrap_or_else(|_| panic!("physical ignores reserves"));
    assert!(r.control_profile.is_none());
    assert_eq!(
        r.check_capacity(group(1), l.bytes, ReadCapacityClass::Ordinary),
        Ok(())
    );
    let rejected = Requests::new(
        binding(1),
        ReadInvocationLimits {
            query_bytes: usize::MAX,
            ..l
        },
        r.execution,
    )
    .err()
    .unwrap();
    assert_eq!(rejected.reason, ReadInvocationError::InvalidLimits);
    assert_eq!(rejected.execution.binding(), original);
    assert!(rejected.execution.is_drained());
}
