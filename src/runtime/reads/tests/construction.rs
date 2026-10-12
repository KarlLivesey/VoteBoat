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
fn every_declared_complete_selected_result_window_and_one_ordinary_window_fit() {
    let l = ReadRouterLimits::default();
    let worst = ReadRouter::<i64>::window_bytes::<Vec<u8>>(l).unwrap();
    assert_eq!(
        worst,
        l.query_bytes + size_of::<Vec<u8>>() + l.result_bytes + size_of::<Held<i64>>()
    );
    assert!(worst * l.control_results <= l.control_bytes);
    assert!(l.bytes - l.control_bytes >= worst);
    let r = selected();
    assert_eq!(r.activation_profile(), Some(&profile()));
    assert_eq!(r.bulk, ReadResultUsage::default());
}
#[test]
fn short_selected_count_bytes_and_large_inline_query_refuse() {
    for field in 0..8 {
        let mut l = ReadRouterLimits::default();
        let worst = ReadRouter::<i64>::window_bytes::<Vec<u8>>(l).unwrap();
        match field {
            0 => l.control_results = 1,
            1 => l.group_control_results = 1,
            2 => l.control_results = l.results,
            3 => l.group_control_results = l.group_results,
            4 => l.group_control_results = l.control_results + 1,
            5 => l.control_bytes = 2 * worst - 1,
            6 => l.bytes = l.control_bytes + worst - 1,
            _ => l.control_bytes = usize::MAX,
        }
        let r = ReadRouter::<i64>::new(binding(), l).unwrap();
        assert_eq!(
            r.select_activation::<Vec<u8>>(profile()).err(),
            Some(ReadRouteError::InvalidLimits)
        );
    }
    assert_eq!(
        ReadRouter::<i64>::new(binding(), ReadRouterLimits::default())
            .unwrap()
            .select_activation::<[u8; 4 * 1024 * 1024]>(profile())
            .err(),
        Some(ReadRouteError::InvalidLimits)
    );
    let l = ReadRouterLimits {
        query_bytes: usize::MAX,
        ..ReadRouterLimits::default()
    };
    assert_eq!(ReadRouter::<i64>::window_bytes::<Vec<u8>>(l), None);
}
#[test]
fn physical_result_constructor_ignores_reserves_and_cannot_claim_selected_authority() {
    let l = ReadRouterLimits {
        results: 1,
        group_results: 1,
        control_results: usize::MAX,
        group_control_results: usize::MAX,
        control_bytes: usize::MAX,
        ..ReadRouterLimits::default()
    };
    let r = ReadRouter::<i64>::new(binding(), l).unwrap();
    assert!(r.activation_profile().is_none());
    assert_eq!(
        r.check_capacity(group(1), l.bytes, ReadCapacityClass::Ordinary),
        Ok(())
    );
    assert_eq!(
        r.check_capacity(group(1), 1, ReadCapacityClass::Activation),
        Err(ReadRouteError::WrongBinding)
    );
    assert_eq!(
        ReadRouter::<i64>::new_logical::<Vec<u8>>(binding(), l, None).err(),
        Some(ReadRouteError::WrongBinding)
    );
}
