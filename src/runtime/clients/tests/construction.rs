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
fn defaults_hold_every_declared_complete_control_window_and_one_ordinary_window() {
    let l = ClientRouterLimits::default();
    let worst = ClientRouter::<CounterReceipt>::window_bytes(l).unwrap();
    assert_eq!(
        worst,
        2 * l.command_bytes + l.receipt_bytes + size_of::<Pending<CounterReceipt>>()
    );
    assert!(l.control_bytes >= l.control_requests * worst);
    assert!(l.bytes - l.control_bytes >= worst);
    let r = ClientRouter::<CounterReceipt>::new(binding(1), l)
        .unwrap()
        .select_activation(profile())
        .unwrap();
    assert_eq!(r.control_profile.as_ref(), Some(&profile()));
    assert_eq!(r.bulk, ClientUsage::default());
}
#[test]
fn short_selected_count_bytes_and_profile_limits_refuse_before_transfer() {
    for field in 0..8 {
        let mut l = limits();
        match field {
            0 => l.control_requests = 1,
            1 => l.group_control_requests = 1,
            2 => l.control_requests = l.requests,
            3 => l.group_control_requests = l.group_requests,
            4 => l.group_control_requests = l.control_requests + 1,
            5 => l.control_bytes -= 1,
            6 => l.bytes -= 1,
            _ => l.command_bytes -= 1,
        }
        let r = ClientRouter::<CounterReceipt>::new(binding(1), l).unwrap();
        assert_eq!(
            r.select_activation(profile()).err(),
            Some(ClientError::InvalidLimits)
        );
    }
    let mut l = limits();
    l.command_bytes = usize::MAX;
    assert_eq!(ClientRouter::<CounterReceipt>::window_bytes(l), None);
    l.command_bytes = 1;
    l.receipt_bytes = usize::MAX;
    assert_eq!(ClientRouter::<CounterReceipt>::window_bytes(l), None);
}
#[test]
fn unselected_physical_construction_ignores_new_reserves() {
    let l = ClientRouterLimits {
        requests: 1,
        group_requests: 1,
        control_requests: usize::MAX,
        group_control_requests: usize::MAX,
        control_bytes: usize::MAX,
        ..ClientRouterLimits::default()
    };
    let r = ClientRouter::<CounterReceipt>::new(binding(1), l).unwrap();
    assert!(r.control_profile.is_none());
    assert_eq!(r.request_control(&release()), Ok(false));
    assert_eq!(r.check_capacity(group(1), l.bytes, false), Ok(()));
}

#[test]
fn nested_command_profile_keeps_complete_ordinary_and_control_windows() {
    let mut l = limits();
    l.command_bytes = 2 * 1024 * 1024;
    let window = ClientRouter::<CounterReceipt>::window_bytes(l).unwrap();
    l.control_bytes = 2 * window;
    l.bytes = 3 * window;
    let mut router = ClientRouter::<CounterReceipt>::new(binding(1), l)
        .unwrap()
        .select_activation(profile())
        .unwrap();
    assert_eq!(router.check_capacity(group(1), window, false), Ok(()));
    assert_eq!(
        router.check_capacity(group(1), window + 1, false),
        Err(ClientError::Overloaded)
    );
    assert_eq!(router.check_capacity(group(1), 2 * window, true), Ok(()));
    router.usage.bytes = window;
    assert_eq!(
        router.check_capacity(group(1), 2 * window + 1, true),
        Err(ClientError::Overloaded)
    );
    l.command_bytes = 16 * 1024 * 1024 + 1;
    assert_eq!(
        ClientRouter::<CounterReceipt>::new(binding(1), l).err(),
        Some(ClientError::InvalidLimits)
    );
}
