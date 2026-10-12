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
fn ordinary_invocation_global_group_and_byte_pressure_preserve_selected_reserve() {
    for field in 0..3 {
        let mut r = selected();
        match field {
            0 => r.bulk.requests = r.limits.requests - r.limits.control_requests,
            1 => {
                r.bulk_groups.insert(
                    group(1),
                    r.limits.group_requests - r.limits.group_control_requests,
                );
            }
            _ => r.bulk.bytes = r.limits.bytes - r.limits.control_bytes,
        }
        r.usage = r.bulk;
        assert_eq!(
            r.check_capacity(group(1), 1, ReadCapacityClass::Ordinary),
            Err(ReadInvocationError::Overloaded)
        );
        assert_eq!(
            r.check_capacity(group(1), 1, ReadCapacityClass::Activation),
            Ok(())
        );
    }
}
#[test]
fn selected_invocation_still_obeys_total_count_group_and_byte_limits() {
    for field in 0..3 {
        let mut r = selected();
        match field {
            0 => r.usage.requests = r.limits.requests,
            1 => {
                r.groups.insert(group(1), r.limits.group_requests);
            }
            _ => r.usage.bytes = r.limits.bytes,
        }
        assert_eq!(
            r.check_capacity(group(1), 1, ReadCapacityClass::Activation),
            Err(ReadInvocationError::Overloaded)
        );
    }
}
