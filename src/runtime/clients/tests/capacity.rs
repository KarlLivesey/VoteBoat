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
fn ordinary_global_group_and_byte_ceilings_cannot_spend_reserve() {
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
            r.check_capacity(group(1), 1, false),
            Err(ClientError::Overloaded)
        );
        assert_eq!(r.check_capacity(group(1), 1, true), Ok(()));
    }
}
#[test]
fn selected_work_still_obeys_total_count_group_and_byte_limits() {
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
            r.check_capacity(group(1), 1, true),
            Err(ClientError::Overloaded)
        );
    }
    let mut r = selected();
    let worst = ClientRouter::<CounterReceipt>::window_bytes(r.limits).unwrap();
    // Pure retained-accounting boundaries, without claimed Applied/child evidence.
    r.usage = ClientUsage {
        requests: 1,
        bytes: worst,
    };
    assert_eq!(r.check_capacity(group(1), worst, true), Ok(()));
    r.usage = ClientUsage {
        requests: 2,
        bytes: 2 * worst,
    };
    assert_eq!(r.check_capacity(group(1), worst, false), Ok(()));
    assert_eq!(
        r.check_capacity(group(1), worst + 1, false),
        Err(ClientError::Overloaded)
    );
}
