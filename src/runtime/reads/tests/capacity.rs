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
fn ordinary_result_counts_groups_and_bytes_cannot_spend_selected_reserve() {
    for field in 0..3 {
        let mut r = selected();
        match field {
            0 => r.bulk.results = r.limits.results - r.limits.control_results,
            1 => {
                r.bulk_groups.insert(
                    group(1),
                    r.limits.group_results - r.limits.group_control_results,
                );
            }
            _ => r.bulk.bytes = r.limits.bytes - r.limits.control_bytes,
        }
        r.usage = r.bulk;
        assert_eq!(
            r.check_capacity(group(1), 1, ReadCapacityClass::Ordinary),
            Err(ReadRouteError::Overloaded)
        );
        assert_eq!(
            r.check_capacity(group(1), 1, ReadCapacityClass::Activation),
            Ok(())
        );
    }
}
#[test]
fn selected_result_class_still_checks_exact_group_and_total_limits() {
    for field in 0..3 {
        let mut r = selected();
        match field {
            0 => r.usage.results = r.limits.results,
            1 => {
                r.groups.insert(group(1), r.limits.group_results);
            }
            _ => r.usage.bytes = r.limits.bytes,
        }
        assert_eq!(
            r.check_capacity(group(1), 1, ReadCapacityClass::Activation),
            Err(ReadRouteError::Overloaded)
        );
    }
    assert_eq!(
        selected().check_capacity(group(2), 1, ReadCapacityClass::Activation),
        Err(ReadRouteError::WrongBinding)
    );
}
