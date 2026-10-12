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
fn held_cancellation_keeps_class_and_credit_until_exact_terminal_cancellation_cleanup() {
    for class in [ReadCapacityClass::Ordinary, ReadCapacityClass::Activation] {
        let mut r = selected();
        let ticket = ReadInvocationTicket {
            binding: r.binding,
            sequence: 1,
            group: group(1),
            request: ReadRequestId::new(1).unwrap(),
        };
        let admission = AdmissionTicket {
            owner: r.binding.owner,
            group: group(1),
            sequence: 1,
        };
        let charge = ReadInvocationUsage {
            requests: 1,
            bytes: 128,
        };
        // Original observation/accounting fixture; this grants no actual barrier.
        r.pending.insert(
            1,
            Pending {
                ticket,
                admission,
                cancellation: Some(admission),
                query: Some(Query::Active),
                query_bound: 0,
                result_bound: 64,
                charged: 128,
                class,
                term: 1,
                stepped: false,
                terminal: false,
                has_output: false,
                exported: false,
                released: false,
                outcome: None,
            },
        );
        r.groups.insert(group(1), 1);
        r.usage = charge;
        r.active.insert(group(1), 1);
        if class == ReadCapacityClass::Ordinary {
            r.bulk = charge;
            r.bulk_groups.insert(group(1), 1);
        }
        r.cancel_wait(ticket).unwrap();
        let output = r.poll().unwrap();
        assert_eq!(r.usage(), charge);
        let execution = ReadRouter::new(
            ReadRouterBinding::new(auth(), ReadRouterGeneration::new(2).unwrap()),
            ReadRouterLimits::default(),
        )
        .unwrap();
        let mut other = Requests::new(binding(2), ReadInvocationLimits::default(), execution)
            .unwrap_or_else(|_| panic!("physical"));
        let rejected = other.complete(output).unwrap_err();
        assert_eq!(rejected.reason, ReadInvocationError::StaleTicket);
        r.close();
        r.complete(*rejected.completion).unwrap();
        assert_eq!(r.usage(), charge);
        r.terminal(1);
        r.cleanup(1);
        assert_eq!(r.usage(), charge);
        assert!(!r.is_drained());
        assert_eq!(
            r.bulk,
            if class == ReadCapacityClass::Ordinary {
                charge
            } else {
                ReadInvocationUsage::default()
            }
        );
        r.pending.get_mut(&1).unwrap().cancellation = None;
        r.cleanup(1);
        assert_eq!(r.usage(), ReadInvocationUsage::default());
        assert_eq!(r.bulk, ReadInvocationUsage::default());
        assert!(
            r.active.is_empty()
                && r.groups.is_empty()
                && r.bulk_groups.is_empty()
                && r.is_drained()
        );
    }
}
