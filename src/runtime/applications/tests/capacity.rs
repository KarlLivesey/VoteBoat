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
fn ordinary_receipts_batches_and_bytes_cannot_spend_control_reservations() {
    for field in 0..3 {
        let mut r = selected();
        match field {
            0 => r.bulk.receipts = r.limits.receipts - r.control_receipts,
            1 => r.bulk.batches = r.limits.batches - r.limits.control_batches,
            _ => r.bulk.bytes = r.limits.bytes - r.limits.control_bytes,
        }
        r.usage = r.bulk;
        assert_eq!(
            r.check_capacity(3, 2048, false),
            Err(ApplicationRouteError::Overloaded)
        );
        assert_eq!(r.check_capacity(3, 2048, true), Ok(()));
    }
}
#[test]
fn selected_control_still_obeys_total_counts_and_bytes() {
    for field in 0..3 {
        let mut r = selected();
        match field {
            0 => r.usage.receipts = r.limits.receipts,
            1 => r.usage.batches = r.limits.batches,
            _ => r.usage.bytes = r.limits.bytes,
        }
        assert_eq!(
            r.check_capacity(1, 1, true),
            Err(ApplicationRouteError::Overloaded)
        );
    }
}
fn output(r: &ApplicationRouter<CounterReceipt>) -> ApplicationResults<CounterReceipt> {
    ApplicationResults {
        ticket: ApplicationResultTicket {
            binding: r.binding,
            sequence: 1,
        },
        effect: EffectTicket {
            visit: VisitTicket {
                owner: r.binding.owner,
                group: group(1),
                sequence: 1,
            },
            sequence: 1,
        },
        through: 3,
        receipts: vec![],
        positions: Box::new([]),
    }
}
#[test]
fn externally_held_credit_survives_wrong_completion_and_close_until_exact_consume() {
    let mut r = selected();
    let held = output(&r);
    let charged = ApplicationResultUsage {
        batches: 1,
        receipts: 3,
        bytes: 2048,
    };
    r.usage = charged;
    // Unit accounting only: an original capture's Held record remains present
    // after its output transfers. This fixture never applies or releases a lease.
    r.held.insert(
        1,
        Held {
            ticket: held.ticket,
            output: None,
            charged,
            control: true,
        },
    );
    let mut other = ApplicationRouter::<CounterReceipt>::new(binding(2), limits()).unwrap();
    let rejected = other.complete_owned(held).unwrap_err();
    assert_eq!(rejected.reason, ApplicationRouteError::StaleResult);
    r.close();
    assert_eq!(r.usage(), charged);
    assert!(!r.is_drained());
    r.complete_owned(*rejected.results).unwrap();
    assert_eq!(r.usage(), ApplicationResultUsage::default());
    assert_eq!(r.bulk, ApplicationResultUsage::default());
    assert!(r.is_drained());
}
