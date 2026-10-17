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
fn pending(r: &ClientRouter<CounterReceipt>, control: bool) -> Pending<CounterReceipt> {
    let ticket = ClientTicket {
        binding: r.binding,
        sequence: 1,
        group: group(1),
        operation: OperationId::new(14).unwrap(),
    };
    Pending {
        ticket,
        admission: AdmissionTicket {
            owner: r.binding.owner,
            group: group(1),
            sequence: 1,
        },
        bytes: vec![1],
        term: 1,
        receipt_bound: size_of::<CounterReceipt>(),
        charged: 128,
        control,
        position: None,
        stepped: false,
        terminal: false,
        has_output: false,
        exported: false,
        released: false,
        outcome: None,
    }
}
#[test]
fn canceled_observation_and_foreign_completion_keep_class_until_terminal_cleanup() {
    for control in [false, true] {
        let mut r = selected();
        let p = pending(&r, control);
        let ticket = p.ticket;
        let charge = ClientUsage {
            requests: 1,
            bytes: p.charged,
        };
        r.pending.insert(1, Box::new(p));
        r.groups.insert(group(1), 1);
        r.usage = charge;
        if !control {
            r.bulk = charge;
            r.bulk_groups.insert(group(1), 1);
        }
        // Only original accounting/observation transitions are exercised. This
        // inserted record grants no Applied result, log takeover or authority.
        r.cancel_wait(ticket).unwrap();
        let output = r.poll().unwrap();
        assert!(matches!(
            output.outcome(),
            ClientOutcome::Unknown(ClientUnknown::CancelledWait)
        ));
        assert_eq!(r.usage(), charge);
        let mut other = ClientRouter::<CounterReceipt>::new(binding(2), limits()).unwrap();
        let rejected = other.complete(output).unwrap_err();
        assert_eq!(rejected.reason, ClientError::StaleTicket);
        assert_eq!(other.usage(), ClientUsage::default());
        r.close();
        r.complete(*rejected.completion).unwrap();
        assert_eq!(r.usage(), charge);
        assert!(!r.is_drained());
        assert_eq!(
            r.bulk,
            if control {
                ClientUsage::default()
            } else {
                charge
            }
        );
        r.pending.get_mut(&1).unwrap().terminal = true;
        r.cleanup(1);
        assert_eq!(r.usage(), ClientUsage::default());
        assert_eq!(r.bulk, ClientUsage::default());
        assert!(r.groups.is_empty() && r.bulk_groups.is_empty() && r.is_drained());
        r.cleanup(1);
        assert_eq!(r.usage(), ClientUsage::default());
    }
}
