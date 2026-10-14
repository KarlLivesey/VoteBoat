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
use std::mem::size_of;
#[test]
fn read_bound_is_stable_across_growth_and_actual_allocations_are_charged() {
    let mut a = app();
    let query = LogicalQuery::Image;
    let bound = a.read_result_bound(&query).unwrap();
    claim(&mut a);
    let c = mutate(
        &a,
        2,
        update(
            &a,
            1,
            Some(Suffix {
                from: 1,
                entries: vec![LogEntry {
                    index: 1,
                    term: 1,
                    payload: EntryPayload::Command {
                        operation: OperationId::new(101).unwrap(),
                        bytes: vec![1; 1024].into(),
                    },
                }],
            }),
        ),
    );
    apply(&mut a, &c);
    assert_eq!(a.read_result_bound(&query).unwrap(), bound);
    let result = a.read_at(2, query).unwrap();
    let charge = a.read_result_bytes(&result, usize::MAX).unwrap();
    assert!(charge > 1024);
    assert!(size_of::<LogicalRead>() + charge <= bound);
    assert_eq!(
        a.read_result_bytes(&result, charge - 1),
        Err(ApplicationError::ReceiptBudget)
    );
    let outcome = a.read_at(2, LogicalQuery::Outcome(c.operation)).unwrap();
    let LogicalRead::Outcome(Some(s)) = &outcome else {
        panic!("retained action");
    };
    assert_eq!(
        a.read_result_bytes(&outcome, usize::MAX).unwrap(),
        size_of::<LogicalActionStatus>() + s.command.capacity()
    );
    let checkpoint = a.checkpoint(usize::MAX).unwrap();
    a.read_at(2, query).unwrap();
    assert_eq!(a.checkpoint(usize::MAX).unwrap(), checkpoint);
}
