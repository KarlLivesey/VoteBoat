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
fn original_configuration_positions_survive_own_checkpoint_and_suffix() {
    let mut a = app(LogicalLimits::default());
    let bound = a
        .read_result_bound(&LogicalQuery::ConfigurationOperation(op(11)))
        .unwrap();
    a.apply_batch(&prefix()).unwrap();
    assert_eq!(history(&a, 10).entries, vec![learners()]);
    assert_eq!(history(&a, 11).entries, vec![joint()]);
    let bytes = a.checkpoint(usize::MAX).unwrap();
    let mut recovered = app(LogicalLimits::default());
    recovered
        .restore_checkpoint(LOGICAL_APPLICATION_SCHEMA, 4, &bytes)
        .unwrap();
    assert_eq!(history(&recovered, 11), history(&a, 11));
    recovered.apply_batch(&suffix()).unwrap();
    a.apply_batch(&suffix()).unwrap();
    let h = history(&recovered, 11);
    assert_eq!(h.child, recovered.mapping().child);
    assert_eq!(h.operation, op(11));
    assert_eq!(h.child_applied, 7);
    assert_eq!(h.entries, vec![joint(), final_entry()]);
    assert_eq!(history(&a, 11), h);
    assert_eq!(
        recovered
            .read_result_bound(&LogicalQuery::ConfigurationOperation(op(11)))
            .unwrap(),
        bound
    );
    let full = recovered.checkpoint(usize::MAX).unwrap();
    let mut cold = app(LogicalLimits::default());
    cold.restore_checkpoint(LOGICAL_APPLICATION_SCHEMA, 7, &full)
        .unwrap();
    assert_eq!(history(&cold, 11), h);
    assert_eq!(cold.checkpoint(usize::MAX).unwrap(), full);
}
#[test]
fn unknown_is_scoped_inspection_and_future_prefix_is_not_applied() {
    let mut a = app(LogicalLimits::default());
    let initial = history(&a, 999);
    assert_eq!(initial.child_applied, 0);
    assert!(initial.entries.is_empty());
    assert_eq!(
        a.read_at(1, LogicalQuery::ConfigurationOperation(op(999))),
        Err(ApplicationError::NotApplied)
    );
    a.apply_batch(&prefix()).unwrap();
    let unknown = history(&a, 999);
    assert_eq!(unknown.child_applied, 4);
    assert!(unknown.entries.is_empty());
    // read_at may be ahead of the requested barrier: driver must check original positions.
    let LogicalRead::ConfigurationOperation(h) = a
        .read_at(2, LogicalQuery::ConfigurationOperation(op(11)))
        .unwrap()
    else {
        panic!("history")
    };
    assert_eq!(h.child_applied, 4);
    assert_eq!(h.entries[0].index, 4);
}
#[test]
fn invalid_configuration_progression_is_atomic() {
    let mut a = app(LogicalLimits::default());
    a.apply_batch(&prefix()).unwrap();
    let original = a.checkpoint(usize::MAX).unwrap();
    let mut invalid = suffix();
    let EntryPayload::Configuration(c) = &mut invalid[2].payload else {
        panic!("configuration")
    };
    c.operation = op(12);
    assert!(a.apply_batch(&invalid).is_err());
    assert_eq!(a.checkpoint(usize::MAX).unwrap(), original);
    assert_eq!(history(&a, 11).entries, vec![joint()]);
}
