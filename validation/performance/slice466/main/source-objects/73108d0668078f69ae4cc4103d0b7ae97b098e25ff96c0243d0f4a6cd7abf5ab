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
fn all_read_variants_preserve_exact_typed_values() {
    let mut a = app();
    assert_read(&a, LogicalRead::Uninitialized);
    assert_read(&a, LogicalRead::Outcome(None));
    claim(&mut a);
    for q in [
        LogicalQuery::Image,
        LogicalQuery::Metadata,
        LogicalQuery::Outcome(OperationId::new(1).unwrap()),
    ] {
        assert_read(&a, a.read_at(1, q).unwrap());
    }
    let snapshot = Snapshot {
        metadata: SnapshotMetadata {
            bootstrap: a.parent_bootstrap.clone(),
            membership: None,
            index: 1,
            term: 1,
            application_schema: 42,
        },
        application: vec![3, 4, 5],
    };
    assert_read(&a, LogicalRead::Snapshot(Box::new(snapshot.clone())));
    let mut changed = snapshot.clone();
    changed.application[2] = 6;
    assert_ne!(
        encoded(&a, &LogicalRead::Snapshot(Box::new(snapshot))),
        encoded(&a, &LogicalRead::Snapshot(Box::new(changed.clone())))
    );
    assert_read(&a, LogicalRead::Snapshot(Box::new(changed)));
}
#[test]
fn complete_outcome_status_and_maximum_command_fit_the_static_bound() {
    let a = app();
    let s = LogicalActionStatus {
        operation: OperationId::new(30).unwrap(),
        first_index: 2,
        first_term: 7,
        preceding_configuration: ConfigurationId::new(3).unwrap(),
        command: vec![0x3a; a.limits.max_command_bytes],
        receipt: receipt(),
    };
    let value = LogicalRead::Outcome(Some(Box::new(s.clone())));
    let b = encoded(&a, &value);
    assert_eq!(
        b.len(),
        a.read_encoding_bound(&LogicalQuery::Outcome(s.operation))
            .unwrap()
    );
    assert_read(&a, value);
    let changes: [fn(&mut LogicalActionStatus); 6] = [
        |s| s.operation = OperationId::new(31).unwrap(),
        |s| s.first_index += 1,
        |s| s.first_term += 1,
        |s| s.preceding_configuration = ConfigurationId::new(4).unwrap(),
        |s| s.command[0] ^= 1,
        |s| s.receipt.duplicate = false,
    ];
    for change in changes {
        let mut other = s.clone();
        change(&mut other);
        let value = LogicalRead::Outcome(Some(Box::new(other)));
        assert_ne!(encoded(&a, &value), b);
        assert_read(&a, value);
    }
}
#[test]
fn complete_image_membership_parent_and_pin_fields_are_retained() {
    let mut a = app();
    claim(&mut a);
    let original = image(&a);
    let original_bytes = encoded(&a, &LogicalRead::Image(Box::new(original.clone())));
    let mut variants = Vec::new();
    let mut i = original.clone();
    i.child_applied += 1;
    variants.push(i);
    let mut i = original.clone();
    i.representative.physical_session = StoreSession::new(3).unwrap();
    variants.push(i);
    let mut i = original.clone();
    i.representative.logical_session = StoreSession::new(3).unwrap();
    variants.push(i);
    let mut i = original.clone();
    i.representative.epoch = RepresentativeEpoch::new(3).unwrap();
    variants.push(i);
    let mut i = original.clone();
    i.mapping.voter = NodeId::new(3).unwrap();
    variants.push(i);
    let mut i = original.clone();
    i.published = Some(reference());
    i.pins = vec![
        reference(),
        SnapshotRef {
            checksum: 0,
            ..reference()
        },
    ];
    variants.push(i);
    let mut i = original.clone();
    let mut b = a.child_bootstrap.clone();
    b.configuration = ConfigurationId::new(2).unwrap();
    i.child_membership = Membership::replay(&b, &[], 0).unwrap();
    variants.push(i);
    let mut i = original.clone();
    let mut groups = BTreeMap::from([(i.mapping.parent, i.parent.clone())]);
    apply_batch(
        &mut groups,
        &[LogMutation::Update(LogUpdate {
            group: i.mapping.parent,
            expected_revision: i.parent.revision,
            hard_state: crate::contracts::HardState {
                term: 1,
                voted_for: Some(NodeId::new(1).unwrap()),
            },
            commit_index: 1,
            suffix: Some(Suffix {
                from: 1,
                entries: vec![LogEntry {
                    index: 1,
                    term: 1,
                    payload: EntryPayload::Command {
                        operation: OperationId::new(99).unwrap(),
                        bytes: vec![9, 8, 7].into(),
                    },
                }]
                .into(),
            }),
            snapshot: None,
            snapshot_membership: None,
        })],
        a.limits.parent_log,
    )
    .unwrap();
    i.parent = groups.remove(&i.mapping.parent).unwrap();
    variants.push(i);
    for i in variants {
        let value = LogicalRead::Image(Box::new(i));
        assert_ne!(encoded(&a, &value), original_bytes);
        assert_read(&a, value);
    }
}
#[test]
fn metadata_optional_and_counter_fields_are_retained() {
    let mut a = app();
    claim(&mut a);
    let original = a.metadata();
    assert_eq!(
        encoded(&a, &LogicalRead::Metadata(Box::new(original))).len(),
        METADATA_BOUND
    );
    let changes: [fn(&mut LogicalMetadata); 7] = [
        |m| m.mapping.voter = NodeId::new(3).unwrap(),
        |m| m.representative = None,
        |m| m.child_applied += 1,
        |m| m.retained_actions += 1,
        |m| m.retained_action_bytes += 1,
        |m| m.parent_revision = None,
        |m| m.parent_generation = None,
    ];
    let b = encoded(&a, &LogicalRead::Metadata(Box::new(original)));
    for change in changes {
        let mut m = original;
        change(&mut m);
        let value = LogicalRead::Metadata(Box::new(m));
        assert_ne!(encoded(&a, &value), b);
        assert_read(&a, value);
    }
}
