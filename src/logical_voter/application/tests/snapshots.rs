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
use crate::snapshot_worker::{SnapshotWorkTicket, SnapshotWorkerBinding};
fn snapshot_origin() -> SnapshotOrigin {
    SnapshotOrigin {
        request: SnapshotWorkTicket {
            binding: SnapshotWorkerBinding {
                store: owner().store,
                generation: SnapshotWorkerGeneration::new(1).unwrap(),
            },
            sequence: 1,
        },
        visit: visit(),
    }
}
fn snapshot(a: &LogicalApplication, index: u64) -> Snapshot {
    Snapshot {
        metadata: SnapshotMetadata {
            bootstrap: a.parent_bootstrap.clone(),
            membership: None,
            index,
            term: 1,
            application_schema: 1,
        },
        application: vec![index as u8],
    }
}
fn publish(a: &mut LogicalApplication, id: u128, s: Snapshot) -> SnapshotRef {
    let p = a.state.parent.as_ref().unwrap();
    let c = command(
        id,
        1,
        CommandBody::Publish {
            expected_revision: p.revision,
            durable: p.snapshot,
            snapshot: s,
            origin: snapshot_origin(),
        },
    );
    let LogicalOutcome::SnapshotPublished(reference) = apply(a, &c).outcome else {
        panic!("original publication");
    };
    reference
}
fn anchor(a: &mut LogicalApplication, id: u128, r: SnapshotRef) {
    let u = LogUpdate {
        snapshot: Some(r),
        ..update(
            a,
            r.index.max(a.state.parent.as_ref().unwrap().commit_index),
            None,
        )
    };
    let c = mutate(a, id, u);
    assert_eq!(apply(a, &c).outcome, LogicalOutcome::Mutated);
}
#[test]
fn two_publications_pin_original_anchor_until_exact_reconcile() {
    let mut a = app();
    claim(&mut a);
    let c = mutate(
        &a,
        2,
        update(
            &a,
            2,
            Some(Suffix {
                from: 1,
                entries: vec![
                    LogEntry {
                        index: 1,
                        term: 1,
                        payload: EntryPayload::Noop,
                    },
                    LogEntry {
                        index: 2,
                        term: 1,
                        payload: EntryPayload::Noop,
                    },
                ]
                .into(),
            }),
        ),
    );
    apply(&mut a, &c);
    let s = snapshot(&a, 1);
    let first = publish(&mut a, 3, s);
    anchor(&mut a, 4, first);
    let s = snapshot(&a, 2);
    let second = publish(&mut a, 5, s);
    assert_eq!(a.state.snapshots.len(), 2);
    let c = command(
        6,
        1,
        CommandBody::Reconcile {
            expected_revision: a.state.parent.as_ref().unwrap().revision,
            reference: second,
            origin: snapshot_origin(),
        },
    );
    assert_eq!(
        apply(&mut a, &c).outcome,
        LogicalOutcome::Refused(LogicalRefusal::SnapshotConflict)
    );
    let before = a.checkpoint(usize::MAX).unwrap();
    let mut recovered = app();
    recovered
        .restore_checkpoint(1, a.applied_index(), &before)
        .unwrap();
    assert_eq!(recovered.state, a.state);
    anchor(&mut recovered, 7, second);
    let c = command(
        8,
        1,
        CommandBody::Reconcile {
            expected_revision: recovered.state.parent.as_ref().unwrap().revision,
            reference: second,
            origin: snapshot_origin(),
        },
    );
    assert_eq!(
        apply(&mut recovered, &c).outcome,
        LogicalOutcome::SnapshotReconciled
    );
    assert_eq!(recovered.state.snapshots.len(), 1);
    assert_eq!(recovered.state.snapshots[0].0, second);
    assert!(apply(&mut recovered, &c).duplicate);
}
mod recovery;
#[test]
fn incoming_ahead_snapshot_must_be_pinned_before_install() {
    let mut a = app();
    claim(&mut a);
    let c = mutate(&a, 2, update(&a, 0, None));
    apply(&mut a, &c);
    let s = snapshot(&a, 5);
    let r = publish(&mut a, 3, s);
    assert_eq!(a.state.parent.as_ref().unwrap().commit_index, 0);
    let mut forged = r;
    forged.checksum ^= 1;
    let c = mutate(
        &a,
        4,
        LogUpdate {
            snapshot: Some(forged),
            ..update(&a, 5, None)
        },
    );
    assert_eq!(
        apply(&mut a, &c).outcome,
        LogicalOutcome::Refused(LogicalRefusal::MissingSnapshot)
    );
    anchor(&mut a, 5, r);
    assert_eq!(a.state.parent.as_ref().unwrap().base_index(), 5);
    let bytes = a.checkpoint(usize::MAX).unwrap();
    let mut restored = app();
    restored
        .restore_checkpoint(1, a.applied_index(), &bytes)
        .unwrap();
    assert_eq!(restored.state, a.state);
}
