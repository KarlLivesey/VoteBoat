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
fn progress(a: &mut LogicalApplication) {
    let c = mutate(
        a,
        2,
        update(
            a,
            3,
            Some(Suffix {
                from: 1,
                entries: (1..=3)
                    .map(|index| LogEntry {
                        index,
                        term: 1,
                        payload: EntryPayload::Noop,
                    })
                    .collect(),
            }),
        ),
    );
    assert_eq!(apply(a, &c).outcome, LogicalOutcome::Mutated);
}
fn publication(a: &LogicalApplication, id: u128, s: Snapshot) -> Command {
    let r = a.state.representative.unwrap();
    let p = a.state.parent.as_ref().unwrap();
    let mut origin = snapshot_origin();
    origin.request.binding.store.session = r.logical_session;
    origin.visit.owner.store.session = r.logical_session;
    command(
        id,
        r.epoch.get(),
        CommandBody::Publish {
            expected_revision: p.revision,
            durable: p.snapshot,
            snapshot: s,
            origin,
        },
    )
}
fn published(a: &mut LogicalApplication, c: &Command) -> SnapshotRef {
    let LogicalOutcome::SnapshotPublished(r) = apply(a, c).outcome else {
        panic!("original publication")
    };
    r
}
fn reconcile(a: &LogicalApplication, id: u128, reference: SnapshotRef) -> Command {
    let r = a.state.representative.unwrap();
    let mut origin = snapshot_origin();
    origin.request.binding.store.session = r.logical_session;
    origin.visit.owner.store.session = r.logical_session;
    command(
        id,
        r.epoch.get(),
        CommandBody::Reconcile {
            expected_revision: a.state.parent.as_ref().unwrap().revision,
            reference,
            origin,
        },
    )
}
fn restored_claim(a: &LogicalApplication, id: u128) -> LogicalApplication {
    let checkpoint = a.checkpoint(usize::MAX).unwrap();
    let mut recovered = app();
    recovered
        .restore_checkpoint(1, a.applied_index(), &checkpoint)
        .unwrap();
    assert_eq!(recovered.state, a.state);
    let c = command(
        id,
        2,
        CommandBody::Claim {
            expected_epoch: Some(representative(1).epoch),
        },
    );
    assert_eq!(apply(&mut recovered, &c).outcome, LogicalOutcome::Claimed);
    recovered
}
#[test]
fn anchored_orphan_recovery_reuses_bytes_then_keeps_generations_after_reconcile() {
    let mut a = app();
    claim(&mut a);
    progress(&mut a);
    let s = snapshot(&a, 1);
    let first = publish(&mut a, 3, s);
    anchor(&mut a, 4, first);
    let second_image = snapshot(&a, 2);
    let old = publication(&a, 5, second_image.clone());
    let orphan = published(&mut a, &old);
    let mut recovered = restored_claim(&a, 6);
    let retry = publication(&recovered, 7, second_image.clone());
    assert_eq!(published(&mut recovered, &retry), orphan);
    assert_eq!(recovered.state.snapshots.len(), 2);
    let selected = reconcile(&recovered, 8, first);
    assert_eq!(
        apply(&mut recovered, &selected).outcome,
        LogicalOutcome::SnapshotReconciled
    );
    assert_eq!(recovered.state.published, Some(first));
    assert_eq!(recovered.state.snapshots.len(), 1);
    let duplicate = apply(&mut recovered, &old);
    assert!(duplicate.duplicate);
    assert_eq!(duplicate.outcome, LogicalOutcome::SnapshotPublished(orphan));
    assert_eq!(
        recovered.state.snapshots.len(),
        1,
        "historical retry does not recreate orphan bytes"
    );
    let retry = publication(&recovered, 9, second_image);
    let fresh = published(&mut recovered, &retry);
    assert!(fresh.generation > orphan.generation);
    let newer = publication(&recovered, 10, snapshot(&recovered, 3));
    let newest = published(&mut recovered, &newer);
    assert!(newest.generation > fresh.generation);
    assert_eq!(
        recovered
            .state
            .snapshots
            .iter()
            .map(|(r, _)| *r)
            .collect::<Vec<_>>(),
        vec![first, newest]
    );
    assert!(apply(&mut recovered, &selected).duplicate);
    assert_eq!(recovered.state.published, Some(newest));
    let checkpoint = recovered.checkpoint(usize::MAX).unwrap();
    let mut second = app();
    second
        .restore_checkpoint(1, recovered.applied_index(), &checkpoint)
        .unwrap();
    assert_eq!(second.state, recovered.state);
}
#[test]
fn identical_anchor_selection_and_invalid_publication_preserve_durable_pins() {
    let mut a = app();
    claim(&mut a);
    progress(&mut a);
    let image = snapshot(&a, 1);
    let first = publish(&mut a, 3, image.clone());
    anchor(&mut a, 4, first);
    let orphan = publication(&a, 5, snapshot(&a, 2));
    published(&mut a, &orphan);
    let pins = a.state.snapshots.clone();
    let pointer = a.state.published;
    let parent = a.state.parent.clone();
    let mut bad = snapshot(&a, 2);
    bad.metadata.application_schema = 2;
    let c = publication(&a, 6, bad);
    assert_eq!(
        apply(&mut a, &c).outcome,
        LogicalOutcome::Refused(LogicalRefusal::SnapshotConflict)
    );
    let mut c = publication(&a, 7, snapshot(&a, 3));
    if let CommandBody::Publish { durable, .. } = &mut c.body {
        *durable = None;
    }
    assert_eq!(
        apply(&mut a, &c).outcome,
        LogicalOutcome::Refused(LogicalRefusal::StaleImage)
    );
    assert_eq!(a.state.snapshots, pins);
    assert_eq!(a.state.published, pointer);
    assert_eq!(a.state.parent, parent);
    let c = publication(&a, 8, image);
    assert_eq!(published(&mut a, &c), first);
    assert_eq!(a.state.published, Some(first));
    assert_eq!(a.state.snapshots.len(), 1);
}
#[test]
fn no_anchor_orphan_does_not_set_restart_publication_floor_or_reuse_generation() {
    let mut a = app();
    claim(&mut a);
    progress(&mut a);
    let s = snapshot(&a, 3);
    let old = publish(&mut a, 3, s);
    let mut recovered = restored_claim(&a, 4);
    let c = publication(&recovered, 5, snapshot(&recovered, 1));
    let new = published(&mut recovered, &c);
    assert!(new.generation > old.generation);
    assert_eq!(recovered.state.snapshots.len(), 1);
    assert_eq!(recovered.state.published, Some(new));
    assert_eq!(recovered.state.parent.as_ref().unwrap().snapshot, None);
}
