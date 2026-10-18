// SPDX-License-Identifier: RPL-1.5
// Copyright (c) 2026 Karl Livesey
//! A representative ordinary child history: a child configuration change,
//! claim, parent mutations, two snapshot publications with an anchor, a stale
//! representative refusal, a reconcile refusal, an exact retry and a term
//! change. The same code generated the v1 legacy fixture on `origin/main`.
use super::*;
use crate::membership::{Configuration, ConfigurationChange, ConfigurationRecord};
use crate::snapshot_worker::{SnapshotWorkTicket, SnapshotWorkerBinding};
pub(super) fn snapshot_origin() -> SnapshotOrigin {
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
pub(super) fn snapshot(a: &LogicalApplication, index: u64) -> Snapshot {
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
/// A new snapshot at the retained boundary with different application bytes,
/// so publishing it allocates a fresh generation.
pub(super) fn later_snapshot(a: &LogicalApplication) -> Snapshot {
    Snapshot {
        application: vec![42],
        ..snapshot(a, 2)
    }
}
pub(super) fn publish(a: &mut LogicalApplication, id: u128, s: Snapshot) -> LogicalReceipt {
    let receipt = publish_unchecked(a, id, s);
    super::assert_restore_equivalent(a);
    receipt
}
/// Publication without the per-apply LV1 equivalence check, for states that
/// simulate future retirement.
pub(super) fn publish_unchecked(
    a: &mut LogicalApplication,
    id: u128,
    s: Snapshot,
) -> LogicalReceipt {
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
    // Append at the current child term, which the history raises to 2.
    let mut e = entry(a, &c, a.applied_index() + 1);
    e.term = a.state.applied_term.max(1);
    a.apply_batch(&[e]).unwrap().remove(0)
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
fn learners() -> LogEntry {
    let configuration = Configuration::new(
        ConfigurationId::new(2).unwrap(),
        Policy::new(
            Tree::Voter(NodeId::new(2).unwrap()),
            PolicyLimits::default(),
        )
        .unwrap(),
        BTreeMap::from([(NodeId::new(2).unwrap(), store(2))]),
        BTreeMap::from([(NodeId::new(3).unwrap(), store(3))]),
    )
    .unwrap();
    LogEntry {
        index: 1,
        term: 1,
        payload: EntryPayload::Configuration(Box::new(ConfigurationRecord {
            operation: OperationId::new(100).unwrap(),
            expected: ConfigurationId::new(1).unwrap(),
            change: ConfigurationChange::Learners(configuration),
        })),
    }
}
fn noops(from: u64, to: u64) -> Suffix {
    Suffix {
        from,
        entries: (from..=to)
            .map(|index| LogEntry {
                index,
                term: 1,
                payload: EntryPayload::Noop,
            })
            .collect(),
    }
}
/// Ten applied child entries, eight retained records.
pub(super) fn populated() -> LogicalApplication {
    let mut a = app();
    a.apply_batch(&[learners()]).unwrap();
    claim(&mut a);
    let c = mutate(&a, 2, update(&a, 2, Some(noops(1, 2))));
    assert_eq!(apply(&mut a, &c).outcome, LogicalOutcome::Mutated);
    let s = snapshot(&a, 1);
    let LogicalOutcome::SnapshotPublished(first) = publish(&mut a, 3, s).outcome else {
        panic!("first publication");
    };
    anchor(&mut a, 4, first);
    let s = snapshot(&a, 2);
    let LogicalOutcome::SnapshotPublished(second) = publish(&mut a, 5, s).outcome else {
        panic!("second publication");
    };
    let stale = command(
        6,
        2,
        CommandBody::Claim {
            expected_epoch: None,
        },
    );
    assert_eq!(
        apply(&mut a, &stale).outcome,
        LogicalOutcome::Refused(LogicalRefusal::StaleRepresentative)
    );
    let reconcile = command(
        7,
        1,
        CommandBody::Reconcile {
            expected_revision: a.state.parent.as_ref().unwrap().revision,
            reference: second,
            origin: snapshot_origin(),
        },
    );
    assert_eq!(
        apply(&mut a, &reconcile).outcome,
        LogicalOutcome::Refused(LogicalRefusal::SnapshotConflict)
    );
    assert!(apply(&mut a, &c).duplicate);
    let c = mutate(&a, 8, update(&a, 3, Some(noops(3, 3))));
    let mut e = entry(&a, &c, a.applied_index() + 1);
    e.term = 2;
    assert_eq!(
        a.apply_batch(&[e]).unwrap()[0].outcome,
        LogicalOutcome::Mutated
    );
    a
}
