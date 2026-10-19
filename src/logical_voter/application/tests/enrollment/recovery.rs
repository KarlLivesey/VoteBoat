// SPDX-License-Identifier: RPL-1.5
// Copyright (c) 2026 Karl Livesey
use super::*;

#[test]
fn original_joint_then_final_parent_updates_promote_without_changing_candidate_schema() {
    let mut a = candidate();
    let c = enrollment(import(&a), 11);
    apply(&mut a, &c);
    let policy = Policy::new(
        Tree::Majority(vec![
            Tree::Voter(NodeId::new(1).unwrap()),
            Tree::Voter(NodeId::new(3).unwrap()),
        ]),
        PolicyLimits::default(),
    )
    .unwrap();
    let target = Configuration::new(
        ConfigurationId::new(4).unwrap(),
        policy,
        BTreeMap::from([
            (NodeId::new(1).unwrap(), store(1)),
            (NodeId::new(3).unwrap(), store(3)),
        ]),
        BTreeMap::new(),
    )
    .unwrap();
    let operation = OperationId::new(21).unwrap();
    let joint = LogEntry {
        index: 2,
        term: 1,
        payload: EntryPayload::Configuration(Box::new(ConfigurationRecord {
            operation,
            expected: ConfigurationId::new(2).unwrap(),
            change: ConfigurationChange::Joint {
                id: ConfigurationId::new(3).unwrap(),
                next: target,
            },
        })),
    };
    mutate_parent(
        &mut a,
        12,
        1,
        Some(Suffix {
            from: 2,
            entries: vec![joint],
        }),
        None,
    );
    mutate_parent(&mut a, 13, 2, None, None);
    let final_entry = LogEntry {
        index: 3,
        term: 1,
        payload: EntryPayload::Configuration(Box::new(ConfigurationRecord {
            operation,
            expected: ConfigurationId::new(3).unwrap(),
            change: ConfigurationChange::Final {
                id: ConfigurationId::new(4).unwrap(),
            },
        })),
    };
    mutate_parent(
        &mut a,
        14,
        2,
        Some(Suffix {
            from: 3,
            entries: vec![final_entry],
        }),
        None,
    );
    mutate_parent(&mut a, 15, 3, None, None);
    assert_eq!(
        a.state
            .parent
            .as_ref()
            .unwrap()
            .membership()
            .unwrap()
            .voter_store(NodeId::new(3).unwrap()),
        Some(store(3))
    );
    assert_eq!(a.schema_version(), 6);
    assert_eq!(
        a.initialization(),
        LogicalInitialization::EnrollmentCandidate(fixture::plan())
    );
    let bytes = a.checkpoint(usize::MAX).unwrap();
    let mut restored = candidate();
    restored
        .restore_checkpoint(
            LOGICAL_ENROLLMENT_APPLICATION_SCHEMA,
            a.applied_index(),
            &bytes,
        )
        .unwrap();
    assert_eq!(restored.state, a.state);
}

fn owner_for(a: &LogicalApplication) -> RuntimeOwner {
    RuntimeOwner {
        store: a.binding(a.state.representative.unwrap()),
        ..owner()
    }
}
fn mutate_parent(
    a: &mut LogicalApplication,
    operation: u128,
    commit: u64,
    suffix: Option<Suffix>,
    snapshot: Option<(SnapshotRef, Option<Box<Membership>>)>,
) {
    let parent = a.state.parent.as_ref().unwrap();
    let o = owner_for(a);
    let update = LogUpdate {
        group: group(1),
        expected_revision: parent.revision,
        hard_state: parent.hard_state,
        commit_index: commit,
        suffix,
        snapshot: snapshot.as_ref().map(|s| s.0),
        snapshot_membership: snapshot.and_then(|s| s.1),
    };
    let c = Command {
        operation: OperationId::new(command::session_operation(
            a.state.representative.unwrap().logical_session.get(),
            u64::try_from(operation).unwrap(),
        ))
        .unwrap(),
        origin_configuration: a.state.membership.id(),
        representative: a.state.representative.unwrap(),
        consumed_through: 0,
        body: CommandBody::Mutate {
            expected_generation: parent.generation,
            update,
            origin: PersistOrigin {
                request: WorkerTicket {
                    binding: WorkerBinding {
                        store: o.store,
                        generation: StorageWorkerGeneration::new(1).unwrap(),
                    },
                    sequence: 1,
                },
                visit: VisitTicket {
                    owner: o,
                    group: group(1),
                    sequence: 1,
                },
            },
        },
    };
    assert_eq!(apply(a, &c).outcome, LogicalOutcome::Mutated);
}
fn retire_import_pin(a: &mut LogicalApplication, reference: SnapshotRef) {
    let membership = a
        .state
        .snapshots
        .iter()
        .find(|(r, _)| *r == reference)
        .unwrap()
        .1
        .metadata
        .membership
        .clone();
    mutate_parent(a, 13, 2, None, Some((reference, membership)));
    let o = owner_for(a);
    let reconcile = command(
        14,
        1,
        CommandBody::Reconcile {
            expected_revision: a.state.parent.as_ref().unwrap().revision,
            reference,
            origin: SnapshotOrigin {
                request: SnapshotWorkTicket {
                    binding: SnapshotWorkerBinding {
                        store: o.store,
                        generation: SnapshotWorkerGeneration::new(1).unwrap(),
                    },
                    sequence: 2,
                },
                visit: VisitTicket {
                    owner: o,
                    group: group(1),
                    sequence: 2,
                },
            },
        },
    );
    assert_eq!(
        apply(a, &reconcile).outcome,
        LogicalOutcome::SnapshotReconciled
    );
    assert_eq!(a.state.snapshots.len(), 1);
    assert_eq!(a.state.snapshots[0].0, reference);
    let checkpoint = a.checkpoint(usize::MAX).unwrap();
    let mut restored = candidate();
    restored
        .restore_checkpoint(
            LOGICAL_ENROLLMENT_APPLICATION_SCHEMA,
            a.applied_index(),
            &checkpoint,
        )
        .unwrap();
    assert_eq!(restored.state, a.state);
    assert_eq!(
        restored
            .state
            .enrollment
            .as_ref()
            .unwrap()
            .target_reference
            .generation
            .get(),
        1
    );
}

#[test]
fn candidate_checkpoint_suffix_reclaim_and_original_retry_preserve_import() {
    let mut a = candidate();
    let c = enrollment(import(&a), 11);
    apply(&mut a, &c);
    let checkpoint = a.checkpoint(usize::MAX).unwrap();
    let first = a.state.enrollment.clone();
    let next = command(
        12,
        2,
        CommandBody::Claim {
            expected_epoch: RepresentativeEpoch::new(1),
        },
    );
    let suffix = entry(&a, &next, 2);
    assert_eq!(
        a.apply_batch(std::slice::from_ref(&suffix)).unwrap()[0].outcome,
        LogicalOutcome::Claimed
    );
    let mut restored = candidate();
    restored
        .restore_checkpoint(LOGICAL_ENROLLMENT_APPLICATION_SCHEMA, 1, &checkpoint)
        .unwrap();
    restored.apply_batch(&[suffix]).unwrap();
    assert_eq!(restored.state, a.state);
    assert_eq!(restored.state.enrollment, first);
    assert_eq!(restored.schema_version(), 6);
    assert_eq!(
        restored.state.representative.unwrap().logical_session.get(),
        2
    );
    let retry = apply(&mut restored, &c);
    assert!(retry.duplicate);
    assert_eq!(retry.first_index, 1);
    assert_eq!(
        restored.state.records[&c.operation].command,
        a.state.records[&c.operation].command
    );
    for cut in [0, 7, 8, checkpoint.len() - 1] {
        let before = restored.checkpoint(usize::MAX).unwrap();
        assert!(restored
            .restore_checkpoint(LOGICAL_ENROLLMENT_APPLICATION_SCHEMA, 1, &checkpoint[..cut])
            .is_err());
        assert_eq!(restored.checkpoint(usize::MAX).unwrap(), before);
    }
}
#[test]
fn permanent_import_generation_floor_survives_checkpoint_and_new_publication() {
    let mut a = candidate();
    let c = enrollment(import(&a), 11);
    apply(&mut a, &c);
    let checkpoint = a.checkpoint(usize::MAX).unwrap();
    let mut a = candidate();
    a.restore_checkpoint(LOGICAL_ENROLLMENT_APPLICATION_SCHEMA, 1, &checkpoint)
        .unwrap();
    let mut snapshot = a.state.snapshots[0].1.clone();
    snapshot.application.push(7);
    // A later snapshot's data is opaque here; source runtime validity belongs
    // to the actual SnapshotWorker/driver, not this deterministic storage app.
    snapshot.metadata.index = 2;
    let parent = a.state.parent.as_ref().unwrap();
    let binding = StoreBinding {
        identity: store(3),
        session: StoreSession::new(1).unwrap(),
    };
    let origin = SnapshotOrigin {
        request: SnapshotWorkTicket {
            binding: SnapshotWorkerBinding {
                store: binding,
                generation: SnapshotWorkerGeneration::new(1).unwrap(),
            },
            sequence: 1,
        },
        visit: VisitTicket {
            owner: RuntimeOwner {
                store: binding,
                ..owner()
            },
            group: group(1),
            sequence: 1,
        },
    };
    let publish = command(
        12,
        1,
        CommandBody::Publish {
            expected_revision: parent.revision,
            durable: parent.snapshot,
            snapshot,
            origin,
        },
    );
    let LogicalOutcome::SnapshotPublished(reference) = apply(&mut a, &publish).outcome else {
        panic!("actual candidate publication");
    };
    assert_eq!(reference.generation.get(), 2);
    assert_eq!(
        a.state
            .enrollment
            .as_ref()
            .unwrap()
            .target_reference
            .generation
            .get(),
        1
    );
    assert_eq!(
        a.state
            .enrollment
            .as_ref()
            .unwrap()
            .origin
            .export
            .reference
            .generation
            .get(),
        9
    );
    retire_import_pin(&mut a, reference);
}
