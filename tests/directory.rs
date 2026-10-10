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
mod support;
use support::*;
use voteboat::{
    application::*, directory::*, identity::*, log::*, placement::PlacementRequirements, routing::*,
};
#[path = "directory/creation.rs"]
mod creation;
#[path = "directory/transfer.rs"]
mod transfer;
fn id(value: u128) -> ResponsibilityIdentity {
    ResponsibilityIdentity {
        id: ResponsibilityId::new(value).unwrap(),
        incarnation: ResponsibilityIncarnation::new(1).unwrap(),
    }
}
fn range(start: u16, end: u16) -> BucketRange {
    BucketRange::new(start, end).unwrap()
}
fn child(value: u128, authority: u128) -> RouteTarget {
    RouteTarget::Child(ChildAuthority {
        responsibility: id(value),
        group: group(authority),
        epoch: OwnershipEpoch::new(1).unwrap(),
    })
}
fn input(value: u128, scope: BucketRange, execution: ExecutionMode) -> ManifestInput {
    ManifestInput {
        responsibility: id(value),
        parent: None,
        authority: group(1),
        application: ApplicationAdapter {
            id: ApplicationAdapterId::new(1).unwrap(),
            version: 1,
        },
        scheme: PartitionScheme {
            id: RoutingSchemeId::new(1).unwrap(),
            version: 1,
        },
        scope,
        epoch: OwnershipEpoch::new(1).unwrap(),
        generation: RouteGeneration::new(1).unwrap(),
        placement: PlacementRequirements {
            minimum_voting_domains: 3,
            survive_any_single_domain_loss: true,
        },
        state: ResponsibilityState::Active,
        execution,
    }
}
fn manifests() -> Vec<ResponsibilityManifest> {
    let root = input(
        1,
        range(0, 256),
        ExecutionMode::Delegated(vec![
            RouteEntry {
                scope: range(0, 128),
                target: child(2, 1),
            },
            RouteEntry {
                scope: range(128, 256),
                target: child(3, 3),
            },
        ]),
    );
    let mut orders = input(
        2,
        range(0, 128),
        ExecutionMode::Partitioned(vec![
            RouteEntry {
                scope: range(0, 64),
                target: RouteTarget::Group(group(20)),
            },
            RouteEntry {
                scope: range(64, 128),
                target: RouteTarget::Group(group(21)),
            },
        ]),
    );
    orders.parent = Some(ParentAuthority {
        responsibility: id(1),
        group: group(1),
    });
    vec![
        ResponsibilityManifest::new(root).unwrap(),
        ResponsibilityManifest::new(orders).unwrap(),
    ]
}
fn plan() -> DirectoryPlan {
    DirectoryPlan::new(group(1), manifests()).unwrap()
}
fn fresh_directory() -> Directory {
    Directory::new(
        plan(),
        DirectoryLimits {
            operations: 20,
            history_bytes: 64 * 1024,
        },
    )
    .unwrap()
}
fn initialize(mut app: Directory) -> Directory {
    let bytes = app.bootstrap_command(MAX_DIRECTORY_COMMAND_BYTES).unwrap();
    let receipt = app
        .apply_batch(&[LogEntry {
            index: 1,
            term: 1,
            payload: EntryPayload::Command {
                operation: OperationId::new(1_000_000).unwrap(),
                bytes,
            },
        }])
        .unwrap()[0];
    assert_eq!(receipt.outcome, DirectoryOutcome::Initialized);
    app
}
fn directory() -> Directory {
    initialize(fresh_directory())
}
fn bootstrap_len() -> usize {
    fresh_directory()
        .bootstrap_command(MAX_DIRECTORY_COMMAND_BYTES)
        .unwrap()
        .len()
}
fn command(manifest: ResponsibilityManifest, expected: Option<u64>) -> Vec<u8> {
    DirectoryCommand {
        expected: expected.map(|g| RouteGeneration::new(g).unwrap()),
        manifest,
    }
    .encode(MAX_DIRECTORY_COMMAND_BYTES)
    .unwrap()
}
fn entry(index: u64, operation: u128, bytes: Vec<u8>) -> LogEntry {
    LogEntry {
        index: index + 1,
        term: 1,
        payload: EntryPayload::Command {
            operation: OperationId::new(operation).unwrap(),
            bytes,
        },
    }
}
fn publish(
    app: &mut Directory,
    index: u64,
    op: u128,
    manifest: ResponsibilityManifest,
    expected: Option<u64>,
) -> DirectoryReceipt {
    app.apply_batch(&[entry(index, op, command(manifest, expected))])
        .unwrap()[0]
}
fn update_manifest(manifest: &ResponsibilityManifest, generation: u64) -> ResponsibilityManifest {
    let mut v = manifest.clone().into_input();
    v.generation = RouteGeneration::new(generation).unwrap();
    ResponsibilityManifest::new(v).unwrap()
}

#[test]
fn directory_publishes_committed_order_retries_original_outcomes_and_rejects_transfers() {
    let mut app = directory();
    let values = manifests();
    assert_eq!(
        publish(&mut app, 1, 1, values[1].clone(), None).outcome,
        DirectoryOutcome::GenerationMismatch
    );
    assert!(app.manifest(id(2)).is_none());
    assert_eq!(
        publish(&mut app, 2, 2, values[0].clone(), None).outcome,
        DirectoryOutcome::Published(RouteGeneration::new(1).unwrap())
    );
    // The failed operation keeps its original outcome even after the parent exists.
    let retry = publish(&mut app, 3, 1, values[1].clone(), None);
    assert!(retry.duplicate);
    assert_eq!(retry.outcome, DirectoryOutcome::GenerationMismatch);
    assert_eq!(
        publish(&mut app, 4, 3, values[1].clone(), None).outcome,
        DirectoryOutcome::Published(RouteGeneration::new(1).unwrap())
    );
    let mut changed = values[0].clone().into_input();
    changed.generation = RouteGeneration::new(2).unwrap();
    changed.placement.minimum_voting_domains = 2;
    let next = ResponsibilityManifest::new(changed).unwrap();
    assert_eq!(
        publish(&mut app, 5, 4, next.clone(), Some(1)).outcome,
        DirectoryOutcome::Published(RouteGeneration::new(2).unwrap())
    );
    assert_eq!(
        publish(&mut app, 6, 4, values[0].clone(), None).outcome,
        DirectoryOutcome::OperationConflict
    );
    for field in 0..7 {
        let mut changed = update_manifest(&next, 3).into_input();
        match field {
            0 => changed.epoch = OwnershipEpoch::new(2).unwrap(),
            1 => changed.execution = ExecutionMode::Single(group(99)),
            2 => {
                changed.parent = Some(ParentAuthority {
                    responsibility: id(88),
                    group: group(88),
                })
            }
            3 => changed.scope = range(0, 255),
            4 => changed.state = ResponsibilityState::Fenced,
            5 => changed.application.version = 2,
            _ => changed.authority = group(99),
        }
        if field == 3 {
            changed.execution = ExecutionMode::Single(group(99));
        }
        let attempted = ResponsibilityManifest::new(changed).unwrap();
        assert_eq!(
            publish(&mut app, 7 + field, 10 + field as u128, attempted, Some(2)).outcome,
            DirectoryOutcome::OwnershipChange
        );
        assert_eq!(app.manifest(id(1)), Some(&next));
    }
    let unknown =
        ResponsibilityManifest::new(input(9, range(0, 256), ExecutionMode::Single(group(9))))
            .unwrap();
    assert_eq!(
        publish(&mut app, 14, 19, unknown, None).outcome,
        DirectoryOutcome::UnknownResponsibility
    );
}

#[test]
fn directory_command_codec_is_bounded_canonical_and_rejects_every_truncation() {
    for value in manifests() {
        let bytes = command(value.clone(), None);
        assert_eq!(
            DirectoryCommand::decode(&bytes).unwrap(),
            DirectoryCommand {
                expected: None,
                manifest: value
            }
        );
        for end in 0..bytes.len() {
            assert!(
                DirectoryCommand::decode(&bytes[..end]).is_err(),
                "truncation {end}"
            );
        }
        let decoded = DirectoryCommand::decode(&bytes).unwrap();
        assert!(decoded.encode(bytes.len() - 1).is_err());
        assert_eq!(decoded.encode(bytes.len()).unwrap(), bytes);
        let mut trailing = bytes.clone();
        trailing.push(0);
        assert!(DirectoryCommand::decode(&trailing).is_err());
        let mut length = bytes.clone();
        length[16..20].copy_from_slice(&u32::MAX.to_le_bytes());
        assert!(DirectoryCommand::decode(&length).is_err());
        // Every accepted byte mutation must have one canonical encoding and a
        // checked manifest; this is a finite decoder exercise, not fuzz proof.
        for index in 0..bytes.len() {
            let mut mutated = bytes.clone();
            mutated[index] ^= 0xff;
            if let Ok(command) = DirectoryCommand::decode(&mutated) {
                assert_eq!(
                    command.encode(MAX_DIRECTORY_COMMAND_BYTES).unwrap(),
                    mutated
                );
            }
        }
    }
    assert!(DirectoryCommand::decode(&vec![0; MAX_DIRECTORY_COMMAND_BYTES + 1]).is_err());
}

#[test]
fn directory_plan_checks_local_ancestry_grants_and_returns_original_inputs() {
    for field in 0..4 {
        let mut values = manifests();
        match field {
            0 => {
                let mut v = values[1].clone().into_input();
                v.authority = group(8);
                values[1] = ResponsibilityManifest::new(v).unwrap();
            }
            1 => {
                values.remove(1);
            }
            2 => {
                let mut v = values[1].clone().into_input();
                v.parent = None;
                values[1] = ResponsibilityManifest::new(v).unwrap();
            }
            _ => {
                let mut v = values[1].clone().into_input();
                v.scheme.version = 2;
                values[1] = ResponsibilityManifest::new(v).unwrap();
            }
        }
        let ptr = values.as_ptr();
        let cap = values.capacity();
        let (_, returned) = DirectoryPlan::new(group(1), values).unwrap_err();
        assert_eq!(returned.as_ptr(), ptr);
        assert_eq!(returned.capacity(), cap);
    }
    let mut a = input(
        1,
        range(0, 256),
        ExecutionMode::Delegated(vec![RouteEntry {
            scope: range(0, 256),
            target: child(2, 1),
        }]),
    );
    let mut b = input(
        2,
        range(0, 256),
        ExecutionMode::Delegated(vec![RouteEntry {
            scope: range(0, 256),
            target: child(1, 1),
        }]),
    );
    a.parent = Some(ParentAuthority {
        responsibility: id(2),
        group: group(1),
    });
    b.parent = Some(ParentAuthority {
        responsibility: id(1),
        group: group(1),
    });
    assert_eq!(
        DirectoryPlan::new(
            group(1),
            vec![
                ResponsibilityManifest::new(a).unwrap(),
                ResponsibilityManifest::new(b).unwrap()
            ]
        )
        .unwrap_err()
        .0,
        RoutingError::Cycle
    );
}

#[test]
fn application_batch_failure_is_atomic_and_pending_admission_reserves_history() {
    let values = manifests();
    let bytes = command(values[0].clone(), None);
    let mut app = initialize(
        Directory::new(
            plan(),
            DirectoryLimits {
                operations: 2,
                history_bytes: bootstrap_len() + bytes.len(),
            },
        )
        .unwrap(),
    );
    assert!(app
        .validate_proposal(
            OperationId::new(1).unwrap(),
            &bytes,
            [(OperationId::new(1).unwrap(), bytes.as_slice())].into_iter()
        )
        .is_ok());
    assert_eq!(
        app.validate_proposal(
            OperationId::new(2).unwrap(),
            &bytes,
            [(OperationId::new(1).unwrap(), bytes.as_slice())].into_iter()
        ),
        Err(ApplicationError::DedupCapacity)
    );
    let before = app.checkpoint(100000).unwrap();
    assert_eq!(
        app.apply_batch(&[entry(1, 1, bytes.clone()), entry(2, 2, bytes.clone())]),
        Err(ApplicationError::DedupCapacity)
    );
    assert_eq!(app.checkpoint(100000).unwrap(), before);
    assert_eq!(
        app.apply_batch(&[entry(2, 1, bytes.clone())]),
        Err(ApplicationError::IndexGap)
    );
    app.apply_batch(&[entry(1, 1, bytes.clone())]).unwrap();
    assert!(app
        .validate_proposal(OperationId::new(1).unwrap(), &bytes, std::iter::empty())
        .is_ok());
    assert!(app
        .validate_proposal(OperationId::new(2).unwrap(), &bytes, std::iter::empty())
        .is_err());
    let mut tiny = initialize(
        Directory::new(
            plan(),
            DirectoryLimits {
                operations: 20,
                history_bytes: bootstrap_len() + bytes.len() - 1,
            },
        )
        .unwrap(),
    );
    assert_eq!(
        tiny.apply_batch(&[entry(1, 1, bytes.clone())]),
        Err(ApplicationError::DedupCapacity)
    );
    assert_eq!(tiny.applied_index(), 1);
    let mut app = directory();
    let before = app.checkpoint(100000).unwrap();
    assert_eq!(
        app.apply_batch(&[entry(1, 1, bytes), entry(2, 2, vec![0])]),
        Err(ApplicationError::InvalidCommand)
    );
    assert_eq!(app.checkpoint(100000).unwrap(), before);
}

#[test]
fn checkpoint_reconstructs_publications_rejections_and_retry_history_atomically() {
    let mut app = directory();
    let values = manifests();
    publish(&mut app, 1, 1, values[0].clone(), None);
    publish(&mut app, 2, 2, values[1].clone(), None);
    let next = update_manifest(&values[0], 2);
    publish(&mut app, 3, 3, next.clone(), Some(9)); // rejected CAS is durable retry state
    publish(&mut app, 4, 4, next.clone(), Some(1));
    publish(&mut app, 5, 1, values[0].clone(), None);
    app.apply_batch(&[LogEntry {
        index: 7,
        term: 1,
        payload: EntryPayload::Noop,
    }])
    .unwrap();
    let bytes = app.checkpoint(100000).unwrap();
    assert_eq!(app.checkpoint(bytes.len()).unwrap(), bytes);
    assert!(app.checkpoint(bytes.len() - 1).is_err());
    let mut restored = directory();
    restored.restore_checkpoint(1, 7, &bytes).unwrap();
    assert_eq!(restored.checkpoint(bytes.len()).unwrap(), bytes);
    assert_eq!(restored.manifest(id(1)), Some(&next));
    let retry = publish(&mut restored, 7, 3, next.clone(), Some(9));
    assert!(retry.duplicate);
    assert_eq!(retry.outcome, DirectoryOutcome::GenerationMismatch);
    assert!(publish(&mut restored, 8, 1, values[0].clone(), None).duplicate);
    let stable = restored.checkpoint(100000).unwrap();
    for end in 0..bytes.len() {
        assert_eq!(
            restored.restore_checkpoint(1, 7, &bytes[..end]),
            Err(ApplicationError::InvalidCheckpoint)
        );
        assert_eq!(restored.checkpoint(100000).unwrap(), stable);
    }
    assert_eq!(
        restored.restore_checkpoint(2, 7, &bytes),
        Err(ApplicationError::UnsupportedSchema)
    );
    assert!(restored.restore_checkpoint(1, 5, &bytes).is_err());
    let mut mismatch = Directory::new(
        plan(),
        DirectoryLimits {
            operations: 19,
            history_bytes: 64 * 1024,
        },
    )
    .unwrap();
    assert!(mismatch.restore_checkpoint(1, 7, &bytes).is_err());
    let mut changed = manifests();
    let mut v = changed[0].clone().into_input();
    v.placement.minimum_voting_domains = 2;
    changed[0] = ResponsibilityManifest::new(v).unwrap();
    let mut other =
        Directory::new(DirectoryPlan::new(group(1), changed).unwrap(), app.limits()).unwrap();
    assert!(other.restore_checkpoint(1, 7, &bytes).is_err());
    assert_eq!(
        app.readiness_requirements(),
        directory().readiness_requirements()
    );
    assert!(bytes.len() <= app.readiness_requirements().snapshot_bytes);
}

#[cfg(feature = "native")]
mod native {
    use super::*;
    use std::collections::{BTreeMap, BTreeSet, VecDeque};
    use std::path::Path;
    use voteboat::{
        native::{log_store::*, routing::*, snapshot_store::*},
        raft::*,
        snapshot::*,
    };
    struct Replica {
        core: Raft,
        log: NativeLogStore<FileLogIo>,
        snapshots: NativeSnapshotStore<FileSnapshotIo>,
        app: Directory,
        receipts: Vec<DirectoryReceipt>,
        read: Option<ResponsibilityManifest>,
        lifecycle_operation: Option<OperationId>,
        lifecycle_read: Option<voteboat::transfer::TransferIntentStatus>,
    }
    struct Cluster {
        replicas: BTreeMap<NodeId, Replica>,
        messages: VecDeque<Message>,
        blocked: BTreeSet<NodeId>,
    }
    impl Cluster {
        fn open(root: &Path, recover: bool) -> Self {
            Self::open_plan(root, recover, plan())
        }
        fn open_plan(root: &Path, recover: bool, plan: DirectoryPlan) -> Self {
            Self::open_plan_mode(root, recover, plan, false)
        }
        fn open_plan_mode(root: &Path, recover: bool, plan: DirectoryPlan, creation: bool) -> Self {
            let mut replicas = BTreeMap::new();
            for n in 1..=3 {
                let path = root.join(n.to_string());
                let snapshot_path = path.join("snapshots");
                let log = if recover {
                    NativeLogStore::recover(
                        FileLogIo::open(&path).unwrap(),
                        identity(n.into()),
                        LogLimits::default(),
                    )
                    .unwrap()
                } else {
                    let mut log = NativeLogStore::create(
                        FileLogIo::create(&path).unwrap(),
                        identity(n.into()),
                        LogLimits::default(),
                    )
                    .unwrap();
                    append(&mut log, vec![LogMutation::Create(bootstrap(1, 3))]);
                    log
                };
                let snapshot_identity = SnapshotIdentity {
                    store: identity(n.into()),
                    group: group(1),
                };
                let mut snapshots = if recover {
                    NativeSnapshotStore::recover(
                        FileSnapshotIo::open(&snapshot_path).unwrap(),
                        snapshot_identity,
                        SnapshotLimits::default(),
                    )
                    .unwrap()
                } else {
                    NativeSnapshotStore::create(
                        FileSnapshotIo::create(&snapshot_path).unwrap(),
                        snapshot_identity,
                        SnapshotLimits::default(),
                    )
                    .unwrap()
                };
                let mut app = Directory::new(
                    plan.clone(),
                    DirectoryLimits {
                        operations: 20,
                        history_bytes: 64 * 1024,
                    },
                )
                .unwrap();
                if creation {
                    app = app
                        .with_group_creation()
                        .unwrap_or_else(|_| panic!("fresh mode"));
                }
                let (core, _) =
                    recover_replica(node(n), group(1), &log, &mut snapshots, &mut app).unwrap();
                replicas.insert(
                    node(n),
                    Replica {
                        core,
                        log,
                        snapshots,
                        app,
                        receipts: vec![],
                        read: None,
                        lifecycle_operation: None,
                        lifecycle_read: None,
                    },
                );
            }
            Self {
                replicas,
                messages: VecDeque::new(),
                blocked: BTreeSet::new(),
            }
        }
        fn act(&mut self, n: u64, event: Event) {
            let effects = self
                .replicas
                .get_mut(&node(n))
                .unwrap()
                .core
                .step(event)
                .unwrap();
            self.effects(node(n), effects);
        }
        fn effects(&mut self, n: NodeId, effects: Vec<Effect>) {
            let mut pending = VecDeque::from(effects);
            while let Some(effect) = pending.pop_front() {
                match effect {
                    Effect::Persist(update) => {
                        let r = self.replicas.get_mut(&n).unwrap();
                        pending.extend(persist_effect(&mut r.core, &mut r.log, update).unwrap());
                    }
                    Effect::Committed(entries) => {
                        let r = self.replicas.get_mut(&n).unwrap();
                        r.receipts.extend(r.app.apply_batch(&entries).unwrap());
                    }
                    Effect::Send(message) => {
                        assert!(self.messages.len() < 1024);
                        self.messages.push_back(message);
                    }
                    Effect::ReadReady(barrier) => {
                        let r = self.replicas.get_mut(&n).unwrap();
                        if let Some(operation) = r.lifecycle_operation {
                            use voteboat::transfer::*;
                            let view = LifecycleDirectory::new(r.app.clone());
                            let DirectoryRead::Transfer(status) = read_at_barrier(
                                &mut r.core,
                                &barrier,
                                &view,
                                DirectoryQuery::Transfer(operation),
                            )
                            .unwrap() else {
                                panic!("wrong query result")
                            };
                            r.lifecycle_read = status;
                        } else {
                            r.read = read_at_barrier(&mut r.core, &barrier, &r.app, id(1)).unwrap();
                        }
                    }
                    Effect::SnapshotRequired {
                        to,
                        context,
                        reference,
                    } => {
                        let r = self.replicas.get_mut(&n).unwrap();
                        pending.extend(
                            supply_snapshot(&r.core, &mut r.snapshots, to, context, reference)
                                .unwrap(),
                        );
                    }
                    Effect::StageSnapshot(message) => {
                        let r = self.replicas.get_mut(&n).unwrap();
                        pending.extend(
                            stage_snapshot_effect(
                                &mut r.core,
                                &mut r.log,
                                &mut r.snapshots,
                                &r.app,
                                message,
                            )
                            .unwrap(),
                        );
                    }
                    Effect::SnapshotInstalled(reference) => {
                        let r = self.replicas.get_mut(&n).unwrap();
                        pending.extend(
                            finish_snapshot_install(
                                &mut r.core,
                                &r.log,
                                &mut r.snapshots,
                                &mut r.app,
                                reference,
                            )
                            .unwrap(),
                        );
                    }
                    other => panic!("unexpected effect {other:?}"),
                }
            }
        }
        fn pump(&mut self) {
            let mut count = 0;
            while let Some(message) = self.messages.pop_front() {
                count += 1;
                assert!(count < 10000);
                if !self.blocked.contains(&message.from) && !self.blocked.contains(&message.to) {
                    let to = message.to;
                    self.act(to.get(), Event::Receive(message));
                }
            }
        }
        fn propose(
            &mut self,
            n: u64,
            op: u128,
            manifest: ResponsibilityManifest,
            expected: Option<u64>,
        ) -> DirectoryReceipt {
            let bytes = command(manifest, expected);
            self.propose_bytes(n, op, bytes)
        }
        fn propose_bytes(&mut self, n: u64, op: u128, bytes: Vec<u8>) -> DirectoryReceipt {
            let operation = OperationId::new(op).unwrap();
            self.replicas[&node(n)]
                .app
                .validate_proposal(operation, &bytes, std::iter::empty())
                .unwrap();
            let previous = self.replicas[&node(n)].receipts.len();
            self.act(n, Event::Propose { operation, bytes });
            self.pump();
            let receipt = self.replicas[&node(n)].receipts[previous..]
                .iter()
                .find(|receipt| receipt.operation == operation)
                .unwrap();
            *receipt
        }
        fn checkpoint(&mut self, n: u64) {
            let r = self.replicas.get_mut(&node(n)).unwrap();
            let receipt = checkpoint_application(&r.core, &r.app, &mut r.snapshots).unwrap();
            let effects = compact_replica(
                &mut r.core,
                &mut r.log,
                &mut r.snapshots,
                &r.app,
                receipt.reference(),
            )
            .unwrap();
            self.effects(node(n), effects);
            self.pump();
        }
    }
    fn provision_recovered_group(
        root: &Path,
        cluster: &Cluster,
        committed: voteboat::directory::GroupCreationStatus,
        intent: voteboat::directory::GroupCreationIntent,
    ) {
        use voteboat::{group_creation::*, native::group_creation::*};
        let authority = LocalCreationAuthority {
            core: &cluster.replicas[&node(3)].core,
            directory: &cluster.replicas[&node(3)].app,
        };
        let mut forged = committed.clone();
        forged.operation = OperationId::new(11).unwrap();
        assert!(VerifiedGroupCreation::verify(
            &authority,
            forged,
            node(1),
            identity(1),
            intent.application,
        )
        .is_err());
        let verified = VerifiedGroupCreation::verify(
            &authority,
            committed.clone(),
            node(1),
            identity(1),
            intent.application,
        )
        .unwrap();
        let target = root.join("created-1");
        let mut log = NativeLogStore::create(
            FileLogIo::create(&target).unwrap(),
            identity(1),
            LogLimits::default(),
        )
        .unwrap();
        let mut bindings = FileCreationBindings::open(&target).unwrap();
        let receipt = establish_created_group(&verified, &mut log, &mut bindings).unwrap();
        assert_eq!(receipt.metadata_index(), committed.index);
        let old_binding = receipt.binding();
        drop(bindings);
        drop(log);
        let mut log = NativeLogStore::recover(
            FileLogIo::open(&target).unwrap(),
            identity(1),
            LogLimits::default(),
        )
        .unwrap();
        let mut bindings = FileCreationBindings::open(&target).unwrap();
        let receipt = establish_created_group(&verified, &mut log, &mut bindings).unwrap();
        assert_ne!(receipt.binding(), old_binding);
        assert_eq!(receipt.operation(), committed.operation);
        assert_eq!(log.state(group(100)).unwrap().bootstrap, intent.bootstrap);
        let mut core = Raft::recover(
            node(1),
            log.binding(),
            log.state(group(100)).unwrap(),
            log.limits(),
        )
        .unwrap();
        let effects = core.step(Event::Campaign).unwrap();
        let [Effect::Persist(update)] = effects.as_slice() else {
            panic!("created group must persist ballot before voting");
        };
        let effects = persist_effect(&mut core, &mut log, update.clone()).unwrap();
        assert!(effects.iter().any(|e| matches!(e,
            Effect::Send(message) if matches!(message.rpc, Rpc::Vote { .. }))));
        drop(bindings);
        drop(log);
    }
    #[test]
    fn native_creation_intent_survives_lost_receipt_snapshot_catchup_and_file_reopen() {
        use voteboat::group_creation::*;
        let root = std::env::temp_dir().join(format!(
            "voteboat-creation-{}-{}",
            std::process::id(),
            std::time::SystemTime::now()
                .duration_since(std::time::UNIX_EPOCH)
                .unwrap()
                .as_nanos()
        ));
        std::fs::create_dir(&root).unwrap();
        let mut cluster = Cluster::open_plan_mode(&root, false, plan(), true);
        cluster.act(1, Event::Campaign);
        cluster.pump();
        let bytes = cluster.replicas[&node(1)]
            .app
            .bootstrap_command(MAX_DIRECTORY_COMMAND_BYTES)
            .unwrap();
        cluster.propose_bytes(1, 1_000_000, bytes);
        cluster.propose(1, 1, manifests()[0].clone(), None);
        cluster.blocked.insert(node(2));
        let intent = super::creation::intent();
        let bytes = intent.encode(MAX_GROUP_CREATION_BYTES).unwrap();
        assert_eq!(
            cluster.propose_bytes(1, 10, bytes.clone()).outcome,
            DirectoryOutcome::CreationReserved
        );
        let status = cluster.replicas[&node(1)]
            .app
            .group_creation_at(0, group(100))
            .unwrap();
        assert!(cluster.replicas[&node(2)]
            .app
            .group_creation_at(0, group(100))
            .unwrap()
            .is_none());
        let committed = status.clone().unwrap();
        let lagging = &cluster.replicas[&node(2)];
        assert!(VerifiedGroupCreation::verify(
            &LocalCreationAuthority {
                core: &lagging.core,
                directory: &lagging.app
            },
            committed.clone(),
            node(1),
            identity(1),
            intent.application,
        )
        .is_err());
        // Discard original receipt; preserve only native committed files/images.
        for n in [1, 3] {
            cluster.checkpoint(n);
        }
        drop(cluster);
        let mut cluster = Cluster::open_plan_mode(&root, true, plan(), true);
        cluster.act(3, Event::Campaign);
        cluster.pump();
        let retry = cluster.propose_bytes(3, 10, bytes);
        assert!(retry.duplicate);
        assert_eq!(retry.outcome, DirectoryOutcome::CreationReserved);
        let mut conflict = intent.clone();
        conflict.mode = GroupCreationMode::Staging;
        assert_eq!(
            cluster
                .propose_bytes(3, 10, conflict.encode(MAX_GROUP_CREATION_BYTES).unwrap())
                .outcome,
            DirectoryOutcome::OperationConflict
        );
        cluster.act(3, Event::Heartbeat);
        cluster.pump();
        for r in cluster.replicas.values() {
            assert_eq!(
                r.app
                    .group_creation_at(r.core.state().commit_index, group(100))
                    .unwrap(),
                status
            );
            assert!(r.app.manifest(id(50)).is_none());
            assert_eq!(r.app.manifest(id(1)), Some(&manifests()[0]));
        }
        provision_recovered_group(&root, &cluster, committed, intent);
        drop(cluster);
        std::fs::remove_dir_all(root).unwrap();
    }

    #[test]
    fn native_transfer_intent_survives_lost_observation_snapshot_catchup_and_reopen() {
        use voteboat::transfer::*;
        let root = std::env::temp_dir().join(format!(
            "voteboat-intent-{}-{}",
            std::process::id(),
            std::time::SystemTime::now()
                .duration_since(std::time::UNIX_EPOCH)
                .unwrap()
                .as_nanos()
        ));
        std::fs::create_dir(&root).unwrap();
        let intent = super::transfer::intent();
        let plan = DirectoryPlan::new(group(1), vec![intent.before().clone()]).unwrap();
        let mut cluster = Cluster::open_plan(&root, false, plan.clone());
        cluster.act(1, Event::Campaign);
        cluster.pump();
        let bytes = cluster.replicas[&node(1)]
            .app
            .bootstrap_command(MAX_DIRECTORY_COMMAND_BYTES)
            .unwrap();
        cluster.propose_bytes(1, 1_000_000, bytes);
        cluster.propose(1, 101, intent.before().clone(), None);
        cluster.blocked.insert(node(2));
        let bytes = intent.encode(MAX_TRANSFER_INTENT_BYTES).unwrap();
        // Ignore the original success observation, then discard volatile state.
        cluster.propose_bytes(1, 200, bytes.clone());
        let status = cluster.replicas[&node(1)]
            .app
            .transfer_intent_at(0, OperationId::new(200).unwrap())
            .unwrap()
            .unwrap();
        assert!(cluster.replicas[&node(2)]
            .app
            .transfer_intent_at(0, status.operation)
            .unwrap()
            .is_none());
        for n in [1, 3] {
            cluster.checkpoint(n);
        }
        drop(cluster);
        let mut cluster = Cluster::open_plan(&root, true, plan.clone());
        cluster.act(3, Event::Campaign);
        cluster.pump();
        let retry = cluster.propose_bytes(3, 200, bytes.clone());
        assert!(retry.duplicate);
        assert_eq!(retry.outcome, DirectoryOutcome::TransferIntentRecorded);
        assert_eq!(
            cluster.propose_bytes(3, 201, bytes).outcome,
            DirectoryOutcome::LifecycleBusy
        );
        cluster
            .replicas
            .get_mut(&node(3))
            .unwrap()
            .lifecycle_operation = Some(status.operation);
        cluster.act(
            3,
            Event::Read {
                request: ReadRequestId::new(91).unwrap(),
            },
        );
        cluster.pump();
        assert_eq!(
            cluster.replicas[&node(3)].lifecycle_read,
            Some(status.clone())
        );
        for r in cluster.replicas.values() {
            assert_eq!(
                r.app.transfer_intent_at(0, status.operation).unwrap(),
                Some(status.clone())
            );
            assert_eq!(
                r.app.manifest(intent.before().input().responsibility),
                Some(intent.before())
            );
        }
        assert!(cluster.replicas[&node(2)].core.state().snapshot.is_some());
        drop(cluster);
        let cluster = Cluster::open_plan(&root, true, plan);
        for r in cluster.replicas.values() {
            assert_eq!(
                r.app.transfer_intent_at(0, status.operation).unwrap(),
                Some(status.clone())
            );
        }
        drop(cluster);
        std::fs::remove_dir_all(root).unwrap();
    }
    fn verify_changed_recovery_envelopes(root: &Path) {
        for change in 0..3 {
            let log = NativeLogStore::recover(
                FileLogIo::open(root.join("1")).unwrap(),
                identity(1),
                LogLimits::default(),
            )
            .unwrap();
            let mut snapshots = NativeSnapshotStore::recover(
                FileSnapshotIo::open(root.join("1/snapshots")).unwrap(),
                SnapshotIdentity {
                    store: identity(1),
                    group: group(1),
                },
                SnapshotLimits::default(),
            )
            .unwrap();
            let mut changed = manifests();
            let mut limits = fresh_directory().limits();
            match change {
                0 => {
                    let mut value = changed[1].clone().into_input();
                    value.placement.minimum_voting_domains = 2;
                    changed[1] = ResponsibilityManifest::new(value).unwrap();
                }
                1 => limits.operations -= 1,
                _ => changed.push(
                    ResponsibilityManifest::new(input(
                        9,
                        range(0, 256),
                        ExecutionMode::Single(group(90)),
                    ))
                    .unwrap(),
                ),
            }
            let mut wrong =
                Directory::new(DirectoryPlan::new(group(1), changed).unwrap(), limits).unwrap();
            assert!(recover_replica(node(1), group(1), &log, &mut snapshots, &mut wrong).is_err());
            assert!(!wrong.is_initialized());
            assert!(wrong.manifest(id(1)).is_none());
        }
    }
    fn replace_isolated_leader(
        cluster: &mut Cluster,
        values: &[ResponsibilityManifest],
    ) -> ResponsibilityManifest {
        cluster.blocked.insert(node(2));
        let old_applied = cluster.replicas[&node(2)].app.applied_index();
        let old_receipts = cluster.replicas[&node(2)].receipts.len();
        let abandoned = update_manifest(&values[0], 2);
        cluster.act(
            2,
            Event::Propose {
                operation: OperationId::new(104).unwrap(),
                bytes: command(abandoned, Some(1)),
            },
        );
        cluster.pump();
        assert_eq!(cluster.replicas[&node(2)].app.applied_index(), old_applied);
        assert_eq!(cluster.replicas[&node(2)].receipts.len(), old_receipts);
        assert!(cluster.replicas[&node(2)].core.state().last_index() > old_applied);
        cluster.act(3, Event::Campaign);
        cluster.pump();
        let mut next = update_manifest(&values[0], 2).into_input();
        next.placement.minimum_voting_domains = 2;
        let next = ResponsibilityManifest::new(next).unwrap();
        assert_eq!(
            cluster.propose(3, 103, next.clone(), Some(1)).outcome,
            DirectoryOutcome::Published(RouteGeneration::new(2).unwrap())
        );
        assert_eq!(
            cluster.replicas[&node(2)].app.manifest(id(1)),
            Some(&values[0])
        );
        cluster.checkpoint(1);
        cluster.checkpoint(3);
        for n in [1, 3] {
            assert!(cluster.replicas[&node(n)].core.state().snapshot.is_some());
        }
        next
    }
    #[test]
    fn native_three_replica_directory_commits_replays_compacts_and_installs_retry_state() {
        let root = std::env::temp_dir().join(format!("voteboat-directory-{}", std::process::id()));
        if root.exists() {
            std::fs::remove_dir_all(&root).unwrap();
        }
        std::fs::create_dir(&root).unwrap();
        let values = manifests();
        let mut cluster = Cluster::open(&root, false);
        cluster.act(1, Event::Campaign);
        cluster.pump();
        let bytes = cluster.replicas[&node(1)]
            .app
            .bootstrap_command(MAX_DIRECTORY_COMMAND_BYTES)
            .unwrap();
        cluster.act(
            1,
            Event::Propose {
                operation: OperationId::new(1_000_000).unwrap(),
                bytes,
            },
        );
        cluster.pump();
        assert!(cluster.replicas.values().all(|r| r.app.is_initialized()));
        assert_eq!(
            cluster.propose(1, 101, values[0].clone(), None).outcome,
            DirectoryOutcome::Published(RouteGeneration::new(1).unwrap())
        );
        cluster.propose(1, 102, values[1].clone(), None);
        for r in cluster.replicas.values() {
            assert_eq!(r.app.manifest(id(1)), Some(&values[0]));
            assert_eq!(r.app.manifest(id(2)), Some(&values[1]));
        }
        // Lose all volatile results before any application checkpoint. WAL replay
        // must reconstruct both the published view and the original retry result.
        drop(cluster);
        verify_changed_recovery_envelopes(&root);
        let mut cluster = Cluster::open(&root, true);
        cluster.act(2, Event::Campaign);
        cluster.pump();
        let receipt = cluster.propose(2, 101, values[0].clone(), None);
        assert!(receipt.duplicate);
        assert_eq!(
            receipt.outcome,
            DirectoryOutcome::Published(RouteGeneration::new(1).unwrap())
        );
        let next = replace_isolated_leader(&mut cluster, &values);
        drop(cluster);
        // The older replica has no new directory checkpoint. Actual snapshot
        // transfer must restore the exact plan and retry history before its ack.
        let mut cluster = Cluster::open(&root, true);
        cluster.act(3, Event::Campaign);
        cluster.pump();
        let receipt = cluster.propose(3, 101, values[0].clone(), None);
        assert!(receipt.duplicate);
        assert_eq!(
            receipt.outcome,
            DirectoryOutcome::Published(RouteGeneration::new(1).unwrap())
        );
        cluster.act(
            3,
            Event::Read {
                request: ReadRequestId::new(1).unwrap(),
            },
        );
        cluster.pump();
        assert_eq!(cluster.replicas[&node(3)].read, Some(next.clone()));
        for r in cluster.replicas.values() {
            assert_eq!(r.app.manifest(id(1)), Some(&next));
            assert_eq!(r.app.manifest(id(2)), Some(&values[1]));
            assert_eq!(r.app.remaining_operations(), 16);
            assert_eq!(r.app.applied_index(), r.core.state().commit_index);
        }
        assert!(cluster.replicas[&node(2)].core.state().snapshot.is_some());
        // Only committed/applied directory views feed the cache; the codec/cache
        // itself never bootstraps a group or grants authority to execute.
        let app = &cluster.replicas[&node(3)].app;
        let mut cache = NativeManifestCache::new(ManifestCacheLimits {
            manifests: 2,
            bytes: 1024 * 1024,
        })
        .unwrap();
        for id in [id(1), id(2)] {
            cache.admit(app.manifest(id).unwrap().clone()).unwrap();
        }
        assert_eq!(
            resolve(&cache, &NativeBytePartition, id(1), &[42], 2)
                .unwrap()
                .group,
            group(20)
        );
        drop(cluster);
        let cluster = Cluster::open(&root, true);
        for r in cluster.replicas.values() {
            assert_eq!(r.app.manifest(id(1)), Some(&next));
            assert_eq!(r.app.remaining_operations(), 16);
        }
        drop(cluster);
        std::fs::remove_dir_all(root).unwrap();
    }
}

#[test]
fn checkpoint_rejects_bad_history_order_identity_and_lengths_without_partial_restore() {
    let mut app = directory();
    let values = manifests();
    publish(&mut app, 1, 1, values[0].clone(), None);
    publish(&mut app, 2, 2, values[1].clone(), None);
    let image = app.checkpoint(100000).unwrap();
    let mut offset = 54; // fixed header through authority and plan count
    for _ in 0..2 {
        let len = u32::from_le_bytes(image[offset..offset + 4].try_into().unwrap()) as usize;
        offset += 4 + len;
    }
    let first = offset + 4;
    let first_len = u32::from_le_bytes(image[first + 24..first + 28].try_into().unwrap()) as usize;
    let second = first + 28 + first_len;
    let mut corruptions = Vec::new();
    let mut bad = image.clone();
    bad[first..first + 8].fill(0);
    corruptions.push(bad);
    let mut bad = image.clone();
    bad[first..first + 8].copy_from_slice(&3u64.to_le_bytes());
    corruptions.push(bad);
    let mut bad = image.clone();
    bad[second..second + 8].copy_from_slice(&1u64.to_le_bytes());
    corruptions.push(bad);
    let mut bad = image.clone();
    bad[second + 8..second + 24].copy_from_slice(&image[first + 8..first + 24]);
    corruptions.push(bad);
    let mut bad = image.clone();
    bad[offset..offset + 4].copy_from_slice(&u32::MAX.to_le_bytes());
    corruptions.push(bad);
    let mut bad = image.clone();
    bad[first + 24..first + 28].copy_from_slice(&u32::MAX.to_le_bytes());
    corruptions.push(bad);
    let mut bad = image.clone();
    bad[first + 28] = 0;
    corruptions.push(bad);
    let mut bad = image.clone();
    bad.push(0);
    corruptions.push(bad);
    for bad in corruptions {
        assert_eq!(
            app.restore_checkpoint(1, 3, &bad),
            Err(ApplicationError::InvalidCheckpoint)
        );
        assert_eq!(app.checkpoint(100000).unwrap(), image);
    }
}

#[test]
fn maximum_partition_map_roundtrips_and_pending_byte_reservation_covers_conflicts() {
    let entries = (0..256)
        .map(|bucket| RouteEntry {
            scope: range(bucket, bucket + 1),
            target: RouteTarget::Group(group(20)),
        })
        .collect();
    let value =
        ResponsibilityManifest::new(input(1, range(0, 256), ExecutionMode::Partitioned(entries)))
            .unwrap();
    let large = command(value, None);
    let small = command(manifests().remove(0), None);
    assert!(large.len() > small.len());
    assert_eq!(
        DirectoryCommand::decode(&large)
            .unwrap()
            .encode(MAX_DIRECTORY_COMMAND_BYTES)
            .unwrap(),
        large
    );
    let mut app = initialize(
        Directory::new(
            plan(),
            DirectoryLimits {
                operations: 20,
                history_bytes: bootstrap_len() + large.len(),
            },
        )
        .unwrap(),
    );
    let operation = OperationId::new(1).unwrap();
    assert!(app
        .validate_proposal(
            operation,
            &small,
            [(operation, small.as_slice()), (operation, large.as_slice())].into_iter()
        )
        .is_ok());
    assert_eq!(
        app.validate_proposal(
            OperationId::new(2).unwrap(),
            &small,
            [(operation, large.as_slice())].into_iter()
        ),
        Err(ApplicationError::DedupCapacity)
    );
    // Shape-valid but outside the fixed grant: rejected outcome still consumes
    // exact original request history. Conflicting retry consumes no new slot.
    assert_eq!(
        app.apply_batch(&[entry(1, 1, large)]).unwrap()[0].outcome,
        DirectoryOutcome::OwnershipChange
    );
    assert_eq!(
        app.apply_batch(&[entry(2, 1, small)]).unwrap()[0].outcome,
        DirectoryOutcome::OperationConflict
    );
    assert_eq!(app.remaining_operations(), 18);
}

#[test]
fn plan_refuses_multiple_parents_and_foreign_aliases_of_local_responsibilities() {
    let mut values = manifests();
    let other = ResponsibilityManifest::new(input(
        4,
        range(0, 256),
        ExecutionMode::Delegated(vec![RouteEntry {
            scope: range(0, 256),
            target: child(3, 3),
        }]),
    ))
    .unwrap();
    values.push(other);
    assert_eq!(
        DirectoryPlan::new(group(1), values).unwrap_err().0,
        RoutingError::DuplicateChild
    );
    let mut values = manifests();
    let mut root = values[0].clone().into_input();
    let ExecutionMode::Delegated(ref mut routes) = root.execution else {
        panic!()
    };
    routes[0].target = child(2, 99);
    values[0] = ResponsibilityManifest::new(root).unwrap();
    assert_eq!(
        DirectoryPlan::new(group(1), values).unwrap_err().0,
        RoutingError::WrongChild
    );
}

#[test]
fn plan_depth_and_lifetime_limits_reject_before_retaining_work() {
    for limits in [
        DirectoryLimits {
            operations: 0,
            history_bytes: 1,
        },
        DirectoryLimits {
            operations: MAX_DIRECTORY_OPERATIONS + 1,
            history_bytes: 1,
        },
        DirectoryLimits {
            operations: 1,
            history_bytes: 0,
        },
        DirectoryLimits {
            operations: 1,
            history_bytes: MAX_DIRECTORY_HISTORY_BYTES + 1,
        },
    ] {
        let original = plan();
        let expected = original.clone();
        let (error, returned) = match Directory::new(original, limits) {
            Ok(_) => panic!("invalid limits accepted"),
            Err(error) => error,
        };
        assert_eq!(error, ApplicationError::DedupCapacity);
        assert_eq!(returned, expected);
    }
    for depth in [32, 33] {
        let mut values = Vec::with_capacity(depth);
        for n in 1..=depth {
            let execution = if n == depth {
                ExecutionMode::Single(group(20))
            } else {
                ExecutionMode::Delegated(vec![RouteEntry {
                    scope: range(0, 256),
                    target: child((n + 1) as u128, 1),
                }])
            };
            let mut value = input(n as u128, range(0, 256), execution);
            if n > 1 {
                value.parent = Some(ParentAuthority {
                    responsibility: id((n - 1) as u128),
                    group: group(1),
                });
            }
            values.push(ResponsibilityManifest::new(value).unwrap());
        }
        let result = DirectoryPlan::new(group(1), values);
        if depth == 32 {
            assert!(result.is_ok());
        } else {
            assert_eq!(result.unwrap_err().0, RoutingError::HopLimit);
        }
    }
    let mut values = Vec::with_capacity(MAX_DIRECTORY_MANIFESTS + 1);
    values.push(
        ResponsibilityManifest::new(input(1, range(0, 256), ExecutionMode::Single(group(20))))
            .unwrap(),
    );
    assert_eq!(
        DirectoryPlan::new(group(1), values).unwrap_err().0,
        RoutingError::Capacity
    );
}

#[test]
fn independently_authored_child_directory_uses_verified_external_bootstrap_grant() {
    let mut parent = directory();
    let values = manifests();
    publish(&mut parent, 1, 1, values[0].clone(), None);
    let mut child_input = input(3, range(128, 256), ExecutionMode::Single(group(30)));
    child_input.authority = group(3);
    child_input.parent = Some(ParentAuthority {
        responsibility: id(1),
        group: group(1),
    });
    let child_manifest = ResponsibilityManifest::new(child_input).unwrap();
    // The host checks an already authenticated committed parent view before
    // provisioning the fixed child plan. The plan is not its own certificate.
    let root = parent.manifest(id(1)).unwrap();
    let ExecutionMode::Delegated(entries) = &root.input().execution else {
        panic!()
    };
    assert!(entries
        .iter()
        .any(|entry| entry.scope == child_manifest.input().scope && entry.target == child(3, 3)));
    let child_plan = DirectoryPlan::new(group(3), vec![child_manifest.clone()]).unwrap();
    let mut child_app = initialize(
        Directory::new(
            child_plan,
            DirectoryLimits {
                operations: 4,
                history_bytes: 4096,
            },
        )
        .unwrap(),
    );
    assert_eq!(
        publish(&mut child_app, 1, 9, child_manifest.clone(), None).outcome,
        DirectoryOutcome::Published(RouteGeneration::new(1).unwrap())
    );
    assert!(parent.manifest(id(3)).is_none());
    assert!(child_app.manifest(id(1)).is_none());
    assert_eq!(child_app.read_at(1, id(3)).unwrap(), Some(child_manifest));
    assert_eq!(
        child_app.read_at(3, id(3)),
        Err(ApplicationError::NotApplied)
    );
    let child_checkpoint = child_app.checkpoint(10000).unwrap();
    drop(parent);
    assert_eq!(child_app.checkpoint(10000).unwrap(), child_checkpoint);
}

#[test]
fn read_receipts_charge_inline_and_nested_capacity_without_double_counting() {
    let mut app = directory();
    let values = manifests();
    publish(&mut app, 1, 1, values[0].clone(), None);
    let query = id(1);
    assert_eq!(app.query_bytes(&query, 0), Ok(0));
    let result = app.read_at(1, query).unwrap();
    let bound = app.read_result_bound(&query).unwrap();
    let nested = app.read_result_bytes(&result, bound).unwrap();
    assert!(nested > 0);
    assert_eq!(
        std::mem::size_of::<Option<ResponsibilityManifest>>() + nested,
        bound
    );
    assert_eq!(
        app.read_result_bytes(&result, nested - 1),
        Err(ApplicationError::ReceiptBudget)
    );
    assert_eq!(app.read_result_bytes(&None, 0), Ok(0));
    assert_eq!(
        app.read_result_bound(&id(99)).unwrap(),
        std::mem::size_of::<Option<ResponsibilityManifest>>()
    );
}

#[test]
fn committed_initialization_binds_the_full_plan_before_any_publication() {
    let limits = DirectoryLimits {
        operations: 20,
        history_bytes: bootstrap_len() - 1,
    };
    let error = match Directory::new(plan(), limits) {
        Ok(_) => panic!("initialization budget accepted"),
        Err((error, _)) => error,
    };
    assert_eq!(error, ApplicationError::DedupCapacity);
    let mut app = fresh_directory();
    let values = manifests();
    let publication = command(values[0].clone(), None);
    assert_eq!(
        app.validate_proposal(
            OperationId::new(1).unwrap(),
            &publication,
            std::iter::empty()
        ),
        Err(ApplicationError::NotApplied)
    );
    let bootstrap = app.bootstrap_command(MAX_DIRECTORY_COMMAND_BYTES).unwrap();
    assert!(app.bootstrap_command(bootstrap.len() - 1).is_err());
    for end in 0..bootstrap.len() {
        assert!(app
            .validate_proposal(
                OperationId::new(1).unwrap(),
                &bootstrap[..end],
                std::iter::empty()
            )
            .is_err());
        assert!(!app.is_initialized());
    }
    let mut bad = bootstrap.clone();
    bad.push(0);
    assert!(app
        .validate_proposal(OperationId::new(1).unwrap(), &bad, std::iter::empty())
        .is_err());
    app = initialize(app);
    assert!(app.is_initialized());
    let before = app.checkpoint(100000).unwrap();
    let mut changed = manifests();
    changed.push(
        ResponsibilityManifest::new(input(9, range(0, 256), ExecutionMode::Single(group(90))))
            .unwrap(),
    );
    let mut wrong =
        Directory::new(DirectoryPlan::new(group(1), changed).unwrap(), app.limits()).unwrap();
    let bootstrap_entry = LogEntry {
        index: 1,
        term: 1,
        payload: EntryPayload::Command {
            operation: OperationId::new(1_000_000).unwrap(),
            bytes: bootstrap,
        },
    };
    assert_eq!(
        wrong.apply_batch(&[bootstrap_entry]),
        Err(ApplicationError::InvalidCommand)
    );
    assert_eq!(wrong.applied_index(), 0);
    assert!(!wrong.is_initialized());
    assert_eq!(app.checkpoint(100000).unwrap(), before);
}

#[test]
fn large_plan_declares_initialization_envelope_and_restores_its_complete_binding() {
    let mut values = Vec::with_capacity(12);
    for responsibility in 1..=12 {
        let routes = (0..256)
            .map(|bucket| RouteEntry {
                scope: range(bucket, bucket + 1),
                target: RouteTarget::Group(group(20)),
            })
            .collect();
        values.push(
            ResponsibilityManifest::new(input(
                responsibility,
                range(0, 256),
                ExecutionMode::Partitioned(routes),
            ))
            .unwrap(),
        );
    }
    let grant = values[0].clone();
    let plan = DirectoryPlan::new(group(1), values).unwrap();
    let limits = DirectoryLimits {
        operations: 20,
        history_bytes: 1024 * 1024,
    };
    let app = Directory::new(plan.clone(), limits).unwrap();
    let bytes = app.bootstrap_command(MAX_DIRECTORY_COMMAND_BYTES).unwrap();
    assert!(bytes.len() > LogLimits::default().max_command_bytes);
    assert_eq!(app.readiness_requirements().command_bytes, bytes.len());
    assert!(app
        .bootstrap_command(MAX_DIRECTORY_PUBLICATION_BYTES)
        .is_err());
    assert_eq!(app.bootstrap_command(bytes.len()).unwrap(), bytes);
    let mut app = initialize(app);
    publish(&mut app, 1, 1, grant.clone(), None);
    let image = app
        .checkpoint(app.readiness_requirements().snapshot_bytes)
        .unwrap();
    let mut restored = Directory::new(plan, limits).unwrap();
    restored.restore_checkpoint(1, 2, &image).unwrap();
    assert!(restored.is_initialized());
    assert_eq!(restored.manifest(id(1)), Some(&grant));
    assert_eq!(restored.checkpoint(image.len()).unwrap(), image);
}
