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
struct Assignment {
    bytes: Vec<u8>,
    intent: GroupCreationIntent,
    status: GroupCreationStatus,
}
fn targets(root: &Path) -> (Vec<NativeStartup>, GroupCreationIntent) {
    let mut targets = configuration(root, 100, &[1, 2, 3], NativeOpenMode::Recover);
    let mut bootstrap = targets[0].bootstrap.clone();
    bootstrap.policy = Policy::new(
        Tree::Majority(vec![
            Tree::Voter(support::node(1)),
            Tree::Weighted(vec![
                WeightedChild {
                    weight: 2,
                    node: Tree::Voter(support::node(2)),
                },
                WeightedChild {
                    weight: 1,
                    node: Tree::Voter(support::node(3)),
                },
            ]),
        ]),
        Limits::default(),
    )
    .unwrap();
    for target in &mut targets {
        target.bootstrap = bootstrap.clone();
    }
    let intent = GroupCreationIntent {
        authority: group(1),
        parent: responsibility(1),
        expected: RouteGeneration::new(1).unwrap(),
        responsibility: responsibility(50),
        bootstrap,
        application: ApplicationAdapter {
            id: ApplicationAdapterId::new(1).unwrap(),
            version: 1,
        },
        mode: GroupCreationMode::Empty,
    };
    (targets, intent)
}
impl Assignment {
    fn reserve(
        parents: &mut [Node<Directory>],
        clock: &Instant,
        intent: GroupCreationIntent,
    ) -> Self {
        campaign(parents, clock, 1);
        let boot = creation_directory()
            .bootstrap_command(MAX_DIRECTORY_COMMAND_BYTES)
            .unwrap();
        assert_eq!(
            propose(parents, clock, 1, 10000, boot).outcome,
            DirectoryOutcome::Initialized
        );
        let publish = DirectoryCommand {
            expected: None,
            manifest: manifests()[0].clone(),
        }
        .encode(MAX_DIRECTORY_COMMAND_BYTES)
        .unwrap();
        assert!(matches!(
            propose(parents, clock, 1, 10001, publish).outcome,
            DirectoryOutcome::Published(_)
        ));
        let bytes = intent.encode(MAX_GROUP_CREATION_BYTES).unwrap();
        assert_eq!(
            propose(parents, clock, 1, 10002, bytes.clone()).outcome,
            DirectoryOutcome::CreationReserved
        );
        let status = parents[0].local().applications[&group(1)]
            .group_creation_at(0, group(100))
            .unwrap()
            .unwrap();
        Self {
            intent,
            status,
            bytes,
        }
    }
    fn verify(&self, parents: &[Node<Directory>], target: &NativeStartup) -> VerifiedGroupCreation {
        VerifiedGroupCreation::verify(
            &LocalCreationAuthority {
                core: parents[0].local().owner.core(group(1)).unwrap(),
                directory: &parents[0].local().applications[&group(1)],
            },
            self.status.clone(),
            target.node,
            target.store,
            self.intent.application,
        )
        .unwrap()
    }

    fn establish_partial(
        &self,
        root: &Path,
        parents: &[Node<Directory>],
        targets: &[NativeStartup],
    ) -> BTreeMap<NodeId, StoreBinding> {
        std::fs::create_dir_all(root.join("100")).unwrap();
        let mut original_bindings = BTreeMap::new();
        // Stop after two assigned replicas; neither a route nor a third replica exists.
        for target in &targets[..2] {
            let verified = self.verify(parents, target);
            let mut log = NativeLogStore::create(
                FileLogIo::create(&target.directory).unwrap(),
                target.store,
                LogLimits::default(),
            )
            .unwrap();
            let mut bindings = FileCreationBindings::open(&target.directory).unwrap();
            let receipt = establish_created_group(&verified, &mut log, &mut bindings).unwrap();
            original_bindings.insert(target.node, receipt.binding());
            NativeSnapshotStore::create(
                FileSnapshotIo::create(target.directory.join("snapshots")).unwrap(),
                SnapshotIdentity {
                    store: target.store,
                    group: group(100),
                },
                SnapshotLimits::default(),
            )
            .unwrap();
        }
        assert!(!targets[2].directory.exists());
        original_bindings
    }
    fn complete(
        &self,
        parents: &[Node<Directory>],
        targets: &[NativeStartup],
        original_bindings: &BTreeMap<NodeId, StoreBinding>,
    ) -> Vec<VerifiedGroupCreation> {
        let mut verified_targets = Vec::new();
        for (position, target) in targets.iter().enumerate() {
            let verified = self.verify(parents, target);
            let mut log = if position < 2 {
                NativeLogStore::recover(
                    FileLogIo::open(&target.directory).unwrap(),
                    target.store,
                    LogLimits::default(),
                )
                .unwrap()
            } else {
                NativeLogStore::create(
                    FileLogIo::create(&target.directory).unwrap(),
                    target.store,
                    LogLimits::default(),
                )
                .unwrap()
            };
            let mut bindings = FileCreationBindings::open(&target.directory).unwrap();
            let receipt = establish_created_group(&verified, &mut log, &mut bindings).unwrap();
            assert_eq!(receipt.operation(), self.status.operation);
            if let Some(previous) = original_bindings.get(&target.node) {
                assert_ne!(*previous, receipt.binding());
            }
            assert_eq!(
                log.state(group(100)).unwrap().bootstrap,
                self.intent.bootstrap
            );
            let snapshot_identity = SnapshotIdentity {
                store: target.store,
                group: group(100),
            };
            if position < 2 {
                NativeSnapshotStore::recover(
                    FileSnapshotIo::open(target.directory.join("snapshots")).unwrap(),
                    snapshot_identity,
                    SnapshotLimits::default(),
                )
                .unwrap();
            } else {
                NativeSnapshotStore::create(
                    FileSnapshotIo::create(target.directory.join("snapshots")).unwrap(),
                    snapshot_identity,
                    SnapshotLimits::default(),
                )
                .unwrap();
            }
            verified_targets.push(verified);
        }
        verified_targets
    }
}
fn exercise(
    targets: Vec<NativeStartup>,
    clock: &Instant,
    protocol: NativePeerProtocol,
) -> (CounterReceipt, BTreeMap<NodeId, GroupLog>) {
    let mut nodes = open(targets, clock, protocol, || Counter::new(64).unwrap());
    campaign(&mut nodes, clock, 100);
    let original = propose(&mut nodes, clock, 100, 20000, 7i64.to_le_bytes().to_vec());
    assert_eq!(original.outcome, CounterOutcome::Value(7));
    assert_eq!(read(&mut nodes, clock, 100, ()), 7);
    for node in &mut nodes {
        node.control(group(100), NodeControl::Checkpoint).unwrap();
    }
    drive(&mut nodes, clock, |ns| {
        ns.iter().all(|n| {
            n.local()
                .owner
                .core(group(100))
                .unwrap()
                .state()
                .base_index()
                == n.local().applications[&group(100)].applied_index()
        })
    });
    let child_ids = nodes
        .iter()
        .map(|n| n.local().owner.core(group(100)).unwrap().local_node())
        .collect::<Vec<_>>();
    let child_logs = child_ids
        .into_iter()
        .zip(close(nodes, clock, 100, || {}))
        .collect::<BTreeMap<_, _>>();
    (original, child_logs)
}
fn repeat_assignment(
    root: &Path,
    verified_targets: &[VerifiedGroupCreation],
    child_logs: &BTreeMap<NodeId, GroupLog>,
) {
    // Re-run the exact creation operation after application progress and compaction.
    // The immutable historical status remains usable while metadata is offline.
    for verified in verified_targets {
        let directory = root.join(format!("100/{}", verified.node().get()));
        let mut log = NativeLogStore::recover(
            FileLogIo::open(&directory).unwrap(),
            verified.store(),
            LogLimits::default(),
        )
        .unwrap();
        let before = log.state(group(100)).unwrap();
        assert_eq!(child_logs[&verified.node()], before);
        let mut bindings = FileCreationBindings::open(&directory).unwrap();
        establish_created_group(verified, &mut log, &mut bindings).unwrap();
        assert_eq!(log.state(group(100)).unwrap(), before);
    }
}
fn recover_service(
    root: &Path,
    clock: &Instant,
    protocol: NativePeerProtocol,
    intent: &GroupCreationIntent,
    original: CounterReceipt,
) {
    let mut targets = configuration(root, 100, &[1, 2, 3], NativeOpenMode::Recover);
    for target in &mut targets {
        target.bootstrap = intent.bootstrap.clone();
    }
    let mut nodes = open(targets, clock, protocol, || Counter::new(64).unwrap());
    campaign(&mut nodes, clock, 100);
    let retry = propose(&mut nodes, clock, 100, 20000, 7i64.to_le_bytes().to_vec());
    assert!(retry.duplicate);
    // Counter receipts name the retry's applied entry, while preserving the
    // original semantic operation/outcome across checkpoint recovery.
    assert!(retry.index > original.index);
    assert_eq!(retry.operation, original.operation);
    assert_eq!(retry.outcome, original.outcome);
    assert_eq!(
        propose(&mut nodes, clock, 100, 20000, 8i64.to_le_bytes().to_vec()).outcome,
        CounterOutcome::OperationConflict
    );
    assert_eq!(
        propose(&mut nodes, clock, 100, 20001, 5i64.to_le_bytes().to_vec()).outcome,
        CounterOutcome::Value(12)
    );
    assert_eq!(read(&mut nodes, clock, 100, ()), 12);
    close(nodes, clock, 100, || {});
}
pub(super) fn run(protocol: NativePeerProtocol) {
    let _history = NATIVE_HISTORY.lock().unwrap_or_else(|e| e.into_inner());
    let clock = Instant::now();
    let root = std::env::temp_dir().join(format!(
        "voteboat-created-service-{}-{protocol:?}",
        std::process::id()
    ));
    std::fs::create_dir_all(&root).unwrap();
    let (targets, intent) = targets(&root);
    let mut parents = open(
        configuration(&root, 1, &[1, 2, 3], NativeOpenMode::Create),
        &clock,
        protocol,
        creation_directory,
    );
    let assignment = Assignment::reserve(&mut parents, &clock, intent);
    let original_bindings = assignment.establish_partial(&root, &parents, &targets);
    close(parents, &clock, 1, || {});
    let mut parents = open(
        configuration(&root, 1, &[1, 2, 3], NativeOpenMode::Recover),
        &clock,
        protocol,
        creation_directory,
    );
    campaign(&mut parents, &clock, 1);
    let retry = propose(&mut parents, &clock, 1, 10002, assignment.bytes.clone());
    assert!(retry.duplicate);
    assert_eq!(retry.outcome, DirectoryOutcome::CreationReserved);
    let verified_targets = assignment.complete(&parents, &targets, &original_bindings);
    assert!(parents.iter().all(|p| p.local().applications[&group(1)]
        .manifest(responsibility(50))
        .is_none()));
    let parent_ids = parents
        .iter()
        .map(|p| p.local().owner.core(group(1)).unwrap().local_node())
        .collect::<Vec<_>>();
    let parent_logs = parent_ids
        .into_iter()
        .zip(close(parents, &clock, 1, || {}))
        .collect::<BTreeMap<_, _>>();
    let (original, child_logs) = exercise(targets, &clock, protocol);
    repeat_assignment(&root, &verified_targets, &child_logs);
    recover_service(&root, &clock, protocol, &assignment.intent, original);
    for n in 1..=3 {
        let log = NativeLogStore::recover(
            FileLogIo::open(root.join(format!("1/{n}"))).unwrap(),
            support::identity(n),
            LogLimits::default(),
        )
        .unwrap();
        assert_eq!(
            parent_logs[&support::node(n as u64)],
            log.state(group(1)).unwrap()
        );
    }

    std::fs::remove_dir_all(root).unwrap();
}
