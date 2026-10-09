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
use voteboat::{
    group_creation::*,
    native::{group_creation::*, snapshot_store::*},
    quorum::WeightedChild,
    snapshot::{SnapshotIdentity, SnapshotLimits},
};
fn creation_directory() -> Directory {
    directory()
        .with_group_creation()
        .unwrap_or_else(|_| panic!("fresh schema2 directory"))
}
fn created_service(protocol: NativePeerProtocol) {
    let _history = NATIVE_HISTORY.lock().unwrap_or_else(|e| e.into_inner());
    let clock = Instant::now();
    let root = std::env::temp_dir().join(format!(
        "voteboat-created-service-{}-{protocol:?}",
        std::process::id(),
    ));
    std::fs::create_dir_all(&root).unwrap();
    let mut targets = configuration(&root, 100, &[1, 2, 3], NativeOpenMode::Recover);
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
    let mut parents = open(
        configuration(&root, 1, &[1, 2, 3], NativeOpenMode::Create),
        &clock,
        protocol,
        creation_directory,
    );
    campaign(&mut parents, &clock, 1);
    let boot = creation_directory()
        .bootstrap_command(MAX_DIRECTORY_COMMAND_BYTES)
        .unwrap();
    assert_eq!(
        propose(&mut parents, &clock, 1, 10000, boot).outcome,
        DirectoryOutcome::Initialized
    );
    let publish = DirectoryCommand {
        expected: None,
        manifest: manifests()[0].clone(),
    }
    .encode(MAX_DIRECTORY_COMMAND_BYTES)
    .unwrap();
    assert!(matches!(
        propose(&mut parents, &clock, 1, 10001, publish).outcome,
        DirectoryOutcome::Published(_)
    ));
    let bytes = intent.encode(MAX_GROUP_CREATION_BYTES).unwrap();
    assert_eq!(
        propose(&mut parents, &clock, 1, 10002, bytes.clone()).outcome,
        DirectoryOutcome::CreationReserved
    );
    let status = parents[0].local().applications[&group(1)]
        .group_creation_at(0, group(100))
        .unwrap()
        .unwrap();
    let verify = |parents: &[Node<Directory>], target: &NativeStartup| {
        VerifiedGroupCreation::verify(
            &LocalCreationAuthority {
                core: parents[0].local().owner.core(group(1)).unwrap(),
                directory: &parents[0].local().applications[&group(1)],
            },
            status.clone(),
            target.node,
            target.store,
            intent.application,
        )
        .unwrap()
    };
    std::fs::create_dir_all(root.join("100")).unwrap();
    let mut original_bindings = BTreeMap::new();
    // Stop after two assigned replicas; neither a route nor a third replica exists.
    for target in &targets[..2] {
        let verified = verify(&parents, target);
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
    close(parents, &clock, 1, || {});
    let mut parents = open(
        configuration(&root, 1, &[1, 2, 3], NativeOpenMode::Recover),
        &clock,
        protocol,
        creation_directory,
    );
    campaign(&mut parents, &clock, 1);
    let retry = propose(&mut parents, &clock, 1, 10002, bytes);
    assert!(retry.duplicate);
    assert_eq!(retry.outcome, DirectoryOutcome::CreationReserved);
    let mut verified_targets = Vec::new();
    for (position, target) in targets.iter().enumerate() {
        let verified = verify(&parents, target);
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
        assert_eq!(receipt.operation(), status.operation);
        if let Some(previous) = original_bindings.get(&target.node) {
            assert_ne!(*previous, receipt.binding());
        }
        assert_eq!(log.state(group(100)).unwrap().bootstrap, intent.bootstrap);
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
    let mut nodes = open(targets, &clock, protocol, || Counter::new(64).unwrap());
    campaign(&mut nodes, &clock, 100);
    let original = propose(&mut nodes, &clock, 100, 20000, 7i64.to_le_bytes().to_vec());
    assert_eq!(original.outcome, CounterOutcome::Value(7));
    assert_eq!(read(&mut nodes, &clock, 100, ()), 7);
    for node in &mut nodes {
        node.control(group(100), NodeControl::Checkpoint).unwrap();
    }
    drive(&mut nodes, &clock, |ns| {
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
        .zip(close(nodes, &clock, 100, || {}))
        .collect::<BTreeMap<_, _>>();
    // Re-run the exact creation operation after application progress and compaction.
    // The immutable historical status remains usable while metadata is offline.
    for verified in &verified_targets {
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
    let mut targets = configuration(&root, 100, &[1, 2, 3], NativeOpenMode::Recover);
    for target in &mut targets {
        target.bootstrap = intent.bootstrap.clone();
    }
    let mut nodes = open(targets, &clock, protocol, || Counter::new(64).unwrap());
    campaign(&mut nodes, &clock, 100);
    let retry = propose(&mut nodes, &clock, 100, 20000, 7i64.to_le_bytes().to_vec());
    assert!(retry.duplicate);
    // Counter receipts name the retry's applied entry, while preserving the
    // original semantic operation/outcome across checkpoint recovery.
    assert!(retry.index > original.index);
    assert_eq!(retry.operation, original.operation);
    assert_eq!(retry.outcome, original.outcome);
    assert_eq!(
        propose(&mut nodes, &clock, 100, 20000, 8i64.to_le_bytes().to_vec()).outcome,
        CounterOutcome::OperationConflict
    );
    assert_eq!(
        propose(&mut nodes, &clock, 100, 20001, 5i64.to_le_bytes().to_vec()).outcome,
        CounterOutcome::Value(12)
    );
    assert_eq!(read(&mut nodes, &clock, 100, ()), 12);
    close(nodes, &clock, 100, || {});
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
#[test]
fn tcp_created_group_resumes_partial_assignment_and_runs_with_metadata_offline() {
    created_service(NativePeerProtocol::TcpTls);
}
#[cfg(feature = "quic")]
#[test]
fn quic_created_group_resumes_partial_assignment_and_runs_with_metadata_offline() {
    created_service(NativePeerProtocol::Quic);
}

use voteboat::namespace_creation::*;
use voteboat::{snapshot_worker::SnapshotWorker, worker::PersistenceWorker};

// Stop core polling immediately. Accepted native I/O may still finish; discard
// its observations and reclaim actual stores before reopening. No rollback or
// hardware power-loss claim follows from owner abort.
pub(super) fn abandon<A>(mut nodes: Vec<Node<A>>, g: u128) -> BTreeMap<NodeId, GroupLog>
where
    A: ProposalAdmission + BoundedReadableStateMachine + CheckpointStateMachine,
    A::Receipt: ApplicationReceipt,
{
    for n in &mut nodes {
        n.abort();
    }
    nodes
        .into_iter()
        .map(|n| {
            let id = n.local().owner.core(group(g)).unwrap().local_node();
            let mut recovery = n
                .into_recovery()
                .unwrap_or_else(|_| panic!("aborted owner"));
            drop(recovery.peers.take());
            let mut snapshots = recovery.local.snapshots.take().unwrap();
            let deadline = Instant::now() + Duration::from_secs(5);
            let mut log = None;
            let mut snapshot_done = false;
            loop {
                if log.is_none() {
                    let _ = recovery.local.persistence.poll(64);
                    let _ = recovery.local.persistence.poll_reclaims(64);
                    if let Some(store) = recovery.local.persistence.try_reclaim().unwrap() {
                        log = Some(store.state(group(g)).unwrap());
                    }
                }
                if !snapshot_done {
                    let _ = snapshots.worker.poll(64);
                    snapshot_done = snapshots.worker.try_reclaim().unwrap().is_some();
                }
                if snapshot_done {
                    if let Some(log) = log.take() {
                        return (id, log);
                    }
                }
                assert!(
                    Instant::now() < deadline,
                    "aborted selected workers must return stores"
                );
                std::thread::park_timeout(Duration::from_millis(1));
            }
        })
        .collect()
}
fn namespace_directory() -> Directory {
    directory()
        .with_namespace_creation()
        .unwrap_or_else(|_| panic!("schema3"))
}
fn namespace_service(protocol: NativePeerProtocol, interrupted: bool) {
    let _history = NATIVE_HISTORY.lock().unwrap_or_else(|e| e.into_inner());
    let clock = Instant::now();
    let root = std::env::temp_dir().join(format!(
        "voteboat-namespace-service-{}-{protocol:?}-{interrupted}",
        std::process::id()
    ));
    std::fs::create_dir_all(&root).unwrap();
    let targets = configuration(&root, 100, &[1, 2, 3], NativeOpenMode::Recover);
    let intent = GroupCreationIntent {
        authority: group(1),
        parent: responsibility(1),
        expected: RouteGeneration::new(1).unwrap(),
        responsibility: responsibility(50),
        bootstrap: targets[0].bootstrap.clone(),
        application: grant().input().application,
        mode: GroupCreationMode::Empty,
    };
    let mut parents = open(
        configuration(&root, 1, &[1, 2, 3], NativeOpenMode::Create),
        &clock,
        protocol,
        namespace_directory,
    );
    campaign(&mut parents, &clock, 1);
    let boot = namespace_directory()
        .bootstrap_command(MAX_DIRECTORY_COMMAND_BYTES)
        .unwrap();
    assert_eq!(
        propose(&mut parents, &clock, 1, 10000, boot).outcome,
        DirectoryOutcome::Initialized
    );
    let parent_manifest = manifests()[0].clone();
    propose(
        &mut parents,
        &clock,
        1,
        10001,
        DirectoryCommand {
            expected: None,
            manifest: parent_manifest.clone(),
        }
        .encode(MAX_DIRECTORY_COMMAND_BYTES)
        .unwrap(),
    );
    assert_eq!(
        propose(
            &mut parents,
            &clock,
            1,
            10002,
            intent.encode(MAX_GROUP_CREATION_BYTES).unwrap()
        )
        .outcome,
        DirectoryOutcome::CreationReserved
    );
    let creation = parents[0].local().applications[&group(1)]
        .group_creation_at(0, group(100))
        .unwrap()
        .unwrap();
    let mut m = grant().into_input();
    m.responsibility = responsibility(50);
    m.parent = None;
    m.execution = ExecutionMode::Single(group(100));
    let plan = NamespacePlan {
        creation: creation.clone(),
        manifest: ResponsibilityManifest::new(m).unwrap(),
    };
    std::fs::create_dir_all(root.join("100")).unwrap();
    for c in &targets {
        let verified = VerifiedGroupCreation::verify(
            &LocalCreationAuthority {
                core: parents[0].local().owner.core(group(1)).unwrap(),
                directory: &parents[0].local().applications[&group(1)],
            },
            creation.clone(),
            c.node,
            c.store,
            intent.application,
        )
        .unwrap();
        let mut log = NativeLogStore::create(
            FileLogIo::create(&c.directory).unwrap(),
            c.store,
            LogLimits::default(),
        )
        .unwrap();
        let mut bindings = FileCreationBindings::open(&c.directory).unwrap();
        establish_created_group(&verified, &mut log, &mut bindings).unwrap();
        NativeSnapshotStore::create(
            FileSnapshotIo::create(c.directory.join("snapshots")).unwrap(),
            SnapshotIdentity {
                store: c.store,
                group: group(100),
            },
            SnapshotLimits::default(),
        )
        .unwrap();
    }
    let fresh = || {
        CreatedNamespace::new(
            plan.clone(),
            Counter::new(64).unwrap(),
            HostPolicy,
            limits(),
        )
        .unwrap_or_else(|_| panic!("created namespace"))
    };
    let mut nodes = open(targets, &clock, protocol, fresh);
    campaign(&mut nodes, &clock, 100);
    let mut hint = super::super::hint(4);
    hint.responsibility = responsibility(50);
    hint.group = group(100);
    let query = || {
        NamespaceQuery::Data(RoutedQuery {
            hint,
            key: vec![4],
            query: (),
        })
    };
    let bytes = encode_routed(hint, &[4], &7i64.to_le_bytes(), 1024).unwrap();
    assert!(nodes[0].local().applications[&group(100)]
        .validate_proposal(OperationId::new(20000).unwrap(), &bytes, std::iter::empty())
        .is_err());
    assert_eq!(
        read(&mut nodes, &clock, 100, query()),
        NamespaceRead::NotActive
    );
    let init = fresh()
        .initialization_command(MAX_ROUTED_COMMAND_BYTES)
        .unwrap();
    if interrupted {
        let ticket = nodes[0]
            .propose(ClientRequest {
                group: group(100),
                operation: creation.operation,
                bytes: init,
            })
            .unwrap();
        drive(&mut nodes, &clock, |ns| {
            ns.iter().all(|n| {
                n.local().applications[&group(100)]
                    .status()
                    .ready_index
                    .is_some()
            })
        });
        assert_eq!(ticket.operation, creation.operation);
        assert_eq!(nodes[0].local().clients.usage().requests, 1);
    } else {
        assert_eq!(
            propose(&mut nodes, &clock, 100, 10002, init).outcome,
            NamespaceOutcome::Ready
        );
    }
    let NamespaceRead::Status(ready) = read(&mut nodes, &clock, 100, NamespaceQuery::Status) else {
        panic!("ready status")
    };
    let publication = NamespacePublication::from_status(&plan, ready).unwrap();
    if !interrupted {
        for n in &mut nodes {
            n.control(group(100), NodeControl::Checkpoint).unwrap();
        }
        drive(&mut nodes, &clock, |ns| {
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
    }
    if interrupted {
        let logs = abandon(nodes, 100);
        assert!(logs.values().all(|s| s.base_index() == 0));
        assert!(logs
            .values()
            .all(|s| s.commit_index >= ready.ready_index.unwrap()));
    } else {
        close(nodes, &clock, 100, || {
            drive(&mut parents, &clock, |_| true);
        });
    }
    let mut nodes = open(
        configuration(&root, 100, &[1, 2, 3], NativeOpenMode::Recover),
        &clock,
        protocol,
        fresh,
    );
    campaign(&mut nodes, &clock, 100);
    assert!(nodes
        .iter()
        .all(|n| n.local().applications[&group(100)].status() == ready));
    assert_eq!(
        read(&mut nodes, &clock, 100, query()),
        NamespaceRead::NotActive
    );
    campaign(&mut parents, &clock, 1);
    let pub_bytes = publication.encode(MAX_NAMESPACE_PUBLICATION_BYTES).unwrap();
    if interrupted {
        let ticket = parents[0]
            .propose(ClientRequest {
                group: group(1),
                operation: OperationId::new(10003).unwrap(),
                bytes: pub_bytes.clone(),
            })
            .unwrap();
        drive(&mut parents[..2], &clock, |ns| {
            ns.iter().all(|n| {
                n.local().applications[&group(1)]
                    .namespace_publication_at(0, creation.operation)
                    .unwrap()
                    .is_some()
            })
        });
        assert_eq!(ticket.operation, OperationId::new(10003).unwrap());
        assert_eq!(parents[0].local().clients.usage().requests, 1);
        assert!(parents[2].local().applications[&group(1)]
            .manifest(responsibility(50))
            .is_none());
    } else {
        assert_eq!(
            propose(&mut parents, &clock, 1, 10003, pub_bytes.clone()).outcome,
            DirectoryOutcome::NamespacePublished(RouteGeneration::new(1).unwrap())
        );
    }
    let status = parents[0].local().applications[&group(1)]
        .namespace_publication_at(0, creation.operation)
        .unwrap()
        .unwrap();
    if !interrupted {
        for n in &mut parents {
            n.control(group(1), NodeControl::Checkpoint).unwrap();
        }
        drive(&mut parents, &clock, |ns| {
            ns.iter().all(|n| {
                n.local().owner.core(group(1)).unwrap().state().base_index()
                    == n.local().applications[&group(1)].applied_index()
            })
        });
    }
    if interrupted {
        let quorum = parents[..2]
            .iter()
            .map(|n| n.local().owner.core(group(1)).unwrap().local_node())
            .collect::<Vec<_>>();
        let lagging = parents[2]
            .local()
            .owner
            .core(group(1))
            .unwrap()
            .local_node();
        let logs = abandon(parents, 1);
        assert!(logs.values().all(|s| s.base_index() == 0));
        assert!(quorum.iter().all(|n| logs[n].commit_index >= status.index));
        assert!(logs[&lagging].commit_index < status.index);
    } else {
        close(parents, &clock, 1, || {
            drive(&mut nodes, &clock, |_| true);
        });
    }
    let mut parents = open(
        configuration(&root, 1, &[1, 2, 3], NativeOpenMode::Recover),
        &clock,
        protocol,
        namespace_directory,
    );
    campaign(&mut parents, &clock, 1);
    assert!(propose(&mut parents, &clock, 1, 10003, pub_bytes).duplicate);
    assert_eq!(
        parents[0].local().applications[&group(1)]
            .namespace_publication_at(0, creation.operation)
            .unwrap(),
        Some(status.clone())
    );
    assert!(parents.iter().all(|n| n.local().applications[&group(1)]
        .namespace_publication_at(0, creation.operation)
        .unwrap()
        == Some(status.clone())));
    assert_eq!(
        parents[0].local().applications[&group(1)].manifest(responsibility(1)),
        Some(&parent_manifest)
    );
    assert_eq!(
        parents[0].local().applications[&group(1)].manifest(responsibility(50)),
        Some(&plan.manifest)
    );
    let observed = read(&mut parents, &clock, 1, responsibility(50)).unwrap();
    let mut cache = NativeManifestCache::new(ManifestCacheLimits {
        manifests: 1,
        bytes: 4096,
    })
    .unwrap();
    cache.admit(observed).unwrap();
    assert_eq!(
        resolve(&cache, &HostPolicy, responsibility(50), &[4], 1).unwrap(),
        hint
    );
    let parent_ids = parents
        .iter()
        .map(|n| n.local().owner.core(group(1)).unwrap().local_node())
        .collect::<Vec<_>>();
    let parent_logs = parent_ids
        .into_iter()
        .zip(close(parents, &clock, 1, || {
            drive(&mut nodes, &clock, |_| true);
        }))
        .collect::<BTreeMap<_, _>>();
    campaign(&mut nodes, &clock, 100);
    assert_eq!(
        read(&mut nodes, &clock, 100, query()),
        NamespaceRead::NotActive
    );
    let activation = nodes[0].local().applications[&group(100)]
        .activation_command(&status, 2000)
        .unwrap();
    if interrupted {
        let ticket = nodes[0]
            .propose(ClientRequest {
                group: group(100),
                operation: status.operation,
                bytes: activation.clone(),
            })
            .unwrap();
        drive(&mut nodes[..2], &clock, |ns| {
            ns.iter().all(|n| {
                n.local().applications[&group(100)]
                    .status()
                    .activation_index
                    .is_some()
            })
        });
        let original = nodes[0].local().applications[&group(100)].status();
        assert_eq!(ticket.operation, status.operation);
        assert_eq!(nodes[0].local().clients.usage().requests, 1);
        assert!(nodes[2].local().applications[&group(100)]
            .status()
            .activation_index
            .is_none());
        assert_eq!(
            nodes[2].local().applications[&group(100)]
                .read_at(
                    nodes[2].local().applications[&group(100)].applied_index(),
                    query()
                )
                .unwrap(),
            NamespaceRead::NotActive
        );
        let quorum = nodes[..2]
            .iter()
            .map(|n| n.local().owner.core(group(100)).unwrap().local_node())
            .collect::<Vec<_>>();
        let lagging = nodes[2]
            .local()
            .owner
            .core(group(100))
            .unwrap()
            .local_node();
        let logs = abandon(nodes, 100);
        assert!(logs.values().all(|s| s.base_index() == 0));
        assert!(quorum
            .iter()
            .all(|n| logs[n].commit_index >= original.activation_index.unwrap()));
        assert!(logs[&lagging].commit_index < original.activation_index.unwrap());
        nodes = open(
            configuration(&root, 100, &[1, 2, 3], NativeOpenMode::Recover),
            &clock,
            protocol,
            fresh,
        );
        campaign(&mut nodes, &clock, 100);
        assert_eq!(
            propose(&mut nodes, &clock, 100, 10003, activation.clone()).outcome,
            NamespaceOutcome::Activated
        );
        assert!(nodes
            .iter()
            .all(|n| n.local().applications[&group(100)].status() == original));
    } else {
        assert_eq!(
            propose(&mut nodes, &clock, 100, 10003, activation.clone()).outcome,
            NamespaceOutcome::Activated
        );
    }
    let original = propose(&mut nodes, &clock, 100, 20000, bytes.clone());
    assert!(matches!(
        original.outcome,
        NamespaceOutcome::Data(RoutedReceipt {
            outcome: RoutedOutcome::Applied(CounterReceipt {
                outcome: CounterOutcome::Value(7),
                duplicate: false,
                ..
            }),
            ..
        })
    ));
    assert_eq!(
        read(&mut nodes, &clock, 100, query()),
        NamespaceRead::Data(RoutedRead::Served(7))
    );
    for n in &mut nodes {
        n.control(group(100), NodeControl::Checkpoint).unwrap();
    }
    drive(&mut nodes, &clock, |ns| {
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
    close(nodes, &clock, 100, || {});
    let mut nodes = open(
        configuration(&root, 100, &[1, 2, 3], NativeOpenMode::Recover),
        &clock,
        protocol,
        fresh,
    );
    campaign(&mut nodes, &clock, 100);
    assert_eq!(
        propose(&mut nodes, &clock, 100, 10003, activation).outcome,
        NamespaceOutcome::Activated
    );
    let retry = propose(&mut nodes, &clock, 100, 20000, bytes);
    assert!(matches!(
        retry.outcome,
        NamespaceOutcome::Data(RoutedReceipt {
            outcome: RoutedOutcome::Applied(CounterReceipt {
                outcome: CounterOutcome::Value(7),
                duplicate: true,
                ..
            }),
            ..
        })
    ));
    assert_eq!(
        read(&mut nodes, &clock, 100, query()),
        NamespaceRead::Data(RoutedRead::Served(7))
    );
    close(nodes, &clock, 100, || {});
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
#[test]
fn tcp_namespace_ready_publish_activate_survives_reopen_and_metadata_outage() {
    namespace_service(NativePeerProtocol::TcpTls, false);
}
#[cfg(feature = "quic")]
#[test]
fn quic_namespace_ready_publish_activate_survives_reopen_and_metadata_outage() {
    namespace_service(NativePeerProtocol::Quic, false);
}

#[test]
fn tcp_namespace_partial_publication_activation_unread_receipts_owner_abort() {
    namespace_service(NativePeerProtocol::TcpTls, true);
}
#[cfg(feature = "quic")]
#[test]
fn quic_namespace_partial_publication_activation_unread_receipts_owner_abort() {
    namespace_service(NativePeerProtocol::Quic, true);
}
