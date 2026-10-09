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
#[path = "namespace_creation/source.rs"]
mod source;
mod support;
#[path = "namespace_creation/transfer.rs"]
mod transfer;
use support::*;
use voteboat::{
    application::*, directory::*, identity::*, log::*, namespace_creation::*,
    placement::PlacementRequirements, routed::*, routing::*,
};
#[derive(Clone)]
struct Policy;
impl PartitionPolicy for Policy {
    fn scheme(&self) -> PartitionScheme {
        PartitionScheme {
            id: RoutingSchemeId::new(1).unwrap(),
            version: 1,
        }
    }
    fn bucket(&self, key: &[u8]) -> Result<u16, RoutingError> {
        match key {
            [b] => Ok((*b).into()),
            _ => Err(RoutingError::InvalidKey),
        }
    }
}
fn identity_of(n: u128) -> ResponsibilityIdentity {
    ResponsibilityIdentity {
        id: ResponsibilityId::new(n).unwrap(),
        incarnation: ResponsibilityIncarnation::new(1).unwrap(),
    }
}
fn manifest(n: u128, g: u128) -> ResponsibilityManifest {
    ResponsibilityManifest::new(ManifestInput {
        responsibility: identity_of(n),
        parent: None,
        authority: group(1),
        application: ApplicationAdapter {
            id: ApplicationAdapterId::new(1).unwrap(),
            version: 1,
        },
        scheme: Policy.scheme(),
        scope: BucketRange::new(0, 256).unwrap(),
        epoch: OwnershipEpoch::new(1).unwrap(),
        generation: RouteGeneration::new(1).unwrap(),
        placement: PlacementRequirements {
            minimum_voting_domains: 2,
            survive_any_single_domain_loss: false,
        },
        state: ResponsibilityState::Active,
        execution: ExecutionMode::Single(group(g)),
    })
    .unwrap()
}
fn directory(ops: usize) -> Directory {
    Directory::new(
        DirectoryPlan::new(group(1), vec![manifest(1, 20)]).unwrap(),
        DirectoryLimits {
            operations: ops,
            history_bytes: 100000,
        },
    )
    .unwrap()
    .with_namespace_creation()
    .unwrap_or_else(|_| panic!("schema3"))
}
fn entry(index: u64, op: u128, bytes: Vec<u8>) -> LogEntry {
    LogEntry {
        index,
        term: 1,
        payload: EntryPayload::Command {
            operation: OperationId::new(op).unwrap(),
            bytes,
        },
    }
}
fn commit<A: StateMachine>(a: &mut A, op: u128, b: Vec<u8>) -> A::Receipt {
    a.apply_batch(&[entry(a.applied_index() + 1, op, b)])
        .unwrap()
        .remove(0)
}
fn reserve(ops: usize) -> (Directory, NamespacePlan) {
    reserve_directory(directory(ops))
}
fn reserve_directory(mut d: Directory) -> (Directory, NamespacePlan) {
    let boot = d.bootstrap_command(MAX_DIRECTORY_COMMAND_BYTES).unwrap();
    commit(&mut d, 1, boot);
    commit(
        &mut d,
        2,
        DirectoryCommand {
            expected: None,
            manifest: manifest(1, 20),
        }
        .encode(MAX_DIRECTORY_COMMAND_BYTES)
        .unwrap(),
    );
    let intent = GroupCreationIntent {
        authority: group(1),
        parent: identity_of(1),
        expected: RouteGeneration::new(1).unwrap(),
        responsibility: identity_of(50),
        bootstrap: bootstrap(100, 3),
        application: manifest(50, 100).input().application,
        mode: GroupCreationMode::Empty,
    };
    assert_eq!(
        commit(&mut d, 3, intent.encode(MAX_GROUP_CREATION_BYTES).unwrap()).outcome,
        DirectoryOutcome::CreationReserved
    );
    let creation = d
        .group_creation_at(d.applied_index(), group(100))
        .unwrap()
        .unwrap();
    (
        d,
        NamespacePlan {
            creation,
            manifest: manifest(50, 100),
        },
    )
}
type Target = CreatedNamespace<Counter, Policy>;
fn target(p: &NamespacePlan) -> Target {
    CreatedNamespace::new(
        p.clone(),
        Counter::new(8).unwrap(),
        Policy,
        RoutedLimits {
            operations: 8,
            semantic_bytes: 10000,
            payload_bytes: 1024,
            inner_checkpoint_bytes: 10000,
        },
    )
    .unwrap_or_else(|_| panic!("target"))
}
fn data(op: u128, delta: i64) -> (OperationId, Vec<u8>) {
    (
        OperationId::new(op).unwrap(),
        encode_routed(
            RouteHint {
                scheme: Policy.scheme(),
                scope: BucketRange::new(0, 256).unwrap(),
                bucket: 4,
                responsibility: identity_of(50),
                group: group(100),
                application: manifest(50, 100).input().application,
                epoch: OwnershipEpoch::new(1).unwrap(),
                generation: RouteGeneration::new(1).unwrap(),
            },
            &[4],
            &delta.to_le_bytes(),
            2000,
        )
        .unwrap(),
    )
}
fn query() -> NamespaceQuery<()> {
    NamespaceQuery::Data(RoutedQuery {
        hint: RouteHint {
            scheme: Policy.scheme(),
            scope: BucketRange::new(0, 256).unwrap(),
            bucket: 4,
            responsibility: identity_of(50),
            group: group(100),
            application: manifest(50, 100).input().application,
            epoch: OwnershipEpoch::new(1).unwrap(),
            generation: RouteGeneration::new(1).unwrap(),
        },
        key: vec![4],
        query: (),
    })
}
fn ready(t: &mut Target) -> NamespacePublication {
    let b = t.initialization_command(100000).unwrap();
    let entries = [entry(t.applied_index() + 1, 3, b)];
    let bound = t.receipt_bytes_bound(&entries).unwrap();
    let receipts = t.apply_batch(&entries).unwrap();
    assert!(receipts.capacity() * std::mem::size_of::<NamespaceReceipt<CounterReceipt>>() <= bound);
    assert_eq!(receipts[0].outcome, NamespaceOutcome::Ready);
    NamespacePublication::from_status(t.plan(), t.status()).unwrap()
}
#[test]
fn publication_reserved_control_completes_full_directory_and_recovers_exact_status() {
    let (mut d, p) = reserve(3);
    assert_eq!(d.remaining_operations(), 0);
    let mut t = target(&p);
    let pubn = ready(&mut t);
    let b = pubn.encode(1024).unwrap();
    assert!(d
        .validate_proposal(OperationId::new(4).unwrap(), &b, std::iter::empty())
        .is_ok());
    assert_eq!(
        commit(&mut d, 4, b.clone()).outcome,
        DirectoryOutcome::NamespacePublished(RouteGeneration::new(1).unwrap())
    );
    assert_eq!(d.remaining_operations(), 0);
    let original = d
        .namespace_publication_at(d.applied_index(), p.creation.operation)
        .unwrap()
        .unwrap();
    let old_parent = d.manifest(identity_of(1)).unwrap().clone();
    let image = d.checkpoint(1000000).unwrap();
    let index = d.applied_index();
    let mut restored = directory(3);
    restored.restore_checkpoint(3, index, &image).unwrap();
    assert_eq!(
        restored
            .namespace_publication_at(index, p.creation.operation)
            .unwrap(),
        Some(original)
    );
    assert!(commit(&mut restored, 4, b).duplicate);
    assert_eq!(restored.manifest(identity_of(1)), Some(&old_parent));
    assert_eq!(restored.manifest(identity_of(50)), Some(&p.manifest));
    for end in 0..image.len() {
        let mut fresh = directory(3);
        assert!(fresh.restore_checkpoint(3, index, &image[..end]).is_err());
        assert_eq!(fresh.applied_index(), 0);
    }
    let mut schema2 = Directory::new(
        DirectoryPlan::new(group(1), vec![manifest(1, 20)]).unwrap(),
        DirectoryLimits {
            operations: 3,
            history_bytes: 100000,
        },
    )
    .unwrap()
    .with_group_creation()
    .unwrap_or_else(|_| panic!("schema2"));
    assert_eq!(
        schema2.restore_checkpoint(3, index, &image),
        Err(ApplicationError::UnsupportedSchema)
    );
}
#[test]
fn namespace_codecs_and_bindings_reject_wrong_modes_parents_and_ready_facts() {
    let (mut d, p) = reserve(16);
    let bytes = p.encode(MAX_NAMESPACE_PLAN_BYTES).unwrap();
    assert_eq!(p.encode(bytes.len()).unwrap(), bytes);
    assert_eq!(NamespacePlan::decode(&bytes).unwrap(), p);
    for end in 0..bytes.len() {
        assert!(NamespacePlan::decode(&bytes[..end]).is_err());
    }
    assert!(NamespacePublication::from_status(&p, target(&p).status()).is_err());
    let mut t = target(&p);
    let original = ready(&mut t);
    let b = original.encode(1024).unwrap();
    assert_eq!(NamespacePublication::decode(&b).unwrap(), original);
    for end in 0..b.len() {
        assert!(NamespacePublication::decode(&b[..end]).is_err());
    }
    for change in 0..5 {
        let mut forged = original.clone();
        match change {
            0 => forged.creation_index += 1,
            1 => forged.creation = OperationId::new(90).unwrap(),
            2 => forged.configuration = ConfigurationId::new(2).unwrap(),
            3 => forged.plan.0[0] ^= 1,
            _ => {
                let mut m = forged.manifest.clone().into_input();
                m.parent = Some(ParentAuthority {
                    responsibility: identity_of(1),
                    group: group(1),
                });
                forged.manifest = ResponsibilityManifest::new(m).unwrap();
            }
        }
        assert_eq!(
            commit(&mut d, 20 + change, forged.encode(1024).unwrap()).outcome,
            DirectoryOutcome::TransferEvidenceMismatch
        );
        assert!(d.manifest(identity_of(50)).is_none());
    }
    let mut staging = p.clone();
    staging.creation.intent.mode = GroupCreationMode::Staging;
    assert!(staging.validate().is_err());
    let mut other = p.clone();
    other.creation.intent.application.version = 2;
    assert!(other.validate().is_err());
    assert_eq!(
        commit(&mut d, 99, b.clone()).outcome,
        DirectoryOutcome::NamespacePublished(RouteGeneration::new(1).unwrap())
    );
    assert_eq!(
        commit(&mut d, 100, b).outcome,
        DirectoryOutcome::TransferEvidenceMismatch
    );
}
#[test]
fn target_never_serves_before_exact_activation_and_preserves_retries_on_restore() {
    let (mut d, p) = reserve(16);
    let mut t = target(&p);
    let (op, bytes) = data(10, 7);
    assert!(t.validate_proposal(op, &bytes, std::iter::empty()).is_err());
    assert_eq!(t.read_at(0, query()).unwrap(), NamespaceRead::NotActive);
    assert_eq!(
        commit(&mut t, 10, bytes.clone()).outcome,
        NamespaceOutcome::NotActive
    );
    let pubn = ready(&mut t);
    assert_eq!(
        t.read_at(t.applied_index(), query()).unwrap(),
        NamespaceRead::NotActive
    );
    let image = t.checkpoint(1000000).unwrap();
    let mut reopened = target(&p);
    reopened
        .restore_checkpoint(1, t.applied_index(), &image)
        .unwrap();
    assert_eq!(reopened.status(), t.status());
    assert!(reopened
        .validate_proposal(op, &bytes, std::iter::empty())
        .is_err());
    assert_eq!(
        commit(&mut d, 4, pubn.encode(1024).unwrap()).outcome,
        DirectoryOutcome::NamespacePublished(RouteGeneration::new(1).unwrap())
    );
    let status = d
        .namespace_publication_at(d.applied_index(), p.creation.operation)
        .unwrap()
        .unwrap();
    let mut bad = status.clone();
    bad.publication.ready_index += 1;
    assert!(reopened.activation_command(&bad, 2000).is_err());
    let activation = reopened.activation_command(&status, 2000).unwrap();
    assert_eq!(
        commit(&mut reopened, 4, activation.clone()).outcome,
        NamespaceOutcome::Activated
    );
    assert_eq!(
        commit(&mut reopened, 10, bytes.clone()).outcome,
        NamespaceOutcome::Data(RoutedReceipt {
            index: 4,
            operation: op,
            outcome: RoutedOutcome::Applied(CounterReceipt {
                index: 4,
                operation: op,
                outcome: CounterOutcome::Value(7),
                duplicate: false
            })
        })
    );
    let index = reopened.applied_index();
    let image = reopened.checkpoint(1000000).unwrap();
    let binding_len = u32::from_le_bytes(image[16..20].try_into().unwrap()) as usize;
    let ready_offset = 20 + binding_len;
    let mut corrupt_ready = image.clone();
    corrupt_ready[ready_offset..ready_offset + 8].copy_from_slice(&3u64.to_le_bytes());
    assert!(target(&p)
        .restore_checkpoint(1, index, &corrupt_ready)
        .is_err());
    let mut corrupt_activation = image.clone();
    corrupt_activation[ready_offset + 9..ready_offset + 17].copy_from_slice(&2u64.to_le_bytes());
    assert!(target(&p)
        .restore_checkpoint(1, index, &corrupt_activation)
        .is_err());
    let mut colliding_operation = image.clone();
    colliding_operation[ready_offset + 17..ready_offset + 33]
        .copy_from_slice(&10u128.to_le_bytes());
    assert!(target(&p)
        .restore_checkpoint(1, index, &colliding_operation)
        .is_err());
    let mut recovered = target(&p);
    recovered.restore_checkpoint(1, index, &image).unwrap();
    assert_eq!(
        recovered.read_at(index, query()).unwrap(),
        NamespaceRead::Data(RoutedRead::Served(7))
    );
    let r = commit(&mut recovered, 10, bytes);
    assert!(matches!(
        r.outcome,
        NamespaceOutcome::Data(RoutedReceipt {
            outcome: RoutedOutcome::Applied(CounterReceipt {
                duplicate: true,
                outcome: CounterOutcome::Value(7),
                ..
            }),
            ..
        })
    ));
    assert_eq!(
        commit(&mut recovered, 4, activation).outcome,
        NamespaceOutcome::Activated
    );
    let (id, b) = data(4, 9);
    assert!(recovered
        .validate_proposal(id, &b, std::iter::empty())
        .is_err());
    assert_eq!(
        commit(&mut recovered, 4, b).outcome,
        NamespaceOutcome::OperationConflict
    );
    assert_eq!(
        recovered
            .read_at(recovered.applied_index(), query())
            .unwrap(),
        NamespaceRead::Data(RoutedRead::Served(7))
    );
    for end in 0..image.len() {
        let mut fresh = target(&p);
        assert!(fresh.restore_checkpoint(1, index, &image[..end]).is_err());
        assert_eq!(fresh.applied_index(), 0);
    }
}

#[cfg(feature = "native")]
#[test]
fn every_torn_native_activation_frame_recovers_nonserving_or_exact_activation() {
    use voteboat::{native::log_store::*, raft::Raft};
    let (mut d, p) = reserve(16);
    let mut ready_app = target(&p);
    let init = ready_app.initialization_command(100000).unwrap();
    let pubn = ready(&mut ready_app);
    commit(&mut d, 4, pubn.encode(1024).unwrap());
    let status = d
        .namespace_publication_at(d.applied_index(), p.creation.operation)
        .unwrap()
        .unwrap();
    let activation = ready_app.activation_command(&status, 2000).unwrap();
    let seed = || {
        let io = ModelIo::default();
        let mut log =
            NativeLogStore::create(io.clone(), identity(1), LogLimits::default()).unwrap();
        append(
            &mut log,
            vec![LogMutation::Create(p.creation.intent.bootstrap.clone())],
        );
        let state = log.state(group(100)).unwrap();
        append(
            &mut log,
            vec![update(
                &state,
                1,
                1,
                Some(Suffix {
                    from: 1,
                    entries: vec![entry(1, 3, init.clone())],
                }),
            )],
        );
        (io, log)
    };
    let (_, log) = seed();
    let state = log.state(group(100)).unwrap();
    let mutation = update(
        &state,
        1,
        2,
        Some(Suffix {
            from: 2,
            entries: vec![entry(2, 4, activation.clone())],
        }),
    );
    let frame = NativeLogCodec
        .encode_batch(3, std::slice::from_ref(&mutation), LogLimits::default())
        .unwrap();
    let mut old = false;
    let mut complete = false;
    for fault in (0..=frame.len()).map(Fault::Append).chain([
        Fault::Sync,
        Fault::PublishBefore,
        Fault::PublishAfter,
    ]) {
        let (io, mut log) = seed();
        io.0.borrow_mut().fault = fault;
        if let Ok(tickets) = log.append_batch(vec![mutation.clone()]) {
            assert!(log.barrier(&tickets).is_err());
        }
        drop(log);
        io.0.borrow_mut().power_loss();
        let mut log = NativeLogStore::recover(io, identity(1), LogLimits::default()).unwrap();
        let core = Raft::recover(
            node(1),
            log.binding(),
            log.state(group(100)).unwrap(),
            log.limits(),
        )
        .unwrap();
        let mut app = target(&p);
        app.apply_batch(core.replay_committed()).unwrap();
        match core.state().commit_index {
            1 => {
                old = true;
                assert!(app.status().activation_index.is_none());
                assert_eq!(app.read_at(1, query()).unwrap(), NamespaceRead::NotActive);
            }
            2 => {
                complete = true;
                assert_eq!(app.status().activation_index, Some(2));
                assert_eq!(
                    app.read_at(2, query()).unwrap(),
                    NamespaceRead::Data(RoutedRead::Served(0))
                );
            }
            _ => panic!("activation recovery must be old or complete"),
        }
        let state = log.state(group(100)).unwrap();
        let index = state.last_index() + 1;
        append(
            &mut log,
            vec![update(
                &state,
                1,
                index,
                Some(Suffix {
                    from: index,
                    entries: vec![entry(index, 4, activation.clone())],
                }),
            )],
        );
        let core = Raft::recover(
            node(1),
            log.binding(),
            log.state(group(100)).unwrap(),
            log.limits(),
        )
        .unwrap();
        let mut retried = target(&p);
        retried.apply_batch(core.replay_committed()).unwrap();
        assert_eq!(retried.status().activation_index, Some(2));
        assert_eq!(
            retried.read_at(index, query()).unwrap(),
            NamespaceRead::Data(RoutedRead::Served(0))
        );
    }
    assert!(old && complete);
}

#[test]
fn creation_deployment_envelope_covers_large_valid_recursive_policy_and_store_map() {
    use voteboat::quorum::{Limits, Policy, Tree};
    let (_, p) = reserve(16);
    let mut intent = p.creation.intent;
    intent.bootstrap.policy = Policy::new(
        Tree::Majority((1..=4096).map(|n| Tree::Voter(node(n))).collect()),
        Limits::default(),
    )
    .unwrap();
    intent.bootstrap.voter_stores = (1..=4096).map(|n| (node(n), identity(n.into()))).collect();
    let encoded = intent.encode(MAX_GROUP_CREATION_BYTES).unwrap();
    assert!(encoded.len() > MAX_DIRECTORY_CONTROL_BYTES);
    assert!(directory(16).readiness_requirements().command_bytes >= encoded.len());
}
