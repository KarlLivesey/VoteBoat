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
use voteboat::{delegation::*, scope::ScopeImage};
fn configuration() -> ConfigurationId {
    ConfigurationId::new(1).unwrap()
}
fn directory() -> Directory {
    super::directory()
        .with_recursive_insertion()
        .unwrap_or_else(|_| panic!("schema6"))
}
pub(super) fn source_hint(m: &ResponsibilityManifest, g: u128, key: u8) -> RouteHint {
    let m = m.input();
    RouteHint {
        responsibility: m.responsibility,
        group: group(g),
        application: m.application,
        scheme: m.scheme,
        scope: m.scope,
        bucket: key.into(),
        epoch: m.epoch,
        generation: m.generation,
    }
}
pub(super) fn data_for(m: &ResponsibilityManifest, g: u128, key: u8, delta: i64) -> Vec<u8> {
    let hint = source_hint(m, g, key);
    encode_routed(
        hint,
        &[key],
        &encode_add(&[key], delta, b"effect", 1024).unwrap(),
        4096,
    )
    .unwrap()
}
pub(super) fn target_for(intent: &TransferIntent, g: u128, operation: u128) -> Target {
    Target::new(
        group(g),
        op(operation),
        intent.clone(),
        BucketCounter::new(
            intent.target_manifest(group(g)).unwrap().input().scope,
            Policy,
            bucket_limits(),
        )
        .unwrap(),
        Policy,
        TargetLimits {
            import_bytes: 32768,
            application_checkpoint_bytes: bucket_limits().checkpoint_bound().unwrap(),
        },
    )
    .unwrap_or_else(|e| panic!("{:?}", e.0))
}
pub(super) fn handoff(
    d: &mut Directory,
    intent: &TransferIntent,
    operation: u128,
    status: SourceFreezeStatus,
    export: impl Fn(GroupIdentity) -> ScopeImage,
) -> (Vec<Target>, TransferPublicationStatus) {
    let mut targets = Vec::new();
    let mut facts = Vec::new();
    for route in intent.targets() {
        let RouteTarget::Group(g) = route.target else {
            unreachable!()
        };
        let g = g.id.get();
        let mut t = target_for(intent, g, operation);
        let boot = t.bootstrap_command(100000).unwrap();
        commit(&mut t, operation, boot);
        let import = TargetImport::new(
            op(operation),
            intent.clone(),
            group(g),
            vec![SourceImport {
                fence: status.fence,
                configuration: configuration(),
                image: export(group(g)),
                digest: status
                    .exports
                    .iter()
                    .find(|e| e.target == group(g))
                    .unwrap()
                    .digest,
            }],
        )
        .unwrap_or_else(|e| panic!("{:?}", e.0));
        let bytes = t.import_command(&import, 100000).unwrap();
        commit(&mut t, operation, bytes);
        facts.push(
            TargetReadyEvidence::from_status(configuration(), t.status())
                .unwrap_or_else(|e| panic!("{:?}", e.0)),
        );
        targets.push(t);
    }
    let evidence = SourceFenceEvidence::from_status(configuration(), status)
        .unwrap_or_else(|e| panic!("{:?}", e.0));
    let publication =
        TransferPublication::new(op(operation), intent.clone(), vec![evidence], facts)
            .unwrap_or_else(|e| panic!("{:?}", e.0));
    assert!(matches!(
        commit(d, operation + 1, publication.encode(100000).unwrap()).outcome,
        DirectoryOutcome::TransferPublished(_)
    ));
    let decision = d
        .transfer_publication_at(d.applied_index(), op(operation))
        .unwrap()
        .unwrap();
    (targets, decision)
}
pub(super) fn activate(
    targets: &mut [Target],
    operation: u128,
    decision: TransferPublicationStatus,
) {
    for t in targets {
        let bytes = t
            .activation_command(
                &TargetActivation {
                    metadata_configuration: configuration(),
                    decision: decision.clone(),
                },
                100000,
            )
            .unwrap();
        assert!(matches!(
            commit(t, operation, bytes).outcome,
            TargetOutcome::Activated(_)
        ));
    }
}
fn root() -> (Directory, TransferIntent, Vec<Target>) {
    let (mut d, intent) = setup_in(directory());
    commit(&mut d, 200, intent.encode(100000).unwrap());
    let mut s = ready();
    commit(&mut s, 1, data(1, 7));
    commit(&mut s, 2, data(200, 11));
    commit(
        &mut s,
        200,
        Source::freeze_command(&intent, 100000).unwrap(),
    );
    let SourceRead::Freeze(Some(status)) =
        s.read_at(s.applied_index(), SourceQuery::Freeze).unwrap()
    else {
        panic!("root fence")
    };
    let (mut targets, decision) = handoff(&mut d, &intent, 200, status, |g| {
        s.export_target(g, 65536).unwrap()
    });
    activate(&mut targets, 200, decision);
    (d, intent, targets)
}
fn plan(d: &mut Directory, root: &TransferIntent) -> DelegationPlan {
    let before = root.insertion_children().unwrap()[0].manifest.clone();
    let mut children = Vec::new();
    for (g, scope) in [(31, range(0, 64)), (32, range(64, 128))] {
        let mut m = before.clone().into_input();
        m.responsibility.id = ResponsibilityId::new(g).unwrap();
        m.parent = Some(ParentAuthority {
            responsibility: before.input().responsibility,
            group: group(1),
        });
        m.scope = scope;
        m.execution = ExecutionMode::Single(group(g));
        let m = ResponsibilityManifest::new(m).unwrap();
        let reservation = GroupCreationIntent {
            authority: group(1),
            parent: before.input().responsibility,
            expected: before.input().generation,
            responsibility: m.input().responsibility,
            bootstrap: support::bootstrap(g, 3),
            application: m.input().application,
            mode: GroupCreationMode::Staging,
        };
        assert_eq!(
            commit(d, g * 10, reservation.encode(100000).unwrap()).outcome,
            DirectoryOutcome::CreationReserved
        );
        children.push(
            InsertionChild::from_creation(
                m,
                &d.group_creation_at(d.applied_index(), group(g))
                    .unwrap()
                    .unwrap(),
            )
            .unwrap(),
        );
    }
    let mut after = before.clone().into_input();
    after.epoch = OwnershipEpoch::new(2).unwrap();
    after.generation = RouteGeneration::new(2).unwrap();
    after.execution = ExecutionMode::Delegated(
        children
            .iter()
            .map(|c| RouteEntry {
                scope: c.manifest.input().scope,
                target: RouteTarget::Child(ChildAuthority {
                    responsibility: c.manifest.input().responsibility,
                    group: group(1),
                    epoch: c.manifest.input().epoch,
                }),
            })
            .collect(),
    );
    DelegationPlan::insertion(
        root.after().clone(),
        before,
        ResponsibilityManifest::new(after).unwrap(),
        children,
        op(300),
    )
    .unwrap()
}
fn reserved(
    d: &mut Directory,
    plan: &DelegationPlan,
) -> (DelegationReservationStatus, TransferIntent) {
    assert_eq!(
        commit(d, 400, plan.encode(100000).unwrap()).outcome,
        DirectoryOutcome::DelegationReserved
    );
    let s = d
        .delegation_reservation_at(d.applied_index(), op(400))
        .unwrap()
        .unwrap();
    let intent = s.child_intent(configuration()).unwrap();
    (s, intent)
}
#[test]
fn nested_mapping_codec_parent_authorization_and_legacy_replay_fail_closed() {
    let (mut d, root, _) = root();
    let plan = plan(&mut d, &root);
    let bytes = plan.encode(100000).unwrap();
    assert_eq!(DelegationPlan::decode(&bytes).unwrap(), plan);
    for end in 0..bytes.len() {
        assert!(DelegationPlan::decode(&bytes[..end]).is_err());
    }
    assert!(TransferIntent::insert_children(
        plan.before().clone(),
        plan.after().clone(),
        plan.insertion_children().unwrap().to_vec()
    )
    .is_err());
    let mut wrong = plan.insertion_children().unwrap().to_vec();
    wrong[0].creation_index += 1;
    let wrong = DelegationPlan::insertion(
        plan.parent().clone(),
        plan.before().clone(),
        plan.after().clone(),
        wrong,
        op(300),
    )
    .unwrap();
    assert_eq!(
        commit(&mut d, 399, wrong.encode(100000).unwrap()).outcome,
        DirectoryOutcome::TransferEvidenceMismatch
    );
    let (status, intent) = reserved(&mut d, &plan);
    let bytes = intent.encode(100000).unwrap();
    assert_eq!(&bytes[..8], b"VBTINT04");
    assert_eq!(TransferIntent::decode(&bytes).unwrap(), intent);
    for end in 0..bytes.len() {
        assert!(TransferIntent::decode(&bytes[..end]).is_err());
    }
    let before_len = u32::from_le_bytes(bytes[8..12].try_into().unwrap()) as usize;
    let after_pos = 12 + before_len;
    let after_len =
        u32::from_le_bytes(bytes[after_pos..after_pos + 4].try_into().unwrap()) as usize;
    for offset in [0, 16, 24] {
        let mut tampered = bytes.clone();
        tampered[after_pos + 4 + after_len + 120 + 2 + offset] ^= 2;
        assert!(TransferIntent::decode(&tampered).is_err());
    }
    assert!(intent.encode(bytes.len() - 1).is_err());
    let view = LifecycleDirectory::new(d.clone());
    let query = DirectoryQuery::DelegationReservation(op(400));
    let result = view.read_at(d.applied_index(), query).unwrap();
    let nested = view.read_result_bytes(&result, 200000).unwrap();
    assert_eq!(
        view.read_result_bound(&query).unwrap(),
        std::mem::size_of_val(&result) + nested
    );
    assert!(view.read_result_bytes(&result, nested - 1).is_err());
    let mut wrong_status = status.clone();
    wrong_status.operation = op(310);
    assert!(wrong_status.child_intent(configuration()).is_err());
    let fake = DelegationReservationStatus {
        plan: wrong,
        ..status.clone()
    }
    .child_intent(configuration())
    .unwrap();
    assert_eq!(
        commit(&mut d, 300, fake.encode(100000).unwrap()).outcome,
        DirectoryOutcome::TransferEvidenceMismatch
    );
    // A refusal is bound to the original ID forever; a fresh branch retains the valid intent.
    let mut branch = d.clone();
    let decline = DelegationDecline::new(intent.clone()).unwrap();
    assert_eq!(
        commit(&mut branch, 401, decline.encode(100000).unwrap()).outcome,
        DirectoryOutcome::DelegationDeclined
    );
    let decline = branch
        .delegation_decline_at(branch.applied_index(), op(300))
        .unwrap()
        .unwrap();
    let cancel = DelegationCancellation {
        reservation: op(400),
        reservation_index: status.index,
        parent_configuration: configuration(),
        child_configuration: configuration(),
        decline,
    };
    assert_eq!(
        commit(&mut branch, 402, cancel.encode(100000).unwrap()).outcome,
        DirectoryOutcome::DelegationCancelled
    );
    let cp = branch.checkpoint(200000).unwrap();
    let mut restored = directory();
    restored
        .restore_checkpoint(6, branch.applied_index(), &cp)
        .unwrap();
    assert_eq!(
        restored.manifest(root.before().input().responsibility),
        Some(root.after())
    );
    // Exact new schema selection; earlier modes neither parse nor replay the new kind.
    let (mut legacy, _) = setup();
    let cp = legacy.checkpoint(200000).unwrap();
    let before = legacy.applied_index();
    assert!(legacy
        .apply_batch(&[entry(before + 1, 400, plan.encode(100000).unwrap())])
        .is_err());
    assert_eq!(legacy.checkpoint(200000).unwrap(), cp);
    assert!(legacy
        .restore_checkpoint(
            6,
            branch.applied_index(),
            &branch.checkpoint(200000).unwrap()
        )
        .is_err());
}
#[test]
fn nested_insert_handoff_preserves_lineage_parent_refresh_and_dynamic_parent_lifecycle() {
    let (mut d, root, mut owners) = root();
    let plan = plan(&mut d, &root);
    let (reservation, intent) = reserved(&mut d, &plan);
    // Distinct valid branch without the previous test's retained failed operation.
    assert_eq!(
        commit(&mut d, 300, intent.encode(100000).unwrap()).outcome,
        DirectoryOutcome::TransferIntentRecorded
    );
    let decline = DelegationDecline::new(intent.clone()).unwrap();
    assert_eq!(
        commit(&mut d, 401, decline.encode(100000).unwrap()).outcome,
        DirectoryOutcome::LifecycleBusy
    );
    assert!(d
        .delegation_decline_at(d.applied_index(), op(300))
        .unwrap()
        .is_none());
    let before = intent.before();
    assert!(matches!(
        commit(&mut owners[0], 80, data_for(before, 21, 80, 5)).outcome,
        TargetOutcome::Applied(_)
    ));
    let freeze = owners[0].freeze_command(&intent, 65536, 100000).unwrap();
    assert!(matches!(
        commit(&mut owners[0], 300, freeze).outcome,
        TargetOutcome::Frozen(_)
    ));
    let TargetRead::Freeze(Some(status)) = owners[0]
        .read_at(owners[0].applied_index(), TargetQuery::Freeze)
        .unwrap()
    else {
        panic!("child fence")
    };
    assert_eq!(status.fence.responsibility, before.input().responsibility);
    let source_cp = owners[0].checkpoint(100000).unwrap();
    let mut source = super::target(&root, 21);
    source
        .restore_checkpoint(3, owners[0].applied_index(), &source_cp)
        .unwrap();
    let (mut targets, decision) = handoff(&mut d, &intent, 300, status, |g| {
        source.export_target(g, 65536).unwrap()
    });
    // No target is active merely because metadata published the descendants.
    assert!(targets.iter().all(|t| t.status().activated.is_none()));
    let h = source_hint(intent.before(), 21, 1);
    assert_eq!(
        source
            .read_at(
                source.applied_index(),
                TargetQuery::Data(RoutedQuery {
                    hint: h,
                    key: vec![1],
                    query: vec![1]
                })
            )
            .unwrap(),
        TargetRead::Rejected(RoutingError::Fenced)
    );
    for (i, g, key) in [(0, 31, 1), (1, 32, 80)] {
        let hint = source_hint(intent.target_manifest(group(g)).unwrap(), g, key);
        assert_eq!(
            targets[i]
                .read_at(
                    targets[i].applied_index(),
                    TargetQuery::Data(RoutedQuery {
                        hint,
                        key: vec![key],
                        query: vec![key]
                    })
                )
                .unwrap(),
            TargetRead::NotActive
        );
    }
    let completion = DelegationCompletion {
        reservation: op(400),
        reservation_index: reservation.index,
        parent_configuration: configuration(),
        child_configuration: configuration(),
        decision: decision.clone(),
    };
    let cp = d.checkpoint(200000).unwrap();
    let mut restored = directory();
    restored
        .restore_checkpoint(6, d.applied_index(), &cp)
        .unwrap();
    assert_eq!(
        restored.manifest(before.input().responsibility),
        Some(intent.after())
    );
    let mut exhausted = restored.clone();
    let mut id = 1000;
    while exhausted.remaining_operations() > 0 {
        commit(
            &mut exhausted,
            id,
            DirectoryCommand {
                expected: None,
                manifest: grant(),
            }
            .encode(100000)
            .unwrap(),
        );
        id += 1;
    }
    assert!(matches!(
        commit(&mut exhausted, 403, completion.encode(100000).unwrap()).outcome,
        DirectoryOutcome::DelegationPublished(_)
    ));
    let before_cp = restored.checkpoint(200000).unwrap();
    assert_eq!(
        restored.apply_batch(&[
            entry(
                restored.applied_index() + 1,
                403,
                completion.encode(100000).unwrap()
            ),
            noop(restored.applied_index() + 3)
        ]),
        Err(ApplicationError::IndexGap)
    );
    assert_eq!(restored.checkpoint(200000).unwrap(), before_cp);
    assert_eq!(
        commit(&mut restored, 403, completion.encode(100000).unwrap()).outcome,
        DirectoryOutcome::DelegationPublished(RouteGeneration::new(3).unwrap())
    );
    #[cfg(feature = "native")]
    {
        let mut cache = voteboat::native::routing::NativeManifestCache::new(ManifestCacheLimits {
            manifests: 5,
            bytes: 65536,
        })
        .unwrap();
        cache
            .admit(
                restored
                    .manifest(root.before().input().responsibility)
                    .unwrap()
                    .clone(),
            )
            .unwrap();
        cache.admit(intent.after().clone()).unwrap();
        cache
            .admit(root.insertion_children().unwrap()[1].manifest.clone())
            .unwrap();
        for c in intent.insertion_children().unwrap() {
            cache.admit(c.manifest.clone()).unwrap();
        }
        for (g, key) in [(31, 1), (32, 80)] {
            assert_eq!(
                resolve(
                    &cache,
                    &Policy,
                    root.before().input().responsibility,
                    &[key],
                    4
                )
                .unwrap(),
                source_hint(intent.target_manifest(group(g)).unwrap(), g, key)
            );
        }
    }
    activate(&mut targets, 300, decision);
    for (i, g, key, old_op, value) in [(0, 31, 1, 1, 7), (1, 32, 80, 80, 5)] {
        let m = intent.target_manifest(group(g)).unwrap();
        let r = commit(&mut targets[i], old_op, data_for(m, g, key, value));
        assert!(
            matches!(r.outcome,TargetOutcome::Applied(v) if v.duplicate&&v.outcome==BucketOutcome::Value(value))
        );
        assert!(matches!(
            commit(&mut targets[i], 4, data_for(m, g, key, 2)).outcome,
            TargetOutcome::Applied(_)
        ));
        assert_eq!(targets[i].application().value(&[key]), Ok(value + 2));
        assert_eq!(targets[i].application().outbox().count(), 2);
        let cp = targets[i].checkpoint(100000).unwrap();
        let mut fresh = target_for(&intent, g, 300);
        fresh
            .restore_checkpoint(4, targets[i].applied_index(), &cp)
            .unwrap();
        assert!(fresh
            .restore_checkpoint(3, targets[i].applied_index(), &cp)
            .is_err());
    }
    let child = intent.target_manifest(group(31)).unwrap().clone();
    let mut after = child.clone().into_input();
    after.epoch = OwnershipEpoch::new(2).unwrap();
    after.generation = RouteGeneration::new(2).unwrap();
    after.execution = ExecutionMode::Partitioned(vec![
        RouteEntry {
            scope: range(0, 32),
            target: RouteTarget::Group(group(41)),
        },
        RouteEntry {
            scope: range(32, 64),
            target: RouteTarget::Group(group(42)),
        },
    ]);
    let later = DelegationPlan::new(
        intent.after().clone(),
        child.clone(),
        ResponsibilityManifest::new(after).unwrap(),
        op(501),
    )
    .unwrap();
    assert_eq!(
        commit(&mut restored, 500, later.encode(100000).unwrap()).outcome,
        DirectoryOutcome::DelegationReserved
    );
    let next = restored
        .delegation_reservation_at(restored.applied_index(), op(500))
        .unwrap()
        .unwrap()
        .child_intent(configuration())
        .unwrap();
    assert_eq!(
        commit(&mut restored, 501, next.encode(100000).unwrap()).outcome,
        DirectoryOutcome::TransferIntentRecorded
    );
    let bytes = targets[0].freeze_command(&next, 65536, 100000).unwrap();
    assert!(
        matches!(commit(&mut targets[0],501,bytes).outcome,TargetOutcome::Frozen(f) if f.responsibility==child.input().responsibility)
    );
    let stable = restored.checkpoint(200000).unwrap();
    for end in 0..stable.len() {
        let mut fresh = directory();
        let old = fresh.checkpoint(200000).unwrap();
        assert!(fresh
            .restore_checkpoint(6, restored.applied_index(), &stable[..end])
            .is_err());
        assert_eq!(fresh.checkpoint(200000).unwrap(), old);
    }
}

#[test]
fn nested_insertion_checks_remaining_hops_before_reserving_parent() {
    for depth in [MAX_ROUTE_HOPS - 1, MAX_ROUTE_HOPS] {
        let mut manifests = Vec::new();
        for i in 0..depth {
            let mut m = grant().into_input();
            m.responsibility.id = ResponsibilityId::new(1000 + i as u128).unwrap();
            m.parent = if i == 0 {
                None
            } else {
                Some(ParentAuthority {
                    responsibility: ResponsibilityIdentity {
                        id: ResponsibilityId::new(999 + i as u128).unwrap(),
                        incarnation: ResponsibilityIncarnation::new(1).unwrap(),
                    },
                    group: group(1),
                })
            };
            if i + 1 < depth {
                m.execution = ExecutionMode::Delegated(vec![RouteEntry {
                    scope: m.scope,
                    target: RouteTarget::Child(ChildAuthority {
                        responsibility: ResponsibilityIdentity {
                            id: ResponsibilityId::new(1001 + i as u128).unwrap(),
                            incarnation: ResponsibilityIncarnation::new(1).unwrap(),
                        },
                        group: group(1),
                        epoch: m.epoch,
                    }),
                }]);
            }
            manifests.push(ResponsibilityManifest::new(m).unwrap());
        }
        let mut d = Directory::new(
            DirectoryPlan::new(group(1), manifests.clone()).unwrap(),
            DirectoryLimits {
                operations: 64,
                history_bytes: 200000,
            },
        )
        .unwrap()
        .with_recursive_insertion()
        .unwrap_or_else(|_| panic!("schema6"));
        let boot = d.bootstrap_command(100000).unwrap();
        commit(&mut d, 100, boot);
        for (i, m) in manifests.iter().enumerate() {
            commit(
                &mut d,
                1000 + i as u128,
                DirectoryCommand {
                    expected: None,
                    manifest: m.clone(),
                }
                .encode(100000)
                .unwrap(),
            );
        }
        let before = manifests.last().unwrap();
        let mut m = before.clone().into_input();
        m.responsibility.id = ResponsibilityId::new(2000).unwrap();
        m.parent = Some(ParentAuthority {
            responsibility: before.input().responsibility,
            group: group(1),
        });
        m.execution = ExecutionMode::Single(group(31));
        let child = ResponsibilityManifest::new(m).unwrap();
        let creation = GroupCreationIntent {
            authority: group(1),
            parent: before.input().responsibility,
            expected: before.input().generation,
            responsibility: child.input().responsibility,
            bootstrap: support::bootstrap(31, 3),
            application: child.input().application,
            mode: GroupCreationMode::Staging,
        };
        assert_eq!(
            commit(&mut d, 310, creation.encode(100000).unwrap()).outcome,
            DirectoryOutcome::CreationReserved
        );
        let child = InsertionChild::from_creation(
            child,
            &d.group_creation_at(d.applied_index(), group(31))
                .unwrap()
                .unwrap(),
        )
        .unwrap();
        let mut after = before.clone().into_input();
        after.epoch = OwnershipEpoch::new(2).unwrap();
        after.generation = RouteGeneration::new(2).unwrap();
        after.execution = ExecutionMode::Delegated(vec![RouteEntry {
            scope: after.scope,
            target: RouteTarget::Child(ChildAuthority {
                responsibility: child.manifest.input().responsibility,
                group: group(1),
                epoch: child.manifest.input().epoch,
            }),
        }]);
        let plan = DelegationPlan::insertion(
            manifests[depth - 2].clone(),
            before.clone(),
            ResponsibilityManifest::new(after).unwrap(),
            vec![child],
            op(300),
        )
        .unwrap();
        let outcome = commit(&mut d, 400, plan.encode(100000).unwrap()).outcome;
        assert_eq!(
            outcome,
            if depth < MAX_ROUTE_HOPS {
                DirectoryOutcome::DelegationReserved
            } else {
                DirectoryOutcome::TransferEvidenceMismatch
            }
        );
    }
}

#[path = "nested_continuity.rs"]
mod continuity;
