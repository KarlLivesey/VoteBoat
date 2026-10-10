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
fn rid(n: u128) -> ResponsibilityIdentity {
    ResponsibilityIdentity {
        id: ResponsibilityId::new(n).unwrap(),
        incarnation: ResponsibilityIncarnation::new(1).unwrap(),
    }
}
fn tree() -> [ResponsibilityManifest; 3] {
    let mut root = grant().into_input();
    root.parent = Some(ParentAuthority {
        responsibility: rid(50),
        group: group(100),
    });
    root.execution = ExecutionMode::Delegated(vec![
        RouteEntry {
            scope: range(0, 128),
            target: RouteTarget::Child(ChildAuthority {
                responsibility: rid(60),
                group: group(200),
                epoch: root.epoch,
            }),
        },
        RouteEntry {
            scope: range(128, 256),
            target: RouteTarget::Group(group(20)),
        },
    ]);
    let mut parent = grant().into_input();
    parent.responsibility = rid(50);
    parent.authority = group(100);
    parent.execution = ExecutionMode::Delegated(vec![RouteEntry {
        scope: range(0, 256),
        target: RouteTarget::Child(ChildAuthority {
            responsibility: rid(10),
            group: group(1),
            epoch: root.epoch,
        }),
    }]);
    let mut child = grant().into_input();
    child.responsibility = rid(60);
    child.authority = group(200);
    child.parent = Some(ParentAuthority {
        responsibility: rid(10),
        group: group(1),
    });
    child.scope = range(0, 128);
    child.execution = ExecutionMode::Single(group(21));
    [parent, root, child].map(|m| ResponsibilityManifest::new(m).unwrap())
}
fn fresh_directory(manifest: &ResponsibilityManifest, operations: usize) -> Directory {
    Directory::new(
        DirectoryPlan::new(manifest.input().authority, vec![manifest.clone()]).unwrap(),
        DirectoryLimits {
            operations,
            history_bytes: 100000,
        },
    )
    .unwrap()
    .with_metadata_locator_updates()
    .unwrap_or_else(|_| panic!("profile"))
}
fn publishing(manifests: &[ResponsibilityManifest]) -> MetadataPublishingSource {
    let d = Directory::new(
        DirectoryPlan::new(group(1), manifests.to_vec()).unwrap(),
        DirectoryLimits {
            operations: 8,
            history_bytes: 100000,
        },
    )
    .unwrap()
    .with_metadata_locator_updates()
    .unwrap_or_else(|_| panic!("profile"));
    let limit = d.readiness_requirements().snapshot_bytes;
    MetadataPublishingSource::new(
        MetadataAuthoritySource::new(LifecycleDirectory::new(d), limit)
            .unwrap_or_else(|_| panic!("source")),
    )
    .unwrap()
}
struct Moved {
    tree: [ResponsibilityManifest; 3],
    plan: MetadataMovePlan,
    activation: MetadataActivationStatus,
}
fn move_manifests(
    manifests: &[ResponsibilityManifest],
) -> (MetadataMovePlan, MetadataActivationStatus) {
    let mut s = publishing(manifests);
    let boot = s.bootstrap_command(100000).unwrap();
    s.apply_batch(&[entry(1, 1000, boot)]).unwrap();
    for (i, m) in manifests.iter().enumerate() {
        s.apply_batch(&[entry(
            i as u64 + 2,
            1001 + i as u128,
            DirectoryCommand {
                expected: None,
                manifest: m.clone(),
            }
            .encode(100000)
            .unwrap(),
        )])
        .unwrap();
    }
    let plan = s.source().plan(group(9)).unwrap();
    let b = s.source().freeze_command(&plan, 100000).unwrap();
    s.apply_batch(&[entry(s.applied_index() + 1, 7, b)])
        .unwrap();
    let image = s.source().export(100000).unwrap();
    let mut t = MetadataServingTarget::new(
        publishing(manifests),
        plan.clone(),
        op(7),
        ConfigurationId::new(1).unwrap(),
        ConfigurationId::new(2).unwrap(),
    )
    .unwrap();
    let b = t.bootstrap_command(100000).unwrap();
    activation::target_apply(&mut t, 7, b);
    let b = t
        .import_command(&image, ConfigurationId::new(1).unwrap(), 100000)
        .unwrap();
    activation::target_apply(&mut t, 7, b);
    let (p, _) = activation::publication(&mut s, &t);
    let b = t.activation_command(p, 100000).unwrap();
    activation::target_apply(&mut t, 7, b);
    (plan, t.status().activation.unwrap())
}
fn moved() -> Moved {
    let tree = tree();
    let (plan, activation) = move_manifests(std::slice::from_ref(&tree[1]));
    Moved {
        tree,
        plan,
        activation,
    }
}
fn update(m: &Moved, index: usize) -> MetadataLocatorUpdate {
    MetadataLocatorUpdate::new(m.tree[index].clone(), m.plan.clone(), m.activation).unwrap()
}
fn seeded(m: &ResponsibilityManifest, operations: usize) -> (Directory, Vec<LogEntry>) {
    let mut d = fresh_directory(m, operations);
    let entries = vec![
        entry(1, 1000, d.bootstrap_command(100000).unwrap()),
        entry(
            2,
            1001,
            DirectoryCommand {
                expected: None,
                manifest: m.clone(),
            }
            .encode(100000)
            .unwrap(),
        ),
    ];
    d.apply_batch(&entries).unwrap();
    (d, entries)
}
fn apply(d: &mut Directory, opid: u128, bytes: Vec<u8>) -> DirectoryReceipt {
    d.apply_batch(&[entry(d.applied_index() + 1, opid, bytes)])
        .unwrap()
        .remove(0)
}
#[test]
fn foreign_parent_and_child_commit_reserved_updates_with_original_status_and_replay() {
    let moved = moved();
    assert_eq!(
        moved
            .activation
            .publication
            .imported
            .source
            .directory_schema,
        15
    );
    for index in [0, 2] {
        let u = update(&moved, index);
        let bytes = u.encode(MAX_METADATA_LOCATOR_BYTES).unwrap();
        assert_eq!(MetadataLocatorUpdate::decode(&bytes).unwrap(), u);
        let (mut d, _) = seeded(u.before(), 2);
        assert_eq!(d.remaining_operations(), 0);
        let binding = d.bootstrap_command(100000).unwrap();
        assert!(binding.starts_with(b"VBDINI15"));
        assert!(d
            .validate_proposal(op(300), &bytes, std::iter::empty())
            .is_ok());
        let receipt = apply(&mut d, 300, bytes.clone());
        assert_eq!(
            receipt.outcome,
            DirectoryOutcome::MetadataLocatorUpdated(RouteGeneration::new(2).unwrap())
        );
        assert_eq!(d.remaining_operations(), 0);
        assert_eq!(
            d.manifest(u.before().input().responsibility),
            Some(&u.after())
        );
        assert_eq!(d.bootstrap_command(100000).unwrap(), binding);
        let status = d
            .metadata_locator_at(d.applied_index(), op(300))
            .unwrap()
            .unwrap();
        assert_eq!(status.index, 3);
        assert_eq!(status.activation, moved.activation);
        assert_eq!(status.command_digest, ContentDigest::sha256(&bytes));
        assert!(d
            .metadata_locator_at(d.applied_index() + 1, op(300))
            .is_err());
        let retry = apply(&mut d, 300, bytes.clone());
        assert!(retry.duplicate);
        assert_eq!(retry.outcome, receipt.outcome);
        let mut a = moved.activation;
        a.publication.target_configuration = ConfigurationId::new(5).unwrap();
        let different = MetadataLocatorUpdate::new(u.before().clone(), moved.plan.clone(), a)
            .unwrap()
            .encode(MAX_METADATA_LOCATOR_BYTES)
            .unwrap();
        assert_eq!(
            apply(&mut d, 300, different).outcome,
            DirectoryOutcome::OperationConflict
        );
        let cp = d.checkpoint(200000).unwrap();
        assert!(cp.starts_with(b"VBDIR015"));
        let mut copy = fresh_directory(u.before(), 2);
        copy.restore_checkpoint(15, d.applied_index(), &cp).unwrap();
        assert_eq!(copy.checkpoint(200000).unwrap(), cp);
        assert_eq!(
            copy.metadata_locator_at(copy.applied_index(), op(300))
                .unwrap(),
            Some(status)
        );
        assert_eq!(apply(&mut copy, 300, bytes).outcome, receipt.outcome);
        let view = LifecycleDirectory::new(copy);
        let q = DirectoryQuery::MetadataLocator(op(300));
        assert_eq!(
            view.read_at(view.applied_index(), q).unwrap(),
            DirectoryRead::MetadataLocator(Some(status))
        );
        assert_eq!(
            view.read_result_bytes(&DirectoryRead::MetadataLocator(Some(status)), 0)
                .unwrap(),
            0
        );
        assert_eq!(
            view.read_result_bound(&q).unwrap(),
            std::mem::size_of::<DirectoryRead>()
        );
    }
}
#[test]
fn locator_provenance_bounds_stale_views_and_atomic_profiles_are_checked() {
    let m = moved();
    let u = update(&m, 0);
    let bytes = u.encode(MAX_METADATA_LOCATOR_BYTES).unwrap();
    assert!(u.encode(bytes.len() - 1).is_err());
    for n in 0..bytes.len() {
        assert!(MetadataLocatorUpdate::decode(&bytes[..n]).is_err());
    }
    let mut extra = bytes.clone();
    extra.push(0);
    assert!(MetadataLocatorUpdate::decode(&extra).is_err());
    for change in 0..8 {
        let mut before = u.before().clone().into_input();
        let mut activation = m.activation;
        match change {
            0 => activation.publication.imported.source.source = group(2),
            1 => activation.publication.imported.source.plan_digest.0[0] ^= 1,
            2 => activation.index = activation.publication.imported.index,
            3 => before.authority = group(1),
            4 => before.generation = RouteGeneration::new(u64::MAX).unwrap(),
            5 => before.state = ResponsibilityState::Fenced,
            6 => {
                if let ExecutionMode::Delegated(routes) = &mut before.execution {
                    if let RouteTarget::Child(c) = &mut routes[0].target {
                        c.epoch = OwnershipEpoch::new(2).unwrap();
                    }
                }
            }
            _ => {
                if let ExecutionMode::Delegated(routes) = &mut before.execution {
                    if let RouteTarget::Child(c) = &mut routes[0].target {
                        c.group.incarnation = GroupIncarnation::new(2).unwrap();
                    }
                }
            }
        }
        assert!(
            MetadataLocatorUpdate::new(
                ResponsibilityManifest::new(before).unwrap(),
                m.plan.clone(),
                activation
            )
            .is_err(),
            "change {change}"
        );
    }
    assert!(MetadataLocatorUpdate::new(grant(), m.plan.clone(), m.activation).is_err());
    let (mut d, _) = seeded(u.before(), 8);
    let original = d.checkpoint(200000).unwrap();
    assert!(d
        .apply_batch(&[entry(3, 300, bytes.clone()), entry(4, 301, b"bad".to_vec())])
        .is_err());
    assert_eq!(d.checkpoint(200000).unwrap(), original);
    let mut copy = fresh_directory(u.before(), 8);
    for n in 0..original.len() {
        assert!(copy.restore_checkpoint(15, 2, &original[..n]).is_err());
    }
    assert_eq!(copy.applied_index(), 0);
    let pending = [(op(300), bytes.as_slice())];
    assert!(d
        .validate_proposal(op(301), &bytes, pending.into_iter())
        .is_err());
    apply(&mut d, 300, bytes.clone());
    assert_eq!(
        apply(&mut d, 301, bytes.clone()).outcome,
        DirectoryOutcome::GenerationMismatch
    );
    assert_eq!(
        d.manifest(u.before().input().responsibility),
        Some(&u.after())
    );
    let mut old = Directory::new(d.plan().clone(), d.limits())
        .unwrap()
        .with_remaining_transfer()
        .unwrap_or_else(|_| panic!("old"));
    let boot = old.bootstrap_command(100000).unwrap();
    apply(&mut old, 1000, boot);
    assert!(old.apply_batch(&[entry(2, 300, bytes)]).is_err());
    assert!(old.metadata_locator_at(1, op(300)).is_err());
    assert!(old
        .restore_checkpoint(15, d.applied_index(), &d.checkpoint(200000).unwrap())
        .is_err());
    assert!(d.with_metadata_locator_updates().is_err());
}
#[test]
fn locator_updates_wait_for_open_creation_without_spending_its_completion_reserve() {
    let m = moved();
    let u = update(&m, 0);
    let (mut d, _) = seeded(u.before(), 8);
    let creation = GroupCreationIntent {
        authority: group(100),
        parent: rid(50),
        expected: RouteGeneration::new(1).unwrap(),
        responsibility: rid(70),
        bootstrap: support::bootstrap(70, 3),
        application: u.before().input().application,
        mode: GroupCreationMode::Staging,
    };
    assert_eq!(
        apply(&mut d, 200, creation.encode(100000).unwrap()).outcome,
        DirectoryOutcome::CreationReserved
    );
    let reserve = d.reserved_publication_bytes();
    assert!(reserve > 0);
    let bytes = u.encode(MAX_METADATA_LOCATOR_BYTES).unwrap();
    assert!(d
        .validate_proposal(op(300), &bytes, std::iter::empty())
        .is_err());
    assert_eq!(
        apply(&mut d, 300, bytes).outcome,
        DirectoryOutcome::LifecycleBusy
    );
    assert_eq!(d.reserved_publication_bytes(), reserve);
    assert_eq!(d.manifest(rid(50)), Some(u.before()));
}
#[cfg(feature = "native")]
#[test]
fn native_cache_requires_complete_foreign_boundary_refreshes() {
    use voteboat::native::routing::NativeManifestCache;
    let m = moved();
    let after = [
        update(&m, 0).after(),
        m.plan.updated_manifests().remove(0),
        update(&m, 2).after(),
    ];
    let limits = ManifestCacheLimits {
        manifests: 8,
        bytes: MAX_CACHE_BYTES,
    };
    for index in [0, 2] {
        let mut cache = NativeManifestCache::new(limits)
            .unwrap()
            .with_metadata_authority_moves()
            .unwrap_or_else(|_| panic!("cache"));
        cache.admit(m.tree[index].clone()).unwrap();
        assert!(cache.admit(after[index].clone()).is_err());
    }
    for order in [[0, 1, 2], [2, 1, 0], [1, 0, 2], [1, 2, 0]] {
        let mut cache = NativeManifestCache::new(limits)
            .unwrap()
            .with_metadata_authority_moves()
            .unwrap_or_else(|_| panic!("cache"))
            .with_metadata_locator_updates()
            .unwrap_or_else(|_| panic!("cache"));
        for before in &m.tree {
            cache.admit(before.clone()).unwrap();
        }
        assert_eq!(
            resolve(&cache, &fixture::Policy, rid(50), &[1], 4)
                .unwrap()
                .group,
            group(21)
        );
        for (position, index) in order.into_iter().enumerate() {
            cache.admit(after[index].clone()).unwrap();
            let result = resolve(&cache, &fixture::Policy, rid(50), &[1], 4);
            if position < 2 {
                assert!(result.is_err());
            } else {
                assert_eq!(result.unwrap().group, group(21));
            }
        }
        assert!(cache.with_metadata_locator_updates().is_err());
    }
    for change in 0..6 {
        let mut b = after[0].clone().into_input();
        match change {
            0 => b.epoch = OwnershipEpoch::new(2).unwrap(),
            1 => b.generation = RouteGeneration::new(3).unwrap(),
            2 => b.authority = group(101),
            3 => b.state = ResponsibilityState::Fenced,
            4 => {
                if let ExecutionMode::Delegated(r) = &mut b.execution {
                    if let RouteTarget::Child(c) = &mut r[0].target {
                        c.responsibility = rid(11);
                    }
                }
            }
            _ => {
                if let ExecutionMode::Delegated(r) = &mut b.execution {
                    if let RouteTarget::Child(c) = &mut r[0].target {
                        c.epoch = OwnershipEpoch::new(2).unwrap();
                    }
                }
            }
        }
        let b = ResponsibilityManifest::new(b).unwrap();
        assert!(!b.refreshes_metadata_locators(&m.tree[0]));
    }
}
#[cfg(feature = "native")]
#[test]
fn native_locator_journal_cuts_recover_old_or_complete_foreign_manifest() {
    use support::{Fault, ModelIo};
    use voteboat::native::log_store::*;
    let m = moved();
    let limits = LogLimits::default();
    for index in [0, 2] {
        let u = update(&m, index);
        let bytes = u.encode(MAX_METADATA_LOCATOR_BYTES).unwrap();
        let (mut expected, mut entries) = seeded(u.before(), 2);
        let status = apply(&mut expected, 300, bytes.clone());
        entries.push(entry(3, 300, bytes.clone()));
        let g = u.before().input().authority;
        let seed = || {
            let io = ModelIo::default();
            let mut log = NativeLogStore::create(io.clone(), support::identity(1), limits).unwrap();
            support::append(
                &mut log,
                vec![LogMutation::Create(support::bootstrap(g.id.get(), 3))],
            );
            let state = log.state(g).unwrap();
            support::append(
                &mut log,
                vec![support::update(
                    &state,
                    1,
                    2,
                    Some(Suffix {
                        from: 1,
                        entries: entries[..2].to_vec(),
                    }),
                )],
            );
            (io, log)
        };
        let (_, log) = seed();
        let mutation = support::update(
            &log.state(g).unwrap(),
            1,
            3,
            Some(Suffix {
                from: 3,
                entries: entries[2..].to_vec(),
            }),
        );
        let frame = NativeLogCodec
            .encode_batch(3, std::slice::from_ref(&mutation), limits)
            .unwrap();
        let mut outcomes = [false; 2];
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
            let log = NativeLogStore::recover(io, support::identity(1), limits).unwrap();
            let state = log.state(g).unwrap();
            let complete = state.commit_index == 3;
            outcomes[usize::from(complete)] = true;
            let mut d = fresh_directory(u.before(), 2);
            d.apply_batch(
                &state
                    .entries
                    .into_iter()
                    .filter(|e| e.index <= state.commit_index)
                    .collect::<Vec<_>>(),
            )
            .unwrap();
            let expected_manifest = if complete {
                u.after()
            } else {
                u.before().clone()
            };
            assert_eq!(
                d.manifest(u.before().input().responsibility),
                Some(&expected_manifest)
            );
            assert_eq!(apply(&mut d, 300, bytes.clone()).outcome, status.outcome);
            assert_eq!(
                d.metadata_locator_at(d.applied_index(), op(300)).unwrap(),
                expected.metadata_locator_at(3, op(300)).unwrap()
            );
        }
        assert_eq!(outcomes, [true, true]);
    }
}

#[test]
fn one_foreign_manifest_refreshes_both_boundaries_atomically() {
    let tree = tree();
    let mut bridge = tree[2].clone().into_input();
    bridge.execution = ExecutionMode::Delegated(vec![
        RouteEntry {
            scope: range(0, 64),
            target: RouteTarget::Child(ChildAuthority {
                responsibility: rid(11),
                group: group(1),
                epoch: bridge.epoch,
            }),
        },
        RouteEntry {
            scope: range(64, 128),
            target: RouteTarget::Group(group(21)),
        },
    ]);
    let bridge = ResponsibilityManifest::new(bridge).unwrap();
    let mut nested = grant().into_input();
    nested.responsibility = rid(11);
    nested.parent = Some(ParentAuthority {
        responsibility: rid(60),
        group: group(200),
    });
    nested.scope = range(0, 64);
    nested.execution = ExecutionMode::Single(group(22));
    let nested = ResponsibilityManifest::new(nested).unwrap();
    let (plan, activation) = move_manifests(&[tree[1].clone(), nested]);
    let u = MetadataLocatorUpdate::new(bridge.clone(), plan.clone(), activation).unwrap();
    let after = u.after();
    assert_eq!(after.input().parent.unwrap().group, group(9));
    let ExecutionMode::Delegated(routes) = &after.input().execution else {
        panic!("routes")
    };
    assert!(matches!(routes[0].target,RouteTarget::Child(c) if c.group==group(9)));
    assert_eq!(routes[1].target, RouteTarget::Group(group(21)));
    assert!(after.refreshes_metadata_locators(&bridge));
    let (mut d, _) = seeded(&bridge, 2);
    apply(&mut d, 300, u.encode(MAX_METADATA_LOCATOR_BYTES).unwrap());
    assert_eq!(d.manifest(rid(60)), Some(&after));
    let mut partial = after.clone().into_input();
    partial.parent = bridge.input().parent;
    assert!(!ResponsibilityManifest::new(partial)
        .unwrap()
        .refreshes_metadata_locators(&bridge));
    let mut inconsistent = after.into_input();
    if let ExecutionMode::Delegated(routes) = &mut inconsistent.execution {
        if let RouteTarget::Child(c) = &mut routes[0].target {
            c.group = group(8);
        }
    }
    assert!(!ResponsibilityManifest::new(inconsistent)
        .unwrap()
        .refreshes_metadata_locators(&bridge));
    let mut bad = bridge.clone().into_input();
    bad.parent.as_mut().unwrap().responsibility = rid(90);
    assert!(MetadataLocatorUpdate::new(
        ResponsibilityManifest::new(bad).unwrap(),
        plan.clone(),
        activation
    )
    .is_err());
    let mut bad = bridge.into_input();
    if let ExecutionMode::Delegated(routes) = &mut bad.execution {
        if let RouteTarget::Child(c) = &mut routes[0].target {
            c.epoch = OwnershipEpoch::new(2).unwrap();
        }
    }
    assert!(MetadataLocatorUpdate::new(
        ResponsibilityManifest::new(bad).unwrap(),
        plan,
        activation
    )
    .is_err());
}
#[test]
fn complete_oversized_move_is_refused_instead_of_truncating_its_plan() {
    let m = moved();
    let mut manifests = m.plan.manifests().to_vec();
    for i in 1..MAX_DIRECTORY_MANIFESTS {
        let mut v = grant().into_input();
        v.responsibility = rid(1000 + i as u128);
        v.execution = ExecutionMode::Partitioned(
            (0..8)
                .map(|n| RouteEntry {
                    scope: range(n * 32, (n + 1) * 32),
                    target: RouteTarget::Group(group(20)),
                })
                .collect(),
        );
        manifests.push(ResponsibilityManifest::new(v).unwrap());
    }
    let plan = MetadataMovePlan::new(group(1), group(9), manifests).unwrap();
    let encoded = plan.encode(MAX_METADATA_PLAN_BYTES).unwrap();
    assert!(encoded.len() > MAX_METADATA_LOCATOR_BYTES);
    let mut activation = m.activation;
    activation.publication.imported.source.plan_digest = ContentDigest::sha256(&encoded);
    assert!(MetadataLocatorUpdate::new(m.tree[0].clone(), plan, activation).is_err());
}
