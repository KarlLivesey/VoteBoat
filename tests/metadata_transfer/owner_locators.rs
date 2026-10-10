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
    bucket_counter::*, delegation::*, retirement::*, routed::*, scoped_source::*,
    transfer_publication::*, transfer_source::*, transfer_target::*,
};
type Owner = RoutedApplication<BucketCounter<fixture::Policy>, fixture::Policy>;
type Source = TransferSource<BucketCounter<fixture::Policy>, fixture::Policy>;
type Scoped = ScopedTransferSource<BucketCounter<fixture::Policy>, fixture::Policy>;
type Target = TransferTarget<BucketCounter<fixture::Policy>, fixture::Policy>;
fn cfg(n: u64) -> ConfigurationId {
    ConfigurationId::new(n).unwrap()
}
fn apply<A: StateMachine>(a: &mut A, id: u128, bytes: Vec<u8>) -> A::Receipt {
    a.apply_batch(&[entry(a.applied_index() + 1, id, bytes)])
        .unwrap()
        .remove(0)
}
fn tracked<A: StateMachine>(
    a: &mut A,
    log: &mut Vec<LogEntry>,
    id: u128,
    bytes: Vec<u8>,
) -> A::Receipt {
    let e = entry(a.applied_index() + 1, id, bytes);
    let r = a.apply_batch(std::slice::from_ref(&e)).unwrap().remove(0);
    log.push(e);
    r
}
fn owner(m: &ResponsibilityManifest, g: u128) -> Owner {
    RoutedApplication::new(
        group(g),
        m.clone(),
        BucketCounter::new(m.input().scope, fixture::Policy, fixture::bucket_limits()).unwrap(),
        fixture::Policy,
        fixture::routed().limits(),
    )
    .unwrap_or_else(|_| panic!("owner"))
}
fn selected(m: &ResponsibilityManifest, g: u128) -> Owner {
    owner(m, g)
        .with_metadata_locator_adoption(2)
        .unwrap_or_else(|_| panic!("profile"))
}
fn source(m: &ResponsibilityManifest) -> Source {
    Source::new(selected(m, 21), 65536).unwrap_or_else(|_| panic!("source"))
}
fn scoped(m: &ResponsibilityManifest) -> Scoped {
    Scoped::new(
        owner(m, 21)
            .with_scoped_fencing(2)
            .unwrap_or_else(|_| panic!("scopes")),
        65536,
    )
    .unwrap_or_else(|_| panic!("source"))
    .with_retained_insertion()
    .unwrap_or_else(|_| panic!("retained"))
    .with_retained_grants()
    .unwrap_or_else(|_| panic!("grants"))
    .with_metadata_locator_adoption(2)
    .unwrap_or_else(|_| panic!("locators"))
}
fn data(m: &ResponsibilityManifest, g: u128, key: u8, delta: i64) -> Vec<u8> {
    let v = m.input();
    let scope = match &v.execution {
        ExecutionMode::Single(_) => v.scope,
        ExecutionMode::Partitioned(r) | ExecutionMode::Delegated(r) => {
            r.iter()
                .find(|r| r.target == RouteTarget::Group(group(g)))
                .unwrap()
                .scope
        }
    };
    encode_routed(
        RouteHint {
            responsibility: v.responsibility,
            group: group(g),
            application: v.application,
            scheme: v.scheme,
            scope,
            bucket: key.into(),
            epoch: v.epoch,
            generation: v.generation,
        },
        &[key],
        &encode_add(&[key], delta, b"outbox", 1024).unwrap(),
        4096,
    )
    .unwrap()
}
fn target(
    i: &TransferIntent,
    id: OperationId,
    g: u128,
    scope: BucketRange,
    profile: Option<bool>,
) -> Target {
    let mut t = Target::new(
        group(g),
        id,
        i.clone(),
        BucketCounter::new(scope, fixture::Policy, fixture::bucket_limits()).unwrap(),
        fixture::Policy,
        TargetLimits {
            import_bytes: 65536,
            application_checkpoint_bytes: fixture::bucket_limits().checkpoint_bound().unwrap(),
        },
    )
    .unwrap_or_else(|_| panic!("target"));
    if let Some(partial) = profile {
        t = t
            .with_metadata_locator_adoption(2)
            .unwrap_or_else(|_| panic!("profile"));
        if partial {
            t = t
                .with_partial_delegation(2, 65536)
                .unwrap_or_else(|_| panic!("partial"));
        }
    }
    t
}
enum Parent {
    Source(Box<MetadataPublishingSource>),
    Target(Box<MetadataServingTarget>),
}
struct World {
    parent: Option<Parent>,
    template: MetadataPublishingSource,
    child: Directory,
}
impl World {
    fn new() -> Self {
        let [_, root, child] = locators::tree();
        let (child, _) = locators::seeded(&child, 32);
        let d = Directory::new(
            DirectoryPlan::new(group(1), vec![root.clone()]).unwrap(),
            DirectoryLimits {
                operations: 32,
                history_bytes: 200000,
            },
        )
        .unwrap()
        .with_metadata_locator_updates()
        .unwrap_or_else(|_| panic!("directory"));
        let limit = d.readiness_requirements().snapshot_bytes;
        let template = MetadataPublishingSource::new(
            MetadataAuthoritySource::new(LifecycleDirectory::new(d), limit)
                .unwrap_or_else(|_| panic!("source")),
        )
        .unwrap();
        let mut parent = template.clone();
        let boot = parent.bootstrap_command(200000).unwrap();
        apply(&mut parent, 1000, boot);
        apply(
            &mut parent,
            1001,
            DirectoryCommand {
                expected: None,
                manifest: root,
            }
            .encode(200000)
            .unwrap(),
        );
        Self {
            parent: Some(Parent::Source(Box::new(parent))),
            template,
            child,
        }
    }
    fn parent_commit(&mut self, id: u128, b: Vec<u8>) {
        match self.parent.as_mut().unwrap() {
            Parent::Source(s) => {
                apply(s.as_mut(), id, b);
            }
            Parent::Target(t) => {
                apply(t.as_mut(), id, b);
            }
        }
    }
    fn parent_read(&self, q: DirectoryQuery) -> DirectoryRead {
        match self.parent.as_ref().unwrap() {
            Parent::Source(s) => {
                let MetadataPublishingRead::Source(MetadataSourceRead::Directory(r)) = s
                    .read_at(
                        s.applied_index(),
                        MetadataPublishingQuery::Source(MetadataSourceQuery::Directory(q)),
                    )
                    .unwrap()
                else {
                    panic!("source read")
                };
                r
            }
            Parent::Target(t) => {
                let MetadataServingRead::Directory(r) = t
                    .read_at(t.applied_index(), MetadataServingQuery::Directory(q))
                    .unwrap()
                else {
                    panic!("target read")
                };
                r
            }
        }
    }
    fn parent_manifest(&self) -> ResponsibilityManifest {
        let DirectoryRead::Manifest(Some(m)) = self.parent_read(DirectoryQuery::Manifest(
            locators::tree()[1].input().responsibility,
        )) else {
            panic!("manifest")
        };
        m
    }
    fn grant(&self) -> ResponsibilityManifest {
        self.child
            .manifest(locators::tree()[2].input().responsibility)
            .unwrap()
            .clone()
    }
    fn move_parent(&mut self) -> OwnerMetadataLocatorAdoption {
        let Parent::Source(s) = self.parent.take().unwrap() else {
            panic!("once")
        };
        let mut s = *s;
        let plan = s.source().plan(group(9)).unwrap();
        let b = s.source().freeze_command(&plan, 200000).unwrap();
        apply(&mut s, 7, b);
        let image = s.source().export(200000).unwrap();
        let mut t =
            MetadataServingTarget::new(self.template.clone(), plan.clone(), op(7), cfg(1), cfg(2))
                .unwrap();
        let b = t.bootstrap_command(200000).unwrap();
        apply(&mut t, 7, b);
        let b = t.import_command(&image, cfg(1), 200000).unwrap();
        apply(&mut t, 7, b);
        let b = s
            .publication_command(t.status().target.imported.unwrap(), cfg(2), 200000)
            .unwrap();
        apply(&mut s, 7, b);
        let b = t
            .activation_command(s.publication().unwrap(), 200000)
            .unwrap();
        apply(&mut t, 7, b);
        let update =
            MetadataLocatorUpdate::new(self.grant(), plan, t.status().activation.unwrap()).unwrap();
        let bytes = update.encode(200000).unwrap();
        apply(&mut self.child, 800, bytes);
        let view = LifecycleDirectory::new(self.child.clone());
        let DirectoryRead::MetadataLocator(Some(observation)) = view
            .read_at(
                view.applied_index(),
                DirectoryQuery::MetadataLocator(op(800)),
            )
            .unwrap()
        else {
            panic!("status")
        };
        self.parent = Some(Parent::Target(Box::new(t)));
        OwnerMetadataLocatorAdoption::new(update, cfg(3), observation).unwrap()
    }
    fn reserve(
        &mut self,
        after: ResponsibilityManifest,
        id: u128,
    ) -> (TransferIntent, DelegationReservationStatus) {
        self.reserve_plan(
            DelegationPlan::new(self.parent_manifest(), self.grant(), after, op(id)).unwrap(),
            id,
        )
    }
    fn reserve_plan(
        &mut self,
        p: DelegationPlan,
        id: u128,
    ) -> (TransferIntent, DelegationReservationStatus) {
        self.parent_commit(id + 10000, p.encode(200000).unwrap());
        let DirectoryRead::DelegationReservation(Some(r)) =
            self.parent_read(DirectoryQuery::DelegationReservation(op(id + 10000)))
        else {
            panic!("reserve")
        };
        let i = r
            .child_intent(if matches!(self.parent, Some(Parent::Source(_))) {
                cfg(1)
            } else {
                cfg(2)
            })
            .unwrap();
        apply(&mut self.child, id, i.encode(200000).unwrap());
        (i, r)
    }
    fn finish(
        &mut self,
        i: &TransferIntent,
        r: &DelegationReservationStatus,
        sources: Vec<SourceFenceEvidence>,
        imports: Vec<(u128, BucketRange, Vec<SourceImport>)>,
        profile: Option<bool>,
    ) -> (Vec<Target>, Vec<Vec<LogEntry>>, TransferPublicationStatus) {
        let mut targets = Vec::new();
        let mut logs = Vec::new();
        for (g, scope, parts) in imports {
            let mut t = target(i, r.plan.child_operation(), g, scope, profile);
            let mut log = Vec::new();
            let b = t.bootstrap_command(200000).unwrap();
            tracked(&mut t, &mut log, r.plan.child_operation().get(), b);
            let import =
                TargetImport::new(r.plan.child_operation(), i.clone(), group(g), parts).unwrap();
            let b = t.import_command(&import, 200000).unwrap();
            tracked(&mut t, &mut log, r.plan.child_operation().get(), b);
            targets.push(t);
            logs.push(log);
        }
        let p = TransferPublication::new(
            r.plan.child_operation(),
            i.clone(),
            sources,
            targets
                .iter()
                .map(|t| {
                    TargetReadyEvidence::from_status(
                        if i.insertion_children().is_some() {
                            cfg(1)
                        } else {
                            cfg(4)
                        },
                        t.status(),
                    )
                    .unwrap()
                })
                .collect(),
        )
        .unwrap();
        apply(
            &mut self.child,
            r.plan.child_operation().get() + 1,
            p.encode(200000).unwrap(),
        );
        let decision = self
            .child
            .transfer_publication_at(self.child.applied_index(), r.plan.child_operation())
            .unwrap()
            .unwrap();
        let completion = DelegationCompletion {
            reservation: r.operation,
            reservation_index: r.index,
            parent_configuration: if matches!(self.parent, Some(Parent::Source(_))) {
                cfg(1)
            } else {
                cfg(2)
            },
            child_configuration: cfg(3),
            decision: decision.clone(),
        };
        self.parent_commit(r.operation.get() + 1, completion.encode(200000).unwrap());
        for (t, log) in targets.iter_mut().zip(&mut logs) {
            let b = t
                .activation_command(
                    &TargetActivation {
                        metadata_configuration: cfg(3),
                        decision: decision.clone(),
                    },
                    200000,
                )
                .unwrap();
            tracked(t, log, r.plan.child_operation().get(), b);
        }
        (targets, logs, decision)
    }
}
fn split_after(before: &ResponsibilityManifest) -> ResponsibilityManifest {
    let mut m = before.clone().into_input();
    m.epoch = OwnershipEpoch::new(m.epoch.get() + 1).unwrap();
    m.generation = RouteGeneration::new(m.generation.get() + 1).unwrap();
    m.execution = ExecutionMode::Partitioned(vec![
        RouteEntry {
            scope: range(0, 64),
            target: RouteTarget::Group(group(30)),
        },
        RouteEntry {
            scope: range(64, 128),
            target: RouteTarget::Group(group(31)),
        },
    ]);
    ResponsibilityManifest::new(m).unwrap()
}
fn split(
    world: &mut World,
    source: &mut Source,
    profile: Option<bool>,
) -> (TransferIntent, Vec<Target>, Vec<Vec<LogEntry>>) {
    let (i, r) = world.reserve(split_after(&world.grant()), 200);
    apply(source, 200, Source::freeze_command(&i, 200000).unwrap());
    let SourceRead::Freeze(Some(status)) = source
        .read_at(source.applied_index(), SourceQuery::Freeze)
        .unwrap()
    else {
        panic!("freeze")
    };
    let imports = [(30, range(0, 64)), (31, range(64, 128))]
        .into_iter()
        .map(|(g, scope)| {
            (
                g,
                scope,
                vec![SourceImport {
                    fence: status.fence,
                    configuration: cfg(4),
                    image: source.export_target(group(g), 65536).unwrap(),
                    digest: status
                        .exports
                        .iter()
                        .find(|x| x.target == group(g))
                        .unwrap()
                        .digest,
                }],
            )
        })
        .collect();
    let (targets, logs, _) = world.finish(
        &i,
        &r,
        vec![SourceFenceEvidence::from_status(cfg(4), status).unwrap()],
        imports,
        profile,
    );
    (i, targets, logs)
}
#[test]
fn original_owner_adopts_real_locator_then_splits_without_changing_data_lineage() {
    let mut world = World::new();
    let before = world.grant();
    let mut source = source(&before);
    let boot = source.bootstrap_command(200000).unwrap();
    apply(&mut source, 90, boot.clone());
    apply(&mut source, 1, data(&before, 21, 1, 7));
    apply(&mut source, 2, data(&before, 21, 100, 11));
    for id in 3..=32 {
        apply(&mut source, id, data(&before, 21, 100, 0));
    }
    assert_eq!(source.routed().remaining_operations(), 0);
    let a = world.move_parent();
    let bytes = a.encode(200000).unwrap();
    assert!(source
        .validate_proposal(op(801), &bytes, std::iter::empty())
        .is_ok());
    let receipt = apply(&mut source, 801, bytes.clone());
    assert!(matches!(receipt.outcome, RoutedOutcome::ParentAdopted(_)));
    assert_eq!(source.routed().grant(), &a.after());
    assert_eq!(source.bootstrap_command(200000).unwrap(), boot);
    assert!(
        matches!(apply(&mut source,1,data(&a.after(),21,1,7)).outcome,RoutedOutcome::Applied(r) if r.duplicate)
    );
    let cp = source.checkpoint(200000).unwrap();
    let mut recovered = self::source(&before);
    recovered
        .restore_checkpoint(source.schema_version(), source.applied_index(), &cp)
        .unwrap();
    assert_eq!(
        apply(&mut recovered, 801, bytes.clone()).outcome,
        receipt.outcome
    );
    let expected = recovered
        .routed()
        .metadata_locator_adoption(op(801))
        .unwrap();
    assert_eq!(expected.locator, a.observation());
    let (i, targets, _) = split(&mut world, &mut recovered, None);
    assert_eq!(targets[0].application().value(&[1]), Ok(7));
    assert_eq!(targets[1].application().value(&[100]), Ok(11));
    assert_eq!(
        recovered.routed().metadata_locator_adoption(op(801)),
        Some(expected)
    );
    assert_eq!(apply(&mut recovered, 801, bytes).outcome, receipt.outcome);
    assert_eq!(i.before(), &a.after());
}
#[test]
fn imported_full_and_partial_locators_preserve_import_and_retire_exact_later_merge() {
    for partial in [false, true] {
        let mut world = World::new();
        let before = world.grant();
        let mut source = source(&before);
        let b = source.bootstrap_command(200000).unwrap();
        apply(&mut source, 90, b);
        apply(&mut source, 1, data(&before, 21, 1, 7));
        apply(&mut source, 2, data(&before, 21, 100, 11));
        let (first, mut targets, mut logs) = split(&mut world, &mut source, Some(partial));
        let templates = [(30, range(0, 64)), (31, range(64, 128))]
            .into_iter()
            .map(|(g, scope)| target(&first, op(200), g, scope, Some(partial)))
            .collect::<Vec<_>>();
        let statuses = targets.iter().map(Target::status).collect::<Vec<_>>();
        let a = world.move_parent();
        let bytes = a.encode(200000).unwrap();
        for (t, log) in targets.iter_mut().zip(&mut logs) {
            let receipt = tracked(t, log, 801, bytes.clone());
            assert!(matches!(receipt.outcome, TargetOutcome::ParentAdopted(_)));
            assert_eq!(t.grant(), &a.after());
            let cp = t.checkpoint(300000).unwrap();
            let mut next = templates[usize::from(t.status().group == group(31))].clone();
            next.restore_checkpoint(if partial { 11 } else { 10 }, t.applied_index(), &cp)
                .unwrap();
            *t = next;
            assert_eq!(
                t.metadata_locator_adoption(op(801)).unwrap().locator,
                a.observation()
            );
        }
        for (index, t) in targets.iter_mut().enumerate() {
            assert_eq!(t.status(), statuses[index]);
            let value = t
                .read_at(
                    t.applied_index(),
                    TargetQuery::MetadataLocatorAdoption(op(801)),
                )
                .unwrap();
            assert!(
                matches!(value,TargetRead::MetadataLocatorAdoption(Some(s)) if s.locator==a.observation())
            );
            assert_eq!(t.read_result_bytes(&value, 0).unwrap(), 0);
            let (g, key, old, delta) = if index == 0 {
                (30, 1, 7, 3)
            } else {
                (31, 100, 11, 4)
            };
            let retry = data(t.grant(), g, key, old);
            let opid = if index == 0 { 1 } else { 2 };
            assert!(
                matches!(tracked(t,&mut logs[index],opid,retry).outcome,TargetOutcome::Applied(r) if r.duplicate)
            );
            let write = data(t.grant(), g, key, delta);
            tracked(t, &mut logs[index], 5 + index as u128, write);
        }
        let mut after = a.after().into_input();
        after.epoch = OwnershipEpoch::new(after.epoch.get() + 1).unwrap();
        after.generation = RouteGeneration::new(after.generation.get() + 1).unwrap();
        after.execution = ExecutionMode::Single(group(40));
        let (i, r) = world.reserve(ResponsibilityManifest::new(after).unwrap(), 900);
        let mut evidence = Vec::new();
        let mut imports = Vec::new();
        for (t, log) in targets.iter_mut().zip(&mut logs) {
            let b = t.freeze_command(&i, 65536, 200000).unwrap();
            tracked(t, log, 900, b);
            let status = t.freeze_status().unwrap().unwrap();
            evidence.push(SourceFenceEvidence::from_status(cfg(4), status.clone()).unwrap());
            let image = t.export_target(group(40), 65536).unwrap();
            imports.push(SourceImport {
                fence: status.fence,
                configuration: cfg(4),
                digest: ContentDigest::scope_image(&image),
                image,
            });
        }
        let (successors, _, decision) =
            world.finish(&i, &r, evidence, vec![(40, range(0, 128), imports)], None);
        assert_eq!(successors[0].application().value(&[1]), Ok(10));
        assert_eq!(successors[0].application().value(&[100]), Ok(15));
        for (index, (t, log)) in targets.iter().zip(&mut logs).enumerate() {
            let template = &templates[index];
            let status = t.freeze_status().unwrap().unwrap();
            let lineage = t.retirement_lineage().unwrap();
            template
                .validate_retirement_evidence(&status, &lineage)
                .unwrap();
            // A well-formed empty grant history must not pass merely because
            // preliminary source-shape normalization permits metadata changes.
            let activation_bytes = u32::from_le_bytes(lineage[8..12].try_into().unwrap()) as usize;
            let count_at = 12 + activation_bytes + 8;
            let mut missing = lineage[..count_at + 2].to_vec();
            missing[count_at..count_at + 2].copy_from_slice(&0u16.to_le_bytes());
            assert!(template
                .validate_retirement_evidence(&status, &missing)
                .is_err());
            let proof = RetirementProof {
                metadata_configuration: cfg(3),
                decision: decision.clone(),
                targets: vec![TargetActivationEvidence::from_status(
                    cfg(4),
                    successors[0].status(),
                )
                .unwrap()],
                release: RetentionRelease {
                    source: t.status().group,
                    operation: op(900),
                    fence_index: status.fence.index,
                    release: op(999),
                },
            };
            let mut guard =
                RetirementGuard::new(template.clone()).unwrap_or_else(|_| panic!("guard"));
            guard.apply_batch(log).unwrap();
            let b = guard.retirement_command(&proof, 300000).unwrap();
            #[cfg(feature = "native")]
            {
                let mut history = log.clone();
                history.push(entry(guard.applied_index() + 1, 900, b.clone()));
                journal_cuts(
                    t.status().group,
                    || RetirementGuard::new(template.clone()).unwrap_or_else(|_| panic!("guard")),
                    &history,
                );
            }
            apply(&mut guard, 900, b);
            let cp = guard.checkpoint(300000).unwrap();
            let mut reopened =
                RetirementGuard::new(template.clone()).unwrap_or_else(|_| panic!("guard"));
            reopened
                .restore_checkpoint(guard.schema_version(), guard.applied_index(), &cp)
                .unwrap();
            assert!(reopened.owner().is_none());
            for n in [0, 8, lineage.len() - 1] {
                assert!(template
                    .validate_retirement_evidence(&status, &lineage[..n])
                    .is_err());
            }
            let old = Target::new(
                t.status().group,
                op(200),
                first.clone(),
                BucketCounter::new(
                    if index == 0 {
                        range(0, 64)
                    } else {
                        range(64, 128)
                    },
                    fixture::Policy,
                    fixture::bucket_limits(),
                )
                .unwrap(),
                fixture::Policy,
                TargetLimits {
                    import_bytes: 65536,
                    application_checkpoint_bytes: fixture::bucket_limits()
                        .checkpoint_bound()
                        .unwrap(),
                },
            )
            .unwrap_or_else(|_| panic!("old"))
            .with_metadata_authority_adoption(2)
            .unwrap_or_else(|_| panic!("old profile"));
            assert!(old.validate_retirement_evidence(&status, &lineage).is_err());
        }
    }
}

#[test]
fn retained_owner_keeps_exports_and_adopts_a_later_retained_transfer() {
    let mut world = World::new();
    let before = world.grant();
    let mut owner = scoped(&before);
    let b = owner.bootstrap_command(200000).unwrap();
    apply(&mut owner, 90, b);
    apply(&mut owner, 1, data(&before, 21, 1, 7));
    apply(&mut owner, 2, data(&before, 21, 100, 11));
    let a = world.move_parent();
    let b = a.encode(200000).unwrap();
    let first = apply(&mut owner, 801, b.clone());
    assert_eq!(owner.grant(), &a.after());
    apply(&mut owner, 3, data(&a.after(), 21, 100, 2));
    let mut child = a.after().into_input();
    child.responsibility.id = ResponsibilityId::new(70).unwrap();
    child.parent = Some(ParentAuthority {
        responsibility: before.input().responsibility,
        group: before.input().authority,
    });
    child.scope = range(0, 64);
    child.generation = RouteGeneration::new(1).unwrap();
    child.execution = ExecutionMode::Single(group(70));
    let child = ResponsibilityManifest::new(child).unwrap();
    let creation = GroupCreationIntent {
        authority: before.input().authority,
        parent: before.input().responsibility,
        expected: a.after().input().generation,
        responsibility: child.input().responsibility,
        bootstrap: support::bootstrap(70, 3),
        application: child.input().application,
        mode: GroupCreationMode::Staging,
    };
    apply(&mut world.child, 70, creation.encode(200000).unwrap());
    let status = world
        .child
        .group_creation_at(world.child.applied_index(), group(70))
        .unwrap()
        .unwrap();
    let insertion = InsertionChild::from_creation(child.clone(), &status).unwrap();
    let mut after = a.after().into_input();
    after.epoch = OwnershipEpoch::new(2).unwrap();
    after.generation = RouteGeneration::new(3).unwrap();
    after.execution = ExecutionMode::Delegated(vec![
        RouteEntry {
            scope: range(0, 64),
            target: RouteTarget::Child(ChildAuthority {
                responsibility: child.input().responsibility,
                group: before.input().authority,
                epoch: child.input().epoch,
            }),
        },
        RouteEntry {
            scope: range(64, 128),
            target: RouteTarget::Group(group(21)),
        },
    ]);
    let plan = DelegationPlan::retained_insertion(
        world.parent_manifest(),
        a.after(),
        ResponsibilityManifest::new(after).unwrap(),
        insertion,
        op(900),
    )
    .unwrap();
    let (i, r) = world.reserve_plan(plan, 900);
    apply(&mut owner, 900, i.encode(200000).unwrap());
    let ScopedSourceRead::Frozen(Some(frozen)) = owner
        .read_at(owner.applied_index(), ScopedSourceQuery::Frozen(op(900)))
        .unwrap()
    else {
        panic!("frozen")
    };
    let image = owner.export(op(900), 65536).unwrap();
    let (targets, _, decision) = world.finish(
        &i,
        &r,
        vec![SourceFenceEvidence::from_scoped_status(cfg(4), frozen, &i).unwrap()],
        vec![(
            70,
            range(0, 64),
            vec![SourceImport {
                fence: frozen.fence.fence,
                configuration: cfg(4),
                digest: frozen.digest,
                image: image.clone(),
            }],
        )],
        None,
    );
    apply(
        &mut owner,
        901,
        RetainedGrantAdoption {
            metadata_configuration: cfg(3),
            decision,
        }
        .encode(200000)
        .unwrap(),
    );
    assert_eq!(owner.export(op(900), 65536).unwrap(), image);
    assert_eq!(targets[0].application().value(&[1]), Ok(7));
    let retry = data(owner.grant(), 21, 100, 11);
    assert!(matches!(apply(&mut owner,2,retry).outcome,RoutedOutcome::Applied(r) if r.duplicate));
    let cp = owner.checkpoint(300000).unwrap();
    let mut reopened = scoped(&before);
    reopened
        .restore_checkpoint(7, owner.applied_index(), &cp)
        .unwrap();
    assert_eq!(reopened.grant(), owner.grant());
    assert_eq!(reopened.export(op(900), 65536).unwrap(), image);
    assert_eq!(apply(&mut reopened, 801, b).outcome, first.outcome);
    let q = ScopedSourceQuery::MetadataLocatorAdoption(op(801));
    let read = reopened.read_at(reopened.applied_index(), q).unwrap();
    assert!(
        matches!(read,ScopedSourceRead::MetadataLocatorAdoption(Some(s)) if s.locator==a.observation())
    );
    assert_eq!(reopened.read_result_bytes(&read, 0).unwrap(), 0);
}
#[test]
fn locator_observation_profiles_pending_and_checkpoint_corruption_refuse_atomically() {
    let mut world = World::new();
    let before = world.grant();
    let a = world.move_parent();
    let bytes = a.encode(200000).unwrap();
    assert_eq!(OwnerMetadataLocatorAdoption::decode(&bytes).unwrap(), a);
    assert!(a.encode(bytes.len() - 1).is_err());
    for n in 0..bytes.len() {
        assert!(OwnerMetadataLocatorAdoption::decode(&bytes[..n]).is_err());
    }
    for change in 0..7 {
        let mut status = a.observation();
        match change {
            0 => status.index = 0,
            1 => status.authority = group(9),
            2 => status.responsibility.id = ResponsibilityId::new(888).unwrap(),
            3 => status.generation = RouteGeneration::new(5).unwrap(),
            4 => status.command_digest.0[0] ^= 1,
            5 => status.activation.index += 1,
            _ => status.index = u64::MAX,
        };
        assert!(OwnerMetadataLocatorAdoption::new(a.update().clone(), cfg(3), status).is_err());
    }
    assert!(owner(&before, 21)
        .with_metadata_locator_adoption(0)
        .is_err());
    assert!(owner(&before, 21)
        .with_metadata_locator_adoption(MAX_PARENT_ADOPTIONS)
        .is_err());
    let mut old = owner(&before, 21)
        .with_metadata_authority_adoption(2)
        .unwrap_or_else(|_| panic!("old"));
    let b = old.bootstrap_command(200000).unwrap();
    apply(&mut old, 90, b);
    assert!(old.apply_batch(&[entry(2, 801, bytes.clone())]).is_err());
    let mut current = selected(&before, 21);
    let b = current.bootstrap_command(200000).unwrap();
    apply(&mut current, 90, b);
    let initial = current.checkpoint(200000).unwrap();
    assert!(current
        .apply_batch(&[entry(2, 801, bytes.clone()), entry(3, 802, vec![0])])
        .is_err());
    assert_eq!(current.checkpoint(200000).unwrap(), initial);
    assert!(current
        .validate_proposal(op(802), &bytes, [(op(801), bytes.as_slice())].into_iter())
        .is_err());
    let receipt = apply(&mut current, 801, bytes.clone());
    assert!(matches!(
        apply(&mut current, 802, bytes.clone()).outcome,
        RoutedOutcome::Rejected(RoutingError::WrongOwner)
    ));
    let cp = current.checkpoint(200000).unwrap();
    let mut empty = selected(&before, 21);
    for n in 0..cp.len() {
        assert!(empty
            .restore_checkpoint(6, current.applied_index(), &cp[..n])
            .is_err());
        assert_eq!(empty.applied_index(), 0);
    }
    assert!(old
        .restore_checkpoint(6, current.applied_index(), &cp)
        .is_err());
    empty
        .restore_checkpoint(6, current.applied_index(), &cp)
        .unwrap();
    assert_eq!(apply(&mut empty, 801, bytes).outcome, receipt.outcome);
    let view = RoutedControlReads::new(empty);
    let q = RoutedControlQuery::MetadataLocatorAdoption(op(801));
    let value = view.read_at(view.applied_index(), q).unwrap();
    assert_eq!(view.read_result_bytes(&value, 0).unwrap(), 0);
}

#[cfg(feature = "native")]
fn journal_cuts<A: CheckpointStateMachine>(
    g: GroupIdentity,
    make: impl Fn() -> A,
    entries: &[LogEntry],
) {
    use support::{Fault, ModelIo};
    use voteboat::native::log_store::*;
    let n = entries.len() as u64;
    let old_entries = &entries[..entries.len() - 1];
    let mut old = make();
    old.apply_batch(old_entries).unwrap();
    let old_cp = old.checkpoint(1000000).unwrap();
    let mut full = make();
    full.apply_batch(entries).unwrap();
    let full_cp = full.checkpoint(1000000).unwrap();
    let mut retry = entries.last().unwrap().clone();
    retry.index += 1;
    full.apply_batch(std::slice::from_ref(&retry)).unwrap();
    let retried_cp = full.checkpoint(1000000).unwrap();
    let limits = LogLimits::default();
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
                n - 1,
                Some(Suffix {
                    from: 1,
                    entries: old_entries.to_vec(),
                }),
            )],
        );
        (io, log)
    };
    let (_, log) = seed();
    let mutation = support::update(
        &log.state(g).unwrap(),
        1,
        n,
        Some(Suffix {
            from: n,
            entries: vec![entries.last().unwrap().clone()],
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
        let complete = state.commit_index == n;
        outcomes[usize::from(complete)] = true;
        let mut app = make();
        app.apply_batch(
            &state
                .entries
                .into_iter()
                .filter(|e| e.index <= state.commit_index)
                .collect::<Vec<_>>(),
        )
        .unwrap();
        assert_eq!(
            app.checkpoint(1000000).unwrap(),
            if complete {
                full_cp.clone()
            } else {
                old_cp.clone()
            }
        );
        if !complete {
            app.apply_batch(std::slice::from_ref(entries.last().unwrap()))
                .unwrap();
        }
        app.apply_batch(std::slice::from_ref(&retry)).unwrap();
        assert_eq!(app.checkpoint(1000000).unwrap(), retried_cp);
    }
    assert_eq!(outcomes, [true, true]);
}
#[cfg(feature = "native")]
#[test]
fn native_locator_adoption_cuts_preserve_all_owner_families_and_exact_retries() {
    let mut world = World::new();
    let before = world.grant();
    let a = world.move_parent();
    let b = a.encode(200000).unwrap();
    let o = selected(&before, 21);
    let entries = vec![
        entry(1, 90, o.bootstrap_command(200000).unwrap()),
        entry(2, 1, data(&before, 21, 1, 7)),
        entry(3, 801, b.clone()),
    ];
    journal_cuts(group(21), || selected(&before, 21), &entries);
    let o = scoped(&before);
    let entries = vec![
        entry(1, 90, o.bootstrap_command(200000).unwrap()),
        entry(2, 1, data(&before, 21, 1, 7)),
        entry(3, 801, b),
    ];
    journal_cuts(group(21), || scoped(&before), &entries);
    for partial in [false, true] {
        let mut world = World::new();
        let before = world.grant();
        let mut source = source(&before);
        let b = source.bootstrap_command(200000).unwrap();
        apply(&mut source, 90, b);
        apply(&mut source, 1, data(&before, 21, 1, 7));
        let (i, targets, mut histories) = split(&mut world, &mut source, Some(partial));
        let a = world.move_parent();
        let bytes = a.encode(200000).unwrap();
        let mut history = histories.remove(0);
        history.push(entry(targets[0].applied_index() + 1, 801, bytes));
        journal_cuts(
            group(30),
            || target(&i, op(200), 30, range(0, 64), Some(partial)),
            &history,
        );
    }
}
