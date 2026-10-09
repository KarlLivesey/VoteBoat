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
use voteboat::{log::LogEntry, retirement::*, scope::ScopeStateMachine};

type Guard = RetirementGuard<Target>;
struct Owner {
    fresh: Guard,
    live: Guard,
    entries: Vec<LogEntry>,
}
impl Owner {
    fn new(intent: &TransferIntent, g: GroupIdentity, operation: u128) -> Self {
        let route = intent
            .targets()
            .into_iter()
            .find(|r| r.target == RouteTarget::Group(g))
            .unwrap();
        let t = Target::new(
            g,
            op(operation),
            intent.clone(),
            BucketCounter::new(route.scope, Policy, bucket_limits()).unwrap(),
            Policy,
            TargetLimits {
                import_bytes: 65536,
                application_checkpoint_bytes: bucket_limits().checkpoint_bound().unwrap(),
            },
        )
        .unwrap_or_else(|e| panic!("target {:?}", e.0));
        let fresh = Guard::new(t).unwrap_or_else(|e| panic!("guard {:?}", e.0));
        Self {
            live: fresh.clone(),
            fresh,
            entries: Vec::new(),
        }
    }
    fn target(&self) -> &Target {
        self.live.owner().unwrap()
    }
    fn recover(&mut self) {
        let cp = self.live.checkpoint(500000).unwrap();
        let mut next = self.fresh.clone();
        next.restore_checkpoint(RETIREMENT_GUARD_SCHEMA, self.live.applied_index(), &cp)
            .unwrap();
        assert_eq!(next.checkpoint(500000).unwrap(), cp);
        assert_eq!(
            next.freeze_status().unwrap(),
            self.live.freeze_status().unwrap()
        );
        assert_eq!(next.status(), self.live.status());
        self.live = next;
    }
    fn command(
        &mut self,
        id: u128,
        bytes: Vec<u8>,
    ) -> RetirementReceipt<TargetReceipt<BucketReceipt>> {
        self.live
            .validate_proposal(op(id), &bytes, std::iter::empty())
            .unwrap();
        let e = entry(self.live.applied_index() + 1, id, bytes);
        let result = self
            .live
            .apply_batch(std::slice::from_ref(&e))
            .unwrap()
            .remove(0);
        self.entries.push(e);
        self.recover();
        result
    }
    fn lifecycle(&mut self, id: u128, bytes: Vec<u8>) {
        let first = self.command(id, bytes.clone()).outcome;
        let retry = self.command(id, bytes).outcome;
        match (first, retry) {
            (RetirementOutcome::Owner(first), RetirementOutcome::Owner(retry)) => {
                assert_eq!(retry.operation, first.operation);
                assert!(retry.index > first.index);
                assert_eq!(retry.outcome, first.outcome);
            }
            (first, retry) => assert_eq!(retry, first),
        }
    }
    fn status(&self) -> TargetStatus {
        self.target().status()
    }
    fn fence(&self) -> SourceFreezeStatus {
        self.live.freeze_status().unwrap().unwrap()
    }
}
fn directory_command(d: &mut Directory, id: u128, bytes: Vec<u8>) -> DirectoryReceipt {
    let first = commit(d, id, bytes.clone());
    let cp = d.checkpoint(500000).unwrap();
    let mut recovered = directory();
    recovered
        .restore_checkpoint(6, d.applied_index(), &cp)
        .unwrap();
    assert_eq!(recovered.checkpoint(500000).unwrap(), cp);
    *d = recovered;
    let retry = commit(d, id, bytes);
    assert!(retry.duplicate);
    assert_eq!(retry.outcome, first.outcome);
    first
}
fn finish(
    d: &mut Directory,
    intent: &TransferIntent,
    operation: u128,
    statuses: Vec<SourceFreezeStatus>,
    export: impl Fn(GroupIdentity) -> Vec<SourceImport>,
) -> (Vec<Owner>, TransferPublicationStatus) {
    let mut owners = Vec::new();
    for r in intent.targets() {
        let RouteTarget::Group(g) = r.target else {
            panic!("concrete target")
        };
        let mut owner = Owner::new(intent, g, operation);
        let boot = owner.target().bootstrap_command(100000).unwrap();
        owner.lifecycle(operation, boot);
        let import = TargetImport::new(op(operation), intent.clone(), g, export(g))
            .unwrap_or_else(|e| panic!("import {:?}", e.0));
        let bytes = owner.target().import_command(&import, 100000).unwrap();
        owner.lifecycle(operation, bytes);
        assert!(owner.status().activated.is_none());
        owners.push(owner);
    }
    let publication = TransferPublication::new(
        op(operation),
        intent.clone(),
        statuses
            .into_iter()
            .map(|s| {
                SourceFenceEvidence::from_status(configuration(), s)
                    .unwrap_or_else(|e| panic!("source {:?}", e.0))
            })
            .collect(),
        owners
            .iter()
            .map(|o| {
                TargetReadyEvidence::from_status(configuration(), o.status())
                    .unwrap_or_else(|e| panic!("ready {:?}", e.0))
            })
            .collect(),
    )
    .unwrap_or_else(|e| panic!("publication {:?}", e.0));
    assert!(matches!(
        directory_command(d, operation + 1, publication.encode(100000).unwrap()).outcome,
        DirectoryOutcome::TransferPublished(_)
    ));
    let decision = d
        .transfer_publication_at(d.applied_index(), op(operation))
        .unwrap()
        .unwrap();
    (owners, decision)
}
fn imports(sources: &[Owner], g: GroupIdentity) -> Vec<SourceImport> {
    sources
        .iter()
        .filter_map(|o| {
            let s = o.fence();
            let e = s.exports.iter().find(|e| e.target == g)?;
            Some(SourceImport {
                fence: s.fence,
                configuration: configuration(),
                image: o.live.export_target(g, 65536).unwrap(),
                digest: e.digest,
            })
        })
        .collect()
}
fn activate_owner(owner: &mut Owner, decision: &TransferPublicationStatus) {
    let activation = TargetActivation {
        metadata_configuration: configuration(),
        decision: decision.clone(),
    };
    let bytes = owner
        .target()
        .activation_command(&activation, 100000)
        .unwrap();
    owner.lifecycle(decision.publication.operation().get(), bytes);
}
fn complete(
    d: &mut Directory,
    reservation: &DelegationReservationStatus,
    decision: &TransferPublicationStatus,
    id: u128,
) {
    let completion = DelegationCompletion {
        reservation: reservation.operation,
        reservation_index: reservation.index,
        parent_configuration: configuration(),
        child_configuration: configuration(),
        decision: decision.clone(),
    };
    assert!(matches!(
        directory_command(d, id, completion.encode(100000).unwrap()).outcome,
        DirectoryOutcome::DelegationPublished(_)
    ));
}
fn route(m: &ResponsibilityManifest, key: u8) -> RouteHint {
    let r = match &m.input().execution {
        ExecutionMode::Single(g) => RouteEntry {
            scope: m.input().scope,
            target: RouteTarget::Group(*g),
        },
        ExecutionMode::Partitioned(routes) => *routes
            .iter()
            .find(|r| r.scope.start() <= key as u16 && (key as u16) < r.scope.end())
            .unwrap(),
        _ => panic!("concrete execution"),
    };
    let RouteTarget::Group(g) = r.target else {
        panic!("group")
    };
    let mut h = source_hint(m, g.id.get(), key);
    h.scope = r.scope;
    h
}
fn command_data(m: &ResponsibilityManifest, key: u8, delta: i64) -> Vec<u8> {
    encode_routed(
        route(m, key),
        &[key],
        &encode_add(&[key], delta, b"effect", 1024).unwrap(),
        4096,
    )
    .unwrap()
}
fn read_data(
    owner: &Owner,
    m: &ResponsibilityManifest,
    key: u8,
) -> RetirementRead<TargetRead<i64>> {
    owner
        .live
        .read_at(
            owner.live.applied_index(),
            RetirementQuery::Owner(TargetQuery::Data(RoutedQuery {
                hint: route(m, key),
                key: vec![key],
                query: vec![key],
            })),
        )
        .unwrap()
}
fn write(
    owner: &mut Owner,
    m: &ResponsibilityManifest,
    id: u128,
    key: u8,
    delta: i64,
    value: i64,
    duplicate: bool,
) {
    let RetirementOutcome::Owner(r) = owner.command(id, command_data(m, key, delta)).outcome else {
        panic!("owner data")
    };
    assert!(
        matches!(r.outcome,TargetOutcome::Applied(v) if v.outcome==BucketOutcome::Value(value)&&v.duplicate==duplicate)
    );
}
struct Scene {
    d: Directory,
    root: TransferIntent,
    nested: TransferIntent,
    source: Owner,
    grandchildren: Vec<Owner>,
    decision: TransferPublicationStatus,
}
fn scene() -> Scene {
    let (mut d, root) = setup_in(directory());
    directory_command(&mut d, 200, root.encode(100000).unwrap());
    let mut source = ready();
    commit(&mut source, 1, data(1, 7));
    commit(&mut source, 2, data(200, 11));
    commit(
        &mut source,
        200,
        Source::freeze_command(&root, 100000).unwrap(),
    );
    let SourceRead::Freeze(Some(s)) = source
        .read_at(source.applied_index(), SourceQuery::Freeze)
        .unwrap()
    else {
        panic!("root source fence")
    };
    let (mut children, decision) = finish(&mut d, &root, 200, vec![s.clone()], |g| {
        vec![SourceImport {
            fence: s.fence,
            configuration: configuration(),
            image: source.export_target(g, 65536).unwrap(),
            digest: s.exports.iter().find(|e| e.target == g).unwrap().digest,
        }]
    });
    for child in &mut children {
        activate_owner(child, &decision);
    }
    let plan = plan(&mut d, &root);
    let (reservation, nested) = reserved(&mut d, &plan);
    directory_command(&mut d, 300, nested.encode(100000).unwrap());
    let mut child = children.remove(0);
    write(&mut child, nested.before(), 80, 80, 5, 5, false);
    // Put a second retained operation into the later split's other half.
    write(&mut child, nested.before(), 81, 40, 3, 3, false);
    let old = child.status();
    let freeze = child
        .target()
        .freeze_command(&nested, 65536, 100000)
        .unwrap();
    child.lifecycle(300, freeze);
    assert_eq!(child.status(), old);
    let sources = std::slice::from_ref(&child);
    let (mut grandchildren, decision) = finish(&mut d, &nested, 300, vec![child.fence()], |g| {
        imports(sources, g)
    });
    complete(&mut d, &reservation, &decision, 403);
    for grandchild in &mut grandchildren {
        activate_owner(grandchild, &decision);
    }
    Scene {
        d,
        root,
        nested,
        source: child,
        grandchildren,
        decision,
    }
}
fn reserve(
    d: &mut Directory,
    before: &ResponsibilityManifest,
    execution: ExecutionMode,
    operation: u128,
) -> (DelegationReservationStatus, TransferIntent) {
    let parent = d
        .manifest(before.input().parent.unwrap().responsibility)
        .unwrap()
        .clone();
    let mut after = before.clone().into_input();
    after.epoch = OwnershipEpoch::new(after.epoch.get() + 1).unwrap();
    after.generation = RouteGeneration::new(after.generation.get() + 1).unwrap();
    after.execution = execution;
    let plan = DelegationPlan::new(
        parent,
        before.clone(),
        ResponsibilityManifest::new(after).unwrap(),
        op(operation),
    )
    .unwrap();
    assert_eq!(
        directory_command(d, operation - 1, plan.encode(100000).unwrap()).outcome,
        DirectoryOutcome::DelegationReserved
    );
    let reservation = d
        .delegation_reservation_at(d.applied_index(), op(operation - 1))
        .unwrap()
        .unwrap();
    let intent = reservation.child_intent(configuration()).unwrap();
    assert_eq!(
        directory_command(d, operation, intent.encode(100000).unwrap()).outcome,
        DirectoryOutcome::TransferIntentRecorded
    );
    (reservation, intent)
}
fn proof(
    source: &Owner,
    targets: &[Owner],
    decision: &TransferPublicationStatus,
    release: u128,
) -> RetirementProof {
    let f = source.fence().fence;
    RetirementProof {
        metadata_configuration: configuration(),
        decision: decision.clone(),
        targets: targets
            .iter()
            .map(|t| {
                TargetActivationEvidence::from_status(configuration(), t.status())
                    .unwrap_or_else(|e| panic!("activated {:?}", e.0))
            })
            .collect(),
        release: RetentionRelease {
            source: f.group,
            operation: f.operation,
            fence_index: f.index,
            release: op(release),
        },
    }
}
fn retire(source: &mut Owner, proof: RetirementProof) {
    let before = source.live.checkpoint(500000).unwrap();
    let provider_payload = source.target().application().checkpoint(65536).unwrap();
    let lineage = source.target().retirement_lineage().unwrap();
    let frozen = source.fence();
    let mut invalid = proof.clone();
    invalid.targets.pop();
    assert!(source.live.retirement_command(&invalid, 100000).is_err());
    let mut invalid = proof.clone();
    invalid.release.fence_index += 1;
    assert!(source.live.retirement_command(&invalid, 100000).is_err());
    let mut invalid = proof.clone();
    invalid.release.source = group(999);
    assert!(source.live.retirement_command(&invalid, 100000).is_err());
    assert_eq!(source.live.checkpoint(500000).unwrap(), before);
    let bytes = source.live.retirement_command(&proof, 100000).unwrap();
    source.lifecycle(proof.release.operation.get(), bytes);
    assert!(source.live.owner().is_none());
    assert_eq!(source.live.retired_lineage(), Some(lineage.as_slice()));
    assert_eq!(source.live.freeze_status().unwrap(), Some(frozen));
    let retired = source.live.checkpoint(500000).unwrap();
    assert!(retired.len() < before.len());
    assert!(!retired
        .windows(provider_payload.len())
        .any(|v| v == provider_payload));
    assert!(source.live.export_target(group(43), 65536).is_err());
}
fn movement(sources: &mut [Owner], intent: &TransferIntent, operation: u128) {
    for i in 0..sources.len() {
        let original = sources[i].status();
        let bytes = sources[i]
            .target()
            .freeze_command(intent, 65536, 100000)
            .unwrap();
        sources[i].lifecycle(operation, bytes);
        assert_eq!(sources[i].status(), original);
        let key = sources[i].target().application().scope().start() as u8;
        assert_eq!(
            read_data(&sources[i], intent.before(), key),
            RetirementRead::Owner(TargetRead::Rejected(RoutingError::Fenced))
        );
        for other in &sources[i + 1..] {
            assert!(other.live.freeze_status().unwrap().is_none());
            let key = other.target().application().scope().start() as u8;
            let value = other.target().application().value(&[key]).unwrap();
            assert_eq!(
                read_data(other, intent.before(), key),
                RetirementRead::Owner(TargetRead::Data(value))
            );
        }
    }
}
#[test]
fn inserted_grandchild_split_merge_retirement_preserves_original_lineage_and_retries() {
    let mut s = scene();
    let original_release = proof(&s.source, &s.grandchildren, &s.decision, 920);
    let root_manifest =
        s.d.manifest(s.root.before().input().responsibility)
            .unwrap()
            .clone();
    let before = s.nested.target_manifest(group(31)).unwrap().clone();
    let (reservation, split) = reserve(
        &mut s.d,
        &before,
        ExecutionMode::Partitioned(vec![
            RouteEntry {
                scope: range(0, 32),
                target: RouteTarget::Group(group(41)),
            },
            RouteEntry {
                scope: range(32, 64),
                target: RouteTarget::Group(group(42)),
            },
        ]),
        501,
    );
    let original = s.grandchildren[0].status();
    movement(&mut s.grandchildren[..1], &split, 501);
    assert_eq!(s.grandchildren[0].status(), original);
    let (mut divided, decision) = finish(
        &mut s.d,
        &split,
        501,
        vec![s.grandchildren[0].fence()],
        |g| imports(&s.grandchildren[..1], g),
    );
    complete(&mut s.d, &reservation, &decision, 503);
    let key = 1;
    assert_eq!(
        read_data(&divided[0], split.after(), key),
        RetirementRead::Owner(TargetRead::NotActive)
    );
    activate_owner(&mut divided[0], &decision);
    assert!(TargetActivationEvidence::from_status(configuration(), divided[1].status()).is_err());
    let partial = proof(&s.grandchildren[0], &divided[..1], &decision, 900);
    assert!(s.grandchildren[0]
        .live
        .retirement_command(&partial, 100000)
        .is_err());
    activate_owner(&mut divided[1], &decision);
    let nested_proof = proof(&s.grandchildren[0], &divided, &decision, 900);
    #[cfg(feature = "native")]
    {
        let initial = s.grandchildren[0].fresh.clone();
        support::retirement_storage::history(
            "nested-insertion",
            || initial.clone(),
            s.grandchildren[0].entries.clone(),
            nested_proof.clone(),
        );
    }
    retire(&mut s.grandchildren[0], nested_proof);
    let retired = s.grandchildren[0].live.checkpoint(500000).unwrap();
    let mut wrong_owner = s.grandchildren[1].fresh.clone();
    let pristine = wrong_owner.checkpoint(500000).unwrap();
    assert!(wrong_owner
        .restore_checkpoint(
            RETIREMENT_GUARD_SCHEMA,
            s.grandchildren[0].live.applied_index(),
            &retired
        )
        .is_err());
    assert_eq!(wrong_owner.checkpoint(500000).unwrap(), pristine);
    write(&mut divided[0], split.after(), 1, 1, 7, 7, true);
    write(&mut divided[1], split.after(), 81, 40, 3, 3, true);
    write(&mut divided[0], split.after(), 82, 1, 2, 9, false);
    let (reservation, merge) = reserve(
        &mut s.d,
        split.after(),
        ExecutionMode::Single(group(43)),
        601,
    );
    movement(&mut divided, &merge, 601);
    let statuses = divided.iter().map(Owner::fence).collect();
    let (mut merged, decision) = finish(&mut s.d, &merge, 601, statuses, |g| imports(&divided, g));
    complete(&mut s.d, &reservation, &decision, 603);
    activate_owner(&mut merged[0], &decision);
    for (i, owner) in divided.iter_mut().enumerate() {
        let p = proof(owner, &merged, &decision, 910 + i as u128);
        retire(owner, p);
    }
    // The original child can retire from its earlier nested decision even after
    // its descendants have subsequently moved; exact retained evidence is used.
    retire(&mut s.source, original_release);
    write(&mut merged[0], merge.after(), 1, 1, 7, 7, true);
    write(&mut merged[0], merge.after(), 81, 40, 3, 3, true);
    write(&mut merged[0], merge.after(), 82, 1, 2, 9, true);
    write(&mut merged[0], merge.after(), 83, 40, 4, 7, false);
    assert_eq!(merged[0].target().application().outbox().count(), 4);
    assert_eq!(
        read_data(&merged[0], merge.after(), 1),
        RetirementRead::Owner(TargetRead::Data(9))
    );
    assert_eq!(
        read_data(&merged[0], merge.after(), 40),
        RetirementRead::Owner(TargetRead::Data(7))
    );
    let sibling = s.nested.target_manifest(group(32)).unwrap();
    write(&mut s.grandchildren[1], sibling, 80, 80, 5, 5, true);
    assert_eq!(
        s.d.manifest(s.root.before().input().responsibility),
        Some(&root_manifest)
    );
    let cp = s.d.checkpoint(500000).unwrap();
    let mut recovered = directory();
    recovered
        .restore_checkpoint(6, s.d.applied_index(), &cp)
        .unwrap();
    assert_eq!(
        recovered.manifest(before.input().responsibility),
        Some(merge.after())
    );
    #[cfg(feature = "native")]
    {
        let mut cache = voteboat::native::routing::NativeManifestCache::new(ManifestCacheLimits {
            manifests: 4,
            bytes: 65536,
        })
        .unwrap();
        for id in [
            s.root.before().input().responsibility,
            s.nested.before().input().responsibility,
            before.input().responsibility,
            sibling.input().responsibility,
        ] {
            cache
                .admit(recovered.manifest(id).unwrap().clone())
                .unwrap();
        }
        for (manifest, key) in [(merge.after(), 1), (merge.after(), 40), (sibling, 80)] {
            assert_eq!(
                resolve(
                    &cache,
                    &Policy,
                    s.root.before().input().responsibility,
                    &[key],
                    4
                )
                .unwrap(),
                route(manifest, key)
            );
        }
    }
}
