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
use voteboat::{retirement::*, scope::ScopeImage};
pub(super) fn selected(first: &TransferIntent, partial: bool) -> Target {
    let t = target(first)
        .with_metadata_authority_adoption(2)
        .unwrap_or_else(|_| panic!("metadata target"));
    if partial {
        t.with_partial_delegation(2, 65536)
            .unwrap_or_else(|_| panic!("partial"))
    } else {
        t
    }
}
fn reopen(t: &Target, first: &TransferIntent, partial: bool) -> Target {
    let mut next = selected(first, partial);
    next.restore_checkpoint(
        t.schema_version(),
        t.applied_index(),
        &t.checkpoint(1000000).unwrap(),
    )
    .unwrap();
    next
}
fn adoption(m: &Moved) -> OwnerMetadataAdoption {
    OwnerMetadataAdoption::new(m.adoption.plan().clone(), rid(21), m.adoption.activation()).unwrap()
}
#[test]
fn imported_profiles_adopt_metadata_without_changing_initial_activation() {
    for partial in [false, true] {
        let mut m = moved_with(Some(partial));
        let a = adoption(&m);
        let original = m.child.status();
        let bytes = a.encode(MAX_METADATA_ADOPTION_BYTES).unwrap();
        assert!(matches!(
            record_target(&mut m.child, &mut m.child_log, 701, bytes.clone()),
            TargetOutcome::ParentAdopted(_)
        ));
        assert_eq!(m.child.grant(), &a.after());
        assert_eq!(m.child.status(), original);
        m.child = reopen(&m.child, &m.first, partial);
        assert_eq!(m.child.grant(), &a.after());
        let result = m
            .child
            .read_at(
                m.child.applied_index(),
                TargetQuery::MetadataAdoption(op(701)),
            )
            .unwrap();
        assert!(
            matches!(result,TargetRead::MetadataAdoption(Some(s)) if s.activation==a.activation())
        );
        assert_eq!(m.child.read_result_bytes(&result, 0).unwrap(), 0);
    }
}

fn query(d: &MetadataServingTarget, q: DirectoryQuery) -> DirectoryRead {
    let MetadataServingRead::Directory(r) = d
        .read_at(d.applied_index(), MetadataServingQuery::Directory(q))
        .unwrap()
    else {
        panic!("directory read")
    };
    r
}
fn manifest(d: &MetadataServingTarget, id: u128) -> ResponsibilityManifest {
    let DirectoryRead::Manifest(Some(m)) = query(d, DirectoryQuery::Manifest(rid(id))) else {
        panic!("manifest")
    };
    m
}
fn create_child(d: &mut MetadataServingTarget, t: &Target) -> InsertionChild {
    let before = t.grant();
    let mut c = before.clone().into_input();
    c.responsibility = rid(30);
    c.parent = Some(ParentAuthority {
        responsibility: before.input().responsibility,
        group: before.input().authority,
    });
    c.scope = range(0, 64);
    c.epoch = OwnershipEpoch::new(1).unwrap();
    c.generation = RouteGeneration::new(1).unwrap();
    c.execution = ExecutionMode::Single(group(30));
    let creation = GroupCreationIntent {
        authority: before.input().authority,
        parent: before.input().responsibility,
        expected: before.input().generation,
        responsibility: rid(30),
        bootstrap: support::bootstrap(30, 3),
        application: c.application,
        mode: GroupCreationMode::Staging,
    };
    commit(d, 30, creation.encode(200000).unwrap());
    let MetadataServingRead::Creation(Some(status)) = d
        .read_at(d.applied_index(), MetadataServingQuery::Creation(group(30)))
        .unwrap()
    else {
        panic!("creation")
    };
    InsertionChild::from_creation(
        ResponsibilityManifest::new(c).unwrap(),
        &GroupCreationStatus {
            operation: status.operation,
            index: status.index,
            intent: GroupCreationIntent::decode(&status.intent).unwrap(),
        },
    )
    .unwrap()
}
fn reserve(
    d: &mut MetadataServingTarget,
    plan: DelegationPlan,
    id: u128,
) -> (TransferIntent, DelegationReservationStatus) {
    commit(d, id, plan.encode(200000).unwrap());
    let DirectoryRead::DelegationReservation(Some(r)) =
        query(d, DirectoryQuery::DelegationReservation(op(id)))
    else {
        panic!("reservation")
    };
    let i = r.child_intent(ConfigurationId::new(2).unwrap()).unwrap();
    commit(
        d,
        i.delegation().unwrap().child_operation.get(),
        i.encode(200000).unwrap(),
    );
    (i, r)
}
fn finish(
    d: &mut MetadataServingTarget,
    t: &mut Target,
    log: &mut Vec<LogEntry>,
    i: &TransferIntent,
    reservation: &DelegationReservationStatus,
    partial: bool,
) -> (Vec<Target>, TransferPublicationStatus) {
    let cfg = ConfigurationId::new(1).unwrap();
    let metadata_cfg = ConfigurationId::new(2).unwrap();
    let operation = i.delegation().unwrap().child_operation;
    let bytes = if partial {
        i.encode(200000).unwrap()
    } else {
        t.freeze_command(i, 65536, 200000).unwrap()
    };
    record_target(t, log, operation.get(), bytes);
    let (fence, evidence) = if partial {
        let status = t.scoped_freeze(operation).unwrap();
        (
            status.fence.fence,
            SourceFenceEvidence::from_scoped_status(cfg, status, i).unwrap(),
        )
    } else {
        let status = t.freeze_status().unwrap().unwrap();
        (
            status.fence,
            SourceFenceEvidence::from_status(cfg, status).unwrap(),
        )
    };
    let mut targets = Vec::new();
    for route in i.targets() {
        let RouteTarget::Group(g) = route.target else {
            panic!("target group")
        };
        let mut successor = Target::new(
            g,
            operation,
            i.clone(),
            BucketCounter::new(route.scope, Policy, bucket_limits()).unwrap(),
            Policy,
            TargetLimits {
                import_bytes: 32768,
                application_checkpoint_bytes: bucket_limits().checkpoint_bound().unwrap(),
            },
        )
        .unwrap_or_else(|_| panic!("successor"));
        let b = successor.bootstrap_command(200000).unwrap();
        commit(&mut successor, operation.get(), b);
        let image = if partial {
            t.export_scoped(operation, 65536).unwrap()
        } else {
            t.export_target(g, 65536).unwrap()
        };
        let digest = ContentDigest::scope_image(&image);
        let import = TargetImport::new(
            operation,
            i.clone(),
            g,
            vec![SourceImport {
                fence,
                configuration: cfg,
                image,
                digest,
            }],
        )
        .unwrap();
        let b = successor.import_command(&import, 200000).unwrap();
        commit(&mut successor, operation.get(), b);
        targets.push(successor);
    }
    let publication = TransferPublication::new(
        operation,
        i.clone(),
        vec![evidence],
        targets
            .iter()
            .map(|t| TargetReadyEvidence::from_status(cfg, t.status()).unwrap())
            .collect(),
    )
    .unwrap();
    commit(
        d,
        operation.get() + 100,
        publication.encode(200000).unwrap(),
    );
    let DirectoryRead::Publication(Some(decision)) =
        query(d, DirectoryQuery::Publication(operation))
    else {
        panic!("publication")
    };
    let completion = DelegationCompletion {
        reservation: reservation.operation,
        reservation_index: reservation.index,
        parent_configuration: metadata_cfg,
        child_configuration: metadata_cfg,
        decision: decision.clone(),
    };
    commit(
        d,
        reservation.operation.get() + 1,
        completion.encode(200000).unwrap(),
    );
    if partial {
        let b = RetainedGrantAdoption {
            metadata_configuration: metadata_cfg,
            decision: decision.clone(),
        }
        .encode(200000)
        .unwrap();
        record_target(t, log, operation.get() + 200, b);
    }
    for successor in &mut targets {
        let b = successor
            .activation_command(
                &TargetActivation {
                    metadata_configuration: metadata_cfg,
                    decision: decision.clone(),
                },
                200000,
            )
            .unwrap();
        commit(successor, operation.get(), b);
    }
    (targets, decision)
}
fn data(t: &Target, key: u8, delta: i64) -> Vec<u8> {
    let m = t.grant().input();
    let scope = match &m.execution {
        ExecutionMode::Single(_) => m.scope,
        ExecutionMode::Delegated(routes) => {
            routes
                .iter()
                .find(|r| r.scope.contains(key.into()))
                .unwrap()
                .scope
        }
        _ => panic!("scope"),
    };
    encode_routed(
        RouteHint {
            responsibility: m.responsibility,
            group: group(21),
            application: m.application,
            scheme: m.scheme,
            scope,
            bucket: key.into(),
            epoch: m.epoch,
            generation: m.generation,
        },
        &[key],
        &encode_add(&[key], delta, b"effect", 1024).unwrap(),
        4096,
    )
    .unwrap()
}
struct Completed {
    first: TransferIntent,
    owner: Target,
    log: Vec<LogEntry>,
    proof: RetirementProof,
    scoped: Option<ScopeImage>,
}
fn completed(partial: bool) -> Completed {
    let mut m = moved_with(Some(partial));
    let a = adoption(&m);
    let b = a.encode(MAX_METADATA_ADOPTION_BYTES).unwrap();
    record_target(&mut m.child, &mut m.child_log, 701, b);
    let retry = data(&m.child, 1, 7);
    assert!(
        matches!(record_target(&mut m.child,&mut m.child_log,1,retry),TargetOutcome::Applied(r) if r.duplicate)
    );
    let b = data(&m.child, 100, 9);
    record_target(&mut m.child, &mut m.child_log, 44, b);
    m.child = reopen(&m.child, &m.first, partial);
    let scoped = if partial {
        let before = m.child.grant().clone();
        let c = create_child(&mut m.target, &m.child);
        let mut after = before.clone().into_input();
        after.epoch = OwnershipEpoch::new(after.epoch.get() + 1).unwrap();
        after.generation = RouteGeneration::new(after.generation.get() + 1).unwrap();
        after.execution = ExecutionMode::Delegated(vec![
            RouteEntry {
                scope: range(0, 64),
                target: RouteTarget::Child(ChildAuthority {
                    responsibility: rid(30),
                    group: group(9),
                    epoch: OwnershipEpoch::new(1).unwrap(),
                }),
            },
            RouteEntry {
                scope: range(64, 128),
                target: RouteTarget::Group(group(21)),
            },
        ]);
        let p = DelegationPlan::retained_insertion(
            manifest(&m.target, 10),
            before,
            ResponsibilityManifest::new(after).unwrap(),
            c,
            op(2000),
        )
        .unwrap();
        let (i, r) = reserve(&mut m.target, p, 6000);
        let (children, _) = finish(&mut m.target, &mut m.child, &mut m.child_log, &i, &r, true);
        assert_eq!(children[0].application().value(&[1]), Ok(7));
        m.child = reopen(&m.child, &m.first, true);
        Some(m.child.export_scoped(op(2000), 65536).unwrap())
    } else {
        None
    };
    let before = m.child.grant().clone();
    let mut after = before.clone().into_input();
    after.epoch = OwnershipEpoch::new(after.epoch.get() + 1).unwrap();
    after.generation = RouteGeneration::new(after.generation.get() + 1).unwrap();
    if partial {
        let ExecutionMode::Delegated(routes) = &mut after.execution else {
            panic!("delegated")
        };
        routes[1].target = RouteTarget::Group(group(40));
    } else {
        after.execution = ExecutionMode::Partitioned(vec![
            RouteEntry {
                scope: range(0, 64),
                target: RouteTarget::Group(group(40)),
            },
            RouteEntry {
                scope: range(64, 128),
                target: RouteTarget::Group(group(41)),
            },
        ]);
    }
    let after = ResponsibilityManifest::new(after).unwrap();
    let parent = manifest(&m.target, 10);
    let p = if partial {
        DelegationPlan::move_remaining(parent, before, after, op(3000))
    } else {
        DelegationPlan::new(parent, before, after, op(3000))
    }
    .unwrap();
    let (i, r) = reserve(&mut m.target, p, 6002);
    let (successors, decision) =
        finish(&mut m.target, &mut m.child, &mut m.child_log, &i, &r, false);
    assert!(successors
        .iter()
        .any(|t| t.application().value(&[100]) == Ok(9)));
    m.child = reopen(&m.child, &m.first, partial);
    let cfg = ConfigurationId::new(2).unwrap();
    let status = m.child.freeze_status().unwrap().unwrap();
    let proof = RetirementProof {
        metadata_configuration: cfg,
        decision,
        targets: successors
            .iter()
            .map(|t| {
                TargetActivationEvidence::from_status(ConfigurationId::new(1).unwrap(), t.status())
                    .unwrap()
            })
            .collect(),
        release: RetentionRelease {
            source: group(21),
            operation: status.fence.operation,
            fence_index: status.fence.index,
            release: op(9900),
        },
    };
    Completed {
        first: m.first,
        owner: m.child,
        log: m.child_log,
        proof,
        scoped,
    }
}
#[test]
fn imported_full_and_partial_metadata_lineages_retire_after_later_handoff() {
    for partial in [false, true] {
        let c = completed(partial);
        let initial = selected(&c.first, partial);
        let lineage = c.owner.retirement_lineage().unwrap();
        let status = c.owner.freeze_status().unwrap().unwrap();
        initial
            .validate_retirement_evidence(&status, &lineage)
            .unwrap();
        if let Some(image) = &c.scoped {
            assert_eq!(c.owner.export_scoped(op(2000), 65536).unwrap(), *image);
        }
        let mut guard = RetirementGuard::new(initial).unwrap_or_else(|_| panic!("guard"));
        guard.apply_batch(&c.log).unwrap();
        let b = guard
            .retirement_command(&c.proof, MAX_RETIREMENT_COMMAND_BYTES)
            .unwrap();
        assert!(matches!(
            commit(&mut guard, 3000, b.clone()).outcome,
            RetirementOutcome::Retired(_)
        ));
        assert!(guard.owner().is_none());
        assert_eq!(guard.retired_lineage(), Some(lineage.as_slice()));
        let cp = guard.checkpoint(1000000).unwrap();
        let mut fresh =
            RetirementGuard::new(selected(&c.first, partial)).unwrap_or_else(|_| panic!("guard"));
        fresh
            .restore_checkpoint(RETIREMENT_GUARD_SCHEMA, guard.applied_index(), &cp)
            .unwrap();
        assert_eq!(fresh.retired_lineage(), Some(lineage.as_slice()));
        assert_eq!(fresh.status(), guard.status());
        assert!(matches!(
            commit(&mut fresh, 3000, b).outcome,
            RetirementOutcome::Retired(_)
        ));
    }
}

#[test]
fn imported_metadata_profiles_refuse_stale_partial_and_cross_profile_state() {
    for partial in [false, true] {
        let mut m = moved_with(Some(partial));
        let a = adoption(&m);
        let bytes = a.encode(MAX_METADATA_ADOPTION_BYTES).unwrap();
        for limit in [0, MAX_PARENT_ADOPTIONS] {
            let t = target(&m.first);
            let boot = t.bootstrap_command(200000).unwrap();
            let (_, old) = t.with_metadata_authority_adoption(limit).err().unwrap();
            assert_eq!(old.bootstrap_command(200000).unwrap(), boot);
        }
        let mut new = selected(&m.first, partial);
        let boot = new.bootstrap_command(200000).unwrap();
        commit(&mut new, 200, boot);
        if partial {
            assert_eq!(
                new.apply_batch(&[entry(2, 701, bytes.clone())]),
                Err(ApplicationError::NotApplied)
            );
        } else {
            assert_eq!(
                commit(&mut new, 701, bytes.clone()).outcome,
                TargetOutcome::NotActive
            );
        }
        for profile in 0..3 {
            let mut old = match profile {
                0 => target(&m.first),
                1 => target(&m.first)
                    .with_parent_adoption(2)
                    .unwrap_or_else(|_| panic!("parent")),
                _ => target(&m.first)
                    .with_parent_adoption(2)
                    .unwrap_or_else(|_| panic!("parent"))
                    .with_partial_delegation(2, 65536)
                    .unwrap_or_else(|_| panic!("partial")),
            };
            let b = old.bootstrap_command(200000).unwrap();
            commit(&mut old, 200, b);
            let cp = old.checkpoint(1000000).unwrap();
            assert!(old
                .apply_batch(&[entry(old.applied_index() + 1, 701, bytes.clone())])
                .is_err());
            assert_eq!(old.checkpoint(1000000).unwrap(), cp);
        }
        let cp = m.child.checkpoint(1000000).unwrap();
        let index = m.child.applied_index();
        assert!(m
            .child
            .apply_batch(&[
                entry(index + 1, 701, bytes.clone()),
                entry(index + 2, 702, vec![0])
            ])
            .is_err());
        assert_eq!(m.child.checkpoint(1000000).unwrap(), cp);
        let mut pending = m.child.clone();
        commit(&mut pending, 701, bytes.clone());
        let b = data(&pending, 100, 3);
        m.child
            .validate_proposal(op(44), &b, [(op(701), bytes.as_slice())].into_iter())
            .unwrap();
        let expected = record_target(&mut m.child, &mut m.child_log, 701, bytes.clone());
        assert!(m
            .child
            .validate_proposal(op(702), &bytes, std::iter::empty())
            .is_err());
        let cp = m.child.checkpoint(1000000).unwrap();
        let mut copy = selected(&m.first, partial);
        let clean = copy.checkpoint(1000000).unwrap();
        for n in (0..cp.len())
            .step_by(17)
            .chain(std::iter::once(cp.len() - 1))
        {
            assert!(copy
                .restore_checkpoint(m.child.schema_version(), m.child.applied_index(), &cp[..n])
                .is_err());
            assert_eq!(copy.checkpoint(1000000).unwrap(), clean);
        }
        let mut opposite = selected(&m.first, !partial);
        assert!(opposite
            .restore_checkpoint(m.child.schema_version(), m.child.applied_index(), &cp)
            .is_err());
        m.child = reopen(&m.child, &m.first, partial);
        assert_eq!(commit(&mut m.child, 701, bytes).outcome, expected);
        assert_eq!(m.child.status().activated.unwrap().index, 3);
        // Original load and activation retries do not roll back the adopted grant.
        for e in &m.child_log[1..3] {
            let EntryPayload::Command { bytes, .. } = &e.payload else {
                unreachable!()
            };
            commit(&mut m.child, 200, bytes.clone());
            assert_eq!(m.child.grant(), &a.after());
        }
    }
}
fn repair_record(bytes: &mut [u8], offset: usize, partial: bool) {
    let n = u32::from_le_bytes(bytes[offset + 56..offset + 60].try_into().unwrap()) as usize;
    let mut hash = if partial {
        b"VBTPART1".to_vec()
    } else {
        b"VBTPARD1".to_vec()
    };
    hash.extend(&bytes[offset + 32..offset + 56]);
    hash.extend(&bytes[offset + 60..offset + 60 + n]);
    bytes[offset..offset + 32].copy_from_slice(&ContentDigest::sha256(&hash).0);
}
#[test]
fn imported_retirement_requires_the_exact_ordered_metadata_history() {
    for partial in [false, true] {
        let c = completed(partial);
        let initial = selected(&c.first, partial);
        let status = c.owner.freeze_status().unwrap().unwrap();
        let lineage = c.owner.retirement_lineage().unwrap();
        assert!(lineage.len() <= initial.retirement_lineage_bound());
        initial
            .validate_retirement_evidence(&status, &lineage)
            .unwrap();
        for n in 0..lineage.len() {
            assert!(initial
                .validate_retirement_evidence(&status, &lineage[..n])
                .is_err());
        }
        let n = u32::from_le_bytes(lineage[8..12].try_into().unwrap()) as usize;
        let count = 20 + n;
        let start = count + 2;
        let first_len =
            60 + u32::from_le_bytes(lineage[start + 56..start + 60].try_into().unwrap()) as usize;
        let mut missing = lineage.clone();
        missing.drain(start..start + first_len);
        let entries = u16::from_le_bytes(missing[count..count + 2].try_into().unwrap());
        missing[count..count + 2].copy_from_slice(&(entries - 1).to_le_bytes());
        assert!(initial
            .validate_retirement_evidence(&status, &missing)
            .is_err());
        for (offset, value) in [
            (start + 32, 200u128.to_le_bytes().to_vec()),
            (start + 48, status.fence.index.to_le_bytes().to_vec()),
        ] {
            let mut bad = lineage.clone();
            bad[offset..offset + value.len()].copy_from_slice(&value);
            repair_record(&mut bad, start, partial);
            assert!(initial.validate_retirement_evidence(&status, &bad).is_err());
        }
        let mut bad = lineage.clone();
        bad[start + 60 + 32] ^= 1;
        repair_record(&mut bad, start, partial);
        assert!(initial.validate_retirement_evidence(&status, &bad).is_err());
        if partial {
            let mut reordered = lineage.clone();
            let second = start + first_len;
            let tail = reordered[second..].to_vec();
            let first = reordered[start..second].to_vec();
            reordered.truncate(start);
            reordered.extend(tail);
            reordered.extend(first);
            assert!(initial
                .validate_retirement_evidence(&status, &reordered)
                .is_err());
        }
        let mut guard =
            RetirementGuard::new(selected(&c.first, partial)).unwrap_or_else(|_| panic!("guard"));
        guard.apply_batch(&c.log).unwrap();
        let b = guard
            .retirement_command(&c.proof, MAX_RETIREMENT_COMMAND_BYTES)
            .unwrap();
        commit(&mut guard, 3000, b);
        let cp = guard.checkpoint(1000000).unwrap();
        let mut restored =
            RetirementGuard::new(selected(&c.first, partial)).unwrap_or_else(|_| panic!("guard"));
        let clean = restored.checkpoint(1000000).unwrap();
        for n in (0..cp.len())
            .step_by(31)
            .chain(std::iter::once(cp.len() - 1))
        {
            assert!(restored
                .restore_checkpoint(RETIREMENT_GUARD_SCHEMA, guard.applied_index(), &cp[..n])
                .is_err());
            assert_eq!(restored.checkpoint(1000000).unwrap(), clean);
        }
        assert_eq!(
            guard
                .read_at(
                    guard.applied_index(),
                    RetirementQuery::Owner(TargetQuery::Status)
                )
                .unwrap(),
            RetirementRead::Retired
        );
    }
}
#[test]
#[cfg(feature = "native")]
fn imported_metadata_adoption_and_retirement_journal_cuts_resume_exact_history() {
    use support::{Fault, ModelIo};
    use voteboat::native::log_store::*;
    for partial in [false, true] {
        let m = moved_with(Some(partial));
        let a = adoption(&m);
        let command = a.encode(MAX_METADATA_ADOPTION_BYTES).unwrap();
        let c = completed(partial);
        for retire in [false, true] {
            let (base, entry, first) = if retire {
                let mut guard = RetirementGuard::new(selected(&c.first, partial))
                    .unwrap_or_else(|_| panic!("guard"));
                guard.apply_batch(&c.log).unwrap();
                let b = guard
                    .retirement_command(&c.proof, MAX_RETIREMENT_COMMAND_BYTES)
                    .unwrap();
                (
                    c.log.clone(),
                    entry(c.owner.applied_index() + 1, 3000, b),
                    c.first.clone(),
                )
            } else {
                (
                    m.child_log.clone(),
                    entry(4, 701, command.clone()),
                    m.first.clone(),
                )
            };
            let last = entry.index;
            let limits = LogLimits::default();
            let seed = || {
                let io = ModelIo::default();
                let mut store =
                    NativeLogStore::create(io.clone(), support::identity(1), limits).unwrap();
                support::append(
                    &mut store,
                    vec![LogMutation::Create(support::bootstrap(21, 3))],
                );
                let state = store.state(group(21)).unwrap();
                support::append(
                    &mut store,
                    vec![support::update(
                        &state,
                        1,
                        last - 1,
                        Some(Suffix {
                            from: 1,
                            entries: base.clone(),
                        }),
                    )],
                );
                (io, store)
            };
            let (_, store) = seed();
            let mutation = support::update(
                &store.state(group(21)).unwrap(),
                1,
                last,
                Some(Suffix {
                    from: last,
                    entries: vec![entry.clone()],
                }),
            );
            let frame = NativeLogCodec
                .encode_batch(3, std::slice::from_ref(&mutation), limits)
                .unwrap();
            let (mut old, mut complete) = (false, false);
            for fault in (0..=frame.len()).map(Fault::Append).chain([
                Fault::Sync,
                Fault::PublishBefore,
                Fault::PublishAfter,
            ]) {
                let (io, mut store) = seed();
                io.0.borrow_mut().fault = fault;
                if let Ok(tickets) = store.append_batch(vec![mutation.clone()]) {
                    assert!(store.barrier(&tickets).is_err());
                }
                drop(store);
                io.0.borrow_mut().power_loss();
                let store = NativeLogStore::recover(io, support::identity(1), limits).unwrap();
                let state = store.state(group(21)).unwrap();
                let mut guard = RetirementGuard::new(selected(&first, partial))
                    .unwrap_or_else(|_| panic!("guard"));
                guard
                    .apply_batch(
                        &state
                            .entries
                            .iter()
                            .filter(|e| e.index <= state.commit_index)
                            .cloned()
                            .collect::<Vec<_>>(),
                    )
                    .unwrap();
                if state.commit_index == last {
                    complete = true;
                } else {
                    old = true;
                    assert_eq!(state.commit_index, last - 1);
                }
                if retire {
                    assert_eq!(guard.owner().is_none(), state.commit_index == last);
                } else {
                    assert_eq!(
                        guard.owner().unwrap().metadata_adoption(op(701)).is_some(),
                        state.commit_index == last
                    );
                    assert_eq!(guard.owner().unwrap().application().outbox().count(), 1);
                }
                let EntryPayload::Command { operation, bytes } = &entry.payload else {
                    unreachable!()
                };
                let outcome = commit(&mut guard, operation.get(), bytes.clone()).outcome;
                let cp = guard.checkpoint(1000000).unwrap();
                let mut next = RetirementGuard::new(selected(&first, partial))
                    .unwrap_or_else(|_| panic!("guard"));
                next.restore_checkpoint(RETIREMENT_GUARD_SCHEMA, guard.applied_index(), &cp)
                    .unwrap();
                let retry = commit(&mut next, operation.get(), bytes.clone()).outcome;
                match (outcome, retry) {
                    (RetirementOutcome::Owner(a), RetirementOutcome::Owner(b)) => {
                        assert_eq!(a.outcome, b.outcome)
                    }
                    (RetirementOutcome::Retired(a), RetirementOutcome::Retired(b)) => {
                        assert_eq!(a, b)
                    }
                    _ => panic!("retry changed phase"),
                }
                if retire {
                    assert_eq!(
                        next.retired_lineage(),
                        Some(c.owner.retirement_lineage().unwrap().as_slice())
                    );
                } else {
                    assert_eq!(next.owner().unwrap().grant(), &a.after());
                }
            }
            assert!(old && complete);
        }
    }
}
