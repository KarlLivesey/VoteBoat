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
#[path = "transfer_target/fixtures.rs"]
mod fixture;
#[path = "transfer_source/fixtures.rs"]
pub mod source_fixture;
mod support;
use source_fixture::{entry, group, op, range, Policy};
use voteboat::{
    application::*, bucket_counter::*, delegation::*, identity::*, log::*, reparent_commit::*,
    reparent_guard::*, reparenting::*, retirement::*, routed::*, routing::*, transfer::*,
    transfer_publication::*, transfer_source::*, transfer_target::*,
};
type Target = fixture::Target;
fn commit<A: StateMachine>(a: &mut A, id: u128, bytes: Vec<u8>) -> A::Receipt {
    a.apply_batch(&[entry(a.applied_index() + 1, id, bytes)])
        .unwrap()
        .remove(0)
}
fn tracked(
    t: &mut Target,
    log: &mut Vec<LogEntry>,
    id: u128,
    bytes: Vec<u8>,
) -> <Target as StateMachine>::Receipt {
    let e = entry(t.applied_index() + 1, id, bytes);
    let receipt = t.apply_batch(std::slice::from_ref(&e)).unwrap().remove(0);
    log.push(e);
    receipt
}
fn intent(cross: bool) -> TransferIntent {
    let mut before = source_fixture::grant().into_input();
    before.parent = Some(ParentAuthority {
        responsibility: rid(500),
        group: group(if cross { 3 } else { 1 }),
    });
    let mut after = source_fixture::intent().after().clone().into_input();
    after.parent = before.parent;
    reserved(
        ResponsibilityManifest::new(before).unwrap(),
        ResponsibilityManifest::new(after).unwrap(),
        200,
    )
}
fn reserved(
    before: ResponsibilityManifest,
    after: ResponsibilityManifest,
    id: u128,
) -> TransferIntent {
    let mut parent = parents(&before, false).0.into_input();
    parent.responsibility = before.input().parent.unwrap().responsibility;
    let parent = ResponsibilityManifest::new(parent).unwrap();
    let plan = DelegationPlan::new(parent, before, after, op(id)).unwrap();
    // Original parent observations are inputs to this owner conformance test.
    // The metadata commit/recovery protocol is covered by reparent_guards.
    DelegationReservationStatus {
        operation: op(400),
        index: 4,
        plan,
    }
    .child_intent(cfg())
    .unwrap()
}
fn rid(n: u128) -> ResponsibilityIdentity {
    ResponsibilityIdentity {
        id: ResponsibilityId::new(n).unwrap(),
        incarnation: ResponsibilityIncarnation::new(1).unwrap(),
    }
}
fn cfg() -> ConfigurationId {
    ConfigurationId::new(1).unwrap()
}
fn fresh(i: &TransferIntent, g: u128, limit: usize) -> Target {
    let scope = i
        .targets()
        .into_iter()
        .find(|r| r.target == RouteTarget::Group(group(g)))
        .unwrap()
        .scope;
    let target = TransferTarget::new(
        group(g),
        op(200),
        i.clone(),
        BucketCounter::new(scope, Policy, source_fixture::bucket_limits()).unwrap(),
        Policy,
        fixture::limits(),
    )
    .unwrap_or_else(|e| panic!("{:?}", e.0));
    if limit == 0 {
        target
    } else {
        target
            .with_parent_adoption(limit)
            .unwrap_or_else(|e| panic!("{:?}", e.0))
    }
}
fn data(m: &ResponsibilityManifest, g: u128, key: u8, delta: i64) -> Vec<u8> {
    let mut h = source_fixture::hint(key);
    h.group = group(g);
    h.epoch = m.input().epoch;
    h.generation = m.input().generation;
    h.scope = if g == 21 {
        range(0, 128)
    } else if g == 22 {
        range(128, 256)
    } else {
        range(0, 256)
    };
    encode_routed(
        h,
        &[key],
        &encode_add(&[key], delta, b"effect", 1024).unwrap(),
        4096,
    )
    .unwrap()
}
fn query(m: &ResponsibilityManifest, g: u128, key: u8) -> TargetQuery<Vec<u8>> {
    let mut h = source_fixture::hint(key);
    h.group = group(g);
    h.epoch = m.input().epoch;
    h.generation = m.input().generation;
    h.scope = if g == 21 {
        range(0, 128)
    } else if g == 22 {
        range(128, 256)
    } else {
        range(0, 256)
    };
    TargetQuery::Data(RoutedQuery {
        hint: h,
        key: vec![key],
        query: vec![key],
    })
}
fn initial(cross: bool, limit: usize) -> (TransferIntent, Vec<Target>, Vec<Vec<LogEntry>>) {
    let i = intent(cross);
    let app = RoutedApplication::new(
        group(20),
        i.before().clone(),
        BucketCounter::new(range(0, 256), Policy, source_fixture::bucket_limits()).unwrap(),
        Policy,
        RoutedLimits {
            operations: 32,
            semantic_bytes: 8192,
            payload_bytes: 1024,
            inner_checkpoint_bytes: source_fixture::bucket_limits().checkpoint_bound().unwrap(),
        },
    )
    .unwrap_or_else(|e| panic!("{:?}", e.error));
    let mut source = TransferSource::new(app, 65536).unwrap_or_else(|e| panic!("{:?}", e.0));
    let boot = source.bootstrap_command(200000).unwrap();
    commit(&mut source, 100, boot);
    commit(&mut source, 1, source_fixture::data(1, 7));
    commit(&mut source, 2, source_fixture::data(200, 11));
    commit(
        &mut source,
        200,
        TransferSource::<BucketCounter<Policy>, Policy>::freeze_command(&i, 200000).unwrap(),
    );
    let SourceRead::Freeze(Some(status)) = source
        .read_at(source.applied_index(), SourceQuery::Freeze)
        .unwrap()
    else {
        panic!("freeze")
    };
    let mut targets = vec![fresh(&i, 21, limit), fresh(&i, 22, limit)];
    let mut logs = Vec::new();
    for (t, g) in targets.iter_mut().zip([21, 22]) {
        let import = TargetImport::new(
            op(200),
            i.clone(),
            group(g),
            vec![SourceImport {
                fence: status.fence,
                configuration: cfg(),
                image: source.export_target(group(g), 65536).unwrap(),
                digest: status
                    .exports
                    .iter()
                    .find(|e| e.target == group(g))
                    .unwrap()
                    .digest,
            }],
        )
        .unwrap_or_else(|e| panic!("{:?}", e.0));
        let entries = vec![
            entry(1, 200, t.bootstrap_command(200000).unwrap()),
            entry(2, 200, t.import_command(&import, 200000).unwrap()),
        ];
        t.apply_batch(&entries).unwrap();
        logs.push(entries);
    }
    let publication = TransferPublication::new(
        op(200),
        i.clone(),
        vec![
            SourceFenceEvidence::from_status(cfg(), status).unwrap_or_else(|e| panic!("{:?}", e.0))
        ],
        targets
            .iter()
            .map(|t| {
                TargetReadyEvidence::from_status(cfg(), t.status())
                    .unwrap_or_else(|e| panic!("{:?}", e.0))
            })
            .collect(),
    )
    .unwrap_or_else(|e| panic!("{:?}", e.0));
    for (t, log) in targets.iter_mut().zip(&mut logs) {
        let a = TargetActivation {
            metadata_configuration: cfg(),
            decision: TransferPublicationStatus {
                publication_operation: op(201),
                index: 4,
                publication: publication.clone(),
            },
        };
        let e = entry(3, 200, t.activation_command(&a, 200000).unwrap());
        t.apply_batch(std::slice::from_ref(&e)).unwrap();
        log.push(e);
    }
    (i, targets, logs)
}
fn parents(
    child: &ResponsibilityManifest,
    cross: bool,
) -> (ResponsibilityManifest, ResponsibilityManifest) {
    let mut old = child.clone().into_input();
    old.responsibility = rid(500);
    old.parent = None;
    old.authority = child.input().parent.unwrap().group;
    old.execution = ExecutionMode::Delegated(vec![RouteEntry {
        scope: old.scope,
        target: RouteTarget::Child(ChildAuthority {
            responsibility: child.input().responsibility,
            group: child.input().authority,
            epoch: child.input().epoch,
        }),
    }]);
    let mut new = old.clone();
    new.responsibility = rid(600);
    new.authority = group(if cross { 2 } else { 1 });
    new.execution = ExecutionMode::Delegated(vec![RouteEntry {
        scope: new.scope,
        target: RouteTarget::Vacant,
    }]);
    (
        ResponsibilityManifest::new(old).unwrap(),
        ResponsibilityManifest::new(new).unwrap(),
    )
}
fn move_command(child: &ResponsibilityManifest, cross: bool) -> (Vec<u8>, ResponsibilityManifest) {
    let (old, new) = parents(child, cross);
    if !cross {
        let plan = ReparentPlan::new(old.clone(), new.clone(), child.clone()).unwrap();
        let a = OwnerParentAdoption {
            metadata_configuration: cfg(),
            decision: ReparentStatus {
                operation: op(700),
                index: 10,
                plan,
            },
        };
        return (a.encode(200000).unwrap(), a.after());
    }
    let plan = CrossReparentPlan::new(
        rid(500),
        rid(600),
        child.input().responsibility,
        vec![old.clone(), new.clone(), child.clone()],
    )
    .unwrap();
    let guards = plan
        .authorities()
        .into_iter()
        .map(|authority| ReparentGuardEvidence {
            authority,
            configuration: cfg(),
            operation: op(700),
            index: 8,
            digest: plan.digest(),
        })
        .collect();
    let decision = PublishReparent {
        configuration: cfg(),
        decision: ReparentDecisionStatus {
            coordinator: plan.coordinator(),
            operation: op(701),
            index: 10,
            commit: CommitReparent::new(op(700), guards).unwrap(),
        },
    };
    let digest = decision.decision.digest().unwrap();
    let child_publication = ReparentPublicationStatus {
        authority: child.input().authority,
        operation: op(701),
        index: 10,
        guard: op(700),
        decision_digest: digest,
    };
    let completion = ReleaseCommittedReparent {
        configuration: cfg(),
        completion: ReparentCompletionStatus {
            coordinator: plan.coordinator(),
            operation: op(703),
            index: 12,
            guard: op(700),
            decision_digest: digest,
        },
    };
    let a = CrossOwnerParentAdoption {
        plan,
        decision,
        child_configuration: cfg(),
        child_publication,
        completion,
    };
    (a.encode(200000).unwrap(), a.after())
}
fn reopen(t: &Target, i: &TransferIntent, limit: usize) -> Target {
    let mut n = fresh(i, t.status().group.id.get(), limit);
    n.restore_checkpoint(
        t.schema_version(),
        t.applied_index(),
        &t.checkpoint(1000000).unwrap(),
    )
    .unwrap();
    n
}
fn merge_intent(before: &ResponsibilityManifest) -> TransferIntent {
    let mut after = before.clone().into_input();
    after.epoch = OwnershipEpoch::new(after.epoch.get() + 1).unwrap();
    after.generation = RouteGeneration::new(after.generation.get() + 1).unwrap();
    after.execution = ExecutionMode::Single(group(23));
    reserved(
        before.clone(),
        ResponsibilityManifest::new(after).unwrap(),
        300,
    )
}
#[test]
fn imported_local_and_cross_moves_preserve_lineage_and_allow_later_transfer_and_retirement() {
    for cross in [false, true] {
        let (i, mut targets, mut logs) = initial(cross, 4);
        let (b, moved) = move_command(i.after(), cross);
        check_moved_targets(&i, &mut targets, &mut logs, &b, &moved);
        let next = merge_intent(&moved);
        let mut statuses = Vec::new();
        for (j, t) in targets.iter_mut().enumerate() {
            let f = t.freeze_command(&next, 65536, 200000).unwrap();
            assert!(matches!(
                tracked(t, &mut logs[j], 300, f.clone()).outcome,
                TargetOutcome::Frozen(_)
            ));
            assert!(matches!(
                tracked(t, &mut logs[j], 800, b.clone()).outcome,
                TargetOutcome::ParentAdopted(_)
            ));
            *t = reopen(t, &i, 4);
            statuses.push(t.freeze_status().unwrap().unwrap());
        }
        let mut merged = TransferTarget::new(
            group(23),
            op(300),
            next.clone(),
            BucketCounter::new(range(0, 256), Policy, source_fixture::bucket_limits()).unwrap(),
            Policy,
            fixture::limits(),
        )
        .unwrap_or_else(|e| panic!("{:?}", e.0));
        let imports = targets
            .iter()
            .zip(&statuses)
            .map(|(t, s)| SourceImport {
                fence: s.fence,
                configuration: cfg(),
                image: t.export_target(group(23), 65536).unwrap(),
                digest: s.exports[0].digest,
            })
            .collect();
        let import = TargetImport::new(op(300), next.clone(), group(23), imports)
            .unwrap_or_else(|e| panic!("{:?}", e.0));
        let boot = merged.bootstrap_command(200000).unwrap();
        commit(&mut merged, 300, boot);
        let load = merged.import_command(&import, 200000).unwrap();
        commit(&mut merged, 300, load);
        let publication = TransferPublication::new(
            op(300),
            next.clone(),
            statuses
                .iter()
                .map(|s| {
                    SourceFenceEvidence::from_status(cfg(), s.clone())
                        .unwrap_or_else(|e| panic!("{:?}", e.0))
                })
                .collect(),
            vec![TargetReadyEvidence::from_status(cfg(), merged.status())
                .unwrap_or_else(|e| panic!("{:?}", e.0))],
        )
        .unwrap_or_else(|e| panic!("{:?}", e.0));
        let decision = TransferPublicationStatus {
            publication_operation: op(301),
            index: 20,
            publication,
        };
        let a = merged
            .activation_command(
                &TargetActivation {
                    metadata_configuration: cfg(),
                    decision: decision.clone(),
                },
                200000,
            )
            .unwrap();
        commit(&mut merged, 300, a);
        assert_eq!(
            merged
                .read_at(merged.applied_index(), query(next.after(), 23, 1))
                .unwrap(),
            TargetRead::Data(10)
        );
        assert_eq!(
            merged
                .read_at(merged.applied_index(), query(next.after(), 23, 200))
                .unwrap(),
            TargetRead::Data(14)
        );
        check_retired_targets(&i, &targets, &logs, &b, &decision, &merged);
    }
}

#[test]
fn imported_parent_reserve_pending_order_conflicts_and_checkpoint_rejection_are_atomic() {
    let (i, mut targets, logs) = initial(true, 1);
    let (bytes, moved) = move_command(i.after(), true);
    let mut t = targets.remove(0);
    check_adoption_admission(&i, &mut t, &bytes);
    // Fill the provider's ordinary operation reserve. Parent adoption has its own budget.
    for id in 100..131 {
        assert!(matches!(
            commit(&mut t, id, data(i.after(), 21, 1, 1)).outcome,
            TargetOutcome::Applied(_)
        ));
    }
    assert!(t
        .validate_proposal(op(999), &data(i.after(), 21, 1, 1), std::iter::empty())
        .is_err());
    let adopted = commit(&mut t, 800, bytes.clone()).outcome;
    assert!(matches!(adopted, TargetOutcome::ParentAdopted(_)));
    let TargetOutcome::ParentAdopted(status) = adopted else {
        unreachable!()
    };
    let q = TargetQuery::ParentAdoption(op(800));
    let read = t.read_at(t.applied_index(), q.clone()).unwrap();
    assert_eq!(read, TargetRead::ParentAdoption(Some(status)));
    assert_eq!(t.query_bytes(&q, 0).unwrap(), 0);
    assert_eq!(t.read_result_bytes(&read, 0).unwrap(), 0);
    assert_eq!(
        t.read_result_bound(&q).unwrap(),
        std::mem::size_of::<TargetRead<i64>>()
    );
    assert!(t.read_at(t.applied_index() + 1, q).is_err());
    assert_eq!(
        commit(&mut t, 800, data(&moved, 21, 1, 1)).outcome,
        TargetOutcome::OperationConflict
    );
    let original_command = CrossOwnerParentAdoption::decode(&bytes).unwrap();
    let mut changed = original_command.clone();
    changed.child_configuration = ConfigurationId::new(2).unwrap();
    assert_eq!(
        commit(&mut t, 800, changed.encode(200000).unwrap()).outcome,
        TargetOutcome::OperationConflict
    );
    assert_eq!(
        commit(&mut t, 801, bytes.clone()).outcome,
        TargetOutcome::Rejected(RoutingError::WrongOwner)
    );
    let reverse = reverse_adoption(&original_command, &moved);
    assert!(t
        .validate_proposal(op(802), &reverse, std::iter::empty())
        .is_err());
    let cp = t.checkpoint(1000000).unwrap();
    let mut n = check_parent_checkpoint_rejection(&i, &t, &cp, &bytes);
    n.restore_checkpoint(PARENT_TRANSFER_TARGET_SCHEMA, t.applied_index(), &cp)
        .unwrap();
    assert_eq!(commit(&mut n, 800, bytes).outcome, adopted);
    let later = merge_intent(&moved);
    let f = n.freeze_command(&later, 65536, 200000).unwrap();
    commit(&mut n, 300, f);
    assert_eq!(
        commit(&mut n, 802, reverse).outcome,
        TargetOutcome::Rejected(RoutingError::Fenced)
    );
    let mut again = reopen(&n, &i, 1);
    assert_eq!(again.fence(), n.fence());
    assert_eq!(
        commit(&mut again, 800, original_command.encode(200000).unwrap()).outcome,
        adopted
    );
    assert_eq!(
        again.status().activated.as_ref().unwrap().index,
        logs[0][2].index
    );
}

#[cfg(feature = "native")]
#[test]
fn imported_parent_and_later_fence_recover_at_every_native_journal_cut() {
    use support::Fault;
    use voteboat::native::log_store::*;
    let (i, _, logs) = initial(true, 2);
    let (bytes, moved) = move_command(i.after(), true);
    let mut entries = logs[0].clone();
    entries.push(entry(4, 800, bytes.clone()));
    let mut owner = fresh(&i, 21, 2);
    owner.apply_batch(&entries).unwrap();
    let f = owner
        .freeze_command(&merge_intent(&moved), 65536, 200000)
        .unwrap();
    entries.push(entry(5, 300, f));
    let limits = LogLimits::default();
    for boundary in [4u64, 5] {
        let seed = || seed_parent_journal(&entries, boundary, limits);
        let (_, log) = seed();
        let mutation = support::update(
            &log.state(group(21)).unwrap(),
            1,
            boundary,
            Some(Suffix {
                from: boundary,
                entries: vec![entries[boundary as usize - 1].clone()],
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
            let (io, mut log) = seed();
            io.0.borrow_mut().fault = fault;
            if let Ok(tickets) = log.append_batch(vec![mutation.clone()]) {
                assert!(log.barrier(&tickets).is_err());
            }
            drop(log);
            io.0.borrow_mut().power_loss();
            let log = NativeLogStore::recover(io, support::identity(1), limits).unwrap();
            let state = log.state(group(21)).unwrap();
            let mut app = fresh(&i, 21, 2);
            app.apply_batch(
                &state
                    .entries
                    .into_iter()
                    .filter(|e| e.index <= state.commit_index)
                    .collect::<Vec<_>>(),
            )
            .unwrap();
            if state.commit_index == boundary {
                complete = true;
            } else {
                old = true;
                assert_eq!(state.commit_index, boundary - 1);
            }
            assert_eq!(
                app.grant(),
                if state.commit_index >= 4 {
                    &moved
                } else {
                    i.after()
                }
            );
            assert_eq!(app.fence().is_some(), state.commit_index == 5);
            let original = app.status();
            let previous = app.parent_adoption(op(800));
            let image = app
                .fence()
                .map(|_| app.export_target(group(23), 65536).unwrap());
            app = reopen(&app, &i, 2);
            assert_eq!(app.status(), original);
            let TargetOutcome::ParentAdopted(s) = commit(&mut app, 800, bytes.clone()).outcome
            else {
                panic!("adoption retry")
            };
            assert_eq!(
                s.index,
                previous.map_or(state.commit_index + 1, |p| p.index)
            );
            if let Some(image) = image {
                assert_eq!(app.export_target(group(23), 65536).unwrap(), image);
            }
            assert_eq!(app.application().outbox().count(), 1);
        }
        assert!(old && complete);
    }
}

#[test]
fn repeated_parent_chain_and_retirement_bind_the_final_grant() {
    let (i, mut owners, _) = initial(false, 2);
    let (first, moved) = move_command(i.after(), false);
    let mut t = owners.remove(0);
    commit(&mut t, 800, first.clone());
    let a = OwnerParentAdoption::decode(&first).unwrap();
    let [old, new, child] = a.decision.plan.updated_manifests();
    let back = OwnerParentAdoption {
        metadata_configuration: cfg(),
        decision: ReparentStatus {
            operation: op(710),
            index: 15,
            plan: ReparentPlan::new(new, old, child).unwrap(),
        },
    };
    let bytes = back.encode(200000).unwrap();
    assert!(matches!(
        commit(&mut t, 801, bytes.clone()).outcome,
        TargetOutcome::ParentAdopted(_)
    ));
    t = reopen(&t, &i, 2);
    assert_eq!(t.grant(), &back.after());
    assert_eq!(t.grant().input().parent, i.after().input().parent);
    assert!(t.grant().input().generation > i.after().input().generation);
    assert!(matches!(
        commit(&mut t, 800, first.clone()).outcome,
        TargetOutcome::ParentAdopted(_)
    ));

    // Recompute the checksum after a semantic record mutation: replay must
    // validate the chain and boundaries as well as the byte integrity.
    let cp = t.checkpoint(1000000).unwrap();
    let record = cp.len() - bytes.len() - 60;
    for (operation, index, command) in [
        (op(200), 5, bytes.clone()),
        (op(801), 3, bytes.clone()),
        (op(801), 5, first.clone()),
    ] {
        let mut body = Vec::new();
        body.extend(b"VBTPARD1");
        body.extend(operation.get().to_le_bytes());
        body.extend((index as u64).to_le_bytes());
        body.extend(&command);
        let digest = ContentDigest::sha256(&body);
        let mut bad = cp[..record].to_vec();
        bad.extend(digest.0);
        bad.extend(operation.get().to_le_bytes());
        bad.extend((index as u64).to_le_bytes());
        bad.extend((command.len() as u32).to_le_bytes());
        bad.extend(command);
        let mut n = fresh(&i, 21, 2);
        let pristine = n.checkpoint(1000000).unwrap();
        assert!(n
            .restore_checkpoint(PARENT_TRANSFER_TARGET_SCHEMA, t.applied_index(), &bad)
            .is_err());
        assert_eq!(n.checkpoint(1000000).unwrap(), pristine);
    }
    let final_grant = t.grant().clone();
    let intent = merge_intent(&final_grant);
    let freeze = t.freeze_command(&intent, 65536, 200000).unwrap();
    commit(&mut t, 300, freeze);
    let status = t.freeze_status().unwrap().unwrap();
    let lineage = t.retirement_lineage().unwrap();
    let template = fresh(&i, 21, 2);
    template
        .validate_retirement_evidence(&status, &lineage)
        .unwrap();
    // An individually well-formed final-grant lineage cannot be paired with
    // another parent state, even though the data ownership fields are identical.
    let mut other = fresh(&i, 21, 2);
    other
        .restore_checkpoint(
            PARENT_TRANSFER_TARGET_SCHEMA,
            t.applied_index(),
            &t.checkpoint(1000000).unwrap(),
        )
        .unwrap();
    let mut altered = status.clone();
    altered.intent = merge_intent(&moved);
    assert!(template
        .validate_retirement_evidence(&altered, &lineage)
        .is_err());
    for end in 0..lineage.len() {
        assert!(template
            .validate_retirement_evidence(&status, &lineage[..end])
            .is_err());
    }
    assert!(matches!(
        commit(&mut other, 801, bytes).outcome,
        TargetOutcome::ParentAdopted(_)
    ));
}

fn check_moved_targets(
    i: &TransferIntent,
    targets: &mut [Target],
    logs: &mut [Vec<LogEntry>],
    b: &[u8],
    moved: &ResponsibilityManifest,
) {
    let original: Vec<_> = targets.iter().map(Target::status).collect();
    for (j, t) in targets.iter_mut().enumerate() {
        let g = 21 + j as u128;
        let key = if j == 0 { 1 } else { 200 };
        assert!(matches!(
            tracked(t, &mut logs[j], 800, b.to_vec()).outcome,
            TargetOutcome::ParentAdopted(_)
        ));
        *t = reopen(t, i, 4);
        assert_eq!(t.grant(), moved);
        assert_eq!(t.status(), original[j]);
        let original_entries = logs[j][..3].to_vec();
        let EntryPayload::Command { bytes: boot, .. } = &original_entries[0].payload else {
            unreachable!()
        };
        assert_eq!(&t.bootstrap_command(200000).unwrap(), boot);
        for e in &original_entries {
            let EntryPayload::Command { bytes, .. } = &e.payload else {
                unreachable!()
            };
            assert!(matches!(
                tracked(t, &mut logs[j], 200, bytes.clone()).outcome,
                TargetOutcome::Staged { .. }
                    | TargetOutcome::Imported { .. }
                    | TargetOutcome::Activated(_)
            ));
        }
        let before = t.application().outbox().count();
        assert!(matches!(
            tracked(
                t,
                &mut logs[j],
                1 + j as u128,
                data(moved, g, key, if j == 0 { 7 } else { 11 })
            )
            .outcome,
            TargetOutcome::Applied(_)
        ));
        assert_eq!(t.application().outbox().count(), before);
        assert!(matches!(
            tracked(t, &mut logs[j], 900 + j as u128, data(moved, g, key, 3)).outcome,
            TargetOutcome::Applied(_)
        ));
        assert_eq!(
            t.read_at(t.applied_index(), query(moved, g, key)).unwrap(),
            TargetRead::Data(if j == 0 { 10 } else { 14 })
        );
    }
}

fn check_retired_targets(
    i: &TransferIntent,
    targets: &[Target],
    logs: &[Vec<LogEntry>],
    b: &[u8],
    decision: &TransferPublicationStatus,
    merged: &Target,
) {
    // Replay the exact owner history under retirement from its original template.
    for (j, t) in targets.iter().enumerate() {
        let mut guard = RetirementGuard::new(fresh(i, 21 + j as u128, 4))
            .unwrap_or_else(|e| panic!("{:?}", e.0));
        // The wrapper is present from construction; replay every original input.
        guard.apply_batch(&logs[j]).unwrap();
        commit(&mut guard, 800, b.to_vec());
        let s = guard.freeze_status().unwrap().unwrap();
        assert_eq!(
            guard
                .owner()
                .unwrap()
                .application()
                .checkpoint(65536)
                .unwrap(),
            t.application().checkpoint(65536).unwrap()
        );
        let proof = RetirementProof {
            metadata_configuration: cfg(),
            decision: decision.clone(),
            targets: vec![
                TargetActivationEvidence::from_status(cfg(), merged.status())
                    .unwrap_or_else(|e| panic!("{:?}", e.0)),
            ],
            release: RetentionRelease {
                source: s.fence.group,
                operation: op(300),
                fence_index: s.fence.index,
                release: op(999),
            },
        };
        let command = guard.retirement_command(&proof, 200000).unwrap();
        commit(&mut guard, 300, command);
        assert!(guard.owner().is_none());
        let mut restored = RetirementGuard::new(fresh(i, 21 + j as u128, 4))
            .unwrap_or_else(|e| panic!("{:?}", e.0));
        restored
            .restore_checkpoint(
                guard.schema_version(),
                guard.applied_index(),
                &guard.checkpoint(1000000).unwrap(),
            )
            .unwrap();
        assert_eq!(restored.freeze_status().unwrap(), Some(s));
        assert!(restored.owner().is_none());
    }
}

fn check_adoption_admission(i: &TransferIntent, t: &mut Target, bytes: &[u8]) {
    for n in [0, 65, usize::MAX] {
        assert!(fresh(i, 21, 0).with_parent_adoption(n).is_err());
    }
    assert!(t.clone().with_parent_adoption(1).is_err());
    let mut old = fresh(i, 21, 0);
    let old_boot = old.bootstrap_command(200000).unwrap();
    commit(&mut old, 200, old_boot);
    assert!(old
        .validate_proposal(op(800), bytes, std::iter::empty())
        .is_err());
    assert!(fresh(i, 21, 1)
        .validate_proposal(op(800), bytes, std::iter::empty())
        .is_err());
    for id in [1, 200] {
        assert!(t
            .validate_proposal(op(id), bytes, std::iter::empty())
            .is_err());
    }
    assert!(t
        .validate_proposal(op(800), bytes, [(op(800), bytes)].into_iter())
        .is_ok());
    assert!(t
        .validate_proposal(op(801), bytes, [(op(800), bytes)].into_iter())
        .is_err());
    let original = t.checkpoint(1000000).unwrap();
    let index = t.applied_index();
    assert!(t
        .apply_batch(&[
            entry(index + 1, 800, bytes.to_vec()),
            entry(index + 2, 1000, vec![0])
        ])
        .is_err());
    assert_eq!(t.checkpoint(1000000).unwrap(), original);
}

fn reverse_adoption(
    original_command: &CrossOwnerParentAdoption,
    moved: &ResponsibilityManifest,
) -> Vec<u8> {
    let updated = original_command.plan.updated_manifests();
    let back_plan = CrossReparentPlan::new(
        rid(600),
        rid(500),
        moved.input().responsibility,
        updated.into_iter().collect(),
    )
    .unwrap();
    // The independent lifetime budget is exhausted even for a valid reverse move.
    let guards = back_plan
        .authorities()
        .into_iter()
        .map(|authority| ReparentGuardEvidence {
            authority,
            configuration: cfg(),
            operation: op(710),
            index: 20,
            digest: back_plan.digest(),
        })
        .collect();
    let decision = PublishReparent {
        configuration: cfg(),
        decision: ReparentDecisionStatus {
            coordinator: back_plan.coordinator(),
            operation: op(711),
            index: 21,
            commit: CommitReparent::new(op(710), guards).unwrap(),
        },
    };
    let digest = decision.decision.digest().unwrap();
    CrossOwnerParentAdoption {
        plan: back_plan,
        decision,
        child_configuration: cfg(),
        child_publication: ReparentPublicationStatus {
            authority: group(1),
            operation: op(711),
            index: 21,
            guard: op(710),
            decision_digest: digest,
        },
        completion: ReleaseCommittedReparent {
            configuration: cfg(),
            completion: ReparentCompletionStatus {
                coordinator: group(1),
                operation: op(712),
                index: 22,
                guard: op(710),
                decision_digest: digest,
            },
        },
    }
    .encode(200000)
    .unwrap()
}

#[cfg(feature = "native")]
fn seed_parent_journal(
    entries: &[LogEntry],
    boundary: u64,
    limits: LogLimits,
) -> (
    support::ModelIo,
    voteboat::native::log_store::NativeLogStore<support::ModelIo>,
) {
    use support::ModelIo;
    use voteboat::native::log_store::*;
    let io = ModelIo::default();
    let mut log = NativeLogStore::create(io.clone(), support::identity(1), limits).unwrap();
    support::append(
        &mut log,
        vec![LogMutation::Create(support::bootstrap(21, 3))],
    );
    let state = log.state(group(21)).unwrap();
    support::append(
        &mut log,
        vec![support::update(
            &state,
            1,
            boundary - 1,
            Some(Suffix {
                from: 1,
                entries: entries[..boundary as usize - 1].to_vec(),
            }),
        )],
    );
    (io, log)
}

fn check_parent_checkpoint_rejection(
    i: &TransferIntent,
    t: &Target,
    cp: &[u8],
    bytes: &[u8],
) -> Target {
    let mut n = fresh(i, 21, 1);
    let pristine = n.checkpoint(1000000).unwrap();
    for end in 0..cp.len() {
        assert!(n
            .restore_checkpoint(PARENT_TRANSFER_TARGET_SCHEMA, t.applied_index(), &cp[..end])
            .is_err());
        assert_eq!(n.checkpoint(1000000).unwrap(), pristine);
    }
    let record = cp.len() - bytes.len() - 60;
    for offset in [record, record + 32, record + 48, record + 56, record + 60] {
        let mut bad = cp.to_vec();
        bad[offset] ^= 0xff;
        assert!(n
            .restore_checkpoint(PARENT_TRANSFER_TARGET_SCHEMA, t.applied_index(), &bad)
            .is_err());
        assert_eq!(n.checkpoint(1000000).unwrap(), pristine);
    }
    assert!(fresh(i, 21, 0)
        .restore_checkpoint(PARENT_TRANSFER_TARGET_SCHEMA, t.applied_index(), cp)
        .is_err());
    assert!(fresh(i, 21, 2)
        .restore_checkpoint(PARENT_TRANSFER_TARGET_SCHEMA, t.applied_index(), cp)
        .is_err());

    n
}
