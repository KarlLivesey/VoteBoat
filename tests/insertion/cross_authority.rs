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
use voteboat::delegation::*;
fn configuration() -> ConfigurationId {
    ConfigurationId::new(1).unwrap()
}
fn before() -> ResponsibilityManifest {
    let mut m = grant().into_input();
    m.parent = Some(ParentAuthority {
        responsibility: responsibility(500),
        group: group(100),
    });
    ResponsibilityManifest::new(m).unwrap()
}
fn responsibility(n: u128) -> ResponsibilityIdentity {
    ResponsibilityIdentity {
        id: ResponsibilityId::new(n).unwrap(),
        incarnation: ResponsibilityIncarnation::new(1).unwrap(),
    }
}
fn parent() -> ResponsibilityManifest {
    let mut m = grant().into_input();
    m.responsibility = responsibility(500);
    m.authority = group(100);
    m.execution = ExecutionMode::Delegated(vec![RouteEntry {
        scope: m.scope,
        target: RouteTarget::Child(ChildAuthority {
            responsibility: before().input().responsibility,
            group: group(1),
            epoch: m.epoch,
        }),
    }]);
    ResponsibilityManifest::new(m).unwrap()
}
fn fresh(m: ResponsibilityManifest, cross: bool) -> Directory {
    let d = Directory::new(
        DirectoryPlan::new(m.input().authority, vec![m]).unwrap(),
        DirectoryLimits {
            operations: 32,
            history_bytes: 200000,
        },
    )
    .unwrap();
    if cross {
        d.with_cross_authority_insertion()
            .unwrap_or_else(|_| panic!("schema7"))
    } else {
        d.with_recursive_insertion()
            .unwrap_or_else(|_| panic!("schema6"))
    }
}
fn initialized(m: ResponsibilityManifest, cross: bool) -> Directory {
    let mut d = fresh(m.clone(), cross);
    let b = d.bootstrap_command(100000).unwrap();
    commit(&mut d, 1000, b);
    assert!(matches!(
        commit(
            &mut d,
            1001,
            DirectoryCommand {
                expected: None,
                manifest: m
            }
            .encode(100000)
            .unwrap()
        )
        .outcome,
        DirectoryOutcome::Published(_)
    ));
    d
}
fn recover(d: &Directory) -> Directory {
    let cp = d.checkpoint(1000000).unwrap();
    let mut r = Directory::new(d.plan().clone(), d.limits())
        .unwrap()
        .with_cross_authority_insertion()
        .unwrap_or_else(|_| panic!("recovery schema"));
    r.restore_checkpoint(7, d.applied_index(), &cp).unwrap();
    assert_eq!(r.checkpoint(1000000).unwrap(), cp);
    r
}
fn setup() -> (Directory, Directory, DelegationPlan) {
    let p = initialized(parent(), true);
    let mut d = initialized(before(), true);
    let mut children = Vec::new();
    for (g, scope) in [(21, range(0, 128)), (22, range(128, 256))] {
        let mut m = before().into_input();
        m.responsibility = responsibility(g);
        m.parent = Some(ParentAuthority {
            responsibility: before().input().responsibility,
            group: group(1),
        });
        m.scope = scope;
        m.execution = ExecutionMode::Single(group(g));
        let m = ResponsibilityManifest::new(m).unwrap();
        let c = GroupCreationIntent {
            authority: group(1),
            parent: before().input().responsibility,
            expected: before().input().generation,
            responsibility: m.input().responsibility,
            bootstrap: support::bootstrap(g, 3),
            application: m.input().application,
            mode: GroupCreationMode::Staging,
        };
        assert_eq!(
            commit(&mut d, g, c.encode(100000).unwrap()).outcome,
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
    let mut a = before().into_input();
    a.epoch = OwnershipEpoch::new(2).unwrap();
    a.generation = RouteGeneration::new(2).unwrap();
    a.execution = ExecutionMode::Delegated(
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
    let plan = DelegationPlan::cross_authority_insertion(
        parent(),
        before(),
        ResponsibilityManifest::new(a).unwrap(),
        children,
        op(300),
    )
    .unwrap();
    (p, d, plan)
}
fn reserve(
    p: &mut Directory,
    plan: &DelegationPlan,
) -> (DelegationReservationStatus, TransferIntent) {
    assert_eq!(
        commit(p, 400, plan.encode(100000).unwrap()).outcome,
        DirectoryOutcome::DelegationReserved
    );
    let reservation = p
        .delegation_reservation_at(p.applied_index(), op(400))
        .unwrap()
        .unwrap();
    let intent = reservation.child_intent(configuration()).unwrap();
    (reservation, intent)
}
#[test]
fn cross_authority_codecs_and_local_creation_admission_preserve_legacy_schema() {
    let (mut p, mut d, plan) = setup();
    let bytes = plan.encode(100000).unwrap();
    assert_eq!(&bytes[..8], b"VBDPLAN3");
    assert_eq!(DelegationPlan::decode(&bytes).unwrap(), plan);
    for end in 0..bytes.len() {
        assert!(DelegationPlan::decode(&bytes[..end]).is_err());
    }
    let mut downgraded = bytes.clone();
    downgraded[..8].copy_from_slice(b"VBDPLAN2");
    assert!(DelegationPlan::decode(&downgraded).is_err());
    assert!(DelegationPlan::insertion(
        plan.parent().clone(),
        plan.before().clone(),
        plan.after().clone(),
        plan.insertion_children().unwrap().to_vec(),
        op(300)
    )
    .is_err());
    let (_, intent) = reserve(&mut p, &plan);
    let bytes = intent.encode(100000).unwrap();
    assert_eq!(&bytes[..8], b"VBTINT05");
    assert_eq!(TransferIntent::decode(&bytes).unwrap(), intent);
    for end in 0..bytes.len() {
        assert!(TransferIntent::decode(&bytes[..end]).is_err());
    }
    let mut downgraded = bytes.clone();
    downgraded[..8].copy_from_slice(b"VBTINT04");
    assert!(TransferIntent::decode(&downgraded).is_err());
    let mut legacy = initialized(parent(), false);
    assert!(legacy
        .validate_proposal(op(400), &plan.encode(100000).unwrap(), std::iter::empty())
        .is_err());
    let mut legacy_child = initialized(before(), false);
    assert!(legacy_child
        .validate_proposal(op(300), &bytes, std::iter::empty())
        .is_err());
    let decline = DelegationDecline::new(intent.clone())
        .unwrap()
        .encode(100000)
        .unwrap();
    assert!(legacy_child
        .validate_proposal(op(300), &decline, std::iter::empty())
        .is_err());
    let mut no_creations = initialized(before(), true);
    assert_eq!(
        commit(&mut no_creations, 300, bytes.clone()).outcome,
        DirectoryOutcome::TransferEvidenceMismatch
    );
    // Changed foreign references cannot replace the child's verifiable local records.
    let mut changed = plan.insertion_children().unwrap().to_vec();
    changed[0].creation_index += 1;
    let changed = DelegationPlan::cross_authority_insertion(
        plan.parent().clone(),
        plan.before().clone(),
        plan.after().clone(),
        changed,
        op(301),
    )
    .unwrap();
    let changed = DelegationReservationStatus {
        operation: op(401),
        index: 99,
        plan: changed,
    }
    .child_intent(configuration())
    .unwrap();
    assert_eq!(
        commit(&mut d, 301, changed.encode(100000).unwrap()).outcome,
        DirectoryOutcome::TransferEvidenceMismatch
    );
    assert_eq!(
        commit(&mut d, 300, bytes).outcome,
        DirectoryOutcome::TransferIntentRecorded
    );
    let cp = d.checkpoint(1000000).unwrap();
    assert!(legacy_child
        .restore_checkpoint(7, d.applied_index(), &cp)
        .is_err());
    assert!(d.clone().with_cross_authority_insertion().is_err());
    legacy = recover(&p);
    assert_eq!(legacy.schema_version(), 7);
}
#[test]
fn cross_authority_insert_fence_import_publish_refresh_activate_and_retry_survive_checkpoints() {
    let (mut p, mut d, plan) = setup();
    let (reservation, intent) = reserve(&mut p, &plan);
    p = recover(&p);
    assert_eq!(
        commit(&mut d, 300, intent.encode(100000).unwrap()).outcome,
        DirectoryOutcome::TransferIntentRecorded
    );
    d = recover(&d);
    let (s, status) = frozen_cross_authority_source(&intent);
    let (mut targets, decision) = nested::handoff(&mut d, &intent, 300, status, |g| {
        s.export_target(g, 65536).unwrap()
    });
    assert!(targets.iter().all(|t| t.status().activated.is_none()));
    for (i, g, key) in [(0, 21, 1), (1, 22, 200)] {
        let cp = targets[i].checkpoint(100000).unwrap();
        let mut reopened = nested::target_for(&intent, g, 300);
        reopened
            .restore_checkpoint(targets[i].schema_version(), targets[i].applied_index(), &cp)
            .unwrap();
        assert_eq!(
            reopened
                .read_at(
                    reopened.applied_index(),
                    TargetQuery::Data(RoutedQuery {
                        hint: nested::source_hint(
                            intent.target_manifest(group(g)).unwrap(),
                            g,
                            key
                        ),
                        key: vec![key],
                        query: vec![key],
                    })
                )
                .unwrap(),
            TargetRead::NotActive
        );
        targets[i] = reopened;
    }
    d = recover(&d);
    assert_eq!(p.manifest(parent().input().responsibility), Some(&parent()));
    assert_eq!(
        d.manifest(before().input().responsibility),
        Some(intent.after())
    );
    for c in intent.insertion_children().unwrap() {
        assert_eq!(
            d.manifest(c.manifest.input().responsibility),
            Some(&c.manifest)
        );
    }
    let completion = DelegationCompletion {
        reservation: op(400),
        reservation_index: reservation.index,
        parent_configuration: configuration(),
        child_configuration: configuration(),
        decision: decision.clone(),
    };
    assert!(matches!(
        commit(&mut p, 401, completion.encode(100000).unwrap()).outcome,
        DirectoryOutcome::DelegationPublished(_)
    ));
    p = recover(&p);
    assert_eq!(
        p.delegation_publication_at(p.applied_index(), op(400))
            .unwrap()
            .unwrap()
            .completion,
        completion
    );
    assert_eq!(
        commit(&mut p, 401, completion.encode(100000).unwrap()).outcome,
        DirectoryOutcome::DelegationPublished(RouteGeneration::new(2).unwrap())
    );
    nested::activate(&mut targets, 300, decision);
    exercise_cross_authority_targets(&targets, &intent);
}

#[cfg(feature = "native")]
#[test]
fn cross_authority_native_intent_torn_frames_recover_local_creations_and_lock() {
    use support::Fault;
    use voteboat::{log::*, native::log_store::*};
    let (mut p, d, plan) = setup();
    let (_, intent) = reserve(&mut p, &plan);
    let mut prefix = vec![
        entry(1, 1000, d.bootstrap_command(100000).unwrap()),
        entry(
            2,
            1001,
            DirectoryCommand {
                expected: None,
                manifest: before(),
            }
            .encode(100000)
            .unwrap(),
        ),
    ];
    for (index, g) in [(3, 21), (4, 22)] {
        let status = d
            .group_creation_at(d.applied_index(), group(g))
            .unwrap()
            .unwrap();
        prefix.push(entry(index, g, status.intent.encode(100000).unwrap()));
    }
    let limits = LogLimits::default();
    let seed = || seed_cross_authority_intent(&prefix);
    let (_, log) = seed();
    let state = log.state(group(1)).unwrap();
    let mutation = support::update(
        &state,
        1,
        5,
        Some(Suffix {
            from: 5,
            entries: vec![entry(5, 300, intent.encode(100000).unwrap())],
        }),
    );
    let frame = NativeLogCodec
        .encode_batch(3, std::slice::from_ref(&mutation), limits)
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
        let recovered = NativeLogStore::recover(io, support::identity(1), limits).unwrap();
        let state = recovered.state(group(1)).unwrap();
        let mut app = fresh(before(), true);
        app.apply_batch(
            &state
                .entries
                .into_iter()
                .filter(|e| e.index <= state.commit_index)
                .collect::<Vec<_>>(),
        )
        .unwrap();
        let status = app.transfer_intent_at(state.commit_index, op(300)).unwrap();
        if state.commit_index == 4 {
            old = true;
            assert!(status.is_none());
        } else {
            complete = true;
            assert_eq!(state.commit_index, 5);
            assert_eq!(status.unwrap().intent, intent);
        }
        for c in intent.insertion_children().unwrap() {
            assert!(app.manifest(c.manifest.input().responsibility).is_none());
        }
        assert_eq!(
            app.manifest(before().input().responsibility),
            Some(&before())
        );
    }
    assert!(old && complete);
}

#[test]
fn cross_authority_pre_intent_refusal_cancels_parent_but_accepted_intent_is_forward_only() {
    let (mut p, mut d, plan) = setup();
    let (reservation, intent) = reserve(&mut p, &plan);
    let decline = DelegationDecline::new(intent.clone()).unwrap();
    assert_eq!(
        commit(&mut d, 399, decline.encode(100000).unwrap()).outcome,
        DirectoryOutcome::DelegationDeclined
    );
    d = recover(&d);
    let status = d
        .delegation_decline_at(d.applied_index(), op(300))
        .unwrap()
        .unwrap();
    let cancellation = DelegationCancellation {
        reservation: op(400),
        reservation_index: reservation.index,
        parent_configuration: configuration(),
        child_configuration: configuration(),
        decline: status,
    };
    assert_eq!(
        commit(&mut p, 401, cancellation.encode(100000).unwrap()).outcome,
        DirectoryOutcome::DelegationCancelled
    );
    p = recover(&p);
    assert_eq!(p.manifest(parent().input().responsibility), Some(&parent()));
    assert_eq!(
        commit(&mut d, 300, intent.encode(100000).unwrap()).outcome,
        DirectoryOutcome::DelegationDeclined
    );
    let fresh = DelegationPlan::cross_authority_insertion(
        plan.parent().clone(),
        plan.before().clone(),
        plan.after().clone(),
        plan.insertion_children().unwrap().to_vec(),
        op(302),
    )
    .unwrap();
    assert_eq!(
        commit(&mut p, 402, fresh.encode(100000).unwrap()).outcome,
        DirectoryOutcome::DelegationReserved
    );
    let next = p
        .delegation_reservation_at(p.applied_index(), op(402))
        .unwrap()
        .unwrap()
        .child_intent(configuration())
        .unwrap();
    assert_eq!(
        commit(&mut d, 302, next.encode(100000).unwrap()).outcome,
        DirectoryOutcome::TransferIntentRecorded
    );
    // No new decline can revoke a successfully recorded child intent.
    assert_eq!(
        commit(
            &mut d,
            303,
            DelegationDecline::new(next)
                .unwrap()
                .encode(100000)
                .unwrap()
        )
        .outcome,
        DirectoryOutcome::LifecycleBusy
    );
}

fn frozen_cross_authority_source(intent: &TransferIntent) -> (Source, SourceFreezeStatus) {
    let mut s = Source::new(
        RoutedApplication::new(
            group(20),
            before(),
            BucketCounter::new(range(0, 256), Policy, bucket_limits()).unwrap(),
            Policy,
            RoutedLimits {
                operations: 32,
                semantic_bytes: 8192,
                payload_bytes: 1024,
                inner_checkpoint_bytes: bucket_limits().checkpoint_bound().unwrap(),
            },
        )
        .unwrap_or_else(|e| panic!("{:?}", e.error)),
        65536,
    )
    .unwrap_or_else(|e| panic!("{:?}", e.0));
    let source_template = s.clone();
    let boot = s.bootstrap_command(100000).unwrap();
    commit(&mut s, 100, boot);
    commit(&mut s, 1, nested::data_for(&before(), 20, 1, 7));
    commit(&mut s, 2, nested::data_for(&before(), 20, 200, 11));
    commit(&mut s, 300, Source::freeze_command(intent, 100000).unwrap());
    let SourceRead::Freeze(Some(status)) =
        s.read_at(s.applied_index(), SourceQuery::Freeze).unwrap()
    else {
        panic!("fence");
    };
    let cp = s.checkpoint(100000).unwrap();
    let mut reopened = source_template;
    reopened
        .restore_checkpoint(s.schema_version(), s.applied_index(), &cp)
        .unwrap();
    s = reopened;
    assert_eq!(
        s.read_at(
            s.applied_index(),
            SourceQuery::Data(RoutedQuery {
                hint: nested::source_hint(&before(), 20, 1),
                key: vec![1],
                query: vec![1]
            })
        )
        .unwrap(),
        SourceRead::Data(RoutedRead::Rejected(RoutingError::Fenced))
    );
    (s, status)
}

fn exercise_cross_authority_targets(targets: &[Target], intent: &TransferIntent) {
    for (i, g, key, value, operation) in [(0, 21, 1, 7, 1), (1, 22, 200, 11, 2)] {
        let m = intent.target_manifest(group(g)).unwrap();
        let cp = targets[i].checkpoint(100000).unwrap();
        let mut t = super::nested::target_for(intent, g, 300);
        t.restore_checkpoint(targets[i].schema_version(), targets[i].applied_index(), &cp)
            .unwrap();
        assert!(matches!(
            commit(&mut t, operation, nested::data_for(m, g, key, value)).outcome,
            TargetOutcome::Applied(BucketReceipt {
                duplicate: true,
                ..
            })
        ));
        assert_eq!(t.application().outbox().count(), 1);
        assert!(matches!(
            commit(&mut t, 3, nested::data_for(m, g, key, 1)).outcome,
            TargetOutcome::Applied(BucketReceipt {
                duplicate: false,
                ..
            })
        ));
        assert_eq!(
            t.read_at(
                t.applied_index(),
                TargetQuery::Data(RoutedQuery {
                    hint: nested::source_hint(m, g, key),
                    key: vec![key],
                    query: vec![key]
                })
            )
            .unwrap(),
            TargetRead::Data(value + 1)
        );
    }
}

#[cfg(feature = "native")]
fn seed_cross_authority_intent(
    prefix: &[voteboat::log::LogEntry],
) -> (
    support::ModelIo,
    voteboat::native::log_store::NativeLogStore<support::ModelIo>,
) {
    use support::ModelIo;
    use voteboat::{log::*, native::log_store::*};
    let limits = LogLimits::default();
    let io = ModelIo::default();
    let mut log = NativeLogStore::create(io.clone(), support::identity(1), limits).unwrap();
    support::append(
        &mut log,
        vec![LogMutation::Create(support::bootstrap(1, 3))],
    );
    let state = log.state(group(1)).unwrap();
    support::append(
        &mut log,
        vec![support::update(
            &state,
            1,
            4,
            Some(Suffix {
                from: 1,
                entries: prefix.to_vec(),
            }),
        )],
    );
    (io, log)
}
