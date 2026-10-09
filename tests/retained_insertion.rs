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

#[path = "transfer_source/fixtures.rs"]
mod fixture;
mod support;
use fixture::*;
use voteboat::{
    application::*, bucket_counter::*, delegation::*, directory::*, identity::*, routed::*,
    routing::*, scoped_source::*, transfer::*, transfer_publication::*, transfer_target::*,
};
type Source = ScopedTransferSource<BucketCounter<Policy>, Policy>;
type Target = TransferTarget<BucketCounter<Policy>, Policy>;
fn commit<A: StateMachine>(a: &mut A, id: u128, bytes: Vec<u8>) -> A::Receipt {
    a.apply_batch(&[entry(a.applied_index() + 1, id, bytes)])
        .unwrap()
        .remove(0)
}
fn fresh(m: ResponsibilityManifest, selected: bool) -> Directory {
    let d = Directory::new(
        DirectoryPlan::new(m.input().authority, vec![m]).unwrap(),
        DirectoryLimits {
            operations: 32,
            history_bytes: 200000,
        },
    )
    .unwrap();
    if selected {
        d.with_retained_insertion()
            .unwrap_or_else(|_| panic!("schema8"))
    } else {
        d.with_cross_authority_insertion()
            .unwrap_or_else(|_| panic!("schema7"))
    }
}
fn initial(m: ResponsibilityManifest) -> Directory {
    let mut d = fresh(m.clone(), true);
    let boot = d.bootstrap_command(100000).unwrap();
    commit(&mut d, 1000, boot);
    commit(
        &mut d,
        1001,
        DirectoryCommand {
            expected: None,
            manifest: m,
        }
        .encode(100000)
        .unwrap(),
    );
    d
}
fn recover(d: &Directory) -> Directory {
    let mut next = fresh(d.plan().manifests().next().unwrap().clone(), true);
    next.restore_checkpoint(8, d.applied_index(), &d.checkpoint(100000).unwrap())
        .unwrap();
    next
}
fn rid(n: u128) -> ResponsibilityIdentity {
    ResponsibilityIdentity {
        id: ResponsibilityId::new(n).unwrap(),
        incarnation: ResponsibilityIncarnation::new(1).unwrap(),
    }
}
fn before(foreign: bool) -> ResponsibilityManifest {
    let mut m = grant().into_input();
    if foreign {
        m.parent = Some(ParentAuthority {
            responsibility: rid(500),
            group: group(100),
        });
    }
    ResponsibilityManifest::new(m).unwrap()
}
fn parent(b: &ResponsibilityManifest) -> ResponsibilityManifest {
    let mut m = grant().into_input();
    m.responsibility = rid(500);
    m.authority = group(100);
    m.execution = ExecutionMode::Delegated(vec![RouteEntry {
        scope: b.input().scope,
        target: RouteTarget::Child(ChildAuthority {
            responsibility: b.input().responsibility,
            group: b.input().authority,
            epoch: b.input().epoch,
        }),
    }]);
    ResponsibilityManifest::new(m).unwrap()
}
fn setup(foreign: bool) -> (Directory, Option<Directory>, TransferIntent) {
    let b = before(foreign);
    let mut d = initial(b.clone());
    let mut c = b.clone().into_input();
    c.parent = Some(ParentAuthority {
        responsibility: b.input().responsibility,
        group: b.input().authority,
    });
    c.responsibility = rid(21);
    c.scope = range(0, 128);
    c.execution = ExecutionMode::Single(group(21));
    let c = ResponsibilityManifest::new(c).unwrap();
    let creation = GroupCreationIntent {
        authority: b.input().authority,
        parent: b.input().responsibility,
        expected: b.input().generation,
        responsibility: c.input().responsibility,
        bootstrap: support::bootstrap(21, 3),
        application: c.input().application,
        mode: GroupCreationMode::Staging,
    };
    assert_eq!(
        commit(&mut d, 21, creation.encode(100000).unwrap()).outcome,
        DirectoryOutcome::CreationReserved
    );
    let status = d
        .group_creation_at(d.applied_index(), group(21))
        .unwrap()
        .unwrap();
    let child = InsertionChild::from_creation(c.clone(), &status).unwrap();
    let mut after = b.clone().into_input();
    after.epoch = OwnershipEpoch::new(2).unwrap();
    after.generation = RouteGeneration::new(2).unwrap();
    after.execution = ExecutionMode::Delegated(vec![
        RouteEntry {
            scope: c.input().scope,
            target: RouteTarget::Child(ChildAuthority {
                responsibility: c.input().responsibility,
                group: c.input().authority,
                epoch: c.input().epoch,
            }),
        },
        RouteEntry {
            scope: range(128, 256),
            target: RouteTarget::Group(group(20)),
        },
    ]);
    let after = ResponsibilityManifest::new(after).unwrap();
    if foreign {
        let mut p = initial(parent(&b));
        let plan =
            DelegationPlan::retained_insertion(parent(&b), b, after, child, op(200)).unwrap();
        let bytes = plan.encode(100000).unwrap();
        assert_eq!(DelegationPlan::decode(&bytes).unwrap(), plan);
        for end in 0..bytes.len() {
            assert!(DelegationPlan::decode(&bytes[..end]).is_err());
        }
        for tag in [b"VBDPLAN1", b"VBDPLAN2", b"VBDPLAN3"] {
            let mut old = bytes.clone();
            old[..8].copy_from_slice(tag);
            assert!(DelegationPlan::decode(&old).is_err());
        }
        assert_eq!(
            commit(&mut p, 400, bytes).outcome,
            DirectoryOutcome::DelegationReserved
        );
        let reservation = p
            .delegation_reservation_at(p.applied_index(), op(400))
            .unwrap()
            .unwrap();
        (
            d,
            Some(p),
            reservation
                .child_intent(ConfigurationId::new(1).unwrap())
                .unwrap(),
        )
    } else {
        (
            d,
            None,
            TransferIntent::insert_retained_child(b, after, child).unwrap(),
        )
    }
}
fn source(intent: &TransferIntent) -> Source {
    let r = RoutedApplication::new(
        group(20),
        intent.before().clone(),
        BucketCounter::new(range(0, 256), Policy, bucket_limits()).unwrap(),
        Policy,
        routed().limits(),
    )
    .unwrap_or_else(|e| panic!("{:?}", e.error))
    .with_scoped_fencing(2)
    .unwrap_or_else(|e| panic!("{:?}", e.0));
    Source::new(r, 65536).unwrap_or_else(|e| panic!("{:?}", e.0))
}
fn target(intent: &TransferIntent) -> Target {
    Target::new(
        group(21),
        op(200),
        intent.clone(),
        BucketCounter::new(range(0, 128), Policy, bucket_limits()).unwrap(),
        Policy,
        TargetLimits {
            import_bytes: 32768,
            application_checkpoint_bytes: bucket_limits().checkpoint_bound().unwrap(),
        },
    )
    .unwrap_or_else(|e| panic!("{:?}", e.0))
}
fn source_hint(intent: &TransferIntent, key: u8) -> RouteHint {
    let b = intent.before().input();
    RouteHint {
        responsibility: b.responsibility,
        group: group(20),
        application: b.application,
        scheme: b.scheme,
        scope: b.scope,
        bucket: key.into(),
        epoch: b.epoch,
        generation: b.generation,
    }
}
fn data_at(intent: &TransferIntent, key: u8, delta: i64) -> Vec<u8> {
    encode_routed(
        source_hint(intent, key),
        &[key],
        &encode_add(&[key], delta, b"effect", 1024).unwrap(),
        4096,
    )
    .unwrap()
}
fn target_hint(intent: &TransferIntent) -> RouteHint {
    let c = intent.target_manifest(group(21)).unwrap().input();
    RouteHint {
        responsibility: c.responsibility,
        group: group(21),
        application: c.application,
        scheme: c.scheme,
        scope: c.scope,
        bucket: 1,
        epoch: c.epoch,
        generation: c.generation,
    }
}
fn handoff(foreign: bool) {
    let (mut d, mut p, intent) = setup(foreign);
    let bytes = intent.encode(100000).unwrap();
    assert_eq!(&bytes[..8], b"VBTINT06");
    assert_eq!(TransferIntent::decode(&bytes).unwrap(), intent);
    for end in 0..bytes.len() {
        assert!(TransferIntent::decode(&bytes[..end]).is_err());
    }
    for tag in [
        b"VBTINT01",
        b"VBTINT02",
        b"VBTINT03",
        b"VBTINT04",
        b"VBTINT05",
    ] {
        let mut old = bytes.clone();
        old[..8].copy_from_slice(tag);
        assert!(TransferIntent::decode(&old).is_err());
    }
    refuse_legacy(intent.before().clone(), &bytes);
    assert_eq!(
        intent.sources(),
        vec![RouteEntry {
            scope: range(0, 128),
            target: RouteTarget::Group(group(20))
        }]
    );
    assert_eq!(
        commit(&mut d, 200, bytes.clone()).outcome,
        DirectoryOutcome::TransferIntentRecorded
    );
    d = recover(&d);
    assert_eq!(
        d.transfer_intent_at(d.applied_index(), op(200))
            .unwrap()
            .unwrap()
            .intent,
        intent
    );
    assert!(commit(&mut d, 200, bytes).duplicate);
    let mut s = source(&intent);
    let boot = s.bootstrap_command(100000).unwrap();
    commit(&mut s, 100, boot);
    commit(&mut s, 1, data_at(&intent, 1, 7));
    commit(&mut s, 2, data_at(&intent, 200, 11));
    let mut t = target(&intent);
    let boot = t.bootstrap_command(100000).unwrap();
    commit(&mut t, 200, boot);
    let q = || {
        TargetQuery::Data(RoutedQuery {
            hint: target_hint(&intent),
            key: vec![1],
            query: vec![1],
        })
    };
    assert_eq!(
        t.read_at(t.applied_index(), q()).unwrap(),
        TargetRead::NotActive
    );
    assert!(
        TargetReadyEvidence::from_status(ConfigurationId::new(1).unwrap(), t.status()).is_err()
    );
    commit(
        &mut s,
        200,
        encode_scope_fence(intent.before().input().epoch, range(0, 128)),
    );
    let ScopedSourceRead::Frozen(Some(status)) = s
        .read_at(s.applied_index(), ScopedSourceQuery::Frozen(op(200)))
        .unwrap()
    else {
        panic!("source")
    };
    let evidence =
        SourceFenceEvidence::from_scoped_status(ConfigurationId::new(1).unwrap(), status, &intent)
            .unwrap_or_else(|e| panic!("{:?}", e.0));
    assert_eq!(evidence.scope, Some(range(0, 128)));
    assert!(SourceFenceEvidence::from_status(
        ConfigurationId::new(1).unwrap(),
        voteboat::transfer_source::SourceFreezeStatus {
            fence: status.fence.fence,
            intent: intent.clone(),
            exports: evidence.exports.clone()
        }
    )
    .is_err());
    let mut wrong_status = status;
    wrong_status.fence.scope = range(0, 64);
    assert!(SourceFenceEvidence::from_scoped_status(
        ConfigurationId::new(1).unwrap(),
        wrong_status,
        &intent
    )
    .is_err());
    let import = TargetImport::new(
        op(200),
        intent.clone(),
        group(21),
        vec![SourceImport {
            fence: status.fence.fence,
            configuration: ConfigurationId::new(1).unwrap(),
            image: s.export(op(200), 65536).unwrap(),
            digest: status.digest,
        }],
    )
    .unwrap_or_else(|e| panic!("{:?}", e.0));
    let bytes = t.import_command(&import, 100000).unwrap();
    commit(&mut t, 200, bytes);
    assert_eq!(
        t.read_at(t.applied_index(), q()).unwrap(),
        TargetRead::NotActive
    );
    let ready = TargetReadyEvidence::from_status(ConfigurationId::new(1).unwrap(), t.status())
        .unwrap_or_else(|e| panic!("{:?}", e.0));
    let publication = TransferPublication::new(
        op(200),
        intent.clone(),
        vec![evidence.clone()],
        vec![ready.clone()],
    )
    .unwrap_or_else(|e| panic!("{:?}", e.0));
    let pub_bytes = publication.encode(100000).unwrap();
    assert_eq!(&pub_bytes[..8], b"VBTPUB02");
    assert_eq!(
        TransferPublication::decode(&pub_bytes).unwrap(),
        publication
    );
    for end in 0..pub_bytes.len() {
        assert!(TransferPublication::decode(&pub_bytes[..end]).is_err());
    }
    let mut old = pub_bytes.clone();
    old[..8].copy_from_slice(b"VBTPUB01");
    assert!(TransferPublication::decode(&old).is_err());
    refuse_legacy(intent.before().clone(), &pub_bytes);
    let mut wrong = evidence.clone();
    wrong.scope = None;
    assert!(
        TransferPublication::new(op(200), intent.clone(), vec![wrong], vec![ready.clone()])
            .is_err()
    );
    let mut wrong = evidence.clone();
    wrong.scope = Some(range(64, 128));
    assert!(TransferPublication::new(op(200), intent.clone(), vec![wrong], vec![ready]).is_err());
    assert_eq!(
        commit(&mut d, 201, pub_bytes.clone()).outcome,
        DirectoryOutcome::TransferPublished(intent.after().input().generation)
    );
    d = recover(&d);
    assert!(commit(&mut d, 201, pub_bytes).duplicate);
    assert_eq!(
        d.manifest(intent.before().input().responsibility),
        Some(intent.after())
    );
    assert_eq!(d.manifest(rid(21)), intent.target_manifest(group(21)));
    let decision = d
        .transfer_publication_at(d.applied_index(), op(200))
        .unwrap()
        .unwrap();
    if let Some(ref mut p) = p {
        let reservation = p
            .delegation_reservation_at(p.applied_index(), op(400))
            .unwrap()
            .unwrap();
        let completion = DelegationCompletion {
            reservation: op(400),
            reservation_index: reservation.index,
            parent_configuration: ConfigurationId::new(1).unwrap(),
            child_configuration: ConfigurationId::new(1).unwrap(),
            decision: decision.clone(),
        };
        refuse_legacy(parent(intent.before()), &completion.encode(100000).unwrap());
        assert_eq!(
            commit(p, 401, completion.encode(100000).unwrap()).outcome,
            DirectoryOutcome::DelegationPublished(RouteGeneration::new(2).unwrap())
        );
        *p = recover(p);
        let expected = RouteTarget::Child(ChildAuthority {
            responsibility: intent.after().input().responsibility,
            group: group(1),
            epoch: intent.after().input().epoch,
        });
        assert_eq!(
            p.manifest(rid(500)).unwrap().input().execution,
            ExecutionMode::Delegated(vec![RouteEntry {
                scope: intent.before().input().scope,
                target: expected
            }])
        );
    }
    let cp = t.checkpoint(100000).unwrap();
    let mut recovered = target(&intent);
    recovered
        .restore_checkpoint(t.schema_version(), t.applied_index(), &cp)
        .unwrap();
    t = recovered;
    assert_eq!(
        t.read_at(t.applied_index(), q()).unwrap(),
        TargetRead::NotActive
    );
    let activation = TargetActivation {
        metadata_configuration: ConfigurationId::new(1).unwrap(),
        decision: decision.clone(),
    };
    let activation_bytes = t.activation_command(&activation, 100000).unwrap();
    commit(&mut t, 200, activation_bytes.clone());
    let cp = t.checkpoint(100000).unwrap();
    let mut recovered = target(&intent);
    recovered
        .restore_checkpoint(t.schema_version(), t.applied_index(), &cp)
        .unwrap();
    t = recovered;
    assert!(matches!(
        commit(&mut t, 200, activation_bytes).outcome,
        TargetOutcome::Activated(_)
    ));
    let write = |delta| {
        encode_routed(
            target_hint(&intent),
            &[1],
            &encode_add(&[1], delta, b"effect", 1024).unwrap(),
            4096,
        )
        .unwrap()
    };
    assert!(
        matches!(commit(&mut t,1,write(7)).outcome,TargetOutcome::Applied(r) if r.duplicate&&r.outcome==BucketOutcome::Value(7))
    );
    assert_eq!(t.application().outbox().count(), 1);
    assert!(matches!(commit(&mut t,30,write(2)).outcome,TargetOutcome::Applied(r) if !r.duplicate));
    assert_eq!(
        t.read_at(t.applied_index(), q()).unwrap(),
        TargetRead::Data(9)
    );
    commit(&mut s, 3, data_at(&intent, 200, 2));
    assert_eq!(
        s.read_at(
            s.applied_index(),
            ScopedSourceQuery::Data(RoutedQuery {
                hint: source_hint(&intent, 200),
                key: vec![200],
                query: vec![200]
            })
        )
        .unwrap(),
        ScopedSourceRead::Data(RoutedRead::Served(13))
    );
    assert_eq!(
        s.read_at(
            s.applied_index(),
            ScopedSourceQuery::Data(RoutedQuery {
                hint: source_hint(&intent, 1),
                key: vec![1],
                query: vec![1]
            })
        )
        .unwrap(),
        ScopedSourceRead::Data(RoutedRead::Rejected(RoutingError::Fenced))
    );
    let image = s.export(op(200), 65536).unwrap();
    let cp = s.checkpoint(100000).unwrap();
    let mut restored = source(&intent);
    restored
        .restore_checkpoint(1, s.applied_index(), &cp)
        .unwrap();
    assert_eq!(restored.export(op(200), 65536).unwrap(), image);
    // Current source phase retains the old grant: checked new-epoch adoption is next.
    let mut fresh = source_hint(&intent, 200);
    fresh.epoch = intent.after().input().epoch;
    fresh.scope = range(128, 256);
    assert_eq!(
        restored.routed().check_context(&fresh, &[200]),
        Err(RoutingError::EpochMismatch)
    );
}
#[test]
fn root_retained_child_moves_real_data_without_full_source_fence() {
    handoff(false)
}
#[test]
fn foreign_nested_retained_child_refreshes_parent_then_activates_independently() {
    handoff(true)
}

fn refuse_legacy(manifest: ResponsibilityManifest, bytes: &[u8]) {
    for schema in 1..=7 {
        let base = Directory::new(
            DirectoryPlan::new(manifest.input().authority, vec![manifest.clone()]).unwrap(),
            DirectoryLimits {
                operations: 32,
                history_bytes: 200000,
            },
        )
        .unwrap();
        let mut d = match schema {
            1 => base,
            2 => base
                .with_group_creation()
                .unwrap_or_else(|_| panic!("profile")),
            3 => base
                .with_namespace_creation()
                .unwrap_or_else(|_| panic!("profile")),
            4 => base
                .with_namespace_transfers()
                .unwrap_or_else(|_| panic!("profile")),
            5 => base
                .with_responsibility_insertion()
                .unwrap_or_else(|_| panic!("profile")),
            6 => base
                .with_recursive_insertion()
                .unwrap_or_else(|_| panic!("profile")),
            7 => base
                .with_cross_authority_insertion()
                .unwrap_or_else(|_| panic!("profile")),
            _ => unreachable!(),
        };
        let boot = d.bootstrap_command(100000).unwrap();
        commit(&mut d, 1000, boot);
        commit(
            &mut d,
            1001,
            DirectoryCommand {
                expected: None,
                manifest: manifest.clone(),
            }
            .encode(100000)
            .unwrap(),
        );
        let original = d.checkpoint(100000).unwrap();
        assert!(
            d.apply_batch(&[entry(3, 200, bytes.to_vec())]).is_err(),
            "schema{schema}"
        );
        assert_eq!(d.checkpoint(100000).unwrap(), original);
    }
}

#[test]
fn retained_intent_requires_actual_creation_and_preserves_remaining_routes() {
    let (mut d, _, intent) = setup(false);
    let child = intent.insertion_children().unwrap()[0].clone();
    let mut unknown = child.clone();
    unknown.creation = op(22);
    let unknown = TransferIntent::insert_retained_child(
        intent.before().clone(),
        intent.after().clone(),
        unknown,
    )
    .unwrap();
    assert_eq!(
        commit(&mut d, 200, unknown.encode(100000).unwrap()).outcome,
        DirectoryOutcome::TransferEvidenceMismatch
    );
    assert!(d
        .transfer_intent_at(d.applied_index(), op(200))
        .unwrap()
        .is_none());
    assert_eq!(
        d.manifest(intent.before().input().responsibility),
        Some(intent.before())
    );
    let mut changed = intent.after().clone().into_input();
    let ExecutionMode::Delegated(ref mut routes) = changed.execution else {
        unreachable!()
    };
    routes[1].target = RouteTarget::Group(group(30));
    assert!(TransferIntent::insert_retained_child(
        intent.before().clone(),
        ResponsibilityManifest::new(changed).unwrap(),
        child.clone()
    )
    .is_err());
    let mut fragmented = intent.after().clone().into_input();
    let ExecutionMode::Delegated(ref mut routes) = fragmented.execution else {
        unreachable!()
    };
    let target = routes[0].target;
    routes[0].scope = range(0, 64);
    routes.insert(
        1,
        RouteEntry {
            scope: range(64, 128),
            target,
        },
    );
    // The existing manifest contract rejects duplicate child selectors first.
    assert!(ResponsibilityManifest::new(fragmented).is_err());
}

#[cfg(feature = "native")]
#[test]
fn retained_intent_native_frame_cuts_recover_original_creation_and_exact_lock() {
    use support::{Fault, ModelIo};
    use voteboat::{log::*, native::log_store::*};
    let (d, _, intent) = setup(false);
    let creation = d
        .group_creation_at(d.applied_index(), group(21))
        .unwrap()
        .unwrap();
    let prefix = vec![
        entry(1, 1000, d.bootstrap_command(100000).unwrap()),
        entry(
            2,
            1001,
            DirectoryCommand {
                expected: None,
                manifest: intent.before().clone(),
            }
            .encode(100000)
            .unwrap(),
        ),
        entry(3, 21, creation.intent.encode(100000).unwrap()),
    ];
    let limits = LogLimits::default();
    let seed = || {
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
                3,
                Some(Suffix {
                    from: 1,
                    entries: prefix.clone(),
                }),
            )],
        );
        (io, log)
    };
    let (_, log) = seed();
    let state = log.state(group(1)).unwrap();
    let mutation = support::update(
        &state,
        1,
        4,
        Some(Suffix {
            from: 4,
            entries: vec![entry(4, 200, intent.encode(100000).unwrap())],
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
        let recovered = NativeLogStore::recover(io, support::identity(1), limits).unwrap();
        let state = recovered.state(group(1)).unwrap();
        let mut app = fresh(intent.before().clone(), true);
        app.apply_batch(
            &state
                .entries
                .into_iter()
                .filter(|e| e.index <= state.commit_index)
                .collect::<Vec<_>>(),
        )
        .unwrap();
        let status = app.transfer_intent_at(state.commit_index, op(200)).unwrap();
        if state.commit_index == 3 {
            old = true;
            assert!(status.is_none());
        } else {
            complete = true;
            assert_eq!(state.commit_index, 4);
            assert_eq!(status.unwrap().intent, intent);
        }
        assert_eq!(
            app.group_creation_at(state.commit_index, group(21))
                .unwrap()
                .unwrap(),
            creation
        );
        assert!(app.manifest(rid(21)).is_none());
        assert_eq!(
            app.manifest(intent.before().input().responsibility),
            Some(intent.before())
        );
        app = recover(&app);
        let retry = commit(&mut app, 200, intent.encode(100000).unwrap());
        assert_eq!(retry.duplicate, state.commit_index == 4);
        assert_eq!(retry.outcome, DirectoryOutcome::TransferIntentRecorded);
    }
    assert!(old && complete);
}

#[test]
fn retained_nested_decline_cancels_only_before_child_intent_acceptance() {
    let (mut child, p, intent) = setup(true);
    let mut p = p.unwrap();
    let decline = DelegationDecline::new(intent.clone())
        .unwrap()
        .encode(100000)
        .unwrap();
    refuse_legacy(intent.before().clone(), &decline);
    assert_eq!(
        commit(&mut child, 500, decline.clone()).outcome,
        DirectoryOutcome::DelegationDeclined
    );
    child = recover(&child);
    assert!(commit(&mut child, 500, decline).duplicate);
    assert_eq!(
        commit(&mut child, 200, intent.encode(100000).unwrap()).outcome,
        DirectoryOutcome::DelegationDeclined
    );
    let status = child
        .delegation_decline_at(child.applied_index(), op(200))
        .unwrap()
        .unwrap();
    let reservation = p
        .delegation_reservation_at(p.applied_index(), op(400))
        .unwrap()
        .unwrap();
    let cancellation = DelegationCancellation {
        reservation: op(400),
        reservation_index: reservation.index,
        parent_configuration: ConfigurationId::new(1).unwrap(),
        child_configuration: ConfigurationId::new(1).unwrap(),
        decline: status,
    }
    .encode(100000)
    .unwrap();
    refuse_legacy(parent(intent.before()), &cancellation);
    assert_eq!(
        commit(&mut p, 401, cancellation.clone()).outcome,
        DirectoryOutcome::DelegationCancelled
    );
    p = recover(&p);
    assert!(commit(&mut p, 401, cancellation).duplicate);
    assert_eq!(p.manifest(rid(500)), Some(&parent(intent.before())));
    assert_eq!(p.reserved_publication_bytes(), 0);
    let (mut accepted, _, intent) = setup(true);
    assert_eq!(
        commit(&mut accepted, 200, intent.encode(100000).unwrap()).outcome,
        DirectoryOutcome::TransferIntentRecorded
    );
    let declined = commit(
        &mut accepted,
        500,
        DelegationDecline::new(intent.clone())
            .unwrap()
            .encode(100000)
            .unwrap(),
    );
    assert_eq!(declined.outcome, DirectoryOutcome::LifecycleBusy);
    assert_eq!(
        accepted
            .transfer_intent_at(accepted.applied_index(), op(200))
            .unwrap()
            .unwrap()
            .intent,
        intent
    );
}
