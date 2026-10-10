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
use voteboat::{reparent_commit::*, reparent_guard::*, reparenting::*};

fn selected(i: &TransferIntent) -> Source {
    retained_source(i)
        .with_parent_slot_adoption(4)
        .unwrap_or_else(|_| panic!("slots"))
}
fn destination(authority: u128) -> ResponsibilityManifest {
    let mut m = grant().into_input();
    m.responsibility = rid(600);
    m.authority = group(authority);
    m.execution = ExecutionMode::Delegated(vec![
        RouteEntry {
            scope: range(0, 128),
            target: RouteTarget::Vacant,
        },
        RouteEntry {
            scope: range(128, 256),
            target: RouteTarget::Group(group(30)),
        },
    ]);
    ResponsibilityManifest::new(m).unwrap()
}
fn new_source(m: &ResponsibilityManifest) -> Source {
    let r = RoutedApplication::new(
        group(30),
        m.clone(),
        BucketCounter::new(range(128, 256), Policy, bucket_limits()).unwrap(),
        Policy,
        routed().limits(),
    )
    .unwrap_or_else(|e| panic!("{:?}", e.error))
    .with_scoped_fencing(2)
    .unwrap_or_else(|_| panic!("fences"));
    Source::new(r, 65536)
        .unwrap_or_else(|_| panic!("source"))
        .with_retained_insertion()
        .unwrap_or_else(|_| panic!("intents"))
        .with_retained_grants()
        .unwrap_or_else(|_| panic!("grants"))
        .with_parent_slot_adoption(4)
        .unwrap_or_else(|_| panic!("slots"))
}
fn cross_move(d: &mut Directory) -> (Directory, CrossOwnerParentAdoption) {
    let new = destination(200);
    let mut p = initial_profile(new.clone(), true);
    let plan = CrossReparentPlan::new(
        rid(10),
        rid(600),
        rid(21),
        vec![
            d.manifest(rid(10)).unwrap().clone(),
            new,
            d.manifest(rid(21)).unwrap().clone(),
        ],
    )
    .unwrap();
    let cfg = ConfigurationId::new(1).unwrap();
    assert_eq!(
        commit(
            d,
            4000,
            PrepareReparent {
                plan: plan.clone(),
                coordinator: None
            }
            .encode(MAX_REPARENT_PREPARE_BYTES)
            .unwrap()
        )
        .outcome,
        DirectoryOutcome::ReparentGuarded
    );
    let guard = ReparentGuardEvidence::from_status(
        cfg,
        &d.reparent_guard_at(d.applied_index(), op(4000))
            .unwrap()
            .unwrap(),
    )
    .unwrap();
    assert_eq!(
        commit(
            &mut p,
            4000,
            PrepareReparent {
                plan: plan.clone(),
                coordinator: Some(guard)
            }
            .encode(MAX_REPARENT_PREPARE_BYTES)
            .unwrap()
        )
        .outcome,
        DirectoryOutcome::ReparentGuarded
    );
    let guards = [&*d, &p]
        .into_iter()
        .map(|n| {
            ReparentGuardEvidence::from_status(
                cfg,
                &n.reparent_guard_at(n.applied_index(), op(4000))
                    .unwrap()
                    .unwrap(),
            )
            .unwrap()
        })
        .collect();
    assert_eq!(
        commit(
            d,
            4001,
            CommitReparent::new(op(4000), guards)
                .unwrap()
                .encode(MAX_REPARENT_COMPLETION_BYTES)
                .unwrap()
        )
        .outcome,
        DirectoryOutcome::ReparentCommitted
    );
    let decision = PublishReparent {
        configuration: cfg,
        decision: d
            .reparent_decision_at(d.applied_index(), op(4000))
            .unwrap()
            .unwrap(),
    };
    assert_eq!(
        commit(
            &mut p,
            4002,
            decision.encode(MAX_REPARENT_COMPLETION_BYTES).unwrap()
        )
        .outcome,
        DirectoryOutcome::ReparentPublished
    );
    let publications = [&*d, &p]
        .into_iter()
        .map(|n| {
            ReparentPublicationEvidence::from_status(
                cfg,
                n.reparent_publication_at(n.applied_index(), op(4000))
                    .unwrap()
                    .unwrap(),
            )
            .unwrap()
        })
        .collect();
    assert_eq!(
        commit(
            d,
            4003,
            FinishReparent::new(op(4000), publications)
                .unwrap()
                .encode(MAX_REPARENT_COMPLETION_BYTES)
                .unwrap()
        )
        .outcome,
        DirectoryOutcome::ReparentCompleted
    );
    let completion = ReleaseCommittedReparent {
        configuration: cfg,
        completion: d
            .reparent_completion_at(d.applied_index(), op(4000))
            .unwrap()
            .unwrap(),
    };
    assert_eq!(
        commit(&mut p, 4004, completion.encode().unwrap()).outcome,
        DirectoryOutcome::ReparentReleased
    );
    let observation = CrossOwnerParentAdoption {
        plan,
        decision,
        child_configuration: cfg,
        child_publication: d
            .reparent_publication_at(d.applied_index(), op(4000))
            .unwrap()
            .unwrap(),
        completion,
    };
    (p, observation)
}
fn slot(
    observation: &CrossOwnerParentAdoption,
    p: &Directory,
    side: ReparentSide,
) -> CrossParentSlotAdoption {
    CrossParentSlotAdoption {
        side,
        observation: observation.clone(),
        parent_configuration: ConfigurationId::new(1).unwrap(),
        parent_publication: p
            .reparent_publication_at(p.applied_index(), op(4000))
            .unwrap()
            .unwrap(),
    }
}
fn write(s: &mut Source, id: u128, key: u8) -> Vec<u8> {
    let m = s.grant().input();
    let hint = RouteHint {
        responsibility: m.responsibility,
        group: s.routed().local(),
        application: m.application,
        scheme: m.scheme,
        scope: range(128, 256),
        bucket: key.into(),
        epoch: m.epoch,
        generation: m.generation,
    };
    let bytes = encode_routed(
        hint,
        &[key],
        &encode_add(&[key], 3, b"later", 1024).unwrap(),
        4096,
    )
    .unwrap();
    assert!(matches!(
        commit(s, id, bytes.clone()).outcome,
        RoutedOutcome::Applied(_)
    ));
    bytes
}
fn reopen(s: &Source, mut fresh: Source) -> Source {
    fresh
        .restore_checkpoint(5, s.applied_index(), &s.checkpoint(200000).unwrap())
        .unwrap();
    assert_eq!(fresh.grant(), s.grant());
    fresh
}
fn later_transfer(d: &mut Directory, s: &mut Source) {
    let before = s.grant().clone();
    let mut c = before.clone().into_input();
    c.responsibility = rid(22);
    c.parent = Some(ParentAuthority {
        responsibility: before.input().responsibility,
        group: before.input().authority,
    });
    c.scope = range(128, 192);
    c.epoch = OwnershipEpoch::new(1).unwrap();
    c.generation = RouteGeneration::new(1).unwrap();
    c.execution = ExecutionMode::Single(group(22));
    let child = ResponsibilityManifest::new(c).unwrap();
    let creation = GroupCreationIntent {
        authority: before.input().authority,
        parent: before.input().responsibility,
        expected: before.input().generation,
        responsibility: rid(22),
        bootstrap: support::bootstrap(22, 3),
        application: before.input().application,
        mode: GroupCreationMode::Staging,
    };
    assert_eq!(
        commit(d, 22, creation.encode(200000).unwrap()).outcome,
        DirectoryOutcome::CreationReserved
    );
    let created = d
        .group_creation_at(d.applied_index(), group(22))
        .unwrap()
        .unwrap();
    let mut after = before.clone().into_input();
    after.epoch = OwnershipEpoch::new(after.epoch.get() + 1).unwrap();
    after.generation = RouteGeneration::new(after.generation.get() + 1).unwrap();
    let ExecutionMode::Delegated(routes) = &mut after.execution else {
        panic!("delegated")
    };
    routes[1] = RouteEntry {
        scope: range(128, 192),
        target: RouteTarget::Child(ChildAuthority {
            responsibility: rid(22),
            group: before.input().authority,
            epoch: child.input().epoch,
        }),
    };
    routes.push(RouteEntry {
        scope: range(192, 256),
        target: RouteTarget::Group(s.routed().local()),
    });
    let intent = TransferIntent::insert_retained_child(
        before,
        ResponsibilityManifest::new(after).unwrap(),
        InsertionChild::from_creation(child, &created).unwrap(),
    )
    .unwrap();
    assert_eq!(
        commit(d, 202, intent.encode(200000).unwrap()).outcome,
        DirectoryOutcome::TransferIntentRecorded
    );
    let bytes = write(s, 6, 150);
    commit(s, 202, intent.encode(200000).unwrap());
    let ScopedSourceRead::Frozen(Some(status)) = s
        .read_at(s.applied_index(), ScopedSourceQuery::Frozen(op(202)))
        .unwrap()
    else {
        panic!("frozen")
    };
    let mut target = Target::new(
        group(22),
        op(202),
        intent.clone(),
        BucketCounter::new(range(128, 192), Policy, bucket_limits()).unwrap(),
        Policy,
        TargetLimits {
            import_bytes: 32768,
            application_checkpoint_bytes: bucket_limits().checkpoint_bound().unwrap(),
        },
    )
    .unwrap_or_else(|_| panic!("target"));
    let boot = target.bootstrap_command(200000).unwrap();
    commit(&mut target, 202, boot);
    let import = TargetImport::new(
        op(202),
        intent.clone(),
        group(22),
        vec![SourceImport {
            fence: status.fence.fence,
            configuration: ConfigurationId::new(1).unwrap(),
            image: s.export(op(202), 65536).unwrap(),
            digest: status.digest,
        }],
    )
    .unwrap();
    let command = target.import_command(&import, 200000).unwrap();
    commit(&mut target, 202, command);
    let cfg = ConfigurationId::new(1).unwrap();
    let publication = TransferPublication::new(
        op(202),
        intent.clone(),
        vec![SourceFenceEvidence::from_scoped_status(cfg, status, &intent).unwrap()],
        vec![TargetReadyEvidence::from_status(cfg, target.status()).unwrap()],
    )
    .unwrap();
    assert!(matches!(
        commit(d, 203, publication.encode(200000).unwrap()).outcome,
        DirectoryOutcome::TransferPublished(_)
    ));
    let decision = d
        .transfer_publication_at(d.applied_index(), op(202))
        .unwrap()
        .unwrap();
    let command = target
        .activation_command(
            &TargetActivation {
                metadata_configuration: cfg,
                decision: decision.clone(),
            },
            200000,
        )
        .unwrap();
    commit(&mut target, 202, command);
    commit(
        s,
        301,
        RetainedGrantAdoption {
            metadata_configuration: cfg,
            decision,
        }
        .encode(200000)
        .unwrap(),
    );
    assert_eq!(s.grant(), intent.after());
    let mut hint = source_hint(&intent, 150);
    let m = intent.target_manifest(group(22)).unwrap().input();
    hint.responsibility = m.responsibility;
    hint.group = group(22);
    hint.scope = m.scope;
    hint.epoch = m.epoch;
    hint.generation = m.generation;
    let original = encode_routed(
        hint,
        &[150],
        &encode_add(&[150], 3, b"later", 1024).unwrap(),
        4096,
    )
    .unwrap();
    assert!(matches!(
        commit(&mut target, 6, original).outcome,
        TargetOutcome::Applied(_)
    ));
    assert_eq!(target.application().outbox().count(), 1);
    // The source's old routed bytes cannot open a frozen scope again.
    assert!(matches!(
        commit(s, 6, bytes).outcome,
        RoutedOutcome::Rejected(_)
    ));
    assert_eq!(target.application().value(&[150]), Ok(3));
}

#[test]
fn cross_parent_slots_preserve_exports_and_both_parents_transfer_again() {
    let (mut d, _, mut old, first, retained) = handoff_family(false, true, true);
    let image = old.export(op(200), 65536).unwrap();
    let (mut p, observation) = cross_move(&mut d);
    let initial_new = destination(200);
    let mut new = new_source(&initial_new);
    let boot = new.bootstrap_command(200000).unwrap();
    commit(&mut new, 100, boot);
    for (s, directory, side) in [
        (&mut old, &mut d, ReparentSide::Old),
        (&mut new, &mut p, ReparentSide::New),
    ] {
        let command = slot(&observation, directory, side);
        let bytes = command.encode(200000).unwrap();
        assert_eq!(CrossParentSlotAdoption::decode(&bytes).unwrap(), command);
        let status = commit(s, 9000, bytes.clone()).outcome;
        assert!(matches!(status, RoutedOutcome::ParentAdopted(_)));
        assert_eq!(s.grant(), &command.after());
        let fresh = if side == ReparentSide::Old {
            selected(&first)
        } else {
            new_source(&initial_new)
        };
        *s = reopen(s, fresh);
        assert_eq!(commit(s, 9000, bytes.clone()).outcome, status);
        later_transfer(directory, s);
        assert_eq!(commit(s, 9000, bytes).outcome, status);
    }
    old = reopen(&old, selected(&first));
    new = reopen(&new, new_source(&initial_new));
    assert_eq!(old.export(op(200), 65536).unwrap(), image);
    assert!(matches!(
        commit(&mut old, 300, retained.encode(200000).unwrap()).outcome,
        RoutedOutcome::GrantAdopted(_)
    ));
    assert_eq!(new.routed().application().outbox().count(), 1);
}

#[test]
fn local_slot_commands_are_checked_for_both_parent_roles() {
    let (d, _, mut old, first, _) = handoff_family(false, true, true);
    let new_manifest = destination(1);
    // Supplied local quorum observation; cross composition above executes its metadata protocol.
    let observation = OwnerParentAdoption {
        metadata_configuration: ConfigurationId::new(1).unwrap(),
        decision: ReparentStatus {
            operation: op(400),
            index: 50,
            plan: ReparentPlan::new(
                d.manifest(rid(10)).unwrap().clone(),
                new_manifest.clone(),
                d.manifest(rid(21)).unwrap().clone(),
            )
            .unwrap(),
        },
    };
    let mut new = new_source(&new_manifest);
    let boot = new.bootstrap_command(200000).unwrap();
    commit(&mut new, 100, boot);
    for (s, side) in [(&mut old, ReparentSide::Old), (&mut new, ReparentSide::New)] {
        let command = ParentSlotAdoption {
            side,
            observation: observation.clone(),
        };
        let bytes = command.encode(200000).unwrap();
        assert_eq!(ParentSlotAdoption::decode(&bytes).unwrap(), command);
        for end in 0..bytes.len() {
            assert!(ParentSlotAdoption::decode(&bytes[..end]).is_err());
        }
        let status = commit(s, 9000, bytes.clone()).outcome;
        assert_eq!(s.grant(), &command.after());
        let write = write(s, 5, 200);
        let fresh = if side == ReparentSide::Old {
            selected(&first)
        } else {
            new_source(&new_manifest)
        };
        *s = reopen(s, fresh);
        assert_eq!(commit(s, 9000, bytes).outcome, status);
        assert!(matches!(
            commit(s, 5, write).outcome,
            RoutedOutcome::Applied(_)
        ));
    }
}

#[test]
fn slot_provenance_pending_profiles_and_checkpoint_corruption_fail_closed() {
    let (mut d, _, mut s, first, _) = handoff_family(false, true, true);
    let (_, observation) = cross_move(&mut d);
    let command = slot(&observation, &d, ReparentSide::Old);
    let bytes = command.encode(200000).unwrap();
    for end in 0..bytes.len() {
        assert!(CrossParentSlotAdoption::decode(&bytes[..end]).is_err());
    }
    let mut wrong = command.clone();
    wrong.parent_publication.authority = group(200);
    assert!(wrong.encode(200000).is_err());
    let mut wrong = command.clone();
    wrong.parent_publication.index += 1;
    assert!(wrong.encode(200000).is_err());
    let mut wrong = command.clone();
    wrong.parent_configuration = ConfigurationId::new(2).unwrap();
    assert!(wrong.encode(200000).is_err());
    let mut wrong = command.clone();
    wrong.parent_publication.decision_digest.0[0] ^= 1;
    assert!(wrong.encode(200000).is_err());
    let original = s.checkpoint(200000).unwrap();
    for id in [100, 1, 2, 200, 21, 300] {
        assert!(s
            .validate_proposal(op(id), &bytes, std::iter::empty())
            .is_err());
    }
    assert!(s
        .validate_proposal(op(9000), &bytes, std::iter::empty())
        .is_ok());
    assert!(s
        .validate_proposal(op(9001), &bytes, [(op(9000), bytes.as_slice())].into_iter())
        .is_err());
    assert_eq!(s.checkpoint(200000).unwrap(), original);
    assert!(source_profile(&first, true)
        .validate_proposal(op(9000), &bytes, std::iter::empty())
        .is_err());
    for maximum in [0, 65, usize::MAX] {
        assert!(retained_source(&first)
            .with_parent_slot_adoption(maximum)
            .is_err());
    }
    assert!(s.clone().with_parent_slot_adoption(1).is_err());
    commit(&mut s, 9000, bytes.clone());
    let cp = s.checkpoint(200000).unwrap();
    for end in 0..cp.len() {
        let mut fresh = selected(&first);
        assert!(fresh
            .restore_checkpoint(5, s.applied_index(), &cp[..end])
            .is_err());
        assert_eq!(fresh.applied_index(), 0);
    }
    let mut bad = cp.clone();
    *bad.last_mut().unwrap() ^= 1;
    assert!(selected(&first)
        .restore_checkpoint(5, s.applied_index(), &bad)
        .is_err());
    assert!(source_profile(&first, true)
        .restore_checkpoint(5, s.applied_index(), &cp)
        .is_err());
    commit(&mut s, 999, encode_fence(command.after().input().epoch));
    assert!(s
        .validate_proposal(op(9000), &bytes, std::iter::empty())
        .is_ok());
    assert!(s
        .validate_proposal(op(9001), &bytes, std::iter::empty())
        .is_err());
    let s = reopen(&s, selected(&first));
    assert!(s.fence().is_some());
}

#[cfg(feature = "native")]
#[test]
fn slot_journal_cuts_recover_original_or_complete_grant_and_immutable_export() {
    use support::{Fault, ModelIo};
    use voteboat::{log::*, native::log_store::*};
    let (mut d, _, _, intent, retained) = handoff_family(false, true, true);
    let (_, observation) = cross_move(&mut d);
    let command = slot(&observation, &d, ReparentSide::Old);
    let bytes = command.encode(200000).unwrap();
    let prototype = selected(&intent);
    let entries = [
        entry(1, 100, prototype.bootstrap_command(200000).unwrap()),
        entry(2, 1, data_at(&intent, 1, 7)),
        entry(3, 2, data_at(&intent, 200, 11)),
        entry(4, 200, intent.encode(200000).unwrap()),
        entry(5, 3, data_at(&intent, 200, 2)),
        entry(6, 300, retained.encode(200000).unwrap()),
        entry(7, 9000, bytes.clone()),
        entry(8, 999, encode_fence(command.after().input().epoch)),
    ];
    let limits = LogLimits::default();
    for boundary in [7u64, 8] {
        let seed = || {
            let io = ModelIo::default();
            let mut log = NativeLogStore::create(io.clone(), support::identity(1), limits).unwrap();
            support::append(
                &mut log,
                vec![LogMutation::Create(support::bootstrap(20, 3))],
            );
            let state = log.state(group(20)).unwrap();
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
        };
        let (_, log) = seed();
        let mutation = support::update(
            &log.state(group(20)).unwrap(),
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
            let state = log.state(group(20)).unwrap();
            let mut app = selected(&intent);
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
                &if state.commit_index >= 7 {
                    command.after()
                } else {
                    intent.after().clone()
                }
            );
            assert_eq!(app.fence().is_some(), state.commit_index == 8);
            let image = app.export(op(200), 65536).unwrap();
            app = reopen(&app, selected(&intent));
            let prior = app.parent_adoption(op(9000));
            let RoutedOutcome::ParentAdopted(status) =
                commit(&mut app, 9000, bytes.clone()).outcome
            else {
                panic!("retry")
            };
            assert_eq!(
                status.index,
                prior.map_or(state.commit_index + 1, |p| p.index)
            );
            assert_eq!(app.export(op(200), 65536).unwrap(), image);
            assert_eq!(app.routed().application().outbox().count(), 3);
        }
        assert!(old && complete);
    }
}

#[test]
fn slot_capacity_and_pending_roundtrip_use_the_same_ordered_ledger() {
    let (d, _, _, intent, retained) = handoff_family(false, true, true);
    let plan = ReparentPlan::new(
        d.manifest(rid(10)).unwrap().clone(),
        destination(1),
        d.manifest(rid(21)).unwrap().clone(),
    )
    .unwrap();
    let first = ParentSlotAdoption {
        side: ReparentSide::Old,
        observation: OwnerParentAdoption {
            metadata_configuration: ConfigurationId::new(1).unwrap(),
            decision: ReparentStatus {
                operation: op(400),
                index: 50,
                plan: plan.clone(),
            },
        },
    };
    let [old, new, child] = plan.updated_manifests();
    let second = ParentSlotAdoption {
        side: ReparentSide::New,
        observation: OwnerParentAdoption {
            metadata_configuration: ConfigurationId::new(1).unwrap(),
            decision: ReparentStatus {
                operation: op(401),
                index: 51,
                plan: ReparentPlan::new(new, old, child).unwrap(),
            },
        },
    };
    let first_bytes = first.encode(200000).unwrap();
    let second_bytes = second.encode(200000).unwrap();
    for maximum in [1, 2] {
        let mut s = retained_source(&intent)
            .with_parent_slot_adoption(maximum)
            .unwrap_or_else(|_| panic!("limit"));
        let boot = s.bootstrap_command(200000).unwrap();
        s.apply_batch(&[
            entry(1, 100, boot),
            entry(2, 1, data_at(&intent, 1, 7)),
            entry(3, 2, data_at(&intent, 200, 11)),
            entry(4, 200, intent.encode(200000).unwrap()),
            entry(5, 3, data_at(&intent, 200, 2)),
            entry(6, 300, retained.encode(200000).unwrap()),
        ])
        .unwrap();
        assert_eq!(
            s.validate_proposal(
                op(9001),
                &second_bytes,
                [(op(9000), first_bytes.as_slice())].into_iter()
            )
            .is_ok(),
            maximum == 2
        );
        let status = commit(&mut s, 9000, first_bytes.clone()).outcome;
        let before = s.checkpoint(200000).unwrap();
        let result = s.apply_batch(&[entry(s.applied_index() + 1, 9001, second_bytes.clone())]);
        if maximum == 1 {
            assert!(result.is_err());
            assert_eq!(s.checkpoint(200000).unwrap(), before);
        } else {
            result.unwrap();
            assert_eq!(s.grant(), &second.after());
            let mut fresh = retained_source(&intent)
                .with_parent_slot_adoption(maximum)
                .unwrap_or_else(|_| panic!("limit"));
            fresh
                .restore_checkpoint(5, s.applied_index(), &s.checkpoint(200000).unwrap())
                .unwrap();
            assert_eq!(fresh.grant(), s.grant());
        }
        assert_eq!(commit(&mut s, 9000, first_bytes.clone()).outcome, status);
        write(&mut s, 7, 200);
    }
}
