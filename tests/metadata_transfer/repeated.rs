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

fn cfg(n: u64) -> ConfigurationId {
    ConfigurationId::new(n).unwrap()
}
fn original() -> MetadataPublishingSource {
    MetadataPublishingSource::new(fresh(16)).unwrap()
}
fn apply(
    a: &mut MetadataPublishingSource,
    id: u128,
    bytes: Vec<u8>,
    entries: &mut Vec<LogEntry>,
) -> MetadataPublishingOutcome {
    let e = entry(a.applied_index() + 1, id, bytes);
    let r = a
        .apply_batch(std::slice::from_ref(&e))
        .unwrap()
        .remove(0)
        .outcome;
    entries.push(e);
    r
}
fn checkpoint<A: CheckpointStateMachine>(a: &A) -> Vec<u8> {
    a.checkpoint(a.deployment_requirements().unwrap().snapshot_bytes)
        .unwrap()
}
fn update(authority: u128, generation: u64) -> Vec<u8> {
    let mut m = grant().into_input();
    m.authority = group(authority);
    m.generation = RouteGeneration::new(generation).unwrap();
    DirectoryCommand {
        expected: Some(RouteGeneration::new(generation - 1).unwrap()),
        manifest: ResponsibilityManifest::new(m).unwrap(),
    }
    .encode(100000)
    .unwrap()
}
fn repeatable(plan: MetadataMovePlan) -> MetadataPublishingSource {
    let t = MetadataServingTarget::new(original(), plan, op(7), cfg(1), cfg(2)).unwrap();
    let bytes = t.readiness_requirements().snapshot_bytes;
    MetadataPublishingSource::new(
        MetadataAuthoritySource::from_serving(t, bytes).unwrap_or_else(|_| panic!("repeatable")),
    )
    .unwrap()
}
struct Prepared {
    source: MetadataPublishingSource,
    template: MetadataPublishingSource,
    entries: Vec<LogEntry>,
    image: MetadataImage,
    target: MetadataServingTarget,
    target_template: MetadataServingTarget,
    target_entries: Vec<LogEntry>,
    old_command: Vec<u8>,
    local_command: Vec<u8>,
}
fn prepared() -> Prepared {
    let (mut a, first_image, old_command) = prepare_original();
    let plan = first_image.plan().clone();
    let template = repeatable(plan.clone());
    let mut source = template.clone();
    let mut entries = vec![];
    let b = source.bootstrap_command(100000).unwrap();
    apply(&mut source, 7, b, &mut entries);
    let mut plain = MetadataServingTarget::new(original(), plan, op(7), cfg(1), cfg(2)).unwrap();
    let boot = plain.bootstrap_command(100000).unwrap();
    activation::target_apply(&mut plain, 7, boot);
    let import = plain.import_command(&first_image, cfg(1), 100000).unwrap();
    activation::target_apply(&mut plain, 7, import.clone());
    apply(&mut source, 7, import, &mut entries);
    let (p, _) = activation::publication(&mut a, &plain);
    let activate = plain.activation_command(p, 100000).unwrap();
    apply(&mut source, 7, activate, &mut entries);
    let local_command = update(9, 3);
    assert!(matches!(
        apply(&mut source, 90, local_command.clone(), &mut entries),
        MetadataPublishingOutcome::Source(MetadataSourceOutcome::Serving(
            MetadataServingOutcome::Directory(_)
        ))
    ));
    // B reserves/publishes a different namespace; C must preserve both domains.
    let intent = activation::create_intent(group(9), 40, 3);
    apply(
        &mut source,
        91,
        intent.encode(100000).unwrap(),
        &mut entries,
    );
    let created = source
        .source()
        .active_directory()
        .unwrap()
        .group_creation_at(source.applied_index(), group(40))
        .unwrap()
        .unwrap();
    apply(
        &mut source,
        92,
        activation::ready_namespace(created).encode(100000).unwrap(),
        &mut entries,
    );
    let q = MetadataPublishingQuery::Source(MetadataSourceQuery::Serving(
        MetadataServingQuery::Creation(group(30)),
    ));
    let read = source.read_at(source.applied_index(), q.clone()).unwrap();
    assert!(
        matches!(&read, MetadataPublishingRead::Source(MetadataSourceRead::Serving(MetadataServingRead::Creation(Some(c)))) if c.authority == group(1) && c.index == 7)
    );
    assert!(source
        .read_result_bytes(&read, source.read_result_bound(&q).unwrap())
        .is_ok());
    let plan = source.source().plan(group(11)).unwrap();
    let freeze = source.source().freeze_command(&plan, 100000).unwrap();
    apply(&mut source, 8, freeze, &mut entries);
    let image = source.source().export(100000).unwrap();
    let target_template =
        MetadataServingTarget::new(template.clone(), plan, op(8), cfg(2), cfg(3)).unwrap();
    let mut target = target_template.clone();
    let target_entries = vec![
        entry(1, 8, target.bootstrap_command(100000).unwrap()),
        entry(2, 8, target.import_command(&image, cfg(2), 100000).unwrap()),
    ];
    target.apply_batch(&target_entries).unwrap();
    Prepared {
        source,
        template,
        entries,
        image,
        target,
        target_template,
        target_entries,
        old_command,
        local_command,
    }
}
fn finish(p: &mut Prepared) -> MetadataActivationStatus {
    let publish = p
        .source
        .publication_command(p.target.status().target.imported.unwrap(), cfg(3), 100000)
        .unwrap();
    let MetadataPublishingOutcome::Published(status) =
        apply(&mut p.source, 8, publish, &mut p.entries)
    else {
        panic!("publication")
    };
    let activate = p.target.activation_command(status, 100000).unwrap();
    let e = entry(p.target.applied_index() + 1, 8, activate);
    let MetadataServingOutcome::Activated(a) = p
        .target
        .apply_batch(std::slice::from_ref(&e))
        .unwrap()
        .remove(0)
        .outcome
    else {
        panic!("activation")
    };
    p.target_entries.push(e);
    a
}
#[test]
fn repeated_move_preserves_all_authority_domains_creations_and_retries() {
    let mut p = prepared();
    let frozen = checkpoint(&p.source);
    assert!(p.source.source().active_directory().is_none());
    assert!(matches!(
        p.target
            .read_at(
                2,
                MetadataServingQuery::Directory(DirectoryQuery::Manifest(
                    grant().input().responsibility
                ))
            )
            .unwrap(),
        MetadataServingRead::NotActive
    ));
    assert_eq!(p.source.source().export(100000).unwrap(), p.image);
    let activation = finish(&mut p);
    assert_eq!(activation.publication.imported.source.source, group(9));
    assert_eq!(activation.publication.imported.source.target, group(11));
    verify_repeated_history(&mut p);
    let before = p
        .target
        .active_directory()
        .unwrap()
        .manifest(grant().input().responsibility)
        .unwrap();
    assert_eq!(before.input().authority, group(11));
    assert_eq!(before.input().generation.get(), 4);
    assert_eq!(before.input().execution, grant().input().execution);
    let current = update(11, 5);
    assert!(matches!(
        activation::target_apply(&mut p.target, 93, current.clone()),
        MetadataServingOutcome::Directory(DirectoryReceipt {
            duplicate: false,
            ..
        })
    ));
    let cp = checkpoint(&p.target);
    let mut reopened = p.target_template.clone();
    reopened
        .restore_checkpoint(p.target.schema_version(), p.target.applied_index(), &cp)
        .unwrap();
    assert_eq!(checkpoint(&reopened), cp);
    assert!(matches!(
        activation::target_apply(&mut reopened, 93, current),
        MetadataServingOutcome::Directory(DirectoryReceipt {
            duplicate: true,
            ..
        })
    ));
    assert!(
        matches!(activation::target_apply(&mut reopened,90,p.local_command.clone()),MetadataServingOutcome::Historical{source,index:4,..} if source==group(9))
    );
    let mut reopened_source = p.template.clone();
    reopened_source
        .restore_checkpoint(p.source.schema_version(), 7, &frozen)
        .unwrap();
    assert_eq!(reopened_source.source().export(100000).unwrap(), p.image);
    let status = reopened_source.source().status().unwrap();
    let freeze = p.entries[6].payload.clone();
    let EntryPayload::Command { bytes, .. } = freeze else {
        panic!()
    };
    assert_eq!(
        apply(&mut reopened_source, 8, bytes, &mut vec![]),
        MetadataPublishingOutcome::Source(MetadataSourceOutcome::Frozen(status))
    );
    assert_eq!(
        apply(&mut reopened_source, 93, update(9, 4), &mut vec![]),
        MetadataPublishingOutcome::Source(MetadataSourceOutcome::Fenced)
    );
    assert_eq!(reopened_source.source().export(100000).unwrap(), p.image);
}

#[test]
fn repeated_profiles_refuse_wrong_bindings_stale_views_and_partial_restore() {
    let mut p = prepared();
    assert!(original().source().serving_target().is_none());
    assert!(p.template.source().serving_target().is_some());
    assert!(p.source.source().serving_target().is_none());
    let boot = p.template.bootstrap_command(100000).unwrap();
    assert!(p
        .template
        .validate_proposal(op(7), &boot, std::iter::empty())
        .is_ok());
    assert!(p
        .template
        .validate_proposal(op(999), &boot, std::iter::empty())
        .is_err());
    let mut live = p.template.clone();
    live.apply_batch(&p.entries[..6]).unwrap();
    assert!(live.source().plan(group(1)).is_err());
    let mut old_incarnation = group(1);
    old_incarnation.incarnation = GroupIncarnation::new(2).unwrap();
    assert!(live.source().plan(old_incarnation).is_err());
    let stale = live.source().plan(group(11)).unwrap();
    let b = update(9, 4);
    apply(&mut live, 94, b.clone(), &mut vec![]);
    let freeze = live.source().freeze_command(&stale, 100000).unwrap();
    assert!(live
        .validate_proposal(op(8), &freeze, std::iter::empty())
        .is_err());
    assert_eq!(
        apply(&mut live, 8, freeze, &mut vec![]),
        MetadataPublishingOutcome::Source(MetadataSourceOutcome::Conflict)
    );
    assert!(live.source().active_directory().is_some());
    let correct = live.source().plan(group(11)).unwrap();
    let freeze = live.source().freeze_command(&correct, 100000).unwrap();
    assert!(live
        .validate_proposal(op(8), &freeze, std::iter::empty())
        .is_err());
    assert!(live
        .validate_proposal(op(95), &freeze, [(op(94), b.as_slice())].into_iter())
        .is_ok());
    assert!(live
        .validate_proposal(op(1001), &freeze, std::iter::empty())
        .is_err());
    let pristine = checkpoint(&p.target_template);
    let import = p.target_entries[1].clone();
    if let EntryPayload::Command {
        mut bytes,
        operation,
    } = import.payload
    {
        for at in [40, 48, 88, 120, 220, bytes.len() - 1] {
            bytes[at] ^= 1;
            let mut staged = p.target_template.clone();
            staged.apply_batch(&p.target_entries[..1]).unwrap();
            let cp = checkpoint(&staged);
            assert!(staged
                .apply_batch(&[entry(2, operation.get(), bytes.clone())])
                .is_err());
            assert_eq!(checkpoint(&staged), cp);
            bytes[at] ^= 1;
        }
    }
    finish(&mut p);
    let cp = checkpoint(&p.target);
    for n in [0, 7, 39, 40, 43, cp.len() / 2, cp.len() - 1] {
        let mut t = p.target_template.clone();
        assert!(t
            .restore_checkpoint(
                p.target.schema_version(),
                p.target.applied_index(),
                &cp[..n]
            )
            .is_err());
        assert_eq!(checkpoint(&t), pristine);
    }
    assert!(p
        .target_template
        .clone()
        .restore_checkpoint(METADATA_SERVING_SCHEMA, p.target.applied_index(), &cp)
        .is_err());
    let first = MetadataServingTarget::new(
        original(),
        MetadataMovePlan::new(group(1), group(9), vec![grant()]).unwrap(),
        op(7),
        cfg(1),
        cfg(2),
    )
    .unwrap();
    let bound = first.readiness_requirements().snapshot_bytes;
    assert!(MetadataAuthoritySource::from_serving(first.clone(), bound - 1).is_err());
    assert!(
        MetadataAuthoritySource::from_serving(first.clone(), MAX_METADATA_IMAGE_BYTES + 1).is_err()
    );
    let mut initialized = first;
    let boot = initialized.bootstrap_command(100000).unwrap();
    activation::target_apply(&mut initialized, 7, boot);
    assert!(MetadataAuthoritySource::from_serving(initialized, bound).is_err());
}

#[cfg(feature = "native")]
#[test]
fn repeated_move_native_journal_cuts_keep_fence_publication_activation_and_retries() {
    let mut p = prepared();
    owner_locators::journal_cuts(group(9), || p.template.clone(), &p.entries);
    finish(&mut p);
    owner_locators::journal_cuts(group(9), || p.template.clone(), &p.entries);
    owner_locators::journal_cuts(
        group(11),
        || p.target_template.clone(),
        &p.target_entries[..2],
    );
    owner_locators::journal_cuts(group(11), || p.target_template.clone(), &p.target_entries);
}

#[test]
fn another_move_preserves_three_origins_and_rejects_old_control_ids() {
    let mut p = prepared();
    finish(&mut p);
    let bound = p.target_template.readiness_requirements().snapshot_bytes;
    let source = MetadataAuthoritySource::from_serving(p.target_template.clone(), bound)
        .unwrap_or_else(|_| panic!("C source"));
    let template = MetadataPublishingSource::new(source).unwrap();
    let mut c = template.clone();
    let boot = c.bootstrap_command(100000).unwrap();
    apply(&mut c, 8, boot, &mut vec![]);
    c.apply_batch(&p.target_entries[1..]).unwrap();
    let current = update(11, 5);
    apply(&mut c, 93, current.clone(), &mut vec![]);
    let plan = c.source().plan(group(13)).unwrap();
    let freeze = c.source().freeze_command(&plan, 100000).unwrap();
    apply(&mut c, 9, freeze, &mut vec![]);
    let image = c.source().export(100000).unwrap();
    let mut d =
        MetadataServingTarget::new(template.clone(), plan.clone(), op(9), cfg(3), cfg(4)).unwrap();
    let b = d.bootstrap_command(100000).unwrap();
    activation::target_apply(&mut d, 9, b);
    let b = d.import_command(&image, cfg(3), 100000).unwrap();
    activation::target_apply(&mut d, 9, b);
    let b = c
        .publication_command(d.status().target.imported.unwrap(), cfg(4), 100000)
        .unwrap();
    let MetadataPublishingOutcome::Published(status) = apply(&mut c, 9, b, &mut vec![]) else {
        panic!("C publication")
    };
    let b = d.activation_command(status, 100000).unwrap();
    activation::target_apply(&mut d, 9, b);
    for (id, command, authority, index) in [
        (1001, p.old_command, 1, 2),
        (90, p.local_command, 9, 4),
        (93, current, 11, 4),
    ] {
        assert!(
            matches!(activation::target_apply(&mut d,id,command),MetadataServingOutcome::Historical{source,index:i,..} if source==group(authority) && i==index)
        );
    }
    for id in [6, 7, 8, 9] {
        assert_eq!(
            activation::target_apply(&mut d, id, update(13, 7)),
            MetadataServingOutcome::Conflict
        );
    }
    let cp = checkpoint(&d);
    let mut restored = MetadataServingTarget::new(template, plan, op(9), cfg(3), cfg(4)).unwrap();
    restored
        .restore_checkpoint(d.schema_version(), d.applied_index(), &cp)
        .unwrap();
    assert_eq!(checkpoint(&restored), cp);
    assert_eq!(
        restored
            .active_directory()
            .unwrap()
            .manifest(grant().input().responsibility)
            .unwrap()
            .input()
            .authority,
        group(13)
    );
}

#[test]
fn nested_construction_is_bounded_before_ownership_transfer() {
    let mut source = original();
    let mut manifests = vec![grant()];
    let mut from = group(1);
    let mut successful = 0;
    for depth in 1..=MAX_METADATA_MOVES + 1 {
        let to = group(100 + depth as u128);
        let plan = MetadataMovePlan::new(from, to, manifests).unwrap();
        manifests = plan.updated_manifests();
        let target =
            MetadataServingTarget::new(source, plan, op(100 + depth as u128), cfg(1), cfg(2))
                .unwrap();
        let budget = target.readiness_requirements().snapshot_bytes;
        match MetadataAuthoritySource::from_serving(target, budget) {
            Ok(next) => {
                source = MetadataPublishingSource::new(next).unwrap();
                successful += 1;
                from = to;
            }
            Err((ApplicationError::InvalidCommand, returned)) => {
                assert_eq!(returned.applied_index(), 0);
                assert!(!checkpoint(&returned).is_empty());
                assert!(successful > 1 && successful < MAX_METADATA_MOVES);
                return;
            }
            Err(_) => panic!("typed construction refusal"),
        }
    }
    panic!("unbounded nested profile");
}

fn prepare_original() -> (MetadataPublishingSource, MetadataImage, Vec<u8>) {
    // A reserves and publishes a namespace before its first move.
    let mut a = original();
    let mut ignored = vec![];
    let b = a.bootstrap_command(100000).unwrap();
    apply(&mut a, 1000, b, &mut ignored);
    let old_command = DirectoryCommand {
        expected: None,
        manifest: grant(),
    }
    .encode(100000)
    .unwrap();
    apply(&mut a, 1001, old_command.clone(), &mut ignored);
    let stale = a.source().plan(group(9)).unwrap();
    for index in 3..=6 {
        a.apply_batch(&[LogEntry {
            index,
            term: 1,
            payload: EntryPayload::Noop,
        }])
        .unwrap();
    }
    let intent = activation::create_intent(group(1), 30, 1);
    apply(&mut a, 1002, intent.encode(100000).unwrap(), &mut ignored);
    let created = a
        .source()
        .active_directory()
        .unwrap()
        .group_creation_at(a.applied_index(), group(30))
        .unwrap()
        .unwrap();
    apply(
        &mut a,
        1003,
        activation::ready_namespace(created).encode(100000).unwrap(),
        &mut ignored,
    );
    let refused = a.source().freeze_command(&stale, 100000).unwrap();
    assert_eq!(
        apply(&mut a, 6, refused, &mut ignored),
        MetadataPublishingOutcome::Source(MetadataSourceOutcome::Conflict)
    );
    let plan = a.source().plan(group(9)).unwrap();
    let command = a.source().freeze_command(&plan, 100000).unwrap();
    apply(&mut a, 7, command, &mut ignored);
    let first_image = a.source().export(100000).unwrap();
    (a, first_image, old_command)
}

fn verify_repeated_history(p: &mut Prepared) {
    assert!(
        matches!(activation::target_apply(&mut p.target, 1001, p.old_command.clone()), MetadataServingOutcome::Historical { source, index: 2, outcome: DirectoryOutcome::Published(_)} if source == group(1))
    );
    assert!(
        matches!(activation::target_apply(&mut p.target, 90, p.local_command.clone()), MetadataServingOutcome::Historical { source, index: 4, outcome: DirectoryOutcome::Published(_)} if source == group(9))
    );
    assert!(
        matches!(activation::target_apply(&mut p.target, 90, p.old_command.clone()), MetadataServingOutcome::Historical { source, index: 4, outcome: DirectoryOutcome::OperationConflict } if source == group(9))
    );
    for id in [6, 7, 8] {
        assert_eq!(
            activation::target_apply(&mut p.target, id, p.local_command.clone()),
            MetadataServingOutcome::Conflict
        );
    }
    for (g, authority, index) in [(30, 1, 7), (40, 9, 5)] {
        let MetadataServingRead::Creation(Some(c)) = p
            .target
            .read_at(
                p.target.applied_index(),
                MetadataServingQuery::Creation(group(g)),
            )
            .unwrap()
        else {
            panic!("creation")
        };
        assert_eq!((c.authority, c.index), (group(authority), index));
        let q = MetadataServingQuery::Directory(DirectoryQuery::Publication(if g == 30 {
            op(1003)
        } else {
            op(92)
        }));
        assert!(
            matches!(p.target.read_at(p.target.applied_index(),q).unwrap(), MetadataServingRead::Historical {source,..} if source == group(authority))
        );
    }
}
