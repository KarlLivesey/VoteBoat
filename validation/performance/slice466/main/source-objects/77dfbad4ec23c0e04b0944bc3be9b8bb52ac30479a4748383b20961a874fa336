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
fn fixture() -> (MetadataImage, MetadataMovePlan) {
    let (mut source, _) = ready(8);
    let plan = source.plan(group(9)).unwrap();
    let mut stale = grant().into_input();
    stale.generation = RouteGeneration::new(2).unwrap();
    let bad = MetadataMovePlan::new(
        group(1),
        group(9),
        vec![ResponsibilityManifest::new(stale).unwrap()],
    )
    .unwrap();
    let bad = source.freeze_command(&bad, 100000).unwrap();
    assert_eq!(commit(&mut source, 6, bad), MetadataSourceOutcome::Conflict);
    let start = source.applied_index() + 1;
    source
        .apply_batch(
            &(start..=11)
                .map(|index| LogEntry {
                    index,
                    term: 1,
                    payload: EntryPayload::Noop,
                })
                .collect::<Vec<_>>(),
        )
        .unwrap();
    let command = source.freeze_command(&plan, 100000).unwrap();
    assert!(matches!(
        commit(&mut source, 7, command),
        MetadataSourceOutcome::Frozen(_)
    ));
    (source.export(100000).unwrap(), plan)
}
fn target(plan: MetadataMovePlan) -> MetadataAuthorityTarget {
    MetadataAuthorityTarget::new(fresh(8), plan, op(7), ConfigurationId::new(1).unwrap()).unwrap()
}
fn apply(t: &mut MetadataAuthorityTarget, id: u128, bytes: Vec<u8>) -> MetadataTargetOutcome {
    t.apply_batch(&[entry(t.applied_index() + 1, id, bytes)])
        .unwrap()
        .remove(0)
        .outcome
}
#[test]
fn metadata_import_preserves_source_indices_and_never_serves_as_active_authority() {
    let (image, plan) = fixture();
    let mut t = target(plan);
    let command = t
        .import_command(&image, ConfigurationId::new(1).unwrap(), 100000)
        .unwrap();
    assert_eq!(
        apply(&mut t, 7, command.clone()),
        MetadataTargetOutcome::NotActive
    );
    assert!(t.historical_directory().is_none());
    let boot = t.bootstrap_command(40).unwrap();
    assert_eq!(
        apply(&mut t, 7, boot.clone()),
        MetadataTargetOutcome::Staged { index: 2 }
    );
    let outcome = apply(&mut t, 7, command.clone());
    let MetadataTargetOutcome::Imported(status) = outcome else {
        panic!("import")
    };
    assert_eq!(status.index, 3);
    assert_eq!(status.source.index, 12);
    assert_eq!(status.source.source, group(1));
    assert_eq!(t.imported_image(), Some(&image));
    assert_eq!(image.rejected_operations(), &[(op(6), 3)]);
    let history = t.historical_directory().unwrap();
    assert_eq!(history.applied_index(), 12);
    assert!(history.validate_group(group(9)).is_err());
    assert_eq!(
        history
            .read_at(12, DirectoryQuery::Manifest(grant().input().responsibility))
            .unwrap(),
        DirectoryRead::Manifest(Some(grant()))
    );
    let publish = DirectoryCommand {
        expected: None,
        manifest: grant(),
    }
    .encode(100000)
    .unwrap();
    let mut history = history.clone();
    let receipt = history
        .apply_batch(&[entry(13, 1001, publish.clone())])
        .unwrap()
        .remove(0);
    assert!(receipt.duplicate);
    assert_eq!(
        receipt.outcome,
        DirectoryOutcome::Published(RouteGeneration::new(1).unwrap())
    );
    assert!(t
        .validate_proposal(op(1001), &publish, std::iter::empty())
        .is_err());
    assert_eq!(
        apply(&mut t, 1001, publish),
        MetadataTargetOutcome::Conflict
    );
    assert_eq!(apply(&mut t, 7, command), outcome);
    assert_eq!(
        apply(&mut t, 7, boot),
        MetadataTargetOutcome::Staged { index: 2 }
    );
    assert_eq!(t.status().imported, Some(status));
    assert_eq!(
        t.read_at(t.applied_index(), MetadataTargetQuery::Status)
            .unwrap(),
        t.status()
    );
}
#[test]
fn target_import_and_checkpoint_bind_all_provenance_and_recover_local_phase_indices() {
    let (image, plan) = fixture();
    let mut t = target(plan.clone());
    let boot = t.bootstrap_command(40).unwrap();
    apply(&mut t, 7, boot.clone());
    let command = t
        .import_command(&image, ConfigurationId::new(1).unwrap(), 100000)
        .unwrap();
    assert_eq!(
        command.len(),
        244 + plan.encode(MAX_METADATA_PLAN_BYTES).unwrap().len()
            + image.bytes().len()
            + 24 * image.rejected_operations().len()
    );
    assert!(t
        .import_command(&image, ConfigurationId::new(2).unwrap(), 100000)
        .is_err());
    assert!(t
        .import_command(&image, ConfigurationId::new(1).unwrap(), command.len() - 1)
        .is_err());
    let before = t.checkpoint(100000).unwrap();
    for end in 0..command.len() {
        let mut copy = t.clone();
        assert!(
            copy.apply_batch(&[entry(2, 7, command[..end].to_vec())])
                .is_err()
                || copy.status().imported.is_none()
        );
        assert!(t
            .validate_proposal(op(7), &command[..end], std::iter::empty())
            .is_err());
    }
    for offset in [8, 40, 48, 72, 96, 112, 120, 128, 160, 192, 224, 232] {
        let mut changed = command.clone();
        changed[offset] ^= 1;
        assert!(t.apply_batch(&[entry(2, 7, changed)]).is_err());
        assert_eq!(t.checkpoint(100000).unwrap(), before);
    }
    assert!(matches!(
        apply(&mut t, 7, command.clone()),
        MetadataTargetOutcome::Imported(_)
    ));
    let checkpoint = t.checkpoint(100000).unwrap();
    let state = t.status();
    let mut copy = target(plan.clone());
    for end in 0..checkpoint.len() {
        assert!(copy
            .restore_checkpoint(
                METADATA_TARGET_SCHEMA,
                t.applied_index(),
                &checkpoint[..end]
            )
            .is_err());
        assert!(copy.status().staged_index.is_none());
    }
    copy.restore_checkpoint(METADATA_TARGET_SCHEMA, t.applied_index(), &checkpoint)
        .unwrap();
    assert_eq!(copy.status(), state);
    assert_eq!(copy.imported_image(), Some(&image));
    assert_eq!(
        apply(&mut copy, 7, command),
        MetadataTargetOutcome::Imported(state.imported.unwrap())
    );
    assert_eq!(
        apply(&mut copy, 7, boot),
        MetadataTargetOutcome::Staged { index: 1 }
    );
    let other =
        MetadataAuthorityTarget::new(fresh(8), plan, op(8), ConfigurationId::new(1).unwrap())
            .unwrap();
    assert!(other
        .clone()
        .restore_checkpoint(METADATA_TARGET_SCHEMA, 2, &checkpoint)
        .is_err());
    assert!(target(image.plan().clone())
        .restore_checkpoint(99, 2, &checkpoint)
        .is_err());
}
#[test]
fn target_construction_pending_order_and_changed_imports_refuse() {
    let (image, plan) = fixture();
    let (live, _) = ready(8);
    assert!(MetadataAuthorityTarget::new(
        live,
        plan.clone(),
        op(7),
        ConfigurationId::new(1).unwrap()
    )
    .is_err());
    let wrong = MetadataMovePlan::new(group(2), group(9), {
        let mut m = grant().into_input();
        m.authority = group(2);
        vec![ResponsibilityManifest::new(m).unwrap()]
    })
    .unwrap();
    assert!(
        MetadataAuthorityTarget::new(fresh(8), wrong, op(7), ConfigurationId::new(1).unwrap())
            .is_err()
    );
    let mut t = target(plan);
    let boot = t.bootstrap_command(40).unwrap();
    let command = t
        .import_command(&image, ConfigurationId::new(1).unwrap(), 100000)
        .unwrap();
    assert!(t
        .validate_proposal(op(7), &command, std::iter::empty())
        .is_err());
    assert!(t
        .validate_proposal(op(7), &command, std::iter::once((op(7), boot.as_slice())))
        .is_ok());
    apply(&mut t, 7, boot);
    apply(&mut t, 7, command.clone());
    let before = t.status();
    let mut changed = command;
    changed[160] ^= 1;
    assert!(t
        .validate_proposal(op(7), &changed, std::iter::empty())
        .is_err());
    assert_eq!(apply(&mut t, 7, changed), MetadataTargetOutcome::Conflict);
    assert_eq!(t.status(), before);
    assert!(t.validate_group(group(1)).is_err());
    assert!(t.validate_group(group(9)).is_ok());
}
#[cfg(feature = "native")]
#[test]
fn interrupted_native_metadata_import_recovers_only_stage_or_complete_history() {
    use support::{Fault, ModelIo};
    use voteboat::native::log_store::*;
    let (image, plan) = fixture();
    let mut t = target(plan.clone());
    let boot = t.bootstrap_command(40).unwrap();
    let command = t
        .import_command(&image, ConfigurationId::new(1).unwrap(), 100000)
        .unwrap();
    let entries = vec![entry(1, 7, boot), entry(2, 7, command.clone())];
    t.apply_batch(&entries).unwrap();
    let expected = t.status();
    let limits = LogLimits::default();
    let seed = || {
        let io = ModelIo::default();
        let mut log = NativeLogStore::create(io.clone(), support::identity(1), limits).unwrap();
        support::append(
            &mut log,
            vec![LogMutation::Create(support::bootstrap(9, 3))],
        );
        let state = log.state(group(9)).unwrap();
        support::append(
            &mut log,
            vec![support::update(
                &state,
                1,
                1,
                Some(Suffix {
                    from: 1,
                    entries: entries[..1].to_vec(),
                }),
            )],
        );
        (io, log)
    };
    let (_, log) = seed();
    let mutation = support::update(
        &log.state(group(9)).unwrap(),
        1,
        2,
        Some(Suffix {
            from: 2,
            entries: entries[1..].to_vec(),
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
        let state = log.state(group(9)).unwrap();
        let mut recovered = target(plan.clone());
        recovered
            .apply_batch(
                &state
                    .entries
                    .into_iter()
                    .filter(|e| e.index <= state.commit_index)
                    .collect::<Vec<_>>(),
            )
            .unwrap();
        if state.commit_index == 1 {
            outcomes[0] = true;
            assert!(recovered.imported_image().is_none());
        } else {
            outcomes[1] = true;
            assert_eq!(recovered.status(), expected);
            assert_eq!(recovered.imported_image(), Some(&image));
        }
        assert!(matches!(
            apply(&mut recovered, 7, command.clone()),
            MetadataTargetOutcome::Imported(_)
        ));
        assert_eq!(recovered.imported_image(), Some(&image));
        let cp = recovered.checkpoint(100000).unwrap();
        let mut reopened = target(plan.clone());
        reopened
            .restore_checkpoint(METADATA_TARGET_SCHEMA, recovered.applied_index(), &cp)
            .unwrap();
        assert_eq!(reopened.imported_image(), Some(&image));
    }
    assert_eq!(outcomes, [true, true]);
}

#[test]
fn rehashed_rejection_records_and_command_shaped_fence_boundaries_refuse() {
    let (image, plan) = fixture();
    let mut t = target(plan);
    let boot = t.bootstrap_command(40).unwrap();
    apply(&mut t, 7, boot);
    let command = t
        .import_command(&image, ConfigurationId::new(1).unwrap(), 100000)
        .unwrap();
    let tail = command.len() - 24;
    for (operation, index) in [(op(6), 0u64), (op(6), 12), (op(7), 3), (op(1000), 3)] {
        let mut changed = command.clone();
        changed[tail..tail + 16].copy_from_slice(&operation.get().to_le_bytes());
        changed[tail + 16..tail + 24].copy_from_slice(&index.to_le_bytes());
        let hash = ContentDigest::sha256(&changed[tail - 4..]);
        changed[192..224].copy_from_slice(&hash.0);
        assert!(t.apply_batch(&[entry(2, 7, changed)]).is_err());
    }
    // Forge a matching checkpoint digest but place F on the original publish
    // command. A real source freeze occupies a directory no-op, never a command.
    let mut changed = command;
    changed[112..120].copy_from_slice(&2u64.to_le_bytes());
    changed.truncate(tail);
    changed[tail - 4..tail].copy_from_slice(&0u32.to_le_bytes());
    changed[192..224].copy_from_slice(&ContentDigest::sha256(&0u32.to_le_bytes()).0);
    let plan_len = u32::from_le_bytes(changed[232..236].try_into().unwrap()) as usize;
    let body_len_at = 236 + plan_len;
    let body_at = body_len_at + 4;
    changed[body_at + 8..body_at + 16].copy_from_slice(&2u64.to_le_bytes());
    let body_len = u32::from_le_bytes(changed[body_len_at..body_at].try_into().unwrap()) as usize;
    let hash = ContentDigest::sha256(&changed[body_at..body_at + body_len]);
    changed[160..192].copy_from_slice(&hash.0);
    assert!(t.apply_batch(&[entry(2, 7, changed)]).is_err());
    assert!(t.status().imported.is_none());
}
