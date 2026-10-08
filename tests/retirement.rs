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
use voteboat::{application::*, bucket_counter::*, identity::*, retirement::*, transfer_target::*};
#[path = "retirement/fixtures.rs"]
mod fixture;
#[path = "retirement/host.rs"]
mod host;
#[path = "transfer_source/fixtures.rs"]
pub mod source_fixture;
use source_fixture::{entry, noop, op};
#[cfg(feature = "native")]
#[path = "retirement/storage.rs"]
mod storage;
#[cfg(feature = "native")]
mod support;
fn retire<S: RetirableOwner>(owner: &mut RetirementGuard<S>, proof: &RetirementProof) -> Vec<u8>
where
    S::Receipt: ApplicationReceipt,
{
    let command = owner
        .retirement_command(proof, MAX_RETIREMENT_COMMAND_BYTES)
        .unwrap();
    owner
        .validate_proposal(proof.release.operation, &command, std::iter::empty())
        .unwrap();
    let index = owner.applied_index() + 1;
    assert!(matches!(
        owner
            .apply_batch(&[entry(index, proof.release.operation.get(), command.clone())])
            .unwrap()[0]
            .outcome,
        RetirementOutcome::Retired(_)
    ));
    command
}
#[test]
fn initial_source_retirement_drops_data_but_retains_original_fence_and_exact_retry() {
    let (mut source, targets, decision) = fixture::active_split();
    let proof = fixture::split_proof(&source, &targets, &decision);
    let frozen = source.freeze_status().unwrap();
    let live = source.checkpoint(500000).unwrap();
    let command = source
        .retirement_command(&proof, MAX_RETIREMENT_COMMAND_BYTES)
        .unwrap();
    let entries = [
        entry(5, 200, command.clone()),
        noop(6),
        entry(7, 200, command.clone()),
        entry(8, 3, source_fixture::data(1, 1)),
    ];
    let bound = source.receipt_bytes_bound(&entries).unwrap();
    let receipts = source.apply_batch(&entries).unwrap();
    assert_eq!(receipts.capacity(), receipts.len());
    assert_eq!(
        bound,
        receipts.len()
            * std::mem::size_of::<
                RetirementReceipt<<source_fixture::Source as StateMachine>::Receipt>,
            >()
    );
    let status = source.status().unwrap();
    assert_eq!(status.index, 5);
    assert_eq!(status.fence_index, 4);
    assert_eq!(receipts[0].outcome, RetirementOutcome::Retired(status));
    assert_eq!(receipts[1].outcome, receipts[0].outcome);
    assert_eq!(receipts[2].outcome, RetirementOutcome::Fenced);
    assert!(source.owner().is_none());
    assert_eq!(source.freeze_status().unwrap(), frozen);
    assert_eq!(source.retired_lineage(), Some(&[][..]));
    assert!(source
        .export_target(source_fixture::group(21), 65536)
        .is_err());
    assert_eq!(
        source
            .read_at(
                8,
                RetirementQuery::Owner(voteboat::transfer_source::SourceQuery::Freeze)
            )
            .unwrap(),
        RetirementRead::Retired
    );
    assert_eq!(
        source.read_at(8, RetirementQuery::Status).unwrap(),
        RetirementRead::Status(Some(status))
    );
    assert!(source.read_at(9, RetirementQuery::Status).is_err());
    let cp = source.checkpoint(500000).unwrap();
    assert!(cp.len() < live.len());
    let mut recovered = fixture::fresh_source();
    recovered
        .restore_checkpoint(RETIREMENT_GUARD_SCHEMA, 8, &cp)
        .unwrap();
    assert!(recovered.owner().is_none());
    assert_eq!(recovered.status(), Some(status));
    assert_eq!(recovered.freeze_status().unwrap(), frozen);
    recovered
        .validate_proposal(op(200), &command, std::iter::empty())
        .unwrap();
    assert!(recovered
        .validate_proposal(op(201), &command, std::iter::empty())
        .is_err());
    let mut changed = proof.clone();
    changed.release.release = op(901);
    let conflicting = recovered
        .retirement_command(&changed, MAX_RETIREMENT_COMMAND_BYTES)
        .unwrap();
    assert!(recovered
        .validate_proposal(op(200), &conflicting, std::iter::empty())
        .is_err());
    assert_eq!(recovered.checkpoint(500000).unwrap(), cp);
    let q = RetirementQuery::Freeze;
    let result = recovered.read_at(8, q.clone()).unwrap();
    let bound = recovered.read_result_bound(&q).unwrap();
    let bytes = recovered.read_result_bytes(&result, usize::MAX).unwrap();
    assert!(bytes + std::mem::size_of_val(&result) <= bound);
    assert!(recovered.read_result_bytes(&result, bytes - 1).is_err());
}
#[test]
fn activated_sources_retire_after_merge_preserving_original_activation_lineage_and_target_data() {
    let (mut original, mut targets, first) = fixture::active_split();
    let original_proof = fixture::split_proof(&original, &targets, &first);
    retire(&mut original, &original_proof);
    let (mut merged, decision) = fixture::merge(&mut targets);
    let merged_status = merged.owner().unwrap().status();
    for (i, t) in targets.iter_mut().enumerate() {
        let lineage = t.owner().unwrap().retirement_lineage().unwrap();
        let old_payload = t.owner().unwrap().application().checkpoint(65536).unwrap();
        let live = t.checkpoint(500000).unwrap();
        let frozen = t.freeze_status().unwrap().unwrap();
        let proof = fixture::proof(&frozen, [merged_status.clone()], &decision, 910 + i as u128);
        retire(t, &proof);
        assert!(t.owner().is_none());
        assert_eq!(t.retired_lineage(), Some(lineage.as_slice()));
        let cp = t.checkpoint(500000).unwrap();
        assert!(cp.len() < live.len());
        assert!(!cp.windows(old_payload.len()).any(|w| w == old_payload));
        let mut recovered = fixture::fresh_target(21 + i as u128);
        recovered
            .restore_checkpoint(RETIREMENT_GUARD_SCHEMA, t.applied_index(), &cp)
            .unwrap();
        assert_eq!(recovered.status(), t.status());
        assert_eq!(recovered.freeze_status().unwrap(), Some(frozen));
        assert_eq!(recovered.retired_lineage(), Some(lineage.as_slice()));
    }
    for (id, key, delta, value, duplicate) in [
        (1, 1, 7, 7, true),
        (2, 200, 11, 11, true),
        (3, 1, 2, 9, false),
    ] {
        let r = merged
            .apply_batch(&[entry(
                merged.applied_index() + 1,
                id,
                fixture::merged_data(key, delta),
            )])
            .unwrap()
            .remove(0);
        let RetirementOutcome::Owner(r) = r.outcome else {
            panic!("live new owner")
        };
        let TargetOutcome::Applied(r) = r.outcome else {
            panic!("data")
        };
        assert_eq!(r.outcome, BucketOutcome::Value(value));
        assert_eq!(r.duplicate, duplicate);
    }
    assert_eq!(merged.owner().unwrap().application().outbox().count(), 3);
    assert_eq!(original.freeze_status().unwrap().unwrap().fence.index, 4);
}
#[test]
fn retirement_requires_complete_matching_activation_and_scoped_retention_release() {
    let source = fixture::frozen_source();
    let mut targets = fixture::imported_targets(&source);
    let decision = fixture::split_decision(&source, &targets);
    assert!(TargetActivationEvidence::from_status(
        fixture::cfg(),
        targets[0].owner().unwrap().status()
    )
    .is_err());
    fixture::activate(&mut targets[0], &decision);
    assert!(TargetActivationEvidence::from_status(
        fixture::cfg(),
        targets[1].owner().unwrap().status()
    )
    .is_err());
    fixture::activate(&mut targets[1], &decision);
    let proof = fixture::split_proof(&source, &targets, &decision);
    let mut cases = Vec::new();
    let mut p = proof.clone();
    p.targets.pop();
    cases.push(p);
    let mut p = proof.clone();
    p.targets.reverse();
    cases.push(p);
    let mut p = proof.clone();
    p.targets[0].activation.publication_index += 1;
    cases.push(p);
    let mut p = proof.clone();
    p.targets[0].import_digest.0[0] ^= 1;
    cases.push(p);
    let mut p = proof.clone();
    p.targets[0].configuration = ConfigurationId::new(2).unwrap();
    cases.push(p);
    let mut p = proof.clone();
    p.metadata_configuration = ConfigurationId::new(2).unwrap();
    cases.push(p);
    let mut p = proof.clone();
    p.release.fence_index += 1;
    cases.push(p);
    let mut p = proof.clone();
    p.release.source = source_fixture::group(99);
    cases.push(p);
    let mut p = proof.clone();
    p.release.operation = op(999);
    cases.push(p);
    let mut p = proof.clone();
    p.targets = Vec::with_capacity(257);
    p.targets.extend(proof.targets.iter().copied());
    cases.push(p);
    for changed in cases {
        assert!(source
            .retirement_command(&changed, MAX_RETIREMENT_COMMAND_BYTES)
            .is_err());
    }
    assert!(fixture::fresh_source()
        .retirement_command(&proof, MAX_RETIREMENT_COMMAND_BYTES)
        .is_err());
    // A wrong local operation cannot execute an otherwise valid deletion command.
    let bytes = source
        .retirement_command(&proof, MAX_RETIREMENT_COMMAND_BYTES)
        .unwrap();
    let before = source.checkpoint(500000).unwrap();
    let mut candidate = source.clone();
    assert!(candidate
        .apply_batch(&[entry(5, 999, bytes.clone())])
        .is_err());
    assert_eq!(candidate.checkpoint(500000).unwrap(), before);
    assert!(candidate
        .apply_batch(&[entry(5, 200, bytes), noop(7)])
        .is_err());
    assert_eq!(candidate.checkpoint(500000).unwrap(), before);
    assert!(candidate.owner().is_some());
}
#[test]
fn retirement_codecs_checkpoint_truncations_replay_and_binding_are_atomic() {
    let (mut source, targets, decision) = fixture::active_split();
    let proof = fixture::split_proof(&source, &targets, &decision);
    let bytes = proof.encode(MAX_RETIREMENT_PROOF_BYTES).unwrap();
    assert_eq!(RetirementProof::decode(&bytes).unwrap(), proof);
    for end in 0..bytes.len() {
        assert!(RetirementProof::decode(&bytes[..end]).is_err());
    }
    assert!(proof.encode(bytes.len() - 1).is_err());
    let command = source
        .retirement_command(&proof, MAX_RETIREMENT_COMMAND_BYTES)
        .unwrap();
    let before = source.checkpoint(500000).unwrap();
    for end in 0..command.len() {
        assert!(source
            .apply_batch(&[entry(5, 200, command[..end].to_vec())])
            .is_err());
        assert_eq!(source.checkpoint(500000).unwrap(), before);
    }
    let mut foreign = command.clone();
    foreign[8] ^= 1;
    assert!(source.apply_batch(&[entry(5, 200, foreign)]).is_err());
    assert_eq!(source.checkpoint(500000).unwrap(), before);
    assert!(source
        .retirement_command(&proof, command.len() - 1)
        .is_err());
    retire(&mut source, &proof);
    source.apply_batch(&[noop(6)]).unwrap();
    let checkpoint = source.checkpoint(500000).unwrap();
    let mut recovered = fixture::fresh_source();
    let pristine = recovered.checkpoint(500000).unwrap();
    for end in 0..checkpoint.len() {
        assert!(recovered
            .restore_checkpoint(RETIREMENT_GUARD_SCHEMA, 6, &checkpoint[..end])
            .is_err());
        assert_eq!(recovered.checkpoint(500000).unwrap(), pristine);
    }
    assert!(recovered.restore_checkpoint(99, 6, &checkpoint).is_err());
    assert!(recovered
        .restore_checkpoint(RETIREMENT_GUARD_SCHEMA, 5, &checkpoint)
        .is_err());
    let mut foreign = checkpoint.clone();
    foreign[16] ^= 1;
    assert!(recovered
        .restore_checkpoint(RETIREMENT_GUARD_SCHEMA, 6, &foreign)
        .is_err());
    assert!(source.checkpoint(checkpoint.len() - 1).is_err());
    let mut replay = fixture::fresh_source();
    let mut log = fixture::source_log();
    log.push(entry(5, 200, command));
    log.push(noop(6));
    replay.apply_batch(&log).unwrap();
    assert_eq!(replay.checkpoint(500000).unwrap(), checkpoint);
    let already_applied = fixture::frozen_source().owner().unwrap().clone();
    assert!(RetirementGuard::new(already_applied).is_err());
}
