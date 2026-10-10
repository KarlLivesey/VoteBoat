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
use voteboat::retirement::*;
type Guard = RetirementGuard<Target>;
fn guard(first: &TransferIntent) -> Guard {
    RetirementGuard::new(selected(first, 2)).unwrap_or_else(|_| panic!("guard"))
}
fn proof(c: &Completed) -> RetirementProof {
    let cfg = ConfigurationId::new(1).unwrap();
    let status = c.t.freeze_status().unwrap().unwrap();
    RetirementProof {
        metadata_configuration: cfg,
        decision: c.decision.clone(),
        targets: c
            .successors
            .iter()
            .map(|t| TargetActivationEvidence::from_status(cfg, t.status()).unwrap())
            .collect(),
        release: RetentionRelease {
            source: group(21),
            operation: status.fence.operation,
            fence_index: status.fence.index,
            release: op(9900),
        },
    }
}
#[test]
fn partial_owner_retires_after_remaining_handoff_and_preserves_its_authority_history() {
    for split in [false, true] {
        let c = completed(split);
        let p = proof(&c);
        let mut g = guard(&c.first);
        assert!(
            g.readiness_requirements().snapshot_bytes
                >= g.owner().unwrap().retirement_lineage_bound()
        );
        g.apply_batch(&c.log).unwrap();
        assert_eq!(
            g.owner().unwrap().checkpoint(500000).unwrap(),
            c.t.checkpoint(500000).unwrap()
        );
        let lineage = c.t.retirement_lineage().unwrap();
        assert!(lineage.windows(8).any(|w| w == b"VBTPRTL1"));
        let first_export = c.t.export_scoped(op(2000), 65536).unwrap();
        assert!(!lineage
            .windows(first_export.bytes().len())
            .any(|w| w == first_export.bytes()));
        let mut incomplete = p.clone();
        incomplete.targets.clear();
        assert!(g
            .retirement_command(&incomplete, MAX_RETIREMENT_COMMAND_BYTES)
            .is_err());
        let b = g
            .retirement_command(&p, MAX_RETIREMENT_COMMAND_BYTES)
            .unwrap();
        g.validate_proposal(p.release.operation, &b, std::iter::empty())
            .unwrap();
        assert!(matches!(
            commit(&mut g, 3000, b.clone()).outcome,
            RetirementOutcome::Retired(_)
        ));
        let status = g.status().unwrap();
        assert!(g.owner().is_none());
        assert_eq!(g.retired_lineage(), Some(lineage.as_slice()));
        assert_eq!(g.freeze_status().unwrap(), c.t.freeze_status().unwrap());
        assert!(g.export_target(group(40), 65536).is_err());
        let cp = g.checkpoint(500000).unwrap();
        let mut fresh = guard(&c.first);
        fresh
            .restore_checkpoint(RETIREMENT_GUARD_SCHEMA, g.applied_index(), &cp)
            .unwrap();
        assert!(fresh.owner().is_none());
        assert_eq!(fresh.status(), Some(status));
        assert_eq!(fresh.retired_lineage(), Some(lineage.as_slice()));
        assert_eq!(
            commit(&mut fresh, 3000, b).outcome,
            RetirementOutcome::Retired(status)
        );
        assert_eq!(
            fresh
                .read_at(
                    fresh.applied_index(),
                    RetirementQuery::Owner(TargetQuery::Status)
                )
                .unwrap(),
            RetirementRead::Retired
        );
        for end in 0..cp.len() {
            let mut candidate = guard(&c.first);
            let before = candidate.checkpoint(500000).unwrap();
            assert!(candidate
                .restore_checkpoint(RETIREMENT_GUARD_SCHEMA, g.applied_index(), &cp[..end])
                .is_err());
            assert_eq!(candidate.checkpoint(500000).unwrap(), before);
        }
        assert_eq!(c.successors[0].application().value(&[100]), Ok(9));
    }
}
fn repair_hash(bytes: &mut [u8], offset: usize) {
    let n = u32::from_le_bytes(bytes[offset + 56..offset + 60].try_into().unwrap()) as usize;
    let mut b = b"VBTPART1".to_vec();
    b.extend(&bytes[offset + 32..offset + 56]);
    b.extend(&bytes[offset + 60..offset + 60 + n]);
    bytes[offset..offset + 32].copy_from_slice(&ContentDigest::sha256(&b).0);
}
#[test]
fn retirement_rejects_rehashed_history_discontinuities_and_mismatched_final_grants() {
    let c = completed(false);
    let initial = selected(&c.first, 2);
    let source = c.t.freeze_status().unwrap().unwrap();
    let lineage = c.t.retirement_lineage().unwrap();
    initial
        .validate_retirement_evidence(&source, &lineage)
        .unwrap();
    for end in 0..lineage.len() {
        assert!(initial
            .validate_retirement_evidence(&source, &lineage[..end])
            .is_err());
    }
    let activation_len = u32::from_le_bytes(lineage[8..12].try_into().unwrap()) as usize;
    let first = 22 + activation_len;
    let mut changed = lineage.clone();
    changed[first + 32..first + 48].copy_from_slice(&op(200).get().to_le_bytes());
    repair_hash(&mut changed, first);
    assert!(initial
        .validate_retirement_evidence(&source, &changed)
        .is_err());
    let mut changed = lineage.clone();
    changed[first + 48..first + 56].copy_from_slice(&source.fence.index.to_le_bytes());
    repair_hash(&mut changed, first);
    assert!(initial
        .validate_retirement_evidence(&source, &changed)
        .is_err());
    let mut changed = lineage.clone();
    changed[first + 48..first + 56].copy_from_slice(&3u64.to_le_bytes());
    repair_hash(&mut changed, first);
    assert!(initial
        .validate_retirement_evidence(&source, &changed)
        .is_err());
    let mut changed = lineage.clone();
    changed[first] ^= 1;
    assert!(initial
        .validate_retirement_evidence(&source, &changed)
        .is_err());
    let (mut d, mut other, _, mut logs) = initial_with_directory(2, directory);
    let _ = transfer(&mut d, &mut other, &mut logs, 0, range(0, 64));
    let shorter = other.retirement_lineage().unwrap();
    initial
        .validate_retirement_lineage(&shorter, source.fence.index)
        .unwrap();
    assert!(initial
        .validate_retirement_evidence(&source, &shorter)
        .is_err());
    assert!(selected(&c.first, 1)
        .validate_retirement_evidence(&source, &lineage)
        .is_err());
    assert!(target(&c.first)
        .validate_retirement_evidence(&source, &lineage)
        .is_err());
}
#[cfg(feature = "native")]
#[test]
fn retired_partial_owner_survives_native_publication_interruption_and_reclaims_old_log() {
    let c = completed(false);
    support::retirement_storage::history(
        "partial-import",
        || guard(&c.first),
        c.log.clone(),
        proof(&c),
    );
}
