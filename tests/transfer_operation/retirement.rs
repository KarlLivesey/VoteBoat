// SPDX-License-Identifier: RPL-1.5
// Copyright (c) 2026 Karl Livesey
use super::*;
use voteboat::retirement::*;

fn complete() -> Views {
    let mut v = Views::imported();
    v.publish();
    for t in &mut v.targets {
        t.activated = Some(ActivationStatus {
            index: t.imported.as_ref().unwrap().index + 1,
            digest: ContentDigest::sha256(b"synthetic activation"),
            metadata_configuration: ConfigurationId::new(1).unwrap(),
            publication_operation: op(201),
            publication_index: 4,
        });
    }
    v
}
#[test]
fn retirement_proof_requires_complete_matching_observations_and_original_source() {
    assert!(plan()
        .retirement_proof(&Views::imported().reads(), group(20), op(700))
        .is_err());
    let mut v = complete();
    let reads = v.reads();
    let proof = plan().retirement_proof(&reads, group(20), op(700)).unwrap();
    assert_eq!(proof.release.source, group(20));
    assert_eq!(proof.release.operation, op(200));
    assert_eq!(
        proof.release.fence_index,
        v.source.as_ref().unwrap().fence.index
    );
    assert_eq!(proof.release.release, op(700));
    assert_eq!(proof.targets.len(), 2);
    assert_eq!(
        RetirementProof::decode(&proof.encode(MAX_RETIREMENT_PROOF_BYTES).unwrap()).unwrap(),
        proof
    );
    for absent in 0..reads.len() {
        let mut partial = reads.clone();
        partial.remove(absent);
        assert!(plan()
            .retirement_proof(&partial, group(20), op(700))
            .is_err());
    }
    assert!(plan().retirement_proof(&reads, group(21), op(700)).is_err());
    let mut duplicate = reads.clone();
    duplicate.push(reads[0].clone());
    assert!(plan()
        .retirement_proof(&duplicate, group(20), op(700))
        .is_err());
    v.targets[1].activated = None;
    assert!(plan()
        .retirement_proof(&v.reads(), group(20), op(700))
        .is_err());
}
#[test]
fn retirement_proof_preserves_published_fence_and_checks_activation_configuration() {
    let mut v = complete();
    v.source_configuration = 8;
    let proof = plan()
        .retirement_proof(&v.reads(), group(20), op(700))
        .unwrap();
    assert_eq!(
        proof.decision.publication.sources()[0].configuration.get(),
        1
    );
    v.targets[0]
        .activated
        .as_mut()
        .unwrap()
        .metadata_configuration = ConfigurationId::new(2).unwrap();
    assert!(plan()
        .retirement_proof(&v.reads(), group(20), op(700))
        .is_err());
}
