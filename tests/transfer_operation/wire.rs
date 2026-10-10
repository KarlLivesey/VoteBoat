// SPDX-License-Identifier: RPL-1.5
// Copyright (c) 2026 Karl Livesey
use super::*;
#[path = "wire_generated.rs"]
mod generated;
#[test]
fn transient_observations_roundtrip_and_reject_truncation_and_trailing_bytes() {
    let mut views = Views::imported();
    views.publish();
    for original in views.reads() {
        let bytes = original.encode(MAX_TRANSFER_OBSERVATION_BYTES).unwrap();
        let decoded = TransferObservation::decode_authenticated(&bytes).unwrap();
        assert_eq!(decoded.read(), original.read());
        assert_eq!(decoded.configuration(), original.configuration());
        assert_eq!(decoded.index(), original.index());
        assert_eq!(
            decoded.encode(MAX_TRANSFER_OBSERVATION_BYTES).unwrap(),
            bytes
        );
        assert!(original.encode(bytes.len() - 1).is_err());
        for end in 0..bytes.len() {
            assert!(
                TransferObservation::decode_authenticated(&bytes[..end]).is_err(),
                "accepted prefix {end}"
            );
        }
        let mut trailing = bytes.clone();
        trailing.push(0);
        assert!(TransferObservation::decode_authenticated(&trailing).is_err());
        let mut corrupt = bytes;
        corrupt[0] ^= 1;
        assert!(TransferObservation::decode_authenticated(&corrupt).is_err());
    }
}
#[test]
fn decoded_observations_preserve_phase_decisions_and_do_not_certify_foreign_groups() {
    let views = Views::imported();
    let reads = views.reads();
    let decoded = reads
        .iter()
        .map(|r| {
            TransferObservation::decode_authenticated(
                &r.encode(MAX_TRANSFER_OBSERVATION_BYTES).unwrap(),
            )
            .unwrap()
        })
        .collect::<Vec<_>>();
    assert_eq!(plan().next(&reads, &[]), plan().next(&decoded, &[]));
    let mut bytes = reads[0].encode(MAX_TRANSFER_OBSERVATION_BYTES).unwrap();
    bytes[8..24].copy_from_slice(&99u128.to_le_bytes());
    let foreign = TransferObservation::decode_authenticated(&bytes).unwrap();
    let mut wrong = decoded;
    wrong[0] = foreign;
    assert_eq!(
        plan().next(&wrong, &[]),
        Err(TransferOperationError::UnexpectedRead)
    );
    assert!(TransferObservation::decode_authenticated(&vec![
        0;
        MAX_TRANSFER_OBSERVATION_BYTES + 1
    ])
    .is_err());
}
