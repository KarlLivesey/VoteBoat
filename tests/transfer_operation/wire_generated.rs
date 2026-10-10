// SPDX-License-Identifier: RPL-1.5
// Copyright (c) 2026 Karl Livesey
use super::*;

const STAGED: usize = 89;
const IMPORT_TAG: usize = 97;
const IMPORTED: usize = 98;
const SOURCE_COUNT: usize = 138;
const SOURCE_RECORD_BYTES: usize = 124;

fn active() -> TransferObservation {
    let mut views = Views::imported();
    views.publish();
    let mut target = target_fixture::fresh();
    let import = target_fixture::from_source(
        &target_fixture::frozen(),
        21,
        ConfigurationId::new(1).unwrap(),
    );
    target
        .apply_batch(&[
            source_fixture::entry(1, 200, target.bootstrap_command(65536).unwrap()),
            source_fixture::entry(2, 200, target.import_command(&import, 65536).unwrap()),
        ])
        .unwrap();
    target
        .apply_batch(&[source_fixture::entry(
            3,
            200,
            target
                .activation_command(
                    &TargetActivation {
                        metadata_configuration: ConfigurationId::new(1).unwrap(),
                        decision: views.publication.unwrap(),
                    },
                    65536,
                )
                .unwrap(),
        )])
        .unwrap();
    TransferObservation::target_read(&outcome(21, 1, TargetRead::<()>::Status(target.status())))
        .unwrap()
}

fn activation_tag(bytes: &[u8]) -> usize {
    let count = u16::from_le_bytes(bytes[SOURCE_COUNT..SOURCE_COUNT + 2].try_into().unwrap());
    SOURCE_COUNT + 2 + usize::from(count) * SOURCE_RECORD_BYTES
}
fn put_index(bytes: &mut [u8], offset: usize, value: u64) {
    bytes[offset..offset + 8].copy_from_slice(&value.to_le_bytes());
}

#[test]
fn target_wire_rejects_missing_and_reversed_local_phase_prerequisites() {
    let original = active().encode(MAX_TRANSFER_OBSERVATION_BYTES).unwrap();
    TransferObservation::decode_authenticated(&original).unwrap();
    let activated = activation_tag(&original) + 1;
    let publication = activated + 8 + 32 + 8 + 16;
    assert_eq!(original.len(), publication + 8);
    for (case, (stage, import, activation, metadata)) in [
        (0, 2, 3, 10),
        (2, 2, 3, 10),
        (3, 2, 4, 10),
        (1, 3, 3, 10),
        (1, 4, 3, 10),
        (1, 2, 0, 10),
        (1, 2, 3, 0),
        (102, 103, 104, 10),
        (1, 102, 103, 10),
        (1, 2, 102, 10),
    ]
    .into_iter()
    .enumerate()
    {
        let mut invalid = original.clone();
        for (offset, value) in [
            (STAGED, stage),
            (IMPORTED, import),
            (activated, activation),
            (publication, metadata),
        ] {
            put_index(&mut invalid, offset, value);
        }
        assert_eq!(
            TransferObservation::decode_authenticated(&invalid).unwrap_err(),
            ApplicationError::InvalidCommand,
            "accepted impossible local phase sequence case={case}"
        );
    }
    let mut missing_import = original[..IMPORT_TAG].to_vec();
    missing_import.push(0);
    missing_import.extend(&original[activation_tag(&original)..]);
    assert_eq!(
        TransferObservation::decode_authenticated(&missing_import).unwrap_err(),
        ApplicationError::InvalidCommand
    );
}

#[test]
fn foreign_publication_prefix_is_independent_of_target_local_prefix() {
    let mut bytes = active().encode(MAX_TRANSFER_OBSERVATION_BYTES).unwrap();
    let publication = activation_tag(&bytes) + 1 + 8 + 32 + 8 + 16;
    for foreign_index in [1, 10000, u64::MAX] {
        put_index(&mut bytes, publication, foreign_index);
        let decoded = TransferObservation::decode_authenticated(&bytes).unwrap();
        assert_eq!(decoded.index(), 101);
        assert_eq!(
            decoded.encode(MAX_TRANSFER_OBSERVATION_BYTES).unwrap(),
            bytes
        );
    }
}
