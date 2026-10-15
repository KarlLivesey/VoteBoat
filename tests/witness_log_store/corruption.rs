// SPDX-License-Identifier: RPL-1.5
// Copyright (c) 2026 Karl Livesey
use super::*;

#[test]
fn exhausted_publication_counter_or_session_never_wraps_or_rewrites_the_witnesses() {
    let root = Directory::new("exhaustion");
    let store = populated(&root);
    drop(store);
    let (wal, metadata) = pair(&root);
    for offset in [24, 56] {
        let mut primary = wal.clone();
        let mut secondary = metadata.clone();
        for record in [&mut primary[..128], &mut secondary[..]] {
            record[offset..offset + 8].copy_from_slice(&u64::MAX.to_le_bytes());
            rechecksum(record);
        }
        write_pair(&root, &primary, &secondary);
        assert!(recover(&root).is_err());
        assert_eq!(pair(&root), (primary, secondary));
    }
}

#[test]
fn every_acknowledged_wal_truncation_rejects_with_either_witness_crc_damaged() {
    let root = Directory::new("truncation");
    let store = populated(&root);
    drop(store);
    let (wal, metadata) = pair(&root);
    for damaged in [0, 1, 2] {
        let mut original = wal.clone();
        let mut secondary = metadata.clone();
        match damaged {
            1 => original[124] ^= 1,
            2 => secondary[124] ^= 1,
            _ => (),
        }
        for cut in 0..wal.len() {
            write_pair(&root, &original[..cut], &secondary);
            assert!(
                recover(&root).is_err(),
                "physicalcut={cut}, damagedwitness={damaged}"
            );
        }
    }
}

#[test]
fn sole_crc_valid_witness_recovers_exact_history_and_repairs_its_counterpart() {
    for damaged in [1, 2] {
        let root = Directory::new("repair");
        let store = populated(&root);
        let expected = store.state(group(1)).unwrap();
        let session = store.binding().session;
        drop(store);
        let (mut wal, mut metadata) = pair(&root);
        if damaged == 1 {
            wal[124] ^= 1;
        } else {
            metadata[124] ^= 1;
        }
        write_pair(&root, &wal, &metadata);
        let recovered = recover(&root).unwrap();
        assert_eq!(recovered.state(group(1)).unwrap(), expected);
        assert_eq!(recovered.binding().session.get(), session.get() + 1);
        drop(recovered);
        let (wal, metadata) = pair(&root);
        assert_eq!(&wal[16..124], &metadata[16..124]);
        let again = recover(&root).unwrap();
        assert_eq!(again.state(group(1)).unwrap(), expected);
    }
}

#[test]
fn both_bad_witnesses_or_corruption_inside_acknowledged_frames_reject() {
    let root = Directory::new("damaged");
    let store = populated(&root);
    drop(store);
    let (original, metadata) = pair(&root);
    let mut bad_a = original.clone();
    let mut bad_b = metadata.clone();
    bad_a[124] ^= 1;
    bad_b[124] ^= 1;
    write_pair(&root, &bad_a, &bad_b);
    assert!(recover(&root).is_err());
    for damaged in [0, 1, 2] {
        let mut wal = original.clone();
        let mut secondary = metadata.clone();
        *wal.last_mut().unwrap() ^= 1;
        if damaged == 1 {
            wal[124] ^= 1;
        } else if damaged == 2 {
            secondary[124] ^= 1;
        }
        write_pair(&root, &wal, &secondary);
        assert!(recover(&root).is_err());
    }
}

fn invalid_fields() -> Vec<(&'static str, usize, Vec<u8>)> {
    vec![
        ("magic", 0, b"UNKNOWN!".to_vec()),
        ("version", 8, 2_u32.to_le_bytes().to_vec()),
        ("role", 12, 3_u32.to_le_bytes().to_vec()),
        ("generation", 16, 19_u64.to_le_bytes().to_vec()),
        ("counter", 24, 0_u64.to_le_bytes().to_vec()),
        ("store", 32, vec![0; 16]),
        ("incarnation", 48, 0_u64.to_le_bytes().to_vec()),
        ("session", 56, 0_u64.to_le_bytes().to_vec()),
        ("length", 64, u64::MAX.to_le_bytes().to_vec()),
        ("reserved", 72, vec![1]),
    ]
}

#[test]
fn checksum_valid_invalid_fields_are_errors_instead_of_fallback_candidates() {
    let root = Directory::new("fields");
    let store = populated(&root);
    drop(store);
    let (wal, metadata) = pair(&root);
    for (name, offset, bytes) in invalid_fields() {
        for side in [1, 2] {
            let mut primary = wal.clone();
            let mut secondary = metadata.clone();
            let record = if side == 1 {
                &mut primary[..128]
            } else {
                &mut secondary[..]
            };
            record[offset..offset + bytes.len()].copy_from_slice(&bytes);
            rechecksum(record);
            write_pair(&root, &primary, &secondary);
            assert!(recover(&root).is_err(), "field={name}, witness={side}");
        }
    }
}

#[test]
fn contradictory_pairs_reject_b_ahead_equal_counter_metadata_and_crossed_identity() {
    let root = Directory::new("pair");
    let store = populated(&root);
    drop(store);
    let (wal, metadata) = pair(&root);
    let counter = u64::from_le_bytes(metadata[24..32].try_into().unwrap());
    let session = u64::from_le_bytes(metadata[56..64].try_into().unwrap());
    let length = u64::from_le_bytes(metadata[64..72].try_into().unwrap());
    let cases = [
        (2, 24, counter + 1),
        (1, 56, session + 2),
        (1, 64, length - 1),
        (1, 32, 2),
        (1, 48, 2),
    ];
    for (side, offset, value) in cases {
        let mut primary = wal.clone();
        let mut secondary = metadata.clone();
        let record = if side == 1 {
            &mut primary[..128]
        } else {
            &mut secondary[..]
        };
        record[offset..offset + 8].copy_from_slice(&value.to_le_bytes());
        rechecksum(record);
        write_pair(&root, &primary, &secondary);
        assert!(recover(&root).is_err(), "side={side}, offset={offset}");
    }
    // A may be a later candidate, but may not retreat below B or skip sessions.
    for (offset, value) in [(64, length - 1), (56, session + 2)] {
        let mut primary = wal.clone();
        primary[24..32].copy_from_slice(&(counter + 3).to_le_bytes());
        primary[offset..offset + 8].copy_from_slice(&value.to_le_bytes());
        rechecksum(&mut primary[..128]);
        write_pair(&root, &primary, &metadata);
        assert!(recover(&root).is_err());
    }
}

#[test]
fn a_ahead_counter_gap_remains_candidate_with_b_mandatory() {
    let root = Directory::new("candidate");
    let store = populated(&root);
    let expected = store.state(group(1)).unwrap();
    drop(store);
    let (mut wal, metadata) = pair(&root);
    let counter = u64::from_le_bytes(wal[24..32].try_into().unwrap());
    wal[24..32].copy_from_slice(&(counter + 4).to_le_bytes());
    rechecksum(&mut wal[..128]);
    write_pair(&root, &wal, &metadata);
    let reopened = recover(&root).unwrap();
    assert_eq!(reopened.state(group(1)).unwrap(), expected);
}
