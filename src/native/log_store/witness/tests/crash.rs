// SPDX-License-Identifier: RPL-1.5
// Copyright (c) 2026 Karl Livesey
use super::*;

#[test]
fn every_torn_candidate_header_with_partial_unsynced_data_preserves_old_boundary() {
    let path = Directory::new();
    let mut store = populated(&path.0);
    let before = fs::read(path.0.join("log.wal")).unwrap();
    let ack = fs::read(path.0.join("MANIFEST")).unwrap();
    let old = store.state(group()).unwrap();
    pending(&mut store, 2);
    let appended = fs::read(path.0.join("log.wal")).unwrap();
    let mut candidate = store.io.primary.unwrap();
    candidate.counter += 1;
    candidate.length = (appended.len() - SIZE) as u64;
    let header = candidate.encode(1);
    drop(store);
    // These are reconstructed durable byte images, not process-crash or physical
    // power-loss claims. Header and appended-data persistence are independent.
    for cut in 0..=SIZE {
        let mut torn = before[..SIZE].to_vec();
        torn[..cut].copy_from_slice(&header[..cut]);
        let primary = format::decode(&torn, 1, 0).unwrap();
        let secondary = format::decode(&ack, 2, 0).unwrap();
        assert_eq!(
            format::mandatory(primary, secondary).unwrap().length,
            (before.len() - SIZE) as u64
        );
    }
    for cut in before.len()..=appended.len() {
        let mut image = appended[..cut].to_vec();
        image[..SIZE].copy_from_slice(&header);
        write_image(&path.0, &image, &ack);
        let recovered = recover(&path.0).unwrap();
        let state = recovered.state(group()).unwrap();
        if cut < appended.len() {
            assert_eq!(state, old, "data cut {cut}");
        } else {
            assert_eq!(state.hard_state.term, 2);
        }
        drop(recovered);
    }
}
#[test]
fn every_secondary_overwrite_byte_cut_recovers_the_synced_complete_transition() {
    let path = Directory::new();
    let mut store = populated(&path.0);
    let old_ack = fs::read(path.0.join("MANIFEST")).unwrap();
    let tickets = pending(&mut store, 2);
    store.barrier(&tickets).unwrap();
    let expected = store.state(group()).unwrap();
    let wal = fs::read(path.0.join("log.wal")).unwrap();
    let new_ack = fs::read(path.0.join("MANIFEST")).unwrap();
    drop(store);
    for cut in 0..=SIZE {
        let mut torn = old_ack.clone();
        torn[..cut].copy_from_slice(&new_ack[..cut]);
        write_image(&path.0, &wal, &torn);
        let recovered = recover(&path.0).unwrap();
        assert_eq!(recovered.state(group()).unwrap(), expected, "cut {cut}");
        assert_eq!(recovered.io.primary, recovered.io.secondary);
    }
}
#[test]
fn every_cold_repair_byte_cut_keeps_the_other_witness_and_restarts_again() {
    let path = Directory::new();
    let store = populated(&path.0);
    let wal = fs::read(path.0.join("log.wal")).unwrap();
    let ack = fs::read(path.0.join("MANIFEST")).unwrap();
    let record = store.io.primary.unwrap();
    drop(store);
    for role in [1, 2] {
        let original = record.encode(role);
        let mut damaged = original;
        damaged[124] ^= 1;
        for cut in 0..=SIZE {
            let mut torn = damaged;
            torn[..cut].copy_from_slice(&original[..cut]);
            let a = if role == 1 {
                format::decode(&torn, 1, 0).unwrap()
            } else {
                Some(record)
            };
            let b = if role == 2 {
                format::decode(&torn, 2, 0).unwrap()
            } else {
                Some(record)
            };
            assert_eq!(format::mandatory(a, b).unwrap(), record);
        }
        for cut in 1..=6 {
            let mut image = wal.clone();
            let mut acknowledgement = ack.clone();
            if role == 1 {
                image[124] ^= 1;
            } else {
                acknowledgement[124] ^= 1;
            }
            write_image(&path.0, &image, &acknowledgement);
            let mut io = WitnessLogIo::open(&path.0).unwrap();
            let manifest = io.read_manifest().unwrap();
            let mut step = 0;
            assert!(io
                .publish_with(&manifest, || {
                    step += 1;
                    if step == cut {
                        Err(io::Error::other("repair/publication interruption"))
                    } else {
                        Ok(())
                    }
                })
                .is_err());
            assert!(WitnessLogIo::open(&path.0).is_err());
            drop(io);
            let first = recover(&path.0).unwrap();
            assert_eq!(first.io.primary, first.io.secondary);
            drop(first);
            drop(recover(&path.0).unwrap());
        }
    }
}
#[test]
fn initialization_primitive_cuts_never_turn_partial_creation_into_empty_recovery() {
    for cut in 1..=10 {
        let path = Directory::new();
        let mut io = WitnessLogIo::create(&path.0).unwrap();
        assert_eq!(
            io.read_manifest().unwrap_err().kind(),
            io::ErrorKind::NotFound
        );
        assert_eq!(io.read_log(1).unwrap(), Vec::<u8>::new());
        let binding = StoreBinding {
            identity: identity(),
            session: StoreSession::new(1).unwrap(),
        };
        let manifest = crate::native::log_store::manifest(binding, 0);
        let mut step = 0;
        assert!(io
            .publish_with(&manifest, || {
                step += 1;
                if step == cut {
                    Err(io::Error::other("initialization interruption"))
                } else {
                    Ok(())
                }
            })
            .is_err());
        assert!(WitnessLogIo::open(&path.0).is_err());
        drop(io);
        if cut < 8 {
            assert!(recover(&path.0).is_err(), "cut {cut}");
        } else {
            assert!(recover(&path.0).is_ok(), "cut {cut}");
        }
        assert!(WitnessLogIo::create(&path.0).is_err());
    }
}
#[test]
fn every_replacement_primitive_cut_selects_exact_old_or_new_pair_with_parity_reuse() {
    for generation in 0..=2 {
        for cut in 1..=16 {
            let path = Directory::new();
            let mut store = populated(&path.0);
            for _ in 0..generation {
                let metadata =
                    crate::native::log_store::manifest(store.binding(), store.length as u64);
                let bytes = store.io.read_log(store.limits.max_wal_bytes).unwrap();
                store.io.replace_log(&bytes, &metadata).unwrap();
            }
            let expected = store.state(group()).unwrap();
            let bytes = store.io.read_log(store.limits.max_wal_bytes).unwrap();
            let metadata = crate::native::log_store::manifest(store.binding(), bytes.len() as u64);
            let mut step = 0;
            assert!(
                store
                    .io
                    .replace_with(&bytes, &metadata, || {
                        step += 1;
                        if step == cut {
                            Err(io::Error::other("replacement interruption"))
                        } else {
                            Ok(())
                        }
                    })
                    .is_err(),
                "generation {generation} cut {cut}"
            );
            assert!(WitnessLogIo::open(&path.0).is_err());
            drop(store);
            let recovered = recover(&path.0).unwrap();
            assert_eq!(recovered.state(group()).unwrap(), expected);
            assert_eq!(
                recovered.io.generation,
                if cut < 12 { generation } else { generation + 1 }
            );
        }
    }
}
