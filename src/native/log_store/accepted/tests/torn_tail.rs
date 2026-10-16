// SPDX-License-Identifier: RPL-1.5
// Copyright (c) 2026 Karl Livesey
//! An interrupted append past the recovery boundary was never acknowledged.
//! Damage there ends the log; damage inside the boundary remains corruption.
use super::*;

fn identity() -> StoreIdentity {
    bootstrap(1).voter_stores[&NodeId::new(1).unwrap()]
}
fn append_unacknowledged(store: &mut Store, index: u64) {
    let update = mutation(
        store,
        1,
        0,
        Some(Suffix {
            from: index,
            entries: vec![entry(index, 1)],
        }),
    );
    store.append_batch(vec![update]).unwrap();
}
fn damage(io: &Memory, range: std::ops::Range<usize>) {
    for byte in &mut io.0.borrow_mut().bytes[range] {
        *byte ^= 0xff;
    }
}
fn recover(io: Memory) -> Result<Store, StorageError> {
    NativeLogStore::recover(io, identity(), LogLimits::default())
}

#[test]
fn damaged_unacknowledged_header_or_body_recovers_the_acknowledged_prefix() {
    // Offsets 0 and 16 damage the frame header; usize::MAX selects the
    // middle of the checksummed body.
    for damaged in [0, 16, usize::MAX] {
        let mut store = open(1);
        let acknowledged = store.state(group(1)).unwrap();
        let boundary = store.io.0.borrow().bytes.len();
        append_unacknowledged(&mut store, 1);
        let io = store.io.clone();
        drop(store);
        let end = io.0.borrow().bytes.len();
        let at = if damaged == usize::MAX {
            (boundary + end) / 2
        } else {
            boundary + damaged
        };
        damage(&io, at..at + 1);
        let mut recovered = recover(io.clone()).unwrap();
        assert_eq!(io.0.borrow().bytes.len(), boundary);
        let state = recovered.state(group(1)).unwrap();
        assert_eq!(state.entries, acknowledged.entries);
        assert_eq!(state.hard_state, acknowledged.hard_state);
        // The recovered store accepts new durable work and reopens again.
        append_unacknowledged(&mut recovered, 1);
        let tickets = recovered.pending.clone();
        recovered.barrier(&tickets).unwrap();
        drop(recovered);
        assert_eq!(
            recover(io).unwrap().state(group(1)).unwrap().last_index(),
            1
        );
    }
}

#[test]
fn valid_unacknowledged_frames_before_the_damage_are_still_replayed() {
    let mut store = open(1);
    append_unacknowledged(&mut store, 1);
    let first = store.io.0.borrow().bytes.len();
    append_unacknowledged(&mut store, 2);
    let io = store.io.clone();
    drop(store);
    damage(&io, first + 16..first + 17);
    let recovered = recover(io.clone()).unwrap();
    assert_eq!(recovered.state(group(1)).unwrap().last_index(), 1);
    assert_eq!(io.0.borrow().bytes.len(), first);
}

#[test]
fn damage_inside_the_acknowledged_prefix_remains_corruption() {
    let mut store = open(1);
    let update = mutation(
        &store,
        1,
        0,
        Some(Suffix {
            from: 1,
            entries: vec![entry(1, 1)],
        }),
    );
    commit(&mut store, vec![update]);
    let io = store.io.clone();
    drop(store);
    let end = io.0.borrow().bytes.len();
    let manifest = io.0.borrow().manifest.clone();
    assert_eq!(read_manifest(&manifest.unwrap()).unwrap().1, end as u64);
    damage(&io, end - 40..end - 39);
    assert!(matches!(recover(io), Err(StorageError::Corrupt(_))));
}

#[test]
fn every_bit_of_an_unacknowledged_frame_recovers_the_acknowledged_prefix() {
    let mut store = open(1);
    let acknowledged = store.state(group(1)).unwrap();
    let boundary = store.io.0.borrow().bytes.len();
    append_unacknowledged(&mut store, 1);
    let full = store.io.0.borrow().bytes.clone();
    let manifest = store.io.0.borrow().manifest.clone();
    drop(store);
    for bit in boundary * 8..full.len() * 8 {
        let mut bytes = full.clone();
        bytes[bit / 8] ^= 1 << (bit % 8);
        let image = Memory::default();
        image.0.borrow_mut().bytes = bytes;
        image.0.borrow_mut().manifest = manifest.clone();
        let recovered = recover(image.clone()).unwrap_or_else(|e| panic!("bit={bit}: {e:?}"));
        let state = recovered.state(group(1)).unwrap();
        // A flip in trailing padding leaves a valid, replayable frame.
        assert!(
            state.entries == acknowledged.entries || state.last_index() == 1,
            "bit={bit}"
        );
        if state.entries == acknowledged.entries {
            assert_eq!(image.0.borrow().bytes.len(), boundary, "bit={bit}");
        }
    }
}
