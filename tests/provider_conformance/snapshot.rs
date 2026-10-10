// SPDX-License-Identifier: RPL-1.5
// Copyright (c) 2026 Karl Livesey
//! Caller-supplied providers share exact identity and retention obligations.
use super::support::*;
use voteboat::{identity::*, snapshot::*};

pub fn metadata(index: u64) -> SnapshotMetadata {
    SnapshotMetadata {
        bootstrap: bootstrap(1, 1),
        membership: None,
        index,
        term: 1,
        application_schema: 1,
    }
}

fn altered_tickets(ticket: SnapshotTicket) -> Vec<SnapshotTicket> {
    let changes: [fn(&mut SnapshotTicket); 6] = [
        |t| t.binding.identity.id = StoreId::new(999).unwrap(),
        |t| t.binding.identity.incarnation = StoreIncarnation::new(999).unwrap(),
        |t| t.binding.session = StoreSession::new(t.binding.session.get() + 1).unwrap(),
        |t| t.group.id = GroupId::new(999).unwrap(),
        |t| t.group.incarnation = GroupIncarnation::new(999).unwrap(),
        |t| t.generation = SnapshotGeneration::new(t.generation.get() + 1).unwrap(),
    ];
    changes
        .into_iter()
        .map(|change| {
            let mut bad = ticket;
            change(&mut bad);
            bad
        })
        .collect()
}

fn altered_references(reference: SnapshotRef) -> Vec<SnapshotRef> {
    let changes: [fn(&mut SnapshotRef); 11] = [
        |r| r.store.id = StoreId::new(999).unwrap(),
        |r| r.store.incarnation = StoreIncarnation::new(999).unwrap(),
        |r| r.group.id = GroupId::new(999).unwrap(),
        |r| r.group.incarnation = GroupIncarnation::new(999).unwrap(),
        |r| r.generation = SnapshotGeneration::new(r.generation.get() + 1).unwrap(),
        |r| r.configuration = ConfigurationId::new(r.configuration.get() + 1).unwrap(),
        |r| r.index += 1,
        |r| r.term += 1,
        |r| r.application_schema += 1,
        |r| r.file_bytes += 1,
        |r| r.checksum ^= 1,
    ];
    changes
        .into_iter()
        .map(|change| {
            let mut bad = reference;
            change(&mut bad);
            bad
        })
        .collect()
}

fn write(store: &mut impl SnapshotStore, ticket: SnapshotTicket, bytes: &[u8]) {
    let size = store.limits().max_chunk_bytes;
    for (i, chunk) in bytes.chunks(size).enumerate() {
        store.write_chunk(ticket, i * size, chunk).unwrap();
    }
}

fn prepare(store: &mut impl SnapshotStore, index: u64) -> SealedSnapshot {
    let ticket = store.begin(metadata(index), 7).unwrap();
    write(store, ticket, &[index as u8; 7]);
    store.seal(ticket).unwrap()
}

pub fn publish(store: &mut impl SnapshotStore, index: u64) -> SnapshotReceipt {
    let sealed = prepare(store, index);
    store.publish(sealed).unwrap()
}

pub fn assert_image(store: &mut impl SnapshotStore, index: u64) {
    assert_eq!(
        store.load().unwrap(),
        Some(Snapshot {
            metadata: metadata(index),
            application: vec![index as u8; 7],
        })
    );
}

pub fn stage_identity_and_limits(store: &mut impl SnapshotStore) {
    let binding = store.binding();
    assert_eq!(binding.identity, store.identity().store);
    assert_eq!(store.identity().group, group(1));
    let limits = store.limits().validate().unwrap();
    assert_eq!(limits.max_chunk_bytes, 4);
    assert_eq!(store.load().unwrap(), None);
    assert!(store.begin(metadata(1), 0).is_err());
    assert!(store
        .begin(metadata(1), limits.max_application_bytes + 1)
        .is_err());
    let mut foreign = metadata(1);
    foreign.bootstrap.group = group(2);
    assert!(store.begin(foreign, 7).is_err());
    let ticket = store.begin(metadata(1), 7).unwrap();
    for bad in altered_tickets(ticket) {
        assert!(store.write_chunk(bad, 0, &[1]).is_err());
        assert!(store.seal(bad).is_err());
        assert!(store.abort(bad).is_err());
        assert_eq!(store.binding(), binding);
        assert_eq!(store.load().unwrap(), None);
    }
    assert!(store.begin(metadata(2), 7).is_err());
    assert!(store.seal(ticket).is_err());
    assert!(store.write_chunk(ticket, 1, &[1]).is_err());
    assert!(store.write_chunk(ticket, 0, &[]).is_err());
    assert!(store.write_chunk(ticket, 0, &[1; 5]).is_err());
    store.write_chunk(ticket, 0, &[1; 4]).unwrap();
    assert!(store.write_chunk(ticket, 0, &[1]).is_err());
    assert!(store.write_chunk(ticket, 4, &[1; 4]).is_err());
    store.write_chunk(ticket, 4, &[1; 3]).unwrap();
    exact_publication(store, ticket);
}

fn exact_publication(store: &mut impl SnapshotStore, ticket: SnapshotTicket) {
    let sealed = store.seal(ticket).unwrap();
    for bad in altered_tickets(ticket) {
        assert!(store
            .publish(SealedSnapshot {
                ticket: bad,
                ..sealed
            })
            .is_err());
    }
    for bad in [
        SealedSnapshot {
            file_bytes: sealed.file_bytes + 1,
            ..sealed
        },
        SealedSnapshot {
            checksum: sealed.checksum ^ 1,
            ..sealed
        },
    ] {
        assert!(store.publish(bad).is_err());
    }
    assert!(store.write_chunk(ticket, 7, &[1]).is_err());
    assert!(store.seal(ticket).is_err());
    assert_eq!(store.load().unwrap(), None);
    let receipt = store.publish(sealed).unwrap();
    assert_eq!(receipt.sealed, sealed);
    assert_eq!(receipt.metadata, metadata(1));
    assert_image(store, 1);
    assert!(store.publish(sealed).is_err());
    assert!(store.abort(ticket).is_err());
    assert!(store.begin(metadata(1), 7).is_err());
}

pub fn abort_preserves_root(store: &mut impl SnapshotStore) {
    for sealed in [false, true] {
        let ticket = store.begin(metadata(2), 7).unwrap();
        write(store, ticket, &[2; 7]);
        let prepared = sealed.then(|| store.seal(ticket).unwrap());
        assert_image(store, 1);
        store.abort(ticket).unwrap();
        assert!(store.write_chunk(ticket, 0, &[2]).is_err());
        if let Some(prepared) = prepared {
            assert!(store.publish(prepared).is_err());
        }
        assert_image(store, 1);
    }
}

pub fn two_pins(store: &mut impl SnapshotRetention) -> [SnapshotRef; 2] {
    let first = store.latest_reference().unwrap().unwrap();
    assert!(store.load_pinned(first).is_err());
    store.pin_for_log(first).unwrap();
    store.pin_for_log(first).unwrap();
    for bad in altered_references(first) {
        assert!(store.pin_for_log(bad).is_err());
        assert!(store.load_pinned(bad).is_err());
        assert!(store.reconcile_log(Some(bad)).is_err());
        assert_eq!(store.latest_reference().unwrap(), Some(first));
        assert_eq!(store.load_pinned(first).unwrap().application, [1; 7]);
    }
    let second = publish(store, 2).reference();
    store.pin_for_log(second).unwrap();
    assert_two_pins(store, [first, second]);
    [first, second]
}

pub fn assert_two_pins(store: &mut impl SnapshotRetention, pins: [SnapshotRef; 2]) {
    assert_eq!(store.latest_reference().unwrap(), Some(pins[1]));
    for (reference, index) in pins.into_iter().zip([1, 2]) {
        let image = store.load_pinned(reference).unwrap();
        assert_eq!(image.metadata, metadata(index));
        assert_eq!(image.application, [index as u8; 7]);
        store.pin_for_log(reference).unwrap();
    }
    assert!(store.begin(metadata(3), 7).is_err());
    assert_image(store, 2);
}

pub fn reconcile(store: &mut impl SnapshotRetention, pins: [SnapshotRef; 2]) {
    store.reconcile_log(Some(pins[0])).unwrap();
    assert_image(store, 1);
    assert!(store.load_pinned(pins[1]).is_err());
    let ticket = store.begin(metadata(3), 7).unwrap();
    assert!(store.reconcile_log(None).is_err());
    assert!(store.reconcile_log(Some(pins[0])).is_err());
    assert_image(store, 1);
    store.abort(ticket).unwrap();
    store.reconcile_log(None).unwrap();
    assert_eq!(store.latest_reference().unwrap(), None);
    assert_eq!(store.load().unwrap(), None);
    for reference in pins {
        assert!(store.load_pinned(reference).is_err());
        assert!(store.pin_for_log(reference).is_err());
    }
    publish(store, 4);
    assert_image(store, 4);
}

pub fn cross_store(first: &mut impl SnapshotRetention, second: &mut impl SnapshotRetention) {
    let a = prepare(first, 1);
    let b = prepare(second, 1);
    assert_ne!(a.ticket.binding, b.ticket.binding);
    assert!(first.publish(b).is_err());
    assert!(second.publish(a).is_err());
    assert_eq!(first.load().unwrap(), None);
    assert_eq!(second.load().unwrap(), None);
    let a = first.publish(a).unwrap();
    let b = second.publish(b).unwrap();
    assert!(first.pin_for_log(b.reference()).is_err());
    assert!(second.pin_for_log(a.reference()).is_err());
    assert_image(first, 1);
    assert_image(second, 1);
}
