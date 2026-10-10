// SPDX-License-Identifier: RPL-1.5
// Copyright (c) 2026 Karl Livesey
//! Reusable cases: providers are supplied by callers through the public seam.
use super::support::*;
use voteboat::{identity::*, log::*};

pub fn cross_store(first: &mut impl LogStore, second: &mut impl LogStore) {
    assert_ne!(first.binding(), second.binding());
    let a = first
        .append_batch(vec![LogMutation::Create(bootstrap(50, 3))])
        .unwrap();
    let b = second
        .append_batch(vec![LogMutation::Create(bootstrap(50, 3))])
        .unwrap();
    assert!(first.barrier(&b).is_err());
    assert!(second.barrier(&a).is_err());
    assert!(first.state(group(50)).is_err());
    assert!(second.state(group(50)).is_err());
    assert_eq!(first.barrier(&a).unwrap().tickets, a);
    assert_eq!(second.barrier(&b).unwrap().tickets, b);
}

fn forged(ticket: LogTicket) -> Vec<LogTicket> {
    let changes: [fn(&mut LogTicket); 10] = [
        |t| t.binding.identity.id = StoreId::new(999).unwrap(),
        |t| {
            t.binding.identity.incarnation =
                StoreIncarnation::new(t.binding.identity.incarnation.get() + 1).unwrap()
        },
        |t| t.binding.session = StoreSession::new(t.binding.session.get() + 1).unwrap(),
        |t| t.group.id = GroupId::new(999).unwrap(),
        |t| t.group.incarnation = GroupIncarnation::new(t.group.incarnation.get() + 1).unwrap(),
        |t| t.batch += 1,
        |t| t.revision = LogRevision::new(t.revision.get() + 1).unwrap(),
        |t| t.generation = LogGeneration::new(t.generation.get() + 1).unwrap(),
        |t| t.last_index += 1,
        |t| t.term += 1,
    ];
    changes
        .into_iter()
        .map(|change| {
            let mut altered = ticket;
            change(&mut altered);
            altered
        })
        .collect()
}

/// The fixture supplies the provider's byte charge for one one-byte entry.
/// Do not impose a native on-disk encoding on a replacement provider.
pub fn exercise(store: &mut impl LogStore, one_entry_bytes: usize) {
    store.limits().validate().unwrap();
    let binding = store.binding();
    let tickets = store
        .append_batch(vec![LogMutation::Create(bootstrap(1, 3))])
        .unwrap();
    assert_eq!(tickets.len(), 1);
    assert_eq!(tickets[0].binding, binding);
    for bad in forged(tickets[0]) {
        assert!(store.barrier(&[bad]).is_err(), "accepted forged {bad:?}");
        assert!(
            store.state(group(1)).is_err(),
            "forged ticket advanced durable state"
        );
        assert_eq!(store.binding(), binding);
    }
    assert_eq!(store.barrier(&tickets).unwrap().tickets, tickets);
    assert_eq!(store.state(group(1)).unwrap().bootstrap, bootstrap(1, 3));
    suffix_and_ranges(store, one_entry_bytes);
    refusal_and_reclamation(store);
}

fn suffix_and_ranges(store: &mut impl LogStore, one_entry_bytes: usize) {
    let empty = store.state(group(1)).unwrap();
    append(
        store,
        vec![update(
            &empty,
            1,
            1,
            Some(Suffix {
                from: 1,
                entries: vec![entry(1, 1, 7), entry(2, 1, 8)],
            }),
        )],
    );
    let original = store.state(group(1)).unwrap();
    let waiting = store
        .append_batch(vec![update(
            &original,
            1,
            1,
            Some(Suffix {
                from: 3,
                entries: vec![entry(3, 1, 9)],
            }),
        )])
        .unwrap();
    assert_eq!(store.state(group(1)).unwrap(), original);
    let mut replacement = update(
        &original,
        2,
        1,
        Some(Suffix {
            from: 2,
            entries: vec![entry(2, 2, 10), entry(3, 2, 11)],
        }),
    );
    let LogMutation::Update(change) = &mut replacement else {
        unreachable!()
    };
    change.expected_revision = waiting[0].revision;
    let current = store.append_batch(vec![replacement]).unwrap();
    assert_ne!(current[0].generation, waiting[0].generation);
    assert!(store.barrier(&waiting).is_err());
    assert_eq!(store.state(group(1)).unwrap(), original);
    assert_eq!(store.barrier(&current).unwrap().tickets, current);
    let state = store.state(group(1)).unwrap();
    assert_eq!(state.commit_index, 1);
    assert_eq!(
        state.entries,
        vec![entry(1, 1, 7), entry(2, 2, 10), entry(3, 2, 11)]
    );
    assert!(store
        .fetch_range(group(1), original.generation, 1, 3, 1024)
        .is_err());
    assert_eq!(
        store
            .fetch_range(group(1), state.generation, 2, 1, 1024)
            .unwrap(),
        vec![entry(2, 2, 10)]
    );
    assert!(store
        .fetch_range(group(1), state.generation, 1, 1, 1)
        .is_err());
    assert_eq!(
        store
            .fetch_range(group(1), state.generation, 1, 3, one_entry_bytes)
            .unwrap(),
        vec![entry(1, 1, 7)]
    );
    assert_eq!(
        store
            .fetch_range(group(1), state.generation, 4, 1, 1024)
            .unwrap(),
        vec![]
    );
    assert!(store
        .fetch_range(group(1), state.generation, 0, 1, 1024)
        .is_err());
    assert!(store
        .fetch_range(group(1), state.generation, 1, 0, 1024)
        .is_err());
}

fn refusal_and_reclamation(store: &mut impl LogStore) {
    let before = store.state(group(1)).unwrap();
    let mut oversized = entry(4, 2, 12);
    let EntryPayload::Command { bytes, .. } = &mut oversized.payload else {
        unreachable!()
    };
    *bytes = vec![1; store.limits().max_command_bytes + 1];
    assert!(store
        .append_batch(vec![update(
            &before,
            2,
            1,
            Some(Suffix {
                from: 4,
                entries: vec![oversized],
            })
        )])
        .is_err());
    assert_eq!(store.state(group(1)).unwrap(), before);
    let pending = store
        .append_batch(vec![LogMutation::Create(bootstrap(2, 3))])
        .unwrap();
    assert!(store.reclaim(store.limits().max_wal_bytes).is_err());
    assert_eq!(store.state(group(1)).unwrap(), before);
    assert!(store.state(group(2)).is_err());
    store.barrier(&pending).unwrap();
    let other = store.state(group(2)).unwrap();
    assert!(store.reclaim(0).is_err());
    if store.supports_reclaim() {
        let report = store.reclaim(store.limits().max_wal_bytes).unwrap();
        assert!(report.after_bytes <= report.before_bytes);
    } else {
        assert!(store.reclaim(store.limits().max_wal_bytes).is_err());
    }
    assert_eq!(store.state(group(1)).unwrap(), before);
    assert_eq!(store.state(group(2)).unwrap(), other);
    assert_eq!(store.state(group(50)).unwrap().bootstrap, bootstrap(50, 3));
}

#[cfg(feature = "native")]
pub fn reopened(store: &mut impl LogStore, old: StoreBinding, states: &[GroupLog]) {
    assert_eq!(store.binding().identity, old.identity);
    assert_ne!(store.binding().session, old.session);
    for state in states {
        assert_eq!(store.state(state.bootstrap.group).unwrap(), *state);
    }
    let before = store.state(group(1)).unwrap();
    let tickets = store
        .append_batch(vec![update(
            &before,
            2,
            1,
            Some(Suffix {
                from: 4,
                entries: vec![entry(4, 2, 12)],
            }),
        )])
        .unwrap();
    let mut stale = tickets[0];
    stale.binding = old;
    assert!(store.barrier(&[stale]).is_err());
    assert_eq!(store.state(group(1)).unwrap(), before);
    assert_eq!(store.barrier(&tickets).unwrap().tickets, tickets);
    assert_eq!(store.state(group(1)).unwrap().last_index(), 4);
}
