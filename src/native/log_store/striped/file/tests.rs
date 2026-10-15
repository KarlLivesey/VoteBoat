// SPDX-License-Identifier: RPL-1.5
use super::super::{StripedJournal, StripedLimits, StripedLogStore};
use super::*;
use crate::{contracts::StorageError, identity::*, log::*};
mod fixture;
mod io_driver;
use fixture::*;
use io_driver::*;
#[test]
fn actual_file_catalog_every_publication_primitive_cut_keeps_exact_acknowledged_prefix() {
    let mut count = 0;
    for cut in cuts() {
        let layout = Layout::new();
        let (mut store, selector) = layout.seeded();
        let old = store.state(group()).unwrap();
        let original = command(&old, 0x72);
        let next = expected(&old, &original);
        let old_binding = store.binding();
        let tickets = store.append_batch(vec![original.clone()]).unwrap();
        assert_eq!(
            store.state(group()).unwrap(),
            old,
            "admission is not acknowledgment"
        );
        selector.arm(cut);
        assert!(
            matches!(store.barrier(&tickets), Err(StorageError::Uncertain(_))),
            "cut{cut:?}"
        );
        assert!(selector.visited(), "actual primitive cut{cut:?} reached");
        assert_eq!(store.barrier(&tickets), Err(StorageError::Fenced));
        fenced(&mut store, &layout, original);
        drop(store);
        let mut recovered = layout.recover();
        assert!(recovered.binding().session > old_binding.session);
        assert_eq!(recovered.barrier(&tickets), Err(StorageError::StaleTicket));
        let selected = if selects_new(cut) { &next } else { &old };
        exact(&recovered, selected);
        assert_eq!(
            recovered
                .state(group())
                .unwrap()
                .entries
                .view(..old.entries.len())
                .to_vec(),
            old.entries.to_vec()
        );
        let follow = command(selected, 0x93);
        let followed = expected(selected, &follow);
        commit(&mut recovered, vec![follow]);
        exact(&recovered, &followed);
        drop(recovered);
        let recovered = layout.recover();
        exact(&recovered, &followed);
        drop(recovered);
        count += 1;
    }
    assert_eq!(count, 30);
}
#[test]
fn actual_file_catalog_reclaim_cuts_preserve_original_frames_through_next_parity_reuse() {
    let mut count = 0;
    for cut in cuts() {
        let layout = Layout::new();
        let (mut store, selector) = layout.seeded();
        let second = command(&store.state(group()).unwrap(), 0x72);
        commit(&mut store, vec![second]);
        fill(&mut store);
        let first = store.reclaim(store.limits().max_wal_bytes).unwrap();
        assert!(first.after_bytes < first.before_bytes);
        let third = command(&store.state(group()).unwrap(), 0x88);
        commit(&mut store, vec![third]);
        fill(&mut store);
        let original = store.state(group()).unwrap();
        let binding = store.binding();
        selector.arm(cut);
        assert!(
            matches!(
                store.reclaim(store.limits().max_wal_bytes),
                Err(StorageError::Uncertain(_))
            ),
            "cut{cut:?}"
        );
        assert!(
            selector.visited(),
            "actual reclaim primitive cut{cut:?} reached"
        );
        fenced(&mut store, &layout, command(&original, 0x93));
        drop(store);
        let generation = if selects_new(cut) { 3 } else { 2 };
        assert_eq!(
            layout.generation(),
            generation,
            "exact visible publication generation{cut:?}"
        );
        let mut recovered = layout.recover();
        assert!(recovered.binding().session > binding.session);
        exact(&recovered, &original);
        // The next actual checkpoint reuses the opposite parity slot. Native
        // original frame/operation equality forbids surviving wrong-generation data.
        let mutation = command(&original, 0x93);
        let next = expected(&original, &mutation);
        commit(&mut recovered, vec![mutation]);
        fill(&mut recovered);
        let expected = recovered.state(group()).unwrap();
        assert_eq!(
            expected.entries.view(..next.entries.len()).to_vec(),
            next.entries.to_vec()
        );
        let reclaimed = recovered.reclaim(recovered.limits().max_wal_bytes).unwrap();
        assert!(reclaimed.after_bytes < reclaimed.before_bytes);
        drop(recovered);
        assert_eq!(layout.generation(), generation + 1);
        let recovered = layout.recover();
        exact(&recovered, &expected);
        drop(recovered);
        count += 1;
    }
    assert_eq!(count, 30);
}
