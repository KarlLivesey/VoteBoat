// SPDX-License-Identifier: RPL-1.5
// Copyright (c) 2026 Karl Livesey
use super::*;

#[test]
fn prepared_singleton_actual_producer_and_selector_work_witness() {
    let mut core = leader();
    let operation = OperationId::new((1u128 << 127) + 19).unwrap();
    let effects = core
        .step(Event::Propose {
            operation,
            bytes: vec![9; 37],
        })
        .unwrap();
    let [Effect::Persist(update)] = effects.as_slice() else {
        panic!("actual proposal persistence")
    };
    let entries = &update.suffix.as_ref().unwrap().entries;
    assert_eq!(entries.len(), 1);
    assert_eq!(entries[0].index, 2);
    assert_eq!(entries[0].term, 1);
    assert!(
        matches!(&entries[0].payload, EntryPayload::Command { operation: actual, bytes } if *actual == operation && bytes.as_slice() == [9;37])
    );
    println!(
        "CPU6_SINGLETON producer_entry_slots={}",
        entries.allocated_entries()
    );
    let expected = entries[0].clone();
    complete_exact(&mut core);
    let selected = core.state().entries.replication_range(1, 1, 1 << 20);
    assert_eq!(selected, [expected]);
    println!(
        "CPU6_SINGLETON selector_entry_slots={}",
        selected.allocated_entries()
    );
    assert_eq!(core.state().last_index(), 2);
}
