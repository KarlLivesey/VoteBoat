// SPDX-License-Identifier: RPL-1.5
// Copyright (c) 2026 Karl Livesey
use super::*;
#[test]
fn persist_effect_preserves_original_capacity_and_pending_has_one_exact_normalized_suffix() {
    let mut core = leader();
    let mut input = proposal(100);
    input.bytes.reserve(10000);
    let raw_pointer = input.bytes.as_ptr();
    let raw_capacity = input.bytes.capacity();
    let effects = core.propose_batch(vec![input]).unwrap();
    let original = effects
        .into_iter()
        .find_map(|effect| {
            if let Effect::Persist(update) = effect {
                Some(update)
            } else {
                None
            }
        })
        .unwrap();
    let EntryPayload::Command { bytes, .. } = &original.suffix.as_ref().unwrap().entries[0].payload
    else {
        unreachable!()
    };
    assert_eq!(bytes.as_ptr(), raw_pointer);
    assert_eq!(bytes.capacity(), raw_capacity);
    let next = &core.pending.as_ref().unwrap().next;
    let owned = next.update().suffix.as_ref().unwrap();
    let EntryPayload::Command { bytes, .. } = &owned.entries[0].payload else {
        unreachable!()
    };
    let retained = bytes.as_ptr();
    assert_ne!(retained, raw_pointer);
    assert_eq!(bytes.capacity(), bytes.len());
    let EntryPayload::Command { bytes, .. } = &next.entry_at(2).unwrap().payload else {
        unreachable!()
    };
    assert_eq!(bytes.as_ptr(), retained);
    assert_eq!(core.validate_persist_effect(&original), Ok(()));
    for operation in [false, true] {
        let mut wrong = original.clone();
        let EntryPayload::Command {
            operation: id,
            bytes,
        } = &mut wrong.suffix.as_mut().unwrap().entries[0].payload
        else {
            unreachable!()
        };
        if operation {
            *id = OperationId::new(999).unwrap();
        } else {
            bytes[0] ^= 1;
        }
        assert_eq!(
            core.validate_persist_effect(&wrong),
            Err(RaftError::WrongCompletion)
        );
        assert!(core
            .pending
            .as_ref()
            .unwrap()
            .next
            .matches_update(&original));
    }
    complete_exact(&mut core);
    assert_eq!(payload_pointers(&core), vec![retained]);
}
