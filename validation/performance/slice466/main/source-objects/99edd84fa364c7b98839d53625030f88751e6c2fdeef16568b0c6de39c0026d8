// SPDX-License-Identifier: RPL-1.5
// Copyright (c) 2026 Karl Livesey
use super::*;
#[test]
fn persist_effect_captures_raw_proposal_once_and_pending_shares_exact_immutable_payload() {
    let mut core = leader();
    let mut input = proposal(100);
    input.bytes.reserve(10000);
    let raw_pointer = input.bytes.as_ptr();
    let logical_len = input.bytes.len();
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
    assert_ne!(bytes.as_ptr(), raw_pointer);
    let captured = bytes.as_ptr();
    assert_eq!(
        bytes.retained_bytes(),
        (logical_len + 2 * std::mem::size_of::<std::sync::atomic::AtomicUsize>())
            .next_multiple_of(std::mem::align_of::<std::sync::atomic::AtomicUsize>().max(2))
    );
    let next = &core.pending.as_ref().unwrap().next;
    let owned = next.update().suffix.as_ref().unwrap();
    let EntryPayload::Command { bytes, .. } = &owned.entries[0].payload else {
        unreachable!()
    };
    let retained = bytes.as_ptr();
    assert_eq!(retained, captured);
    assert_eq!(
        bytes.retained_bytes(),
        (logical_len + 2 * std::mem::size_of::<std::sync::atomic::AtomicUsize>())
            .next_multiple_of(std::mem::align_of::<std::sync::atomic::AtomicUsize>().max(2))
    );
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
            let mut raw = bytes.to_vec();
            raw[0] ^= 1;
            *bytes = raw.into();
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
