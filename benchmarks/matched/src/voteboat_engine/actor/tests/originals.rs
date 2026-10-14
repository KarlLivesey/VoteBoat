// SPDX-License-Identifier: RPL-1.5
// Copyright (c) 2026 Karl Livesey
use super::*;

type Wait = oneshot::Receiver<OperationId>;

fn request(ids: &[OperationId], mixed: bool) -> (Request, Vec<Wait>) {
    let mut originals = Vec::with_capacity(ids.len());
    let mut waits = Vec::with_capacity(ids.len());
    for (n, &operation) in ids.iter().enumerate() {
        let mut bytes = Vec::with_capacity(n % 4 + 11);
        if mixed && n % 3 != 0 {
            bytes.extend_from_slice(&[n as u8, 255]);
        }
        let (reply, wait) = oneshot::channel();
        originals.push(Original {
            operation,
            bytes,
            reply,
        });
        waits.push(wait);
    }
    (Request { originals }, waits)
}

#[test]
fn queued_boundary_moves_mixed_originals_without_receipts_and_drop_cancels_them() {
    let (mut actor, sender) = ready_actor::<false>(crate::Envelopes::Common64);
    let width = actor.parts.core.batch_limits().max_proposal_entries;
    let ids: Vec<_> = (1..width)
        .map(|n| OperationId::new(u128::MAX ^ n as u128).unwrap())
        .collect();
    let (first, mut first_waits) = request(&ids, true);
    let capacities: Vec<_> = first.originals.iter().map(|o| o.bytes.capacity()).collect();
    let pointers: Vec<_> = first.originals.iter().map(|o| o.bytes.as_ptr()).collect();
    let expected: Vec<_> = first.originals.iter().map(|o| o.bytes.clone()).collect();
    let tail_ids = [
        OperationId::new((1 << 127) | 17).unwrap(),
        OperationId::new(u128::MAX).unwrap(),
    ];
    let (tail, mut tail_waits) = request(&tail_ids, true);
    let tail_bytes: Vec<_> = tail.originals.iter().map(|o| o.bytes.clone()).collect();
    assert!(sender.try_send(tail).is_ok());
    let proposals = actor.collect(first).unwrap();
    assert_eq!(proposals.len(), width - 1);
    assert_eq!(actor.pending.len(), width - 1);
    assert_eq!(actor.deferred.as_ref().unwrap().originals.len(), 2);
    for (n, proposal) in proposals.iter().enumerate() {
        assert_eq!(proposal.operation, ids[n]);
        assert_eq!(proposal.bytes, expected[n]);
        assert_eq!(proposal.bytes.capacity(), capacities[n]);
        assert_eq!(proposal.bytes.as_ptr(), pointers[n]);
    }
    for wait in first_waits.iter_mut().chain(&mut tail_waits) {
        assert_eq!(wait.try_recv(), Err(oneshot::error::TryRecvError::Empty));
    }
    let deferred = actor.deferred.as_ref().unwrap();
    for ((original, id), bytes) in deferred.originals.iter().zip(tail_ids).zip(tail_bytes) {
        assert_eq!(original.operation, id);
        assert_eq!(original.bytes, bytes);
    }
    drop(actor);
    for wait in first_waits.iter_mut().chain(&mut tail_waits) {
        assert_eq!(wait.try_recv(), Err(oneshot::error::TryRecvError::Closed));
    }
    // Collected mixed bytes remain independently owned even after Actor teardown.
    assert_eq!(
        proposals
            .iter()
            .map(|p| p.bytes.clone())
            .collect::<Vec<_>>(),
        expected
    );
}

#[test]
fn queued_boundary_empty_originals_commit_in_order_after_deferral() {
    let (mut actor, sender) = ready_actor::<false>(crate::Envelopes::Common64);
    let width = actor.parts.core.batch_limits().max_proposal_entries;
    let ids: Vec<_> = (1..width)
        .map(|n| OperationId::new(u128::MAX ^ n as u128).unwrap())
        .collect();
    let tail_ids = [
        OperationId::new((1 << 127) | 17).unwrap(),
        OperationId::new(u128::MAX).unwrap(),
    ];
    let (first, mut first_waits) = request(&ids, false);
    let (tail, mut tail_waits) = request(&tail_ids, false);
    assert!(sender.try_send(tail).is_ok());
    let proposals = actor.collect(first).unwrap();
    assert_eq!(proposals.len(), width - 1);
    assert!(proposals.iter().all(|p| p.bytes.is_empty()));
    let effects = actor.parts.core.propose_batch(proposals).unwrap();
    actor.effects(effects).unwrap();
    for (id, wait) in ids.iter().zip(&mut first_waits) {
        assert_eq!(wait.try_recv().unwrap(), *id);
    }
    for wait in &mut tail_waits {
        assert_eq!(wait.try_recv(), Err(oneshot::error::TryRecvError::Empty));
    }
    let deferred = actor.deferred.take().unwrap();
    assert!(deferred.originals.iter().all(|o| o.bytes.is_empty()));
    actor.requests(deferred).unwrap();
    for (id, wait) in tail_ids.iter().zip(&mut tail_waits) {
        assert_eq!(wait.try_recv().unwrap(), *id);
    }
    let actual: Vec<_> = actor
        .parts
        .core
        .state()
        .entries
        .iter()
        .filter_map(|e| match &e.payload {
            EntryPayload::Command { operation, bytes } => {
                assert!(bytes.is_empty());
                Some(*operation)
            }
            _ => None,
        })
        .collect();
    assert_eq!(actual, ids.into_iter().chain(tail_ids).collect::<Vec<_>>());
    assert!(actor.pending.is_empty());
}

#[test]
fn empty_and_oversized_shapes_precede_full_pending_budget_without_admission() {
    let (mut actor, _sender) = ready_actor_clients::<false>(crate::Envelopes::Common64, 1);
    let ids: Vec<_> = (1..=5)
        .map(|n| OperationId::new((n << 96) | 1).unwrap())
        .collect();
    let (full, mut waits) = request(&ids, true);
    let mut proposals = Vec::new();
    actor.include(full, &mut proposals).unwrap();
    let before = proposals.clone();
    assert_eq!(
        actor
            .include(
                Request {
                    originals: Vec::new()
                },
                &mut proposals
            )
            .unwrap_err()
            .to_string(),
        "original proposal/reply shape differs"
    );
    let width = actor.parts.core.batch_limits().max_proposal_entries;
    let (oversized, mut rejected) = request(&vec![ids[0]; width + 1], true);
    assert_eq!(
        actor
            .include(oversized, &mut proposals)
            .unwrap_err()
            .to_string(),
        "original proposal/reply shape differs"
    );
    assert_eq!(actor.pending.len(), 5);
    assert_eq!(proposals, before);
    for wait in &mut rejected {
        assert_eq!(wait.try_recv(), Err(oneshot::error::TryRecvError::Closed));
    }
    for wait in &mut waits {
        assert_eq!(wait.try_recv(), Err(oneshot::error::TryRecvError::Empty));
    }
    actor.deliver(ids.clone()).unwrap();
    for (id, wait) in ids.iter().zip(&mut waits) {
        assert_eq!(wait.try_recv().unwrap(), *id);
    }
}
