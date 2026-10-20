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

fn commands(actor: &Actor<false>) -> Vec<(OperationId, Vec<u8>)> {
    actor
        .parts
        .core
        .state()
        .entries
        .iter()
        .filter_map(|e| match &e.payload {
            EntryPayload::Command { operation, bytes } => Some((*operation, bytes.to_vec())),
            _ => None,
        })
        .collect()
}

#[test]
fn queued_boundary_stages_mixed_originals_without_receipts_and_drop_cancels_them() {
    let (mut actor, sender) = ready_actor::<false>(crate::Envelopes::Common64);
    let width = actor.parts.core.batch_limits().max_proposal_entries;
    let ids: Vec<_> = (1..width)
        .map(|n| OperationId::new(u128::MAX ^ n as u128).unwrap())
        .collect();
    let (first, mut first_waits) = request(&ids, true);
    let expected: Vec<_> = first.originals.iter().map(|o| o.bytes.clone()).collect();
    let tail_ids = [
        OperationId::new((1 << 127) | 17).unwrap(),
        OperationId::new(u128::MAX).unwrap(),
    ];
    let (tail, mut tail_waits) = request(&tail_ids, true);
    let tail_bytes: Vec<_> = tail.originals.iter().map(|o| o.bytes.clone()).collect();
    assert!(sender.try_send(tail).is_ok());
    let batch = actor.collect(first).unwrap().into_batch();
    assert_eq!(batch.len(), width - 1);
    assert_eq!(actor.pending.len(), width - 1);
    assert_eq!(actor.deferred.as_ref().unwrap().originals.len(), 2);
    for wait in first_waits.iter_mut().chain(&mut tail_waits) {
        assert_eq!(wait.try_recv(), Err(oneshot::error::TryRecvError::Empty));
    }
    let deferred = actor.deferred.as_ref().unwrap();
    for ((original, id), bytes) in deferred.originals.iter().zip(&tail_ids).zip(&tail_bytes) {
        assert_eq!(original.operation, *id);
        assert_eq!(&original.bytes, bytes);
    }
    drop(actor);
    for wait in first_waits.iter_mut().chain(&mut tail_waits) {
        assert_eq!(wait.try_recv(), Err(oneshot::error::TryRecvError::Closed));
    }
    // The staged batch is independently owned, outlives Actor teardown and
    // holds every original exactly, in order.
    let staged: Vec<_> = batch.iter().map(|(id, b)| (id, b.to_vec())).collect();
    assert_eq!(
        staged,
        ids.iter().copied().zip(expected).collect::<Vec<_>>()
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
    let batch = actor.collect(first).unwrap().into_batch();
    assert_eq!(batch.len(), width - 1);
    let effects = actor.parts.core.propose_entries(batch).unwrap();
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
    let mut batch = actor.parts.core.proposal_batch(0).unwrap();
    actor.include(full, &mut batch).unwrap();
    let before = (batch.len(), batch.retained_bytes());
    assert_eq!(
        actor
            .include(
                Request {
                    originals: Vec::new()
                },
                &mut batch
            )
            .unwrap_err()
            .to_string(),
        "original proposal/reply shape differs"
    );
    let width = actor.parts.core.batch_limits().max_proposal_entries;
    let (oversized, mut rejected) = request(&vec![ids[0]; width + 1], true);
    assert_eq!(
        actor
            .include(oversized, &mut batch)
            .unwrap_err()
            .to_string(),
        "original proposal/reply shape differs"
    );
    assert_eq!(actor.pending.len(), 5);
    assert_eq!((batch.len(), batch.retained_bytes()), before);
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

#[test]
fn width_one_passes_use_the_propose_event_and_staging_matches_it() {
    // The no-op application accepts only empty commands; nonempty bytes are
    // compared up to the persistence request.
    for bytes in [Vec::new(), vec![3, 1, 4]] {
        let operation = OperationId::new(41).unwrap();
        let (mut staged, _sender) = ready_actor::<false>(crate::Envelopes::Common64);
        let (mut old, _old_sender) = ready_actor::<false>(crate::Envelopes::Common64);
        let (mut event, _event_sender) = ready_actor::<false>(crate::Envelopes::Common64);
        // A pass of one original, with nothing else queued, keeps the
        // original for the core's single-proposal event.
        let (reply, mut wait) = oneshot::channel();
        let collected = event
            .collect(Request {
                originals: vec![Original {
                    operation,
                    bytes: bytes.clone(),
                    reply,
                }],
            })
            .unwrap();
        let Collected::One(lone, lone_bytes) = collected else {
            panic!("a lone original is not staged")
        };
        assert_eq!((lone, &lone_bytes), (operation, &bytes));
        assert_eq!(event.pending.len(), 1);
        // The builder's width-1 path still matches the event exactly.
        let mut batch = staged.parts.core.proposal_batch(1).unwrap();
        batch.push(operation, bytes.clone()).unwrap();
        let effects = staged.parts.core.propose_entries(batch).unwrap();
        let expected = old
            .parts
            .core
            .step(Event::Propose {
                operation,
                bytes: bytes.clone(),
            })
            .unwrap();
        // Tokens differ only in their core's process-unique incarnation.
        let ([Effect::PersistVolatile(a)], [Effect::PersistVolatile(b)]) =
            (&effects[..], &expected[..])
        else {
            panic!("one persistence request each: {effects:?} {expected:?}");
        };
        assert_eq!(
            (a.revision(), a.generation()),
            (b.revision(), b.generation())
        );
        if !bytes.is_empty() {
            continue;
        }
        let event_effects = event
            .parts
            .core
            .step(Event::Propose {
                operation: lone,
                bytes: lone_bytes,
            })
            .unwrap();
        let (staged_reply, mut staged_wait) = oneshot::channel();
        assert!(staged.pending.insert(operation, staged_reply).is_none());
        let (old_reply, mut old_wait) = oneshot::channel();
        assert!(old.pending.insert(operation, old_reply).is_none());
        staged.effects(effects).unwrap();
        old.effects(expected).unwrap();
        event.effects(event_effects).unwrap();
        assert_eq!(wait.try_recv().unwrap(), operation);
        assert_eq!(staged_wait.try_recv().unwrap(), operation);
        assert_eq!(old_wait.try_recv().unwrap(), operation);
        assert_eq!(staged.parts.core.state(), old.parts.core.state());
        assert_eq!(event.parts.core.state(), old.parts.core.state());
        assert_eq!(commands(&event), vec![(operation, bytes)]);
    }
}

#[test]
fn a_lone_original_with_queued_work_is_staged_with_it() {
    let (mut actor, sender) = ready_actor::<false>(crate::Envelopes::Common64);
    let (first, _first_waits) = request(&[OperationId::new(7).unwrap()], false);
    let (next, _next_waits) = request(&[OperationId::new(8).unwrap()], false);
    assert!(sender.try_send(next).is_ok());
    let batch = actor.collect(first).unwrap().into_batch();
    assert_eq!(batch.len(), 2);
    assert_eq!(actor.pending.len(), 2);
}
