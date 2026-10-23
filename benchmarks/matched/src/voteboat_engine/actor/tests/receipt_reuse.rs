// SPDX-License-Identifier: RPL-1.5
// Copyright (c) 2026 Karl Livesey
use super::*;

fn entries(start: u64, width: usize) -> Vec<LogEntry> {
    (0..width)
        .map(|offset| {
            let index = start + offset as u64;
            LogEntry {
                index,
                term: 1,
                payload: EntryPayload::Command {
                    operation: OperationId::new((1 << 127) + u128::from(index) * 7919).unwrap(),
                    bytes: Vec::new().into(),
                },
            }
        })
        .collect()
}
#[test]
fn actual_actor_public_seam_selection_reuses_borrowed_and_fifo_owned_output() {
    for width in [1, 231, 515] {
        for queued in [false, true] {
            let (mut actor, _sender) = ready_actor::<false>(crate::Envelopes::UpstreamDefault);
            actor.receipts = Vec::with_capacity(width * 2);
            let pointer = actor.receipts.as_ptr();
            let capacity = actor.receipts.capacity();
            let ids: Vec<_> = entries(2, width)
                .into_iter()
                .map(|entry| {
                    let EntryPayload::Command { operation, .. } = entry.payload else {
                        unreachable!()
                    };
                    operation
                })
                .collect();
            let (update, waits) = completion::commit_update(&mut actor, &ids, &[]);
            let mut effects = vec![Effect::PersistVolatile(update)];
            if queued {
                effects.push(Effect::Committed(Vec::new()));
            }
            actor.effects(effects).unwrap();
            assert!(actor.receipts.is_empty());
            assert_eq!(
                actor.receipts.as_ptr(),
                pointer,
                "actual Actor must select caller-owned public seam"
            );
            assert_eq!(actor.receipts.capacity(), capacity);
            assert_eq!(actor.parts.application.applied, width as u64 + 1);
            assert!(actor.pending.is_empty());
            for (mut wait, id) in waits.into_iter().zip(ids) {
                assert_eq!(wait.try_recv().unwrap(), id);
            }
        }
    }
}
#[test]
fn actual_follower_produces_receipts_then_reuses_or_evicts_capacity() {
    for width in [1, 231, 515] {
        let (mut actor, _sender) = ready_actor::<false>(crate::Envelopes::UpstreamDefault);
        actor.parts.slot = 1;
        actor
            .effects(vec![Effect::Committed(entries(2, width))])
            .unwrap();
        assert!(actor.receipts.is_empty());
        assert!(
            actor.receipts.capacity() >= width,
            "follower must produce full receipt vector"
        );
        let pointer = actor.receipts.as_ptr();
        let capacity = actor.receipts.capacity();
        actor
            .effects(vec![Effect::Committed(entries(width as u64 + 2, width))])
            .unwrap();
        assert_eq!(actor.receipts.as_ptr(), pointer);
        assert_eq!(actor.receipts.capacity(), capacity);
        assert_eq!(actor.parts.application.applied, 1 + 2 * width as u64);
        println!(
            "follower width={width} retained_capacity={capacity} bytes={}",
            capacity * size_of::<OperationId>()
        );
    }
    let (mut small, _sender) = ready_actor_clients::<false>(crate::Envelopes::UpstreamDefault, 1);
    small.parts.slot = 1;
    small
        .effects(vec![Effect::Committed(entries(2, 515))])
        .unwrap();
    assert_eq!(small.parts.application.applied, 516);
    assert_eq!(
        small.receipts.capacity(),
        0,
        "legal output exceeds retained-only ceiling and must be consumed before eviction"
    );
    small
        .effects(vec![Effect::Committed(entries(517, 1))])
        .unwrap();
    assert_eq!(small.parts.application.applied, 517);
    assert!(small.receipts.capacity() <= Actor::<false>::receipt_capacity_limit(1));
    assert_eq!(
        Actor::<false>::receipt_capacity_limit(usize::MAX),
        isize::MAX as usize / size_of::<OperationId>()
    );
}
#[test]
fn late_noop_failure_clears_prefix_receipts_and_keeps_actual_state() {
    let (mut actor, _sender) = ready_actor::<false>(crate::Envelopes::UpstreamDefault);
    actor.parts.slot = 1;
    for gap in [false, true] {
        let mut interval = entries(2, 3);
        if gap {
            interval[2].index += 1;
        } else if let EntryPayload::Command { bytes, .. } = &mut interval[2].payload {
            *bytes = vec![255].into();
        }
        assert!(actor
            .effects(vec![Effect::Committed(interval.clone())])
            .is_err());
        assert_eq!(actor.parts.application.applied, 1);
        assert!(actor.receipts.is_empty());
        // Direct public call catches a provider exposing partial output even
        // when the fatal actor's cleanup would subsequently discard it.
        let error = actor
            .parts
            .application
            .apply_batch_into(&interval, &mut actor.receipts);
        assert_eq!(
            error,
            Err(if gap {
                ApplicationError::IndexGap
            } else {
                ApplicationError::InvalidCommand
            })
        );
        assert!(
            actor.receipts.is_empty(),
            "public error cannot expose partial output"
        );
    }
    actor
        .effects(vec![Effect::Committed(entries(2, 3))])
        .unwrap();
    assert_eq!(actor.parts.application.applied, 4);
}
#[test]
fn independent_application_buffers_survive_other_actor_drop() {
    let (mut first, _sender1) = ready_actor::<false>(crate::Envelopes::UpstreamDefault);
    let (mut second, _sender2) =
        ready_actor_group::<false>(crate::Envelopes::UpstreamDefault, 4096, 2);
    assert_ne!(
        first.parts.application.group,
        second.parts.application.group
    );
    first.parts.slot = 1;
    second.parts.slot = 2;
    first
        .effects(vec![Effect::Committed(entries(2, 231))])
        .unwrap();
    second
        .effects(vec![Effect::Committed(entries(2, 515))])
        .unwrap();
    let pointer = second.receipts.as_ptr();
    let capacity = second.receipts.capacity();
    assert_ne!(first.receipts.as_ptr(), pointer);
    drop(first);
    second
        .effects(vec![Effect::Committed(entries(517, 515))])
        .unwrap();
    assert_eq!(second.receipts.as_ptr(), pointer);
    assert_eq!(second.receipts.capacity(), capacity);
    assert_eq!(second.parts.application.applied, 1031);
}
