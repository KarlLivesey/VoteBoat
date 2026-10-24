// SPDX-License-Identifier: RPL-1.5
// Copyright (c) 2026 Karl Livesey
use super::*;

fn variants() -> Vec<Effect> {
    let core = crate::raft::membership_tests::core();
    let state = core.state();
    let context = RequestContext {
        origin: core.binding,
        sequence: 7,
    };
    let message = core.message(NodeId::new(2).unwrap(), context, Rpc::ReadProbe);
    let reference = SnapshotRef {
        store: core.binding.identity,
        group: state.bootstrap.group,
        generation: SnapshotGeneration::new(3).unwrap(),
        configuration: core.membership().id(),
        index: 1,
        term: 1,
        application_schema: 3,
        file_bytes: 99,
        checksum: 17,
    };
    let token = VolatilePersist {
        incarnation: 77,
        revision: state.revision,
        generation: state.generation,
    };
    vec![
        Effect::PersistVolatile(token),
        Effect::Send(message.clone()),
        Effect::Persist(LogUpdate {
            group: state.bootstrap.group,
            expected_revision: state.revision,
            hard_state: state.hard_state,
            commit_index: state.commit_index,
            suffix: None,
            snapshot: None,
            snapshot_membership: None,
        }),
        Effect::Committed(vec![LogEntry {
            index: 2,
            term: 1,
            payload: EntryPayload::Command {
                operation: OperationId::new((1u128 << 100) + 31).unwrap(),
                bytes: vec![3, 1, 4, 1, 5].into(),
            },
        }]),
        Effect::ReadReady(ReadBarrier {
            group: state.bootstrap.group,
            configuration: core.membership().id(),
            term: 1,
            context,
            request: ReadRequestId::new(91).unwrap(),
            index: 2,
        }),
        Effect::SnapshotRequired {
            to: message.to,
            context,
            reference,
        },
        Effect::StageSnapshot(message.clone()),
        Effect::SnapshotInstalled(reference),
        Effect::CheckpointRequired { context },
        Effect::CheckpointCompacted(reference),
        Effect::VerifyLearnerReadiness(message),
    ]
}

// Test-only observation: the expectation is independently constructed above,
// never exported from a new queue and re-imported as its own oracle.
fn observe(front: EffectFront<'_>) -> Effect {
    match front {
        EffectFront::PersistVolatile(front) => Effect::PersistVolatile(front.take()),
        EffectFront::Send(front) => Effect::Send(front.take()),
        EffectFront::Persist(front) => Effect::Persist(front.take()),
        EffectFront::Committed(front) => Effect::Committed(front.take()),
        EffectFront::ReadReady(front) => Effect::ReadReady(front.take()),
        EffectFront::SnapshotRequired(front) => {
            let SnapshotRequest {
                to,
                context,
                reference,
            } = front.take();
            Effect::SnapshotRequired {
                to,
                context,
                reference,
            }
        }
        EffectFront::StageSnapshot(front) => Effect::StageSnapshot(front.take()),
        EffectFront::SnapshotInstalled(front) => Effect::SnapshotInstalled(front.take()),
        EffectFront::CheckpointRequired(front) => Effect::CheckpointRequired {
            context: front.take(),
        },
        EffectFront::CheckpointCompacted(front) => Effect::CheckpointCompacted(front.take()),
        EffectFront::VerifyLearnerReadiness(front) => Effect::VerifyLearnerReadiness(front.take()),
    }
}

#[test]
fn mixed_fronts_keep_full_values_fifo_and_capacity_through_wrap_growth_and_drop() {
    let expected = variants();
    let mut queue = EffectQueue::from(expected.clone());
    let initial_capacity = queue.capacity();
    for effect in expected.iter().take(7) {
        assert_eq!(&observe(queue.front().unwrap()), effect);
    }
    // Reuse the same ring with a nonzero head, then force real growth.
    queue.append(EffectQueue::from(expected.clone()));
    let oracle: Vec<_> = expected[7..]
        .iter()
        .chain(expected.iter())
        .cloned()
        .collect();
    assert!(queue.capacity() >= initial_capacity);
    let capacity = queue.capacity();
    for effect in oracle {
        let length = queue.len();
        {
            let _front = queue.front();
        }
        let _forgotten = std::mem::ManuallyDrop::new(queue.front());
        assert_eq!(queue.len(), length);
        assert_eq!(queue.capacity(), capacity);
        assert_eq!(observe(queue.front().unwrap()), effect);
    }
    assert!(queue.front().is_none());
    assert_eq!(queue.capacity(), capacity);
    queue.append(EffectQueue::from(expected));
    queue.truncate(1);
    let Some(EffectFront::PersistVolatile(front)) = queue.front() else {
        panic!("token stays first")
    };
    assert_eq!(front.take().incarnation, 77);
    assert!(queue.is_empty());
    assert_eq!(queue.capacity(), capacity);
}

#[test]
fn exact_one_adoption_and_queue_headers_do_not_add_storage_or_credits() {
    use std::mem::size_of;
    let source = vec![variants().remove(0)];
    let capacity = source.capacity();
    let mut queue = EffectQueue::from(source);
    assert_eq!(capacity, 1);
    assert_eq!(queue.capacity(), capacity);
    let Some(EffectFront::PersistVolatile(front)) = queue.front() else {
        panic!("volatile front")
    };
    assert_eq!(front.take().incarnation, 77);
    assert_eq!(queue.capacity(), capacity);
    // The downstream doctest checks the normal, transparent header. Unit tests
    // additionally carry a heap-free RAII witness after the real slot owner.
    assert_eq!(
        size_of::<EffectQueue>(),
        size_of::<(
            VecDeque<Effect>,
            crate::snapshot::input_lifetime_tests::StorageWitness,
        )>()
    );
    assert_eq!(size_of::<VolatilePersist>(), 24);
    let largest_payload = [
        size_of::<Message>(),
        size_of::<LogUpdate>(),
        size_of::<SnapshotRequest>(),
        size_of::<Vec<LogEntry>>(),
        size_of::<ReadBarrier>(),
        size_of::<SnapshotRef>(),
        size_of::<RequestContext>(),
    ]
    .into_iter()
    .max()
    .unwrap();
    assert!(largest_payload + size_of::<&mut EffectQueue>() <= size_of::<Effect>());
    println!(
        "largest_payload={largest_payload} owned_vec_header={} old_result={} queue_result={}",
        size_of::<Vec<Effect>>(),
        size_of::<Result<Vec<Effect>, RaftError>>(),
        size_of::<Result<EffectQueue, RaftError>>()
    );
    assert_eq!(
        size_of::<VolatileFront<'_>>(),
        size_of::<&mut EffectQueue>()
    );
    println!(
        "queue={} slot={} front={} volatile_front={} drain={}",
        size_of::<EffectQueue>(),
        size_of::<Effect>(),
        size_of::<EffectFront<'_>>(),
        size_of::<VolatileFront<'_>>(),
        size_of::<std::collections::vec_deque::Drain<'_, Effect>>()
    );
}

#[test]
fn concatenation_matches_the_old_owned_deque_capacity_and_fifo_oracle() {
    let values = variants();
    for initial in [0, 1, 2, 4, 5, 11, 16] {
        for length in 0..=initial {
            for offset in [0, 3, 7] {
                for count in [0, 1, 2, 3, 7, 11, 100] {
                    let mut old = VecDeque::with_capacity(initial);
                    let mut slots = VecDeque::with_capacity(initial);
                    for _ in 0..offset {
                        old.push_back(values[0].clone());
                        slots.push_back(values[0].clone());
                        old.pop_front();
                        slots.pop_front();
                    }
                    for i in 0..length {
                        old.push_back(values[i % values.len()].clone());
                        slots.push_back(values[i % values.len()].clone());
                    }
                    let mut source_a = Vec::with_capacity(count + 3);
                    let mut source_b = Vec::with_capacity(count + 3);
                    for i in 0..count {
                        source_a.push(values[i % values.len()].clone());
                        source_b.push(values[i % values.len()].clone());
                    }
                    let mut queue = EffectQueue::from(slots);
                    assert_eq!(queue.capacity(), old.capacity());
                    old.extend(source_a);
                    queue.append(EffectQueue::from(source_b));
                    assert_eq!(
                        queue.capacity(),
                        old.capacity(),
                        "initial={initial} length={length} offset={offset} count={count}"
                    );
                    assert!(queue.iter().eq(old.iter()));
                }
            }
        }
    }
}

#[test]
fn durable_bridge_refuses_a_volatile_front_without_consuming_it() {
    let expected = variants();
    let mut queue = EffectQueue::from(expected.clone());
    assert!(queue.take_durable().is_none());
    assert_eq!(queue.len(), expected.len());
    assert_eq!(observe(queue.front().unwrap()), expected[0]);
    for effect in &expected[1..] {
        assert_eq!(queue.take_durable().as_ref(), Some(effect));
    }
    assert!(queue.take_durable().is_none());
}
