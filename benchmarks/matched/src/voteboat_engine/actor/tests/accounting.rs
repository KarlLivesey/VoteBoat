// SPDX-License-Identifier: RPL-1.5
// Copyright (c) 2026 Karl Livesey
//! Untimed enqueue-failure closure, not a complete consensus qualification.
use super::super::r3_ports::{registration, Pool};
use super::*;
use crate::voteboat_engine::writes::Reply;

type Waits = VecDeque<(u128, oneshot::Receiver<OperationId>)>;

// Source-anchored old A: cpu1-driver-r3/driver-addition.rs:32-43,
// SHA256 c2b3bf066abcc02419b0d2c485ffddbacad4e4c612eadcd527b7b2c3b2b172f0.
// Current Owned/Reply::Owned spellings represent the same actual Originals,
// oneshots and global FIFO. This is the enqueue segment, not a second engine.
fn old_a_enqueue(
    sender: &mpsc::Sender<Request>,
    waits: &mut Waits,
    next: &mut u128,
    count: usize,
) -> Result<(), Failure> {
    let mut originals = Vec::with_capacity(count);
    for _ in 0..count {
        let (reply, wait) = oneshot::channel();
        let operation = OperationId::new(*next).unwrap();
        originals.push(Original {
            operation,
            bytes: Vec::new(),
            reply: Reply::Owned(reply),
        });
        waits.push_back((operation.get(), wait));
        *next += 1;
    }
    sender
        .try_send(Request::Owned { originals })
        .map_err(|_| "r3 client queue")?;
    Ok(())
}

#[test]
fn a1_failed_send_counts_every_owned_original_for_full_and_closed_queues() {
    for count in [1usize, 4, 3] {
        for closed in [false, true] {
            for first in [1, (1u128 << 120) | 17] {
                let (mut actor, _, _) =
                    ready_actor_with_factory::<false>(crate::Envelopes::UpstreamDefault, 2);
                let (domain, factory, maximum) =
                    registration(actor.parts.domain.binding(), 9, count, count).unwrap();
                actor.parts.domain = domain;
                let (sender, mut receiver) = mpsc::channel(1);
                let (old_sender, mut old_receiver) = mpsc::channel(1);
                let mut pool = Pool::new(&factory, &sender, count, count, maximum).unwrap();
                let (mut next, mut old_next) = (first, first);
                let mut waits = Waits::new();
                pool.enqueue(&mut next, 1).unwrap();
                old_a_enqueue(&old_sender, &mut waits, &mut old_next, 1).unwrap();
                assert_eq!(sender.capacity(), 0, "genuinely bounded FULL queue");
                assert_eq!(old_sender.capacity(), 0);
                if closed {
                    receiver.close();
                    old_receiver.close();
                }
                assert_eq!(sender.is_closed(), closed);
                let error = pool.enqueue(&mut next, count).unwrap_err();
                let old_error =
                    old_a_enqueue(&old_sender, &mut waits, &mut old_next, count).unwrap_err();
                assert_eq!(error.to_string(), "r3 client queue");
                assert_eq!(error.to_string(), old_error.to_string());
                assert_eq!(
                    next, old_next,
                    "failed-send generated-count must include every owned original"
                );
                assert_eq!(next - first, (count + 1) as u128);
                assert_eq!(pool.unreplied(), waits.len());
                assert_eq!(pool.unreplied(), count + 1);
                let expected: Vec<_> = (0..count).map(|n| first + 1 + n as u128).collect();
                assert_eq!(pool.rejected_originals(), expected);
                assert_eq!(
                    waits.iter().skip(1).map(|(id, _)| *id).collect::<Vec<_>>(),
                    expected
                );
                for (_, wait) in waits.iter_mut().skip(1) {
                    assert!(matches!(
                        wait.try_recv(),
                        Err(oneshot::error::TryRecvError::Closed)
                    ));
                }
                println!("a1 closed={closed} count={count} first={first} next={next} remaining={} failed_ids={expected:?}", pool.unreplied());
            }
        }
    }
}

#[test]
fn a1_real_prepare_and_begin_failures_do_not_create_fifo_owners_or_advance_count() {
    let (mut actor, _, _) = ready_actor_with_factory::<false>(crate::Envelopes::UpstreamDefault, 2);
    let (domain, factory, maximum) = registration(actor.parts.domain.binding(), 9, 8, 4).unwrap();
    actor.parts.domain = domain;
    let (sender, mut receiver) = mpsc::channel(2);
    let mut pool = Pool::new(&factory, &sender, 8, 4, maximum).unwrap();
    let mut next = 0;
    assert_eq!(
        pool.enqueue(&mut next, 3).unwrap_err().to_string(),
        "zero original identity"
    );
    assert_eq!(next, 0);
    assert_eq!(pool.unreplied(), 0);
    assert!(receiver.try_recv().is_err());
    next = (1u128 << 120) | 17;
    pool.enqueue(&mut next, 3).unwrap();
    assert_eq!(
        pool.live_ports(),
        2,
        "failed prepare did not claim a cursor/slot"
    );
    let before = next;
    // The second already-open slot is idle: this reaches real Client::prepare
    // and public port.begin after close, rather than failing cold registration.
    actor.parts.domain.close(WriteUnknown::OwnerFailed);
    assert!(pool
        .enqueue(&mut next, 3)
        .unwrap_err()
        .to_string()
        .contains("Closed"));
    assert_eq!(
        next, before,
        "actual port.begin failure precedes cursor ownership"
    );
    assert_eq!(
        pool.unreplied(),
        3,
        "no new FIFO original owners on begin failure"
    );
}

struct FailureTrace {
    next: u128,
    replied: usize,
    wrong_reply: bool,
    remaining: usize,
    error: Option<String>,
}
// Same ten final driver predicates, evaluated on real Actor fields and actual
// enqueue observations in a bounded single-round/no-warmup fault control.
fn failed_conjuncts(actors: &[Actor], trace: &FailureTrace, width: usize) -> Vec<&'static str> {
    let expected = width as u64 + 1;
    let commit: Vec<_> = actors
        .iter()
        .map(|a| a.parts.core.state().commit_index)
        .collect();
    let applied: Vec<_> = actors.iter().map(|a| a.parts.application.applied).collect();
    let reported: Vec<_> = actors
        .iter()
        .map(|a| a.parts.frontiers[a.parts.slot].load(Ordering::Acquire))
        .collect();
    [
        ("replicas", actors.len() == 3),
        ("expected_count", trace.next - 1 == width as u128),
        ("reply_count", trace.replied == width),
        ("reply_ids", !trace.wrong_reply),
        ("unreplied", trace.remaining == 0),
        ("commit_frontiers", commit == vec![expected; 3]),
        ("applied_frontiers", applied == vec![expected; 3]),
        ("reported_frontiers", reported == vec![expected; 3]),
        (
            "nonfenced",
            actors.iter().all(|a| !a.parts.core.is_fenced()),
        ),
        ("propagated_error", trace.error.is_none()),
    ]
    .into_iter()
    .filter_map(|(name, ok)| (!ok).then_some(name))
    .collect()
}

fn observed_old_a(waits: &mut Waits, next: u128, error: Failure) -> FailureTrace {
    let mut replied = 0;
    let mut wrong_reply = false;
    while let Some((id, wait)) = waits.front_mut() {
        match wait.try_recv() {
            Ok(operation) if operation.get() == *id => {
                waits.pop_front();
                replied += 1;
            }
            Ok(_) => {
                wrong_reply = true;
                break;
            }
            Err(_) => break,
        }
    }
    FailureTrace {
        next,
        replied,
        wrong_reply,
        remaining: waits.len(),
        error: Some(error.to_string()),
    }
}

#[test]
fn a1_final_three_failure_preserves_old_a_count_while_both_reject_actual_guards() {
    for width in [231usize, 515] {
        for closed in [false, true] {
            let requests = width.div_ceil(4);
            let collection::Replicas { mut actors, .. } = collection::replicas_for(requests);
            actors[0].step(Event::Campaign).unwrap();
            super::super::r3_steps::r3_pump(&mut actors, true).unwrap();
            let (domain, factory, maximum) =
                registration(actors[0].parts.domain.binding(), requests * 4 + 1, width, 4).unwrap();
            actors[0].parts.domain = domain;
            let (sender, receiver) = mpsc::channel(requests - 1);
            actors[0].parts.receiver = receiver;
            let (old_sender, mut old_receiver) = mpsc::channel(requests - 1);
            let mut pool = Pool::new(&factory, &sender, width, 4, maximum).unwrap();
            let (mut next, mut old_next) = (1, 1);
            let mut waits = Waits::new();
            for _ in 0..requests - 1 {
                pool.enqueue(&mut next, 4).unwrap();
                old_a_enqueue(&old_sender, &mut waits, &mut old_next, 4).unwrap();
            }
            assert_eq!(sender.capacity(), 0);
            assert_eq!(old_sender.capacity(), 0);
            if closed {
                actors[0].parts.receiver.close();
                old_receiver.close();
            }
            let old_b_next = next; // Actual successful-enqueue prefix; old B excludes the rejected tail.
            let error = pool.enqueue(&mut next, 3).unwrap_err();
            let old_error = old_a_enqueue(&old_sender, &mut waits, &mut old_next, 3).unwrap_err();
            assert_eq!(
                next, old_next,
                "final3 generated-count must match original A"
            );
            let mut replied = 0;
            while pool.try_take_front().unwrap().is_some() {
                replied += 1;
            }
            let trace = FailureTrace {
                next,
                replied,
                wrong_reply: false,
                remaining: pool.unreplied(),
                error: Some(error.to_string()),
            };
            let old_a = observed_old_a(&mut waits, old_next, old_error);
            let fixed_failed = failed_conjuncts(&actors, &trace, width);
            assert_eq!(fixed_failed, failed_conjuncts(&actors, &old_a, width));
            assert_eq!(
                fixed_failed,
                [
                    "reply_count",
                    "unreplied",
                    "commit_frontiers",
                    "applied_frontiers",
                    "reported_frontiers",
                    "propagated_error"
                ]
            );
            let old_b = FailureTrace {
                next: old_b_next,
                ..trace
            };
            let old_b_failed = failed_conjuncts(&actors, &old_b, width);
            assert!(old_b_failed.contains(&"expected_count"));
            assert_eq!(old_b_failed.len(), fixed_failed.len() + 1);
            assert_eq!(
                pool.rejected_originals(),
                vec![width as u128 - 2, width as u128 - 1, width as u128]
            );
            println!("a1 final3 width={width} closed={closed} generated={} old_b_generated={} remaining={} actual_commit={:?} actual_app={:?} fixed_and_a_failed={fixed_failed:?} old_b_failed={old_b_failed:?}", next - 1, old_b_next - 1, pool.unreplied(), actors.iter().map(|a| a.parts.core.state().commit_index).collect::<Vec<_>>(), actors.iter().map(|a| a.parts.application.applied).collect::<Vec<_>>());
        }
    }
}
