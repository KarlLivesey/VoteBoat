// SPDX-License-Identifier: RPL-1.5
// Copyright (c) 2026 Karl Livesey
use super::*;
use crate::{
    contracts::HardState,
    log::*,
    quorum::{Limits, Policy, Tree},
};
#[path = "fixture.rs"]
mod fixture;
use fixture::*;
#[cfg(feature = "native")]
#[path = "owner.rs"]
mod owner;

#[test]
fn two_replies_and_original_prefix_follow_real_cursor_and_keep_full_ids() {
    let (mut shard, replies) = prepared(ShardLimits::default(), BatchLimits::default());
    for reply in replies {
        shard.admit(group(), Event::Receive(reply)).unwrap();
    }
    let ids = [u128::MAX, (1u128 << 96) + 7, 7];
    let tickets: Vec<_> = ids
        .into_iter()
        .map(|id| shard.admit_tracked(group(), command(id)).unwrap())
        .collect();
    let visit = shard.poll(MonoTime(0)).unwrap().unwrap();
    let before = shard.core(group()).unwrap().state().clone();
    let mut seen = Vec::new();
    let joined = shard
        .step_joined_checked(visit, MonoTime(0), 4, |_, event, prefix| {
            let Event::Propose { operation, .. } = event else {
                panic!()
            };
            seen.push((operation.get(), prefix.len()));
            Ok(())
        })
        .unwrap()
        .unwrap();
    assert_eq!(seen, vec![(ids[0], 0), (ids[1], 1)]);
    assert_eq!(
        joined
            .controls
            .iter()
            .map(|c| c.received_from)
            .collect::<Vec<_>>(),
        vec![node(2), node(3)]
    );
    assert_eq!(joined.proposals.admission, Some(tickets[0]));
    assert_eq!(joined.proposals.operation.unwrap().get(), ids[0]);
    assert_eq!(joined.proposals.proposed.unwrap().index, 2);
    assert_eq!(
        joined.proposals.batch_members[0].admission,
        Some(tickets[1])
    );
    assert_eq!(joined.proposals.batch_members[0].operation.get(), ids[1]);
    assert_eq!(joined.proposals.batch_members[0].proposed.unwrap().index, 3);
    let g = &shard.groups[&group()];
    assert_eq!(g.turn, 3);
    assert_eq!(g.visit.as_ref().unwrap().used[0].items, 2);
    assert_eq!(g.visit.as_ref().unwrap().used[1].items, 2);
    assert_eq!(g.queues[1].len(), 1);
    assert_eq!(g.core.state(), &before);
    let Effect::Persist(update) = &joined.proposals.result.as_ref().unwrap()[0] else {
        panic!()
    };
    assert_eq!(update.commit_index, before.last_index());
    assert_eq!(update.suffix.as_ref().unwrap().entries.len(), 2);
}

#[test]
fn rejected_second_original_remains_queued_and_first_failure_preserves_all_inputs() {
    for rejected in [1, 2] {
        let (mut shard, replies) = prepared(ShardLimits::default(), BatchLimits::default());
        for reply in replies {
            shard.admit(group(), Event::Receive(reply)).unwrap();
        }
        for id in 1..=3 {
            shard.admit_tracked(group(), command(id)).unwrap();
        }
        let visit = shard.poll(MonoTime(0)).unwrap().unwrap();
        let before = shard.core(group()).unwrap().state().clone();
        let result = shard
            .step_joined_checked(visit, MonoTime(0), 5, |_, event, _| {
                if matches!(event, Event::Propose { operation, .. } if operation.get() == rejected)
                {
                    Err(RaftError::Busy)
                } else {
                    Ok(())
                }
            })
            .unwrap();
        let g = &shard.groups[&group()];
        if rejected == 1 {
            assert!(result.is_none());
            assert_eq!(g.queues[0].len(), 2);
            assert_eq!(g.queues[1].len(), 3);
            assert_eq!(g.turn, 0);
            assert_eq!(sum(&g.visit.as_ref().unwrap().used).items, 0);
            assert!(!g.core.has_pending_dependency());
        } else {
            let joined = result.unwrap();
            assert_eq!(joined.proposals.operation.unwrap().get(), 1);
            assert!(joined.proposals.batch_members.is_empty());
            assert_eq!(g.queues[1].len(), 2);
            assert!(
                matches!(&g.queues[1][0].event, Event::Propose { operation, .. } if operation.get() == 2)
            );
        }
        assert_eq!(g.core.state(), &before);
    }
}

#[test]
fn selector_respects_reply_cap_protected_tail_background_timers_and_output_limit() {
    for boundary in 0..7 {
        let batch = BatchLimits {
            max_ready_reply_inputs: if boundary == 0 { 1 } else { 2 },
            ..BatchLimits::default()
        };
        let (mut shard, replies) = prepared(ShardLimits::default(), batch);
        for reply in replies {
            shard.admit(group(), Event::Receive(reply)).unwrap();
        }
        shard.admit_tracked(group(), command(1)).unwrap();
        match boundary {
            1 => {
                shard.admit(group(), Event::Campaign).unwrap();
            }
            2 => {
                shard.admit(group(), Event::Checkpoint).unwrap();
            }
            3 => {
                let timer = token(&shard);
                shard.groups.get_mut(&group()).unwrap().queues[0][0].timer = Some(timer);
            }
            4 => {
                shard.groups.get_mut(&group()).unwrap().turn = 2;
            }
            _ => (),
        }
        let visit = shard.poll(MonoTime(0)).unwrap().unwrap();
        let now = if boundary == 5 {
            shard.groups[&group()].visit.as_ref().unwrap().deadline
        } else {
            MonoTime(0)
        };
        let limit = if boundary == 6 { 2 } else { 4 };
        assert!(shard
            .joined_reservation(visit, now, limit)
            .unwrap()
            .is_none());
        assert!(shard
            .step_joined_checked(visit, now, limit, |_, _, _| panic!(
                "boundary must precede application checks"
            ))
            .unwrap()
            .is_none());
        assert_eq!(
            shard.groups[&group()].queues[0].len(),
            if boundary == 1 { 3 } else { 2 }
        );
        assert_eq!(shard.groups[&group()].queues[1].len(), 1);
    }
}

#[test]
fn cursor_skips_only_nonfitting_controls_and_does_not_fish_past_fitting_reply() {
    let (mut shard, replies) = prepared(ShardLimits::default(), BatchLimits::default());
    for reply in replies {
        shard.admit(group(), Event::Receive(reply)).unwrap();
    }
    for id in 1..=4 {
        shard.admit_tracked(group(), command(id)).unwrap();
    }
    // A third routine reply is still a control boundary after the first Data
    // turn; it cannot be pulled before Data or skipped to enlarge its batch.
    let extra = shard.groups[&group()].queues[0][0].event.clone();
    shard.admit(group(), extra).unwrap();
    let visit = shard.poll(MonoTime(0)).unwrap().unwrap();
    let g = shard.groups.get_mut(&group()).unwrap();
    let selected = selection(g, ShardLimits::default(), MonoTime(0), 8).unwrap();
    assert_eq!((selected.replies, selected.proposals), (2, 1));
    let mut cursor = Cursor {
        turn: 0,
        offsets: [0; 3],
        used: Usage::default(),
    };
    let mut order = Vec::new();
    for _ in 0..4 {
        let (turn, class) = cursor.next(g, ShardLimits::default()).unwrap();
        order.push(class);
        cursor.consume(g, turn, class);
    }
    assert_eq!(order, [0, 0, 1, 0]);
    let original_cost = g.queues[0][0].cost;
    let bytes = original_cost - 1;
    let small = ShardLimits {
        visit_bytes: bytes,
        ..ShardLimits::default()
    };
    // The real scheduler and scalar selector agree that an ineligible control
    // does not consume its turn; Data may be the ordinary head instead.
    g.queues[1][0].cost = 1;
    assert_eq!(g.next_queued(small, MonoTime(0)), Some((2, 1)));
    assert!(selection(g, small, MonoTime(0), 8).is_none());
    assert_eq!(g.visit.as_ref().unwrap().ticket, visit);
}
