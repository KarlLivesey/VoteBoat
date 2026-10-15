// SPDX-License-Identifier: RPL-1.5
// Copyright (c) 2026 Karl Livesey
use super::*;
use crate::log::LogLimits;
mod fixture;
mod routine;
use fixture::*;

#[test]
fn conversion_peak_preview_preserves_exact_ready_prefix_and_tail_ownership() {
    for length in 1..=8 {
        let shared = crate::log::CommandBytes::from(vec![7; length]).retained_bytes();
        let boundary = Raft::proposal_batch_staging_bytes(2, 2, 2 * length, 2 * shared)
            .unwrap()
            .max(256);
        let log = LogLimits {
            max_command_bytes: length,
            max_batch_bytes: boundary,
            ..LogLimits::default()
        };
        let mut shard = shard(ShardLimits::default(), log);
        let events: Vec<_> = (1..=3)
            .map(|id| Event::Propose {
                operation: OperationId::new((1 << 127) | id).unwrap(),
                bytes: vec![7; length],
            })
            .collect();
        let admissions: Vec<_> = events
            .iter()
            .map(|event| shard.admit_tracked(group(), event.clone()).unwrap())
            .collect();
        let visit = shard.poll(MonoTime(0)).unwrap().unwrap();
        shard
            .next_ready_effect_reservation(visit, MonoTime(0), 3)
            .unwrap()
            .unwrap();
        assert_eq!(shard.groups[&group()].queues[1].len(), 3);
        let step = shard
            .step_ready_checked(visit, MonoTime(0), 3, |_, _, _| Ok(()))
            .unwrap()
            .unwrap();
        assert_eq!(step.admission, Some(admissions[0]));
        assert_eq!(step.batch_members.len(), 1);
        assert_eq!(step.batch_members[0].admission, Some(admissions[1]));
        assert!(step.batch_members[0].error.is_none());
        let effects = step.result.unwrap();
        let [Effect::Persist(update)] = effects.as_slice() else {
            panic!("ready prefix must be one persistence transition")
        };
        assert_eq!(update.suffix.as_ref().unwrap().entries.len(), 2);
        let queued = shard.groups[&group()].queues[1].front().unwrap();
        assert_eq!(queued.admission, Some(admissions[2]));
        assert_eq!(queued.event, events[2]);
        assert_eq!(shard.usage().items, 3);
        shard
            .with_core(visit, |core| finish(core, effects))
            .unwrap();
        shard.finish(visit).unwrap();
        assert_eq!(shard.usage().items, 1);
    }
}

#[test]
fn conversion_peak_that_disallows_a_pair_retains_the_original_singleton_path() {
    let length = 128;
    let raw_pair = size_of::<Vec<Proposal>>()
        + 2 * size_of::<Proposal>()
        + 2 * size_of::<crate::log::LogEntry>()
        + 2 * length;
    let log = LogLimits {
        max_command_bytes: length,
        max_batch_bytes: raw_pair.max(256),
        ..LogLimits::default()
    };
    let mut shard = shard(ShardLimits::default(), log);
    let events = [1u128, 2].map(|id| Event::Propose {
        operation: OperationId::new((1 << 127) | id).unwrap(),
        bytes: vec![7; length],
    });
    for event in &events {
        shard.admit_tracked(group(), event.clone()).unwrap();
    }
    let visit = shard.poll(MonoTime(0)).unwrap().unwrap();
    let group_state = &shard.groups[&group()];
    assert!(window(group_state, shard.limits, MonoTime(0), 2).is_none());
    let step = shard
        .step_ready_checked(visit, MonoTime(0), 2, |_, _, _| Ok(()))
        .unwrap()
        .unwrap();
    assert!(step.batch_members.is_empty());
    assert_eq!(
        step.operation,
        Some(OperationId::new((1 << 127) | 1).unwrap())
    );
    assert!(step.result.is_ok());
    assert_eq!(
        shard.groups[&group()].queues[1].front().unwrap().event,
        events[1]
    );
    assert_eq!(shard.usage().items, 2);
}

#[test]
fn four_ready_originals_share_one_suffix_keep_positions_and_hold_all_credits() {
    let mut shard = shard(ShardLimits::default(), LogLimits::default());
    let admissions: Vec<_> = (1..=4)
        .map(|id| shard.admit_tracked(group(), command(id)).unwrap())
        .collect();
    let visit = shard.poll(MonoTime(0)).unwrap().unwrap();
    let reserve = shard
        .next_ready_effect_reservation(visit, MonoTime(0), 4)
        .unwrap()
        .unwrap();
    let mut prefixes = Vec::new();
    let step = shard
        .step_ready_checked(visit, MonoTime(0), 4, |_, _, prefix| {
            prefixes.push(
                prefix
                    .iter()
                    .map(|p| p.command.operation.get())
                    .collect::<Vec<_>>(),
            );
            Ok(())
        })
        .unwrap()
        .unwrap();
    assert_eq!(prefixes, vec![vec![], vec![1], vec![1, 2], vec![1, 2, 3]]);
    assert_eq!(step.admission, Some(admissions[0]));
    assert_eq!(step.proposed.unwrap().index, 2);
    assert_eq!(step.batch_members.len(), 3);
    for (offset, member) in step.batch_members.iter().enumerate() {
        assert_eq!(member.admission, Some(admissions[offset + 1]));
        assert_eq!(member.proposed.unwrap().index, offset as u64 + 3);
        assert!(member.error.is_none());
    }
    let effects = step.result.unwrap();
    assert_eq!(effects.len(), 1);
    let Effect::Persist(update) = &effects[0] else {
        panic!("expected persistence");
    };
    assert_eq!(update.suffix.as_ref().unwrap().entries.len(), 4);
    assert!(reserve >= shard.core(group()).unwrap().effect_reservation(0).unwrap());
    assert_eq!(shard.usage().items, 4);
    assert_eq!(shard.finish(visit), Err(RuntimeError::DependencyPending));
    shard
        .with_core(visit, |core| finish(core, effects))
        .unwrap();
    shard.finish(visit).unwrap();
    assert_eq!(shard.usage().items, 0);
}

#[test]
fn failed_later_candidate_stays_queued_and_first_check_is_never_repeated() {
    let mut shard = shard(ShardLimits::default(), LogLimits::default());
    for id in 1..=4 {
        shard.admit_tracked(group(), command(id)).unwrap();
    }
    let visit = shard.poll(MonoTime(0)).unwrap().unwrap();
    let mut calls = Vec::new();
    let step = shard
        .step_ready_checked(visit, MonoTime(0), 4, |_, event, prefix| {
            let Event::Propose { operation, .. } = event else {
                unreachable!()
            };
            calls.push((operation.get(), prefix.len()));
            if operation.get() == 3 {
                Err(RaftError::Busy)
            } else {
                Ok(())
            }
        })
        .unwrap()
        .unwrap();
    assert_eq!(calls, vec![(1, 0), (2, 1), (3, 2)]);
    assert_eq!(step.batch_members.len(), 1);
    let group_state = &shard.groups[&group()];
    assert_eq!(group_state.queues[1].len(), 2);
    assert_eq!(group_state.visit.as_ref().unwrap().used[1].items, 2);
    assert_eq!(shard.usage().items, 4);
    let Event::Propose { operation, .. } = group_state.queues[1].front().unwrap().event else {
        unreachable!()
    };
    assert_eq!(operation.get(), 3);
    shard
        .with_core(visit, |core| finish(core, step.result.unwrap()))
        .unwrap();
    shard.finish(visit).unwrap();
    assert_eq!(shard.usage().items, 2);
}

#[test]
fn first_failure_and_declined_second_keep_legacy_singleton_execution() {
    for fail_first in [true, false] {
        let mut shard = shard(ShardLimits::default(), LogLimits::default());
        for id in 1..=2 {
            shard.admit_tracked(group(), command(id)).unwrap();
        }
        let visit = shard.poll(MonoTime(0)).unwrap().unwrap();
        let mut calls = 0;
        let step = shard
            .step_ready_checked(visit, MonoTime(0), 2, |_, _, prefix| {
                calls += 1;
                if fail_first || !prefix.is_empty() {
                    Err(RaftError::Busy)
                } else {
                    Ok(())
                }
            })
            .unwrap()
            .unwrap();
        assert!(step.batch_members.is_empty());
        assert_eq!(shard.groups[&group()].queues[1].len(), 1);
        assert_eq!(calls, if fail_first { 1 } else { 2 });
        if fail_first {
            assert_eq!(step.result, Err(RaftError::Busy));
        } else {
            assert!(step.result.is_ok());
        }
    }
}

#[test]
fn original_output_and_visit_item_limits_cap_the_same_preview_and_collection() {
    for (visit_limit, output_limit, expected) in [(16, 2, 2), (2, 16, 2), (16, 1, 1)] {
        let limits = ShardLimits {
            visit_items: visit_limit,
            ..ShardLimits::default()
        };
        let mut shard = shard(limits, LogLimits::default());
        for id in 1..=4 {
            shard.admit_tracked(group(), command(id)).unwrap();
        }
        let visit = shard.poll(MonoTime(0)).unwrap().unwrap();
        let before = shard.groups[&group()].queues[1].len();
        shard
            .next_ready_effect_reservation(visit, MonoTime(0), output_limit)
            .unwrap()
            .unwrap();
        assert_eq!(shard.groups[&group()].queues[1].len(), before);
        let step = shard
            .step_ready_checked(visit, MonoTime(0), output_limit, |_, _, _| Ok(()))
            .unwrap()
            .unwrap();
        assert_eq!(step.batch_members.len() + 1, expected);
        assert_eq!(shard.groups[&group()].queues[1].len(), 4 - expected);
    }
}

#[test]
fn nonproposal_untracked_and_protected_queues_are_boundaries() {
    for boundary in 0..5 {
        let mut shard = shard(ShardLimits::default(), LogLimits::default());
        shard.admit_tracked(group(), command(1)).unwrap();
        match boundary {
            0 => shard
                .admit(
                    group(),
                    Event::Read {
                        request: ReadRequestId::new(1).unwrap(),
                    },
                )
                .unwrap(),
            1 => shard.admit(group(), command(2)).unwrap(),
            2 => shard.admit(group(), Event::Heartbeat).unwrap(),
            3 => shard.admit(group(), Event::Checkpoint).unwrap(),
            _ => {
                let token = TimerToken {
                    owner: shard.owner,
                    group: group(),
                    kind: TimerKind::Election,
                    sequence: 1,
                    deadline: MonoTime(1),
                };
                shard
                    .admit_record(group(), command(2), Some(token), true, None)
                    .unwrap();
            }
        }
        shard.admit_tracked(group(), command(3)).unwrap();
        let visit = shard.poll(MonoTime(0)).unwrap().unwrap();
        let step = shard
            .step_ready_checked(visit, MonoTime(0), 4, |_, _, _| Ok(()))
            .unwrap()
            .unwrap();
        assert!(step.batch_members.is_empty());
    }
}

#[test]
fn small_core_batch_limit_never_changes_legacy_singleton_acceptance() {
    let log = LogLimits {
        max_command_bytes: 256,
        max_batch_bytes: 256,
        ..LogLimits::default()
    };
    let mut shard = shard(ShardLimits::default(), log);
    for id in 1..=2 {
        let mut bytes = Vec::with_capacity(4096);
        bytes.push(7);
        shard
            .admit_tracked(
                group(),
                Event::Propose {
                    operation: OperationId::new(id).unwrap(),
                    bytes,
                },
            )
            .unwrap();
    }
    let visit = shard.poll(MonoTime(0)).unwrap().unwrap();
    let step = shard
        .step_ready_checked(visit, MonoTime(0), 2, |_, _, _| Ok(()))
        .unwrap()
        .unwrap();
    assert!(step.batch_members.is_empty());
    assert!(step.result.is_ok());
    assert_eq!(shard.groups[&group()].queues[1].len(), 1);
}

#[test]
fn batch_reservation_charges_aggregate_input_and_selected_original_metadata() {
    let limits = ShardLimits {
        max_event_bytes: 8192,
        ..ShardLimits::default()
    };
    let mut shard = shard(limits, LogLimits::default());
    for id in 1..=4 {
        shard
            .admit_tracked(
                group(),
                Event::Propose {
                    operation: OperationId::new(id).unwrap(),
                    bytes: vec![0; 4096],
                },
            )
            .unwrap();
    }
    let visit = shard.poll(MonoTime(0)).unwrap().unwrap();
    let reserved = shard
        .next_ready_effect_reservation(visit, MonoTime(0), 4)
        .unwrap()
        .unwrap();
    let input = proposal_input(4, 4 * 4096).unwrap();
    let exact = shard
        .core(group())
        .unwrap()
        .effect_reservation(input)
        .unwrap()
        + selected_metadata(4).unwrap();
    assert_eq!(reserved, exact);
    assert_eq!(shard.usage().items, 4);
}

#[test]
fn visit_bytes_and_hard_batch_count_are_independent_caps() {
    let cost = size_of::<Event>() + size_of::<AdmissionTicket>() + 128;
    let limits = ShardLimits {
        max_event_bytes: cost,
        visit_bytes: 2 * cost,
        ..ShardLimits::default()
    };
    let mut bounded = shard(limits, LogLimits::default());
    for id in 1..=4 {
        bounded
            .admit_tracked(
                group(),
                Event::Propose {
                    operation: OperationId::new(id).unwrap(),
                    bytes: vec![0; 128],
                },
            )
            .unwrap();
    }
    let visit = bounded.poll(MonoTime(0)).unwrap().unwrap();
    let step = bounded
        .step_ready_checked(visit, MonoTime(0), 4, |_, _, _| Ok(()))
        .unwrap()
        .unwrap();
    assert_eq!(step.batch_members.len(), 1);
    assert_eq!(
        bounded.groups[&group()].visit.as_ref().unwrap().used[1].bytes,
        2 * cost
    );
    let limits = ShardLimits {
        visit_items: 128,
        ..ShardLimits::default()
    };
    let mut many = shard(limits, LogLimits::default());
    for id in 1..=70 {
        many.admit_tracked(group(), command(id)).unwrap();
    }
    let visit = many.poll(MonoTime(0)).unwrap().unwrap();
    let step = many
        .step_ready_checked(visit, MonoTime(0), 128, |_, _, _| Ok(()))
        .unwrap()
        .unwrap();
    assert_eq!(step.batch_members.len(), MAX_PROPOSAL_BATCH_ENTRIES - 1);
    assert_eq!(many.groups[&group()].queues[1].len(), 6);
}

#[test]
fn duplicate_operation_ids_keep_distinct_original_admissions_and_ordinal_positions() {
    let mut shard = shard(ShardLimits::default(), LogLimits::default());
    let first = shard.admit_tracked(group(), command(1)).unwrap();
    let second = shard.admit_tracked(group(), command(1)).unwrap();
    let visit = shard.poll(MonoTime(0)).unwrap().unwrap();
    let step = shard
        .step_ready_checked(visit, MonoTime(0), 2, |_, _, prefix| {
            if let Some(prior) = prefix.first() {
                assert_eq!(prior.admission, first);
            }
            Ok(())
        })
        .unwrap()
        .unwrap();
    assert_eq!(step.admission, Some(first));
    assert_eq!(step.batch_members[0].admission, Some(second));
    assert_eq!(step.operation, Some(step.batch_members[0].operation));
    assert_eq!(step.proposed.unwrap().index, 2);
    assert_eq!(step.batch_members[0].proposed.unwrap().index, 3);
}
