// SPDX-License-Identifier: RPL-1.5
// Copyright (c) 2026 Karl Livesey
use super::*;
use crash::{assert_original_counter, counter, cut, finish_local};

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub(super) enum PublishCut {
    Before,
    After,
}

#[test]
fn short_vote_tails_and_complete_error_frames_have_distinct_recovery() {
    let mut clean = Replay::new();
    let base = clean.images[&1].device.0.borrow().log.len();
    clean.action(&Action::Delay(1)).unwrap();
    clean.action(&Action::Campaign(1)).unwrap();
    let frame = clean.images[&1].device.0.borrow().log.len() - base;
    assert!(frame > 64 && frame <= MAX_FAULT_BYTES);
    for delayed in [false, true] {
        for power_loss in [false, true] {
            for bytes in [0, 1, 31, 32, 64, frame - 1, frame] {
                let mut run = Replay::new();
                let base = run.images[&1].device.0.borrow().log.len();
                if delayed {
                    run.action(&Action::Delay(1)).unwrap();
                }
                run.action(&Action::ShortWrite(1, bytes)).unwrap();
                run.action(&Action::Campaign(1)).unwrap();
                assert_eq!(run.images[&1].device.0.borrow().log.len(), base + bytes);
                assert_eq!(run.point().append_failures, 1);
                assert_eq!(run.point().fenced, BTreeSet::from([1]));
                assert!(run.point().held.is_empty());
                assert_eq!(run.point().queued, 0);
                assert_eq!(run.point().logs[&1].hard_state.term, 0);
                run.action(&cut(1, power_loss)).unwrap();
                let retained = bytes == frame && !power_loss;
                assert_eq!(run.point().logs[&1].hard_state.term, u64::from(retained));
                assert_eq!(
                    run.point().logs[&1].hard_state.voted_for,
                    retained.then_some(node(1))
                );
                assert_eq!(run.point().queued, 0);
            }
        }
    }
}

#[test]
fn manifest_failure_before_or_after_replacement_never_releases_votes() {
    for publication in [PublishCut::Before, PublishCut::After] {
        for power_loss in [false, true] {
            let mut run = Replay::new();
            let old = run.images[&1].device.0.borrow().manifest.clone();
            run.action(&Action::Delay(1)).unwrap();
            run.action(&Action::Campaign(1)).unwrap();
            run.action(&Action::FailPublish(1, publication)).unwrap();
            let device = run.images[&1].device.0.borrow();
            assert_eq!(device.log, device.synced);
            assert_eq!(device.manifest != old, publication == PublishCut::After);
            drop(device);
            assert_eq!(run.point().publish_failures, 1);
            assert_eq!(run.point().fenced, BTreeSet::from([1]));
            assert!(run.point().held.is_empty());
            assert_eq!(run.point().queued, 0);
            assert_eq!(run.point().logs[&1].hard_state.term, 0);
            run.action(&cut(1, power_loss)).unwrap();
            assert_eq!(run.point().logs[&1].hard_state.term, 1);
            assert_eq!(run.point().logs[&1].hard_state.voted_for, Some(node(1)));
            assert_eq!(run.point().queued, 0);
        }
    }
}

#[test]
fn ambiguous_commit_publication_recovers_counter_and_original_retry_once() {
    for publication in [PublishCut::Before, PublishCut::After] {
        for power_loss in [false, true] {
            let mut run = Replay::new();
            run.action(&Action::Campaign(1)).unwrap();
            run.action(&Action::Pump).unwrap();
            run.action(&Action::Delay(1)).unwrap();
            run.action(&Action::TryCounter(1, 9911, 9)).unwrap();
            run.action(&Action::Sync(1)).unwrap();
            run.action(&Action::Complete(1)).unwrap();
            run.action(&Action::Pump).unwrap();
            assert!(run.point().held.contains_key(&1));
            assert_eq!(counter(&run, 1).0.read_applied(0), Ok(0));
            run.action(&Action::FailPublish(1, publication)).unwrap();
            assert_eq!(counter(&run, 1).0.read_applied(0), Ok(0));
            assert_eq!(run.point().queued, 0);
            run.action(&cut(1, power_loss)).unwrap();
            assert_original_counter(&run, 1);
            run.action(&Action::Campaign(1)).unwrap();
            finish_local(&mut run, 1);
            run.action(&Action::TryCounter(1, 9911, 9)).unwrap();
            finish_local(&mut run, 1);
            for id in 1..=3 {
                assert_original_counter(&run, id);
                assert!(counter(&run, id).1.last().unwrap().duplicate);
                run.action(&Action::PowerLoss(id)).unwrap();
                assert_original_counter(&run, id);
            }
        }
    }
}

#[test]
fn short_write_and_manifest_failures_share_exact_composed_replay() {
    let mut actions = vec![
        Action::ShortWrite(1, 31),
        Action::Campaign(1),
        Action::ProcessCrash(1),
        Action::Delay(1),
    ];
    for publication in [PublishCut::Before, PublishCut::After] {
        actions.extend([
            Action::Campaign(1),
            Action::FailPublish(1, publication),
            Action::PowerLoss(1),
        ]);
    }
    actions.push(Action::Campaign(1));
    for _ in 0..3 {
        actions.extend([Action::Sync(1), Action::Complete(1), Action::Pump]);
    }
    actions.push(Action::TryCounter(1, 9911, 9));
    for _ in 0..2 {
        actions.extend([Action::Sync(1), Action::Complete(1), Action::Pump]);
    }
    actions.extend([Action::PowerLoss(1), Action::Stale(1)]);
    let trace = Trace { seed: 266, actions };
    let saved = trace.encode();
    let decoded = Trace::decode(&saved).unwrap();
    let points = replay(&trace).unwrap();
    assert_eq!(points, replay(&decoded).unwrap(), "{saved}");
    let last = points.last().unwrap();
    assert_eq!(last.append_failures, 1);
    assert_eq!(last.publish_failures, 2);
    assert_eq!(last.stale_rejected, 1);
    assert!(last.fenced.is_empty());
    eprintln!("composed append/publication trace:\n{saved}");
}
