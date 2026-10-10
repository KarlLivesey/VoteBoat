// SPDX-License-Identifier: RPL-1.5
// Copyright (c) 2026 Karl Livesey
// Unless explicitly acquired and licensed from Licensor under another license,
// the contents of this file are subject to the Reciprocal Public License
// ("RPL") Version 1.5, or subsequent versions as allowed by the RPL, and You may
// not copy or use this file in either source code or executable form, except
// in compliance with the terms and conditions of the RPL.
//
// All software distributed under the RPL is provided strictly on an "AS IS"
// basis, WITHOUT WARRANTY OF ANY KIND, EITHER EXPRESS OR IMPLIED, AND LICENSOR
// HEREBY DISCLAIMS ALL SUCH WARRANTIES, INCLUDING WITHOUT LIMITATION, ANY
// WARRANTIES OF MERCHANTABILITY, FITNESS FOR A PARTICULAR PURPOSE, QUIET
// ENJOYMENT, OR NON-INFRINGEMENT. See the RPL for specific language governing
// rights and limitations under the RPL.
use super::*;
fn policy() -> CheckpointPolicy {
    CheckpointPolicy {
        min_entries: 1,
        interval_ms: 1,
        scan_groups: 1,
        max_in_flight: 2,
    }
}
fn tick(n: &mut Boat, time: u64) {
    n.poll(MonoTime(time), NodePollBudget::default()).unwrap();
}
fn drain(n: &mut Boat, time: u64, done: impl Fn(&Boat) -> bool) {
    for _ in 0..300 {
        tick(n, time);
        if done(n) {
            return;
        }
    }
    panic!("checkpoint did not progress: {:?}", n.checkpoints());
}
fn written() -> Boat {
    let mut n = boat(parts(1, false));
    for g in [1, 2] {
        n.control(group(g), NodeControl::Campaign).unwrap();
    }
    settle(&mut n);
    for g in [1, 2] {
        let mut r = request(1);
        r.group = group(g);
        n.propose(r).unwrap();
    }
    settle(&mut n);
    while let Some(output) = n.poll_client() {
        n.complete_client(output).unwrap();
    }
    n
}
fn base(n: &Boat, g: u128) -> u64 {
    n.local().owner.core(group(g)).unwrap().state().base_index()
}
#[test]
fn policy_is_opt_in_and_validated_without_work() {
    let mut n = written();
    tick(&mut n, 100);
    assert_eq!(base(&n, 1), 0);
    assert_eq!(n.checkpoints(), &CheckpointStatus::default());
    n.configure_checkpoints(Some(policy())).unwrap();
    let before = n.checkpoints().clone();
    for p in [
        CheckpointPolicy {
            min_entries: 0,
            ..policy()
        },
        CheckpointPolicy {
            interval_ms: 0,
            ..policy()
        },
        CheckpointPolicy {
            scan_groups: 0,
            ..policy()
        },
        CheckpointPolicy {
            max_in_flight: 0,
            ..policy()
        },
        CheckpointPolicy {
            scan_groups: 4097,
            ..policy()
        },
        CheckpointPolicy {
            max_in_flight: 4097,
            ..policy()
        },
        CheckpointPolicy {
            interval_ms: u64::MAX,
            ..policy()
        },
    ] {
        assert_eq!(
            n.configure_checkpoints(Some(p)),
            Err(NodeError::InvalidLimits)
        );
        assert_eq!(n.checkpoints(), &before);
    }
    n.configure_checkpoints(None).unwrap();
    tick(&mut n, 200);
    assert_eq!(base(&n, 1), 0);
    for snapshots in [true, false] {
        let mut p = parts(1, false);
        if snapshots {
            p.local
                .snapshots
                .as_mut()
                .unwrap()
                .worker
                .checkpoint_supported = false;
        } else {
            p.local.snapshots = None;
        }
        let mut n = boat(p);
        assert_eq!(
            n.configure_checkpoints(Some(policy())),
            Err(NodeError::MissingSnapshots)
        );
        assert!(n.checkpoints().policy.is_none());
    }
}
#[test]
fn threshold_is_applied_distance_and_admission_is_not_completion() {
    let mut n = written();
    n.configure_checkpoints(Some(CheckpointPolicy {
        min_entries: 3,
        ..policy()
    }))
    .unwrap();
    tick(&mut n, 1);
    assert_eq!(n.checkpoints().cursor, Some(group(1)));
    tick(&mut n, 2);
    assert_eq!(n.checkpoints().cursor, Some(group(2)));
    assert!(n.checkpoints().pending.is_empty());
    n.propose(request(2)).unwrap();
    drain(&mut n, 2, |n| {
        n.local().applications[&group(1)].applied_index() >= 3
    });
    let output = n.poll_client().unwrap();
    n.complete_client(output).unwrap();
    tick(&mut n, 3);
    let pending = n.checkpoints().pending[&group(1)];
    assert_eq!(pending.target_index, 3);
    assert!(!pending.executed);
    assert_eq!(base(&n, 1), 0);
    assert!(n.configure_checkpoints(None).is_err());
    drain(&mut n, 3, |n| n.checkpoints().pending.is_empty());
    assert!(base(&n, 1) >= 3);
    assert_eq!(base(&n, 2), 0);
    assert!(
        matches!(n.checkpoints().last_result, Some(CheckpointResult::Completed { group: g, index: 3 }) if g == group(1))
    );
}
#[test]
fn held_group_does_not_duplicate_or_block_another_group() {
    let mut n = written();
    let hold = n.local().snapshots.as_ref().unwrap().worker.hold.clone();
    hold.set(Some(group(1)));
    n.configure_checkpoints(Some(policy())).unwrap();
    tick(&mut n, 1);
    let first = n.checkpoints().pending[&group(1)].admission;
    tick(&mut n, 2);
    drain(&mut n, 2, |n| base(n, 2) >= 2);
    assert_eq!(base(&n, 1), 0);
    assert_eq!(n.checkpoints().pending[&group(1)].admission, first);
    assert!(n.checkpoints().pending[&group(1)].executed);
    let mut r = request(2);
    r.group = group(2);
    n.propose(r).unwrap();
    drain(&mut n, 2, |n| {
        n.local().applications[&group(2)].applied_index() >= 3
    });
    let output = n.poll_client().unwrap();
    n.complete_client(output).unwrap();
    tick(&mut n, 3);
    tick(&mut n, 4);
    drain(&mut n, 4, |n| base(n, 2) >= 3);
    assert_eq!(n.checkpoints().pending[&group(1)].admission, first);
    assert_eq!(base(&n, 1), 0);
    hold.set(None);
    drain(&mut n, 4, |n| n.checkpoints().pending.is_empty());
    assert!(base(&n, 1) >= 2);
}
#[test]
fn concurrency_bound_and_shutdown_keep_exact_requests_until_durable() {
    let mut n = written();
    let hold = n.local().snapshots.as_ref().unwrap().worker.hold.clone();
    hold.set(Some(group(1)));
    n.configure_checkpoints(Some(CheckpointPolicy {
        scan_groups: 10,
        max_in_flight: 1,
        ..policy()
    }))
    .unwrap();
    tick(&mut n, 1);
    let admitted = n.checkpoints().pending[&group(1)];
    for time in 2..10 {
        tick(&mut n, time);
        assert_eq!(n.checkpoints().pending.len(), 1);
    }
    assert_eq!(base(&n, 2), 0);
    assert_eq!(
        n.checkpoints().pending[&group(1)].admission,
        admitted.admission
    );
    n.begin_shutdown();
    assert_eq!(n.configure_checkpoints(None), Err(NodeError::Closed));
    tick(&mut n, 10);
    assert!(!n.is_drained());
    hold.set(None);
    drain(&mut n, 10, |n| n.is_drained());
    assert!(n.checkpoints().pending.is_empty());
    assert_eq!(base(&n, 2), 0);
}
#[test]
fn manual_checkpoint_and_late_scans_do_not_invent_a_new_boundary() {
    let mut n = written();
    n.configure_checkpoints(Some(policy())).unwrap();
    // FIFO group scheduling consumes the other group's visit, leaving the
    // manual request ahead of the automatic request queued by this scan.
    n.control(group(2), NodeControl::Heartbeat).unwrap();
    n.control(group(1), NodeControl::Checkpoint).unwrap();
    n.poll(
        MonoTime(1),
        NodePollBudget {
            replica: ReplicaPollBudget {
                steps: 1,
                ..Default::default()
            },
            ..Default::default()
        },
    )
    .unwrap();
    assert!(n.checkpoints().pending.contains_key(&group(1)));
    drain(&mut n, 1, |n| {
        n.checkpoints().pending.is_empty() && n.local().owner.is_drained()
    });
    assert!(
        matches!(n.checkpoints().last_result, Some(CheckpointResult::Rejected { group: g, error: RaftError::NotApplied }) if g == group(1))
    );
    let first = base(&n, 1);
    assert!(first >= 2);
    tick(&mut n, 100);
    assert_eq!(n.checkpoints().next_scan, Some(MonoTime(101)));
    drain(&mut n, 100, |n| n.checkpoints().pending.is_empty());
    assert_eq!(base(&n, 1), first);
    assert!(base(&n, 2) >= 2);
    assert_eq!(
        n.poll(MonoTime(99), Default::default()).unwrap_err(),
        NodeError::TimeWentBack
    );
}
#[test]
fn abort_and_snapshot_failure_preserve_checkpoint_identity() {
    for fail in [false, true] {
        let mut p = parts(1, false);
        let worker = &mut p.local.snapshots.as_mut().unwrap().worker;
        worker.fail_checkpoint = fail;
        if !fail {
            worker.hold.set(Some(group(1)));
        }
        let mut n = boat(p);
        n.control(group(1), NodeControl::Campaign).unwrap();
        settle(&mut n);
        n.configure_checkpoints(Some(policy())).unwrap();
        tick(&mut n, 1);
        let pending = n.checkpoints().pending[&group(1)];
        if fail {
            let mut failed = false;
            for _ in 0..20 {
                if n.poll(MonoTime(1), Default::default()).is_err() {
                    failed = true;
                    break;
                }
            }
            assert!(failed);
        } else {
            tick(&mut n, 1);
            n.abort();
        }
        assert_eq!(n.state(), NodeState::RecoveryRequired);
        let recovered = n.into_recovery().unwrap_or_else(|_| panic!("not failed"));
        assert_eq!(
            recovered.checkpoints.pending[&group(1)].admission,
            pending.admission
        );
        assert_eq!(
            recovered
                .local
                .owner
                .core(group(1))
                .unwrap()
                .state()
                .base_index(),
            0
        );
    }
}
