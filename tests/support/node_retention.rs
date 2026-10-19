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
//! ST2: store maintenance on by default, in simulated time.
use super::*;
use voteboat::contracts::StorageError;

/// The counter binary's application capacity (`counter_setup.rs`).
const COUNTER_CAPACITY: u64 = 10_000;
const ENTRY_LIMIT: u64 = 16_384;
/// Writes in flight per round; within the default client router limits.
const WINDOW: u64 = 64;

/// The counter binary's application on the default runtime node, with room in
/// the in-memory snapshot worker for its 10,000-receipt checkpoint.
fn counter_parts() -> Parts {
    let mut p = parts(1, false);
    p.local
        .applications
        .insert(group(1), Counter::new(COUNTER_CAPACITY as usize).unwrap());
    p.local.snapshots.as_mut().unwrap().worker.allowance = 1 << 22;
    p
}
/// Client write `k`: operation IDs cycle through the application's own
/// capacity, so its lifetime table (audit S1, DR1's row) is never what fails.
/// Repeats are ordinary client retries; every one is a new log entry.
fn write(k: u64) -> ClientRequest {
    ClientRequest {
        group: group(1),
        operation: OperationId::new(u128::from(k % COUNTER_CAPACITY + 1)).unwrap(),
        bytes: 1i64.to_le_bytes().to_vec(),
    }
}
struct Clock(u64);
impl Clock {
    fn poll(&mut self, n: &mut Boat) {
        self.0 += 1;
        n.poll(MonoTime(self.0), NodePollBudget::default())
            .unwrap_or_else(|e| panic!("poll at {} ms: {e:?}", self.0));
    }
}
fn core(n: &Boat) -> &Raft {
    n.local().owner.core(group(1)).unwrap()
}
fn retained(n: &Boat) -> u64 {
    core(n).state().last_index() - core(n).state().base_index()
}
/// The typed, non-fatal refusals at the retained-entry limit: the batched
/// admission check, or the single-proposal transition plan.
fn at_entry_limit(outcome: &ClientOutcome<CounterReceipt>) -> bool {
    matches!(
        outcome,
        ClientOutcome::NotProposed(RaftError::Storage(StorageError::Rejected(
            "proposal retained entry budget" | "group entry limit"
        )))
    )
}
fn stop(n: &mut Boat, clock: &mut Clock) {
    n.begin_shutdown();
    for _ in 0..1000 {
        clock.poll(n);
        if n.is_drained() {
            return;
        }
    }
    panic!("not shut down");
}
fn elect(n: &mut Boat, clock: &mut Clock) {
    let term = core(n).state().hard_state.term;
    n.control(group(1), NodeControl::Campaign).unwrap();
    for _ in 0..1000 {
        clock.poll(n);
        let c = core(n);
        if c.role() == Role::Leader
            && c.state().hard_state.term > term
            && n.local().applications[&group(1)].applied_index() == c.state().last_index()
        {
            return;
        }
    }
    panic!("no leader: {:?}", core(n).role());
}
/// Issue `total` writes, keeping `WINDOW` in flight; every write must apply.
/// `round` runs after every poll.
fn drive(n: &mut Boat, clock: &mut Clock, first: u64, total: u64, round: impl FnMut(&Boat)) {
    assert_eq!(drive_until(n, clock, first, total, round, |_| false), total);
}
/// As `drive`, but stop at the first outcome `stop` accepts instead of
/// failing; returns the number of applied writes.
fn drive_until(
    n: &mut Boat,
    clock: &mut Clock,
    first: u64,
    total: u64,
    mut round: impl FnMut(&Boat),
    stop: impl Fn(&ClientOutcome<CounterReceipt>) -> bool,
) -> u64 {
    let (mut issued, mut done) = (0, 0);
    let mut stopped = false;
    while done < total && !(stopped && issued == done) {
        while !stopped && issued < total && issued - done < WINDOW {
            if n.propose(write(first + issued)).is_err() {
                break;
            }
            issued += 1;
        }
        clock.poll(n);
        round(n);
        while let Some(output) = n.poll_client() {
            match n.complete_client(output).unwrap() {
                ClientOutcome::Applied { .. } => done += 1,
                other if stop(&other) => {
                    stopped = true;
                    issued -= 1;
                }
                other => panic!(
                    "write {} not applied at log index {} (retained {}): {other:?}",
                    first + done,
                    core(n).state().last_index(),
                    retained(n)
                ),
            }
        }
    }
    done
}

/// Fail@main (3d552e2e): the default node refuses write 16,384 with a typed
/// `NotProposed(Storage(Rejected(..)))` at the retained-entry limit. With ST2
/// it applies 2.5× that many writes (10× and more with
/// `VOTEBOAT_ST2_SOAK_WRITES`, recorded in release) plus an election every
/// 2,048; the retained log stays below the limit (the threshold of 8,192 plus
/// writes that arrive while a checkpoint runs), so no write is refused.
#[test]
fn default_node_keeps_accepting_past_the_retained_entry_limit() {
    const ELECTION_EVERY: u64 = 2_048;
    // VOTEBOAT_ST2_SOAK_WRITES runs longer (release builds).
    let total: u64 = std::env::var("VOTEBOAT_ST2_SOAK_WRITES")
        .map_or(5 * ENTRY_LIMIT / 2, |v| v.parse().unwrap())
        .next_multiple_of(ELECTION_EVERY);
    let mut n = boat(counter_parts());
    let mut clock = Clock(0);
    elect(&mut n, &mut clock);
    let mut max_retained = 0;
    let mut bases = vec![core(&n).state().base_index()];
    // Sample every tenth of the run, and at most once per 16,384 writes: the
    // writes arrive at 64 per simulated millisecond, so one 100 ms scan
    // interval spans 6,400 of them and a checkpoint must land between samples.
    let every = (total / 10).max(ENTRY_LIMIT);
    let (mut written, mut sample) = (0, every);
    while written < total {
        drive(&mut n, &mut clock, written, ELECTION_EVERY, |n| {
            max_retained = max_retained.max(retained(n));
        });
        written += ELECTION_EVERY;
        elect(&mut n, &mut clock);
        if written >= sample {
            // Every tenth of the run: bounded, still checkpointing, serving.
            sample += every;
            let base = core(&n).state().base_index();
            assert!(retained(&n) < ENTRY_LIMIT, "{}", retained(&n));
            assert!(
                base > *bases.last().unwrap(),
                "no checkpoint since {bases:?}"
            );
            assert_eq!(n.state(), NodeState::Running);
            bases.push(base);
        }
    }
    let last = core(&n).state().last_index();
    assert!(last > total, "{last}");
    assert!(max_retained < ENTRY_LIMIT, "{max_retained}");
    assert!(matches!(
        n.checkpoints().last_result,
        Some(CheckpointResult::Completed { .. })
    ));
    eprintln!(
        "ST2 runtime soak: writes={total} last_index={last} max_retained={max_retained} bases={bases:?}"
    );
    stop(&mut n, &mut clock);
}

/// U1 / Q6: with maintenance explicitly disabled, a leader elected at the
/// retained-entry limit stays leader and refuses typed (on 3d552e2e the node
/// entered `RecoveryRequired` with `Storage(Rejected("group entry limit"))`
/// from inside the vote completion). An operator checkpoint then frees space
/// and the deferred noop commits.
#[test]
fn lifetime_bounded_leader_at_the_entry_limit_refuses_typed_and_stays_leader() {
    let mut n = bounded(counter_parts());
    let mut clock = Clock(0);
    elect(&mut n, &mut clock);
    let full = drive_until(&mut n, &mut clock, 0, ENTRY_LIMIT, |_| {}, at_entry_limit);
    assert_eq!(retained(&n), ENTRY_LIMIT);
    let refused = |n: &mut Boat, clock: &mut Clock| {
        let ticket = n.propose(write(full)).unwrap();
        for _ in 0..100 {
            clock.poll(n);
            if let Some(output) = n.poll_client() {
                assert_eq!(output.ticket(), ticket);
                return n.complete_client(output).unwrap();
            }
        }
        panic!("no outcome");
    };
    assert!(at_entry_limit(&refused(&mut n, &mut clock)));
    for _ in 0..3 {
        let term = core(&n).state().hard_state.term;
        n.control(group(1), NodeControl::Campaign).unwrap();
        for _ in 0..200 {
            clock.poll(&mut n);
        }
        assert_eq!(n.state(), NodeState::Running);
        assert_eq!(core(&n).role(), Role::Leader);
        assert_eq!(core(&n).state().hard_state.term, term + 1);
        assert!(core(&n).leader_noop_pending());
        assert_eq!(retained(&n), ENTRY_LIMIT);
        assert!(at_entry_limit(&refused(&mut n, &mut clock)));
    }
    n.control(group(1), NodeControl::Checkpoint).unwrap();
    let term = core(&n).state().hard_state.term;
    for _ in 0..1000 {
        clock.poll(&mut n);
        let state = core(&n).state();
        if !core(&n).leader_noop_pending()
            && state.last_term() == term
            && state.commit_index == state.last_index()
        {
            break;
        }
    }
    assert!(core(&n).state().base_index() > 0);
    assert!(!core(&n).leader_noop_pending());
    // The deferred noop is the leader's first current-term entry; it commits.
    assert_eq!(core(&n).state().last_term(), term);
    assert_eq!(core(&n).state().hard_state.term, term);
    assert_eq!(core(&n).role(), Role::Leader);
    drive(&mut n, &mut clock, full, 1, |_| {});
    assert_eq!(n.state(), NodeState::Running);
    stop(&mut n, &mut clock);
}

/// RT4 fault matrix: after every automatic-reclaim refusal or failed
/// completion the schedule has a next deadline, or the node reports
/// `RecoveryRequired` for a fatal storage or provider fault. Maintenance
/// never stops while the node serves.
#[test]
fn every_reclaim_failure_reschedules_or_recovers_visibly() {
    let admission = [
        WorkerError::Overloaded,
        WorkerError::BatchTooLarge,
        WorkerError::WrongBinding,
        WorkerError::Exhausted,
        WorkerError::InvalidLimits,
        WorkerError::Unsupported,
        WorkerError::Runtime(RuntimeError::Overloaded),
        WorkerError::Consensus(RaftError::Busy),
        WorkerError::Closed,
        WorkerError::Fenced,
    ];
    for error in admission {
        let mut p = parts(1, false);
        p.local.persistence.reclaim_supported = true;
        p.local.persistence.reclaim_reject = Some(error.clone());
        let mut n = boat(p);
        let _ = n.poll(MonoTime(1000), NodePollBudget::default());
        let status = n.wal_maintenance();
        assert_eq!(status.last_admission_error.as_ref(), Some(&error));
        let fatal = matches!(error, WorkerError::Closed | WorkerError::Fenced);
        if fatal {
            assert_eq!(n.state(), NodeState::RecoveryRequired, "{error:?}");
        } else {
            assert_eq!(n.state(), NodeState::Running, "{error:?}");
            assert_eq!(status.next_deadline, Some(MonoTime(1100)), "{error:?}");
            assert_eq!(status.consecutive_failures, 1);
            // The retry runs and succeeds.
            n.poll(MonoTime(1100), NodePollBudget::default()).unwrap();
            n.poll(MonoTime(1101), NodePollBudget::default()).unwrap();
            assert_eq!(n.wal_maintenance().consecutive_failures, 0, "{error:?}");
            assert_eq!(
                n.wal_maintenance().next_deadline,
                Some(MonoTime(2101)),
                "{error:?}"
            );
        }
    }
    let completion = [
        StorageError::Rejected("reclamation requires drained transitions"),
        StorageError::WrongIdentity,
        StorageError::StaleTicket,
        StorageError::Compacted { first_index: 1 },
        StorageError::Uncertain("publication".into()),
        StorageError::Corrupt("image"),
        StorageError::Fenced,
    ];
    for error in completion {
        let mut p = parts(1, false);
        p.local.persistence.reclaim_supported = true;
        p.local.persistence.reclaim_error = Some(error.clone());
        let mut n = boat(p);
        n.poll(MonoTime(1000), NodePollBudget::default()).unwrap();
        let _ = n.poll(MonoTime(1001), NodePollBudget::default());
        let fatal = matches!(
            error,
            StorageError::Uncertain(_) | StorageError::Corrupt(_) | StorageError::Fenced
        );
        let status = n.wal_maintenance();
        assert_eq!(
            status.last_completion.as_ref().unwrap().result,
            Err(error.clone())
        );
        if fatal {
            assert_eq!(n.state(), NodeState::RecoveryRequired, "{error:?}");
            assert!(status.next_deadline.is_none());
        } else {
            assert_eq!(n.state(), NodeState::Running, "{error:?}");
            assert_eq!(status.next_deadline, Some(MonoTime(1101)), "{error:?}");
            assert_eq!(status.consecutive_failures, 1);
        }
    }
}
