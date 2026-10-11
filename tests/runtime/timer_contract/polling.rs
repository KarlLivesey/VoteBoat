// SPDX-License-Identifier: RPL-1.5
// Copyright (c) 2026 Karl Livesey
use super::*;

pub(super) fn refusal_preserves_deadlines<T: TimerService>(mut timers: T) {
    let original = timers
        .register(group(1), TimerKind::Election, MonoTime(10))
        .unwrap();
    assert!(timers.poll(MonoTime(12), 0).unwrap().is_empty());
    assert_eq!(timers.len(), 1, "zero budget retains an already due token");
    assert_eq!(
        timers.poll(MonoTime(11), 1),
        Err(RuntimeError::ClockRegressed)
    );
    assert_eq!(timers.len(), 1, "refusal retains the original deadline");
    assert_eq!(
        timers.poll(MonoTime(12), 1).unwrap(),
        vec![Expiration {
            token: original,
            late_ms: 2
        }]
    );
    assert!(timers.is_empty());
    assert!(timers.poll(MonoTime(12), usize::MAX).unwrap().is_empty());
    assert_eq!(timers.cancel(original), Err(RuntimeError::StaleTicket));
}

pub(super) fn ordered_budget_and_lateness<T: TimerService>(mut timers: T) {
    let later = timers
        .register(group(1), TimerKind::Election, MonoTime(20))
        .unwrap();
    let earliest = timers
        .register(group(2), TimerKind::Election, MonoTime(3))
        .unwrap();
    let middle = timers
        .register(group(3), TimerKind::Heartbeat, MonoTime(10))
        .unwrap();
    for (token, late_ms, retained) in [(earliest, 27, 2), (middle, 20, 1), (later, 10, 0)] {
        assert_eq!(
            timers.poll(MonoTime(30), 1).unwrap(),
            vec![Expiration { token, late_ms }]
        );
        assert_eq!(timers.len(), retained);
        assert_eq!(timers.cancel(token), Err(RuntimeError::StaleTicket));
    }
    assert!(timers.is_empty());
}

fn boundary_lateness<T: TimerService>(mut timers: T) {
    let original = timers
        .register(group(1), TimerKind::Election, MonoTime(0))
        .unwrap();
    assert_eq!(
        timers.poll(MonoTime(u64::MAX), 1).unwrap(),
        vec![Expiration {
            token: original,
            late_ms: u64::MAX
        }]
    );
    let boundary = timers
        .register(group(1), TimerKind::Election, MonoTime(u64::MAX))
        .unwrap();
    assert!(timers.poll(MonoTime(u64::MAX), 0).unwrap().is_empty());
    assert_eq!(timers.len(), 1);
    assert_eq!(
        timers.poll(MonoTime(u64::MAX), 1).unwrap(),
        vec![Expiration {
            token: boundary,
            late_ms: 0
        }]
    );
    assert!(timers.is_empty());
}

fn poll_obligations<T: TimerService>(factory: impl Fn(usize) -> T) {
    for capacity in [1, 3, 17] {
        refusal_preserves_deadlines(factory(capacity));
        boundary_lateness(factory(capacity));
    }
    ordered_budget_and_lateness(factory(3));
}

#[test]
fn host_timer_poll_retains_refused_work_and_transfers_exact_bounded_expirations() {
    poll_obligations(|capacity| HostTimers::new(owner(HostLogStore::new(1).binding()), capacity));
}

#[cfg(feature = "native")]
#[test]
fn native_timer_poll_retains_refused_work_and_transfers_exact_bounded_expirations() {
    poll_obligations(|capacity| {
        voteboat::native::runtime::DeadlineQueue::new(
            owner(HostLogStore::new(1).binding()),
            capacity,
        )
        .unwrap()
    });
}
