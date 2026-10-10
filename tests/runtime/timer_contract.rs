// SPDX-License-Identifier: RPL-1.5
// Copyright (c) 2026 Karl Livesey
use super::*;

fn scoped_cancellation<T: TimerService>(timers: &mut T) {
    let token = timers
        .register(group(1), TimerKind::Election, MonoTime(10))
        .unwrap();
    assert_eq!(token.owner, timers.owner());
    let mut mutations = [token; 5];
    mutations[0].owner.generation = RuntimeGeneration::new(2).unwrap();
    mutations[1].group.incarnation = GroupIncarnation::new(2).unwrap();
    mutations[2].kind = TimerKind::Heartbeat;
    mutations[3].sequence += 1;
    mutations[4].deadline = MonoTime(11);
    for stale in mutations {
        assert_eq!(timers.cancel(stale), Err(RuntimeError::StaleTicket));
        assert_eq!(timers.len(), 1);
        assert!(!timers.is_empty());
    }
    assert_eq!(
        timers.poll(MonoTime(12), 1).unwrap(),
        vec![Expiration { token, late_ms: 2 }]
    );
    assert_eq!(timers.cancel(token), Err(RuntimeError::StaleTicket));
    assert!(timers.is_empty());
}

fn replacement_and_budget<T: TimerService>(mut timers: T) {
    assert_eq!(timers.capacity(), 3);
    assert!(timers.is_empty());
    scoped_cancellation(&mut timers);
    let first = timers
        .register(group(1), TimerKind::Election, MonoTime(20))
        .unwrap();
    let heartbeat = timers
        .register(group(1), TimerKind::Heartbeat, MonoTime(20))
        .unwrap();
    let mut reincarnated = group(1);
    reincarnated.incarnation = GroupIncarnation::new(2).unwrap();
    let other = timers
        .register(reincarnated, TimerKind::Election, MonoTime(20))
        .unwrap();
    assert_eq!(timers.len(), 3);
    assert_eq!(
        timers.register(group(2), TimerKind::Election, MonoTime(15)),
        Err(RuntimeError::Overloaded)
    );
    let replacement = timers
        .register(group(1), TimerKind::Election, MonoTime(30))
        .unwrap();
    assert_ne!(replacement, first);
    assert_eq!(timers.len(), 3);
    assert_eq!(timers.cancel(first), Err(RuntimeError::StaleTicket));
    assert!(timers.poll(MonoTime(25), 0).unwrap().is_empty());
    assert_eq!(timers.len(), 3);
    assert_eq!(
        timers.poll(MonoTime(24), 3),
        Err(RuntimeError::ClockRegressed)
    );
    let expired = timers.poll(MonoTime(25), 1).unwrap();
    assert_eq!(expired.len(), 1);
    assert!(expired[0].token == heartbeat || expired[0].token == other);
    assert_eq!(expired[0].late_ms, 5);
    let remaining = timers.poll(MonoTime(25), usize::MAX).unwrap();
    assert_eq!(remaining.len(), 1);
    assert!(remaining[0].token == heartbeat || remaining[0].token == other);
    assert_ne!(remaining[0].token, expired[0].token);
    assert_eq!(remaining[0].late_ms, 5);
    assert_eq!(timers.len(), 1);
    timers.cancel(replacement).unwrap();
    assert_eq!(timers.cancel(replacement), Err(RuntimeError::StaleTicket));
    let past = timers
        .register(group(1), TimerKind::Election, MonoTime(10))
        .unwrap();
    assert_ne!(past, first);
    assert_ne!(past, replacement);
    assert_eq!(
        timers.poll(MonoTime(25), 3).unwrap(),
        vec![Expiration {
            token: past,
            late_ms: 15
        }]
    );
    assert!(timers.is_empty());
}

fn poll_model<T: TimerService>(
    timers: &mut T,
    live: &mut Vec<TimerToken>,
    now: MonoTime,
    limit: usize,
) {
    let due = live.iter().filter(|token| token.deadline <= now).count();
    let expirations = timers.poll(now, limit).unwrap();
    assert_eq!(expirations.len(), due.min(limit));
    for expiration in expirations {
        let token = expiration.token;
        assert!(token.deadline <= now);
        assert_eq!(expiration.late_ms, now.0 - token.deadline.0);
        assert!(!live.iter().any(|other| other.deadline < token.deadline));
        let position = live.iter().position(|other| *other == token).unwrap();
        live.remove(position);
        assert_eq!(timers.cancel(token), Err(RuntimeError::StaleTicket));
    }
}

fn modeled_traces<T: TimerService>(factory: impl Fn() -> T) {
    for seed in 1..=8_u64 {
        let mut timers = factory();
        let binding = timers.owner();
        let mut live = Vec::<TimerToken>::new();
        let mut history = Vec::<TimerToken>::new();
        let mut now = MonoTime(0);
        let mut random = seed;
        for _ in 0..128 {
            random = random
                .wrapping_mul(6_364_136_223_846_793_005)
                .wrapping_add(1);
            match random >> 61 {
                0..=3 => {
                    let identity = group(u128::from(1 + (random >> 16) % 4));
                    let kind = if random & 1 == 0 {
                        TimerKind::Election
                    } else {
                        TimerKind::Heartbeat
                    };
                    let deadline = MonoTime(now.0.saturating_sub(2) + (random >> 8) % 12);
                    let position = live
                        .iter()
                        .position(|t| t.group == identity && t.kind == kind);
                    let result = timers.register(identity, kind, deadline);
                    if position.is_none() && live.len() == timers.capacity() {
                        assert_eq!(result, Err(RuntimeError::Overloaded));
                    } else {
                        let token = result.unwrap();
                        assert_eq!(
                            (token.owner, token.group, token.kind, token.deadline),
                            (binding, identity, kind, deadline)
                        );
                        assert!(!history.contains(&token));
                        history.push(token);
                        if let Some(index) = position {
                            live.remove(index);
                        }
                        live.push(token);
                    }
                }
                4 => {
                    let choice = usize::try_from((random >> 8) % 128).unwrap();
                    if let Some(token) = history.get(choice % history.len().max(1)) {
                        let position = live.iter().position(|t| t == token);
                        if let Some(index) = position {
                            timers.cancel(*token).unwrap();
                            live.remove(index);
                        } else {
                            assert_eq!(timers.cancel(*token), Err(RuntimeError::StaleTicket));
                        }
                    }
                }
                5..=6 => {
                    now = MonoTime(now.0 + 1);
                    let limit = usize::try_from((random >> 8) % 4).unwrap();
                    poll_model(&mut timers, &mut live, now, limit);
                }
                _ if now.0 > 0 => assert_eq!(
                    timers.poll(MonoTime(now.0 - 1), 0),
                    Err(RuntimeError::ClockRegressed)
                ),
                _ => {}
            }
            assert_eq!(timers.owner(), binding);
            assert_eq!(timers.len(), live.len());
            assert_eq!(timers.is_empty(), live.is_empty());
        }
        poll_model(&mut timers, &mut live, MonoTime(now.0 + 12), usize::MAX);
        assert!(timers.is_empty());
    }
}

fn owner_lifetime<T: TimerService>(factory: impl Fn(RuntimeOwner) -> T) {
    let old_owner = owner(HostLogStore::new(1).binding());
    let mut first = factory(old_owner);
    let old = first
        .register(group(1), TimerKind::Election, MonoTime(10))
        .unwrap();
    drop(first);
    let mut new_owner = old_owner;
    new_owner.generation = RuntimeGeneration::new(2).unwrap();
    let mut reopened = factory(new_owner);
    let current = reopened
        .register(group(1), TimerKind::Election, MonoTime(10))
        .unwrap();
    assert_eq!(old.sequence, current.sequence);
    assert_ne!(old, current);
    assert_eq!(current.owner, new_owner);
    assert_eq!(reopened.cancel(old), Err(RuntimeError::StaleTicket));
    assert_eq!(reopened.len(), 1);
    assert_eq!(reopened.poll(MonoTime(10), 1).unwrap()[0].token, current);
    assert!(reopened.is_empty());
}

#[test]
fn host_timer_exact_tokens_and_bounded_histories() {
    let factory = || HostTimers::new(owner(HostLogStore::new(1).binding()), 3);
    replacement_and_budget(factory());
    modeled_traces(factory);
    owner_lifetime(|binding| HostTimers::new(binding, 3));
}

#[cfg(feature = "native")]
#[test]
fn native_timer_exact_tokens_and_bounded_histories() {
    let factory = || {
        voteboat::native::runtime::DeadlineQueue::new(owner(HostLogStore::new(1).binding()), 3)
            .unwrap()
    };
    replacement_and_budget(factory());
    modeled_traces(factory);
    owner_lifetime(|binding| voteboat::native::runtime::DeadlineQueue::new(binding, 3).unwrap());
}

struct WrongOwnerCancellation(HostTimers);
impl TimerService for WrongOwnerCancellation {
    fn owner(&self) -> RuntimeOwner {
        self.0.owner()
    }
    fn capacity(&self) -> usize {
        self.0.capacity()
    }
    fn register(
        &mut self,
        group: GroupIdentity,
        kind: TimerKind,
        deadline: MonoTime,
    ) -> Result<TimerToken, RuntimeError> {
        self.0.register(group, kind, deadline)
    }
    fn cancel(&mut self, mut token: TimerToken) -> Result<(), RuntimeError> {
        token.owner = self.0.owner();
        self.0.cancel(token)
    }
    fn poll(&mut self, now: MonoTime, limit: usize) -> Result<Vec<Expiration>, RuntimeError> {
        self.0.poll(now, limit)
    }
    fn len(&self) -> usize {
        self.0.len()
    }
}

#[test]
fn shared_timer_assertions_detect_cancellation_ignoring_owner() {
    let mut wrong =
        WrongOwnerCancellation(HostTimers::new(owner(HostLogStore::new(1).binding()), 3));
    let failure = std::panic::catch_unwind(std::panic::AssertUnwindSafe(|| {
        scoped_cancellation(&mut wrong);
    }));
    assert!(failure.is_err());
    assert!(
        wrong.is_empty(),
        "incorrect cancellation removed the live token"
    );
}
