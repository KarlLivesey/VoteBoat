// SPDX-License-Identifier: RPL-1.5
// Copyright (c) 2026 Karl Livesey
use super::*;

#[derive(Clone, Copy)]
enum Fault {
    StoreBlind,
    IgnoreBudget,
    DestructiveRefusal,
    FalseLateness,
    ReversedOrder,
}

struct DefectiveTimers {
    inner: HostTimers,
    fault: Fault,
}

impl DefectiveTimers {
    fn new(binding: RuntimeOwner, fault: Fault) -> Self {
        Self {
            inner: HostTimers::new(binding, 3),
            fault,
        }
    }
}

impl TimerService for DefectiveTimers {
    fn owner(&self) -> RuntimeOwner {
        self.inner.owner()
    }
    fn capacity(&self) -> usize {
        self.inner.capacity()
    }
    fn len(&self) -> usize {
        self.inner.len()
    }
    fn register(
        &mut self,
        group: GroupIdentity,
        kind: TimerKind,
        deadline: MonoTime,
    ) -> Result<TimerToken, RuntimeError> {
        self.inner.register(group, kind, deadline)
    }
    fn cancel(&mut self, mut token: TimerToken) -> Result<(), RuntimeError> {
        if matches!(self.fault, Fault::StoreBlind) {
            token.owner.store = self.inner.owner().store;
        }
        self.inner.cancel(token)
    }
    fn poll(&mut self, now: MonoTime, limit: usize) -> Result<Vec<Expiration>, RuntimeError> {
        if matches!(self.fault, Fault::DestructiveRefusal) && now < self.inner.now {
            self.inner.entries.clear();
        }
        if matches!(self.fault, Fault::ReversedOrder) {
            return self.reverse_poll(now, limit);
        }
        let budget = if matches!(self.fault, Fault::IgnoreBudget) {
            usize::MAX
        } else {
            limit
        };
        let mut expired = self.inner.poll(now, budget)?;
        if matches!(self.fault, Fault::FalseLateness) {
            for expiration in &mut expired {
                expiration.late_ms = 0;
            }
        }
        Ok(expired)
    }
}

impl DefectiveTimers {
    fn reverse_poll(
        &mut self,
        now: MonoTime,
        limit: usize,
    ) -> Result<Vec<Expiration>, RuntimeError> {
        if now < self.inner.now {
            return Err(RuntimeError::ClockRegressed);
        }
        self.inner.now = now;
        self.inner
            .entries
            .sort_by_key(|token| std::cmp::Reverse(token.deadline));
        let mut expired = Vec::new();
        while expired.len() < limit {
            let Some(position) = self
                .inner
                .entries
                .iter()
                .position(|token| token.deadline <= now)
            else {
                break;
            };
            let token = self.inner.entries.remove(position);
            expired.push(Expiration {
                token,
                late_ms: now.0 - token.deadline.0,
            });
        }
        Ok(expired)
    }
}

fn rejects(fault: Fault, check: impl FnOnce(DefectiveTimers)) {
    let provider = DefectiveTimers::new(owner(HostLogStore::new(1).binding()), fault);
    assert!(std::panic::catch_unwind(std::panic::AssertUnwindSafe(|| check(provider))).is_err());
}

#[test]
fn shared_owner_assertions_detect_store_blind_cancellation() {
    assert!(std::panic::catch_unwind(|| {
        isolation::exact_owner_isolation(|binding| {
            DefectiveTimers::new(binding, Fault::StoreBlind)
        });
    })
    .is_err());
}

#[test]
fn shared_poll_assertions_detect_budget_bypass() {
    rejects(Fault::IgnoreBudget, polling::refusal_preserves_deadlines);
}

#[test]
fn shared_poll_assertions_detect_destructive_regression_refusal() {
    rejects(
        Fault::DestructiveRefusal,
        polling::refusal_preserves_deadlines,
    );
}

#[test]
fn shared_poll_assertions_detect_false_lateness() {
    rejects(Fault::FalseLateness, polling::ordered_budget_and_lateness);
}

#[test]
fn shared_poll_assertions_detect_reversed_deadline_order() {
    rejects(Fault::ReversedOrder, polling::ordered_budget_and_lateness);
}
