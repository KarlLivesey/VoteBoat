// SPDX-License-Identifier: RPL-1.5
// Copyright (c) 2026 Karl Livesey
//! Provider counts expose hidden work even when a ready pass returns no steps.
use super::*;
use crate::native::runtime::{DeadlineQueue, JitterEntropy};
use std::{cell::Cell, rc::Rc};

struct CountedReady {
    queue: VecDeque<GroupIdentity>,
    pops: Rc<Cell<usize>>,
    capacity: usize,
}
impl ReadyScheduler for CountedReady {
    fn capacity(&self) -> usize {
        self.capacity
    }
    fn enqueue(&mut self, group: GroupIdentity) -> Result<(), RuntimeError> {
        if !self.queue.contains(&group) {
            self.queue.push_back(group);
        }
        Ok(())
    }
    fn pop(&mut self) -> Option<GroupIdentity> {
        self.pops.set(self.pops.get() + 1);
        self.queue.pop_front()
    }
    fn cancel(&mut self, group: GroupIdentity) {
        self.queue.retain(|g| *g != group);
    }
    fn len(&self) -> usize {
        self.queue.len()
    }
}
struct CountedTimers {
    inner: DeadlineQueue,
    polls: Rc<Cell<usize>>,
}
impl TimerService for CountedTimers {
    fn owner(&self) -> RuntimeOwner {
        self.inner.owner()
    }
    fn capacity(&self) -> usize {
        self.inner.capacity()
    }
    fn register(
        &mut self,
        group: GroupIdentity,
        kind: TimerKind,
        deadline: MonoTime,
    ) -> Result<TimerToken, RuntimeError> {
        self.inner.register(group, kind, deadline)
    }
    fn cancel(&mut self, token: TimerToken) -> Result<(), RuntimeError> {
        self.inner.cancel(token)
    }
    fn poll(&mut self, now: MonoTime, limit: usize) -> Result<Vec<Expiration>, RuntimeError> {
        self.polls.set(self.polls.get() + 1);
        self.inner.poll(now, limit)
    }
    fn len(&self) -> usize {
        self.inner.len()
    }
}
type TestOwner = EffectOwner<CountedReady, CountedTimers, JitterEntropy>;
fn owner(
    groups: usize,
    limits: EffectOwnerLimits,
) -> (TestOwner, Rc<Cell<usize>>, Rc<Cell<usize>>) {
    let mut original = shard(ShardLimits::default(), LogLimits::default());
    let identity = original.owner;
    let core = original.groups.remove(&group()).unwrap().core;
    let other = if groups == 2 {
        let mut state = core.state().clone();
        state.bootstrap.group = second_group();
        Some(
            Raft::recover(
                core.local_node(),
                identity.store,
                state,
                LogLimits::default(),
            )
            .unwrap(),
        )
    } else {
        None
    };
    let pops = Rc::new(Cell::new(0));
    let polls = Rc::new(Cell::new(0));
    let mut shard = Shard::new(
        identity,
        ShardLimits {
            max_groups: groups,
            ..ShardLimits::default()
        },
        CountedReady {
            queue: VecDeque::new(),
            pops: pops.clone(),
            capacity: groups,
        },
    )
    .unwrap();
    shard.register(core).unwrap();
    if let Some(other) = other {
        shard.register(other).unwrap();
    }
    let timed = TimedShard::new(
        shard,
        CountedTimers {
            inner: DeadlineQueue::new(identity, groups).unwrap(),
            polls: polls.clone(),
        },
        JitterEntropy::new(1),
        TimerConfig::default(),
        MonoTime(0),
    )
    .unwrap();
    let worker = crate::worker::WorkerBinding {
        store: identity.store,
        generation: StorageWorkerGeneration::new(1).unwrap(),
    };
    (
        EffectOwner::new(timed, worker, limits).unwrap(),
        pops,
        polls,
    )
}
fn second_group() -> GroupIdentity {
    GroupIdentity {
        id: GroupId::new(2).unwrap(),
        ..group()
    }
}
fn advance(owner: &mut TestOwner, budget: &mut ReadyAdvanceBudget) -> Vec<OwnerStep> {
    owner
        .advance_batch_budgeted(MonoTime(0), budget, |_| true, |_| false, |_, _, _| Ok(()))
        .unwrap()
}

#[test]
fn zero_step_backpressure_consumes_the_original_scheduler_attempt_allowance() {
    let (mut owner, pops, _) = owner(
        2,
        EffectOwnerLimits {
            active_visits: 2,
            control_visits: 1,
            ..EffectOwnerLimits::default()
        },
    );
    owner.admit_tracked(group(), command(1)).unwrap();
    owner.admit_tracked(second_group(), command(2)).unwrap();
    let first = advance(&mut owner, &mut ReadyAdvanceBudget::new(1));
    assert_eq!(first.len(), 1);
    assert!(first[0].error.is_none());
    assert_eq!(owner.usage().active_visits, 1);
    let before = owner.usage();
    let baseline_pops = pops.get();
    // One retained data visit exhausts the only bulk slot. The other group's
    // genuine queued input is repeatedly polled and returned without execution.
    let mut budget = ReadyAdvanceBudget::new(3);
    assert!(advance(&mut owner, &mut budget).is_empty());
    assert_eq!(pops.get() - baseline_pops, 3);
    for _ in 0..4 {
        assert!(advance(&mut owner, &mut budget).is_empty());
    }
    assert_eq!(
        pops.get() - baseline_pops,
        3,
        "no returned steps cannot renew attempts"
    );
    assert_eq!(owner.usage(), before);
    assert_eq!(owner.core(second_group()).unwrap().state().last_index(), 1);
    assert_eq!(owner.core(second_group()).unwrap().state().commit_index, 1);
}

#[test]
fn initial_due_timer_poll_is_once_while_per_attempt_timer_precedence_is_preserved() {
    let (mut owner, pops, polls) = owner(1, EffectOwnerLimits::default());
    let before = owner.core(group()).unwrap().state().clone();
    let mut budget = ReadyAdvanceBudget::new(3);
    // Each empty pass still performs its ordinary TimedShard::poll timer check.
    // Only the extra initial timer poll is shared. Thus deltas are2,1,1,0,
    // rather than renewing the initial poll or granting attempts at every pass.
    for expected in [2, 3, 4, 4] {
        assert!(advance(&mut owner, &mut budget).is_empty());
        assert_eq!(polls.get(), expected);
    }
    assert_eq!(pops.get(), 0);
    assert_eq!(owner.core(group()).unwrap().state(), &before);
    assert_eq!(owner.usage(), EffectOwnerUsage::default());
    let mut next_poll = ReadyAdvanceBudget::new(1);
    assert!(advance(&mut owner, &mut next_poll).is_empty());
    assert_eq!(
        polls.get(),
        6,
        "a new driver poll retains the original initial+attempt cadence"
    );
}
