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
mod support;
use std::{
    cell::Cell,
    collections::{BTreeMap, BTreeSet, VecDeque},
    rc::Rc,
};
use support::*;
use voteboat::{application::*, identity::*, log::*, raft::*, runtime::*};

// Independent downstream providers use only public contracts.
struct HostReady {
    groups: Vec<GroupIdentity>,
    capacity: usize,
}
impl HostReady {
    fn new(capacity: usize) -> Self {
        Self {
            groups: Vec::new(),
            capacity,
        }
    }
}
impl ReadyScheduler for HostReady {
    fn capacity(&self) -> usize {
        self.capacity
    }
    fn enqueue(&mut self, group: GroupIdentity) -> Result<(), RuntimeError> {
        if self.groups.contains(&group) {
            return Ok(());
        }
        if self.groups.len() == self.capacity {
            return Err(RuntimeError::Overloaded);
        }
        self.groups.push(group);
        Ok(())
    }
    fn pop(&mut self) -> Option<GroupIdentity> {
        if self.groups.is_empty() {
            None
        } else {
            Some(self.groups.remove(0))
        }
    }
    fn cancel(&mut self, group: GroupIdentity) {
        self.groups.retain(|g| *g != group);
    }
    fn len(&self) -> usize {
        self.groups.len()
    }
}
struct HostTimers {
    owner: RuntimeOwner,
    sequence: u64,
    now: MonoTime,
    entries: Vec<TimerToken>,
    capacity: usize,
}
impl HostTimers {
    fn new(owner: RuntimeOwner, capacity: usize) -> Self {
        Self {
            owner,
            sequence: 0,
            now: MonoTime(0),
            entries: Vec::new(),
            capacity,
        }
    }
}
impl TimerService for HostTimers {
    fn owner(&self) -> RuntimeOwner {
        self.owner
    }
    fn capacity(&self) -> usize {
        self.capacity
    }
    fn register(
        &mut self,
        group: GroupIdentity,
        kind: TimerKind,
        deadline: MonoTime,
    ) -> Result<TimerToken, RuntimeError> {
        let old = self
            .entries
            .iter()
            .position(|t| t.group == group && t.kind == kind);
        if old.is_none() && self.entries.len() == self.capacity {
            return Err(RuntimeError::Overloaded);
        }
        if let Some(pos) = old {
            self.entries.remove(pos);
        }
        self.sequence += 1;
        let t = TimerToken {
            owner: self.owner,
            group,
            kind,
            sequence: self.sequence,
            deadline,
        };
        self.entries.push(t);
        Ok(t)
    }
    fn cancel(&mut self, token: TimerToken) -> Result<(), RuntimeError> {
        let pos = self
            .entries
            .iter()
            .position(|t| *t == token)
            .ok_or(RuntimeError::StaleTicket)?;
        self.entries.remove(pos);
        Ok(())
    }
    fn poll(&mut self, now: MonoTime, limit: usize) -> Result<Vec<Expiration>, RuntimeError> {
        if now < self.now {
            return Err(RuntimeError::ClockRegressed);
        }
        self.now = now;
        self.entries.sort_by_key(|t| (t.deadline, t.sequence));
        let count = self
            .entries
            .iter()
            .take_while(|t| t.deadline <= now)
            .count()
            .min(limit);
        Ok(self
            .entries
            .drain(..count)
            .map(|token| Expiration {
                token,
                late_ms: now.0 - token.deadline.0,
            })
            .collect())
    }
    fn len(&self) -> usize {
        self.entries.len()
    }
}
#[derive(Clone)]
struct VirtualClock(Rc<Cell<u64>>);
impl Clock for VirtualClock {
    fn now(&self) -> MonoTime {
        MonoTime(self.0.get())
    }
}
struct FixedEntropy(u64);
impl ElectionEntropy for FixedEntropy {
    fn sample(&mut self) -> u64 {
        self.0 += 1;
        self.0
    }
}
fn owner(binding: StoreBinding) -> RuntimeOwner {
    RuntimeOwner {
        store: binding,
        lane: ExecutionLaneId::new(1).unwrap(),
        generation: RuntimeGeneration::new(1).unwrap(),
    }
}
fn scheduler_conformance<Q: ReadyScheduler>(mut q: Q) {
    assert_eq!(q.capacity(), 3);
    for id in 1..=3 {
        q.enqueue(group(id)).unwrap();
        q.enqueue(group(id)).unwrap();
    }
    assert_eq!(q.len(), 3);
    assert_eq!(q.enqueue(group(4)), Err(RuntimeError::Overloaded));
    assert_eq!(q.pop(), Some(group(1)));
    q.enqueue(group(1)).unwrap();
    assert_eq!(q.pop(), Some(group(2)));
    q.cancel(group(3));
    q.cancel(group(3));
    assert_eq!(q.pop(), Some(group(1)));
    assert!(q.is_empty());
}
fn timer_conformance<T: TimerService>(mut timers: T) {
    let old = timers
        .register(group(1), TimerKind::Election, MonoTime(10))
        .unwrap();
    let current = timers
        .register(group(1), TimerKind::Election, MonoTime(30))
        .unwrap();
    assert_ne!(old, current);
    assert_eq!(timers.cancel(old), Err(RuntimeError::StaleTicket));
    assert!(timers.poll(MonoTime(10), 10).unwrap().is_empty());
    let heartbeat = timers
        .register(group(1), TimerKind::Heartbeat, MonoTime(20))
        .unwrap();
    let second = timers
        .register(group(2), TimerKind::Election, MonoTime(20))
        .unwrap();
    assert_eq!(
        timers.register(group(3), TimerKind::Election, MonoTime(20)),
        Err(RuntimeError::Overloaded)
    );
    assert_eq!(timers.poll(MonoTime(25), 0).unwrap(), vec![]);
    assert_eq!(
        timers.poll(MonoTime(25), 1).unwrap(),
        vec![Expiration {
            token: heartbeat,
            late_ms: 5
        }]
    );
    assert_eq!(
        timers.poll(MonoTime(25), 3).unwrap(),
        vec![Expiration {
            token: second,
            late_ms: 5
        }]
    );
    assert_eq!(timers.cancel(second), Err(RuntimeError::StaleTicket));
    assert_eq!(
        timers.poll(MonoTime(24), 1),
        Err(RuntimeError::ClockRegressed)
    );
    let mut foreign = current;
    foreign.owner.generation = RuntimeGeneration::new(2).unwrap();
    assert_eq!(timers.cancel(foreign), Err(RuntimeError::StaleTicket));
    for ms in 30..1030 {
        timers
            .register(group(1), TimerKind::Election, MonoTime(ms))
            .unwrap();
        assert_eq!(timers.len(), 1);
    }
    assert!(timers.poll(MonoTime(1028), 3).unwrap().is_empty());
    assert_eq!(timers.poll(MonoTime(1030), 3).unwrap()[0].late_ms, 1);
    assert!(timers.is_empty());
}
#[test]
fn host_ready_deadline_clock_and_entropy_contracts() {
    scheduler_conformance(HostReady::new(3));
    timer_conformance(HostTimers::new(owner(HostLogStore::new(1).binding()), 3));
    let clock = VirtualClock(Rc::new(Cell::new(100)));
    let mut entropy = FixedEntropy(0);
    assert_eq!(
        election_deadline(&clock, &mut entropy, 100, 20),
        Ok(MonoTime(201))
    );
    clock.0.set(u64::MAX);
    assert_eq!(
        election_deadline(&clock, &mut entropy, 100, 20),
        Err(RuntimeError::Exhausted)
    );
    assert_eq!(
        election_deadline(&clock, &mut entropy, 0, 20),
        Err(RuntimeError::InvalidLimits)
    );
}
fn small_limits() -> ShardLimits {
    ShardLimits {
        max_groups: 3,
        max_items: 16,
        max_bytes: 16000,
        group_items: 8,
        group_bytes: 8000,
        control_items: 4,
        control_bytes: 2000,
        group_control_items: 2,
        group_control_bytes: 1000,
        background_items: 2,
        background_bytes: 4000,
        max_event_bytes: 2048,
        visit_items: 1,
        visit_bytes: 2048,
        visit_ms: 5,
    }
}
fn host_shard<Q: ReadyScheduler>(scheduler: Q) -> (Shard<Q>, HostLogStore) {
    let mut log = HostLogStore::new(1);
    append(
        &mut log,
        (1..=3)
            .map(|g| LogMutation::Create(bootstrap(g, 3)))
            .collect(),
    );
    let mut shard = Shard::new(owner(log.binding()), small_limits(), scheduler).unwrap();
    for id in 1..=3 {
        shard
            .register(
                Raft::recover(
                    node(1),
                    log.binding(),
                    log.state(group(id)).unwrap(),
                    log.limits(),
                )
                .unwrap(),
            )
            .unwrap();
    }
    (shard, log)
}
fn proposal(operation: u128) -> Event {
    Event::Propose {
        operation: OperationId::new(operation).unwrap(),
        bytes: 7i64.to_le_bytes().to_vec(),
    }
}
fn shard_overload_history<Q: ReadyScheduler>(scheduler: Q) {
    let (mut shard, _) = host_shard(scheduler);
    for n in 1..=6 {
        shard.admit(group(1), proposal(n)).unwrap();
    }
    let rejected = shard.admit(group(1), proposal(7)).unwrap_err();
    assert_eq!(rejected.reason, RuntimeError::Overloaded);
    assert_eq!(*rejected.event, proposal(7));
    shard.admit(group(1), Event::Heartbeat).unwrap();
    shard.admit(group(1), Event::Heartbeat).unwrap();
    assert_eq!(
        shard.admit(group(1), Event::Heartbeat).unwrap_err().reason,
        RuntimeError::Overloaded
    );
    shard.admit(group(2), Event::Heartbeat).unwrap();
    let first = shard.poll(MonoTime(0)).unwrap().unwrap();
    assert_eq!(first.group, group(1));
    let control = shard.step_next(first, MonoTime(0)).unwrap().unwrap();
    assert!(control.operation.is_none());
    assert!(control.result.unwrap().is_empty());
    assert_eq!(shard.group_usage(group(1)).unwrap().items, 8); // includes leased input
    shard.finish(first).unwrap();
    assert_eq!(
        shard.with_core(first, |_| ()),
        Err(RuntimeError::StaleTicket)
    );
    let healthy = shard.poll(MonoTime(0)).unwrap().unwrap();
    assert_eq!(healthy.group, group(2));
    shard.step_next(healthy, MonoTime(0)).unwrap();
    shard.finish(healthy).unwrap();
    // Continuing control arrivals cannot starve proposals, even with one item visits.
    let mut seen = 0;
    for _ in 0..12 {
        let _ = shard.admit(group(1), Event::Heartbeat);
        let t = shard.poll(MonoTime(0)).unwrap().unwrap();
        if let Some(s) = shard.step_next(t, MonoTime(0)).unwrap() {
            if s.operation.is_some() {
                assert_eq!(s.result.unwrap_err(), RaftError::NotLeader);
                seen += 1;
            }
        }
        shard.finish(t).unwrap();
    }
    assert!(seen >= 3);
    shard.close_admission();
    assert_eq!(
        shard.admit(group(2), proposal(99)).unwrap_err().reason,
        RuntimeError::Closed
    );
    while let Some(t) = shard.poll(MonoTime(0)).unwrap() {
        shard.step_next(t, MonoTime(0)).unwrap();
        shard.finish(t).unwrap();
    }
    assert!(shard.is_drained());
}
#[test]
fn host_shard_reserves_control_bounds_retention_and_fairly_drains() {
    shard_overload_history(HostReady::new(3));
}

#[test]
fn node_item_and_group_byte_limits_each_preserve_control_reserves() {
    let (mut shard, _) = host_shard(HostReady::new(3));
    for g in 1..=2 {
        for n in 1..=6 {
            shard.admit(group(g), proposal(n)).unwrap();
        }
    }
    assert_eq!(
        shard.admit(group(3), proposal(1)).unwrap_err().reason,
        RuntimeError::Overloaded
    );
    for _ in 0..4 {
        shard.admit(group(3), Event::Heartbeat).unwrap();
    }
    assert_eq!(
        shard.admit(group(3), Event::Heartbeat).unwrap_err().reason,
        RuntimeError::Overloaded
    );
    assert_eq!(shard.usage().items, 16);

    let mut log = HostLogStore::new(1);
    append(&mut log, vec![LogMutation::Create(bootstrap(1, 3))]);
    // Exactly two data events and one reserved control event, independent of
    // the inline enum size as public protocol variants are added.
    let event_bytes = std::mem::size_of::<Event>();
    let limits = ShardLimits {
        group_bytes: 3 * event_bytes + 256,
        group_control_bytes: event_bytes,
        max_event_bytes: event_bytes + 128,
        visit_bytes: event_bytes + 128,
        ..small_limits()
    };
    let mut shard = Shard::new(owner(log.binding()), limits, HostReady::new(3)).unwrap();
    shard
        .register(
            Raft::recover(
                node(1),
                log.binding(),
                log.state(group(1)).unwrap(),
                log.limits(),
            )
            .unwrap(),
        )
        .unwrap();
    for n in 1..=2 {
        shard
            .admit(
                group(1),
                Event::Propose {
                    operation: OperationId::new(n).unwrap(),
                    bytes: vec![0; 128],
                },
            )
            .unwrap();
    }
    assert_eq!(
        shard.admit(group(1), proposal(3)).unwrap_err().reason,
        RuntimeError::Overloaded
    );
    shard.admit(group(1), Event::Heartbeat).unwrap();
    assert_eq!(shard.usage().items, 3);
    assert_eq!(shard.usage().bytes, limits.group_bytes);
}

#[test]
fn suspended_dependency_scopes_completion_and_allows_another_group_to_run() {
    let (mut shard, mut log) = host_shard(HostReady::new(3));
    shard.admit(group(1), Event::Campaign).unwrap();
    shard.admit(group(2), Event::Heartbeat).unwrap();
    let t = shard.poll(MonoTime(10)).unwrap().unwrap();
    let effects = shard
        .step_next(t, MonoTime(10))
        .unwrap()
        .unwrap()
        .result
        .unwrap();
    let [Effect::Persist(update)] = effects.as_slice() else {
        panic!("campaign must first persist")
    };
    assert_eq!(shard.finish(t), Err(RuntimeError::DependencyPending));
    assert!(matches!(
        shard.step_next(t, MonoTime(10)),
        Err(RuntimeError::DependencyPending)
    ));
    let tickets = log
        .append_batch(vec![LogMutation::Update(update.clone())])
        .unwrap();
    let mut bad = update.clone();
    bad.hard_state.term += 1;
    assert_eq!(
        shard
            .with_core(t, |core| core.admit_effect(&bad, tickets[0]))
            .unwrap(),
        Err(RaftError::WrongCompletion)
    );
    shard
        .with_core(t, |core| core.admit_effect(update, tickets[0]))
        .unwrap()
        .unwrap();
    let healthy = shard.poll(MonoTime(11)).unwrap().unwrap();
    assert_eq!(healthy.group, group(2));
    shard
        .step_next(healthy, MonoTime(11))
        .unwrap()
        .unwrap()
        .result
        .unwrap();
    shard.finish(healthy).unwrap();
    assert!(shard.poll(MonoTime(12)).unwrap().is_none());
    assert_eq!(
        shard
            .with_core(t, |core| core.complete(&DurableLog { tickets: vec![] }))
            .unwrap(),
        Err(RaftError::WrongCompletion)
    );
    let completion = log.barrier(&tickets).unwrap();
    let sent = shard
        .with_core(t, |core| core.complete(&completion))
        .unwrap()
        .unwrap();
    assert_eq!(sent.len(), 2);
    assert!(sent.iter().all(|e| matches!(
        e,
        Effect::Send(Message {
            rpc: Rpc::Vote { .. },
            ..
        })
    )));
    shard.finish(t).unwrap();
    assert!(shard.is_drained());
    assert_eq!(shard.poll(MonoTime(11)), Err(RuntimeError::ClockRegressed));
}

#[test]
fn deadlines_visit_tokens_and_shutdown_do_not_alias_or_roll_back_work() {
    let (mut shard, _) = host_shard(HostReady::new(3));
    shard.admit(group(1), Event::Heartbeat).unwrap();
    let t = shard.poll(MonoTime(0)).unwrap().unwrap();
    assert!(shard.step_next(t, MonoTime(5)).unwrap().is_none());
    shard.finish(t).unwrap();
    let next = shard.poll(MonoTime(5)).unwrap().unwrap();
    assert_ne!(t, next);
    let mut forged = next;
    forged.owner.generation = RuntimeGeneration::new(2).unwrap();
    assert_eq!(
        shard.with_core(forged, |_| ()),
        Err(RuntimeError::StaleTicket)
    );
    let stopped = shard.stop_group(group(1)).unwrap();
    assert!(stopped.had_active_visit);
    assert_eq!(stopped.queued, vec![Event::Heartbeat]);
    assert_eq!(
        shard.with_core(next, |_| ()),
        Err(RuntimeError::StaleTicket)
    );
    assert_eq!(
        shard.admit(group(1), Event::Heartbeat).unwrap_err().reason,
        RuntimeError::Fenced
    );
    assert!(shard.is_drained());
}

#[test]
fn retained_capacity_group_identity_and_background_are_charged_before_admission() {
    let (mut shard, _) = host_shard(HostReady::new(3));
    let mut bytes = Vec::with_capacity(4096);
    bytes.extend(7i64.to_le_bytes());
    assert_eq!(
        shard
            .admit(
                group(1),
                Event::Propose {
                    operation: OperationId::new(1).unwrap(),
                    bytes
                }
            )
            .unwrap_err()
            .reason,
        RuntimeError::EventTooLarge
    );
    assert_eq!(shard.usage(), Usage::default());
    assert_eq!(
        shard.admit(group(99), Event::Heartbeat).unwrap_err().reason,
        RuntimeError::UnknownGroup
    );
    let rpc = Message {
        group: group(1),
        configuration: ConfigurationId::new(1).unwrap(),
        from: node(2),
        sender: HostLogStore::new(2).binding(),
        to: node(1),
        term: 1,
        context: RequestContext {
            origin: HostLogStore::new(2).binding(),
            sequence: 1,
        },
        rpc: Rpc::Snapshot {
            snapshot: Box::new(voteboat::snapshot::Snapshot {
                metadata: voteboat::snapshot::SnapshotMetadata {
                    membership: None,
                    bootstrap: bootstrap(1, 3),
                    index: 1,
                    term: 1,
                    application_schema: 1,
                },
                application: vec![7; 4],
            }),
        },
    };
    assert_eq!(
        shard
            .admit(group(2), Event::Receive(rpc.clone()))
            .unwrap_err()
            .reason,
        RuntimeError::WrongOwner
    );
    shard.admit(group(1), Event::Receive(rpc.clone())).unwrap();
    shard.admit(group(1), Event::Receive(rpc.clone())).unwrap();
    assert_eq!(
        shard
            .admit(group(1), Event::Receive(rpc))
            .unwrap_err()
            .reason,
        RuntimeError::Overloaded
    );
    shard.admit(group(2), Event::Heartbeat).unwrap();
}

#[cfg(feature = "native")]
#[test]
fn native_scheduler_deadline_clock_and_entropy_conformance() {
    use voteboat::native::runtime::*;
    scheduler_conformance(FairScheduler::new(3).unwrap());
    shard_overload_history(FairScheduler::new(3).unwrap());
    timer_conformance(DeadlineQueue::new(owner(HostLogStore::new(1).binding()), 3).unwrap());
    let clock = MonotonicClock::new();
    let shared = clock.clone();
    let before = clock.now();
    assert!(shared.now() >= before);
    let mut a = JitterEntropy::new(7);
    let mut b = JitterEntropy::new(7);
    let now = VirtualClock(Rc::new(Cell::new(200)));
    let mut samples = BTreeSet::new();
    for _ in 0..100 {
        let x = election_deadline(&now, &mut a, 100, 100).unwrap();
        assert_eq!(x, election_deadline(&now, &mut b, 100, 100).unwrap());
        assert!((300..400).contains(&x.0));
        samples.insert(x);
    }
    assert!(samples.len() > 20);
}

struct MultiNode<L: LogStore, Q: ReadyScheduler> {
    shard: Shard<Q>,
    log: L,
    apps: BTreeMap<GroupIdentity, Counter>,
    pending: BTreeMap<GroupIdentity, (VisitTicket, LogUpdate)>,
    delayed: BTreeSet<GroupIdentity>,
    waiting: BTreeMap<GroupIdentity, (VisitTicket, DurableLog)>,
    messages: VecDeque<Message>,
    deferred: VecDeque<Message>,
    results: Vec<CounterReceipt>,
    reads: Vec<i64>,
    largest_batch: usize,
}
impl<L: LogStore, Q: ReadyScheduler> MultiNode<L, Q> {
    fn new(id: u64, mut log: L, ready: Q) -> Self {
        append(
            &mut log,
            (1..=100)
                .map(|g| LogMutation::Create(bootstrap(g, 3)))
                .collect(),
        );
        let limits = ShardLimits {
            max_groups: 100,
            max_items: 2048,
            max_bytes: 2 * 1024 * 1024,
            control_items: 128,
            control_bytes: 64 * 1024,
            ..small_limits()
        };
        let mut shard = Shard::new(owner(log.binding()), limits, ready).unwrap();
        let mut apps = BTreeMap::new();
        for id_group in 1..=100 {
            let g = group(id_group);
            shard
                .register(
                    Raft::recover(node(id), log.binding(), log.state(g).unwrap(), log.limits())
                        .unwrap(),
                )
                .unwrap();
            apps.insert(g, Counter::new(10).unwrap());
        }
        Self {
            shard,
            log,
            apps,
            pending: BTreeMap::new(),
            delayed: BTreeSet::new(),
            waiting: BTreeMap::new(),
            messages: VecDeque::new(),
            deferred: VecDeque::new(),
            results: Vec::new(),
            reads: Vec::new(),
            largest_batch: 0,
        }
    }
    fn effects(&mut self, visit: VisitTicket, effects: Vec<Effect>) {
        for effect in effects {
            match effect {
                Effect::Persist(update) => {
                    assert!(self.pending.insert(visit.group, (visit, update)).is_none());
                }
                Effect::Send(m) => {
                    assert!(self.messages.len() < 8192);
                    self.messages.push_back(m);
                }
                Effect::Committed(entries) => self.results.extend(
                    self.apps
                        .get_mut(&visit.group)
                        .unwrap()
                        .apply_batch(&entries)
                        .unwrap(),
                ),
                Effect::ReadReady(barrier) => {
                    let app = &self.apps[&visit.group];
                    self.reads.push(
                        self.shard
                            .with_core(visit, |core| read_at_barrier(core, &barrier, app, ()))
                            .unwrap()
                            .unwrap(),
                    );
                }
                _ => panic!("this shared-WAL history does not compact"),
            }
        }
        if !self
            .shard
            .core(visit.group)
            .unwrap()
            .has_pending_dependency()
        {
            self.shard.finish(visit).unwrap();
        }
    }
    fn poll_ready(&mut self, now: MonoTime) -> bool {
        let mut progress = false;
        // At most 100 group visits, independent of a hot group's retained work.
        for _ in 0..100 {
            let Some(t) = self.shard.poll(now).unwrap() else {
                break;
            };
            let step = self.shard.step_next(t, now).unwrap().unwrap();
            let effects = step.result.unwrap();
            self.effects(t, effects);
            progress = true;
        }
        progress
    }
    fn flush(&mut self) -> bool {
        if self.pending.is_empty() {
            return false;
        }
        let pending = std::mem::take(&mut self.pending)
            .into_values()
            .collect::<Vec<_>>();
        assert!(pending.len() <= self.log.limits().max_batch_units);
        self.largest_batch = self.largest_batch.max(pending.len());
        let tickets = self
            .log
            .append_batch(
                pending
                    .iter()
                    .map(|(_, u)| LogMutation::Update(u.clone()))
                    .collect(),
            )
            .unwrap();
        assert_eq!(pending.len(), tickets.len());
        for ((visit, update), ticket) in pending.iter().zip(&tickets) {
            self.shard
                .with_core(*visit, |core| core.admit_effect(update, *ticket))
                .unwrap()
                .unwrap();
        }
        let durable = self.log.barrier(&tickets).unwrap();
        for (visit, _) in pending {
            if self.delayed.contains(&visit.group) {
                assert!(self
                    .waiting
                    .insert(visit.group, (visit, durable.clone()))
                    .is_none());
            } else {
                let effects = self
                    .shard
                    .with_core(visit, |core| core.complete(&durable))
                    .unwrap()
                    .unwrap();
                self.effects(visit, effects);
            }
        }
        true
    }
    fn release(&mut self, g: GroupIdentity) {
        self.delayed.remove(&g);
        let (visit, durable) = self.waiting.remove(&g).unwrap();
        let effects = self
            .shard
            .with_core(visit, |core| core.complete(&durable))
            .unwrap()
            .unwrap();
        self.effects(visit, effects);
    }
}
fn pump_multi<L: LogStore, Q: ReadyScheduler>(nodes: &mut [MultiNode<L, Q>], now: MonoTime) {
    for _ in 0..10000 {
        let mut progress = false;
        for n in nodes.iter_mut() {
            progress |= n.poll_ready(now);
        }
        for n in nodes.iter_mut() {
            progress |= n.flush();
        }
        let mut messages = VecDeque::new();
        for n in nodes.iter_mut() {
            messages.append(&mut n.deferred);
            messages.append(&mut n.messages);
        }
        assert!(messages.len() <= 8192);
        while let Some(m) = messages.pop_front() {
            let target = m.to.get() as usize - 1;
            match nodes[target].shard.admit(m.group, Event::Receive(m)) {
                Ok(()) => progress = true,
                Err(rejected) if rejected.reason == RuntimeError::Overloaded => {
                    let Event::Receive(message) = *rejected.event else {
                        panic!()
                    };
                    nodes[target].deferred.push_back(message);
                }
                Err(rejected) => panic!("unexpected network rejection: {rejected:?}"),
            }
        }
        assert!(nodes.iter().map(|n| n.deferred.len()).sum::<usize>() <= 8192);
        if !progress {
            return;
        }
    }
    panic!("shared runtime did not converge");
}

fn hundred_group_history<L: LogStore, Q: ReadyScheduler, T: TimerService>(
    nodes: &mut [MultiNode<L, Q>],
    mut timers: T,
) {
    let clock = VirtualClock(Rc::new(Cell::new(0)));
    let mut expected = BTreeMap::new();
    for g in 1..=100 {
        expected.insert(
            group(g),
            timers
                .register(group(g), TimerKind::Election, MonoTime(10))
                .unwrap(),
        );
    }
    clock.0.set(11);
    // Explicit virtual-time service, bounded expiration batches. Duplicate or
    // canceled expirations cannot create another campaign after their token is consumed.
    let mut observed = Vec::new();
    while !timers.is_empty() {
        let due = timers.poll(clock.now(), 17).unwrap();
        assert!(due.len() <= 17);
        for expiration in due {
            assert_eq!(expiration.late_ms, 1);
            assert_eq!(
                expected.remove(&expiration.token.group),
                Some(expiration.token)
            );
            nodes[0]
                .shard
                .admit(expiration.token.group, Event::Campaign)
                .unwrap();
            observed.push(expiration.token);
        }
    }
    assert!(expected.is_empty());
    assert!(!expected.contains_key(&observed[0].group));
    pump_multi(nodes, clock.now());
    for n in nodes.iter() {
        for g in 1..=100 {
            assert_eq!(n.shard.core(group(g)).unwrap().state().commit_index, 1);
        }
    }
    assert_eq!(nodes[0].largest_batch, 100); // one shared WAL batch, not 100 stores
    nodes[0].delayed.insert(group(1));
    for g in 1..=100 {
        nodes[0].shard.admit(group(g), proposal(1)).unwrap();
    }
    pump_multi(nodes, clock.now());
    assert_eq!(
        nodes[0].shard.core(group(1)).unwrap().state().commit_index,
        1
    );
    assert_eq!(nodes[0].shard.active_visits(), 1);
    assert_eq!(nodes[0].shard.group_usage(group(1)).unwrap().items, 1);
    for n in nodes.iter() {
        for g in 2..=100 {
            assert_eq!(n.apps[&group(g)].read_applied(2), Ok(7));
        }
    }
    // Group-local overload retains the active input and leaves control credits.
    let mut accepted = 0;
    while nodes[0].shard.admit(group(1), proposal(1)).is_ok() {
        accepted += 1;
        assert!(accepted < 8);
    }
    assert_eq!(accepted, 5);
    nodes[0].shard.admit(group(1), Event::Heartbeat).unwrap();
    nodes[0]
        .shard
        .admit(
            group(100),
            Event::Read {
                request: ReadRequestId::new(1).unwrap(),
            },
        )
        .unwrap();
    pump_multi(nodes, clock.now());
    assert_eq!(nodes[0].reads, [7]);
    nodes[0].release(group(1));
    pump_multi(nodes, clock.now());
    assert_eq!(nodes[0].apps[&group(1)].read_applied(7), Ok(7));
    for g in 1..=100 {
        nodes[0].shard.admit(group(g), proposal(1)).unwrap();
    }
    pump_multi(nodes, clock.now());
    for g in 1..=100 {
        nodes[0]
            .shard
            .admit(
                group(g),
                Event::Propose {
                    operation: OperationId::new(2).unwrap(),
                    bytes: 3i64.to_le_bytes().to_vec(),
                },
            )
            .unwrap();
    }
    pump_multi(nodes, clock.now());
    for n in nodes.iter() {
        for (g, app) in &n.apps {
            assert_eq!(
                app.read_applied(n.shard.core(*g).unwrap().state().commit_index),
                Ok(10)
            );
        }
        assert!(n.shard.is_drained());
        assert!(n
            .results
            .iter()
            .any(|r| r.duplicate && r.outcome == CounterOutcome::Value(7)));
    }
    for g in 1..=100 {
        nodes[0]
            .shard
            .admit(
                group(g),
                Event::Read {
                    request: ReadRequestId::new(2).unwrap(),
                },
            )
            .unwrap();
    }
    pump_multi(nodes, clock.now());
    assert_eq!(nodes[0].reads.len(), 101);
    assert!(nodes[0].reads[1..].iter().all(|v| *v == 10));
}
#[test]
fn hundred_groups_share_host_log_and_scheduler_with_delayed_completion_isolation() {
    let mut nodes = (1..=3)
        .map(|id| MultiNode::new(id, HostLogStore::new(id as u128), HostReady::new(100)))
        .collect::<Vec<_>>();
    let timers = HostTimers::new(owner(nodes[0].log.binding()), 100);
    hundred_group_history(&mut nodes, timers);
}

#[cfg(feature = "native")]
#[test]
fn hundred_groups_share_native_wals_and_recover_acknowledged_state() {
    use voteboat::native::{log_store::*, runtime::*};
    let images = (1..=3).map(|_| ModelIo::default()).collect::<Vec<_>>();
    let mut nodes = (1..=3)
        .map(|id| {
            MultiNode::new(
                id,
                NativeLogStore::create(
                    images[id as usize - 1].clone(),
                    identity(id as u128),
                    LogLimits::default(),
                )
                .unwrap(),
                FairScheduler::new(100).unwrap(),
            )
        })
        .collect::<Vec<_>>();
    let timers = DeadlineQueue::new(owner(nodes[0].log.binding()), 100).unwrap();
    hundred_group_history(&mut nodes, timers);
    let old_ticket_owner = owner(nodes[0].log.binding());
    drop(nodes);
    for (i, image) in images.into_iter().enumerate() {
        image.0.borrow_mut().power_loss();
        let log =
            NativeLogStore::recover(image, identity(i as u128 + 1), LogLimits::default()).unwrap();
        assert_ne!(log.binding().session, old_ticket_owner.store.session);
        let mut shard = Shard::new(
            owner(log.binding()),
            ShardLimits {
                max_groups: 100,
                ..ShardLimits::default()
            },
            FairScheduler::new(100).unwrap(),
        )
        .unwrap();
        for g in 1..=100 {
            let core = Raft::recover(
                node(i as u64 + 1),
                log.binding(),
                log.state(group(g)).unwrap(),
                log.limits(),
            )
            .unwrap();
            let mut app = Counter::new(10).unwrap();
            app.apply_batch(core.replay_committed()).unwrap();
            assert_eq!(app.read_applied(core.state().commit_index), Ok(10));
            shard.register(core).unwrap();
        }
        shard.admit(group(1), Event::Heartbeat).unwrap();
        let current = shard.poll(MonoTime(100)).unwrap().unwrap();
        let mut stale = current;
        stale.owner = old_ticket_owner;
        assert_eq!(
            shard.with_core(stale, |_| ()),
            Err(RuntimeError::StaleTicket)
        );
        shard
            .step_next(current, MonoTime(100))
            .unwrap()
            .unwrap()
            .result
            .unwrap();
        shard.finish(current).unwrap();
    }
}

#[cfg(feature = "native")]
#[test]
fn hundred_group_history_uses_actual_shared_files_on_three_nodes() {
    use voteboat::native::{log_store::*, runtime::*};
    let root = std::env::temp_dir().join(format!("voteboat-shared-runtime-{}", std::process::id()));
    let _ = std::fs::remove_dir_all(&root);
    std::fs::create_dir_all(&root).unwrap();
    let mut nodes = (1..=3)
        .map(|id| {
            MultiNode::new(
                id,
                NativeLogStore::create(
                    FileLogIo::create(root.join(id.to_string())).unwrap(),
                    identity(id as u128),
                    LogLimits::default(),
                )
                .unwrap(),
                FairScheduler::new(100).unwrap(),
            )
        })
        .collect::<Vec<_>>();
    let timers = DeadlineQueue::new(owner(nodes[0].log.binding()), 100).unwrap();
    hundred_group_history(&mut nodes, timers);
    drop(nodes);
    for id in 1..=3 {
        let log = NativeLogStore::recover(
            FileLogIo::open(root.join(id.to_string())).unwrap(),
            identity(id as u128),
            LogLimits::default(),
        )
        .unwrap();
        for g in 1..=100 {
            let core = Raft::recover(
                node(id),
                log.binding(),
                log.state(group(g)).unwrap(),
                log.limits(),
            )
            .unwrap();
            let mut app = Counter::new(10).unwrap();
            app.apply_batch(core.replay_committed()).unwrap();
            assert_eq!(app.read_applied(core.state().commit_index), Ok(10));
        }
    }
    std::fs::remove_dir_all(root).unwrap();
}

#[cfg(feature = "native")]
#[test]
fn failed_shared_barrier_fences_submitted_groups_without_votes_escaping() {
    use voteboat::native::{log_store::*, runtime::*};
    for fault in [Fault::Sync, Fault::PublishBefore, Fault::PublishAfter] {
        let image = ModelIo::default();
        let mut log =
            NativeLogStore::create(image.clone(), identity(1), LogLimits::default()).unwrap();
        append(
            &mut log,
            (1..=2)
                .map(|g| LogMutation::Create(bootstrap(g, 3)))
                .collect(),
        );
        let mut shard = Shard::new(
            owner(log.binding()),
            small_limits(),
            FairScheduler::new(3).unwrap(),
        )
        .unwrap();
        for g in 1..=2 {
            shard
                .register(
                    Raft::recover(
                        node(1),
                        log.binding(),
                        log.state(group(g)).unwrap(),
                        log.limits(),
                    )
                    .unwrap(),
                )
                .unwrap();
            shard.admit(group(g), Event::Campaign).unwrap();
        }
        let mut pending = Vec::new();
        for _ in 0..2 {
            let visit = shard.poll(MonoTime(0)).unwrap().unwrap();
            let effects = shard
                .step_next(visit, MonoTime(0))
                .unwrap()
                .unwrap()
                .result
                .unwrap();
            let [Effect::Persist(update)] = effects.as_slice() else {
                panic!("no vote before sync")
            };
            pending.push((visit, update.clone()));
        }
        let tickets = log
            .append_batch(
                pending
                    .iter()
                    .map(|(_, u)| LogMutation::Update(u.clone()))
                    .collect(),
            )
            .unwrap();
        for ((visit, update), ticket) in pending.iter().zip(&tickets) {
            shard
                .with_core(*visit, |core| core.admit_effect(update, *ticket))
                .unwrap()
                .unwrap();
        }
        image.0.borrow_mut().fault = fault;
        assert!(log.barrier(&tickets).is_err());
        for (visit, _) in pending {
            assert_eq!(shard.finish(visit), Err(RuntimeError::DependencyPending));
            let stopped = shard.stop_group(visit.group).unwrap();
            assert!(stopped.had_active_visit);
            assert_eq!(
                shard.with_core(visit, |_| ()),
                Err(RuntimeError::StaleTicket)
            );
        }
        assert!(shard.is_drained());
        drop(log);
        drop(shard);
        image.0.borrow_mut().power_loss();
        let log = NativeLogStore::recover(image, identity(1), LogLimits::default()).unwrap();
        let a = log.state(group(1)).unwrap();
        let b = log.state(group(2)).unwrap();
        assert_eq!(a.hard_state, b.hard_state);
        assert!(a.hard_state.term <= 1);
        for g in 1..=2 {
            Raft::recover(
                node(1),
                log.binding(),
                log.state(group(g)).unwrap(),
                log.limits(),
            )
            .unwrap();
        }
    }
}

#[test]
fn dropping_one_shard_leaves_the_host_store_and_other_owner_live() {
    let mut log = HostLogStore::new(1);
    append(
        &mut log,
        vec![
            LogMutation::Create(bootstrap(1, 3)),
            LogMutation::Create(bootstrap(2, 3)),
        ],
    );
    let mut a = Shard::new(owner(log.binding()), small_limits(), HostReady::new(3)).unwrap();
    let mut second = owner(log.binding());
    second.lane = ExecutionLaneId::new(2).unwrap();
    let mut b = Shard::new(second, small_limits(), HostReady::new(3)).unwrap();
    a.register(
        Raft::recover(
            node(1),
            log.binding(),
            log.state(group(1)).unwrap(),
            log.limits(),
        )
        .unwrap(),
    )
    .unwrap();
    b.register(
        Raft::recover(
            node(1),
            log.binding(),
            log.state(group(2)).unwrap(),
            log.limits(),
        )
        .unwrap(),
    )
    .unwrap();
    a.admit(group(1), Event::Heartbeat).unwrap();
    let first = a.poll(MonoTime(0)).unwrap().unwrap();
    drop(a); // no store, executor or clock shutdown is implied
    b.admit(group(2), Event::Campaign).unwrap();
    let visit = b.poll(MonoTime(0)).unwrap().unwrap();
    assert_eq!(b.with_core(first, |_| ()), Err(RuntimeError::StaleTicket));
    let effects = b
        .step_next(visit, MonoTime(0))
        .unwrap()
        .unwrap()
        .result
        .unwrap();
    let [Effect::Persist(update)] = effects.as_slice() else {
        panic!()
    };
    let sent = b
        .with_core(visit, |core| persist_effect(core, &mut log, update.clone()))
        .unwrap()
        .unwrap();
    assert_eq!(sent.len(), 2);
    b.finish(visit).unwrap();
    assert_eq!(log.state(group(2)).unwrap().hard_state.term, 1);
    assert_eq!(log.state(group(1)).unwrap().hard_state.term, 0);
}

fn timer_config() -> TimerConfig {
    TimerConfig {
        heartbeat_ms: 4,
        election_min_ms: 20,
        election_spread_ms: 20,
        expirations_per_poll: 3,
    }
}
fn heartbeat(term: u64) -> Message {
    Message {
        group: group(1),
        configuration: ConfigurationId::new(1).unwrap(),
        from: node(2),
        to: node(1),
        sender: HostLogStore::new(2).binding(),
        term,
        context: RequestContext {
            origin: HostLogStore::new(2).binding(),
            sequence: 1,
        },
        rpc: Rpc::Append {
            previous_index: 0,
            previous_term: 0,
            entries: vec![],
            leader_commit: 0,
        },
    }
}
fn host_timed(
    term: u64,
    vote: Option<NodeId>,
) -> (
    TimedShard<HostReady, HostTimers, FixedEntropy>,
    HostLogStore,
) {
    let mut log = HostLogStore::new(1);
    append(&mut log, vec![LogMutation::Create(bootstrap(1, 3))]);
    if term > 0 {
        let state = log.state(group(1)).unwrap();
        let mut mutation = update(&state, term, 0, None);
        if let LogMutation::Update(u) = &mut mutation {
            u.hard_state.voted_for = vote;
        }
        append(&mut log, vec![mutation]);
    }
    let mut shard = Shard::new(owner(log.binding()), small_limits(), HostReady::new(3)).unwrap();
    shard
        .register(
            Raft::recover(
                node(1),
                log.binding(),
                log.state(group(1)).unwrap(),
                log.limits(),
            )
            .unwrap(),
        )
        .unwrap();
    let timers = HostTimers::new(owner(log.binding()), 3);
    (
        TimedShard::new(shard, timers, FixedEntropy(0), timer_config(), MonoTime(0)).unwrap(),
        log,
    )
}
#[test]
fn leader_contact_invalidates_an_election_already_queued_for_dispatch() {
    let (mut runtime, _) = host_timed(1, None);
    let initial = runtime.deadline(group(1)).unwrap();
    runtime
        .admit(group(1), Event::Receive(heartbeat(1)))
        .unwrap();
    let now = MonoTime(initial.deadline.0 + 1);
    assert_eq!(runtime.poll_timers(now).unwrap().admitted, 1);
    let visit = runtime.poll(now).unwrap().unwrap();
    let step = runtime.step_next(visit, now).unwrap().unwrap();
    assert!(step
        .result
        .unwrap()
        .iter()
        .all(|e| matches!(e, Effect::Send(_))));
    runtime.finish(visit).unwrap();
    let fresh = runtime.deadline(group(1)).unwrap();
    assert_ne!(fresh, initial);
    let visit = runtime.poll(now).unwrap().unwrap();
    let stale = runtime.step_next(visit, now).unwrap().unwrap();
    assert!(stale.timer.is_none());
    assert!(stale.result.unwrap().is_empty());
    runtime.finish(visit).unwrap();
    assert_eq!(runtime.core(group(1)).unwrap().state().hard_state.term, 1);
    assert_eq!(runtime.core(group(1)).unwrap().role(), Role::Follower);
    assert_eq!(runtime.deadline(group(1)), Some(fresh));
    runtime.close_admission().unwrap();
    assert!(runtime.deadline(group(1)).is_none());
    assert!(runtime.poll(MonoTime(1000)).unwrap().is_none());
}
#[test]
fn stale_malformed_denied_and_unrelated_messages_do_not_extend_election_deadlines() {
    let (mut runtime, _) = host_timed(2, Some(node(3)));
    let initial = runtime.deadline(group(1));
    let mut vote = heartbeat(2);
    vote.rpc = Rpc::Vote {
        last_index: 0,
        last_term: 0,
    };
    let mut malformed = heartbeat(2);
    malformed.rpc = Rpc::Append {
        previous_index: 99,
        previous_term: 1,
        entries: vec![entry(150, 1, 7)],
        leader_commit: 0,
    };
    let mut foreign = heartbeat(2);
    foreign.configuration = ConfigurationId::new(99).unwrap();
    foreign.rpc = Rpc::ReadProbe; // Read authority still requires equal configurations.
    let mut ack = heartbeat(2);
    ack.context.origin = HostLogStore::new(1).binding();
    ack.rpc = Rpc::Appended {
        success: true,
        matching_index: 0,
    };
    for message in [heartbeat(1), vote, malformed, foreign, ack] {
        runtime.admit(group(1), Event::Receive(message)).unwrap();
        let t = runtime.poll(MonoTime(10)).unwrap().unwrap();
        let _ = runtime.step_next(t, MonoTime(10)).unwrap().unwrap().result;
        runtime.finish(t).unwrap();
        assert_eq!(runtime.deadline(group(1)), initial);
    }
}
#[test]
fn retained_voter_replication_contact_bridges_scope_without_activating_it() {
    let (mut runtime, _) = host_timed(2, Some(node(3)));
    let initial = runtime.deadline(group(1));
    let mut contact = heartbeat(2);
    contact.configuration = ConfigurationId::new(99).unwrap();
    runtime
        .admit(group(1), Event::Receive(contact.clone()))
        .unwrap();
    let visit = runtime.poll(MonoTime(10)).unwrap().unwrap();
    let effects = runtime
        .step_next(visit, MonoTime(10))
        .unwrap()
        .unwrap()
        .result
        .unwrap();
    assert!(matches!(&effects[..], [Effect::Send(message)]
        if message.configuration == contact.configuration
        && matches!(message.rpc, Rpc::Appended { success: true, matching_index: 0 })));
    runtime.finish(visit).unwrap();
    assert_ne!(runtime.deadline(group(1)), initial);
    assert_eq!(
        runtime.core(group(1)).unwrap().membership().id(),
        ConfigurationId::new(1).unwrap()
    );
    assert_eq!(
        runtime.core(group(1)).unwrap().state().hard_state.voted_for,
        Some(node(3))
    );
}
#[test]
fn delayed_durable_vote_completion_resets_and_fences_a_queued_expiration() {
    let (mut runtime, mut log) = host_timed(2, None);
    let old = runtime.deadline(group(1)).unwrap();
    let mut vote = heartbeat(2);
    vote.rpc = Rpc::Vote {
        last_index: 0,
        last_term: 0,
    };
    runtime.admit(group(1), Event::Receive(vote)).unwrap();
    let t = runtime.poll(MonoTime(10)).unwrap().unwrap();
    let effects = runtime
        .step_next(t, MonoTime(10))
        .unwrap()
        .unwrap()
        .result
        .unwrap();
    let [Effect::Persist(update)] = effects.as_slice() else {
        panic!()
    };
    assert_eq!(runtime.deadline(group(1)), Some(old));
    let now = MonoTime(old.deadline.0 + 2);
    assert_eq!(runtime.poll_timers(now).unwrap().admitted, 1);
    let tickets = log
        .append_batch(vec![LogMutation::Update(update.clone())])
        .unwrap();
    runtime
        .with_core(t, now, |core| core.admit_effect(update, tickets[0]))
        .unwrap()
        .unwrap();
    let completion = log.barrier(&tickets).unwrap();
    let effects = runtime
        .with_core(t, now, |core| core.complete(&completion))
        .unwrap()
        .unwrap();
    assert!(matches!(
        effects.as_slice(),
        [Effect::Send(Message {
            rpc: Rpc::Voted { granted: true },
            ..
        })]
    ));
    assert_ne!(runtime.deadline(group(1)), Some(old));
    runtime.finish(t).unwrap();
    let stale = runtime.poll(now).unwrap().unwrap();
    assert!(runtime
        .step_next(stale, now)
        .unwrap()
        .unwrap()
        .result
        .unwrap()
        .is_empty());
    runtime.finish(stale).unwrap();
    assert_eq!(runtime.core(group(1)).unwrap().state().hard_state.term, 2);
}
#[test]
fn expiration_backpressure_retains_one_token_and_close_cancels_queued_campaigns() {
    let (mut runtime, _) = host_timed(0, None);
    let old = runtime.deadline(group(1)).unwrap();
    for _ in 0..8 {
        runtime.admit(group(1), Event::Heartbeat).unwrap();
    }
    let now = MonoTime(old.deadline.0 + 1);
    let p = runtime.poll_timers(now).unwrap();
    assert_eq!(p.blocked, 1);
    assert_eq!(p.max_lateness_ms, 1);
    for _ in 0..100 {
        assert_eq!(runtime.poll_timers(now).unwrap().blocked, 1);
        assert_eq!(runtime.pending_expirations(), 1);
    }
    let visit = runtime.poll(now).unwrap().unwrap();
    runtime
        .step_next(visit, now)
        .unwrap()
        .unwrap()
        .result
        .unwrap();
    runtime.finish(visit).unwrap();
    assert_eq!(
        runtime.poll_timers(MonoTime(now.0 + 3)).unwrap().admitted,
        1
    );
    assert_eq!(runtime.pending_expirations(), 0);
    runtime.close_admission().unwrap();
    while let Some(visit) = runtime.poll(MonoTime(now.0 + 3)).unwrap() {
        assert!(runtime
            .step_next(visit, MonoTime(now.0 + 3))
            .unwrap()
            .unwrap()
            .result
            .unwrap()
            .is_empty());
        runtime.finish(visit).unwrap();
    }
    assert!(runtime.is_drained());
    assert_eq!(runtime.core(group(1)).unwrap().state().hard_state.term, 0);
}

struct AutoNode<L: LogStore, Q: ReadyScheduler, T: TimerService, E: ElectionEntropy> {
    runtime: TimedShard<Q, T, E>,
    log: L,
    apps: BTreeMap<GroupIdentity, Counter>,
    messages: VecDeque<Message>,
    reads: Vec<i64>,
}
impl<L: LogStore, Q: ReadyScheduler, T: TimerService, E: ElectionEntropy> AutoNode<L, Q, T, E> {
    fn new(
        id: u64,
        mut log: L,
        ready: Q,
        make_timers: impl FnOnce(RuntimeOwner) -> T,
        entropy: E,
        now: MonoTime,
        create: bool,
    ) -> Self {
        if create {
            append(
                &mut log,
                (1..=2)
                    .map(|g| LogMutation::Create(bootstrap(g, 3)))
                    .collect(),
            );
        }
        let mut shard = Shard::new(owner(log.binding()), small_limits(), ready).unwrap();
        let mut apps = BTreeMap::new();
        for g in 1..=2 {
            let core = Raft::recover(
                node(id),
                log.binding(),
                log.state(group(g)).unwrap(),
                log.limits(),
            )
            .unwrap();
            let mut app = Counter::new(10).unwrap();
            app.apply_batch(core.replay_committed()).unwrap();
            shard.register(core).unwrap();
            apps.insert(group(g), app);
        }
        let runtime = TimedShard::new(
            shard,
            make_timers(owner(log.binding())),
            entropy,
            timer_config(),
            now,
        )
        .unwrap();
        Self {
            runtime,
            log,
            apps,
            messages: VecDeque::new(),
            reads: Vec::new(),
        }
    }
    fn drain(&mut self, now: MonoTime) -> bool {
        let mut progress = false;
        for _ in 0..16 {
            let Some(visit) = self.runtime.poll(now).unwrap() else {
                break;
            };
            let step = self.runtime.step_next(visit, now).unwrap().unwrap();
            let effects = match step.result {
                Ok(e) => e,
                Err(RaftError::WrongIdentity | RaftError::InvalidMessage) => vec![],
                Err(e) => panic!("unexpected event failure: {e:?}"),
            };
            let mut effects = VecDeque::from(effects);
            while let Some(effect) = effects.pop_front() {
                assert!(effects.len() < 128);
                match effect {
                    Effect::Persist(update) => effects.extend(
                        self.runtime
                            .with_core(visit, now, |core| {
                                persist_effect(core, &mut self.log, update)
                            })
                            .unwrap()
                            .unwrap(),
                    ),
                    Effect::Send(m) => {
                        assert!(self.messages.len() < 512);
                        self.messages.push_back(m);
                    }
                    Effect::Committed(entries) => {
                        self.apps
                            .get_mut(&visit.group)
                            .unwrap()
                            .apply_batch(&entries)
                            .unwrap();
                    }
                    Effect::ReadReady(barrier) => {
                        let app = &self.apps[&visit.group];
                        self.reads.push(
                            self.runtime
                                .with_core(visit, now, |core| {
                                    read_at_barrier(core, &barrier, app, ())
                                })
                                .unwrap()
                                .unwrap(),
                        );
                    }
                    _ => panic!("this timer history does not compact"),
                }
            }
            self.runtime.finish(visit).unwrap();
            progress = true;
        }
        progress
    }
}
fn pump_auto<L: LogStore, Q: ReadyScheduler, T: TimerService, E: ElectionEntropy>(
    nodes: &mut [AutoNode<L, Q, T, E>],
    now: MonoTime,
    partition: Option<usize>,
    delayed: &mut Vec<Message>,
) {
    for _ in 0..10000 {
        let mut progress = false;
        for n in nodes.iter_mut() {
            progress |= n.drain(now);
        }
        let mut messages = VecDeque::new();
        for n in nodes.iter_mut() {
            messages.append(&mut n.messages);
        }
        assert!(messages.len() < 512);
        while let Some(m) = messages.pop_front() {
            if m.group == group(1)
                && partition
                    .is_some_and(|p| m.from.get() as usize == p + 1 || m.to.get() as usize == p + 1)
            {
                if delayed.len() < 512 {
                    delayed.push(m);
                }
                continue;
            }
            nodes[m.to.get() as usize - 1]
                .runtime
                .admit(m.group, Event::Receive(m))
                .unwrap();
            progress = true;
        }
        if !progress {
            return;
        }
    }
    panic!("automatic timer history failed to converge");
}
fn automatic_history<L: LogStore, Q: ReadyScheduler, T: TimerService, E: ElectionEntropy>(
    nodes: &mut Vec<AutoNode<L, Q, T, E>>,
    restart: impl FnOnce(usize, AutoNode<L, Q, T, E>, MonoTime) -> AutoNode<L, Q, T, E>,
) {
    let mut delayed = Vec::new();
    for ms in 0..=100 {
        pump_auto(nodes, MonoTime(ms), None, &mut delayed);
    }
    let leader = nodes
        .iter()
        .position(|n| n.runtime.core(group(1)).unwrap().role() == Role::Leader)
        .unwrap();
    assert_eq!(
        nodes
            .iter()
            .filter(|n| n.runtime.core(group(1)).unwrap().role() == Role::Leader)
            .count(),
        1
    );
    let other = nodes
        .iter()
        .position(|n| n.runtime.core(group(2)).unwrap().role() == Role::Leader)
        .unwrap();
    let healthy_term = nodes[other]
        .runtime
        .core(group(2))
        .unwrap()
        .state()
        .hard_state
        .term;
    nodes[leader].runtime.admit(group(1), proposal(1)).unwrap();
    pump_auto(nodes, MonoTime(100), None, &mut delayed);
    for n in nodes.iter() {
        assert_eq!(n.apps[&group(1)].read_applied(2), Ok(7));
    }
    for ms in 101..=250 {
        pump_auto(nodes, MonoTime(ms), Some(leader), &mut delayed);
    }
    let replacement = nodes
        .iter()
        .enumerate()
        .position(|(i, n)| i != leader && n.runtime.core(group(1)).unwrap().role() == Role::Leader)
        .unwrap();
    assert!(
        nodes[replacement]
            .runtime
            .core(group(1))
            .unwrap()
            .state()
            .hard_state
            .term
            > nodes[leader]
                .runtime
                .core(group(1))
                .unwrap()
                .state()
                .hard_state
                .term
    );
    assert_eq!(
        nodes[other]
            .runtime
            .core(group(2))
            .unwrap()
            .state()
            .hard_state
            .term,
        healthy_term
    );
    nodes[leader]
        .runtime
        .admit(
            group(1),
            Event::Read {
                request: ReadRequestId::new(1).unwrap(),
            },
        )
        .unwrap();
    nodes[replacement]
        .runtime
        .admit(
            group(1),
            Event::Propose {
                operation: OperationId::new(2).unwrap(),
                bytes: 3i64.to_le_bytes().to_vec(),
            },
        )
        .unwrap();
    nodes[other].runtime.admit(group(2), proposal(1)).unwrap();
    pump_auto(nodes, MonoTime(250), Some(leader), &mut delayed);
    assert!(nodes[leader].reads.is_empty());
    for (i, n) in nodes.iter().enumerate() {
        if i != leader {
            assert_eq!(
                n.apps[&group(1)]
                    .read_applied(n.runtime.core(group(1)).unwrap().state().commit_index),
                Ok(10)
            );
        }
        assert_eq!(
            n.apps[&group(2)].read_applied(n.runtime.core(group(2)).unwrap().state().commit_index),
            Ok(7)
        );
    }
    let old = nodes.remove(leader);
    nodes.insert(leader, restart(leader, old, MonoTime(250)));
    // Delayed old requests and replies cross recovery; some refer to the old
    // store session and none can replace a current-term authority check.
    for message in delayed.drain(..).take(8) {
        for _ in 0..2 {
            nodes[message.to.get() as usize - 1]
                .runtime
                .admit(message.group, Event::Receive(message.clone()))
                .unwrap();
        }
        pump_auto(nodes, MonoTime(250), None, &mut vec![]);
    }
    for ms in 251..=400 {
        pump_auto(nodes, MonoTime(ms), None, &mut delayed);
    }
    assert_eq!(
        nodes
            .iter()
            .filter(|n| n.runtime.core(group(1)).unwrap().role() == Role::Leader)
            .count(),
        1
    );
    let current = nodes
        .iter()
        .position(|n| n.runtime.core(group(1)).unwrap().role() == Role::Leader)
        .unwrap();
    nodes[current].runtime.admit(group(1), proposal(1)).unwrap();
    nodes[current]
        .runtime
        .admit(
            group(1),
            Event::Read {
                request: ReadRequestId::new(2).unwrap(),
            },
        )
        .unwrap();
    pump_auto(nodes, MonoTime(400), None, &mut delayed);
    assert_eq!(nodes[current].reads.last(), Some(&10));
    for n in nodes.iter() {
        assert_eq!(
            n.apps[&group(1)].read_applied(n.runtime.core(group(1)).unwrap().state().commit_index),
            Ok(10)
        );
    }
}
#[test]
fn virtual_time_elects_replaces_and_recovers_leaders_with_host_providers() {
    let mut nodes = (1..=3)
        .map(|id| {
            AutoNode::new(
                id,
                HostLogStore::new(id as u128),
                HostReady::new(3),
                |owner| HostTimers::new(owner, 3),
                FixedEntropy((id - 1) * 8),
                MonoTime(0),
                true,
            )
        })
        .collect::<Vec<_>>();
    automatic_history(&mut nodes, |id, old, now| {
        let mut log = old.log;
        log.binding.session = StoreSession::new(2).unwrap();
        log.accepted = log.durable.clone();
        log.pending.clear();
        AutoNode::new(
            id as u64 + 1,
            log,
            HostReady::new(3),
            |owner| HostTimers::new(owner, 3),
            FixedEntropy(id as u64 * 8),
            now,
            false,
        )
    });
}
#[cfg(feature = "native")]
#[test]
fn virtual_time_leader_loss_and_recovery_use_native_timers_and_wals() {
    use voteboat::native::{log_store::*, runtime::*};
    let images = (1..=3).map(|_| ModelIo::default()).collect::<Vec<_>>();
    let mut nodes = (1..=3)
        .map(|id| {
            AutoNode::new(
                id,
                NativeLogStore::create(
                    images[id as usize - 1].clone(),
                    identity(id as u128),
                    LogLimits::default(),
                )
                .unwrap(),
                FairScheduler::new(3).unwrap(),
                |owner| DeadlineQueue::new(owner, 3).unwrap(),
                JitterEntropy::new(id),
                MonoTime(0),
                true,
            )
        })
        .collect::<Vec<_>>();
    automatic_history(&mut nodes, |id, old, now| {
        drop(old);
        images[id].0.borrow_mut().power_loss();
        let log = NativeLogStore::recover(
            images[id].clone(),
            identity(id as u128 + 1),
            LogLimits::default(),
        )
        .unwrap();
        AutoNode::new(
            id as u64 + 1,
            log,
            FairScheduler::new(3).unwrap(),
            |owner| DeadlineQueue::new(owner, 3).unwrap(),
            JitterEntropy::new(id as u64 + 1),
            now,
            false,
        )
    });
}

#[cfg(feature = "native")]
#[test]
fn automatic_timer_history_reopens_three_actual_native_files() {
    use voteboat::native::{log_store::*, runtime::*};
    let root = std::env::temp_dir().join(format!("voteboat-auto-runtime-{}", std::process::id()));
    let _ = std::fs::remove_dir_all(&root);
    std::fs::create_dir_all(&root).unwrap();
    let mut nodes = (1..=3)
        .map(|id| {
            AutoNode::new(
                id,
                NativeLogStore::create(
                    FileLogIo::create(root.join(id.to_string())).unwrap(),
                    identity(id as u128),
                    LogLimits::default(),
                )
                .unwrap(),
                FairScheduler::new(3).unwrap(),
                |owner| DeadlineQueue::new(owner, 3).unwrap(),
                JitterEntropy::new(id),
                MonoTime(0),
                true,
            )
        })
        .collect::<Vec<_>>();
    automatic_history(&mut nodes, |id, old, now| {
        drop(old);
        let log = NativeLogStore::recover(
            FileLogIo::open(root.join((id + 1).to_string())).unwrap(),
            identity(id as u128 + 1),
            LogLimits::default(),
        )
        .unwrap();
        AutoNode::new(
            id as u64 + 1,
            log,
            FairScheduler::new(3).unwrap(),
            |owner| DeadlineQueue::new(owner, 3).unwrap(),
            JitterEntropy::new(id as u64 + 1),
            now,
            false,
        )
    });
    drop(nodes);
    std::fs::remove_dir_all(root).unwrap();
}

struct FailingTimers {
    inner: HostTimers,
    fail: Rc<Cell<bool>>,
}
impl TimerService for FailingTimers {
    fn owner(&self) -> RuntimeOwner {
        self.inner.owner()
    }
    fn capacity(&self) -> usize {
        self.inner.capacity()
    }
    fn register(
        &mut self,
        g: GroupIdentity,
        k: TimerKind,
        d: MonoTime,
    ) -> Result<TimerToken, RuntimeError> {
        if self.fail.get() {
            Err(RuntimeError::SchedulerContract)
        } else {
            self.inner.register(g, k, d)
        }
    }
    fn cancel(&mut self, t: TimerToken) -> Result<(), RuntimeError> {
        self.inner.cancel(t)
    }
    fn poll(&mut self, n: MonoTime, l: usize) -> Result<Vec<Expiration>, RuntimeError> {
        self.inner.poll(n, l)
    }
    fn len(&self) -> usize {
        self.inner.len()
    }
}
#[test]
fn timer_provider_failure_latches_and_still_allows_explicit_stop() {
    let mut log = HostLogStore::new(1);
    append(&mut log, vec![LogMutation::Create(bootstrap(1, 3))]);
    let mut shard = Shard::new(owner(log.binding()), small_limits(), HostReady::new(3)).unwrap();
    shard
        .register(
            Raft::recover(
                node(1),
                log.binding(),
                log.state(group(1)).unwrap(),
                log.limits(),
            )
            .unwrap(),
        )
        .unwrap();
    let fail = Rc::new(Cell::new(false));
    let timers = FailingTimers {
        inner: HostTimers::new(owner(log.binding()), 3),
        fail: fail.clone(),
    };
    let mut runtime =
        TimedShard::new(shard, timers, FixedEntropy(0), timer_config(), MonoTime(0)).unwrap();
    let mut message = heartbeat(0);
    message.term = 1;
    runtime.admit(group(1), Event::Receive(message)).unwrap();
    let visit = runtime.poll(MonoTime(10)).unwrap().unwrap();
    fail.set(true);
    assert!(matches!(
        runtime.step_next(visit, MonoTime(10)),
        Err(RuntimeError::SchedulerContract)
    ));
    // No dependent effect escaped when its timer replacement failed. The core
    // may have accepted work; the host resolves it by explicit stop/recovery.
    assert_eq!(
        runtime.admit(group(1), proposal(1)).unwrap_err().reason,
        RuntimeError::SchedulerContract
    );
    assert_eq!(
        runtime.poll(MonoTime(100)),
        Err(RuntimeError::SchedulerContract)
    );
    let stopped = runtime.stop_group(group(1)).unwrap();
    assert!(stopped.had_active_visit);
    assert!(runtime.core(group(1)).unwrap().is_fenced());
    assert!(runtime.is_drained());
    assert!(runtime.deadline(group(1)).is_none());
}

fn delayed_auto_history<L: LogStore, Q: ReadyScheduler, T: TimerService, E: ElectionEntropy>(
    nodes: &mut [AutoNode<L, Q, T, E>],
) {
    let mut network = VecDeque::<(u64, Message)>::new();
    let mut chosen = None;
    for ms in 0..=1000 {
        if ms == 500 {
            let leader = nodes
                .iter()
                .position(|n| n.runtime.core(group(1)).unwrap().role() == Role::Leader)
                .unwrap();
            assert!(
                nodes[leader]
                    .runtime
                    .core(group(1))
                    .unwrap()
                    .state()
                    .commit_index
                    > 0,
                "heartbeat retries must not invalidate slower acknowledgements"
            );
            let term = nodes[leader]
                .runtime
                .core(group(1))
                .unwrap()
                .state()
                .hard_state
                .term;
            chosen = Some((leader, term));
            nodes[leader].runtime.admit(group(1), proposal(1)).unwrap();
        }
        if ms == 800 {
            nodes[chosen.unwrap().0]
                .runtime
                .admit(
                    group(1),
                    Event::Read {
                        request: ReadRequestId::new(1).unwrap(),
                    },
                )
                .unwrap();
        }
        while network.front().is_some_and(|(due, _)| *due <= ms) {
            let (_, message) = network.pop_front().unwrap();
            nodes[message.to.get() as usize - 1]
                .runtime
                .admit(message.group, Event::Receive(message))
                .unwrap();
        }
        for n in nodes.iter_mut() {
            n.drain(MonoTime(ms));
            while let Some(message) = n.messages.pop_front() {
                assert!(network.len() < 512);
                network.push_back((ms + 6, message));
            }
        }
    }
    let (leader, term) = chosen.unwrap();
    assert_eq!(
        nodes[leader]
            .runtime
            .core(group(1))
            .unwrap()
            .state()
            .hard_state
            .term,
        term
    );
    assert_eq!(nodes[leader].reads, [7]);
    for n in nodes.iter() {
        assert_eq!(
            n.apps[&group(1)].read_applied(n.runtime.core(group(1)).unwrap().state().commit_index),
            Ok(7)
        );
    }
}
#[test]
fn network_round_trip_longer_than_heartbeat_interval_still_commits_and_reads() {
    let mut nodes = (1..=3)
        .map(|id| {
            AutoNode::new(
                id,
                HostLogStore::new(id as u128),
                HostReady::new(3),
                |owner| HostTimers::new(owner, 3),
                FixedEntropy((id - 1) * 8),
                MonoTime(0),
                true,
            )
        })
        .collect::<Vec<_>>();
    delayed_auto_history(&mut nodes);
}
#[cfg(feature = "native")]
#[test]
fn native_timer_replication_progress_survives_multiple_heartbeat_retries() {
    use voteboat::native::{log_store::*, runtime::*};
    let mut nodes = (1..=3)
        .map(|id| {
            AutoNode::new(
                id,
                NativeLogStore::create(
                    ModelIo::default(),
                    identity(id as u128),
                    LogLimits::default(),
                )
                .unwrap(),
                FairScheduler::new(3).unwrap(),
                |owner| DeadlineQueue::new(owner, 3).unwrap(),
                JitterEntropy::new(id),
                MonoTime(0),
                true,
            )
        })
        .collect::<Vec<_>>();
    delayed_auto_history(&mut nodes);
}

#[test]
fn enrolled_learner_has_no_election_timer_and_still_processes_durable_replication() {
    use voteboat::membership::*;
    let mut log = HostLogStore::new(4);
    let b = bootstrap(1, 3);
    append(&mut log, vec![LogMutation::Create(b.clone())]);
    let state = log.state(group(1)).unwrap();
    append(
        &mut log,
        vec![update(
            &state,
            1,
            1,
            Some(Suffix {
                from: 1,
                entries: vec![LogEntry {
                    index: 1,
                    term: 1,
                    payload: EntryPayload::Configuration(Box::new(ConfigurationRecord {
                        operation: OperationId::new(100).unwrap(),
                        expected: b.configuration,
                        change: ConfigurationChange::Learners(
                            Configuration::new(
                                ConfigurationId::new(2).unwrap(),
                                b.policy.clone(),
                                b.voter_stores.clone(),
                                [(node(4), identity(4))].into(),
                            )
                            .unwrap(),
                        ),
                    })),
                }],
            }),
        )],
    );
    let core = Raft::recover_learner(
        node(4),
        log.binding(),
        log.state(group(1)).unwrap(),
        log.limits(),
    )
    .unwrap();
    let mut shard = Shard::new(owner(log.binding()), small_limits(), HostReady::new(3)).unwrap();
    shard.register(core).unwrap();
    let timers = HostTimers::new(owner(log.binding()), 3);
    let mut runtime =
        TimedShard::new(shard, timers, FixedEntropy(0), timer_config(), MonoTime(0)).unwrap();
    assert!(runtime.deadline(group(1)).is_none());
    assert_eq!(runtime.poll_timers(MonoTime(100)).unwrap().admitted, 0);
    let sender = HostLogStore::new(1).binding();
    let message = Message {
        group: group(1),
        configuration: ConfigurationId::new(2).unwrap(),
        from: node(1),
        sender,
        to: node(4),
        term: 1,
        context: RequestContext {
            origin: sender,
            sequence: 1,
        },
        rpc: Rpc::Append {
            previous_index: 1,
            previous_term: 1,
            entries: vec![LogEntry {
                index: 2,
                term: 1,
                payload: EntryPayload::Noop,
            }],
            leader_commit: 2,
        },
    };
    runtime.admit(group(1), Event::Receive(message)).unwrap();
    let visit = runtime.poll(MonoTime(100)).unwrap().unwrap();
    let effects = runtime
        .step_next(visit, MonoTime(100))
        .unwrap()
        .unwrap()
        .result
        .unwrap();
    let [Effect::Persist(update)] = effects.as_slice() else {
        panic!()
    };
    let tickets = log
        .append_batch(vec![LogMutation::Update(update.clone())])
        .unwrap();
    runtime
        .with_core(visit, MonoTime(100), |core| {
            core.admit_effect(update, tickets[0])
        })
        .unwrap()
        .unwrap();
    let completion = log.barrier(&tickets).unwrap();
    let effects = runtime
        .with_core(visit, MonoTime(100), |core| core.complete(&completion))
        .unwrap()
        .unwrap();
    assert!(effects.iter().any(|e| matches!(
        e,
        Effect::Send(Message {
            rpc: Rpc::Appended {
                success: true,
                matching_index: 2
            },
            ..
        })
    )));
    runtime.finish(visit).unwrap();
    assert!(runtime.deadline(group(1)).is_none());
    assert_eq!(runtime.poll_timers(MonoTime(10000)).unwrap().admitted, 0);
    assert_eq!(runtime.core(group(1)).unwrap().state().commit_index, 2);
    assert!(!runtime.core(group(1)).unwrap().local_voter());
}

#[test]
fn prospective_reservation_peeks_the_exact_queue_choice_without_consumption() {
    use voteboat::membership::*;
    let (mut shard, _) = host_shard(HostReady::new(3));
    let core = shard.core(group(1)).unwrap();
    let before = core.state().clone();
    let next = Configuration::new(
        ConfigurationId::new(2).unwrap(),
        before.bootstrap.policy.clone(),
        before.bootstrap.voter_stores.clone(),
        [(
            node(4),
            StoreIdentity {
                id: StoreId::new(4).unwrap(),
                incarnation: StoreIncarnation::new(1).unwrap(),
            },
        )]
        .into_iter()
        .collect(),
    )
    .unwrap();
    let incoming = Event::Receive(Message {
        group: group(1),
        configuration: before.bootstrap.configuration,
        from: node(2),
        to: node(1),
        sender: core.storage_binding(),
        term: 1,
        context: RequestContext {
            origin: core.storage_binding(),
            sequence: 1,
        },
        rpc: Rpc::Append {
            previous_index: 0,
            previous_term: 0,
            leader_commit: 0,
            entries: vec![LogEntry {
                index: 1,
                term: 1,
                payload: EntryPayload::Configuration(Box::new(ConfigurationRecord {
                    operation: OperationId::new(700).unwrap(),
                    expected: before.bootstrap.configuration,
                    change: ConfigurationChange::Learners(next),
                })),
            }],
        },
    });
    let ordinary = core
        .effect_reservation(small_limits().max_event_bytes)
        .unwrap();
    let growth = core
        .event_effect_reservation(&incoming, small_limits().max_event_bytes)
        .unwrap();
    assert!(growth > ordinary);
    shard.admit(group(1), incoming).unwrap();
    shard.admit(group(1), Event::Heartbeat).unwrap();
    shard.admit(group(1), Event::Heartbeat).unwrap();
    // Two control turns precede data. Repeated inspection must not move that cursor.
    for expected in [ordinary, ordinary, growth] {
        let visit = shard.poll(MonoTime(0)).unwrap().unwrap();
        let usage = shard.usage();
        for _ in 0..3 {
            assert_eq!(
                shard.next_effect_reservation(visit, MonoTime(0)).unwrap(),
                Some(expected)
            );
        }
        assert_eq!(shard.usage(), usage);
        assert_eq!(shard.core(group(1)).unwrap().state(), &before);
        let step = shard.step_next(visit, MonoTime(0)).unwrap().unwrap();
        if expected == growth {
            assert_eq!(step.result, Err(RaftError::InvalidMessage));
        } else {
            assert!(step.result.unwrap().is_empty());
        }
        assert_eq!(
            shard.next_effect_reservation(visit, MonoTime(0)).unwrap(),
            None
        );
        shard.finish(visit).unwrap();
        assert_eq!(
            shard.next_effect_reservation(visit, MonoTime(0)),
            Err(RuntimeError::StaleTicket)
        );
    }
    assert!(shard.is_drained());
    shard.admit(group(1), Event::Heartbeat).unwrap();
    let visit = shard.poll(MonoTime(0)).unwrap().unwrap();
    assert_eq!(
        shard.next_effect_reservation(visit, MonoTime(5)).unwrap(),
        None
    );
    assert!(shard.step_next(visit, MonoTime(5)).unwrap().is_none());
    shard.finish(visit).unwrap();
    assert_eq!(
        shard.usage().items,
        1,
        "expired inspection leaves input queued"
    );
}
