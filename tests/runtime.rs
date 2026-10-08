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
    let limits = ShardLimits {
        group_bytes: 1000,
        group_control_bytes: 250,
        max_event_bytes: 512,
        visit_bytes: 512,
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
    assert!(shard.usage().bytes <= 1000);
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
            messages.append(&mut n.messages);
        }
        assert!(messages.len() <= 8192);
        while let Some(m) = messages.pop_front() {
            nodes[m.to.get() as usize - 1]
                .shard
                .admit(m.group, Event::Receive(m))
                .unwrap();
            progress = true;
        }
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
