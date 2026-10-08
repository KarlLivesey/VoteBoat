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
use std::collections::{BTreeMap, VecDeque};
use support::*;
use voteboat::{application::*, identity::*, log::*, raft::*, runtime::*, worker::*};
struct Ready(VecDeque<GroupIdentity>);
impl ReadyScheduler for Ready {
    fn capacity(&self) -> usize {
        100
    }
    fn enqueue(&mut self, g: GroupIdentity) -> Result<(), RuntimeError> {
        if !self.0.contains(&g) {
            self.0.push_back(g);
        }
        Ok(())
    }
    fn pop(&mut self) -> Option<GroupIdentity> {
        self.0.pop_front()
    }
    fn cancel(&mut self, g: GroupIdentity) {
        self.0.retain(|x| *x != g);
    }
    fn len(&self) -> usize {
        self.0.len()
    }
}
struct Timers {
    owner: RuntimeOwner,
    sequence: u64,
    entries: BTreeMap<u64, TimerToken>,
}
impl TimerService for Timers {
    fn owner(&self) -> RuntimeOwner {
        self.owner
    }
    fn capacity(&self) -> usize {
        100
    }
    fn register(
        &mut self,
        group: GroupIdentity,
        kind: TimerKind,
        deadline: MonoTime,
    ) -> Result<TimerToken, RuntimeError> {
        self.sequence += 1;
        let token = TimerToken {
            owner: self.owner,
            group,
            kind,
            deadline,
            sequence: self.sequence,
        };
        self.entries.insert(token.sequence, token);
        Ok(token)
    }
    fn cancel(&mut self, token: TimerToken) -> Result<(), RuntimeError> {
        self.entries.remove(&token.sequence);
        Ok(())
    }
    fn poll(&mut self, now: MonoTime, limit: usize) -> Result<Vec<Expiration>, RuntimeError> {
        let mut due = self
            .entries
            .values()
            .filter(|t| t.deadline <= now)
            .copied()
            .collect::<Vec<_>>();
        due.sort_by_key(|t| (t.deadline, t.sequence));
        due.truncate(limit);
        Ok(due
            .into_iter()
            .map(|token| {
                self.entries.remove(&token.sequence);
                Expiration {
                    token,
                    late_ms: now.0 - token.deadline.0,
                }
            })
            .collect())
    }
    fn len(&self) -> usize {
        self.entries.len()
    }
}
struct Entropy(u64);
impl ElectionEntropy for Entropy {
    fn sample(&mut self) -> u64 {
        self.0 += 1;
        self.0
    }
}
type Owner = EffectOwner<Ready, Timers, Entropy>;
fn timed<L: LogStore>(
    id: u64,
    store: &mut L,
    count: u128,
    voters: u64,
) -> TimedShard<Ready, Timers, Entropy> {
    append(
        store,
        (1..=count)
            .map(|g| LogMutation::Create(bootstrap(g, voters)))
            .collect(),
    );
    let owner = RuntimeOwner {
        store: store.binding(),
        lane: ExecutionLaneId::new(1).unwrap(),
        generation: RuntimeGeneration::new(1).unwrap(),
    };
    let mut shard = Shard::new(
        owner,
        ShardLimits {
            max_groups: 100,
            visit_items: 1,
            ..ShardLimits::default()
        },
        Ready(VecDeque::new()),
    )
    .unwrap();
    for g in 1..=count {
        shard
            .register(
                Raft::recover(
                    node(id),
                    store.binding(),
                    store.state(group(g)).unwrap(),
                    store.limits(),
                )
                .unwrap(),
            )
            .unwrap();
    }
    TimedShard::new(
        shard,
        Timers {
            owner,
            sequence: 0,
            entries: BTreeMap::new(),
        },
        Entropy(id * 17),
        TimerConfig::default(),
        MonoTime(0),
    )
    .unwrap()
}
struct HostWorker {
    store: HostLogStore,
    binding: WorkerBinding,
    sequence: u64,
    accepted: Option<(WorkerTicket, Vec<PersistUnit>)>,
    written: Option<(WorkerTicket, Vec<VisitTicket>, Vec<LogTicket>)>,
    reject: bool,
    fail: bool,
    closed: bool,
}
impl HostWorker {
    fn new(store: HostLogStore) -> Self {
        let binding = WorkerBinding {
            store: store.binding(),
            generation: StorageWorkerGeneration::new(1).unwrap(),
        };
        Self {
            store,
            binding,
            sequence: 0,
            accepted: None,
            written: None,
            reject: false,
            fail: false,
            closed: false,
        }
    }
}
impl PersistenceWorker for HostWorker {
    fn binding(&self) -> WorkerBinding {
        self.binding
    }
    fn limits(&self) -> WorkerLimits {
        WorkerLimits::default()
    }
    fn usage(&self) -> WorkerUsage {
        WorkerUsage {
            requests: usize::from(self.accepted.is_some() || self.written.is_some()),
            units: self.accepted.as_ref().map_or_else(
                || self.written.as_ref().map_or(0, |(_, v, _)| v.len()),
                |(_, u)| u.len(),
            ),
            bytes: 0,
        }
    }
    fn submit(&mut self, units: Vec<PersistUnit>) -> Result<WorkerTicket, WorkerRejected> {
        let reason = if self.closed {
            Some(WorkerError::Closed)
        } else if self.reject || !self.is_drained() {
            Some(WorkerError::Overloaded)
        } else {
            batch_cost(&units, units.capacity(), self.limits()).err()
        };
        if let Some(reason) = reason {
            return Err(WorkerRejected { reason, units });
        }
        self.sequence += 1;
        let ticket = WorkerTicket {
            binding: self.binding,
            sequence: self.sequence,
        };
        self.accepted = Some((ticket, units));
        Ok(ticket)
    }
    fn poll(&mut self, limit: usize) -> Vec<WorkerEvent> {
        if limit == 0 {
            return vec![];
        }
        if let Some((request, visits, tickets)) = self.written.take() {
            return vec![if self.fail {
                WorkerEvent::Failed {
                    request,
                    visits,
                    error: voteboat::contracts::StorageError::Rejected("injected failure"),
                }
            } else {
                WorkerEvent::Durable {
                    request,
                    visits,
                    completion: self.store.barrier(&tickets).unwrap(),
                }
            }];
        }
        if let Some((request, units)) = self.accepted.take() {
            let visits = units.iter().map(|u| u.visit).collect::<Vec<_>>();
            let tickets = self
                .store
                .append_batch(
                    units
                        .into_iter()
                        .map(|u| LogMutation::Update(u.update))
                        .collect(),
                )
                .unwrap();
            let admissions = visits
                .iter()
                .copied()
                .zip(tickets.iter().copied())
                .collect();
            self.written = Some((request, visits, tickets));
            return vec![WorkerEvent::Written {
                request,
                admissions,
            }];
        }
        vec![]
    }
    fn close(&mut self) {
        self.closed = true;
    }
}
fn single(count: u128) -> (Owner, HostWorker) {
    let mut store = HostLogStore::new(1);
    let runtime = timed(1, &mut store, count, 1);
    let worker = HostWorker::new(store);
    (
        EffectOwner::new(runtime, worker.binding(), EffectOwnerLimits::default()).unwrap(),
        worker,
    )
}
fn pump(
    owner: &mut Owner,
    worker: &mut impl PersistenceWorker,
    app: &mut BTreeMap<GroupIdentity, Counter>,
    now: MonoTime,
) {
    for _ in 0..10000 {
        for event in worker.poll(1) {
            owner.deliver_worker(event, now).unwrap();
        }
        for step in owner.advance(now, 100).unwrap() {
            assert!(step.error.is_none(), "step: {:?}", step.error);
        }
        let mut persists = Vec::new();
        while let Some(lease) = owner.take_effect().unwrap() {
            match &lease.effect {
                Effect::Persist(_) => persists.push(lease),
                Effect::Committed(entries) => {
                    let a = app
                        .entry(lease.ticket.visit.group)
                        .or_insert_with(|| Counter::new(100).unwrap());
                    a.apply_batch(entries).unwrap();
                    owner.release(lease, a.applied_index(), now).unwrap();
                }
                _ => panic!("unexpected single-voter effect"),
            }
        }
        if !persists.is_empty() {
            owner.submit_persists(worker, persists, now).unwrap();
        }
        if owner.is_drained() && worker.is_drained() {
            return;
        }
        std::thread::yield_now();
    }
    panic!("owner did not drain");
}
#[test]
fn automatic_election_rejection_written_and_exact_durable_ownership() {
    let (mut owner, mut worker) = single(1);
    let now = MonoTime(400);
    owner.advance(now, 1).unwrap();
    let lease = owner.take_effect().unwrap().unwrap();
    let ticket = lease.ticket;
    let reserved = owner.usage().reserved_bytes;
    let rejected = owner.release(lease, 0, now).unwrap_err();
    assert_eq!(rejected.reason, EffectOwnerError::PersistenceEffect);
    worker.reject = true;
    let rejected = owner
        .submit_persists(&mut worker, vec![*rejected.lease], now)
        .unwrap_err();
    assert_eq!(rejected.leases[0].ticket, ticket);
    assert_eq!(owner.usage().reserved_bytes, reserved);
    worker.reject = false;
    owner
        .submit_persists(&mut worker, rejected.leases, now)
        .unwrap();
    let written = worker.poll(1).pop().unwrap();
    owner.deliver_worker(written.clone(), now).unwrap();
    assert!(owner.take_effect().unwrap().is_none());
    assert_eq!(owner.core(group(1)).unwrap().state().hard_state.term, 0);
    assert_eq!(
        owner.deliver_worker(written, now),
        Err(EffectOwnerError::StaleCompletion)
    );
    let durable = worker.poll(1).pop().unwrap();
    let mut stale = durable.clone();
    if let WorkerEvent::Durable { request, .. } = &mut stale {
        request.binding.generation = StorageWorkerGeneration::new(2).unwrap();
    }
    assert_eq!(
        owner.deliver_worker(stale, now),
        Err(EffectOwnerError::StaleCompletion)
    );
    owner.deliver_worker(durable.clone(), now).unwrap();
    assert_eq!(
        owner.deliver_worker(durable, now),
        Err(EffectOwnerError::StaleCompletion)
    );
    let mut apps = BTreeMap::new();
    pump(&mut owner, &mut worker, &mut apps, now);
    assert_eq!(owner.core(group(1)).unwrap().role(), Role::Leader);
    assert_eq!(owner.usage().reserved_bytes, 0);
}
#[test]
fn held_effect_stops_its_group_while_other_group_commits() {
    let (mut owner, mut worker) = single(2);
    let now = MonoTime(400);
    owner.advance(now, 2).unwrap();
    let held = owner.take_effect().unwrap().unwrap();
    let held_group = held.ticket.visit.group;
    let mut apps = BTreeMap::new();
    // Keep the first owned persistence effect outside the owner. The second
    // group must finish without freeing the first group's reservation.
    for _ in 0..1000 {
        for event in worker.poll(1) {
            owner.deliver_worker(event, now).unwrap();
        }
        owner.advance(now, 2).unwrap();
        let mut persists = Vec::new();
        while let Some(lease) = owner.take_effect().unwrap() {
            match &lease.effect {
                Effect::Persist(_) => persists.push(lease),
                Effect::Committed(entries) => {
                    let a = apps
                        .entry(lease.ticket.visit.group)
                        .or_insert_with(|| Counter::new(100).unwrap());
                    a.apply_batch(entries).unwrap();
                    owner.release(lease, a.applied_index(), now).unwrap();
                }
                _ => panic!(),
            }
        }
        if !persists.is_empty() {
            owner.submit_persists(&mut worker, persists, now).unwrap();
        }
        if owner.usage().active_visits == 1 && worker.is_drained() {
            break;
        }
    }
    assert_eq!(owner.usage().leased, 1);
    assert!(owner.usage().reserved_bytes > 0);
    let other = if held_group == group(1) {
        group(2)
    } else {
        group(1)
    };
    assert_eq!(owner.core(other).unwrap().state().commit_index, 1);
    owner.submit_persists(&mut worker, vec![held], now).unwrap();
    pump(&mut owner, &mut worker, &mut apps, now);
    assert!(owner.is_drained());
}
#[test]
fn application_boundary_and_one_use_read_are_checked_before_release() {
    let (mut owner, mut worker) = single(1);
    let now = MonoTime(400);
    let mut apps = BTreeMap::new();
    pump(&mut owner, &mut worker, &mut apps, now);
    owner
        .admit(
            group(1),
            Event::Propose {
                operation: OperationId::new(1).unwrap(),
                bytes: 7i64.to_le_bytes().to_vec(),
            },
        )
        .unwrap();
    loop {
        for event in worker.poll(1) {
            owner.deliver_worker(event, now).unwrap();
        }
        owner.advance(now, 1).unwrap();
        if let Some(lease) = owner.take_effect().unwrap() {
            match &lease.effect {
                Effect::Persist(_) => {
                    owner
                        .submit_persists(&mut worker, vec![lease], now)
                        .unwrap();
                }
                Effect::Committed(entries) => {
                    let rejected = owner
                        .release(
                            EffectLease {
                                ticket: lease.ticket,
                                effect: lease.effect.clone(),
                            },
                            0,
                            now,
                        )
                        .unwrap_err();
                    assert_eq!(rejected.reason, EffectOwnerError::NotApplied);
                    apps.get_mut(&group(1))
                        .unwrap()
                        .apply_batch(entries)
                        .unwrap();
                    owner
                        .release(lease, apps[&group(1)].applied_index(), now)
                        .unwrap();
                    break;
                }
                _ => panic!(),
            }
        }
    }
    owner
        .admit(
            group(1),
            Event::Read {
                request: ReadRequestId::new(1).unwrap(),
            },
        )
        .unwrap();
    owner.advance(now, 1).unwrap();
    let lease = owner.take_effect().unwrap().unwrap();
    let ticket = lease.ticket;
    let original = lease.effect.clone();
    let rejected = owner
        .complete_effect::<()>(lease, 0, now, |_| {
            panic!("read callback ran before application catch-up")
        })
        .unwrap_err();
    assert_eq!(
        rejected.reason,
        EffectOwnerError::Consensus(RaftError::NotApplied)
    );
    let lease = *rejected.lease;
    let value = owner
        .complete_effect(lease, apps[&group(1)].applied_index(), now, |_| {
            Ok((vec![], apps[&group(1)].read_at(2, ()).unwrap()))
        })
        .unwrap();
    assert_eq!(value, 7);
    assert_eq!(
        owner
            .release(
                EffectLease {
                    ticket,
                    effect: original
                },
                2,
                now
            )
            .unwrap_err()
            .reason,
        EffectOwnerError::StaleEffect
    );
}
#[test]
fn failure_keeps_external_lease_charged_until_explicit_discard() {
    let (mut owner, mut worker) = single(2);
    let now = MonoTime(400);
    owner.advance(now, 2).unwrap();
    let held = owner.take_effect().unwrap().unwrap();
    let failing = owner.take_effect().unwrap().unwrap();
    owner
        .submit_persists(&mut worker, vec![failing], now)
        .unwrap();
    owner
        .deliver_worker(worker.poll(1).pop().unwrap(), now)
        .unwrap();
    worker.fail = true;
    assert!(owner
        .deliver_worker(worker.poll(1).pop().unwrap(), now)
        .is_err());
    assert!(owner.core(held.ticket.visit.group).unwrap().is_fenced());
    assert_eq!(owner.usage().leased, 1);
    assert!(owner.usage().reserved_bytes > 0);
    owner.discard_failed(held).unwrap();
    assert_eq!(owner.usage().reserved_bytes, 0);
    assert!(!owner.is_drained());
}
#[test]
fn invalid_reservation_is_rejected_before_core_progress() {
    let mut store = HostLogStore::new(1);
    let runtime = timed(1, &mut store, 1, 1);
    let worker = HostWorker::new(store);
    assert!(matches!(
        EffectOwner::new(
            runtime,
            worker.binding(),
            EffectOwnerLimits {
                active_visits: 2,
                reserved_bytes: 2,
                control_visits: 1,
                control_bytes: 1,
            }
        ),
        Err(EffectOwnerError::ReservationTooLarge)
    ));
}

#[test]
fn bulk_leases_leave_a_reserved_visit_for_control() {
    let mut store = HostLogStore::new(1);
    let runtime = timed(1, &mut store, 4, 1);
    let mut worker = HostWorker::new(store);
    let mut owner = EffectOwner::new(
        runtime,
        worker.binding(),
        EffectOwnerLimits {
            active_visits: 4,
            control_visits: 1,
            ..EffectOwnerLimits::default()
        },
    )
    .unwrap();
    let now = MonoTime(400);
    let mut apps = BTreeMap::new();
    pump(&mut owner, &mut worker, &mut apps, now);
    for g in 1..=3 {
        owner
            .admit(
                group(g),
                Event::Propose {
                    operation: OperationId::new(1).unwrap(),
                    bytes: 7i64.to_le_bytes().to_vec(),
                },
            )
            .unwrap();
    }
    owner.advance(now, 4).unwrap();
    let mut held = Vec::new();
    while let Some(lease) = owner.take_effect().unwrap() {
        held.push(lease);
    }
    assert_eq!(held.len(), 3);
    owner.admit(group(4), Event::Campaign).unwrap();
    assert_eq!(owner.advance(now, 4).unwrap().len(), 1);
    held.push(owner.take_effect().unwrap().unwrap());
    assert_eq!(owner.usage().active_visits, 4);
    owner.submit_persists(&mut worker, held, now).unwrap();
    pump(&mut owner, &mut worker, &mut apps, now);
    assert!(owner.is_drained());
}

#[cfg(feature = "native")]
mod native {
    use super::*;
    use std::{
        sync::Arc,
        time::{Duration, Instant},
    };
    use voteboat::{
        native::{log_store::*, outbound::NativeOutbound, worker::*},
        outbound::*,
    };
    type Worker = NativeLogWorker<NativeLogStore<FileLogIo>>;
    struct Node {
        owner: Owner,
        worker: Worker,
        apps: BTreeMap<GroupIdentity, Counter>,
        outbound: NativeOutbound,
        pending: Vec<EffectLease>,
        reads: Vec<i64>,
        transports: BTreeMap<NodeId, Box<dyn voteboat::transport::PeerTransport>>,
        staged: VecDeque<OutboundBatch>,
        sent: usize,
        received: usize,
    }
    fn make(id: u64, path: &std::path::Path, recover: bool) -> Node {
        let mut store = if recover {
            NativeLogStore::recover(
                FileLogIo::open(path).unwrap(),
                identity(id.into()),
                LogLimits::default(),
            )
            .unwrap()
        } else {
            NativeLogStore::create(
                FileLogIo::create(path).unwrap(),
                identity(id.into()),
                LogLimits::default(),
            )
            .unwrap()
        };
        // Bootstrap is explicit in this test; incoming peer traffic cannot create groups.
        if !recover {
            append(
                &mut store,
                (1..=100)
                    .map(|g| LogMutation::Create(bootstrap(g, 3)))
                    .collect(),
            );
        }
        let runtime_owner = RuntimeOwner {
            store: store.binding(),
            lane: ExecutionLaneId::new(1).unwrap(),
            generation: RuntimeGeneration::new(1).unwrap(),
        };
        let mut shard = Shard::new(
            runtime_owner,
            ShardLimits {
                max_groups: 100,
                visit_items: 1,
                ..ShardLimits::default()
            },
            Ready(VecDeque::new()),
        )
        .unwrap();
        let mut apps = BTreeMap::new();
        for g in 1..=100 {
            let core = Raft::recover(
                node(id),
                store.binding(),
                store.state(group(g)).unwrap(),
                store.limits(),
            )
            .unwrap();
            let mut app = Counter::new(100).unwrap();
            app.apply_batch(core.replay_committed()).unwrap();
            apps.insert(group(g), app);
            shard.register(core).unwrap();
        }
        let runtime = TimedShard::new(
            shard,
            Timers {
                owner: runtime_owner,
                sequence: 0,
                entries: BTreeMap::new(),
            },
            Entropy(id * 17),
            TimerConfig::default(),
            MonoTime(0),
        )
        .unwrap();
        let worker = NativeLogWorker::spawn(
            store,
            StorageWorkerGeneration::new(1).unwrap(),
            WorkerLimits::default(),
            Arc::new(ThreadWake::current()),
        )
        .unwrap();
        let owner =
            EffectOwner::new(runtime, worker.binding(), EffectOwnerLimits::default()).unwrap();
        let outbound = NativeOutbound::new(
            OutboundBinding {
                node: node(id),
                store: runtime_owner.store,
                generation: OutboundGeneration::new(1).unwrap(),
            },
            OutboundLimits::default(),
        )
        .unwrap();
        Node {
            owner,
            worker,
            apps,
            outbound,
            pending: Vec::new(),
            reads: Vec::new(),
            transports: BTreeMap::new(),
            staged: VecDeque::new(),
            sent: 0,
            received: 0,
        }
    }
    #[cfg(feature = "tls")]
    fn mesh(nodes: &mut [Node]) {
        use voteboat::{
            native::{transport::NativePeerTransport, wire::NativeWireCodec},
            secure::LocalIdentity,
            transport::TransportLimits,
            wire::WireLimits,
        };
        for a in 0..3 {
            for b in a + 1..3 {
                let local = |n: &Node| LocalIdentity {
                    node: n.outbound.binding().node,
                    store: n.outbound.binding().store,
                };
                let (left, right) =
                    support::tls::pair(local(&nodes[a]), local(&nodes[b]), (a * 3 + b + 1) as u64);
                let left = NativePeerTransport::new(
                    left,
                    NativeWireCodec::new(WireLimits::default()).unwrap(),
                    &nodes[a].outbound,
                    TransportLimits::default(),
                )
                .unwrap();
                let right = NativePeerTransport::new(
                    right,
                    NativeWireCodec::new(WireLimits::default()).unwrap(),
                    &nodes[b].outbound,
                    TransportLimits::default(),
                )
                .unwrap();
                nodes[a]
                    .transports
                    .insert(node(b as u64 + 1), Box::new(left));
                nodes[b]
                    .transports
                    .insert(node(a as u64 + 1), Box::new(right));
            }
        }
    }
    fn drain(nodes: &mut [Node], isolated: Option<NodeId>, now: MonoTime) {
        let deadline = Instant::now() + Duration::from_secs(10);
        let mut network = VecDeque::new();
        loop {
            assert!(
                Instant::now() < deadline,
                "effect-owner cluster did not drain"
            );
            for n in nodes.iter_mut() {
                for event in n.worker.poll(1) {
                    n.owner.deliver_worker(event, now).unwrap();
                }
                for step in n.owner.advance(now, 100).unwrap() {
                    assert!(step.error.is_none(), "{:?}", step.error);
                }
                while let Some(lease) = n.owner.take_effect().unwrap() {
                    match &lease.effect {
                        Effect::Persist(_) => n.pending.push(lease),
                        Effect::Committed(entries) => {
                            let app = n.apps.get_mut(&lease.ticket.visit.group).unwrap();
                            app.apply_batch(entries).unwrap();
                            n.owner.release(lease, app.applied_index(), now).unwrap();
                        }
                        Effect::ReadReady(barrier) => {
                            let app = &n.apps[&lease.ticket.visit.group];
                            let index = barrier.index();
                            let value = n
                                .owner
                                .complete_effect(lease, app.applied_index(), now, |_| {
                                    Ok((vec![], app.read_at(index, ()).unwrap()))
                                })
                                .unwrap();
                            n.reads.push(value);
                        }
                        Effect::Send(_) => {
                            let EffectLease {
                                ticket,
                                effect: Effect::Send(message),
                            } = lease
                            else {
                                unreachable!()
                            };
                            n.outbound.submit(vec![message]).unwrap();
                            n.owner.release_transferred_send(ticket).unwrap();
                        }
                        _ => panic!("snapshot is outside this owner history"),
                    }
                }
                if !n.pending.is_empty() {
                    let leases = std::mem::take(&mut n.pending);
                    if let Err(rejected) = n.owner.submit_persists(&mut n.worker, leases, now) {
                        assert_eq!(
                            rejected.reason,
                            EffectOwnerError::Worker(WorkerError::Overloaded)
                        );
                        n.pending = rejected.leases;
                    }
                }
                if n.transports.is_empty() {
                    for mut batch in n.outbound.poll(32) {
                        assert!(network.len() + batch.messages.len() <= 8192);
                        network.extend(batch.messages.drain(..));
                        n.outbound.complete(batch, LocalSendResult::Sent).unwrap();
                    }
                } else {
                    if n.staged.is_empty() {
                        n.staged.extend(n.outbound.poll(32));
                    }
                    for _ in 0..n.staged.len().min(32) {
                        let batch = n.staged.pop_front().unwrap();
                        match n
                            .transports
                            .get_mut(&batch.ticket.peer)
                            .unwrap()
                            .submit(batch)
                        {
                            Ok(()) => n.sent += 1,
                            Err(rejected) => {
                                assert_eq!(
                                    rejected.reason,
                                    voteboat::transport::TransportError::Overloaded
                                );
                                n.staged.push_back(*rejected.batch);
                            }
                        }
                    }
                    for t in n.transports.values_mut() {
                        t.poll(now, voteboat::transport::TransportPollBudget::default())
                            .unwrap();
                        if let Some(done) = t.take_send() {
                            n.outbound.complete(done.batch, done.result).unwrap();
                        }
                        if network.len() <= 8192 - 128 {
                            if let Some(batch) = t.take_received() {
                                network.extend(batch.messages);
                                n.received += 1;
                            }
                        }
                    }
                }
                assert!(
                    n.owner.usage().reserved_bytes <= EffectOwnerLimits::default().reserved_bytes
                );
            }
            for _ in 0..32 {
                let Some(message) = network.pop_front() else {
                    break;
                };
                if isolated.is_some_and(|id| id == message.from || id == message.to) {
                    continue;
                }
                let target = message.to.get() as usize - 1;
                if let Err(rejected) = nodes[target]
                    .owner
                    .admit(message.group, Event::Receive(message))
                {
                    assert_eq!(rejected.reason, RuntimeError::Overloaded);
                    let Event::Receive(message) = *rejected.event else {
                        panic!()
                    };
                    network.push_back(message);
                }
            }
            if network.is_empty()
                && nodes.iter().all(|n| {
                    n.owner.is_drained()
                        && n.worker.is_drained()
                        && n.outbound.is_drained()
                        && n.pending.is_empty()
                        && n.staged.is_empty()
                })
                && nodes.iter().map(|n| n.sent).sum::<usize>()
                    == nodes.iter().map(|n| n.received).sum::<usize>()
            {
                return;
            }
            std::thread::park_timeout(Duration::from_millis(1));
        }
    }
    fn close(nodes: &mut [Node]) {
        for n in nodes {
            n.owner.close_admission().unwrap();
            assert!(n.owner.is_drained());
            n.worker.close();
            let deadline = Instant::now() + Duration::from_secs(5);
            loop {
                if let Some(store) = n.worker.try_reclaim().unwrap() {
                    drop(store);
                    break;
                }
                assert!(Instant::now() < deadline);
                std::thread::park_timeout(Duration::from_millis(1));
            }
        }
    }
    fn proposals(
        nodes: &mut [Node],
        leader: usize,
        operation: u128,
        delta: i64,
        isolated: Option<NodeId>,
        now: MonoTime,
    ) {
        for g in 1..=100 {
            nodes[leader]
                .owner
                .admit(
                    group(g),
                    Event::Propose {
                        operation: OperationId::new(operation).unwrap(),
                        bytes: delta.to_le_bytes().to_vec(),
                    },
                )
                .unwrap();
        }
        drain(nodes, isolated, now);
    }
    #[test]
    fn hundred_groups_run_through_reserved_owner_native_wal_and_secure_transport() {
        let root =
            std::env::temp_dir().join(format!("voteboat-effect-owner-{}", std::process::id()));
        let _ = std::fs::remove_dir_all(&root);
        std::fs::create_dir_all(&root).unwrap();
        let mut nodes = (1..=3)
            .map(|id| make(id, &root.join(id.to_string()), false))
            .collect::<Vec<_>>();
        #[cfg(feature = "tls")]
        mesh(&mut nodes);
        for g in 1..=100 {
            nodes[0].owner.admit(group(g), Event::Campaign).unwrap();
        }
        drain(&mut nodes, None, MonoTime(0));
        proposals(&mut nodes, 0, 1, 7, None, MonoTime(0));
        proposals(&mut nodes, 0, 1, 7, None, MonoTime(0));
        for g in 1..=100 {
            nodes[0]
                .owner
                .admit(
                    group(g),
                    Event::Read {
                        request: ReadRequestId::new(1).unwrap(),
                    },
                )
                .unwrap();
        }
        drain(&mut nodes, None, MonoTime(0));
        assert_eq!(nodes[0].reads, vec![7; 100]);
        proposals(&mut nodes, 0, 99, 100, Some(node(1)), MonoTime(1));
        for g in 1..=100 {
            nodes[1].owner.admit(group(g), Event::Campaign).unwrap();
        }
        drain(&mut nodes, Some(node(1)), MonoTime(1));
        proposals(&mut nodes, 1, 2, 3, Some(node(1)), MonoTime(1));
        for g in 1..=100 {
            nodes[1].owner.admit(group(g), Event::Heartbeat).unwrap();
        }
        drain(&mut nodes, None, MonoTime(2));
        for n in &nodes {
            for a in n.apps.values() {
                assert_eq!(a.read_applied(a.applied_index()).unwrap(), 10);
            }
        }
        let old = nodes
            .iter()
            .map(|n| n.worker.binding().store)
            .collect::<Vec<_>>();
        close(&mut nodes);
        drop(nodes);
        let mut nodes = (1..=3)
            .map(|id| make(id, &root.join(id.to_string()), true))
            .collect::<Vec<_>>();
        #[cfg(feature = "tls")]
        mesh(&mut nodes);
        for (id, n) in nodes.iter().enumerate() {
            assert_ne!(n.worker.binding().store, old[id]);
            for a in n.apps.values() {
                assert_eq!(a.read_applied(a.applied_index()).unwrap(), 10);
            }
        }
        for g in 1..=100 {
            nodes[1].owner.admit(group(g), Event::Campaign).unwrap();
        }
        drain(&mut nodes, None, MonoTime(0));
        proposals(&mut nodes, 1, 1, 7, None, MonoTime(0));
        proposals(&mut nodes, 1, 3, 5, None, MonoTime(0));
        for n in &nodes {
            for a in n.apps.values() {
                assert_eq!(a.read_applied(a.applied_index()).unwrap(), 15);
            }
        }
        close(&mut nodes);
        drop(nodes);
        std::fs::remove_dir_all(root).unwrap();
    }
}

#[test]
fn durable_before_written_fences_without_releasing_a_vote() {
    let (mut owner, mut worker) = single(1);
    let now = MonoTime(400);
    owner.advance(now, 1).unwrap();
    let lease = owner.take_effect().unwrap().unwrap();
    owner
        .submit_persists(&mut worker, vec![lease], now)
        .unwrap();
    let _written = worker.poll(1).pop().unwrap();
    let durable = worker.poll(1).pop().unwrap();
    assert_eq!(
        owner.deliver_worker(durable, now),
        Err(EffectOwnerError::ProviderContract)
    );
    assert!(owner.core(group(1)).unwrap().is_fenced());
    assert_eq!(owner.core(group(1)).unwrap().state().hard_state.term, 0);
    assert!(owner.take_effect().is_err());
}
#[test]
fn callback_capacity_and_reservation_extension_are_bounded() {
    let (mut owner, mut worker) = single(1);
    let now = MonoTime(400);
    let lease = loop {
        for event in worker.poll(1) {
            owner.deliver_worker(event, now).unwrap();
        }
        owner.advance(now, 1).unwrap();
        if let Some(lease) = owner.take_effect().unwrap() {
            if matches!(lease.effect, Effect::Committed(_)) {
                break lease;
            }
            owner
                .submit_persists(&mut worker, vec![lease], now)
                .unwrap();
        }
    };
    let before = owner.usage().reserved_bytes;
    assert_eq!(
        owner.extend_reservation(lease.ticket, EffectOwnerLimits::default().reserved_bytes),
        Err(EffectOwnerError::Overloaded)
    );
    assert_eq!(owner.usage().reserved_bytes, before);
    owner.extend_reservation(lease.ticket, 4096).unwrap();
    let capacity = owner.usage().reserved_bytes / (4 * std::mem::size_of::<Effect>()) + 1;
    let rejected = owner
        .complete_effect(lease, 1, now, |_| Ok((Vec::with_capacity(capacity), ())))
        .unwrap_err();
    assert_eq!(rejected.reason, EffectOwnerError::ReservationTooLarge);
    assert!(owner.core(group(1)).unwrap().is_fenced());
    assert_eq!(owner.usage().leased, 1);
    owner.discard_failed(*rejected.lease).unwrap();
    assert_eq!(owner.usage().reserved_bytes, 0);
}
#[test]
fn closing_drains_accepted_work_and_disarms_automatic_timers() {
    let (mut owner, mut worker) = single(1);
    let now = MonoTime(400);
    owner.advance(now, 1).unwrap();
    let lease = owner.take_effect().unwrap().unwrap();
    owner
        .submit_persists(&mut worker, vec![lease], now)
        .unwrap();
    owner.close_admission().unwrap();
    assert_eq!(
        owner.admit(group(1), Event::Heartbeat).unwrap_err().reason,
        RuntimeError::Closed
    );
    let mut apps = BTreeMap::new();
    pump(&mut owner, &mut worker, &mut apps, now);
    assert!(owner.is_drained());
    assert!(owner.deadline(group(1)).is_none());
    assert_eq!(apps[&group(1)].applied_index(), 1);
}
