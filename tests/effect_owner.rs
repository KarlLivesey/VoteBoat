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
    use voteboat::{
        native::{
            runtime::{DeadlineQueue, FairScheduler, JitterEntropy},
            snapshot_store::*,
            snapshot_worker::NativeSnapshotWorker,
        },
        snapshot::*,
        snapshot_worker::*,
    };
    type Owner = EffectOwner<FairScheduler, DeadlineQueue, JitterEntropy>;
    type Snapshots = NativeSnapshotWorker<NativeSnapshotStore<FileSnapshotIo>>;
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
        snapshots: Option<Snapshots>,
        router: Option<SnapshotRouter>,
        snapshot_pending: VecDeque<EffectLease>,
        send_pending: VecDeque<EffectLease>,
        installed: usize,
        supplied: usize,
    }
    fn make(id: u64, path: &std::path::Path, recover: bool) -> Node {
        make_snapshots(id, path, recover, false)
    }
    fn make_snapshots(
        id: u64,
        path: &std::path::Path,
        recover: bool,
        with_snapshots: bool,
    ) -> Node {
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
            FairScheduler::new(100).unwrap(),
        )
        .unwrap();
        if with_snapshots && !recover && id <= 2 {
            let mutations = (1..=100)
                .map(|g| {
                    let state = store.state(group(g)).unwrap();
                    update(
                        &state,
                        1,
                        1,
                        Some(Suffix {
                            from: 1,
                            entries: vec![LogEntry {
                                index: 1,
                                term: 1,
                                payload: EntryPayload::Command {
                                    operation: OperationId::new(1).unwrap(),
                                    bytes: 7i64.to_le_bytes().to_vec(),
                                },
                            }],
                        }),
                    )
                })
                .collect();
            append(&mut store, mutations);
        }
        let mut apps = BTreeMap::new();
        let mut snapshot_stores = BTreeMap::new();
        if with_snapshots && !recover {
            std::fs::create_dir(path.join("snapshots")).unwrap();
        }
        for g in 1..=100 {
            let mut app = Counter::new(100).unwrap();
            let mut snap = with_snapshots.then(|| {
                let directory = path.join("snapshots").join(g.to_string());
                let identity = SnapshotIdentity {
                    store: store.binding().identity,
                    group: group(g),
                };
                let limits = SnapshotLimits {
                    max_application_bytes: 65536,
                    max_metadata_bytes: 4096,
                    max_chunk_bytes: 1024,
                };
                if recover {
                    NativeSnapshotStore::recover(
                        FileSnapshotIo::open(directory).unwrap(),
                        identity,
                        limits,
                    )
                    .unwrap()
                } else {
                    NativeSnapshotStore::create(
                        FileSnapshotIo::create(directory).unwrap(),
                        identity,
                        limits,
                    )
                    .unwrap()
                }
            });
            let mut core = if recover && with_snapshots {
                recover_replica(node(id), group(g), &store, snap.as_mut().unwrap(), &mut app)
                    .unwrap()
                    .0
            } else {
                let core = Raft::recover(
                    node(id),
                    store.binding(),
                    store.state(group(g)).unwrap(),
                    store.limits(),
                )
                .unwrap();
                app.apply_batch(core.replay_committed()).unwrap();
                core
            };
            if with_snapshots && !recover && id <= 2 {
                let receipt = checkpoint_application(&core, &app, snap.as_mut().unwrap()).unwrap();
                compact_replica(
                    &mut core,
                    &mut store,
                    snap.as_mut().unwrap(),
                    &app,
                    receipt.reference(),
                )
                .unwrap();
            }
            if let Some(snap) = snap {
                snapshot_stores.insert(group(g), snap);
            }
            apps.insert(group(g), app);
            shard.register(core).unwrap();
        }
        let runtime = TimedShard::new(
            shard,
            DeadlineQueue::new(runtime_owner, 100).unwrap(),
            JitterEntropy::new(id * 17),
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
        let snapshots = with_snapshots.then(|| {
            NativeSnapshotWorker::spawn(
                snapshot_stores,
                SnapshotWorkerBinding {
                    store: runtime_owner.store,
                    generation: SnapshotWorkerGeneration::new(1).unwrap(),
                },
                SnapshotWorkLimits {
                    max_groups: 100,
                    ..SnapshotWorkLimits::default()
                },
                Arc::new(ThreadWake::current()),
            )
            .unwrap()
        });
        let router = snapshots.as_ref().map(|worker| {
            SnapshotRouter::new(
                owner.identity(),
                worker.binding(),
                SnapshotRouterLimits::default(),
            )
            .unwrap()
        });
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
            snapshots,
            router,
            snapshot_pending: VecDeque::new(),
            send_pending: VecDeque::new(),
            installed: 0,
            supplied: 0,
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
                if let (Some(router), Some(worker)) = (&mut n.router, &mut n.snapshots) {
                    for event in worker.poll(1) {
                        let installed = matches!(
                            &event.result,
                            Ok(SnapshotOutput::Loaded {
                                reconciled: true,
                                ..
                            })
                        );
                        let supplied = matches!(
                            &event.result,
                            Ok(SnapshotOutput::Loaded {
                                reconciled: false,
                                ..
                            })
                        );
                        let app = n.apps.get_mut(&event.visit.group).unwrap();
                        router.deliver(&mut n.owner, app, event, now).unwrap();
                        n.installed += usize::from(installed);
                        n.supplied += usize::from(supplied);
                    }
                    for _ in 0..n.snapshot_pending.len() {
                        let lease = n.snapshot_pending.pop_front().unwrap();
                        let app = &n.apps[&lease.ticket.visit.group];
                        if let Err(rejected) = router.submit(&mut n.owner, worker, lease, app) {
                            assert!(matches!(
                                rejected.reason,
                                SnapshotRouteError::Overloaded
                                    | SnapshotRouteError::Worker(SnapshotWorkError::Overloaded)
                                    | SnapshotRouteError::Owner(EffectOwnerError::Overloaded)
                            ));
                            n.snapshot_pending.push_back(*rejected.lease);
                        }
                    }
                }
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
                        Effect::Send(_) => n.send_pending.push_back(lease),
                        Effect::StageSnapshot(_)
                        | Effect::SnapshotRequired { .. }
                        | Effect::SnapshotInstalled(_)
                        | Effect::CheckpointRequired { .. }
                        | Effect::CheckpointCompacted(_) => {
                            assert!(n.router.is_some());
                            n.snapshot_pending.push_back(lease);
                        }
                    }
                }
                for _ in 0..n.send_pending.len() {
                    let EffectLease {
                        ticket,
                        effect: Effect::Send(message),
                    } = n.send_pending.pop_front().unwrap()
                    else {
                        unreachable!()
                    };
                    match n.outbound.submit(vec![message]) {
                        Ok(_) => n.owner.release_transferred_send(ticket).unwrap(),
                        Err(mut rejected) => {
                            assert_eq!(rejected.reason, OutboundError::Overloaded);
                            assert_eq!(rejected.messages.len(), 1);
                            n.send_pending.push_back(EffectLease {
                                ticket,
                                effect: Effect::Send(rejected.messages.remove(0)),
                            });
                        }
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
                assert_eq!(
                    n.owner.usage().leased,
                    n.pending.len()
                        + n.send_pending.len()
                        + n.snapshot_pending.len()
                        + n.router.as_ref().map_or(0, |r| r.usage().requests)
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
                        && n.send_pending.is_empty()
                        && n.snapshot_pending.is_empty()
                        && n.router.as_ref().is_none_or(|r| r.is_drained())
                        && n.snapshots.as_ref().is_none_or(|w| w.is_drained())
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
            assert!(n.snapshot_pending.is_empty());
            assert!(n.send_pending.is_empty());
            assert!(n.router.as_ref().is_none_or(|r| r.is_drained()));
            if let Some(worker) = &mut n.snapshots {
                worker.close();
                let deadline = Instant::now() + Duration::from_secs(5);
                loop {
                    if let Some(stores) = worker.try_reclaim().unwrap() {
                        drop(stores);
                        break;
                    }
                    assert!(Instant::now() < deadline);
                    std::thread::park_timeout(Duration::from_millis(1));
                }
            }
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
    fn hundred_compacted_groups_catch_up_over_workers_and_authenticated_transport() {
        let root =
            std::env::temp_dir().join(format!("voteboat-snapshot-router-{}", std::process::id()));
        std::fs::create_dir_all(&root).unwrap();
        let mut nodes = (1..=3)
            .map(|id| make_snapshots(id, &root.join(id.to_string()), false, true))
            .collect::<Vec<_>>();
        #[cfg(feature = "tls")]
        mesh(&mut nodes);
        for g in 1..=100 {
            nodes[0].owner.admit(group(g), Event::Campaign).unwrap();
        }
        drain(&mut nodes, None, MonoTime(0));
        assert_eq!(nodes[2].installed, 100);
        assert!(nodes[0].supplied >= 100);
        for n in &nodes {
            for g in 1..=100 {
                assert!(n.owner.core(group(g)).unwrap().state().snapshot.is_some());
                let app = &n.apps[&group(g)];
                assert_eq!(app.read_applied(app.applied_index()), Ok(7));
            }
        }
        // No explicit heartbeat: the native timer provider drives these sends.
        #[cfg(feature = "tls")]
        let before_heartbeat = nodes[0].sent;
        let before_deadline = nodes[0].owner.deadline(group(1)).unwrap().deadline;
        drain(&mut nodes, None, MonoTime(100));
        assert!(nodes[0].owner.deadline(group(1)).unwrap().deadline > before_deadline);
        #[cfg(feature = "tls")]
        assert!(nodes[0].sent > before_heartbeat);
        proposals(&mut nodes, 0, 1, 7, None, MonoTime(101));
        proposals(&mut nodes, 0, 2, 3, None, MonoTime(101));
        checkpoints(&mut nodes, MonoTime(101));
        // Continue writing with both workers still owning all storage handles.
        proposals(&mut nodes, 0, 1, 7, None, MonoTime(101));
        checkpoints(&mut nodes, MonoTime(101));
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
        drain(&mut nodes, None, MonoTime(101));
        assert_eq!(nodes[0].reads, vec![10; 100]);
        let bindings = nodes
            .iter()
            .map(|n| n.owner.identity().store)
            .collect::<Vec<_>>();
        close(&mut nodes);
        drop(nodes);
        let mut nodes = (1..=3)
            .map(|id| make_snapshots(id, &root.join(id.to_string()), true, true))
            .collect::<Vec<_>>();
        #[cfg(feature = "tls")]
        mesh(&mut nodes);
        for (i, n) in nodes.iter().enumerate() {
            assert_ne!(n.owner.identity().store, bindings[i]);
            for app in n.apps.values() {
                assert_eq!(app.read_applied(app.applied_index()), Ok(10));
            }
        }
        for g in 1..=100 {
            nodes[1].owner.admit(group(g), Event::Campaign).unwrap();
        }
        drain(&mut nodes, None, MonoTime(0));
        proposals(&mut nodes, 1, 1, 7, None, MonoTime(0));
        proposals(&mut nodes, 1, 3, 5, None, MonoTime(0));
        checkpoints(&mut nodes, MonoTime(0));
        for n in &nodes {
            for app in n.apps.values() {
                assert_eq!(app.read_applied(app.applied_index()), Ok(15));
            }
        }
        close(&mut nodes);
        drop(nodes);
        std::fs::remove_dir_all(root).unwrap();
    }
    fn checkpoints(nodes: &mut [Node], now: MonoTime) {
        let before = nodes
            .iter()
            .map(|n| {
                (1..=100)
                    .map(|g| n.owner.core(group(g)).unwrap().state().snapshot.unwrap())
                    .collect::<Vec<_>>()
            })
            .collect::<Vec<_>>();
        // Background admission has a smaller ceiling than foreground traffic.
        for start in (1..=100).step_by(8) {
            for n in nodes.iter_mut() {
                for g in start..=(start + 7).min(100) {
                    n.owner.admit(group(g), Event::Checkpoint).unwrap();
                }
            }
            drain(nodes, None, now);
        }
        for (i, n) in nodes.iter().enumerate() {
            for g in 1..=100 {
                let core = n.owner.core(group(g)).unwrap();
                let reference = core.state().snapshot.unwrap();
                assert!(reference.index > before[i][g as usize - 1].index);
                assert!(reference.generation > before[i][g as usize - 1].generation);
                assert_eq!(reference.index, n.apps[&group(g)].applied_index());
                assert_eq!(core.state().base_index(), reference.index);
                assert!(!core.has_pending_dependency());
            }
        }
    }
    fn leaders(nodes: &[Node], isolated: Option<NodeId>) -> Option<Vec<usize>> {
        (1..=100)
            .map(|g| {
                let leaders = nodes
                    .iter()
                    .enumerate()
                    .filter(|(_, n)| {
                        Some(n.outbound.binding().node) != isolated
                            && n.owner.core(group(g)).unwrap().role() == Role::Leader
                    })
                    .map(|(i, _)| i)
                    .collect::<Vec<_>>();
                (leaders.len() == 1).then(|| leaders[0])
            })
            .collect()
    }
    fn elect_automatically(
        nodes: &mut [Node],
        isolated: Option<NodeId>,
        start: u64,
        value: Option<i64>,
    ) -> (Vec<usize>, MonoTime) {
        for tick in 0..=60 {
            let now = MonoTime(start + tick * 25);
            drain(nodes, isolated, now);
            if let Some(leaders) = leaders(nodes, isolated) {
                let caught_up = nodes
                    .iter()
                    .filter(|n| Some(n.outbound.binding().node) != isolated)
                    .all(|n| {
                        n.apps.values().all(|a| {
                            value.is_none_or(|v| a.read_applied(a.applied_index()) == Ok(v))
                        })
                    });
                if caught_up {
                    return (leaders, now);
                }
            }
        }
        panic!("automatic elections/catch-up did not converge");
    }
    fn routed_proposals(
        nodes: &mut [Node],
        leaders: &[usize],
        operation: u128,
        delta: i64,
        isolated: Option<NodeId>,
        now: MonoTime,
    ) {
        for (g, leader) in leaders.iter().enumerate() {
            nodes[*leader]
                .owner
                .admit(
                    group(g as u128 + 1),
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
    fn automatic_elections_and_partition_replacement_preserve_hundred_group_history() {
        let root =
            std::env::temp_dir().join(format!("voteboat-automatic-network-{}", std::process::id()));
        std::fs::create_dir_all(&root).unwrap();
        let mut nodes = (1..=3)
            .map(|id| make(id, &root.join(id.to_string()), false))
            .collect::<Vec<_>>();
        #[cfg(feature = "tls")]
        mesh(&mut nodes);
        let (initial, now) = elect_automatically(&mut nodes, None, 150, Some(0));
        routed_proposals(&mut nodes, &initial, 1, 7, None, now);
        let isolated_index = (0..3)
            .max_by_key(|i| initial.iter().filter(|l| **l == *i).count())
            .unwrap();
        let isolated = Some(node(isolated_index as u64 + 1));
        for (g, leader) in initial
            .iter()
            .enumerate()
            .filter(|(_, l)| **l == isolated_index)
        {
            nodes[*leader]
                .owner
                .admit(
                    group(g as u128 + 1),
                    Event::Propose {
                        operation: OperationId::new(99).unwrap(),
                        bytes: 100i64.to_le_bytes().to_vec(),
                    },
                )
                .unwrap();
        }
        drain(&mut nodes, isolated, MonoTime(now.0 + 1));
        let (replacement, now) = elect_automatically(&mut nodes, isolated, now.0 + 25, Some(7));
        assert!(replacement.iter().all(|i| *i != isolated_index));
        routed_proposals(&mut nodes, &replacement, 2, 3, isolated, now);
        let (healed, now) = elect_automatically(&mut nodes, None, now.0 + 25, Some(10));
        routed_proposals(&mut nodes, &healed, 1, 7, None, now);
        for (g, leader) in healed.iter().enumerate() {
            nodes[*leader]
                .owner
                .admit(
                    group(g as u128 + 1),
                    Event::Read {
                        request: ReadRequestId::new(1).unwrap(),
                    },
                )
                .unwrap();
        }
        drain(&mut nodes, None, now);
        assert_eq!(nodes.iter().map(|n| n.reads.len()).sum::<usize>(), 100);
        assert!(nodes.iter().all(|n| n.reads.iter().all(|v| *v == 10)));
        for n in &nodes {
            for app in n.apps.values() {
                assert_eq!(app.read_applied(app.applied_index()), Ok(10));
            }
        }
        close(&mut nodes);
        drop(nodes);
        std::fs::remove_dir_all(root).unwrap();
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

mod snapshot_routes {
    use super::*;
    use voteboat::{contracts::StorageError, snapshot::*, snapshot_worker::*};
    struct Worker {
        binding: SnapshotWorkerBinding,
        sequence: u64,
        pending: VecDeque<(SnapshotWorkTicket, SnapshotWork)>,
        reject: bool,
        bad_ticket: bool,
        closed: bool,
        allowance: usize,
        images: BTreeMap<GroupIdentity, Snapshot>,
    }
    impl SnapshotWorker for Worker {
        fn checkpoint_bytes(&self, _: GroupIdentity) -> Option<usize> {
            Some(self.allowance / 2)
        }
        fn binding(&self) -> SnapshotWorkerBinding {
            self.binding
        }
        fn limits(&self) -> SnapshotWorkLimits {
            SnapshotWorkLimits::default()
        }
        fn usage(&self) -> SnapshotWorkUsage {
            SnapshotWorkUsage {
                requests: self.pending.len(),
                bytes: self.pending.len() * self.allowance,
            }
        }
        fn load_reservation(&self, _: GroupIdentity) -> Option<usize> {
            Some(self.allowance)
        }
        fn submit(
            &mut self,
            work: SnapshotWork,
        ) -> Result<SnapshotWorkTicket, SnapshotWorkRejected> {
            if self.reject || self.closed {
                return Err(SnapshotWorkRejected {
                    reason: if self.closed {
                        SnapshotWorkError::Closed
                    } else {
                        SnapshotWorkError::Overloaded
                    },
                    work: Box::new(work),
                });
            }
            self.sequence += 1;
            let ticket = SnapshotWorkTicket {
                binding: self.binding,
                sequence: if self.bad_ticket { 0 } else { self.sequence },
            };
            self.pending.push_back((ticket, work));
            Ok(ticket)
        }
        fn poll(&mut self, limit: usize) -> Vec<SnapshotWorkEvent> {
            (0..limit)
                .filter_map(|_| self.pending.pop_front())
                .map(|(request, work)| {
                    let result = match work.job {
                        SnapshotJob::Reconcile { reference } => {
                            SnapshotOutput::Reconciled(reference)
                        }
                        SnapshotJob::Publish { snapshot, .. } => {
                            let reference = reference(&snapshot);
                            self.images.insert(work.visit.group, snapshot);
                            SnapshotOutput::Published(reference)
                        }
                        SnapshotJob::Load { reference, install } => SnapshotOutput::Loaded {
                            reference,
                            snapshot: self.images[&work.visit.group].clone(),
                            reconciled: install,
                        },
                    };
                    SnapshotWorkEvent {
                        request,
                        visit: work.visit,
                        result: Ok(result),
                    }
                })
                .collect()
        }
        fn close(&mut self) {
            self.closed = true;
        }
    }
    fn reference(snapshot: &Snapshot) -> SnapshotRef {
        SnapshotRef {
            store: identity(2),
            group: snapshot.metadata.bootstrap.group,
            generation: SnapshotGeneration::new(1).unwrap(),
            configuration: snapshot.metadata.bootstrap.configuration,
            index: snapshot.metadata.index,
            term: snapshot.metadata.term,
            application_schema: snapshot.metadata.application_schema,
            file_bytes: snapshot.application.len() as u64,
            checksum: 7,
        }
    }
    fn setup(count: u128, requests: usize) -> (Owner, HostWorker, Worker, SnapshotRouter) {
        let mut log = HostLogStore::new(2);
        let runtime = timed(2, &mut log, count, 3);
        let wal = HostWorker::new(log);
        let owner = EffectOwner::new(runtime, wal.binding(), EffectOwnerLimits::default()).unwrap();
        let binding = SnapshotWorkerBinding {
            store: owner.identity().store,
            generation: SnapshotWorkerGeneration::new(1).unwrap(),
        };
        let worker = Worker {
            binding,
            sequence: 0,
            pending: VecDeque::new(),
            reject: false,
            bad_ticket: false,
            closed: false,
            allowance: 65536,
            images: BTreeMap::new(),
        };
        let router = SnapshotRouter::new(
            owner.identity(),
            binding,
            SnapshotRouterLimits {
                requests,
                ..SnapshotRouterLimits::default()
            },
        )
        .unwrap();
        (owner, wal, worker, router)
    }
    fn stage(owner: &mut Owner, g: u128) -> EffectLease {
        let mut app = Counter::new(20).unwrap();
        app.apply_batch(&[LogEntry {
            index: 1,
            term: 1,
            payload: EntryPayload::Command {
                operation: OperationId::new(1).unwrap(),
                bytes: 7i64.to_le_bytes().to_vec(),
            },
        }])
        .unwrap();
        let snapshot = Snapshot {
            metadata: SnapshotMetadata {
                bootstrap: bootstrap(g, 3),
                index: 1,
                term: 1,
                application_schema: app.schema_version(),
            },
            application: app.checkpoint(65536).unwrap(),
        };
        let sender = StoreBinding {
            identity: identity(1),
            session: StoreSession::new(1).unwrap(),
        };
        let message = Message {
            group: group(g),
            configuration: ConfigurationId::new(1).unwrap(),
            from: node(1),
            sender,
            to: node(2),
            term: 1,
            context: RequestContext {
                origin: sender,
                sequence: 1,
            },
            rpc: Rpc::Snapshot {
                snapshot: Box::new(snapshot),
            },
        };
        owner.admit(group(g), Event::Receive(message)).unwrap();
        owner.advance(MonoTime(0), 1).unwrap();
        let lease = owner.take_effect().unwrap().unwrap();
        assert!(matches!(lease.effect, Effect::StageSnapshot(_)));
        lease
    }
    #[test]
    fn rejection_retries_keep_exact_lease_and_stale_completions_do_not_release_it() {
        let (mut owner, mut wal, mut worker, mut router) = setup(1, 1);
        let lease = stage(&mut owner, 1);
        let ticket = lease.ticket;
        let mut app = Counter::new(20).unwrap();
        worker.reject = true;
        let rejected = router
            .submit(&mut owner, &mut worker, lease, &app)
            .unwrap_err();
        assert_eq!(
            rejected.reason,
            SnapshotRouteError::Worker(SnapshotWorkError::Overloaded)
        );
        assert_eq!(rejected.lease.ticket, ticket);
        let charged = owner.usage().reserved_bytes;
        let rejected = router
            .submit(&mut owner, &mut worker, *rejected.lease, &app)
            .unwrap_err();
        assert_eq!(owner.usage().reserved_bytes, charged);
        assert!(router.is_drained());
        worker.reject = false;
        let request = router
            .submit(&mut owner, &mut worker, *rejected.lease, &app)
            .unwrap();
        assert_eq!(router.usage().requests, 1);
        assert_eq!(owner.usage().leased, 1);
        assert!(owner.take_effect().unwrap().is_none());
        let stale = SnapshotWorkEvent {
            request: SnapshotWorkTicket {
                binding: SnapshotWorkerBinding {
                    generation: SnapshotWorkerGeneration::new(2).unwrap(),
                    ..request.binding
                },
                ..request
            },
            visit: ticket.visit,
            result: Err(StorageError::Fenced),
        };
        assert_eq!(
            router.deliver(&mut owner, &mut app, stale, MonoTime(0)),
            Err(SnapshotRouteError::StaleCompletion)
        );
        let mut wrong = ticket.visit;
        wrong.owner.generation = RuntimeGeneration::new(2).unwrap();
        let stale = SnapshotWorkEvent {
            request,
            visit: wrong,
            result: Err(StorageError::Fenced),
        };
        assert_eq!(
            router.deliver(&mut owner, &mut app, stale, MonoTime(0)),
            Err(SnapshotRouteError::StaleCompletion)
        );
        assert!(!owner.is_failed());
        assert_eq!(router.usage().requests, 1);
        let event = worker.poll(1).pop().unwrap();
        router
            .deliver(&mut owner, &mut app, event, MonoTime(0))
            .unwrap();
        assert!(router.is_drained());
        let duplicate = SnapshotWorkEvent {
            request,
            visit: ticket.visit,
            result: Err(StorageError::Fenced),
        };
        assert_eq!(
            router.deliver(&mut owner, &mut app, duplicate, MonoTime(0)),
            Err(SnapshotRouteError::StaleCompletion)
        );
        assert!(!owner.is_failed());
        let persist = owner.take_effect().unwrap().unwrap();
        assert!(matches!(persist.effect, Effect::Persist(_)));
        owner
            .submit_persists(&mut wal, vec![persist], MonoTime(0))
            .unwrap();
        owner
            .deliver_worker(wal.poll(1).pop().unwrap(), MonoTime(0))
            .unwrap();
        assert!(owner.take_effect().unwrap().is_none());
        owner
            .deliver_worker(wal.poll(1).pop().unwrap(), MonoTime(0))
            .unwrap();
        let installed = owner.take_effect().unwrap().unwrap();
        assert!(matches!(installed.effect, Effect::SnapshotInstalled(_)));
        assert_eq!(app.applied_index(), 0);
    }
    #[test]
    fn accepted_snapshot_failure_keeps_other_leases_until_explicit_failed_discard() {
        let (mut owner, _, mut worker, mut router) = setup(2, 2);
        let mut app = Counter::new(20).unwrap();
        let first = stage(&mut owner, 1);
        let first_visit = first.ticket.visit;
        let request = router.submit(&mut owner, &mut worker, first, &app).unwrap();
        let second = stage(&mut owner, 2);
        router
            .submit(&mut owner, &mut worker, second, &app)
            .unwrap();
        assert!(router.discard_failed(&mut owner).is_err());
        assert_eq!(router.usage().requests, 2);
        let event = SnapshotWorkEvent {
            request,
            visit: first_visit,
            result: Err(StorageError::Uncertain("lost publication".into())),
        };
        assert!(matches!(
            router.deliver(&mut owner, &mut app, event, MonoTime(0)),
            Err(SnapshotRouteError::Checkpoint(_))
        ));
        assert!(owner.is_failed());
        assert_eq!(owner.usage().leased, 1);
        assert!(owner.usage().reserved_bytes > 0);
        assert_eq!(router.usage().requests, 1);
        router.discard_failed(&mut owner).unwrap();
        assert_eq!(owner.usage().reserved_bytes, 0);
        assert!(router.is_drained());
        // Dropping leases does not undo accepted provider work.
        assert_eq!(worker.usage().requests, 2);
        worker.close();
        worker.poll(2);
        assert!(worker.is_drained());
    }
    #[test]
    fn bounded_routing_and_wrong_binding_reject_before_transfer() {
        let (mut owner, _, mut worker, mut router) = setup(2, 1);
        let app = Counter::new(20).unwrap();
        let first = stage(&mut owner, 1);
        router.submit(&mut owner, &mut worker, first, &app).unwrap();
        let second = stage(&mut owner, 2);
        let rejected = router
            .submit(&mut owner, &mut worker, second, &app)
            .unwrap_err();
        assert_eq!(rejected.reason, SnapshotRouteError::Overloaded);
        assert_eq!(worker.usage().requests, 1);
        let mut other = SnapshotRouter::new(
            owner.identity(),
            SnapshotWorkerBinding {
                generation: SnapshotWorkerGeneration::new(2).unwrap(),
                ..worker.binding
            },
            SnapshotRouterLimits::default(),
        )
        .unwrap();
        let rejected = other
            .submit(&mut owner, &mut worker, *rejected.lease, &app)
            .unwrap_err();
        assert_eq!(rejected.reason, SnapshotRouteError::WrongWorker);
        let mut obsolete = SnapshotRouter::new(
            RuntimeOwner {
                generation: RuntimeGeneration::new(2).unwrap(),
                ..owner.identity()
            },
            worker.binding(),
            SnapshotRouterLimits::default(),
        )
        .unwrap();
        let rejected = obsolete
            .submit(&mut owner, &mut worker, *rejected.lease, &app)
            .unwrap_err();
        assert_eq!(rejected.reason, SnapshotRouteError::WrongOwner);
        worker.allowance = 240 * 1024 * 1024;
        let mut excessive = SnapshotRouter::new(
            owner.identity(),
            worker.binding(),
            SnapshotRouterLimits {
                requests: 1,
                max_image_bytes: 300 * 1024 * 1024,
            },
        )
        .unwrap();
        let rejected = excessive
            .submit(&mut owner, &mut worker, *rejected.lease, &app)
            .unwrap_err();
        assert_eq!(
            rejected.reason,
            SnapshotRouteError::Owner(EffectOwnerError::ReservationTooLarge)
        );
        worker.allowance = usize::MAX;
        let mut roomy = SnapshotRouter::new(
            owner.identity(),
            worker.binding(),
            SnapshotRouterLimits::default(),
        )
        .unwrap();
        let rejected = roomy
            .submit(&mut owner, &mut worker, *rejected.lease, &app)
            .unwrap_err();
        assert_eq!(rejected.reason, SnapshotRouteError::TooLarge);
        assert!(!owner.is_failed());
    }
    #[test]
    fn invalid_accepted_ticket_fences_but_returns_original_lease_for_discard() {
        let (mut owner, _, mut worker, mut router) = setup(1, 1);
        let app = Counter::new(20).unwrap();
        let lease = stage(&mut owner, 1);
        worker.bad_ticket = true;
        let rejected = router
            .submit(&mut owner, &mut worker, lease, &app)
            .unwrap_err();
        assert_eq!(rejected.reason, SnapshotRouteError::ProviderContract);
        assert!(owner.is_failed());
        assert_eq!(owner.usage().leased, 1);
        assert_eq!(worker.usage().requests, 1);
        owner.discard_failed(*rejected.lease).unwrap();
        assert_eq!(owner.usage().reserved_bytes, 0);
        worker.close();
        worker.poll(1);
    }
    #[test]
    fn closing_owner_drains_snapshot_wal_and_application_dependencies() {
        let (mut owner, mut wal, mut worker, mut router) = setup(1, 1);
        let mut app = Counter::new(20).unwrap();
        let lease = stage(&mut owner, 1);
        router.submit(&mut owner, &mut worker, lease, &app).unwrap();
        owner.close_admission().unwrap();
        assert!(!owner.is_drained());
        router
            .deliver(
                &mut owner,
                &mut app,
                worker.poll(1).pop().unwrap(),
                MonoTime(0),
            )
            .unwrap();
        let lease = owner.take_effect().unwrap().unwrap();
        owner
            .submit_persists(&mut wal, vec![lease], MonoTime(0))
            .unwrap();
        owner
            .deliver_worker(wal.poll(1).pop().unwrap(), MonoTime(0))
            .unwrap();
        owner
            .deliver_worker(wal.poll(1).pop().unwrap(), MonoTime(0))
            .unwrap();
        let installed = owner.take_effect().unwrap().unwrap();
        assert!(matches!(installed.effect, Effect::SnapshotInstalled(_)));
        // Closing ingress must still permit this dependency of accepted work.
        router
            .submit(&mut owner, &mut worker, installed, &app)
            .unwrap();
        router
            .deliver(
                &mut owner,
                &mut app,
                worker.poll(1).pop().unwrap(),
                MonoTime(0),
            )
            .unwrap();
        let ack = owner.take_effect().unwrap().unwrap();
        assert!(matches!(
            &ack.effect,
            Effect::Send(Message {
                rpc: Rpc::SnapshotAck { index: 1 },
                ..
            })
        ));
        assert_eq!(app.read_applied(1), Ok(7));
        owner
            .release(ack, app.applied_index(), MonoTime(0))
            .unwrap();
        assert!(owner.is_drained());
        assert!(router.is_drained());
        assert!(worker.is_drained());
        worker.close();
    }
    #[test]
    fn oversized_provider_result_fences_and_releases_only_router_owned_payload() {
        let (mut owner, _, mut worker, mut router) = setup(1, 1);
        let mut app = Counter::new(20).unwrap();
        let lease = stage(&mut owner, 1);
        let visit = lease.ticket.visit;
        let request = router.submit(&mut owner, &mut worker, lease, &app).unwrap();
        let SnapshotJob::Publish { snapshot, .. } = &worker.pending.front().unwrap().1.job else {
            panic!()
        };
        let mut snapshot = snapshot.clone();
        snapshot.application.reserve(worker.allowance * 2);
        let event = SnapshotWorkEvent {
            request,
            visit,
            result: Ok(SnapshotOutput::Loaded {
                reference: reference(&snapshot),
                snapshot,
                reconciled: true,
            }),
        };
        assert_eq!(
            router.deliver(&mut owner, &mut app, event, MonoTime(0)),
            Err(SnapshotRouteError::ProviderContract)
        );
        assert!(owner.is_failed());
        assert_eq!(owner.usage().reserved_bytes, 0);
        assert!(router.is_drained());
        assert_eq!(app.applied_index(), 0);
        assert_eq!(worker.usage().requests, 1);
        worker.close();
        worker.poll(1);
    }
}
