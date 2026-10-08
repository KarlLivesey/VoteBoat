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
use std::collections::VecDeque;
use support::*;
use voteboat::{identity::*, log::*, raft::*, runtime::*, worker::*};

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
        self.0.retain(|v| *v != g);
    }
    fn len(&self) -> usize {
        self.0.len()
    }
}
fn owner(store: StoreBinding) -> RuntimeOwner {
    RuntimeOwner {
        store,
        lane: ExecutionLaneId::new(1).unwrap(),
        generation: RuntimeGeneration::new(1).unwrap(),
    }
}
fn shard<L: LogStore>(store: &mut L, count: u128) -> Shard<Ready> {
    append(
        store,
        (1..=count)
            .map(|g| LogMutation::Create(bootstrap(g, 3)))
            .collect(),
    );
    let mut shard = Shard::new(
        owner(store.binding()),
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
                    node(1),
                    store.binding(),
                    store.state(group(g)).unwrap(),
                    store.limits(),
                )
                .unwrap(),
            )
            .unwrap();
    }
    shard
}
fn campaign<Q: ReadyScheduler>(shard: &mut Shard<Q>, g: u128) -> PersistUnit {
    shard.admit(group(g), Event::Campaign).unwrap();
    let visit = shard.poll(MonoTime(0)).unwrap().unwrap();
    let effects = shard
        .step_next(visit, MonoTime(0))
        .unwrap()
        .unwrap()
        .result
        .unwrap();
    let [Effect::Persist(update)] = effects.as_slice() else {
        panic!()
    };
    PersistUnit {
        visit,
        update: update.clone(),
    }
}
/// A public host replacement exposes one manual progress stage per poll. No
/// thread, native journal or private core state is needed.
struct HostWorker {
    store: HostLogStore,
    binding: WorkerBinding,
    sequence: u64,
    accepted: Option<(WorkerTicket, Vec<PersistUnit>)>,
    written: Option<(WorkerTicket, Vec<VisitTicket>, Vec<LogTicket>)>,
    usage: WorkerUsage,
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
            usage: WorkerUsage::default(),
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
        self.usage
    }
    fn submit(&mut self, units: Vec<PersistUnit>) -> Result<WorkerTicket, WorkerRejected> {
        let error = if self.closed {
            Some(WorkerError::Closed)
        } else if self.usage.requests > 0 {
            Some(WorkerError::Overloaded)
        } else {
            None
        };
        if let Some(reason) = error {
            return Err(WorkerRejected { reason, units });
        }
        let (bytes, _) = match batch_cost(&units, units.capacity(), self.limits()) {
            Ok(v) => v,
            Err(reason) => return Err(WorkerRejected { reason, units }),
        };
        if units
            .iter()
            .any(|u| u.visit.owner.store != self.binding.store)
        {
            return Err(WorkerRejected {
                reason: WorkerError::WrongBinding,
                units,
            });
        }
        self.sequence += 1;
        let ticket = WorkerTicket {
            binding: self.binding,
            sequence: self.sequence,
        };
        self.usage = WorkerUsage {
            requests: 1,
            units: units.len(),
            bytes,
        };
        self.accepted = Some((ticket, units));
        Ok(ticket)
    }
    fn poll(&mut self, limit: usize) -> Vec<WorkerEvent> {
        if limit == 0 {
            return vec![];
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
            vec![WorkerEvent::Written {
                request,
                admissions,
            }]
        } else if let Some((request, visits, tickets)) = self.written.take() {
            let completion = self.store.barrier(&tickets).unwrap();
            self.usage = WorkerUsage::default();
            vec![WorkerEvent::Durable {
                request,
                visits,
                completion,
            }]
        } else {
            vec![]
        }
    }
    fn close(&mut self) {
        self.closed = true;
    }
}
#[test]
fn host_worker_stages_exact_effects_and_durable_owner_delivery_without_native_features() {
    let mut store = HostLogStore::new(1);
    let mut shard = shard(&mut store, 2);
    let mut unit = campaign(&mut shard, 1);
    let visit = unit.visit;
    let original = unit.update.clone();
    let mut worker = HostWorker::new(store);
    unit.update.hard_state.term += 1;
    let rejected = submit_for_shard(&mut shard, &mut worker, vec![unit]).unwrap_err();
    assert_eq!(
        rejected.reason,
        WorkerError::Consensus(RaftError::WrongCompletion)
    );
    assert!(worker.is_drained());
    let mut units = rejected.units;
    units[0].update = original;
    let ticket = submit_for_shard(&mut shard, &mut worker, units).unwrap();
    assert_eq!(worker.usage().units, 1);
    assert_eq!(shard.finish(visit), Err(RuntimeError::DependencyPending));
    assert!(worker.poll(0).is_empty());
    let written = worker.poll(1).pop().unwrap();
    assert_eq!(written.request(), ticket);
    assert!(!written.terminal());
    assert!(apply_to_shard(&mut shard, written)[0]
        .result
        .as_ref()
        .unwrap()
        .is_empty());
    assert_eq!(worker.usage().requests, 1);
    assert_eq!(shard.finish(visit), Err(RuntimeError::DependencyPending));
    let durable = worker.poll(1).pop().unwrap();
    let replay = durable.clone();
    assert!(durable.terminal());
    let deliveries = apply_to_shard(&mut shard, durable);
    assert_eq!(deliveries[0].result.as_ref().unwrap().len(), 2);
    shard.finish(visit).unwrap();
    assert_eq!(worker.usage(), WorkerUsage::default());
    assert!(matches!(
        &apply_to_shard(&mut shard, replay)[0].result,
        Err(WorkerError::Runtime(RuntimeError::StaleTicket))
    ));
    worker.close();
    assert!(worker.is_drained());
    assert_eq!(
        worker.store.state(group(1)).unwrap().hard_state.voted_for,
        Some(node(1))
    );
}
#[test]
fn stopped_visit_does_not_discard_other_groups_in_a_shared_completion() {
    let mut store = HostLogStore::new(1);
    let mut shard = shard(&mut store, 2);
    let first = campaign(&mut shard, 1);
    let second = campaign(&mut shard, 2);
    let surviving = second.visit;
    let mut worker = HostWorker::new(store);
    submit_for_shard(&mut shard, &mut worker, vec![first, second]).unwrap();
    let event = worker.poll(1).pop().unwrap();
    apply_to_shard(&mut shard, event)
        .iter()
        .for_each(|d| assert!(d.result.is_ok()));
    shard.stop_group(group(1)).unwrap();
    let event = worker.poll(1).pop().unwrap();
    let delivered = apply_to_shard(&mut shard, event);
    assert!(matches!(
        delivered[0].result,
        Err(WorkerError::Runtime(RuntimeError::StaleTicket))
    ));
    assert_eq!(delivered[1].result.as_ref().unwrap().len(), 2);
    shard.finish(surviving).unwrap();
}

#[cfg(feature = "native")]
mod native {
    use super::*;
    use std::{
        sync::{
            atomic::{AtomicUsize, Ordering},
            Arc, Condvar, Mutex,
        },
        time::{Duration, Instant},
    };
    use voteboat::{
        contracts::{HardState, StorageError},
        native::{log_store::*, worker::*},
    };
    #[derive(Default)]
    struct Wake(AtomicUsize);
    impl WorkerWake for Wake {
        fn wake(&self) {
            self.0.fetch_add(1, Ordering::SeqCst);
        }
    }
    fn event<W: PersistenceWorker>(worker: &mut W) -> WorkerEvent {
        let end = Instant::now() + Duration::from_secs(5);
        loop {
            if let Some(event) = worker.poll(1).pop() {
                return event;
            }
            assert!(Instant::now() < end, "worker did not complete");
            std::thread::park_timeout(Duration::from_millis(1));
        }
    }
    fn reclaim<L: LogStore + Send + 'static>(
        worker: &mut NativeLogWorker<L>,
    ) -> Result<L, StorageError> {
        worker.close();
        let end = Instant::now() + Duration::from_secs(5);
        loop {
            match worker.try_reclaim() {
                Ok(Some(store)) => return Ok(store),
                Err(e) => return Err(e),
                Ok(None) => {
                    assert!(Instant::now() < end);
                    std::thread::park_timeout(Duration::from_millis(1));
                }
            }
        }
    }
    #[derive(Default)]
    struct Gate {
        released: bool,
        entered: bool,
        fail: bool,
        panic: bool,
    }
    struct Gated<L: LogStore> {
        store: L,
        gate: Arc<(Mutex<Gate>, Condvar)>,
    }
    impl<L: LogStore> LogStore for Gated<L> {
        fn binding(&self) -> StoreBinding {
            self.store.binding()
        }
        fn limits(&self) -> LogLimits {
            self.store.limits()
        }
        fn state(&self, g: GroupIdentity) -> Result<GroupLog, StorageError> {
            self.store.state(g)
        }
        fn append_batch(&mut self, u: Vec<LogMutation>) -> Result<Vec<LogTicket>, StorageError> {
            self.store.append_batch(u)
        }
        fn barrier(&mut self, t: &[LogTicket]) -> Result<DurableLog, StorageError> {
            let (lock, cv) = &*self.gate;
            let mut gate = lock.lock().unwrap();
            gate.entered = true;
            cv.notify_all();
            while !gate.released {
                gate = cv.wait(gate).unwrap();
            }
            let fail = gate.fail;
            let panic = gate.panic;
            drop(gate);
            assert!(!panic, "injected worker panic");
            if fail {
                Err(StorageError::Uncertain("injected barrier failure".into()))
            } else {
                self.store.barrier(t)
            }
        }
        fn fetch_range(
            &self,
            g: GroupIdentity,
            v: LogGeneration,
            f: u64,
            n: usize,
            b: usize,
        ) -> Result<Vec<LogEntry>, StorageError> {
            self.store.fetch_range(g, v, f, n, b)
        }
    }
    fn release(gate: &Arc<(Mutex<Gate>, Condvar)>) {
        let (lock, cv) = &**gate;
        lock.lock().unwrap().released = true;
        cv.notify_all();
    }
    #[test]
    fn admission_reserves_control_and_keeps_credits_until_terminal_delivery() {
        let mut store = HostLogStore::new(1);
        let mut shard = shard(&mut store, 4);
        let mut first = campaign(&mut shard, 1);
        let mut second = campaign(&mut shard, 2);
        let control = campaign(&mut shard, 3);
        let fourth = campaign(&mut shard, 4);
        for unit in [&mut first, &mut second] {
            unit.update.suffix = Some(Suffix {
                from: 1,
                entries: vec![entry(1, 1, 7)],
            });
        }
        let duplicate = PersistUnit {
            visit: first.visit,
            update: first.update.clone(),
        };
        let gate = Arc::new((Mutex::new(Gate::default()), Condvar::new()));
        let wake = Arc::new(Wake::default());
        let limits = WorkerLimits {
            max_requests: 3,
            control_requests: 1,
            max_units: 4,
            control_units: 1,
            batch_units: 3,
            ..WorkerLimits::default()
        };
        let mut worker = NativeLogWorker::spawn(
            Gated {
                store,
                gate: gate.clone(),
            },
            StorageWorkerGeneration::new(1).unwrap(),
            limits,
            wake.clone(),
        )
        .unwrap();
        worker.submit(vec![first]).unwrap();
        assert_eq!(
            worker.submit(vec![duplicate]).unwrap_err().reason,
            WorkerError::Overloaded
        );
        worker.submit(vec![second]).unwrap();
        let mut oversized = Vec::with_capacity(4);
        oversized.push(fourth);
        let rejected = worker.submit(oversized).unwrap_err();
        assert_eq!(rejected.reason, WorkerError::BatchTooLarge);
        assert_eq!(rejected.units.len(), 1);
        let mut fourth = rejected.units.into_iter().next().unwrap();
        fourth.update.suffix = Some(Suffix {
            from: 1,
            entries: vec![entry(1, 1, 9)],
        });
        assert_eq!(
            worker.submit(vec![fourth]).unwrap_err().reason,
            WorkerError::Overloaded
        );
        worker.submit(vec![control]).unwrap();
        assert_eq!(worker.usage().requests, 3);
        assert!(matches!(event(&mut worker), WorkerEvent::Written { .. }));
        worker.close();
        release(&gate);
        let end = Instant::now() + Duration::from_secs(5);
        while wake.0.load(Ordering::SeqCst) < 6 {
            assert!(Instant::now() < end);
            std::thread::park_timeout(Duration::from_millis(1));
        }
        // All physical barriers completed, but no terminal has been consumed.
        assert_eq!(worker.usage().requests, 3);
        assert!(worker.try_reclaim().unwrap().is_none());
        let mut durable = 0;
        for _ in 0..5 {
            if matches!(event(&mut worker), WorkerEvent::Durable { .. }) {
                durable += 1;
            }
        }
        assert_eq!(durable, 3);
        assert_eq!(worker.usage(), WorkerUsage::default());
        let store = reclaim(&mut worker).unwrap();
        assert_eq!(store.store.state(group(1)).unwrap().entries.len(), 1);
        assert_eq!(store.store.state(group(3)).unwrap().hard_state.term, 1);
    }

    #[test]
    fn hundred_groups_share_one_native_worker_and_recover_exact_votes() {
        let root = std::env::temp_dir().join(format!("voteboat-wal-worker-{}", std::process::id()));
        let _ = std::fs::remove_dir_all(&root);
        let mut store = NativeLogStore::create(
            FileLogIo::create(&root).unwrap(),
            identity(1),
            LogLimits::default(),
        )
        .unwrap();
        let mut shard = shard(&mut store, 100);
        let old_binding = store.binding();
        let units = (1..=100)
            .map(|g| campaign(&mut shard, g))
            .collect::<Vec<_>>();
        let visits = units.iter().map(|u| u.visit).collect::<Vec<_>>();
        let mut worker = NativeLogWorker::spawn(
            store,
            StorageWorkerGeneration::new(1).unwrap(),
            WorkerLimits::default(),
            Arc::new(ThreadWake::current()),
        )
        .unwrap();
        submit_for_shard(&mut shard, &mut worker, units).unwrap();
        assert_eq!(worker.usage().units, 100);
        let written = apply_to_shard(&mut shard, event(&mut worker));
        assert_eq!(written.len(), 100);
        assert!(written
            .iter()
            .all(|d| d.result.as_ref().unwrap().is_empty()));
        let durable = apply_to_shard(&mut shard, event(&mut worker));
        assert_eq!(durable.len(), 100);
        assert!(durable
            .iter()
            .all(|d| d.result.as_ref().unwrap().len() == 2));
        for visit in visits {
            shard.finish(visit).unwrap();
        }
        drop(reclaim(&mut worker).unwrap());
        let store = NativeLogStore::recover(
            FileLogIo::open(&root).unwrap(),
            identity(1),
            LogLimits::default(),
        )
        .unwrap();
        assert_ne!(store.binding(), old_binding);
        for g in 1..=100 {
            let state = store.state(group(g)).unwrap();
            assert_eq!(
                state.hard_state,
                HardState {
                    term: 1,
                    voted_for: Some(node(1))
                }
            );
            Raft::recover(node(1), store.binding(), state, store.limits()).unwrap();
        }
        drop(store);
        std::fs::remove_dir_all(root).unwrap();
    }
    #[test]
    fn native_written_stage_never_releases_vote_and_another_group_runs_while_sync_blocks() {
        let mut store = HostLogStore::new(1);
        let mut shard = shard(&mut store, 2);
        let unit = campaign(&mut shard, 1);
        let visit = unit.visit;
        let gate = Arc::new((Mutex::new(Gate::default()), Condvar::new()));
        let wake = Arc::new(Wake::default());
        let mut worker = NativeLogWorker::spawn(
            Gated {
                store,
                gate: gate.clone(),
            },
            StorageWorkerGeneration::new(1).unwrap(),
            WorkerLimits::default(),
            wake.clone(),
        )
        .unwrap();
        submit_for_shard(&mut shard, &mut worker, vec![unit]).unwrap();
        let written = event(&mut worker);
        assert!(matches!(written, WorkerEvent::Written { .. }));
        assert!(apply_to_shard(&mut shard, written)[0]
            .result
            .as_ref()
            .unwrap()
            .is_empty());
        assert_eq!(worker.usage().requests, 1);
        assert!(worker.poll(10).is_empty());
        assert_eq!(shard.finish(visit), Err(RuntimeError::DependencyPending));
        shard.admit(group(2), Event::Heartbeat).unwrap();
        let other = shard.poll(MonoTime(1)).unwrap().unwrap();
        assert_eq!(other.group, group(2));
        assert!(shard
            .step_next(other, MonoTime(1))
            .unwrap()
            .unwrap()
            .result
            .unwrap()
            .is_empty());
        shard.finish(other).unwrap();
        worker.close();
        release(&gate);
        let durable = event(&mut worker);
        assert!(matches!(durable, WorkerEvent::Durable { .. }));
        assert_eq!(
            apply_to_shard(&mut shard, durable)[0]
                .result
                .as_ref()
                .unwrap()
                .len(),
            2
        );
        shard.finish(visit).unwrap();
        assert!(wake.0.load(Ordering::SeqCst) >= 1);
        let returned = reclaim(&mut worker).unwrap();
        assert_eq!(returned.store.state(group(1)).unwrap().hard_state.term, 1);
    }
    #[test]
    fn fatal_barrier_and_panicked_worker_fail_every_accepted_request_without_durable_evidence() {
        for panic in [false, true] {
            let mut store = HostLogStore::new(1);
            let mut shard = shard(&mut store, 2);
            let first = campaign(&mut shard, 1);
            let second = campaign(&mut shard, 2);
            let gate = Arc::new((
                Mutex::new(Gate {
                    fail: !panic,
                    panic,
                    ..Gate::default()
                }),
                Condvar::new(),
            ));
            let mut worker = NativeLogWorker::spawn(
                Gated {
                    store,
                    gate: gate.clone(),
                },
                StorageWorkerGeneration::new(1).unwrap(),
                WorkerLimits::default(),
                Arc::new(ThreadWake::current()),
            )
            .unwrap();
            submit_for_shard(&mut shard, &mut worker, vec![first]).unwrap();
            submit_for_shard(&mut shard, &mut worker, vec![second]).unwrap();
            let written = event(&mut worker);
            assert!(matches!(written, WorkerEvent::Written { .. }));
            apply_to_shard(&mut shard, written);
            release(&gate);
            for _ in 0..2 {
                let failed = event(&mut worker);
                assert!(matches!(failed, WorkerEvent::Failed { .. }));
                assert!(apply_to_shard(&mut shard, failed)
                    .iter()
                    .all(|d| d.result.is_err()));
            }
            assert!(shard.core(group(1)).unwrap().is_fenced());
            assert!(shard.core(group(2)).unwrap().is_fenced());
            assert!(worker.is_drained());
            let returned = reclaim(&mut worker);
            assert_eq!(returned.is_err(), panic);
        }
    }
}
