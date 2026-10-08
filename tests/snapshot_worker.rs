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
use support::*;
use voteboat::{
    application::*, contracts::*, identity::*, log::*, raft::*, runtime::*, snapshot::*,
    snapshot_worker::*,
};
fn binding() -> SnapshotWorkerBinding {
    SnapshotWorkerBinding {
        store: StoreBinding {
            identity: identity(2),
            session: StoreSession::new(1).unwrap(),
        },
        generation: SnapshotWorkerGeneration::new(1).unwrap(),
    }
}
fn visit() -> VisitTicket {
    VisitTicket {
        owner: RuntimeOwner {
            store: binding().store,
            lane: ExecutionLaneId::new(1).unwrap(),
            generation: RuntimeGeneration::new(1).unwrap(),
        },
        group: group(1),
        sequence: 1,
    }
}
fn image(g: u128) -> Snapshot {
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
    Snapshot {
        metadata: SnapshotMetadata {
            bootstrap: bootstrap(g, 3),
            index: 1,
            term: 1,
            application_schema: app.schema_version(),
        },
        application: app.checkpoint(65536).unwrap(),
    }
}
fn message(snapshot: Snapshot) -> Message {
    Message {
        group: snapshot.metadata.bootstrap.group,
        configuration: snapshot.metadata.bootstrap.configuration,
        from: node(1),
        sender: StoreBinding {
            identity: identity(1),
            session: StoreSession::new(1).unwrap(),
        },
        to: node(2),
        term: 1,
        context: RequestContext {
            origin: StoreBinding {
                identity: identity(1),
                session: StoreSession::new(1).unwrap(),
            },
            sequence: 1,
        },
        rpc: Rpc::Snapshot {
            snapshot: Box::new(snapshot),
        },
    }
}
fn follower() -> (Raft, HostLogStore) {
    let mut log = HostLogStore::new(2);
    append(&mut log, vec![LogMutation::Create(bootstrap(1, 3))]);
    let raft = Raft::recover(
        node(2),
        log.binding(),
        log.state(group(1)).unwrap(),
        log.limits(),
    )
    .unwrap();
    (raft, log)
}
/// Public host provider: deliberately controlled completion, no native thread.
struct HostWorker {
    pending: Option<(SnapshotWorkTicket, SnapshotWork)>,
    sequence: u64,
    closed: bool,
}
impl SnapshotWorker for HostWorker {
    fn checkpoint_bytes(&self, group: GroupIdentity) -> Option<usize> {
        (group == support::group(1)).then_some(SnapshotLimits::default().max_application_bytes)
    }
    fn binding(&self) -> SnapshotWorkerBinding {
        binding()
    }
    fn limits(&self) -> SnapshotWorkLimits {
        SnapshotWorkLimits::default()
    }
    fn usage(&self) -> SnapshotWorkUsage {
        SnapshotWorkUsage {
            requests: usize::from(self.pending.is_some()),
            bytes: usize::from(self.pending.is_some()) * 1024,
        }
    }
    fn load_reservation(&self, group: GroupIdentity) -> Option<usize> {
        (group == support::group(1))
            .then(|| snapshot_load_reservation(SnapshotLimits::default()).unwrap())
    }
    fn submit(&mut self, work: SnapshotWork) -> Result<SnapshotWorkTicket, SnapshotWorkRejected> {
        if self.closed || self.pending.is_some() {
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
        let t = SnapshotWorkTicket {
            binding: binding(),
            sequence: self.sequence,
        };
        self.pending = Some((t, work));
        Ok(t)
    }
    fn poll(&mut self, limit: usize) -> Vec<SnapshotWorkEvent> {
        if limit == 0 {
            return vec![];
        }
        let Some((request, work)) = self.pending.take() else {
            return vec![];
        };
        vec![SnapshotWorkEvent {
            request,
            visit: work.visit,
            result: Err(StorageError::Uncertain("injected snapshot failure".into())),
        }]
    }
    fn close(&mut self) {
        self.closed = true;
    }
}
#[test]
fn downstream_worker_selection_rejection_stale_envelope_and_failure() {
    let (mut core, _) = follower();
    let effect = core
        .step(Event::Receive(message(image(1))))
        .unwrap()
        .remove(0);
    assert!(matches!(effect, Effect::StageSnapshot(_)));
    let mut app = Counter::new(20).unwrap();
    let mut worker: Box<dyn SnapshotWorker> = Box::new(HostWorker {
        pending: None,
        sequence: 0,
        closed: false,
    });
    let work = prepare_snapshot_work(&core, &app, visit(), &effect, worker.binding()).unwrap();
    let ticket = worker.submit(work).unwrap();
    let retry = prepare_snapshot_work(&core, &app, visit(), &effect, worker.binding()).unwrap();
    let rejected = worker.submit(retry).unwrap_err();
    assert_eq!(rejected.reason, SnapshotWorkError::Overloaded);
    assert_eq!(rejected.work.visit, visit());
    assert_eq!(worker.usage().requests, 1);
    assert!(worker.poll(0).is_empty());
    let mut event = worker.poll(1).remove(0);
    event.request.binding.generation = SnapshotWorkerGeneration::new(2).unwrap();
    assert_eq!(
        complete_snapshot_work(&mut core, &mut app, &effect, ticket, visit(), event),
        Err(CheckpointError::InvalidBinding)
    );
    assert!(!core.is_fenced());
    let event = SnapshotWorkEvent {
        request: ticket,
        visit: visit(),
        result: Err(StorageError::Uncertain("failed sync".into())),
    };
    assert!(complete_snapshot_work(&mut core, &mut app, &effect, ticket, visit(), event).is_err());
    assert!(core.is_fenced());
    assert_eq!(app.applied_index(), 0);
    worker.close();
    assert!(worker.is_drained());
    assert_eq!(
        worker.submit(*rejected.work).unwrap_err().reason,
        SnapshotWorkError::Closed
    );
}
#[test]
fn invalid_application_and_store_scope_never_submit_work() {
    let (mut core, _) = follower();
    let mut snapshot = image(1);
    snapshot.application[0] ^= 1;
    let effect = core
        .step(Event::Receive(message(snapshot)))
        .unwrap()
        .remove(0);
    assert!(prepare_snapshot_work(
        &core,
        &Counter::new(20).unwrap(),
        visit(),
        &effect,
        binding()
    )
    .is_err());
    let mut wrong = binding();
    wrong.store.session = StoreSession::new(9).unwrap();
    assert_eq!(
        prepare_snapshot_work(&core, &Counter::new(20).unwrap(), visit(), &effect, wrong)
            .unwrap_err(),
        CheckpointError::InvalidBinding
    );
    assert!(!core.is_fenced());
}

fn checkpoint_source() -> (Raft, HostLogStore, Counter) {
    let (_, mut log) = follower();
    let command = LogEntry {
        index: 1,
        term: 1,
        payload: EntryPayload::Command {
            operation: OperationId::new(1).unwrap(),
            bytes: 7i64.to_le_bytes().to_vec(),
        },
    };
    let mutation = update(
        &log.state(group(1)).unwrap(),
        1,
        1,
        Some(Suffix {
            from: 1,
            entries: vec![command.clone()],
        }),
    );
    append(&mut log, vec![mutation]);
    let core = Raft::recover(
        node(2),
        log.binding(),
        log.state(group(1)).unwrap(),
        log.limits(),
    )
    .unwrap();
    let mut app = Counter::new(20).unwrap();
    app.apply_batch(&[command]).unwrap();
    (core, log, app)
}
#[test]
fn local_checkpoint_checks_context_boundary_and_exact_retention_completion() {
    let (mut core, mut log, mut app) = checkpoint_source();
    let effect = core.step(Event::Checkpoint).unwrap().remove(0);
    let Effect::CheckpointRequired { context } = effect else {
        panic!()
    };
    assert_eq!(core.step(Event::Heartbeat), Err(RaftError::Busy));
    let mut wrong = context;
    wrong.sequence += 1;
    assert!(prepare_local_checkpoint_work(&core, &app, visit(), wrong, binding(), 65536).is_err());
    assert!(prepare_local_checkpoint_work(&core, &app, visit(), context, binding(), 1).is_err());
    assert!(prepare_local_checkpoint_work(
        &core,
        &Counter::new(20).unwrap(),
        visit(),
        context,
        binding(),
        65536
    )
    .is_err());
    let work =
        prepare_local_checkpoint_work(&core, &app, visit(), context, binding(), 65536).unwrap();
    let SnapshotJob::Publish {
        snapshot,
        durable: None,
    } = work.job
    else {
        panic!()
    };
    assert_eq!(snapshot, image(1));
    let mut snapshots = support::snapshot::HostSnapshots::new();
    snapshots.identity.store = identity(2);
    snapshots.binding.identity = identity(2);
    let reference = checkpoint_application(&core, &app, &mut snapshots)
        .unwrap()
        .reference();
    snapshots.pin_for_log(reference).unwrap();
    let ticket = SnapshotWorkTicket {
        binding: binding(),
        sequence: 1,
    };
    let mut stale = ticket;
    stale.binding.store.session = StoreSession::new(2).unwrap();
    assert_eq!(
        complete_snapshot_work(
            &mut core,
            &mut app,
            &effect,
            ticket,
            visit(),
            SnapshotWorkEvent {
                request: stale,
                visit: visit(),
                result: Ok(SnapshotOutput::Published(reference)),
            }
        ),
        Err(CheckpointError::InvalidBinding)
    );
    assert!(!core.is_fenced());
    let effects = complete_snapshot_work(
        &mut core,
        &mut app,
        &effect,
        ticket,
        visit(),
        SnapshotWorkEvent {
            request: ticket,
            visit: visit(),
            result: Ok(SnapshotOutput::Published(reference)),
        },
    )
    .unwrap();
    assert_eq!(core.state().base_index(), 0);
    let [Effect::Persist(update)] = effects.as_slice() else {
        panic!()
    };
    let effects = persist_effect(&mut core, &mut log, update.clone()).unwrap();
    assert_eq!(effects, vec![Effect::CheckpointCompacted(reference)]);
    assert_eq!(core.state().base_index(), 1);
    assert!(core.has_pending_dependency());
    // The pin is now anchored by WAL, but a send-load receipt cannot finish
    // maintenance: only the exact verified retention operation can release it.
    let wrong_output = SnapshotOutput::Loaded {
        reference,
        snapshot,
        reconciled: true,
    };
    assert!(complete_snapshot_work(
        &mut core,
        &mut app,
        &effects[0],
        ticket,
        visit(),
        SnapshotWorkEvent {
            request: ticket,
            visit: visit(),
            result: Ok(wrong_output),
        }
    )
    .is_err());
    assert!(core.is_fenced());
    assert_eq!(app.read_applied(1), Ok(7));
}
#[test]
fn local_checkpoint_rejects_foreign_published_boundary_before_wal() {
    for invalid in 0..5 {
        let (mut core, _, mut app) = checkpoint_source();
        let effect = core.step(Event::Checkpoint).unwrap().remove(0);
        let mut snapshots = support::snapshot::HostSnapshots::new();
        snapshots.identity.store = identity(2);
        snapshots.binding.identity = identity(2);
        let mut reference = checkpoint_application(&core, &app, &mut snapshots)
            .unwrap()
            .reference();
        match invalid {
            0 => reference.store = identity(9),
            1 => reference.group = group(9),
            2 => reference.configuration = ConfigurationId::new(9).unwrap(),
            3 => reference.index = 2,
            _ => reference.application_schema += 1,
        }
        let ticket = SnapshotWorkTicket {
            binding: binding(),
            sequence: 1,
        };
        assert!(complete_snapshot_work(
            &mut core,
            &mut app,
            &effect,
            ticket,
            visit(),
            SnapshotWorkEvent {
                request: ticket,
                visit: visit(),
                result: Ok(SnapshotOutput::Published(reference)),
            }
        )
        .is_err());
        assert!(core.is_fenced());
        assert!(core.state().snapshot.is_none());
    }
}

#[cfg(feature = "native")]
mod native {
    use super::*;
    use std::{
        collections::BTreeMap,
        sync::Arc,
        time::{Duration, Instant},
    };
    use voteboat::native::{
        log_store::{FileLogIo, NativeLogStore},
        runtime::{DeadlineQueue, FairScheduler, JitterEntropy},
        snapshot_store::{FileSnapshotIo, NativeSnapshotStore},
        snapshot_worker::NativeSnapshotWorker,
        worker::{NativeLogWorker, ThreadWake},
    };
    use voteboat::worker::*;
    type Owner = EffectOwner<FairScheduler, DeadlineQueue, JitterEntropy>;
    fn limits() -> SnapshotLimits {
        SnapshotLimits {
            max_application_bytes: 65536,
            max_metadata_bytes: 4096,
            max_chunk_bytes: 7,
        }
    }
    fn owner<L: LogStore>(log: &L, core: Raft, worker: WorkerBinding) -> Owner {
        owner_with(log, vec![core], worker)
    }
    fn owner_with<L: LogStore>(log: &L, cores: Vec<Raft>, worker: WorkerBinding) -> Owner {
        let runtime = RuntimeOwner {
            store: log.binding(),
            ..visit().owner
        };
        let mut shard = Shard::new(
            runtime,
            ShardLimits {
                max_groups: 16,
                ..ShardLimits::default()
            },
            FairScheduler::new(16).unwrap(),
        )
        .unwrap();
        for core in cores {
            shard.register(core).unwrap();
        }
        let timed = TimedShard::new(
            shard,
            DeadlineQueue::new(runtime, 16).unwrap(),
            JitterEntropy::new(7),
            TimerConfig::default(),
            MonoTime(0),
        )
        .unwrap();
        EffectOwner::new(timed, worker, EffectOwnerLimits::default()).unwrap()
    }
    fn wait<W: SnapshotWorker>(worker: &mut W) -> SnapshotWorkEvent {
        let until = Instant::now() + Duration::from_secs(10);
        loop {
            if let Some(e) = worker.poll(1).pop() {
                return e;
            }
            assert!(Instant::now() < until);
            std::thread::park_timeout(Duration::from_millis(1));
        }
    }
    fn reclaim<S: SnapshotRetention + Send + 'static>(
        worker: &mut NativeSnapshotWorker<S>,
    ) -> BTreeMap<GroupIdentity, S> {
        worker.close();
        let until = Instant::now() + Duration::from_secs(10);
        loop {
            if let Some(s) = worker.try_reclaim().unwrap() {
                return s;
            }
            assert!(Instant::now() < until);
            std::thread::park_timeout(Duration::from_millis(1));
        }
    }
    fn host_stores(n: u128) -> BTreeMap<GroupIdentity, support::snapshot::HostSnapshots> {
        (1..=n)
            .map(|g| {
                let mut s = support::snapshot::HostSnapshots::new();
                s.identity = SnapshotIdentity {
                    store: identity(2),
                    group: group(g),
                };
                s.binding.identity = identity(2);
                s.limits = limits();
                (group(g), s)
            })
            .collect()
    }
    fn publish_job(g: u128) -> SnapshotWork {
        SnapshotWork {
            visit: VisitTicket {
                group: group(g),
                ..visit()
            },
            job: SnapshotJob::Publish {
                snapshot: image(g),
                durable: None,
            },
        }
    }
    #[test]
    fn native_worker_uses_host_stores_and_retains_credits_until_terminal_poll() {
        let mut worker = NativeSnapshotWorker::spawn(
            host_stores(3),
            binding(),
            SnapshotWorkLimits {
                max_requests: 4,
                control_requests: 1,
                ..SnapshotWorkLimits::default()
            },
            Arc::new(ThreadWake::current()),
        )
        .unwrap();
        let first = worker.submit(publish_job(1)).unwrap();
        let bytes = worker.usage().bytes;
        assert!(bytes > 0);
        assert_eq!(
            worker.submit(publish_job(1)).unwrap_err().reason,
            SnapshotWorkError::Overloaded
        );
        let second = worker.submit(publish_job(2)).unwrap();
        worker.close();
        assert_eq!(
            worker.submit(publish_job(3)).unwrap_err().reason,
            SnapshotWorkError::Closed
        );
        assert!(!worker.is_drained());
        assert!(worker.try_reclaim().unwrap().is_none());
        let a = wait(&mut worker);
        let b = wait(&mut worker);
        assert_eq!([a.request, b.request], [first, second]);
        assert!(matches!(a.result, Ok(SnapshotOutput::Published(_))));
        assert!(matches!(b.result, Ok(SnapshotOutput::Published(_))));
        assert_eq!(worker.usage(), SnapshotWorkUsage::default());
        let stores = reclaim(&mut worker);
        assert!(stores[&group(1)].latest_reference().unwrap().is_some());
        assert!(stores[&group(2)].latest_reference().unwrap().is_some());
        assert!(stores[&group(3)].latest_reference().unwrap().is_none());
    }
    #[test]
    fn admission_rejects_wrong_scope_and_excess_capacity_without_progress() {
        let mut worker = NativeSnapshotWorker::spawn(
            host_stores(1),
            binding(),
            SnapshotWorkLimits {
                max_bytes: 1024 * 1024,
                control_bytes: 128 * 1024,
                ..SnapshotWorkLimits::default()
            },
            Arc::new(ThreadWake::current()),
        )
        .unwrap();
        let mut work = publish_job(1);
        work.visit.owner.generation = RuntimeGeneration::new(2).unwrap();
        work.visit.owner.store.session = StoreSession::new(2).unwrap();
        assert_eq!(
            worker.submit(work).unwrap_err().reason,
            SnapshotWorkError::WrongBinding
        );
        let mut work = publish_job(1);
        if let SnapshotJob::Publish { snapshot, .. } = &mut work.job {
            snapshot.application.reserve(2 * 1024 * 1024);
        }
        assert_eq!(
            worker.submit(work).unwrap_err().reason,
            SnapshotWorkError::TooLarge
        );
        assert!(worker.is_drained());
        reclaim(&mut worker);
        let mut stores = host_stores(1);
        let s = stores.remove(&group(1)).unwrap();
        stores.insert(group(2), s);
        assert!(matches!(
            NativeSnapshotWorker::spawn(
                stores,
                binding(),
                SnapshotWorkLimits::default(),
                Arc::new(ThreadWake::current())
            ),
            Err(SnapshotWorkError::WrongBinding)
        ));
    }
    #[test]
    fn a_failed_snapshot_request_reports_every_accepted_group_without_success() {
        let mut first = NativeSnapshotWorker::spawn(
            host_stores(2),
            binding(),
            SnapshotWorkLimits::default(),
            Arc::new(ThreadWake::current()),
        )
        .unwrap();
        first.submit(publish_job(1)).unwrap();
        let SnapshotOutput::Published(mut reference) = wait(&mut first).result.unwrap() else {
            panic!()
        };
        let stores = reclaim(&mut first);
        let mut worker = NativeSnapshotWorker::spawn(
            stores,
            SnapshotWorkerBinding {
                generation: SnapshotWorkerGeneration::new(2).unwrap(),
                ..binding()
            },
            SnapshotWorkLimits::default(),
            Arc::new(ThreadWake::current()),
        )
        .unwrap();
        reference.checksum ^= 1;
        let a = worker
            .submit(SnapshotWork {
                visit: visit(),
                job: SnapshotJob::Load {
                    reference,
                    install: false,
                },
            })
            .unwrap();
        let b = worker.submit(publish_job(2)).unwrap();
        let failed = wait(&mut worker);
        assert_eq!(failed.request, a);
        assert!(failed.result.is_err());
        let fenced = wait(&mut worker);
        assert_eq!(fenced.request, b);
        assert!(matches!(fenced.result, Err(StorageError::Fenced)));
        assert_eq!(worker.usage(), SnapshotWorkUsage::default());
        let stores = reclaim(&mut worker);
        assert!(stores[&group(2)].latest_reference().unwrap().is_none());
    }
    #[test]
    fn bulk_snapshot_loads_leave_control_capacity_for_installation() {
        let mut first = NativeSnapshotWorker::spawn(
            host_stores(3),
            binding(),
            SnapshotWorkLimits::default(),
            Arc::new(ThreadWake::current()),
        )
        .unwrap();
        first.submit(publish_job(1)).unwrap();
        first.submit(publish_job(2)).unwrap();
        let mut refs = BTreeMap::new();
        for _ in 0..2 {
            let e = wait(&mut first);
            let SnapshotOutput::Published(r) = e.result.unwrap() else {
                panic!()
            };
            refs.insert(r.group, r);
        }
        let stores = reclaim(&mut first);
        let mut worker = NativeSnapshotWorker::spawn(
            stores,
            SnapshotWorkerBinding {
                generation: SnapshotWorkerGeneration::new(2).unwrap(),
                ..binding()
            },
            SnapshotWorkLimits {
                max_requests: 3,
                control_requests: 1,
                max_bytes: 1024 * 1024,
                control_bytes: 256 * 1024,
                ..SnapshotWorkLimits::default()
            },
            Arc::new(ThreadWake::current()),
        )
        .unwrap();
        let load = |g| SnapshotWork {
            visit: VisitTicket {
                group: group(g),
                ..visit()
            },
            job: SnapshotJob::Load {
                reference: SnapshotRef {
                    group: group(g),
                    ..refs[&group(if g == 3 { 1 } else { g })]
                },
                install: false,
            },
        };
        worker.submit(load(1)).unwrap();
        worker.submit(load(2)).unwrap();
        assert_eq!(
            worker.submit(load(3)).unwrap_err().reason,
            SnapshotWorkError::Overloaded
        );
        worker.submit(publish_job(3)).unwrap();
        assert_eq!(worker.usage().requests, 3);
        for _ in 0..3 {
            assert!(wait(&mut worker).result.is_ok());
        }
        assert!(worker.is_drained());
        reclaim(&mut worker);
    }
    #[test]
    fn asynchronous_pinned_load_supplies_exact_leader_snapshot_request() {
        let mut log = HostLogStore::new(1);
        append(&mut log, vec![LogMutation::Create(bootstrap(1, 3))]);
        let command = LogEntry {
            index: 1,
            term: 1,
            payload: EntryPayload::Command {
                operation: OperationId::new(1).unwrap(),
                bytes: 7i64.to_le_bytes().to_vec(),
            },
        };
        let state = log.state(group(1)).unwrap();
        append(
            &mut log,
            vec![update(
                &state,
                1,
                1,
                Some(Suffix {
                    from: 1,
                    entries: vec![command.clone()],
                }),
            )],
        );
        let mut core = Raft::recover(
            node(1),
            log.binding(),
            log.state(group(1)).unwrap(),
            log.limits(),
        )
        .unwrap();
        let mut app = Counter::new(20).unwrap();
        app.apply_batch(&[command]).unwrap();
        let mut snapshots = support::snapshot::HostSnapshots::new();
        snapshots.limits = limits();
        let receipt = checkpoint_application(&core, &app, &mut snapshots).unwrap();
        compact_replica(
            &mut core,
            &mut log,
            &mut snapshots,
            &app,
            receipt.reference(),
        )
        .unwrap();
        let sb = SnapshotWorkerBinding {
            store: log.binding(),
            ..binding()
        };
        let mut worker = NativeSnapshotWorker::spawn(
            [(group(1), snapshots)].into(),
            sb,
            SnapshotWorkLimits::default(),
            Arc::new(ThreadWake::current()),
        )
        .unwrap();
        let effects = core.step(Event::Campaign).unwrap();
        let Effect::Persist(u) = effects.into_iter().next().unwrap() else {
            panic!()
        };
        let effects = persist_effect(&mut core, &mut log, u).unwrap();
        let vote = effects
            .into_iter()
            .find_map(|e| match e {
                Effect::Send(m) if m.to == node(2) => Some(m),
                _ => None,
            })
            .unwrap();
        let reply = Message {
            from: node(2),
            sender: binding().store,
            to: node(1),
            rpc: Rpc::Voted { granted: true },
            ..vote
        };
        let effects = core.step(Event::Receive(reply)).unwrap();
        let Effect::Persist(u) = effects.into_iter().next().unwrap() else {
            panic!()
        };
        let effects = persist_effect(&mut core, &mut log, u).unwrap();
        let append = effects
            .into_iter()
            .find_map(|e| match e {
                Effect::Send(m) if m.to == node(2) => Some(m),
                _ => None,
            })
            .unwrap();
        let reply = Message {
            from: node(2),
            sender: binding().store,
            to: node(1),
            rpc: Rpc::Appended {
                success: false,
                matching_index: 0,
            },
            ..append
        };
        let mut effects = core.step(Event::Receive(reply)).unwrap();
        let effect = effects.remove(0);
        assert!(matches!(effect, Effect::SnapshotRequired { .. }));
        let v = VisitTicket {
            owner: RuntimeOwner {
                store: log.binding(),
                ..visit().owner
            },
            ..visit()
        };
        let work = prepare_snapshot_work(&core, &app, v, &effect, sb).unwrap();
        let request = worker.submit(work).unwrap();
        let event = wait(&mut worker);
        let effects =
            complete_snapshot_work(&mut core, &mut app, &effect, request, v, event).unwrap();
        let [Effect::Send(m)] = effects.as_slice() else {
            panic!()
        };
        assert_eq!(m.to, node(2));
        assert_eq!(m.term, core.state().hard_state.term);
        let Rpc::Snapshot { snapshot } = &m.rpc else {
            panic!()
        };
        assert_eq!(**snapshot, image(1));
        reclaim(&mut worker);
    }
    #[test]
    fn local_checkpoint_receipt_loss_recovers_exact_wal_anchor_and_tail() {
        for phase in 0..5 {
            let root = std::env::temp_dir().join(format!(
                "voteboat-local-checkpoint-{}-{phase}",
                std::process::id()
            ));
            std::fs::create_dir(&root).unwrap();
            let mut log = NativeLogStore::create(
                FileLogIo::create(root.join("wal")).unwrap(),
                identity(2),
                LogLimits::default(),
            )
            .unwrap();
            append(
                &mut log,
                vec![
                    LogMutation::Create(bootstrap(1, 3)),
                    LogMutation::Create(bootstrap(2, 3)),
                ],
            );
            let commands = [7i64, 3]
                .into_iter()
                .enumerate()
                .map(|(i, delta)| LogEntry {
                    index: i as u64 + 1,
                    term: 1,
                    payload: EntryPayload::Command {
                        operation: OperationId::new(i as u128 + 1).unwrap(),
                        bytes: delta.to_le_bytes().to_vec(),
                    },
                })
                .collect::<Vec<_>>();
            let mutation = update(
                &log.state(group(1)).unwrap(),
                1,
                2,
                Some(Suffix {
                    from: 1,
                    entries: commands.clone(),
                }),
            );
            append(&mut log, vec![mutation]);
            let mut core = Raft::recover(
                node(2),
                log.binding(),
                log.state(group(1)).unwrap(),
                log.limits(),
            )
            .unwrap();
            let mut snapshots = NativeSnapshotStore::create(
                FileSnapshotIo::create(root.join("snap")).unwrap(),
                SnapshotIdentity {
                    store: identity(2),
                    group: group(1),
                },
                limits(),
            )
            .unwrap();
            let mut app = Counter::new(20).unwrap();
            app.apply_batch(&commands[..1]).unwrap();
            let old = checkpoint_application(&core, &app, &mut snapshots)
                .unwrap()
                .reference();
            compact_replica(&mut core, &mut log, &mut snapshots, &app, old).unwrap();
            app.apply_batch(&commands[1..]).unwrap();
            let sb = SnapshotWorkerBinding {
                store: log.binding(),
                ..binding()
            };
            let wb = WorkerBinding {
                store: log.binding(),
                generation: StorageWorkerGeneration::new(1).unwrap(),
            };
            let other = Raft::recover(
                node(2),
                log.binding(),
                log.state(group(2)).unwrap(),
                log.limits(),
            )
            .unwrap();
            let mut owner = owner_with(&log, vec![core, other], wb);
            let mut router =
                SnapshotRouter::new(owner.identity(), sb, SnapshotRouterLimits::default()).unwrap();
            let mut sw = NativeSnapshotWorker::spawn(
                [(group(1), snapshots)].into(),
                sb,
                SnapshotWorkLimits::default(),
                Arc::new(ThreadWake::current()),
            )
            .unwrap();
            let mut wal = NativeLogWorker::spawn(
                log,
                wb.generation,
                WorkerLimits::default(),
                Arc::new(ThreadWake::current()),
            )
            .unwrap();
            owner.admit(group(1), Event::Checkpoint).unwrap();
            owner.advance(MonoTime(0), 1).unwrap();
            let lease = owner.take_effect().unwrap().unwrap();
            assert!(matches!(lease.effect, Effect::CheckpointRequired { .. }));
            router.submit(&mut owner, &mut sw, lease, &app).unwrap();
            let event = wait(&mut sw);
            assert_eq!(owner.core(group(1)).unwrap().state().snapshot, Some(old));
            assert!(owner.core(group(1)).unwrap().has_pending_dependency());
            if phase == 0 {
                // Delay delivery of this snapshot receipt while the same owner
                // and WAL worker complete another group's durable election.
                owner.admit(group(2), Event::Campaign).unwrap();
                owner.advance(MonoTime(0), 1).unwrap();
                let persist = owner.take_effect().unwrap().unwrap();
                assert_eq!(persist.ticket.visit.group, group(2));
                owner
                    .submit_persists(&mut wal, vec![persist], MonoTime(0))
                    .unwrap();
                let until = Instant::now() + Duration::from_secs(10);
                while owner.core(group(2)).unwrap().has_pending_dependency() {
                    for event in wal.poll(1) {
                        owner.deliver_worker(event, MonoTime(0)).unwrap();
                    }
                    assert!(Instant::now() < until);
                    std::thread::park_timeout(Duration::from_millis(1));
                }
                while let Some(lease) = owner.take_effect().unwrap() {
                    assert_eq!(lease.ticket.visit.group, group(2));
                    assert!(matches!(lease.effect, Effect::Send(_)));
                    owner.release(lease, 0, MonoTime(0)).unwrap();
                }
                assert_eq!(owner.core(group(2)).unwrap().state().hard_state.term, 1);
                assert!(owner.core(group(1)).unwrap().has_pending_dependency());
                assert_eq!(router.usage().requests, 1);
            }
            if phase > 0 {
                router
                    .deliver(&mut owner, &mut app, event, MonoTime(0))
                    .unwrap();
                let persist = owner.take_effect().unwrap().unwrap();
                assert!(matches!(persist.effect, Effect::Persist(_)));
                assert_eq!(owner.core(group(1)).unwrap().state().snapshot, Some(old));
                if phase > 1 {
                    owner
                        .submit_persists(&mut wal, vec![persist], MonoTime(0))
                        .unwrap();
                    let until = Instant::now() + Duration::from_secs(10);
                    let mut durable = false;
                    while !durable {
                        for event in wal.poll(1) {
                            let written = matches!(event, WorkerEvent::Written { .. });
                            durable |= matches!(event, WorkerEvent::Durable { .. });
                            owner.deliver_worker(event, MonoTime(0)).unwrap();
                            if written {
                                assert_eq!(
                                    owner.core(group(1)).unwrap().state().snapshot,
                                    Some(old)
                                );
                                assert!(owner.take_effect().unwrap().is_none());
                            }
                        }
                        assert!(Instant::now() < until);
                        std::thread::park_timeout(Duration::from_millis(1));
                    }
                    let compacted = owner.take_effect().unwrap().unwrap();
                    assert!(matches!(compacted.effect, Effect::CheckpointCompacted(_)));
                    assert!(owner.core(group(1)).unwrap().has_pending_dependency());
                    if phase > 2 {
                        router.submit(&mut owner, &mut sw, compacted, &app).unwrap();
                        let event = wait(&mut sw);
                        if phase > 3 {
                            router
                                .deliver(&mut owner, &mut app, event, MonoTime(0))
                                .unwrap();
                            assert!(owner.is_drained());
                        }
                    }
                }
            }
            // Abandon observation at each boundary; accepted work is drained,
            // then actual files are reopened with fresh recovered sessions.
            drop(router);
            drop(owner);
            let mut stores = reclaim(&mut sw);
            let snapshots = stores.get_mut(&group(1)).unwrap();
            assert_eq!(snapshots.load_pinned(old).is_ok(), phase < 3);
            drop(stores);
            wal.close();
            let until = Instant::now() + Duration::from_secs(10);
            loop {
                if let Some(log) = wal.try_reclaim().unwrap() {
                    drop(log);
                    break;
                }
                assert!(Instant::now() < until);
                std::thread::park_timeout(Duration::from_millis(1));
            }
            let log = NativeLogStore::recover(
                FileLogIo::open(root.join("wal")).unwrap(),
                identity(2),
                LogLimits::default(),
            )
            .unwrap();
            assert_ne!(log.binding(), wb.store);
            let mut snapshots = NativeSnapshotStore::recover(
                FileSnapshotIo::open(root.join("snap")).unwrap(),
                SnapshotIdentity {
                    store: identity(2),
                    group: group(1),
                },
                limits(),
            )
            .unwrap();
            let mut app = Counter::new(20).unwrap();
            let (core, restored) =
                recover_replica(node(2), group(1), &log, &mut snapshots, &mut app).unwrap();
            assert_eq!(core.state().base_index(), if phase < 2 { 1 } else { 2 });
            assert_eq!(core.state().commit_index, 2);
            assert_eq!(restored.checkpoint_index, core.state().base_index());
            assert_eq!(app.read_applied(2), Ok(10));
            let retry = LogEntry {
                index: 3,
                ..commands[0].clone()
            };
            let receipts = app.apply_batch(&[retry]).unwrap();
            assert!(receipts[0].duplicate);
            assert_eq!(app.read_applied(3), Ok(10));
            drop(snapshots);
            drop(log);
            std::fs::remove_dir_all(root).unwrap();
        }
    }
    #[test]
    fn real_files_install_through_effect_owner_snapshot_worker_and_wal_worker() {
        for phase in 0..3 {
            let root = std::env::temp_dir().join(format!(
                "voteboat-async-snapshot-{}-{phase}",
                std::process::id()
            ));
            std::fs::create_dir(&root).unwrap();
            let mut log = NativeLogStore::create(
                FileLogIo::create(root.join("wal")).unwrap(),
                identity(2),
                LogLimits::default(),
            )
            .unwrap();
            append(&mut log, vec![LogMutation::Create(bootstrap(1, 3))]);
            let core = Raft::recover(
                node(2),
                log.binding(),
                log.state(group(1)).unwrap(),
                log.limits(),
            )
            .unwrap();
            let snap = NativeSnapshotStore::create(
                FileSnapshotIo::create(root.join("snap")).unwrap(),
                SnapshotIdentity {
                    store: identity(2),
                    group: group(1),
                },
                limits(),
            )
            .unwrap();
            let sb = SnapshotWorkerBinding {
                store: log.binding(),
                ..binding()
            };
            let mut sw = NativeSnapshotWorker::spawn(
                [(group(1), snap)].into(),
                sb,
                SnapshotWorkLimits::default(),
                Arc::new(ThreadWake::current()),
            )
            .unwrap();
            let wb = WorkerBinding {
                store: log.binding(),
                generation: StorageWorkerGeneration::new(1).unwrap(),
            };
            let mut owner = owner(&log, core, wb);
            let mut wal = NativeLogWorker::spawn(
                log,
                wb.generation,
                WorkerLimits::default(),
                Arc::new(ThreadWake::current()),
            )
            .unwrap();
            let mut app = Counter::new(20).unwrap();
            owner
                .admit(group(1), Event::Receive(message(image(1))))
                .unwrap();
            owner.advance(MonoTime(0), 1).unwrap();
            let lease = owner.take_effect().unwrap().unwrap();
            assert!(matches!(lease.effect, Effect::StageSnapshot(_)));
            let work = prepare_snapshot_work(
                owner.core(group(1)).unwrap(),
                &app,
                lease.ticket.visit,
                &lease.effect,
                sb,
            )
            .unwrap();
            let request = sw.submit(work).unwrap();
            assert!(owner.take_effect().unwrap().is_none());
            let event = wait(&mut sw);
            let v = lease.ticket.visit;
            owner
                .complete_effect_with(lease, 0, MonoTime(0), |core, effect| {
                    let effects = complete_snapshot_work(core, &mut app, effect, request, v, event)
                        .map_err(|e| {
                            RaftError::Storage(StorageError::Uncertain(format!("{e:?}")))
                        })?;
                    Ok((effects, ()))
                })
                .unwrap();
            // Published and pinned snapshot alone releases only a WAL update.
            assert_eq!(owner.core(group(1)).unwrap().state().commit_index, 0);
            assert_eq!(app.applied_index(), 0);
            let persist = owner.take_effect().unwrap().unwrap();
            assert!(matches!(persist.effect, Effect::Persist(_)));
            if phase != 0 {
                owner
                    .submit_persists(&mut wal, vec![persist], MonoTime(0))
                    .unwrap();
                let until = Instant::now() + Duration::from_secs(10);
                let mut written = false;
                let mut durable = false;
                while !durable {
                    for event in wal.poll(1) {
                        let is_written = matches!(event, WorkerEvent::Written { .. });
                        durable |= matches!(event, WorkerEvent::Durable { .. });
                        owner.deliver_worker(event, MonoTime(0)).unwrap();
                        if is_written {
                            written = true;
                            assert!(owner.take_effect().unwrap().is_none());
                            assert_eq!(owner.core(group(1)).unwrap().state().commit_index, 0);
                        }
                    }
                    assert!(Instant::now() < until);
                    std::thread::park_timeout(Duration::from_millis(1));
                }
                assert!(written);
                let installed = owner.take_effect().unwrap().unwrap();
                assert!(matches!(installed.effect, Effect::SnapshotInstalled(_)));
                assert_eq!(app.applied_index(), 0);
                // Lost application completion: durable log+pin are sufficient for
                // recovery; no SnapshotAck has escaped from this live owner.
                if phase == 2 {
                    owner
                        .extend_reservation(
                            installed.ticket,
                            snapshot_load_reservation(limits()).unwrap(),
                        )
                        .unwrap();
                    let work = prepare_snapshot_work(
                        owner.core(group(1)).unwrap(),
                        &app,
                        installed.ticket.visit,
                        &installed.effect,
                        sb,
                    )
                    .unwrap();
                    let request = sw.submit(work).unwrap();
                    let event = wait(&mut sw);
                    let v = installed.ticket.visit;
                    owner
                        .complete_effect_with(installed, 0, MonoTime(0), |core, effect| {
                            let effects =
                                complete_snapshot_work(core, &mut app, effect, request, v, event)
                                    .map_err(|e| {
                                    RaftError::Storage(StorageError::Uncertain(format!("{e:?}")))
                                })?;
                            Ok((effects, ()))
                        })
                        .unwrap();
                    assert_eq!(app.read_applied(1), Ok(7));
                    let ack = owner.take_effect().unwrap().unwrap();
                    assert!(matches!(
                        &ack.effect,
                        Effect::Send(Message {
                            rpc: Rpc::SnapshotAck { index: 1 },
                            ..
                        })
                    ));
                    owner
                        .release(ack, app.applied_index(), MonoTime(0))
                        .unwrap();
                    assert!(owner.is_drained());
                }
            }
            drop(owner);
            let stores = reclaim(&mut sw);
            drop(stores);
            wal.close();
            let log = loop {
                if let Some(log) = wal.try_reclaim().unwrap() {
                    break log;
                }
                std::thread::park_timeout(Duration::from_millis(1));
            };
            drop(log);
            let log = NativeLogStore::recover(
                FileLogIo::open(root.join("wal")).unwrap(),
                identity(2),
                LogLimits::default(),
            )
            .unwrap();
            let mut snapshots = NativeSnapshotStore::recover(
                FileSnapshotIo::open(root.join("snap")).unwrap(),
                SnapshotIdentity {
                    store: identity(2),
                    group: group(1),
                },
                limits(),
            )
            .unwrap();
            let mut app = Counter::new(20).unwrap();
            let recovered =
                recover_replica(node(2), group(1), &log, &mut snapshots, &mut app).unwrap();
            assert_eq!(recovered.0.state().commit_index, u64::from(phase != 0));
            if phase != 0 {
                assert_eq!(app.read_applied(1), Ok(7));
                let receipts = app
                    .apply_batch(&[LogEntry {
                        index: 2,
                        term: 2,
                        payload: EntryPayload::Command {
                            operation: OperationId::new(1).unwrap(),
                            bytes: 7i64.to_le_bytes().to_vec(),
                        },
                    }])
                    .unwrap();
                assert!(receipts[0].duplicate);
                assert_eq!(app.read_applied(2), Ok(7));
            } else {
                assert_eq!(app.applied_index(), 0);
                assert!(snapshots.latest_reference().unwrap().is_none());
            }
            drop(snapshots);
            drop(log);
            std::fs::remove_dir_all(root).unwrap();
        }
    }
}

#[cfg(feature = "native")]
mod faults {
    use super::*;
    use std::{
        collections::BTreeMap,
        io,
        sync::{Arc, Mutex},
        time::{Duration, Instant},
    };
    use voteboat::native::{
        snapshot_store::*, snapshot_worker::NativeSnapshotWorker, worker::ThreadWake,
    };
    #[derive(Clone, Copy, Debug, Default)]
    enum Fault {
        #[default]
        None,
        Begin,
        Chunk,
        SyncBefore,
        SyncAfter,
        RootBefore,
        RootAfter,
        PinBefore,
        PinAfter,
        ReconcileBefore,
        ReconcileAfter,
        Panic,
    }
    #[derive(Default)]
    struct Device {
        slots: [Option<Vec<u8>>; 2],
        synced: [Option<Vec<u8>>; 2],
        manifest: Option<Vec<u8>>,
        fault: Fault,
        roots: usize,
    }
    #[derive(Clone, Default)]
    struct Io(Arc<Mutex<Device>>);
    impl SnapshotIo for Io {
        fn read_manifest(&mut self) -> io::Result<Vec<u8>> {
            self.0
                .lock()
                .unwrap()
                .manifest
                .clone()
                .ok_or(io::ErrorKind::NotFound.into())
        }
        fn read_slot(&mut self, slot: u8, limit: usize) -> io::Result<Vec<u8>> {
            let d = self.0.lock().unwrap();
            let b = d.slots[slot as usize]
                .as_ref()
                .ok_or(io::ErrorKind::NotFound)?;
            if b.len() > limit {
                return Err(io::Error::other("oversize"));
            }
            Ok(b.clone())
        }
        fn begin_slot(&mut self, slot: u8, bytes: &[u8]) -> io::Result<()> {
            let mut d = self.0.lock().unwrap();
            if matches!(d.fault, Fault::Panic) {
                drop(d);
                panic!("injected provider panic");
            }
            d.slots[slot as usize] = Some(bytes.to_vec());
            if matches!(d.fault, Fault::Begin) {
                return Err(io::Error::other("short prefix"));
            }
            Ok(())
        }
        fn append_slot(&mut self, slot: u8, bytes: &[u8]) -> io::Result<()> {
            let mut d = self.0.lock().unwrap();
            let short = matches!(d.fault, Fault::Chunk);
            d.slots[slot as usize]
                .as_mut()
                .unwrap()
                .extend(&bytes[..if short { bytes.len() / 2 } else { bytes.len() }]);
            if short {
                return Err(io::Error::other("short chunk"));
            }
            Ok(())
        }
        fn sync_slot(&mut self, slot: u8) -> io::Result<()> {
            let mut d = self.0.lock().unwrap();
            if matches!(d.fault, Fault::SyncBefore) {
                return Err(io::Error::other("sync failed"));
            }
            d.synced[slot as usize] = d.slots[slot as usize].clone();
            if matches!(d.fault, Fault::SyncAfter) {
                return Err(io::Error::other("sync completion lost"));
            }
            Ok(())
        }
        fn publish_manifest(&mut self, bytes: &[u8]) -> io::Result<()> {
            let mut d = self.0.lock().unwrap();
            d.roots += 1;
            // First root is reconcile(None), second is publication, third pin.
            let before = matches!(
                (d.fault, d.roots),
                (Fault::RootBefore, 2) | (Fault::PinBefore, 3) | (Fault::ReconcileBefore, 4)
            );
            let after = matches!(
                (d.fault, d.roots),
                (Fault::RootAfter, 2) | (Fault::PinAfter, 3) | (Fault::ReconcileAfter, 4)
            );
            if before {
                return Err(io::Error::other("root publication failed"));
            }
            d.manifest = Some(bytes.to_vec());
            if after {
                return Err(io::Error::other("root receipt lost"));
            }
            Ok(())
        }
    }
    fn wait<W: SnapshotWorker>(worker: &mut W) -> SnapshotWorkEvent {
        let until = Instant::now() + Duration::from_secs(10);
        loop {
            if let Some(e) = worker.poll(1).pop() {
                return e;
            }
            assert!(Instant::now() < until);
            std::thread::park_timeout(Duration::from_millis(1));
        }
    }
    #[test]
    fn checkpoint_retention_failure_fences_and_recovers_the_durable_anchor() {
        for fault in [Fault::ReconcileBefore, Fault::ReconcileAfter] {
            let io = Io::default();
            let sid = SnapshotIdentity {
                store: identity(2),
                group: group(1),
            };
            let store =
                NativeSnapshotStore::create(io.clone(), sid, SnapshotLimits::default()).unwrap();
            io.0.lock().unwrap().roots = 0;
            let mut worker = NativeSnapshotWorker::spawn(
                [(group(1), store)].into(),
                binding(),
                SnapshotWorkLimits::default(),
                Arc::new(ThreadWake::current()),
            )
            .unwrap();
            let (mut core, mut log, mut app) = checkpoint_source();
            let effect = core.step(Event::Checkpoint).unwrap().remove(0);
            let Effect::CheckpointRequired { context } = effect else {
                panic!()
            };
            let work =
                prepare_local_checkpoint_work(&core, &app, visit(), context, binding(), 65536)
                    .unwrap();
            let ticket = worker.submit(work).unwrap();
            let event = wait(&mut worker);
            let effects =
                complete_snapshot_work(&mut core, &mut app, &effect, ticket, visit(), event)
                    .unwrap();
            let [Effect::Persist(update)] = effects.as_slice() else {
                panic!()
            };
            let effects = persist_effect(&mut core, &mut log, update.clone()).unwrap();
            assert!(matches!(effects[0], Effect::CheckpointCompacted(_)));
            io.0.lock().unwrap().fault = fault;
            let work = prepare_snapshot_work(&core, &app, visit(), &effects[0], binding()).unwrap();
            let ticket = worker.submit(work).unwrap();
            let event = wait(&mut worker);
            assert!(event.result.is_err());
            assert!(complete_snapshot_work(
                &mut core,
                &mut app,
                &effects[0],
                ticket,
                visit(),
                event
            )
            .is_err());
            assert!(core.is_fenced());
            assert_eq!(core.state().base_index(), 1);
            worker.close();
            let until = Instant::now() + Duration::from_secs(10);
            loop {
                if let Some(stores) = worker.try_reclaim().unwrap() {
                    drop(stores);
                    break;
                }
                assert!(Instant::now() < until);
                std::thread::park_timeout(Duration::from_millis(1));
            }
            {
                let mut device = io.0.lock().unwrap();
                device.fault = Fault::None;
                device.slots = device.synced.clone();
            }
            let mut recovered =
                NativeSnapshotStore::recover(io, sid, SnapshotLimits::default()).unwrap();
            let mut app = Counter::new(20).unwrap();
            let (core, _) =
                recover_replica(node(2), group(1), &log, &mut recovered, &mut app).unwrap();
            assert_eq!(core.state().base_index(), 1);
            assert!(!core.has_pending_dependency());
            assert_eq!(app.read_applied(1), Ok(7));
        }
    }
    #[test]
    fn async_snapshot_short_write_sync_publication_pin_and_panic_fail_closed() {
        for fault in [
            Fault::Begin,
            Fault::Chunk,
            Fault::SyncBefore,
            Fault::SyncAfter,
            Fault::RootBefore,
            Fault::RootAfter,
            Fault::PinBefore,
            Fault::PinAfter,
            Fault::Panic,
        ] {
            let io = Io::default();
            let sid = SnapshotIdentity {
                store: identity(2),
                group: group(1),
            };
            let store =
                NativeSnapshotStore::create(io.clone(), sid, SnapshotLimits::default()).unwrap();
            {
                let mut d = io.0.lock().unwrap();
                d.fault = fault;
                d.roots = 0;
            }
            let mut worker = NativeSnapshotWorker::spawn(
                BTreeMap::from([(group(1), store)]),
                binding(),
                SnapshotWorkLimits::default(),
                Arc::new(ThreadWake::current()),
            )
            .unwrap();
            let (mut core, _) = follower();
            let effect = core
                .step(Event::Receive(message(image(1))))
                .unwrap()
                .remove(0);
            let mut app = Counter::new(20).unwrap();
            let work =
                prepare_snapshot_work(&core, &app, visit(), &effect, worker.binding()).unwrap();
            let ticket = worker.submit(work).unwrap();
            let event = wait(&mut worker);
            assert!(event.result.is_err(), "fault={fault:?}");
            assert!(
                complete_snapshot_work(&mut core, &mut app, &effect, ticket, visit(), event)
                    .is_err()
            );
            assert!(core.is_fenced());
            assert_eq!(core.state().commit_index, 0);
            assert_eq!(app.applied_index(), 0);
            assert!(worker.is_drained());
            worker.close();
            let until = Instant::now() + Duration::from_secs(10);
            loop {
                match worker.try_reclaim() {
                    Ok(Some(stores)) => {
                        drop(stores);
                        break;
                    }
                    Err(StorageError::Uncertain(_)) if matches!(fault, Fault::Panic) => break,
                    Ok(None) => {
                        assert!(Instant::now() < until);
                        std::thread::park_timeout(Duration::from_millis(1));
                    }
                    other => panic!("unexpected reclaim {fault:?}: {}", other.is_err()),
                }
            }
            {
                let mut d = io.0.lock().unwrap();
                d.fault = Fault::None;
                d.slots = d.synced.clone();
            }
            let mut recovered =
                NativeSnapshotStore::recover(io, sid, SnapshotLimits::default()).unwrap();
            let published = recovered.load().unwrap();
            assert_eq!(
                published.is_some(),
                matches!(fault, Fault::RootAfter | Fault::PinBefore | Fault::PinAfter)
            );
            // No durable WAL anchor exists: recovery discards any uncertain
            // publication/pin instead of activating or acknowledging it.
            let (_, log) = follower();
            let mut app = Counter::new(20).unwrap();
            let (core, _) =
                recover_replica(node(2), group(1), &log, &mut recovered, &mut app).unwrap();
            assert_eq!(core.state().commit_index, 0);
            assert_eq!(app.applied_index(), 0);
            assert!(recovered.load().unwrap().is_none());
        }
    }
}

#[test]
fn install_requires_reconciled_image_and_valid_application_before_acknowledgement() {
    for invalid_application in [false, true] {
        let (mut core, mut log) = follower();
        let mut app = Counter::new(20).unwrap();
        let snapshot = image(1);
        let effect = core
            .step(Event::Receive(message(snapshot.clone())))
            .unwrap()
            .remove(0);
        let mut store = support::snapshot::HostSnapshots::new();
        store.identity.store = identity(2);
        store.binding.identity = identity(2);
        let t = store
            .begin(snapshot.metadata.clone(), snapshot.application.len())
            .unwrap();
        for (i, chunk) in snapshot
            .application
            .chunks(store.limits().max_chunk_bytes)
            .enumerate()
        {
            store
                .write_chunk(t, i * store.limits().max_chunk_bytes, chunk)
                .unwrap();
        }
        let sealed = store.seal(t).unwrap();
        let reference = store.publish(sealed).unwrap().reference();
        store.pin_for_log(reference).unwrap();
        let request = SnapshotWorkTicket {
            binding: binding(),
            sequence: 1,
        };
        let event = SnapshotWorkEvent {
            request,
            visit: visit(),
            result: Ok(SnapshotOutput::Published(reference)),
        };
        let effects =
            complete_snapshot_work(&mut core, &mut app, &effect, request, visit(), event).unwrap();
        let [Effect::Persist(update)] = effects.as_slice() else {
            panic!()
        };
        assert_eq!(app.applied_index(), 0);
        let effects = persist_effect(&mut core, &mut log, update.clone()).unwrap();
        let [effect @ Effect::SnapshotInstalled(_)] = effects.as_slice() else {
            panic!()
        };
        let mut loaded = snapshot;
        if invalid_application {
            loaded.application[0] ^= 1;
        }
        let request = SnapshotWorkTicket {
            sequence: 2,
            ..request
        };
        let event = SnapshotWorkEvent {
            request,
            visit: visit(),
            result: Ok(SnapshotOutput::Loaded {
                reference,
                snapshot: loaded,
                reconciled: invalid_application,
            }),
        };
        assert!(
            complete_snapshot_work(&mut core, &mut app, effect, request, visit(), event).is_err()
        );
        assert!(core.is_fenced());
        assert_eq!(app.applied_index(), 0);
        assert_eq!(core.state().commit_index, reference.index);
    }
}
