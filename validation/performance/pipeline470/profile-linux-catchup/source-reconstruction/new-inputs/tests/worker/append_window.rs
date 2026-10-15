// SPDX-License-Identifier: RPL-1.5
// Copyright (c) 2026 Karl Livesey
//! Actual NativeLogStore and WAL thread; a gated journal controls barrier order.
use super::*;
use std::{
    io,
    sync::{Arc, Condvar, Mutex},
    thread,
    time::{Duration, Instant},
};
use voteboat::{
    contracts::HardState,
    native::log_store::{JournalIo, NativeLogStore},
    native::worker::{NativeLogWorker, ThreadWake},
    quorum::*,
};

#[derive(Default)]
struct Device {
    manifest: Option<Vec<u8>>,
    bytes: Vec<u8>,
    armed: bool,
    entered: bool,
    released: bool,
    syncs: usize,
    publications: usize,
    fail_second: bool,
}
#[derive(Clone, Default)]
struct Journal(Arc<(Mutex<Device>, Condvar)>);
impl JournalIo for Journal {
    fn read_manifest(&mut self) -> io::Result<Vec<u8>> {
        self.0
             .0
            .lock()
            .unwrap()
            .manifest
            .clone()
            .ok_or(io::ErrorKind::NotFound.into())
    }
    fn read_log(&mut self, _: usize) -> io::Result<Vec<u8>> {
        Ok(self.0 .0.lock().unwrap().bytes.clone())
    }
    fn append(&mut self, bytes: &[u8]) -> io::Result<()> {
        self.0 .0.lock().unwrap().bytes.extend_from_slice(bytes);
        Ok(())
    }
    fn truncate_log(&mut self, length: u64) -> io::Result<()> {
        self.0 .0.lock().unwrap().bytes.truncate(length as usize);
        Ok(())
    }
    fn sync_log(&mut self) -> io::Result<()> {
        let (lock, signal) = &*self.0;
        let mut device = lock.lock().unwrap();
        if device.armed {
            device.syncs += 1;
            if device.syncs == 1 {
                device.entered = true;
                signal.notify_all();
                while !device.released {
                    device = signal.wait(device).unwrap();
                }
            }
        }
        Ok(())
    }
    fn publish_manifest(&mut self, bytes: &[u8]) -> io::Result<()> {
        let mut device = self.0 .0.lock().unwrap();
        if device.armed {
            device.publications += 1;
            if device.fail_second && device.publications == 2 {
                return Err(io::Error::other("second publication failed"));
            }
        }
        device.manifest = Some(bytes.to_vec());
        Ok(())
    }
}
fn group() -> GroupIdentity {
    GroupIdentity {
        id: GroupId::new(1).unwrap(),
        incarnation: GroupIncarnation::new(1).unwrap(),
    }
}
fn prepared(journal: Journal) -> NativeLogStore<Journal> {
    let identity = StoreIdentity {
        id: StoreId::new(1).unwrap(),
        incarnation: StoreIncarnation::new(1).unwrap(),
    };
    let mut store = NativeLogStore::create(journal, identity, LogLimits::default()).unwrap();
    let node = NodeId::new(1).unwrap();
    let bootstrap = Bootstrap {
        group: group(),
        configuration: ConfigurationId::new(1).unwrap(),
        policy: Policy::new(Tree::Voter(node), Limits::default()).unwrap(),
        voter_stores: BTreeMap::from([(node, identity)]),
    };
    let tickets = store
        .append_batch(vec![LogMutation::Create(bootstrap)])
        .unwrap();
    store.barrier(&tickets).unwrap();
    store
}
fn unit(binding: StoreBinding, sequence: u64) -> PersistUnit {
    let visit = VisitTicket {
        owner: RuntimeOwner {
            store: binding,
            lane: ExecutionLaneId::new(1).unwrap(),
            generation: RuntimeGeneration::new(1).unwrap(),
        },
        group: group(),
        sequence,
    };
    PersistUnit {
        id: PersistUnitId {
            effect: EffectTicket { visit, sequence: 1 },
            append: None,
        },
        update: LogUpdate {
            group: group(),
            expected_revision: LogRevision::new(sequence).unwrap(),
            hard_state: HardState {
                term: 1,
                voted_for: None,
            },
            commit_index: 0,
            suffix: Some(Suffix {
                from: sequence,
                entries: vec![LogEntry {
                    index: sequence,
                    term: 1,
                    payload: EntryPayload::Noop,
                }],
            }),
            snapshot: None,
            snapshot_membership: None,
        },
    }
}
fn await_first_sync(journal: &Journal) {
    let (lock, signal) = &*journal.0;
    let deadline = Instant::now() + Duration::from_secs(5);
    let mut device = lock.lock().unwrap();
    while !device.entered {
        let remaining = deadline.saturating_duration_since(Instant::now());
        assert!(!remaining.is_zero(), "WAL worker did not enter sync");
        device = signal.wait_timeout(device, remaining).unwrap().0;
    }
}
fn terminals(worker: &mut NativeLogWorker<NativeLogStore<Journal>>) -> Vec<WorkerEvent> {
    let deadline = Instant::now() + Duration::from_secs(5);
    let mut events = Vec::new();
    while events
        .iter()
        .filter(|event: &&WorkerEvent| event.terminal())
        .count()
        < 3
    {
        events.extend(worker.poll(16));
        assert!(Instant::now() < deadline, "missing terminal WAL events");
        thread::yield_now();
    }
    events
}
fn run(fail_second: bool) {
    let journal = Journal::default();
    let store = prepared(journal.clone());
    let binding = store.binding();
    {
        let mut device = journal.0 .0.lock().unwrap();
        device.armed = true;
        device.fail_second = fail_second;
    }
    let mut worker = NativeLogWorker::spawn_with_group_pipeline(
        store,
        StorageWorkerGeneration::new(1).unwrap(),
        WorkerLimits::default(),
        Arc::new(ThreadWake::current()),
        3,
    )
    .unwrap();
    let a = worker.submit(vec![unit(binding, 1)]).unwrap();
    await_first_sync(&journal);
    let b = worker.submit(vec![unit(binding, 2)]).unwrap();
    let c = worker.submit(vec![unit(binding, 3)]).unwrap();
    assert_eq!(worker.usage().requests, 3);
    assert_eq!(
        worker.submit(vec![unit(binding, 4)]).unwrap_err().reason,
        WorkerError::Overloaded
    );
    {
        let mut device = journal.0 .0.lock().unwrap();
        assert_eq!(device.syncs, 1);
        device.released = true;
    }
    journal.0 .1.notify_all();
    let events = terminals(&mut worker);
    assert_events(&events, [a, b, c], fail_second);
    assert_eq!(worker.usage(), WorkerUsage::default());
    if fail_second {
        assert_eq!(
            worker.submit(vec![unit(binding, 4)]).unwrap_err().reason,
            WorkerError::Fenced
        );
    }
    worker.close();
    let deadline = Instant::now() + Duration::from_secs(5);
    while worker.try_reclaim().unwrap().is_none() {
        assert!(Instant::now() < deadline);
        thread::yield_now();
    }
    assert_eq!(journal.0 .0.lock().unwrap().syncs, 2);
    let recovered =
        NativeLogStore::recover(journal, binding.identity, LogLimits::default()).unwrap();
    // Complete fsynced frames may recover past the last published manifest.
    // Their recovery does not convert the failed B/C observations into success.
    assert_eq!(recovered.state(group()).unwrap().last_index(), 3);
    assert_eq!(recovered.binding().identity, binding.identity);
    assert_eq!(recovered.binding().session.get(), binding.session.get() + 1);
}
fn assert_events(events: &[WorkerEvent], requests: [WorkerTicket; 3], failed: bool) {
    assert_eq!(events.len(), 6);
    assert_eq!(
        events
            .iter()
            .filter(|event| matches!(event, WorkerEvent::Durable { .. }))
            .count(),
        if failed { 1 } else { 3 }
    );
    assert_eq!(
        events
            .iter()
            .filter(|event| matches!(event, WorkerEvent::Failed { .. }))
            .count(),
        if failed { 2 } else { 0 }
    );
    for (index, request) in requests.into_iter().enumerate() {
        let sequence = index as u64 + 1;
        let written = events.iter().position(|event| matches!(event, WorkerEvent::Written { request: actual, admissions }
            if *actual == request && admissions.len() == 1 && admissions[0].0.effect.visit.sequence == sequence)).unwrap();
        let terminal = events
            .iter()
            .position(|event| event.request() == request && event.terminal())
            .unwrap();
        assert!(written < terminal);
        match &events[terminal] {
            WorkerEvent::Durable {
                units, completion, ..
            } if !failed || index == 0 => {
                assert_eq!(units.len(), 1);
                assert_eq!(units[0].effect.visit.sequence, sequence);
                assert_eq!(completion.tickets.len(), 1);
                assert_eq!(completion.tickets[0].last_index, sequence);
            }
            WorkerEvent::Failed { units, .. } if failed && index > 0 => {
                assert_eq!(units.len(), 1);
                assert_eq!(units[0].effect.visit.sequence, sequence);
            }
            _ => panic!("wrong exact request terminal"),
        }
    }
}
#[test]
fn syncing_a_cannot_absorb_later_b_and_c_but_their_next_barrier_is_shared() {
    run(false);
}
#[test]
fn failed_shared_publication_fails_b_c_but_recovers_the_valid_unacknowledged_suffix() {
    run(true);
}
