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
#![cfg(feature = "native")]
mod support;
use std::{
    sync::{Arc, Condvar, Mutex},
    time::{Duration, Instant},
};
use support::*;
use voteboat::{contracts::*, identity::*, log::*, native::worker::*, runtime::*, worker::*};
#[derive(Clone, Copy)]
enum Outcome {
    Success,
    Rejected,
    Fatal,
    Panic,
    Invalid,
    WrongBinding,
}
struct Gate {
    entered: bool,
    released: bool,
    trace: Vec<&'static str>,
    outcome: Outcome,
}
type Shared = Arc<(Mutex<Gate>, Condvar)>;
struct Release(Shared);
impl Drop for Release {
    fn drop(&mut self) {
        release(&self.0);
    }
}
fn release(g: &Shared) {
    let (m, c) = &**g;
    m.lock().unwrap().released = true;
    c.notify_all();
}
struct Store {
    inner: HostLogStore,
    gate: Shared,
}
impl LogStore for Store {
    fn binding(&self) -> StoreBinding {
        self.inner.binding()
    }
    fn limits(&self) -> LogLimits {
        self.inner.limits()
    }
    fn state(&self, g: GroupIdentity) -> Result<GroupLog, StorageError> {
        self.inner.state(g)
    }
    fn append_batch(&mut self, m: Vec<LogMutation>) -> Result<Vec<LogTicket>, StorageError> {
        self.gate.0.lock().unwrap().trace.push("append");
        self.inner.append_batch(m)
    }
    fn barrier(&mut self, t: &[LogTicket]) -> Result<DurableLog, StorageError> {
        self.gate.0.lock().unwrap().trace.push("barrier");
        self.inner.barrier(t)
    }
    fn fetch_range(
        &self,
        g: GroupIdentity,
        v: LogGeneration,
        f: u64,
        n: usize,
        b: usize,
    ) -> Result<Vec<LogEntry>, StorageError> {
        self.inner.fetch_range(g, v, f, n, b)
    }
    fn supports_reclaim(&self) -> bool {
        true
    }
    fn reclaim(&mut self, max_bytes: usize) -> Result<LogReclaimed, StorageError> {
        let (m, c) = &*self.gate;
        let mut state = m.lock().unwrap();
        state.entered = true;
        state.trace.push("enter");
        c.notify_all();
        while !state.released {
            state = c.wait(state).unwrap();
        }
        state.trace.push("exit");
        let outcome = state.outcome;
        drop(state);
        match outcome {
            Outcome::Success => Ok(LogReclaimed {
                before_bytes: 100,
                after_bytes: max_bytes.min(50),
            }),
            Outcome::Rejected => Err(StorageError::Rejected("bounded maintenance refusal")),
            Outcome::Fatal => Err(StorageError::Uncertain("lost publication receipt".into())),
            Outcome::Panic => panic!("injected cleanup panic"),
            Outcome::Invalid => Ok(LogReclaimed {
                before_bytes: 100,
                after_bytes: 200,
            }),
            Outcome::WrongBinding => {
                self.inner.binding.session = StoreSession::new(99).unwrap();
                Ok(LogReclaimed {
                    before_bytes: 100,
                    after_bytes: 50,
                })
            }
        }
    }
}
fn setup(outcome: Outcome, limits: WorkerLimits) -> (NativeLogWorker<Store>, Shared, Release) {
    let mut inner = HostLogStore::new(1);
    append(
        &mut inner,
        vec![
            LogMutation::Create(bootstrap(1, 3)),
            LogMutation::Create(bootstrap(2, 3)),
        ],
    );
    let gate = Arc::new((
        Mutex::new(Gate {
            entered: false,
            released: false,
            trace: Vec::new(),
            outcome,
        }),
        Condvar::new(),
    ));
    let worker = NativeLogWorker::spawn(
        Store {
            inner,
            gate: gate.clone(),
        },
        StorageWorkerGeneration::new(1).unwrap(),
        limits,
        Arc::new(ThreadWake::current()),
    )
    .unwrap();
    (worker, gate.clone(), Release(gate))
}
fn wait_entered(g: &Shared) {
    let deadline = Instant::now() + Duration::from_secs(5);
    while !g.0.lock().unwrap().entered {
        assert!(Instant::now() < deadline);
        std::thread::park_timeout(Duration::from_millis(1));
    }
}
fn unit(w: &impl PersistenceWorker, g: u128, data: bool) -> Vec<PersistUnit> {
    let group = group(g);
    vec![PersistUnit {
        visit: VisitTicket {
            owner: RuntimeOwner {
                store: w.binding().store,
                lane: ExecutionLaneId::new(1).unwrap(),
                generation: RuntimeGeneration::new(1).unwrap(),
            },
            group,
            sequence: 1,
        },
        update: LogUpdate {
            snapshot_membership: None,
            group,
            expected_revision: LogRevision::new(1).unwrap(),
            hard_state: HardState {
                term: 1,
                voted_for: None,
            },
            commit_index: 0,
            snapshot: None,
            suffix: data.then(|| Suffix {
                from: 1,
                entries: vec![entry(1, 1, 7)],
            }),
        },
    }]
}
fn event(w: &mut impl PersistenceWorker) -> WorkerEvent {
    let deadline = Instant::now() + Duration::from_secs(5);
    loop {
        if let Some(e) = w.poll(1).pop() {
            return e;
        }
        assert!(Instant::now() < deadline);
        std::thread::park_timeout(Duration::from_millis(1));
    }
}
fn maintenance(w: &mut impl PersistenceWorker) -> ReclaimEvent {
    let deadline = Instant::now() + Duration::from_secs(5);
    loop {
        if let Some(e) = w.poll_reclaims(1).pop() {
            return e;
        }
        assert!(Instant::now() < deadline);
        std::thread::park_timeout(Duration::from_millis(1));
    }
}
fn finish<L: LogStore + Send + 'static>(w: &mut NativeLogWorker<L>) -> Result<L, StorageError> {
    w.close();
    let deadline = Instant::now() + Duration::from_secs(5);
    loop {
        match w.try_reclaim() {
            Ok(Some(s)) => return Ok(s),
            Err(e) => return Err(e),
            Ok(None) => {
                assert!(Instant::now() < deadline);
                std::thread::park_timeout(Duration::from_millis(1));
            }
        }
    }
}
#[test]
fn cleanup_runs_between_complete_barriers_and_retains_credits_through_close() {
    let (mut w, g, _release) = setup(Outcome::Success, WorkerLimits::default());
    let first = w.submit(unit(&w, 1, false)).unwrap();
    let ticket = w.submit_reclaim(1024).unwrap();
    wait_entered(&g);
    assert!(matches!(event(&mut w),WorkerEvent::Written {request,..} if request==first));
    assert_eq!(w.usage().requests, 2);
    assert!(matches!(event(&mut w),WorkerEvent::Durable {request,..} if request==first));
    assert_eq!(w.usage().requests, 1);
    assert!(w.poll_reclaims(0).is_empty());
    let second = w.submit(unit(&w, 2, false)).unwrap();
    assert_eq!(w.submit_reclaim(1024), Err(WorkerError::Overloaded));
    w.close();
    assert!(!w.is_drained());
    assert!(w.try_reclaim().unwrap().is_none());
    release(&g);
    let result = maintenance(&mut w);
    assert_eq!(result.request, ticket);
    assert_eq!(result.result.unwrap().after_bytes, 50);
    assert!(matches!(event(&mut w),WorkerEvent::Written {request,..} if request==second));
    assert!(matches!(event(&mut w),WorkerEvent::Durable {request,..} if request==second));
    assert!(w.is_drained());
    let store = finish(&mut w).unwrap();
    assert_eq!(store.inner.state(group(2)).unwrap().hard_state.term, 1);
    assert_eq!(
        g.0.lock().unwrap().trace,
        vec!["append", "barrier", "enter", "exit", "append", "barrier"]
    );
}
#[test]
fn maintenance_uses_data_admission_and_preserves_control_reserve() {
    let limits = WorkerLimits {
        max_requests: 2,
        control_requests: 1,
        ..Default::default()
    };
    let (mut w, g, _release) = setup(Outcome::Success, limits);
    w.submit_reclaim(1024).unwrap();
    wait_entered(&g);
    let data = unit(&w, 2, true);
    let pointer = data.as_ptr();
    let rejected = w.submit(data).unwrap_err();
    assert_eq!(rejected.reason, WorkerError::Overloaded);
    assert_eq!(rejected.units.as_ptr(), pointer);
    w.submit(unit(&w, 1, false)).unwrap();
    assert_eq!(w.usage().requests, 2);
    release(&g);
    maintenance(&mut w);
    event(&mut w);
    event(&mut w);
    drop(finish(&mut w).unwrap());
}
#[test]
fn rejected_cleanup_continues_but_uncertain_cleanup_fences_queued_writes() {
    for outcome in [
        Outcome::Rejected,
        Outcome::Fatal,
        Outcome::Invalid,
        Outcome::WrongBinding,
    ] {
        let (mut w, g, _release) = setup(outcome, WorkerLimits::default());
        let ticket = w.submit_reclaim(1024).unwrap();
        wait_entered(&g);
        let write = w.submit(unit(&w, 1, false)).unwrap();
        release(&g);
        let result = maintenance(&mut w);
        assert_eq!(result.request, ticket);
        match outcome {
            Outcome::Rejected => {
                assert!(matches!(result.result, Err(StorageError::Rejected(_))));
                assert!(matches!(event(&mut w), WorkerEvent::Written { .. }));
                assert!(
                    matches!(event(&mut w),WorkerEvent::Durable {request,..} if request==write)
                );
            }
            Outcome::Fatal => {
                assert!(matches!(result.result, Err(StorageError::Uncertain(_))));
                assert!(
                    matches!(event(&mut w),WorkerEvent::Failed {request,error:StorageError::Fenced,..} if request==write)
                );
                assert_eq!(w.submit_reclaim(1024), Err(WorkerError::Fenced));
            }
            Outcome::Invalid | Outcome::WrongBinding => {
                assert!(matches!(result.result, Err(StorageError::Corrupt(_))));
                assert!(
                    matches!(event(&mut w), WorkerEvent::Failed { request, error: StorageError::Fenced, .. } if request == write)
                );
                assert_eq!(w.submit_reclaim(1024), Err(WorkerError::Fenced));
            }
            _ => unreachable!(),
        }
        assert!(w.is_drained());
        drop(finish(&mut w).unwrap());
    }
}
#[test]
fn panic_reports_each_class_once_without_fabricating_write_durability() {
    let (mut w, g, _release) = setup(Outcome::Panic, WorkerLimits::default());
    let ticket = w.submit_reclaim(1024).unwrap();
    wait_entered(&g);
    let write = w.submit(unit(&w, 1, false)).unwrap();
    release(&g);
    let result = maintenance(&mut w);
    assert_eq!(result.request, ticket);
    assert!(matches!(result.result, Err(StorageError::Uncertain(_))));
    assert!(
        matches!(event(&mut w),WorkerEvent::Failed {request,error:StorageError::Uncertain(_),..} if request==write)
    );
    assert!(w.poll(10).is_empty());
    assert!(w.poll_reclaims(10).is_empty());
    assert!(w.is_drained());
    assert!(matches!(finish(&mut w), Err(StorageError::Uncertain(_))));
}

#[test]
fn capability_and_byte_limits_reject_before_ownership_or_sequence_consumption() {
    let mut unsupported = NativeLogWorker::spawn(
        HostLogStore::new(1),
        StorageWorkerGeneration::new(1).unwrap(),
        WorkerLimits::default(),
        Arc::new(ThreadWake::current()),
    )
    .unwrap();
    assert_eq!(
        unsupported.submit_reclaim(1024),
        Err(WorkerError::Unsupported)
    );
    assert!(unsupported.is_drained());
    unsupported.close();
    let deadline = Instant::now() + Duration::from_secs(5);
    loop {
        if unsupported.try_reclaim().unwrap().is_some() {
            break;
        }
        assert!(Instant::now() < deadline);
        std::thread::park_timeout(Duration::from_millis(1));
    }
    let (mut worker, gate, _release) = setup(Outcome::Success, WorkerLimits::default());
    assert_eq!(worker.submit_reclaim(0), Err(WorkerError::BatchTooLarge));
    assert_eq!(
        worker.submit_reclaim(LogLimits::default().max_wal_bytes + 1),
        Err(WorkerError::BatchTooLarge)
    );
    assert!(worker.is_drained());
    // Host stores need not emulate the native checkpoint's minimum frame size.
    assert_eq!(worker.submit_reclaim(1).unwrap().sequence, 1);
    release(&gate);
    assert_eq!(maintenance(&mut worker).result.unwrap().after_bytes, 1);
    drop(finish(&mut worker).unwrap());
}

/// A store whose physical log is "full" for data until a rewrite frees it
/// (when `frees` is set): the shape of `NativeLogStore`'s capacity refusal.
struct FullStore {
    inner: HostLogStore,
    full: bool,
    frees: bool,
    trace: Arc<Mutex<Vec<&'static str>>>,
}
impl LogStore for FullStore {
    fn binding(&self) -> StoreBinding {
        self.inner.binding()
    }
    fn limits(&self) -> LogLimits {
        self.inner.limits()
    }
    fn state(&self, g: GroupIdentity) -> Result<GroupLog, StorageError> {
        self.inner.state(g)
    }
    fn append_batch(&mut self, m: Vec<LogMutation>) -> Result<Vec<LogTicket>, StorageError> {
        self.inner.append_batch(m)
    }
    fn append_batch_within_capacity(
        &mut self,
        m: Vec<LogMutation>,
    ) -> Result<Vec<LogTicket>, AppendRefused> {
        let data = m.iter().any(
            |m| matches!(m, LogMutation::Update(u) if u.suffix.as_ref().is_some_and(|s| !s.entries.is_empty())),
        );
        if self.full && data {
            self.trace.lock().unwrap().push("full");
            return Err(AppendRefused::Full(m));
        }
        self.trace.lock().unwrap().push("append");
        self.inner.append_batch(m).map_err(AppendRefused::Failed)
    }
    fn barrier(&mut self, t: &[LogTicket]) -> Result<DurableLog, StorageError> {
        self.inner.barrier(t)
    }
    fn fetch_range(
        &self,
        g: GroupIdentity,
        v: LogGeneration,
        f: u64,
        n: usize,
        b: usize,
    ) -> Result<Vec<LogEntry>, StorageError> {
        self.inner.fetch_range(g, v, f, n, b)
    }
    fn supports_reclaim(&self) -> bool {
        true
    }
    fn physical_usage(&self) -> Option<WalUsage> {
        Some(WalUsage {
            bytes: if self.full { 1000 } else { 10 },
            capacity: 1000,
            parked: 0,
        })
    }
    fn reclaim(&mut self, _max_bytes: usize) -> Result<LogReclaimed, StorageError> {
        self.trace.lock().unwrap().push("reclaim");
        if self.frees {
            self.full = false;
        }
        Ok(LogReclaimed {
            before_bytes: 1000,
            after_bytes: if self.full { 1000 } else { 10 },
        })
    }
}
fn full_worker(frees: bool) -> (NativeLogWorker<FullStore>, Arc<Mutex<Vec<&'static str>>>) {
    let mut inner = HostLogStore::new(1);
    append(
        &mut inner,
        vec![
            LogMutation::Create(bootstrap(1, 3)),
            LogMutation::Create(bootstrap(2, 3)),
        ],
    );
    let trace = Arc::new(Mutex::new(Vec::new()));
    let worker = NativeLogWorker::spawn(
        FullStore {
            inner,
            full: true,
            frees,
            trace: trace.clone(),
        },
        StorageWorkerGeneration::new(1).unwrap(),
        WorkerLimits::default(),
        Arc::new(ThreadWake::current()),
    )
    .unwrap();
    (worker, trace)
}
fn wait_parked(w: &impl PersistenceWorker, parked: bool) {
    let deadline = Instant::now() + Duration::from_secs(5);
    while (w.wal_usage().unwrap().parked > 0) != parked {
        assert!(Instant::now() < deadline);
        std::thread::park_timeout(Duration::from_millis(1));
    }
}

/// ST2b: a batch that does not fit is held on the WAL thread, never failed:
/// no event at all until a rewrite makes room, control work for other groups
/// still passes, and after the rewrite the held batch is written and durable
/// before the rewrite's own event. No capacity refusal reaches a core.
#[test]
fn a_batch_that_does_not_fit_is_held_until_a_rewrite_and_never_failed() {
    let (mut w, trace) = full_worker(true);
    let data = w.submit(unit(&w, 1, true)).unwrap();
    wait_parked(&w, true);
    assert!(w.poll(8).is_empty(), "a held batch reports nothing");
    assert_eq!(w.usage().requests, 1, "its admission credit is kept");
    // Control work (here a hard-state update for another group) still runs.
    let control = w.submit(unit(&w, 2, false)).unwrap();
    assert!(matches!(event(&mut w), WorkerEvent::Written { request, .. } if request == control));
    assert!(matches!(event(&mut w), WorkerEvent::Durable { request, .. } if request == control));
    // The same group cannot queue behind its held batch.
    assert_eq!(
        w.submit(unit(&w, 1, true)).unwrap_err().reason,
        WorkerError::Overloaded
    );
    let reclaim = w.submit_reclaim(1024).unwrap();
    assert!(matches!(event(&mut w), WorkerEvent::Written { request, .. } if request == data));
    assert!(matches!(event(&mut w), WorkerEvent::Durable { request, .. } if request == data));
    assert_eq!(maintenance(&mut w).request, reclaim);
    wait_parked(&w, false);
    let store = finish(&mut w).unwrap();
    assert_eq!(store.inner.state(group(1)).unwrap().last_index(), 1);
    assert_eq!(
        *trace.lock().unwrap(),
        vec!["full", "append", "reclaim", "append"]
    );
}

/// Closed while a batch is still held and a rewrite cannot free room: the
/// WAL thread rewrites once, then reports the never-written batch as a
/// non-fatal refusal so the owner can finish shutting down.
#[test]
fn a_batch_still_held_at_close_is_refused_after_one_rewrite() {
    let (mut w, trace) = full_worker(false);
    let data = w.submit(unit(&w, 1, true)).unwrap();
    wait_parked(&w, true);
    w.close();
    match event(&mut w) {
        WorkerEvent::Failed { request, error, .. } => {
            assert_eq!(request, data);
            assert_eq!(error, StorageError::Rejected("WAL capacity at close"));
        }
        other => panic!("{other:?}"),
    }
    let store = finish(&mut w).unwrap();
    assert_eq!(store.inner.state(group(1)).unwrap().last_index(), 0);
    assert_eq!(*trace.lock().unwrap(), vec!["full", "reclaim", "full"]);
}
