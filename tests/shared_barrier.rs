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
    collections::BTreeMap,
    sync::{Arc, Condvar, Mutex},
    time::{Duration, Instant},
};
use support::*;
use voteboat::{
    contracts::*,
    identity::*,
    log::*,
    native::{log_store::*, worker::*},
    runtime::*,
    worker::*,
};

#[derive(Clone, Debug, PartialEq)]
enum Step {
    Append(Vec<GroupIdentity>),
    Barrier(Vec<LogTicket>),
    Reclaim,
}
#[derive(Default)]
struct Gate {
    entered: usize,
    released: usize,
    steps: Vec<Step>,
}
type Shared = Arc<(Mutex<Gate>, Condvar)>;
struct Release(Shared);
impl Drop for Release {
    fn drop(&mut self) {
        release(&self.0, usize::MAX);
    }
}
fn release(g: &Shared, n: usize) {
    g.0.lock().unwrap().released = n;
    g.1.notify_all();
}
fn entered(g: &Shared, n: usize) {
    let end = Instant::now() + Duration::from_secs(5);
    let mut lock = g.0.lock().unwrap();
    while lock.entered < n {
        let (next, _) =
            g.1.wait_timeout(lock, end.saturating_duration_since(Instant::now()))
                .unwrap();
        lock = next;
        assert!(
            lock.entered >= n || Instant::now() < end,
            "barrier did not start"
        );
    }
}
#[derive(Clone, Copy, Default)]
enum Reply {
    #[default]
    Exact,
    Reverse,
    Partial,
    Duplicate,
    WrongSession,
    WrongGeneration,
    Failed,
}
struct Gated<L> {
    inner: L,
    gate: Shared,
    reply: Reply,
    fatal_append: bool,
}
impl<L: LogStore> LogStore for Gated<L> {
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
        let groups = m
            .iter()
            .map(|m| match m {
                LogMutation::Create(b) => b.group,
                LogMutation::Update(u) => u.group,
            })
            .collect::<Vec<_>>();
        self.gate
            .0
            .lock()
            .unwrap()
            .steps
            .push(Step::Append(groups.clone()));
        if self.fatal_append && groups.contains(&group(3)) {
            return Err(StorageError::Uncertain("injected append".into()));
        }
        self.inner.append_batch(m)
    }
    fn barrier(&mut self, t: &[LogTicket]) -> Result<DurableLog, StorageError> {
        let mut lock = self.gate.0.lock().unwrap();
        lock.entered += 1;
        let call = lock.entered;
        lock.steps.push(Step::Barrier(t.to_vec()));
        self.gate.1.notify_all();
        while lock.released < call {
            lock = self.gate.1.wait(lock).unwrap();
        }
        drop(lock);
        if call > 1 && matches!(self.reply, Reply::Failed) {
            return Err(StorageError::Uncertain("injected barrier".into()));
        }
        let mut completion = self.inner.barrier(t)?;
        if call > 1 {
            match self.reply {
                Reply::Reverse => completion.tickets.reverse(),
                Reply::Partial => {
                    completion.tickets.pop();
                }
                Reply::Duplicate => completion.tickets[1] = completion.tickets[0],
                Reply::WrongSession => {
                    completion.tickets[0].binding.session = StoreSession::new(999).unwrap()
                }
                Reply::WrongGeneration => {
                    completion.tickets[0].generation = LogGeneration::new(999).unwrap()
                }
                _ => {}
            }
        }
        Ok(completion)
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
    fn reclaim(&mut self, _: usize) -> Result<LogReclaimed, StorageError> {
        self.gate.0.lock().unwrap().steps.push(Step::Reclaim);
        Ok(LogReclaimed {
            before_bytes: 100,
            after_bytes: 50,
        })
    }
}
fn unit<L: LogStore + Send + 'static>(w: &NativeLogWorker<L>, g: u64) -> Vec<PersistUnit> {
    vec![PersistUnit {
        visit: VisitTicket {
            owner: RuntimeOwner {
                store: w.binding().store,
                lane: ExecutionLaneId::new(1).unwrap(),
                generation: RuntimeGeneration::new(1).unwrap(),
            },
            group: group(g.into()),
            sequence: g,
        },
        update: LogUpdate {
            group: group(g.into()),
            expected_revision: LogRevision::new(1).unwrap(),
            hard_state: HardState {
                term: 1,
                voted_for: Some(node(1)),
            },
            commit_index: 0,
            suffix: None,
            snapshot: None,
            snapshot_membership: None,
        },
    }]
}
fn event(w: &mut impl PersistenceWorker) -> WorkerEvent {
    let end = Instant::now() + Duration::from_secs(5);
    loop {
        if let Some(e) = w.poll(1).pop() {
            return e;
        }
        assert!(Instant::now() < end, "worker did not complete");
        std::thread::park_timeout(Duration::from_millis(1));
    }
}
fn finish<L: LogStore + Send + 'static>(mut w: NativeLogWorker<L>) -> L {
    w.close();
    let end = Instant::now() + Duration::from_secs(5);
    loop {
        if let Some(s) = w.try_reclaim().unwrap() {
            return s;
        }
        assert!(Instant::now() < end);
        std::thread::park_timeout(Duration::from_millis(1));
    }
}
fn host(
    limits: WorkerLimits,
    reply: Reply,
    fatal_append: bool,
) -> (NativeLogWorker<Gated<HostLogStore>>, Shared, Release) {
    let mut inner = HostLogStore::new(1);
    append(
        &mut inner,
        (1..=5)
            .map(|g| LogMutation::Create(bootstrap(g, 3)))
            .collect(),
    );
    start(inner, limits, reply, fatal_append)
}
fn start<L: LogStore + Send + 'static>(
    inner: L,
    limits: WorkerLimits,
    reply: Reply,
    fatal_append: bool,
) -> (NativeLogWorker<Gated<L>>, Shared, Release) {
    let gate = Arc::new((Mutex::new(Gate::default()), Condvar::new()));
    let mut w = NativeLogWorker::spawn(
        Gated {
            inner,
            gate: gate.clone(),
            reply,
            fatal_append,
        },
        StorageWorkerGeneration::new(1).unwrap(),
        limits,
        Arc::new(ThreadWake::current()),
    )
    .unwrap();
    w.submit(unit(&w, 1)).unwrap();
    entered(&gate, 1);
    assert!(matches!(event(&mut w), WorkerEvent::Written { .. }));
    (w, gate.clone(), Release(gate))
}
fn collect(
    w: &mut impl PersistenceWorker,
    count: usize,
) -> (BTreeMap<u64, Vec<LogTicket>>, Vec<WorkerEvent>) {
    let mut written = BTreeMap::new();
    let mut terminal = Vec::new();
    while terminal.len() < count {
        let e = event(w);
        if let WorkerEvent::Written {
            request,
            admissions,
        } = e
        {
            assert!(written
                .insert(
                    request.sequence,
                    admissions.into_iter().map(|(_, t)| t).collect()
                )
                .is_none());
        } else {
            terminal.push(e);
        }
    }
    (written, terminal)
}
#[test]
fn queued_requests_share_one_barrier_but_keep_exact_results_and_credits() {
    let (mut w, g, _release) = host(WorkerLimits::default(), Reply::Reverse, false);
    let first = unit(&w, 2);
    let first_visit = first[0].visit;
    let mut second = unit(&w, 3);
    second[0].visit.owner.lane = ExecutionLaneId::new(2).unwrap();
    second[0].visit.owner.generation = RuntimeGeneration::new(2).unwrap();
    let second_visit = second[0].visit;
    let a = w.submit(first).unwrap();
    let b = w.submit(second).unwrap();
    release(&g, 1);
    entered(&g, 2);
    assert!(matches!(event(&mut w), WorkerEvent::Durable { request, .. } if request.sequence==1));
    let mut admissions = BTreeMap::new();
    for expected in [a, b] {
        let WorkerEvent::Written {
            request,
            admissions: actual,
        } = event(&mut w)
        else {
            panic!("expected original append result")
        };
        assert_eq!(request, expected);
        assert_eq!(actual.len(), 1);
        assert_eq!(
            actual[0].0,
            if request == a {
                first_visit
            } else {
                second_visit
            }
        );
        admissions.insert(request.sequence, actual[0].1);
    }
    assert_eq!(w.usage().requests, 2);
    assert!(w.poll(10).is_empty());
    w.close();
    assert!(w.try_reclaim().unwrap().is_none());
    release(&g, usize::MAX);
    for expected in [a, b] {
        let WorkerEvent::Durable {
            request,
            visits,
            completion,
        } = event(&mut w)
        else {
            panic!("shared barrier must complete originals")
        };
        assert_eq!(request, expected);
        assert_eq!(
            visits,
            vec![if request == a {
                first_visit
            } else {
                second_visit
            }]
        );
        assert_eq!(completion.tickets, vec![admissions[&request.sequence]]);
        assert_eq!(visits[0].group, completion.tickets[0].group);
    }
    assert!(w.is_drained());
    let store = finish(w);
    assert_eq!(store.inner.state(group(2)).unwrap().hard_state.term, 1);
    let steps = &g.0.lock().unwrap().steps;
    assert_eq!(
        steps
            .iter()
            .filter(|s| matches!(s, Step::Append(_)))
            .count(),
        3
    );
    let barriers = steps
        .iter()
        .filter_map(|s| {
            if let Step::Barrier(t) = s {
                Some(t.len())
            } else {
                None
            }
        })
        .collect::<Vec<_>>();
    assert_eq!(barriers, vec![1, 2]);
}
#[test]
fn independent_rejected_append_does_not_discard_another_request() {
    let (mut w, g, _release) = host(WorkerLimits::default(), Reply::Exact, false);
    let good = w.submit(unit(&w, 2)).unwrap();
    let mut bad = unit(&w, 3);
    bad[0].update.expected_revision = LogRevision::new(999).unwrap();
    let rejected = w.submit(bad).unwrap();
    release(&g, usize::MAX);
    let (_, terminal) = collect(&mut w, 3);
    assert!(terminal
        .iter()
        .any(|e| matches!(e,WorkerEvent::Durable {request,..} if *request==good)));
    assert!(terminal.iter().any(|e| matches!(e,WorkerEvent::Failed {request,error: StorageError::Rejected(_),..} if *request==rejected)));
    assert_eq!(w.usage().requests, 0);
    w.submit(unit(&w, 4)).unwrap();
    let (_, terminal) = collect(&mut w, 1);
    assert!(matches!(&terminal[0], WorkerEvent::Durable { .. }));
    let store = finish(w);
    assert_eq!(store.inner.state(group(2)).unwrap().hard_state.term, 1);
    assert_eq!(store.inner.state(group(3)).unwrap().hard_state.term, 0);
}
#[test]
fn malformed_or_failed_shared_completion_and_fatal_append_never_acknowledge_pending_work() {
    for (reply, fatal_append) in [
        (Reply::Partial, false),
        (Reply::Duplicate, false),
        (Reply::WrongSession, false),
        (Reply::WrongGeneration, false),
        (Reply::Failed, false),
        (Reply::Exact, true),
    ] {
        let (mut w, g, _release) = host(WorkerLimits::default(), reply, fatal_append);
        let a = w.submit(unit(&w, 2)).unwrap();
        let b = w.submit(unit(&w, 3)).unwrap();
        release(&g, usize::MAX);
        let (_, terminal) = collect(&mut w, 3);
        for request in [a, b] {
            assert!(terminal
                .iter()
                .any(|e| matches!(e,WorkerEvent::Failed {request:actual,..} if *actual==request)));
        }
        assert!(terminal
            .iter()
            .all(|e| !matches!(e,WorkerEvent::Durable {request,..} if *request==a||*request==b)));
        assert_eq!(
            w.submit(unit(&w, 4)).unwrap_err().reason,
            WorkerError::Fenced
        );
        assert!(w.is_drained());
        drop(finish(w));
    }
}
#[test]
fn ready_window_unit_and_byte_bounds_cut_fifo_without_losing_deferred_work() {
    for bytes in [false, true] {
        let mut limits = WorkerLimits::default();
        if bytes {
            limits.batch_bytes = std::mem::size_of::<PersistUnit>()
                + 3 * std::mem::size_of::<VisitTicket>()
                + 2 * std::mem::size_of::<LogTicket>();
        } else {
            limits.batch_units = 2;
        }
        let (mut w, g, _release) = host(limits, Reply::Exact, false);
        for n in 2..=4 {
            w.submit(unit(&w, n)).unwrap();
        }
        w.close();
        release(&g, usize::MAX);
        let (_, terminal) = collect(&mut w, 4);
        assert!(terminal
            .iter()
            .all(|e| matches!(e, WorkerEvent::Durable { .. })));
        drop(finish(w));
        let barriers =
            g.0.lock()
                .unwrap()
                .steps
                .iter()
                .filter_map(|s| {
                    if let Step::Barrier(t) = s {
                        Some(t.iter().map(|t| t.group).collect::<Vec<_>>())
                    } else {
                        None
                    }
                })
                .collect::<Vec<_>>();
        assert_eq!(
            barriers,
            if bytes {
                vec![
                    vec![group(1)],
                    vec![group(2)],
                    vec![group(3)],
                    vec![group(4)],
                ]
            } else {
                vec![vec![group(1)], vec![group(2), group(3)], vec![group(4)]]
            }
        );
    }
}
#[test]
fn queued_reclamation_is_not_crossed_by_shared_barriers() {
    let (mut w, g, _release) = host(WorkerLimits::default(), Reply::Exact, false);
    w.submit(unit(&w, 2)).unwrap();
    let cleanup = w.submit_reclaim(1024).unwrap();
    w.submit(unit(&w, 3)).unwrap();
    w.close();
    release(&g, usize::MAX);
    let (_, terminal) = collect(&mut w, 3);
    assert!(terminal
        .iter()
        .all(|e| matches!(e, WorkerEvent::Durable { .. })));
    let end = Instant::now() + Duration::from_secs(5);
    loop {
        if let Some(e) = w.poll_reclaims(1).pop() {
            assert_eq!(e.request, cleanup);
            assert!(e.result.is_ok());
            break;
        }
        assert!(Instant::now() < end);
        std::thread::park_timeout(Duration::from_millis(1));
    }
    drop(finish(w));
    let steps = &g.0.lock().unwrap().steps;
    assert!(
        matches!(&steps[2..], [Step::Append(a),Step::Barrier(b),Step::Reclaim,Step::Append(c),Step::Barrier(d)] if a==&vec![group(2)]&&b.len()==1&&c==&vec![group(3)]&&d.len()==1)
    );
}

#[derive(Clone, Default)]
struct Memory(Arc<Mutex<Device>>);
impl JournalIo for Memory {
    fn read_manifest(&mut self) -> std::io::Result<Vec<u8>> {
        self.0
            .lock()
            .unwrap()
            .manifest
            .clone()
            .ok_or(std::io::ErrorKind::NotFound.into())
    }
    fn read_log(&mut self, limit: usize) -> std::io::Result<Vec<u8>> {
        let d = self.0.lock().unwrap();
        if d.log.len() > limit {
            return Err(std::io::Error::other("oversized journal"));
        }
        Ok(d.log.clone())
    }
    fn append(&mut self, b: &[u8]) -> std::io::Result<()> {
        self.0.lock().unwrap().log.extend_from_slice(b);
        Ok(())
    }
    fn sync_log(&mut self) -> std::io::Result<()> {
        let mut d = self.0.lock().unwrap();
        d.synced = d.log.clone();
        Ok(())
    }
    fn truncate_log(&mut self, n: u64) -> std::io::Result<()> {
        self.0.lock().unwrap().log.truncate(n as usize);
        Ok(())
    }
    fn publish_manifest(&mut self, b: &[u8]) -> std::io::Result<()> {
        self.0.lock().unwrap().manifest = Some(b.to_vec());
        Ok(())
    }
}
#[derive(Clone, Copy, Debug)]
enum Cut {
    None,
    Append(usize, usize),
    Sync,
    AfterSync,
    PublishBefore,
    PublishAfter,
}
struct Injected<I> {
    inner: I,
    cut: Cut,
    appends: usize,
    syncs: usize,
    publications: usize,
}
impl<I> Injected<I> {
    fn new(inner: I, cut: Cut) -> Self {
        Self {
            inner,
            cut,
            appends: 0,
            syncs: 0,
            publications: 0,
        }
    }
}
impl<I: JournalIo> JournalIo for Injected<I> {
    fn read_manifest(&mut self) -> std::io::Result<Vec<u8>> {
        self.inner.read_manifest()
    }
    fn read_log(&mut self, n: usize) -> std::io::Result<Vec<u8>> {
        self.inner.read_log(n)
    }
    fn append(&mut self, b: &[u8]) -> std::io::Result<()> {
        self.appends += 1;
        if let Cut::Append(call, cut) = self.cut {
            if self.appends == call {
                self.inner.append(&b[..cut.min(b.len())])?;
                return Err(std::io::Error::other("injected partial append"));
            }
        }
        self.inner.append(b)
    }
    fn sync_log(&mut self) -> std::io::Result<()> {
        self.syncs += 1;
        if self.syncs == 4 && matches!(self.cut, Cut::Sync) {
            return Err(std::io::Error::other("injected sync"));
        }
        self.inner.sync_log()?;
        if self.syncs == 4 && matches!(self.cut, Cut::AfterSync) {
            return Err(std::io::Error::other("injected after sync"));
        }
        Ok(())
    }
    fn publish_manifest(&mut self, b: &[u8]) -> std::io::Result<()> {
        self.publications += 1;
        if self.publications == 4 && matches!(self.cut, Cut::PublishBefore) {
            return Err(std::io::Error::other("injected publication"));
        }
        self.inner.publish_manifest(b)?;
        if self.publications == 4 && matches!(self.cut, Cut::PublishAfter) {
            return Err(std::io::Error::other("injected after publication"));
        }
        Ok(())
    }
    fn truncate_log(&mut self, n: u64) -> std::io::Result<()> {
        self.inner.truncate_log(n)
    }
    fn supports_replacement(&self) -> bool {
        self.inner.supports_replacement()
    }
    fn replace_log(&mut self, b: &[u8], m: &[u8]) -> std::io::Result<()> {
        self.inner.replace_log(b, m)
    }
}
fn boot<I: JournalIo>(io: I) -> NativeLogStore<I> {
    let mut store = NativeLogStore::create(io, identity(1), LogLimits::default()).unwrap();
    append(
        &mut store,
        (1..=3)
            .map(|g| LogMutation::Create(bootstrap(g, 3)))
            .collect(),
    );
    store
}
fn failed_window<L: LogStore + Send + 'static>(store: L) -> L {
    let (mut w, g, _release) = start(store, WorkerLimits::default(), Reply::Exact, false);
    let a = w.submit(unit(&w, 2)).unwrap();
    let b = w.submit(unit(&w, 3)).unwrap();
    release(&g, usize::MAX);
    let (_, terminal) = collect(&mut w, 3);
    assert!(terminal
        .iter()
        .any(|e| matches!(e,WorkerEvent::Durable {request,..} if request.sequence==1)));
    for original in [a, b] {
        assert!(terminal
            .iter()
            .any(|e| matches!(e,WorkerEvent::Failed {request,..} if *request==original)));
        assert!(!terminal
            .iter()
            .any(|e| matches!(e,WorkerEvent::Durable {request,..} if *request==original)));
    }
    assert!(w.is_drained());
    finish(w).inner
}
fn vote(g: u128) -> LogUpdate {
    LogUpdate {
        group: group(g),
        expected_revision: LogRevision::new(1).unwrap(),
        hard_state: HardState {
            term: 1,
            voted_for: Some(node(1)),
        },
        commit_index: 0,
        suffix: None,
        snapshot: None,
        snapshot_membership: None,
    }
}
#[test]
fn native_framing_every_byte_cut_of_either_shared_record_preserves_acknowledged_promises() {
    let size = NativeLogCodec
        .encode_batch(3, &[LogMutation::Update(vote(2))], LogLimits::default())
        .unwrap()
        .len();
    let mut cuts = vec![
        Cut::Sync,
        Cut::AfterSync,
        Cut::PublishBefore,
        Cut::PublishAfter,
    ];
    for call in [3, 4] {
        for cut in 0..=size {
            cuts.push(Cut::Append(call, cut));
        }
    }
    for cut in cuts {
        for retain_unacknowledged_tail in [false, true] {
            let memory = Memory::default();
            let store = boot(Injected::new(memory.clone(), cut));
            drop(failed_window(store));
            {
                let mut d = memory.0.lock().unwrap();
                if retain_unacknowledged_tail {
                    d.synced = d.log.clone();
                }
                d.power_loss();
            }
            let recovered =
                NativeLogStore::recover(memory, identity(1), LogLimits::default()).unwrap();
            assert_eq!(
                recovered.state(group(1)).unwrap().hard_state,
                HardState {
                    term: 1,
                    voted_for: Some(node(1))
                },
                "{cut:?}"
            );
            for g in [2, 3] {
                let state = recovered.state(group(g)).unwrap();
                assert!(state.hard_state.term <= 1);
                assert_eq!(
                    state.hard_state.voted_for,
                    if state.hard_state.term == 1 {
                        Some(node(1))
                    } else {
                        None
                    }
                );
                voteboat::raft::Raft::recover(
                    node(1),
                    recovered.binding(),
                    state,
                    recovered.limits(),
                )
                .unwrap();
            }
            if matches!(cut, Cut::AfterSync | Cut::PublishBefore | Cut::PublishAfter) {
                assert_eq!(recovered.state(group(2)).unwrap().hard_state.term, 1);
                assert_eq!(recovered.state(group(3)).unwrap().hard_state.term, 1);
            }
        }
    }
    eprintln!(
        "shared native record_bytes={size} crash_schedules={}",
        2 * (4 + 2 * (size + 1))
    );
}
#[test]
fn actual_file_partial_append_sync_and_publication_failures_reopen_without_false_acknowledgements()
{
    let size = NativeLogCodec
        .encode_batch(3, &[LogMutation::Update(vote(2))], LogLimits::default())
        .unwrap()
        .len();
    let cuts = [
        Cut::Append(3, 0),
        Cut::Append(3, size / 2),
        Cut::Append(4, size / 2),
        Cut::Append(4, size),
        Cut::Sync,
        Cut::AfterSync,
        Cut::PublishBefore,
        Cut::PublishAfter,
    ];
    for (i, cut) in cuts.into_iter().enumerate() {
        let root =
            std::env::temp_dir().join(format!("voteboat-shared-cut-{}-{i}", std::process::id()));
        let store = boot(Injected::new(FileLogIo::create(&root).unwrap(), cut));
        drop(failed_window(store));
        let recovered = NativeLogStore::recover(
            FileLogIo::open(&root).unwrap(),
            identity(1),
            LogLimits::default(),
        )
        .unwrap();
        assert_eq!(
            recovered.state(group(1)).unwrap().hard_state.term,
            1,
            "{cut:?}"
        );
        for g in 1..=3 {
            voteboat::raft::Raft::recover(
                node(1),
                recovered.binding(),
                recovered.state(group(g)).unwrap(),
                recovered.limits(),
            )
            .unwrap();
        }
        drop(recovered);
        std::fs::remove_dir_all(root).unwrap();
    }
}
#[test]
fn store_dependency_limit_bounds_the_shared_window() {
    let mut store = NativeLogStore::create(
        Injected::new(Memory::default(), Cut::None),
        identity(1),
        LogLimits {
            max_batch_units: 1,
            max_pending_units: 1,
            ..LogLimits::default()
        },
    )
    .unwrap();
    for g in 1..=3 {
        append(&mut store, vec![LogMutation::Create(bootstrap(g, 3))]);
    }
    let (mut w, g, _release) = start(
        store,
        WorkerLimits {
            batch_units: 1,
            ..WorkerLimits::default()
        },
        Reply::Exact,
        false,
    );
    w.submit(unit(&w, 2)).unwrap();
    w.submit(unit(&w, 3)).unwrap();
    release(&g, usize::MAX);
    let (_, terminal) = collect(&mut w, 3);
    assert!(terminal
        .iter()
        .all(|e| matches!(e, WorkerEvent::Durable { .. })));
    drop(finish(w));
    assert_eq!(
        g.0.lock()
            .unwrap()
            .steps
            .iter()
            .filter_map(|s| if let Step::Barrier(t) = s {
                Some(t.len())
            } else {
                None
            })
            .collect::<Vec<_>>(),
        vec![1, 1, 1]
    );
}
