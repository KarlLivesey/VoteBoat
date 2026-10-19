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
//! ST2b on the native store, opened through the binaries' storage path
//! (`NativeStartup::prepare_for_discovery`): a write burst that outruns the
//! WAL rewrite is retryable backpressure, never a node failure. Protocol time
//! is simulated; the wall clock only bounds a hang.
use super::retention::{write, Padded, PADDED};
use super::*;
use std::sync::atomic::{AtomicBool, Ordering};
use voteboat::{
    native::{
        connect::{NativePeerProtocol, NativeServiceConnector},
        log_store::{FileLogIo, NativeLogStore},
        outbound::NativeOutbound,
        runtime::{DeadlineQueue, FairScheduler, JitterEntropy},
        snapshot_store::{FileSnapshotIo, NativeSnapshotStore},
        snapshot_worker::NativeSnapshotWorker,
        transport::NativeTransportFactory,
        wire::NativeWireCodec,
    },
    raft::{Raft, Role},
    worker::*,
};

/// The native WAL worker with its rewrite held back while `hold` is set:
/// every reclaim submission is refused as `Overloaded` (maintenance retries
/// it after `retry_ms`), so writes outrun the drain for as long as the test
/// keeps the hold. Everything else is the unchanged native worker.
struct SlowRewrite {
    inner: NativeLogWorker<NativeLogStore<FileLogIo>>,
    hold: Arc<AtomicBool>,
}
impl PersistenceWorker for SlowRewrite {
    fn binding(&self) -> WorkerBinding {
        self.inner.binding()
    }
    fn limits(&self) -> WorkerLimits {
        self.inner.limits()
    }
    fn usage(&self) -> WorkerUsage {
        self.inner.usage()
    }
    fn submit(&mut self, units: Vec<PersistUnit>) -> Result<WorkerTicket, WorkerRejected> {
        self.inner.submit(units)
    }
    fn poll(&mut self, limit: usize) -> Vec<WorkerEvent> {
        self.inner.poll(limit)
    }
    fn reclaim_limit(&self) -> Option<usize> {
        self.inner.reclaim_limit()
    }
    fn wal_usage(&self) -> Option<WalUsage> {
        self.inner.wal_usage()
    }
    fn submit_reclaim(&mut self, max_bytes: usize) -> Result<ReclaimTicket, WorkerError> {
        if self.hold.load(Ordering::Relaxed) {
            return Err(WorkerError::Overloaded);
        }
        self.inner.submit_reclaim(max_bytes)
    }
    fn poll_reclaims(&mut self, limit: usize) -> Vec<ReclaimEvent> {
        self.inner.poll_reclaims(limit)
    }
    fn close(&mut self) {
        self.inner.close()
    }
    fn is_drained(&self) -> bool {
        self.inner.is_drained()
    }
}
type Held = Node<
    FairScheduler,
    DeadlineQueue,
    JitterEntropy,
    Padded,
    SlowRewrite,
    NativeOutbound,
    NativeSnapshotWorker<NativeSnapshotStore<FileSnapshotIo>>,
    NativeServiceConnector,
    NativeTransportFactory<NativeWireCodec>,
>;
/// The binaries' storage and peer assembly, with the WAL worker wrapped.
fn open(config: NativeStartup, hold: Arc<AtomicBool>) -> Held {
    let limits = config.limits;
    let parts = config
        .prepare_for_discovery(
            NativePeerProtocol::TcpTls,
            TimerConfig::default(),
            Padded(app()),
            Arc::new(ThreadWake::current()),
            MonoTime(0),
        )
        .unwrap_or_else(|e| panic!("{:?}", e.reason));
    let NetworkNodeParts { local, peers } = parts;
    let NodeLocalParts {
        owner,
        persistence,
        applications,
        results,
        clients,
        reads,
        outbound,
        snapshots,
    } = local;
    Node::from_parts(
        NetworkNodeParts {
            local: NodeLocalParts {
                owner,
                persistence: SlowRewrite {
                    inner: persistence,
                    hold,
                },
                applications,
                results,
                clients,
                reads,
                outbound,
                snapshots,
            },
            peers,
        },
        limits,
        MonoTime(0),
    )
    .unwrap_or_else(|r| panic!("{:?}", r.reason))
}
fn core(n: &Held) -> &Raft {
    n.local().owner.core(group()).unwrap()
}
/// Per-node state for a stall report.
fn dump(nodes: &[Held]) -> String {
    nodes
        .iter()
        .enumerate()
        .map(|(i, n)| {
            let c = core(n);
            let w = n.wal_maintenance();
            format!(
                "\nnode {}: {:?} term={} last={} commit={} base={} applied={} pending_dependency={} wal={:?} high_water={} reclaim_pending={} failures={} last_reclaim={:?} checkpoints_pending={} checkpoint_last={:?} worker={:?}",
                i + 1,
                c.role(),
                c.state().hard_state.term,
                c.state().last_index(),
                c.state().commit_index,
                c.state().base_index(),
                n.local().applications[&group()].applied_index(),
                c.has_pending_dependency(),
                n.wal_usage(),
                w.high_water,
                w.pending.is_some(),
                w.consecutive_failures,
                w.last_reclaimed,
                n.checkpoints().pending.len(),
                n.checkpoints().last_result,
                n.local().persistence.usage(),
            )
        })
        .collect()
}
/// Simulated protocol time: one millisecond per `POLLS_PER_MS` rounds.
const POLLS_PER_MS: u64 = 10;
struct Clock {
    now: u64,
    rounds: u64,
    hang: Instant,
    /// Every node must stay `Running` until the test begins shutdown.
    serving: bool,
}
impl Clock {
    fn new() -> Self {
        Self {
            now: 0,
            rounds: 0,
            hang: Instant::now() + Duration::from_secs(600),
            serving: true,
        }
    }
    /// One round over every node. Any node error (the node-fatal WAL refusal
    /// on the base) fails the test with the node's own error.
    fn poll(&mut self, nodes: &mut [Held]) {
        self.rounds += 1;
        if self.rounds.is_multiple_of(POLLS_PER_MS) {
            self.now += 1;
        }
        let mut progress = false;
        for (i, n) in nodes.iter_mut().enumerate() {
            let p = n
                .poll(MonoTime(self.now), NodePollBudget::default())
                .unwrap_or_else(|e| panic!("node {} at {} ms: {e:?}", i + 1, self.now));
            progress |= p.made_progress();
            if self.serving {
                assert_eq!(n.state(), NodeState::Running, "node {}", i + 1);
            }
        }
        if !progress {
            std::thread::park_timeout(Duration::from_micros(200));
        }
        assert!(Instant::now() < self.hang, "hang at {} ms", self.now);
    }
}
fn stop(nodes: Vec<Held>, clock: &mut Clock) {
    let mut nodes = nodes;
    clock.serving = false;
    for n in &mut nodes {
        n.begin_shutdown();
    }
    while nodes.iter().any(|n| !n.is_drained()) {
        clock.poll(&mut nodes);
    }
    for n in nodes {
        let mut parts = n.into_parts().unwrap_or_else(|_| panic!("not drained"));
        let mut dialer = parts
            .peers
            .take()
            .unwrap()
            .connector
            .into_dialer()
            .unwrap_or_else(|_| panic!("dialer"));
        let mut local = parts.local;
        let mut snapshots = local.snapshots.take().unwrap();
        let hang = Instant::now() + Duration::from_secs(60);
        let (mut log, mut snap, mut dial) = (false, false, false);
        while !(log && snap && dial) {
            dial = dial || dialer.as_mut().is_none_or(|d| d.try_finish().unwrap());
            log = log || local.persistence.inner.try_reclaim().unwrap().is_some();
            snap = snap || snapshots.worker.try_reclaim().unwrap().is_some();
            assert!(Instant::now() < hang, "release");
            std::thread::park_timeout(Duration::from_millis(1));
        }
    }
}
/// Writes offered at most `WINDOW` ahead of completion. Every accepted write
/// must be applied; a refusal must be the typed retryable backpressure error.
#[derive(Default)]
struct Writes {
    issued: u64,
    applied: u64,
    progress_ms: u64,
    refused: u64,
    first_refusal_ms: Option<u64>,
    last_refusal_ms: Option<u64>,
}
const WINDOW: u64 = 64;
impl Writes {
    fn offer(&mut self, n: &mut Held, total: u64, now: u64) {
        while self.issued < total && self.issued - self.applied < WINDOW {
            match n.propose(write(self.issued)) {
                Ok(_) => self.issued += 1,
                Err(rejected) => {
                    assert!(
                        matches!(rejected.reason, ClientError::StoragePressure),
                        "write {} refused at {now} ms: {:?}",
                        self.issued,
                        rejected.reason
                    );
                    self.refused += 1;
                    self.first_refusal_ms.get_or_insert(now);
                    self.last_refusal_ms = Some(now);
                    return;
                }
            }
        }
    }
    /// No write applied for five simulated seconds is a stall.
    fn check(&mut self, nodes: &[Held], now: u64, applied: u64) {
        if applied != self.applied {
            self.progress_ms = now;
        }
        assert!(
            now < self.progress_ms + 5_000,
            "stalled at {now} ms with {} applied, {} issued:{}",
            self.applied,
            self.issued,
            dump(nodes)
        );
    }
    fn complete(&mut self, n: &mut Held) {
        while let Some(output) = n.poll_client() {
            match n.complete_client(output).unwrap() {
                ClientOutcome::Applied { .. } => self.applied += 1,
                other => panic!(
                    "accepted write {} not applied at log index {}: {other:?}",
                    self.applied,
                    core(n).state().last_index()
                ),
            }
        }
    }
}

/// Fail@47646b4c: with the rewrite held back, 4 KiB writes fill the WAL and
/// the node fails at append time with
/// `Replica(Owner(Worker(Consensus(Storage(Rejected("WAL capacity/control reserve"))))))`
/// (recorded in docs/IMPLEMENTATION.md). With ST2b, proposals are refused
/// with `StoragePressure` once the WAL passes its high-water mark, the node
/// stays `Running` and `Leader`, every accepted write is applied, the WAL
/// never passes its cap, and writes resume after the held rewrite runs.
#[test]
fn write_burst_past_a_slow_rewrite_is_backpressure_not_a_node_failure() {
    // 96 MiB of 4 KiB writes, 1.5 times the WAL.
    const TOTAL: u64 = 24_576;
    // How long the rewrite stays held once refusals start.
    const HELD_MS: u64 = 1_000;
    let directory = root();
    let hold = Arc::new(AtomicBool::new(true));
    let mut nodes = vec![open(
        config(directory.clone(), NativeOpenMode::Create),
        hold.clone(),
    )];
    let mut clock = Clock::new();
    nodes[0].control(group(), NodeControl::Campaign).unwrap();
    while core(&nodes[0]).role() != Role::Leader {
        clock.poll(&mut nodes);
    }
    let term = core(&nodes[0]).state().hard_state.term;
    let usage = nodes[0].wal_usage().unwrap();
    let cap = usage.capacity + LogLimits::default().control_reserve_bytes;
    let (mut peak, mut peak_held, mut released_ms) = (0, 0, None);
    let mut writes = Writes::default();
    while writes.applied < TOTAL {
        writes.offer(&mut nodes[0], TOTAL, clock.now);
        clock.poll(&mut nodes);
        let applied = writes.applied;
        writes.complete(&mut nodes[0]);
        writes.check(&nodes, clock.now, applied);
        let u = nodes[0].wal_usage().unwrap();
        peak = peak.max(u.bytes);
        assert!(u.bytes <= cap, "WAL {} past its cap {cap}", u.bytes);
        if released_ms.is_none() {
            peak_held = peak;
            if writes
                .first_refusal_ms
                .is_some_and(|t| clock.now >= t + HELD_MS)
            {
                hold.store(false, Ordering::Relaxed);
                released_ms = Some(clock.now);
            }
        }
    }
    let n = &nodes[0];
    let status = n.wal_maintenance();
    assert!(writes.refused > 0, "no backpressure");
    let released = released_ms.expect("hold never released");
    assert_eq!(writes.issued, TOTAL);
    // Writes resumed after the rewrite: refusals stopped and the rest applied.
    assert!(writes.last_refusal_ms.unwrap() >= released);
    assert!(status.high_water_entries >= 1);
    assert_eq!(status.refused_proposals, writes.refused);
    assert!(status.last_reclaimed.is_some_and(|(t, _)| t.0 >= released));
    assert_eq!(core(n).role(), Role::Leader);
    assert_eq!(core(n).state().hard_state.term, term);
    eprintln!(
        "ST2b native burst: writes={TOTAL} bytes={} refused={} first_refusal_ms={:?} released_ms={released} last_refusal_ms={:?} peak_wal_while_held={peak_held} peak_wal={peak} cap={cap} usable={} high_water_entries={} last_reclaim={:?} simulated_ms={}",
        TOTAL * PADDED as u64,
        writes.refused,
        writes.first_refusal_ms,
        writes.last_refusal_ms,
        usage.capacity,
        status.high_water_entries,
        status.last_reclaimed,
        clock.now
    );
    stop(nodes, &mut clock);
    std::fs::remove_dir_all(directory).unwrap();
}

fn cluster_bootstrap() -> Bootstrap {
    let stores = (1..=3u64)
        .map(|n| {
            (
                NodeId::new(n).unwrap(),
                StoreIdentity {
                    id: StoreId::new(n.into()).unwrap(),
                    incarnation: StoreIncarnation::new(1).unwrap(),
                },
            )
        })
        .collect::<BTreeMap<_, _>>();
    Bootstrap {
        group: group(),
        configuration: ConfigurationId::new(9).unwrap(),
        policy: Policy::new(
            Tree::Majority(stores.keys().copied().map(Tree::Voter).collect()),
            Limits::default(),
        )
        .unwrap(),
        voter_stores: stores,
    }
}

/// A follower whose rewrite is held fills its WAL with replicated data. With
/// ST2b the follower, once above its high-water mark, keeps leader contact
/// and commit but declines new data (the leader resends on its heartbeat), so
/// no node fails, no election happens, the leader commits with the other
/// follower, and the follower catches up once its rewrite runs. On 47646b4c
/// the follower fails with the same node-fatal WAL refusal.
#[test]
fn follower_with_a_slow_rewrite_declines_new_data_instead_of_failing() {
    const TOTAL: u64 = 20_480;
    const HELD_MS: u64 = 1_000;
    let directory = root();
    std::fs::create_dir(&directory).unwrap();
    let sockets = (0..3)
        .map(|_| std::net::TcpListener::bind("127.0.0.1:0").unwrap())
        .collect::<Vec<_>>();
    let addresses = sockets
        .iter()
        .map(|s| s.local_addr().unwrap())
        .collect::<Vec<_>>();
    drop(sockets);
    let bootstrap = cluster_bootstrap();
    let holds = (0..3)
        .map(|i| Arc::new(AtomicBool::new(i == 2)))
        .collect::<Vec<_>>();
    let mut nodes = (1..=3)
        .map(|n| {
            let c = selected_wire_config(
                &directory,
                &addresses,
                &bootstrap,
                n,
                NativeOpenMode::Create,
                8,
            );
            open(c, holds[n - 1].clone())
        })
        .collect::<Vec<_>>();
    let mut clock = Clock::new();
    nodes[0].control(group(), NodeControl::Campaign).unwrap();
    while !(core(&nodes[0]).role() == Role::Leader
        && nodes
            .iter()
            .all(|n| core(n).state().commit_index == core(&nodes[0]).state().last_index()))
    {
        clock.poll(&mut nodes);
    }
    let term = core(&nodes[0]).state().hard_state.term;
    let cap = nodes[2].wal_usage().unwrap().capacity + LogLimits::default().control_reserve_bytes;
    let (mut pressure_ms, mut released_ms, mut peak, mut parked) = (None, None, 0, 0);
    // Release the follower's rewrite once it has been above its high-water
    // mark for HELD_MS.
    let release = |nodes: &[Held], now, pressure: &mut Option<u64>, released: &mut Option<u64>| {
        if nodes[2].wal_maintenance().high_water {
            pressure.get_or_insert(now);
        }
        if released.is_none() && pressure.is_some_and(|t| now >= t + HELD_MS) {
            holds[2].store(false, Ordering::Relaxed);
            *released = Some(now);
        }
    };
    let mut writes = Writes::default();
    while writes.applied < TOTAL {
        writes.offer(&mut nodes[0], TOTAL, clock.now);
        clock.poll(&mut nodes);
        let applied = writes.applied;
        writes.complete(&mut nodes[0]);
        writes.check(&nodes, clock.now, applied);
        for n in &nodes {
            let u = n.wal_usage().unwrap();
            peak = peak.max(u.bytes);
            parked = parked.max(u.parked);
            assert!(u.bytes <= cap, "WAL {} past its cap {cap}", u.bytes);
        }
        assert_eq!(core(&nodes[0]).role(), Role::Leader);
        release(&nodes, clock.now, &mut pressure_ms, &mut released_ms);
    }
    let leader_last = core(&nodes[0]).state().last_index();
    let caught_up = clock.now;
    while core(&nodes[2]).state().commit_index < leader_last {
        clock.poll(&mut nodes);
        release(&nodes, clock.now, &mut pressure_ms, &mut released_ms);
        assert!(
            clock.now < caught_up + 10_000,
            "follower never caught up:{}",
            dump(&nodes)
        );
    }
    let pressure = pressure_ms.expect("the follower never reached high water");
    let released = released_ms.expect("hold never released");
    assert_eq!(core(&nodes[0]).role(), Role::Leader);
    assert_eq!(core(&nodes[0]).state().hard_state.term, term);
    assert_eq!(nodes[2].wal_usage().unwrap().parked, 0);
    assert!(!nodes[2].wal_maintenance().high_water);
    assert_eq!(core(&nodes[2]).state().hard_state.term, term);
    assert!(nodes[2]
        .wal_maintenance()
        .last_reclaimed
        .is_some_and(|(t, _)| t.0 >= released));
    eprintln!(
        "ST2b follower pressure: writes={TOTAL} leader_refused={} follower_high_water_ms={pressure} released_ms={released} peak_wal={peak} max_parked={parked} cap={cap} leader_last={leader_last} follower_commit={} term={term} simulated_ms={}",
        writes.refused,
        core(&nodes[2]).state().commit_index,
        clock.now
    );
    stop(nodes, &mut clock);
    std::fs::remove_dir_all(directory).unwrap();
}
