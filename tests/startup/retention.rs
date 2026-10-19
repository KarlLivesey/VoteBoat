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
//! ST2 on the native store, opened through `NativeStartup` like the three
//! service binaries, with protocol time simulated: one millisecond per poll.
use super::*;
use voteboat::{native::connect::NativeServiceConnector, raft::Role};

/// A canonical zero-padded Counter command: each write puts 4 KiB in the WAL,
/// so 64 MiB passes in about 16,000 writes. Capacity probe only.
pub(super) const PADDED: usize = 4096;
const ENTRY_LIMIT: u64 = 16_384;
const WINDOW: u64 = 64;
#[derive(Clone)]
pub(super) struct Padded(pub(super) HostApplication);
/// Unpadded 8-byte deltas are accepted too (the absent-follower soak uses
/// them so the retained-entry budget, not the WAL, is what fills).
fn canonical(bytes: &[u8]) -> Result<&[u8], ApplicationError> {
    if bytes.len() == 8 {
        return Ok(bytes);
    }
    if bytes.len() != PADDED || bytes[8..].iter().any(|b| *b != 0) {
        return Err(ApplicationError::InvalidCommand);
    }
    Ok(&bytes[..8])
}
fn entries(input: &[LogEntry]) -> Result<Vec<LogEntry>, ApplicationError> {
    input
        .iter()
        .map(|original| {
            let mut entry = original.clone();
            if let EntryPayload::Command { bytes, .. } = &mut entry.payload {
                *bytes = canonical(bytes)?.into();
            }
            Ok(entry)
        })
        .collect()
}
impl StateMachine for Padded {
    type Receipt = CounterReceipt;
    fn applied_index(&self) -> u64 {
        self.0.applied_index()
    }
    fn apply_batch(&mut self, input: &[LogEntry]) -> Result<Vec<CounterReceipt>, ApplicationError> {
        self.0.apply_batch(&entries(input)?)
    }
}
impl BoundedStateMachine for Padded {
    fn receipt_bytes_bound(&self, input: &[LogEntry]) -> Result<usize, ApplicationError> {
        self.0.receipt_bytes_bound(&entries(input)?)
    }
}
impl ProposalAdmission for Padded {
    fn validate_proposal<'a>(
        &self,
        operation: OperationId,
        bytes: &[u8],
        pending: impl Iterator<Item = (OperationId, &'a [u8])>,
    ) -> Result<usize, ApplicationError> {
        let bytes = canonical(bytes)?;
        let pending = pending
            .map(|(o, b)| canonical(b).map(|b| (o, b)))
            .collect::<Result<Vec<_>, _>>()?;
        self.0
            .validate_proposal(operation, bytes, pending.into_iter())
    }
}
impl ReadableStateMachine for Padded {
    type Query = ();
    type ReadResult = i64;
    fn read_at(&self, index: u64, query: ()) -> Result<i64, ApplicationError> {
        self.0.read_at(index, query)
    }
}
impl BoundedReadableStateMachine for Padded {
    fn query_bytes(&self, query: &(), limit: usize) -> Result<usize, ApplicationError> {
        self.0.query_bytes(query, limit)
    }
    fn read_result_bound(&self, query: &()) -> Result<usize, ApplicationError> {
        self.0.read_result_bound(query)
    }
    fn read_result_bytes(&self, result: &i64, limit: usize) -> Result<usize, ApplicationError> {
        self.0.read_result_bytes(result, limit)
    }
}
impl CheckpointStateMachine for Padded {
    fn schema_version(&self) -> u64 {
        self.0.schema_version()
    }
    fn checkpoint(&self, limit: usize) -> Result<Vec<u8>, ApplicationError> {
        self.0.checkpoint(limit)
    }
    fn restore_checkpoint(
        &mut self,
        schema: u64,
        index: u64,
        bytes: &[u8],
    ) -> Result<(), ApplicationError> {
        self.0.restore_checkpoint(schema, index, bytes)
    }
}
type Boat = NativeNode<Padded, NativeServiceConnector>;
/// Write `k`: operation IDs cycle through the application's capacity (100),
/// so repeats are client retries and every write is a new log entry.
pub(super) fn write(k: u64) -> ClientRequest {
    let mut bytes = vec![0; PADDED];
    bytes[..8].copy_from_slice(&1i64.to_le_bytes());
    ClientRequest {
        group: group(),
        operation: OperationId::new(u128::from(k % 100 + 1)).unwrap(),
        bytes,
    }
}
/// The service binaries' open path (`service_setup::open_application`).
fn open(directory: &std::path::Path, mode: NativeOpenMode) -> Boat {
    config(directory.to_path_buf(), mode)
        .open_with_protocol_and_timers(
            voteboat::native::connect::NativePeerProtocol::TcpTls,
            TimerConfig::default(),
            Padded(app()),
            Arc::new(ThreadWake::current()),
            MonoTime(0),
        )
        .unwrap_or_else(|e| panic!("{:?}", e.reason))
}
/// Simulated protocol time, one millisecond per `polls_per_ms` polls; the
/// wall clock only bounds a hang.
struct Clock {
    now: u64,
    polls: u64,
    polls_per_ms: u64,
    hang: Instant,
}
impl Clock {
    fn new() -> Self {
        Self::with_rate(1)
    }
    fn with_rate(polls_per_ms: u64) -> Self {
        Self {
            now: 0,
            polls: 0,
            polls_per_ms,
            hang: Instant::now() + Duration::from_secs(600),
        }
    }
    fn poll(&mut self, n: &mut Boat) {
        self.polls += 1;
        if self.polls.is_multiple_of(self.polls_per_ms) {
            self.now += 1;
        }
        let progress = n
            .poll(MonoTime(self.now), NodePollBudget::default())
            .unwrap_or_else(|e| panic!("poll at {} ms: {e:?}", self.now));
        if !progress.made_progress() {
            // Worker threads own the I/O; give them the core.
            std::thread::park_timeout(Duration::from_micros(200));
        }
        assert!(Instant::now() < self.hang, "hang at {} ms", self.now);
    }
}
fn core(n: &Boat) -> &voteboat::raft::Raft {
    n.local().owner.core(group()).unwrap()
}
fn retained(n: &Boat) -> u64 {
    core(n).state().last_index() - core(n).state().base_index()
}
fn elect(n: &mut Boat, clock: &mut Clock) {
    if core(n).role() != Role::Leader {
        n.control(group(), NodeControl::Campaign).unwrap();
    }
    while !(core(n).role() == Role::Leader
        && core(n).state().last_term() == core(n).state().hard_state.term
        && n.local().applications[&group()].applied_index() == core(n).state().last_index())
    {
        clock.poll(n);
    }
}
fn drive(n: &mut Boat, clock: &mut Clock, first: u64, total: u64, mut round: impl FnMut(&Boat)) {
    let (mut issued, mut done) = (0, 0);
    while done < total {
        while issued < total && issued - done < WINDOW {
            if n.propose(write(first + issued)).is_err() {
                break;
            }
            issued += 1;
        }
        clock.poll(n);
        round(n);
        while let Some(output) = n.poll_client() {
            match n.complete_client(output).unwrap() {
                ClientOutcome::Applied { .. } => done += 1,
                other => panic!(
                    "write {} not applied at log index {} (retained {}, wal {:?}): {other:?}",
                    first + done,
                    core(n).state().last_index(),
                    retained(n),
                    n.wal_usage()
                ),
            }
        }
    }
}
fn stop(mut n: Boat, clock: &mut Clock) {
    n.begin_shutdown();
    while !n.is_drained() {
        clock.poll(&mut n);
    }
    let mut parts = n.into_parts().unwrap_or_else(|_| panic!("not drained"));
    let dialer = parts
        .peers
        .take()
        .unwrap()
        .connector
        .into_dialer()
        .unwrap_or_else(|_| panic!("dialer"));
    release(parts.local, dialer);
}
/// Wait for the node's providers to return their files (and the dialer).
fn release(
    mut local: voteboat::native::node::NativeLocalParts<Padded>,
    dialer: Option<voteboat::native::dial::NativeTcpDialer>,
) {
    let mut dialer = dialer;
    let mut snapshots = local.snapshots.take().unwrap();
    let (mut log, mut snap) = (false, false);
    let hang = Instant::now() + Duration::from_secs(60);
    loop {
        let dial = dialer.as_mut().is_none_or(|d| d.try_finish().unwrap());
        if !log {
            // An aborted host still takes the terminal events of accepted
            // writes and of an in-flight rewrite before the store returns.
            use voteboat::worker::PersistenceWorker;
            local.persistence.poll(64);
            local.persistence.poll_reclaims(1);
        }
        log = log || local.persistence.try_reclaim().unwrap().is_some();
        if !snap {
            // Likewise an aborted checkpoint's snapshot work.
            use voteboat::snapshot_worker::SnapshotWorker;
            snapshots.worker.poll(64);
        }
        snap = snap || snapshots.worker.try_reclaim().unwrap().is_some();
        if dial && log && snap {
            return;
        }
        assert!(
            Instant::now() < hang,
            "release: dialer {dial} log {log} snapshots {snap}"
        );
        std::thread::park_timeout(Duration::from_millis(1));
    }
}

/// Fail@main (3d552e2e): the default native node fails at the WAL limit
/// (recorded in docs/IMPLEMENTATION.md). With ST2 it appends 128 MiB of WAL
/// and 32,768 entries, and occupancy and the retained log stay bounded. The
/// clock advances one millisecond per 100 polls, so writes arrive far faster
/// than the 1 s reclaim interval and the 100 ms checkpoint scan: the 50%
/// occupancy path must keep the WAL bounded.
///
/// `VOTEBOAT_ST2_NATIVE_SOAK_WRITES` runs longer (release builds; ST2 round 3
/// recorded 163,840 writes, 640 MiB, ten times both old bounds).
#[test]
fn default_native_node_keeps_accepting_past_the_wal_and_entry_limits() {
    let total: u64 = std::env::var("VOTEBOAT_ST2_NATIVE_SOAK_WRITES")
        .map_or(32_768u64, |v| v.parse().unwrap())
        .next_multiple_of(4);
    let directory = root();
    let mut n = open(&directory, NativeOpenMode::Create);
    let mut clock = Clock::with_rate(100);
    elect(&mut n, &mut clock);
    let capacity = n.wal_usage().unwrap().capacity;
    let (mut max_wal, mut max_retained) = (0, 0);
    let (mut rewrites, mut last_rewrite) = (0u64, None);
    let mut bases = vec![core(&n).state().base_index()];
    let mut written = 0;
    while written < total {
        // Quarters of 8,192 writes (32 MiB): each passes the 50% occupancy
        // point, so each must include a checkpoint.
        drive(&mut n, &mut clock, written, total / 4, |n| {
            max_wal = max_wal.max(n.wal_usage().unwrap().bytes);
            max_retained = max_retained.max(retained(n));
            let at = n.wal_maintenance().last_reclaimed.map(|(t, _)| t);
            if at != last_rewrite {
                rewrites += 1;
                last_rewrite = at;
            }
        });
        written += total / 4;
        let base = core(&n).state().base_index();
        assert!(
            base > *bases.last().unwrap(),
            "no checkpoint since {bases:?}"
        );
        assert_eq!(n.state(), NodeState::Running);
        bases.push(base);
    }
    let reclaimed = n.wal_maintenance().last_reclaimed;
    assert!(reclaimed.is_some());
    assert_eq!(n.wal_maintenance().consecutive_failures, 0);
    assert!(max_wal <= capacity * 3 / 4, "{max_wal} of {capacity}");
    assert!(max_retained < ENTRY_LIMIT, "{max_retained}");
    eprintln!(
        "ST2 native soak: writes={total} appended_bytes={} last_index={} max_wal={max_wal} capacity={capacity} max_retained={max_retained} bases={bases:?} rewrites={rewrites} last_reclaim={reclaimed:?} simulated_ms={}",
        total * PADDED as u64,
        core(&n).state().last_index(),
        clock.now
    );
    // More rewrites than the interval alone schedules: occupancy drove them.
    assert!(
        rewrites > clock.now / 1000 + 1,
        "{rewrites} in {} ms",
        clock.now
    );
    stop(n, &mut clock);
    std::fs::remove_dir_all(directory).unwrap();
}

#[derive(Clone, Copy, Debug)]
enum Phase {
    Checkpoint,
    Reclaim,
}
/// Partial progress: the host stops (abort, then provider release) while an
/// automatic checkpoint or WAL rewrite is in flight. Reopening recovers the
/// same applied state, and maintenance and writes continue. Torn snapshot and
/// WAL publication at the file level are covered by `snapshot.rs`
/// (`native_wal_and_checkpoint_crashes_recover_acknowledged_state_and_retries`)
/// and `log_reclaim.rs`
/// (`replacement_failure_fences_and_power_loss_recovers_whole_old_or_new_state`).
#[test]
fn restart_during_automatic_checkpoint_or_reclaim_recovers_the_same_state() {
    for phase in [Phase::Checkpoint, Phase::Reclaim] {
        let directory = root();
        let mut n = open(&directory, NativeOpenMode::Create);
        let mut clock = Clock::new();
        elect(&mut n, &mut clock);
        // Write one entry at a time until the selected maintenance is in
        // flight: a checkpoint at the entry threshold or 50% occupancy, a
        // rewrite once enough of the WAL is reclaimable.
        drive(&mut n, &mut clock, 0, 2_000, |_| {});
        // Keep one write in flight and check after every poll, so a rewrite
        // that starts and finishes within one write is still caught.
        let (mut written, mut outstanding) = (2_000, false);
        loop {
            let busy = match phase {
                Phase::Checkpoint => !n.checkpoints().pending.is_empty(),
                Phase::Reclaim => n.wal_maintenance().pending.is_some(),
            };
            if busy {
                break;
            }
            if !outstanding {
                n.propose(write(written)).unwrap();
                outstanding = true;
            }
            clock.poll(&mut n);
            if let Some(output) = n.poll_client() {
                assert!(matches!(
                    n.complete_client(output).unwrap(),
                    ClientOutcome::Applied { .. }
                ));
                written += 1;
                outstanding = false;
            }
        }
        // A write may still be in flight when the host stops: the WAL worker
        // finishes accepted work, so recovery may include exactly that write.
        let in_flight = outstanding.then(|| write(written).operation);
        let app = &n.local().applications[&group()];
        let applied = app.applied_index();
        let image = app.checkpoint(1 << 20).unwrap();
        let committed = core(&n).state().commit_index;
        assert!(committed <= applied + 1, "{phase:?}");
        n.abort();
        let mut recovery = n.into_recovery().unwrap_or_else(|_| panic!("not failed"));
        let mut peers = recovery.peers.take().unwrap();
        while !peers.is_drained() {
            clock.now += 1;
            peers
                .drain(
                    &mut recovery.local.outbound,
                    MonoTime(clock.now),
                    PeerDriverBudget::default(),
                )
                .unwrap();
            std::thread::park_timeout(Duration::from_micros(200));
        }
        let dialer = peers
            .into_parts()
            .unwrap_or_else(|_| panic!("peers"))
            .connector
            .into_dialer()
            .unwrap_or_else(|_| panic!("dialer"));
        release(recovery.local, dialer);
        let mut n = open(&directory, NativeOpenMode::Recover);
        let mut clock = Clock::new();
        let app = &n.local().applications[&group()];
        let reopened = app.applied_index();
        if reopened == applied {
            assert_eq!(app.checkpoint(1 << 20).unwrap(), image, "{phase:?}");
        } else {
            // Only the one accepted write, exactly once.
            assert_eq!(reopened, applied + 1, "{phase:?}");
            let entry = core(&n).state().entry_at(reopened).unwrap();
            assert!(
                matches!(&entry.payload, EntryPayload::Command { operation, .. }
                    if Some(*operation) == in_flight),
                "{phase:?}: {entry:?}"
            );
        }
        elect(&mut n, &mut clock);
        drive(&mut n, &mut clock, written, 5_000, |_| {});
        assert!(core(&n).state().base_index() > 0, "{phase:?}");
        assert_eq!(n.retention(), RetentionMode::Maintained);
        stop(n, &mut clock);
        std::fs::remove_dir_all(directory).unwrap();
    }
}

/// The explicit opt-out through the binaries' open path installs no
/// maintenance; U1 itself is exercised on the runtime node.
#[test]
fn explicit_native_opt_out_installs_no_maintenance() {
    let directory = root();
    let mut c = config(directory.clone(), NativeOpenMode::Create);
    c.limits.retention = RetentionMode::LifetimeBounded;
    let mut n = c
        .open_with_protocol_and_timers(
            voteboat::native::connect::NativePeerProtocol::TcpTls,
            TimerConfig::default(),
            Padded(app()),
            Arc::new(ThreadWake::current()),
            MonoTime(0),
        )
        .unwrap_or_else(|e| panic!("{:?}", e.reason));
    assert_eq!(n.retention(), RetentionMode::LifetimeBounded);
    assert_eq!(n.checkpoints().policy, None);
    assert_eq!(n.wal_maintenance().policy, None);
    assert!(n.wal_usage().is_some());
    let mut clock = Clock::new();
    elect(&mut n, &mut clock);
    stop(n, &mut clock);
    std::fs::remove_dir_all(directory).unwrap();
}
