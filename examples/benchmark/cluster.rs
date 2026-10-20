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
//! Cluster mode: one replica per process, three processes on loopback, each
//! with its own host loop on the production wait policy (`HostWait`).
//!
//! Protocol on stdin/stdout lines: every node prints `ready node=N` once open.
//! Node 1 waits for `start`, campaigns, warms up, runs the offered phase (the
//! same ledger and offerer as the single-process offered mode), checks the
//! history and one quorum read, and prints a `mode=cluster` summary. Nodes 2
//! and 3 run until stdin reaches EOF. Every node then drains, joins its workers
//! and prints one `node_counters` line covering its whole lifetime.
//!
//! Stdin closing is how a node learns that its parent is gone (the kernel
//! closes the pipe when the parent dies, however it dies). For node 1 every
//! phase from waiting for `start` to the quorum read stops at the first poll
//! round that sees it closed; only the final drain ignores it. Node 1 then
//! prints `stdin_closed node=1 phase=P rounds_to_notice=K`, aborts its node
//! (pending writes become unknown), prints `node_counters` and exits with
//! [`STDIN_CLOSED_STATUS`]. It does not drain: a graceful drain waits for every
//! pending write to resolve, which cannot happen once the followers are gone.
use super::*;
use std::{
    io::BufRead,
    path::PathBuf,
    sync::atomic::{AtomicBool, AtomicUsize, Ordering},
};
use voteboat::{
    native::readiness::{HostWait, HostWaitOutcome, HostWake},
    worker::WorkerWake,
};

/// Bound on any one node-1 phase; a run that needs longer is invalid.
const PHASE_LIMIT: Duration = Duration::from_secs(120);
/// Exit status of a node 1 that stopped because its stdin closed before it
/// reported (its parent is gone or released it); distinct from 1, any other
/// failure.
pub(crate) const STDIN_CLOSED_STATUS: i32 = 4;

/// Node 1's stdin closed during `phase`, before the summary.
#[derive(Debug)]
struct StdinClosed {
    phase: &'static str,
    /// Poll rounds that began after the close and before node 1 stopped: 0 or
    /// 1, because every round of an armed phase checks (see [`Control`]).
    rounds_to_notice: usize,
}
impl std::fmt::Display for StdinClosed {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(
            f,
            "stdin closed during {} (the parent is gone or released this node) after {} further poll round(s); the run is abandoned",
            self.phase, self.rounds_to_notice
        )
    }
}
impl std::error::Error for StdinClosed {}

/// [`run`] for a node process: a stop on closed stdin exits with
/// [`STDIN_CLOSED_STATUS`] once the node is aborted and its counters printed.
/// Exiting ends its worker threads; the run's directory is abandoned.
pub(crate) fn run_process(
    args: &[String],
    ready: Option<usize>,
    plain: bool,
) -> Result<(), Failure> {
    let result = run(args, ready, plain);
    if let Err(error) = &result {
        if error.is::<StdinClosed>() {
            eprintln!("Error: {error}");
            let _ = std::io::stdout().flush();
            std::process::exit(STDIN_CLOSED_STATUS);
        }
    }
    result
}

#[derive(Debug)]
struct NodeArgs {
    n: u64,
    root: PathBuf,
    protocol: NativePeerProtocol,
    /// Listen port of node 1, 2 and 3.
    ports: [u16; 3],
    count: usize,
    window: usize,
    rate: usize,
    ready_visit_items: usize,
}
pub(crate) const USAGE: &str = "native_benchmark node N(1..3) FRESH_DIRECTORY tcp|quic PORTS(BASE_PORT: node N listens on BASE+N | P1,P2,P3: three distinct ports) OFFERS(1..50000) WINDOW RATE(1..100000) [--ready-items K]; every node takes the same PORTS; the example needs the tls feature (native-only builds have no cluster mode) and quic needs --features quic";
fn parse(args: &[String], ready_visit_items: usize) -> Result<NodeArgs, Failure> {
    let [n, root, protocol, ports, count, window, rate] = args else {
        return Err(format!("usage: {USAGE}").into());
    };
    let n = n.parse::<u64>()?;
    let ports = parse_ports(ports)?;
    if !(1..=3).contains(&n) || !(1..=1024).contains(&ready_visit_items) {
        return Err(format!("usage: {USAGE}").into());
    }
    let protocol = parse_protocol(protocol)?;
    let (count, window, rate) = (count.parse()?, window.parse()?, rate.parse()?);
    offered::validate(count, rate)?;
    CapacityProfile::Legacy.validate_workload(count, true, window, ready_visit_items)?;
    Ok(NodeArgs {
        n,
        root: PathBuf::from(root),
        protocol,
        ports,
        count,
        window,
        rate,
        ready_visit_items,
    })
}

/// `BASE` (ports BASE+1..3, a convenience for manual runs) or `P1,P2,P3`.
fn parse_ports(text: &str) -> Result<[u16; 3], Failure> {
    let ports = if text.contains(',') {
        let list = text
            .split(',')
            .map(str::parse::<u16>)
            .collect::<Result<Vec<_>, _>>()?;
        <[u16; 3]>::try_from(list).map_err(|_| format!("usage: {USAGE}"))?
    } else {
        let base = text.parse::<u16>()?;
        if base > u16::MAX - 3 {
            return Err(format!("usage: {USAGE}").into());
        }
        [base + 1, base + 2, base + 3]
    };
    let [a, b, c] = ports;
    if ports.contains(&0) || a == b || a == c || b == c {
        return Err(format!("usage: {USAGE}").into());
    }
    Ok(ports)
}

/// Run one cluster node; `args` follow `node`. Only `--ready-items` (default
/// 1024, as in the services) may accompany them (`plain`).
fn run(args: &[String], ready: Option<usize>, plain: bool) -> Result<(), Failure> {
    if !plain {
        return Err("node mode takes only --ready-items".into());
    }
    let args = parse(args, ready.unwrap_or(1024))?;
    std::fs::create_dir(&args.root)?;
    let wake = Arc::new(HostWake::new()?);
    let control = Control::spawn(wake.clone());
    let clock = Instant::now();
    let replica = open_node(&args, &clock, wake.clone())?;
    let mut host = Host {
        nodes: vec![replica],
        wait: HostWait::new(wake.clone()),
        totals: PollTotals::default(),
        clock,
        control: control.clone(),
        armed: None,
    };
    println!("ready node={}", args.n);
    let result = if args.n == 1 {
        lead(&mut host, &args, wake)
    } else {
        // EOF is a follower's normal end, not an abort.
        host.until(None, |_| Ok(control.closed()))
    };
    if let Err(error) = &result {
        eprintln!("node {} failed: {error}", args.n);
        if let Some(closed) = error.downcast_ref::<StdinClosed>() {
            println!(
                "stdin_closed node={} phase={} rounds_to_notice={}",
                args.n,
                closed.phase.replace(' ', "_"),
                closed.rounds_to_notice
            );
            // Abandon the run: a graceful drain would wait for pending writes
            // that cannot commit without the followers.
            host.nodes[0].abort();
            let counters = Counters::of(&host.totals);
            println!("node_counters node={} {}", args.n, counters.line());
            return result;
        }
    }
    // Drain and join even after a failure, as the single-process mode does.
    let finished = host.finish();
    println!("node_counters node={} {}", args.n, finished?.line());
    result
}

fn open_node(args: &NodeArgs, clock: &Instant, wake: Arc<HostWake>) -> Result<Replica, Failure> {
    let addresses = (1..=3u64)
        .zip(args.ports)
        .map(|(p, port)| (p, std::net::SocketAddr::from(([127, 0, 0, 1], port))))
        .collect();
    let config = replica_startup(
        &args.root,
        NativeOpenMode::Create,
        args.n,
        &addresses,
        args.ready_visit_items,
    )?;
    let app = checked(Counter::new(args.count + WARMUP))?;
    let now = MonoTime(clock.elapsed().as_millis() as u64);
    finish_open(config.open_with_protocol_and_timers(args.protocol, OFFERED_TIMERS, app, wake, now))
}

/// Lines on stdin: `start` (node 1 only) and EOF. A reader thread owns stdin
/// and wakes the host, so an idle node sleeps in its readiness wait; while it
/// is blocked in `read` it costs nothing.
///
/// Noticing a close in a bounded number of rounds: an armed host stores its
/// round count, then loads `closed`, every round; the reader stores `closed`,
/// then loads the round count into `closed_at`. All four are `SeqCst`, and the
/// host stops storing once it has noticed. Every round whose store follows the
/// reader's load therefore sees `closed`, so the host stops at round
/// `closed_at` or `closed_at + 1`.
#[derive(Clone)]
struct Control(Arc<ControlState>);
struct ControlState {
    started: AtomicBool,
    closed: AtomicBool,
    rounds: AtomicUsize,
    closed_at: AtomicUsize,
}
impl Control {
    fn spawn(wake: Arc<HostWake>) -> Self {
        let state = Arc::new(ControlState {
            started: AtomicBool::new(false),
            closed: AtomicBool::new(false),
            rounds: AtomicUsize::new(0),
            closed_at: AtomicUsize::new(usize::MAX),
        });
        let reader = state.clone();
        std::thread::Builder::new()
            .name("cluster-control".into())
            .spawn(move || {
                for line in std::io::stdin().lock().lines() {
                    match line {
                        Ok(line) if line.trim() == "start" => {
                            reader.started.store(true, Ordering::SeqCst);
                        }
                        Ok(_) => continue,
                        Err(_) => break,
                    }
                    wake.wake();
                }
                reader.closed.store(true, Ordering::SeqCst);
                let at = reader.rounds.load(Ordering::SeqCst);
                reader.closed_at.store(at, Ordering::SeqCst);
                wake.wake();
            })
            .expect("cluster control thread");
        Self(state)
    }
    fn started(&self) -> bool {
        self.0.started.load(Ordering::SeqCst)
    }
    fn closed(&self) -> bool {
        self.0.closed.load(Ordering::SeqCst)
    }
    /// An armed host's per-round check: the round count the reader recorded
    /// when stdin closed, or `None` while it is open.
    fn observe(&self, round: usize) -> Option<usize> {
        self.0.rounds.store(round, Ordering::SeqCst);
        if !self.closed() {
            return None;
        }
        // The reader is between its two stores; it records at once.
        loop {
            let at = self.0.closed_at.load(Ordering::SeqCst);
            if at != usize::MAX {
                return Some(at);
            }
            std::thread::yield_now();
        }
    }
}

/// One node's host loop: poll, then wait with the production policy.
struct Host {
    nodes: Vec<Replica>,
    wait: HostWait,
    totals: PollTotals,
    clock: Instant,
    control: Control,
    /// The node-1 phase that stops on closed stdin; `None` for followers and
    /// for the final drain.
    armed: Option<&'static str>,
}
impl Host {
    fn round(&mut self) -> Result<(), Failure> {
        poll(&mut self.nodes, &self.clock, &mut self.totals)?;
        let Some(phase) = self.armed else {
            return Ok(());
        };
        let Some(at) = self.control.observe(self.totals.rounds) else {
            return Ok(());
        };
        self.armed = None;
        Err(Box::new(StdinClosed {
            phase,
            rounds_to_notice: self.totals.rounds - at,
        }))
    }
    fn wait(&mut self) -> Result<(), Failure> {
        let node = &self.nodes[0];
        let now = MonoTime(self.clock.elapsed().as_millis() as u64);
        let outcome = self
            .wait
            .wait(|fds| node.readiness_fds(fds), node.next_deadline(), now)?;
        self.totals.waits += 1;
        match outcome {
            HostWaitOutcome::Immediate => self.totals.waits_immediate += 1,
            HostWaitOutcome::Ready => self.totals.waits_ready += 1,
            HostWaitOutcome::Timeout => self.totals.waits_timeout += 1,
        }
        Ok(())
    }
    /// Poll and wait until `done` holds after a poll, within `limit` if given.
    fn until(
        &mut self,
        limit: Option<Duration>,
        mut done: impl FnMut(&mut [Replica]) -> Result<bool, Failure>,
    ) -> Result<(), Failure> {
        let deadline = limit.map(|limit| Instant::now() + limit);
        loop {
            self.round()?;
            if done(&mut self.nodes)? {
                return Ok(());
            }
            if deadline.is_some_and(|d| Instant::now() > d) {
                return Err("cluster phase timed out; the run is invalid".into());
            }
            self.wait()?;
        }
    }
    fn finish(mut self) -> Result<Counters, Failure> {
        // The drain ends the process either way: closed stdin cannot shorten it.
        self.armed = None;
        self.nodes[0].begin_shutdown();
        self.until(Some(PHASE_LIMIT), |ns| Ok(ns[0].is_drained()))?;
        let counters = Counters::of(&self.totals);
        join(self.nodes)?;
        Ok(counters)
    }
}

/// Whole-process (or phase) counters, in the order they are printed.
#[derive(Clone, Copy, Default)]
struct Counters {
    poll_rounds: usize,
    host_waits: usize,
    host_waits_immediate: usize,
    host_waits_ready: usize,
    host_waits_timeout: usize,
    sends: usize,
    received: usize,
    persistence_batches: usize,
}
impl Counters {
    fn of(t: &PollTotals) -> Self {
        Self {
            poll_rounds: t.rounds,
            host_waits: t.waits,
            host_waits_immediate: t.waits_immediate,
            host_waits_ready: t.waits_ready,
            host_waits_timeout: t.waits_timeout,
            sends: t.peer_sends,
            received: t.peer_received,
            persistence_batches: t.persistence_batches,
        }
    }
    fn since(self, before: Self) -> Self {
        Self {
            poll_rounds: self.poll_rounds - before.poll_rounds,
            host_waits: self.host_waits - before.host_waits,
            host_waits_immediate: self.host_waits_immediate - before.host_waits_immediate,
            host_waits_ready: self.host_waits_ready - before.host_waits_ready,
            host_waits_timeout: self.host_waits_timeout - before.host_waits_timeout,
            sends: self.sends - before.sends,
            received: self.received - before.received,
            persistence_batches: self.persistence_batches - before.persistence_batches,
        }
    }
    fn line(self) -> String {
        format!(
            "poll_rounds={} host_waits={} host_waits_immediate={} host_waits_ready={} host_waits_timeout={} sends={} received={} persistence_batches={}",
            self.poll_rounds,
            self.host_waits,
            self.host_waits_immediate,
            self.host_waits_ready,
            self.host_waits_timeout,
            self.sends,
            self.received,
            self.persistence_batches
        )
    }
}

fn ready_leader(nodes: &[Replica]) -> bool {
    leader(nodes, group()).is_ok_and(|_| {
        let core = nodes[0].local().owner.core(group()).unwrap();
        nodes[0].local().applications[&group()].applied_index() == core.state().commit_index
    })
}
fn term(nodes: &[Replica]) -> u64 {
    nodes[0]
        .local()
        .owner
        .core(group())
        .unwrap()
        .state()
        .hard_state
        .term
}

/// Node 1: campaign, warm up, offer, check, report.
fn lead(host: &mut Host, args: &NodeArgs, wake: Arc<HostWake>) -> Result<(), Failure> {
    std::fs::write(
        args.root.join("run.txt"),
        format!(
            "mode=cluster node=1 protocol={:?} ports={},{},{} offers={} offered_rate={} window={} groups=1 warmup={WARMUP} {}\n",
            args.protocol,
            args.ports[0],
            args.ports[1],
            args.ports[2],
            args.count,
            args.rate,
            args.window,
            ready_items::settings(args.ready_visit_items)
        ),
    )?;
    let control = host.control.clone();
    host.armed = Some("waiting for start");
    host.until(None, |_| Ok(control.started()))?;
    host.armed = Some("campaign");
    if !ready_leader(&host.nodes) {
        checked(host.nodes[0].control(group(), NodeControl::Campaign))?;
    }
    host.until(Some(PHASE_LIMIT), |ns| Ok(ready_leader(ns)))?;
    host.armed = Some("warmup");
    let warmup = warm_up(host, args.window)?;
    let term_start = term(&host.nodes);
    let mut ledger = Ledger::new(args.count, args.rate, args.window, 1, None);
    let before = Counters::of(&host.totals);
    let (host_ns, max_host_ns) = (host.totals.host_ns, host.totals.max_host_ns);
    host.armed = Some("offering");
    eprintln!(
        "phase=offering node=1 operations={} rate={}",
        args.count, args.rate
    );
    println!("offering node=1");
    let elapsed = offer(host, &mut ledger, wake)?;
    let phase = Counters::of(&host.totals).since(before);
    let host_ms = (host.totals.host_ns - host_ns) as f64 / 1e6;
    let max_host_ms = host.totals.max_host_ns.max(max_host_ns) as f64 / 1e6;
    let term_end = term(&host.nodes);
    ledger.write(&mut exclusive(&args.root.join("offers.csv"))?)?;
    let (expected, last) = ledger.history(&warmup)?;
    host.armed = Some("quorum read");
    let value = quorum_read(host, last[&group()])?;
    // No more rounds before the summary; the drain is not armed.
    host.armed = None;
    if value != expected[&group()] {
        return Err(format!(
            "quorum read {value} disagrees with the history {}",
            expected[&group()]
        )
        .into());
    }
    let summary = summary(
        args,
        &ledger,
        elapsed,
        (term_start, term_end, value),
        phase,
        (host_ms, max_host_ms),
    );
    let mut file = exclusive(&args.root.join("summary.txt"))?;
    file.write_all(summary.as_bytes())?;
    file.sync_all()?;
    print!("{summary}");
    if term_start != term_end || !ready_leader(&host.nodes) {
        return Err("leadership changed during the offered phase; the run is invalid".into());
    }
    Ok(())
}

/// The closed-loop warmup of the single-process mode, on this node alone.
fn warm_up(host: &mut Host, window: usize) -> Result<Measurement, Failure> {
    let start = Instant::now();
    let mut sent = 0;
    let mut pending = BTreeMap::new();
    let mut group_pending = BTreeMap::new();
    let mut samples = Vec::with_capacity(WARMUP);
    host.until(Some(PHASE_LIMIT), |ns| {
        while sent < WARMUP && pending.len() < window {
            let ticket = ns[0]
                .propose(ClientRequest {
                    group: group(),
                    operation: OperationId::new(sent as u128 + 1).unwrap(),
                    bytes: 1i64.to_le_bytes().to_vec(),
                })
                .map_err(|e| format!("warmup admission refused: {:?}", e.reason))?;
            pending.insert((0, ticket.sequence), (ticket, start.elapsed().as_nanos()));
            *group_pending.entry(group()).or_default() += 1;
            sent += 1;
        }
        collect_workload_receipts(ns, start, 1, &mut pending, &mut group_pending, &mut samples)?;
        Ok(samples.len() == WARMUP)
    })?;
    Ok(Measurement {
        elapsed: start.elapsed(),
        samples,
        max_inflight: window.min(WARMUP),
        polls: PollTotals::default(),
    })
}

/// The offered phase of `offered::measure`, on this node's host loop.
fn offer(
    host: &mut Host,
    ledger: &mut Ledger<'_>,
    wake: Arc<HostWake>,
) -> Result<Duration, Failure> {
    let start = Instant::now();
    let offers = Offerer::spawn(ledger.count, ledger.rate, start, wake);
    loop {
        host.round()?;
        outputs(&mut host.nodes, &start, ledger)?;
        for _ in 0..64 {
            let Ok(offset) = offers.due.try_recv() else {
                break;
            };
            if offset != ledger.rows.len() {
                return Err("offer schedule out of order".into());
            }
            let row = ledger.begin(start.elapsed().as_nanos());
            let group = ledger.rows[row].group;
            if ledger.full(group) {
                ledger.rows[row].status = "window_refused";
                continue;
            }
            let Ok(replica) = leader(&host.nodes, group) else {
                ledger.rows[row].status = "no_leader";
                continue;
            };
            let request = ClientRequest {
                group,
                operation: OperationId::new(ledger.rows[row].operation as u128).unwrap(),
                bytes: 1i64.to_le_bytes().to_vec(),
            };
            match host.nodes[replica].propose(request) {
                Ok(ticket) => ledger.admit(row, replica, ticket)?,
                Err(error) => {
                    ledger.rows[row].status = "admission_refused";
                    ledger.rows[row].reason = format!("{:?}", error.reason);
                }
            }
        }
        let now = start.elapsed().as_nanos();
        if ledger.rows.len() == ledger.count && ledger.pending.is_empty() && now >= ledger.horizon()
        {
            return Ok(start.elapsed());
        }
        if now > ledger.horizon() + PHASE_LIMIT.as_nanos() {
            return Err("offered drain timed out; pending outcomes remain uncertain".into());
        }
        host.wait()?;
    }
}

/// Wait until this node has applied `last`, then read the counter through a
/// quorum read.
fn quorum_read(host: &mut Host, last: u64) -> Result<i64, Failure> {
    host.until(Some(PHASE_LIMIT), |ns| {
        Ok(ns[0].local().applications[&group()].applied_index() >= last)
    })?;
    let ticket = checked(host.nodes[0].read(group(), ()))?;
    let mut value = None;
    host.until(Some(PHASE_LIMIT), |ns| {
        let Some(output) = ns[0].poll_read() else {
            return Ok(false);
        };
        if output.ticket() != ticket {
            return Err("read ticket mismatch".into());
        }
        match checked(ns[0].complete_read(output).map_err(|e| e.reason))? {
            ReadOutcome::Read {
                result: Ok(read), ..
            } => {
                value = Some(read);
                Ok(true)
            }
            other => Err(format!("quorum read failed: {other:?}").into()),
        }
    })?;
    value.ok_or_else(|| "quorum read produced no value".into())
}

fn summary(
    args: &NodeArgs,
    ledger: &Ledger<'_>,
    elapsed: Duration,
    (term_start, term_end, value): (u64, u64, i64),
    phase: Counters,
    (host_ms, max_host_ms): (f64, f64),
) -> String {
    let horizon = ledger.horizon();
    let applied = ledger.count_status("applied");
    let admitted = ledger.rows.iter().filter(|r| r.sequence > 0).count();
    let applied_rows = || ledger.rows.iter().filter(|r| r.status == "applied");
    let p99 = percentile(
        &mut applied_rows()
            .map(|r| r.completed - r.intended)
            .collect::<Vec<_>>(),
        99,
    );
    let service_p99 = percentile(
        &mut applied_rows()
            .map(|r| r.completed - r.dispatched)
            .collect::<Vec<_>>(),
        99,
    );
    let dispatch_p99 = percentile(
        &mut ledger
            .rows
            .iter()
            .map(|r| r.dispatched - r.intended)
            .collect::<Vec<_>>(),
        99,
    );
    let NodeArgs {
        protocol,
        count,
        window,
        rate,
        ready_visit_items,
        ..
    } = *args;
    format!(
        "mode=cluster node=1 processes=3 protocol={protocol:?} replicas=3 groups=1 assembly=startup host_wait=production heartbeat_ms=50 election_min_ms=10000 election_spread_ms=10000 payload_bytes=8 warmup={WARMUP} offers={count} offered_rate={rate} window={window} max_inflight={} admitted={admitted} applied={applied} window_refused={} no_leader={} admission_refused={} not_proposed={} unknown=0 pending=0 horizon_ns={horizon} elapsed_ns={} drain_ns={} offered_ops_s={:.3} applied_total_ops_s={:.3} p99_us={p99} service_p99_us={service_p99} dispatch_p99_us={dispatch_p99} read_value={value} leader_term_start={term_start} leader_term_end={term_end} {} host_poll_ms={host_ms:.3} max_host_poll_ms={max_host_ms:.3} {}\n",
        ledger.max_pending,
        ledger.count_status("window_refused"),
        ledger.count_status("no_leader"),
        ledger.count_status("admission_refused"),
        ledger.count_status("not_proposed"),
        elapsed.as_nanos(),
        elapsed.as_nanos().saturating_sub(horizon),
        count as f64 * 1e9 / horizon as f64,
        applied as f64 / elapsed.as_secs_f64(),
        phase.line(),
        ready_items::settings(ready_visit_items),
    )
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::{
        collections::BTreeSet,
        fs::File,
        io::BufReader,
        process::{Child, Command, ExitStatus, Stdio},
        sync::mpsc,
    };

    const CHILD: &str = "VOTEBOAT_CLUSTER_NODE_ARGS";
    const SEPARATOR: char = '\u{1f}';

    /// One cluster node when the cluster test re-executes this test binary;
    /// a no-op in an ordinary test run.
    #[test]
    fn cluster_node_child() {
        let Ok(spec) = std::env::var(CHILD) else {
            return;
        };
        let args = spec.split(SEPARATOR).map(String::from).collect::<Vec<_>>();
        // As in the binary: a stop on closed stdin exits STDIN_CLOSED_STATUS.
        run_process(&args, None, true).unwrap();
    }

    /// Three OS-selected loopback ports, each held for TCP and UDP (the
    /// acquisition the native startup fixtures use), as the `P1,P2,P3`
    /// argument. The nodes bind them after the holds are released; a binder
    /// that takes one in between makes that node fail to open with
    /// `AddrInUse`, and `cluster` retries the whole cluster on fresh ports.
    fn reserve() -> (String, Vec<(TcpListener, UdpSocket)>) {
        let held = (0..3)
            .map(|_| {
                (0..32)
                    .find_map(|_| {
                        let tcp = TcpListener::bind("127.0.0.1:0").ok()?;
                        let udp = UdpSocket::bind(tcp.local_addr().ok()?).ok()?;
                        Some((tcp, udp))
                    })
                    .expect("available TCP/UDP endpoint")
            })
            .collect::<Vec<_>>();
        let ports = held
            .iter()
            .map(|(tcp, _)| tcp.local_addr().unwrap().port().to_string())
            .collect::<Vec<_>>()
            .join(",");
        (ports, held)
    }

    /// Children are killed and reaped if the test fails before they exit.
    struct Children(Vec<Child>);
    impl Drop for Children {
        fn drop(&mut self) {
            for child in &mut self.0 {
                if matches!(child.try_wait(), Ok(None)) {
                    let _ = child.kill();
                }
                let _ = child.wait();
            }
        }
    }

    fn fields(line: &str) -> BTreeMap<String, String> {
        line.split_whitespace()
            .filter_map(|w| w.split_once('='))
            .map(|(k, v)| (k.to_string(), v.to_string()))
            .collect()
    }
    fn number(map: &BTreeMap<String, String>, key: &str) -> usize {
        map.get(key)
            .unwrap_or_else(|| panic!("missing {key}"))
            .parse()
            .unwrap()
    }
    /// The text from `marker` on: libtest may print the test name before a
    /// child's first line.
    fn marked<'a>(lines: &'a [String], marker: &str) -> Vec<&'a str> {
        lines
            .iter()
            .filter_map(|l| l.find(marker).map(|i| &l[i..]))
            .collect()
    }

    type Events = mpsc::Receiver<(u64, Option<String>)>;
    /// The node workload: offers and offered rate.
    #[derive(Clone, Copy)]
    struct Load {
        offers: usize,
        rate: usize,
    }
    /// The checked run: 100 offers at 50/s.
    const CHECKED: Load = Load {
        offers: 100,
        rate: 50,
    };

    /// Start three nodes re-executing this binary; their stdout lines arrive
    /// on the returned channel, `None` when a stream ends.
    fn spawn(root: &Path, protocol: &str, ports: &str, load: Load) -> (Children, Events) {
        let exe = std::env::current_exe().unwrap();
        let (sender, events) = mpsc::channel();
        let mut children = Children(Vec::new());
        for n in 1..=3u64 {
            let spec = [
                n.to_string(),
                root.join(format!("node{n}")).display().to_string(),
                protocol.to_string(),
                ports.to_string(),
                load.offers.to_string(),
                "1024".into(),
                load.rate.to_string(),
            ]
            .join(&SEPARATOR.to_string());
            let mut child = Command::new(&exe)
                .args([
                    "--exact",
                    "offered::cluster::tests::cluster_node_child",
                    "--nocapture",
                    "--test-threads=1",
                ])
                .env(CHILD, spec)
                .stdin(Stdio::piped())
                .stdout(Stdio::piped())
                .stderr(File::create(root.join(format!("node{n}.err"))).unwrap())
                .spawn()
                .unwrap();
            let stdout = child.stdout.take().unwrap();
            let sender = sender.clone();
            std::thread::spawn(move || {
                for line in BufReader::new(stdout).lines() {
                    let Ok(line) = line else { break };
                    if sender.send((n, Some(line))).is_err() {
                        return;
                    }
                }
                let _ = sender.send((n, None));
            });
            children.0.push(child);
        }
        (children, events)
    }

    enum Outcome {
        /// Every node exited: each node's stdout lines and exit status (node
        /// 1 first).
        Done(BTreeMap<u64, Vec<String>>, Vec<ExitStatus>),
        /// This node failed to open with `AddrInUse` before `ready`.
        Collision(u64),
    }

    /// The parent protocol of `cluster.py`: `start` once all three are ready,
    /// every stdin closed once node 1 prints a line containing `release` or
    /// any node has ended.
    fn drive(children: &mut Children, events: &Events, root: &Path, release: &str) -> Outcome {
        let mut lines = BTreeMap::<u64, Vec<String>>::new();
        let (mut ready, mut open) = (BTreeSet::new(), 3);
        let close_all = |children: &mut Children| {
            for child in &mut children.0 {
                drop(child.stdin.take());
            }
        };
        while open > 0 {
            // A failure guard only: nothing checked depends on elapsed time.
            let (n, line) = events
                .recv_timeout(Duration::from_secs(600))
                .unwrap_or_else(|_| panic!("cluster child hung; logs in {}", root.display()));
            let Some(line) = line else {
                // A node that ends before node 1 reports releases the others.
                open -= 1;
                close_all(children);
                continue;
            };
            if line.contains("ready node=") {
                ready.insert(n);
                // Stdin is gone once a node has ended; that node's exit status
                // and log then say why, so a failed write is not a failure here.
                if let (3, Some(stdin)) = (ready.len(), children.0[0].stdin.as_mut()) {
                    let _ = writeln!(stdin, "start");
                }
            }
            if n == 1 && line.contains(release) {
                close_all(children);
            }
            lines.entry(n).or_default().push(line);
        }
        let statuses = children
            .0
            .iter_mut()
            .map(|child| child.wait().unwrap())
            .collect::<Vec<_>>();
        for (n, status) in (1..=3u64).zip(&statuses) {
            let log = std::fs::read_to_string(root.join(format!("node{n}.err"))).unwrap();
            if !status.success() && !ready.contains(&n) && log.contains("AddrInUse") {
                return Outcome::Collision(n);
            }
        }
        Outcome::Done(lines, statuses)
    }

    /// Bounds the retries; each needs a node to fail to bind one of three
    /// fresh OS-selected ports in the moment between release and bind.
    const ATTEMPTS: usize = 8;

    /// Run a cluster, retrying on fresh ports after a collision. Returns the
    /// run's directory (logs), each node's stdout lines and exit statuses.
    fn run_cluster(
        name: &str,
        protocol: &str,
        load: Load,
        release: &str,
    ) -> (PathBuf, BTreeMap<u64, Vec<String>>, Vec<ExitStatus>) {
        let mut collisions = Vec::new();
        for attempt in 0..ATTEMPTS {
            let root = fresh_root(&format!("{name}-{attempt}"));
            let (ports, held) = reserve();
            // The nodes bind their own listeners: release the holds first.
            drop(held);
            let (mut children, events) = spawn(&root, protocol, &ports, load);
            match drive(&mut children, &events, &root, release) {
                Outcome::Done(lines, statuses) => return (root, lines, statuses),
                Outcome::Collision(n) => {
                    let port = ports.split(',').nth(n as usize - 1).unwrap();
                    let what = format!("node {n} (port {port} of {ports})");
                    eprintln!("{what} failed to open with AddrInUse before ready; retrying");
                    collisions.push(what);
                    drop(children);
                    // Keep the last attempt's logs for the panic below.
                    if attempt + 1 < ATTEMPTS {
                        std::fs::remove_dir_all(&root).unwrap();
                    }
                }
            }
        }
        // Known: in every attempt the named node's open failed with AddrInUse
        // before `ready`. Not known: whether another program took the released
        // port or a node bound a port that is not its own.
        panic!(
            "{ATTEMPTS} attempts each had a node fail to open with AddrInUse before ready: {}; the port was taken by another program or by a node that bound a port not its own (logs of the last attempt in {})",
            collisions.join(", "),
            fresh_root_path(&format!("{name}-{}", ATTEMPTS - 1)).display()
        );
    }

    /// The statuses must all be success; otherwise name them and the logs.
    fn all_succeeded(root: &Path, statuses: &[ExitStatus]) {
        let failed = (1..=3u64)
            .zip(statuses)
            .filter(|(_, status)| !status.success())
            .collect::<Vec<_>>();
        assert!(
            failed.is_empty(),
            "nodes exited {failed:?}; logs in {}",
            root.display()
        );
    }

    fn check(lines: &BTreeMap<u64, Vec<String>>) {
        let summary = marked(&lines[&1], "mode=cluster ");
        assert_eq!(summary.len(), 1);
        let summary = fields(summary[0]);
        for (key, value) in [
            ("applied", "100"),
            ("admitted", "100"),
            ("window_refused", "0"),
            ("no_leader", "0"),
            ("admission_refused", "0"),
            ("not_proposed", "0"),
            ("read_value", "164"),
        ] {
            assert_eq!(summary[key], value, "{key}");
        }
        assert_eq!(summary["leader_term_start"], summary["leader_term_end"]);
        let (mut sends, mut received) = (0, 0);
        for n in 1..=3u64 {
            assert_eq!(marked(&lines[&n], "ready node=").len(), 1);
            let counters = marked(&lines[&n], "node_counters ");
            assert_eq!(counters.len(), 1, "node {n}");
            let c = fields(counters[0]);
            assert_eq!(c["node"], n.to_string());
            let waits = number(&c, "host_waits");
            assert_eq!(
                waits,
                number(&c, "host_waits_immediate")
                    + number(&c, "host_waits_ready")
                    + number(&c, "host_waits_timeout")
            );
            // Every wait follows a poll round.
            assert!(number(&c, "poll_rounds") > waits);
            // Every node exchanged frames and persisted entries.
            for key in ["sends", "received", "persistence_batches"] {
                assert!(number(&c, key) >= 1, "node {n} {key}");
            }
            sends += number(&c, "sends");
            received += number(&c, "received");
            if n == 1 {
                // The offered phase is part of node 1's lifetime.
                for key in ["poll_rounds", "host_waits", "sends", "persistence_batches"] {
                    assert!(number(&summary, key) <= number(&c, key), "{key}");
                }
                assert!(number(&summary, "persistence_batches") >= 1);
            }
        }
        // A frame is received at most once per frame sent.
        assert!(received <= sends);
    }

    fn fresh_root_path(name: &str) -> PathBuf {
        std::env::temp_dir().join(format!("voteboat-cluster-{}-{name}", std::process::id()))
    }
    fn fresh_root(name: &str) -> PathBuf {
        let root = fresh_root_path(name);
        let _ = std::fs::remove_dir_all(&root);
        std::fs::create_dir(&root).unwrap();
        root
    }

    fn cluster(protocol: &str) {
        let (root, lines, statuses) = run_cluster(protocol, protocol, CHECKED, "mode=cluster ");
        all_succeeded(&root, &statuses);
        check(&lines);
        std::fs::remove_dir_all(&root).unwrap();
    }

    #[test]
    fn a_node_whose_port_is_taken_is_reported_as_a_collision() {
        let root = fresh_root("collision");
        let (ports, mut held) = reserve();
        // Keep node 3's TCP and UDP holds: its open fails exactly as if
        // another binder had taken the port. Nodes 1 and 2 are released by
        // EOF and exit; nothing is offered.
        let taken = held.pop().unwrap();
        drop(held);
        let (mut children, events) = spawn(&root, "tcp", &ports, CHECKED);
        let outcome = drive(&mut children, &events, &root, "mode=cluster ");
        assert!(matches!(outcome, Outcome::Collision(3)));
        drop((children, taken));
        std::fs::remove_dir_all(&root).unwrap();
    }

    /// The parent script's own tests (round pairing, per-process exit stamps,
    /// retained invalid runs), on fixture nodes; needs `python3` (3.9+).
    #[test]
    fn cluster_script_self_test_passes() {
        let script = concat!(
            env!("CARGO_MANIFEST_DIR"),
            "/benchmarks/tools/cluster_test.py"
        );
        let output = Command::new("python3")
            .args(["-I", script])
            .output()
            .expect("python3 runs benchmarks/tools/cluster_test.py");
        assert!(
            output.status.success(),
            "{}",
            String::from_utf8_lossy(&output.stderr)
        );
    }

    /// Review round 2, P2: a parent that dies mid-run closes every node's
    /// stdin (the kernel closes the pipes). Node 1 must stop within one poll
    /// round instead of running to its phase limits, abort and exit 4; the
    /// followers drain and exit 0.
    /// 15,000 offers at 50/s is a 300 s horizon, so the offered phase is
    /// nowhere near done when the test closes stdin.
    #[test]
    fn node_1_stops_within_one_round_when_stdin_closes_in_the_offered_phase() {
        let load = Load {
            offers: 15_000,
            rate: 50,
        };
        let (root, lines, statuses) = run_cluster("stdin-closed", "tcp", load, "offering node=1");
        let codes = statuses.iter().map(ExitStatus::code).collect::<Vec<_>>();
        assert_eq!(
            codes,
            [Some(STDIN_CLOSED_STATUS), Some(0), Some(0)],
            "logs in {}",
            root.display()
        );
        let closed = marked(&lines[&1], "stdin_closed ");
        assert_eq!(closed.len(), 1, "logs in {}", root.display());
        let closed = fields(closed[0]);
        assert_eq!(closed["phase"], "offering");
        assert!(number(&closed, "rounds_to_notice") <= 1, "{closed:?}");
        // Node 1 stopped before reporting; every node reported its counters.
        assert!(marked(&lines[&1], "mode=cluster ").is_empty());
        for n in 1..=3u64 {
            assert_eq!(marked(&lines[&n], "node_counters ").len(), 1, "node {n}");
        }
        std::fs::remove_dir_all(&root).unwrap();
    }

    #[test]
    fn three_process_tcp_cluster_completes_an_offered_run_with_consistent_counters() {
        cluster("tcp");
    }

    #[cfg(feature = "quic")]
    #[test]
    fn three_process_quic_cluster_completes_an_offered_run_with_consistent_counters() {
        cluster("quic");
    }

    #[cfg(not(feature = "quic"))]
    #[test]
    fn a_build_without_quic_refuses_it_before_creating_anything() {
        let root =
            std::env::temp_dir().join(format!("voteboat-cluster-noquic-{}", std::process::id()));
        let args = [
            "1",
            root.to_str().unwrap(),
            "quic",
            "4000",
            "100",
            "1024",
            "50",
        ]
        .map(String::from);
        let error = run(&args, None, true).unwrap_err().to_string();
        assert!(error.contains("--features quic"), "{error}");
        assert!(!root.exists());
    }

    #[test]
    fn node_arguments_are_checked_before_anything_is_created() {
        let root =
            std::env::temp_dir().join(format!("voteboat-cluster-args-{}", std::process::id()));
        let dir = root.to_str().unwrap();
        for args in [
            vec!["0", dir, "tcp", "4000", "100", "1024", "50"],
            vec!["4", dir, "tcp", "4000", "100", "1024", "50"],
            vec!["1", dir, "tcp", "65533", "100", "1024", "50"],
            vec!["1", dir, "tcp", "0,4002,4003", "100", "1024", "50"],
            vec!["1", dir, "tcp", "4001,4002,4001", "100", "1024", "50"],
            vec!["1", dir, "tcp", "4001,4002", "100", "1024", "50"],
            vec!["1", dir, "tcp", "4001,4002,4003,4004", "100", "1024", "50"],
            vec!["1", dir, "tcp", "4001,,4003", "100", "1024", "50"],
            vec!["1", dir, "tcp", "4000", "0", "1024", "50"],
            vec!["1", dir, "tcp", "4000", "100", "0", "50"],
            vec!["1", dir, "tcp", "4000", "100", "1024", "0"],
            vec!["1", dir, "udp", "4000", "100", "1024", "50"],
            vec!["1", dir, "tcp", "4000", "100", "1024"],
        ] {
            let args = args.into_iter().map(String::from).collect::<Vec<_>>();
            assert!(run(&args, None, true).is_err(), "{args:?}");
        }
        let args = ["1", dir, "tcp", "4000", "100", "1024", "50"].map(String::from);
        assert!(run(&args, None, false).is_err());
        assert!(run(&args, Some(5000), true).is_err());
        assert!(!root.exists());
        let parsed = parse(&args, 1024).unwrap();
        assert_eq!(
            (parsed.n, parsed.ports, parsed.count),
            (1, [4001, 4002, 4003], 100)
        );
        assert_eq!((parsed.window, parsed.rate), (1024, 50));
        let explicit =
            ["2", dir, "tcp", "50007,40001,61000", "100", "1024", "50"].map(String::from);
        assert_eq!(parse(&explicit, 1024).unwrap().ports, [50007, 40001, 61000]);
        let base = ["3", dir, "tcp", "0", "100", "1024", "50"].map(String::from);
        assert_eq!(parse(&base, 1024).unwrap().ports, [1, 2, 3]);
    }
}
