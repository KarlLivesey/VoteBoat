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
use super::*;
use std::{
    io::BufRead,
    path::PathBuf,
    sync::atomic::{AtomicBool, Ordering},
};
use voteboat::{
    native::readiness::{HostWait, HostWaitOutcome, HostWake},
    worker::WorkerWake,
};

/// Bound on any one node-1 phase; a run that needs longer is invalid.
const PHASE_LIMIT: Duration = Duration::from_secs(120);

#[derive(Debug)]
struct NodeArgs {
    n: u64,
    root: PathBuf,
    protocol: NativePeerProtocol,
    base: u16,
    count: usize,
    window: usize,
    rate: usize,
    ready_visit_items: usize,
}
const USAGE: &str = "usage: native_benchmark node N(1..3) FRESH_DIRECTORY tcp|quic BASE_PORT(listens on BASE+N) OFFERS(1..50000) WINDOW RATE(1..100000) [--ready-items K]";
fn parse(args: &[String], ready_visit_items: usize) -> Result<NodeArgs, Failure> {
    let [n, root, protocol, base, count, window, rate] = args else {
        return Err(USAGE.into());
    };
    let n = n.parse::<u64>()?;
    let base = base.parse::<u16>()?;
    if !(1..=3).contains(&n)
        || base == 0
        || base > u16::MAX - 3
        || !(1..=1024).contains(&ready_visit_items)
    {
        return Err(USAGE.into());
    }
    let protocol = parse_protocol(protocol)?;
    let (count, window, rate) = (count.parse()?, window.parse()?, rate.parse()?);
    offered::validate(count, rate)?;
    CapacityProfile::Legacy.validate_workload(count, true, window, ready_visit_items)?;
    Ok(NodeArgs {
        n,
        root: PathBuf::from(root),
        protocol,
        base,
        count,
        window,
        rate,
        ready_visit_items,
    })
}

/// Run one cluster node; `args` follow `node`. Only `--ready-items` (default
/// 1024, as in the services) may accompany them (`plain`).
pub(crate) fn run(args: &[String], ready: Option<usize>, plain: bool) -> Result<(), Failure> {
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
    };
    println!("ready node={}", args.n);
    let result = if args.n == 1 {
        lead(&mut host, &args, &control, wake)
    } else {
        host.until(None, |_| Ok(control.closed()))
    };
    if let Err(error) = &result {
        eprintln!("node {} failed: {error}", args.n);
    }
    // Drain and join even after a failure, as the single-process mode does.
    let finished = host.finish();
    println!("node_counters node={} {}", args.n, finished?.line());
    result
}

fn open_node(args: &NodeArgs, clock: &Instant, wake: Arc<HostWake>) -> Result<Replica, Failure> {
    let addresses = (1..=3u64)
        .map(|p| {
            let port = args.base + p as u16;
            (p, std::net::SocketAddr::from(([127, 0, 0, 1], port)))
        })
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
/// and wakes the host, so an idle node sleeps in its readiness wait.
struct Control {
    started: Arc<AtomicBool>,
    closed: Arc<AtomicBool>,
}
impl Control {
    fn spawn(wake: Arc<HostWake>) -> Self {
        let started = Arc::new(AtomicBool::new(false));
        let closed = Arc::new(AtomicBool::new(false));
        let (s, c) = (started.clone(), closed.clone());
        std::thread::Builder::new()
            .name("cluster-control".into())
            .spawn(move || {
                for line in std::io::stdin().lock().lines() {
                    match line {
                        Ok(line) if line.trim() == "start" => s.store(true, Ordering::SeqCst),
                        Ok(_) => continue,
                        Err(_) => break,
                    }
                    wake.wake();
                }
                c.store(true, Ordering::SeqCst);
                wake.wake();
            })
            .expect("cluster control thread");
        Self { started, closed }
    }
    fn started(&self) -> bool {
        self.started.load(Ordering::SeqCst)
    }
    fn closed(&self) -> bool {
        self.closed.load(Ordering::SeqCst)
    }
}

/// One node's host loop: poll, then wait with the production policy.
struct Host {
    nodes: Vec<Replica>,
    wait: HostWait,
    totals: PollTotals,
    clock: Instant,
}
impl Host {
    fn round(&mut self) -> Result<(), Failure> {
        poll(&mut self.nodes, &self.clock, &mut self.totals)
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
fn lead(
    host: &mut Host,
    args: &NodeArgs,
    control: &Control,
    wake: Arc<HostWake>,
) -> Result<(), Failure> {
    std::fs::write(
        args.root.join("run.txt"),
        format!(
            "mode=cluster node=1 protocol={:?} base_port={} offers={} offered_rate={} window={} groups=1 warmup={WARMUP} {}\n",
            args.protocol,
            args.base,
            args.count,
            args.rate,
            args.window,
            ready_items::settings(args.ready_visit_items)
        ),
    )?;
    host.until(None, |_| Ok(control.started() || control.closed()))?;
    if !control.started() {
        return Err("stdin closed before start".into());
    }
    if !ready_leader(&host.nodes) {
        checked(host.nodes[0].control(group(), NodeControl::Campaign))?;
    }
    host.until(Some(PHASE_LIMIT), |ns| Ok(ready_leader(ns)))?;
    let warmup = warm_up(host, args.window)?;
    let term_start = term(&host.nodes);
    let mut ledger = Ledger::new(args.count, args.rate, args.window, 1, None);
    let before = Counters::of(&host.totals);
    let (host_ns, max_host_ns) = (host.totals.host_ns, host.totals.max_host_ns);
    eprintln!(
        "phase=offering node=1 operations={} rate={}",
        args.count, args.rate
    );
    let elapsed = offer(host, &mut ledger, wake)?;
    let phase = Counters::of(&host.totals).since(before);
    let host_ms = (host.totals.host_ns - host_ns) as f64 / 1e6;
    let max_host_ms = host.totals.max_host_ns.max(max_host_ns) as f64 / 1e6;
    let term_end = term(&host.nodes);
    ledger.write(&mut exclusive(&args.root.join("offers.csv"))?)?;
    let (expected, last) = ledger.history(&warmup)?;
    let value = quorum_read(host, last[&group()])?;
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
    use crate::test_ports;
    use std::{
        fs::File,
        io::BufReader,
        process::{Child, Command, Stdio},
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
        run(&args, None, true).unwrap();
    }

    /// Ports BASE+1..3 at offsets 8..10 of a 128-port block below every
    /// ephemeral range. No other suite uses these offsets: counter blocks use
    /// 1..4, 11..14, 101..104 and 111..114, directory ladders land on 5..7 and
    /// 33..35, transfer ladders sit in 10000..20000 (skipped here) and the
    /// shared lease pool never takes offsets below 17 in this band. Lock files
    /// in the shared lease namespace keep concurrent runs of this test apart
    /// for the whole run; probe binds prove the ports free until the nodes
    /// start.
    struct Block {
        base: u16,
        _locks: Vec<File>,
        probes: Vec<(TcpListener, UdpSocket)>,
    }
    fn block() -> Block {
        let shared = test_ports::shared_directory();
        let directory = match test_ports::prepare_directory(&shared) {
            Ok(()) => shared,
            Err(error) => {
                let fallback = test_ports::fallback_directory(&shared, &error);
                test_ports::prepare_directory(&fallback).unwrap();
                fallback
            }
        };
        let blocks = (0..248u16)
            .map(|k| 1024 + 128 * k)
            .filter(|b| !(9984..20096).contains(b));
        for block in blocks {
            let base = block + 7;
            let ports = [base + 1, base + 2, base + 3];
            let Ok(locks) = ports
                .iter()
                .map(|p| {
                    let lock = test_ports::open_lock(&directory.join(format!("{p}.lock")))?;
                    lock.try_lock().map_err(std::io::Error::other)?;
                    Ok::<_, std::io::Error>(lock)
                })
                .collect::<Result<Vec<_>, _>>()
            else {
                continue;
            };
            let Ok(probes) = ports
                .iter()
                .map(|p| {
                    let tcp = TcpListener::bind(("127.0.0.1", *p))?;
                    let udp = UdpSocket::bind(("127.0.0.1", *p))?;
                    Ok::<_, std::io::Error>((tcp, udp))
                })
                .collect::<Result<Vec<_>, _>>()
            else {
                continue;
            };
            return Block {
                base,
                _locks: locks,
                probes,
            };
        }
        panic!("no free cluster port block");
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
    /// Start three nodes re-executing this binary; their stdout lines arrive
    /// on the returned channel, `None` when a stream ends.
    fn spawn(root: &Path, protocol: &str, base: u16) -> (Children, Events) {
        let exe = std::env::current_exe().unwrap();
        let (sender, events) = mpsc::channel();
        let mut children = Children(Vec::new());
        for n in 1..=3u64 {
            let spec = [
                n.to_string(),
                root.join(format!("node{n}")).display().to_string(),
                protocol.to_string(),
                base.to_string(),
                "100".into(),
                "1024".into(),
                "50".into(),
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

    /// The parent protocol of `cluster.py`: `start` once all three are ready,
    /// stdin closed once node 1 has reported. Returns each node's lines.
    fn drive(children: &mut Children, events: &Events, root: &Path) -> BTreeMap<u64, Vec<String>> {
        let mut lines = BTreeMap::<u64, Vec<String>>::new();
        let (mut ready, mut open) = (0, 3);
        let release = |children: &mut Children| {
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
                release(children);
                continue;
            };
            if line.contains("ready node=") {
                ready += 1;
                if ready == 3 {
                    let stdin = children.0[0].stdin.as_mut().unwrap();
                    writeln!(stdin, "start").unwrap();
                }
            }
            if line.contains("mode=cluster ") {
                release(children);
            }
            lines.entry(n).or_default().push(line);
        }
        for (n, child) in children.0.iter_mut().enumerate() {
            let status = child.wait().unwrap();
            assert!(
                status.success(),
                "node {} exited {status}; logs in {}",
                n + 1,
                root.display()
            );
        }
        lines
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

    fn cluster(protocol: &str) {
        let mut block = block();
        let root = std::env::temp_dir().join(format!(
            "voteboat-cluster-{}-{protocol}",
            std::process::id()
        ));
        let _ = std::fs::remove_dir_all(&root);
        std::fs::create_dir(&root).unwrap();
        // The nodes bind their own listeners; release the probes first. The
        // locks stay held until the nodes have exited.
        block.probes.clear();
        let (mut children, events) = spawn(&root, protocol, block.base);
        let lines = drive(&mut children, &events, &root);
        check(&lines);
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
            vec!["1", dir, "tcp", "0", "100", "1024", "50"],
            vec!["1", dir, "tcp", "65533", "100", "1024", "50"],
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
        assert_eq!((parsed.n, parsed.base, parsed.count), (1, 4000, 100));
        assert_eq!((parsed.window, parsed.rate), (1024, 50));
    }
}
