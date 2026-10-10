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
//! Local three-replica durable benchmark. Embedded public test keys are suitable
//! only for loopback benchmarking. See docs/PERFORMANCE.md for measurement scope.
use std::{
    collections::BTreeMap,
    fs::{File, OpenOptions},
    io::Write,
    net::{TcpListener, UdpSocket},
    path::Path,
    sync::Arc,
    time::{Duration, Instant},
};
use voteboat::{
    application::*,
    identity::*,
    log::*,
    native::{
        connect::{NativePeerProtocol, NativeServiceConnector},
        startup::*,
        tls::*,
        worker::ThreadWake,
    },
    quorum::*,
    runtime::*,
};
#[path = "benchmark/shared.rs"]
mod shared;
type Failure = Box<dyn std::error::Error>;
type Replica<
    L = voteboat::native::log_store::NativeLogStore<voteboat::native::log_store::FileLogIo>,
> = Node<
    voteboat::native::runtime::FairScheduler,
    voteboat::native::runtime::DeadlineQueue,
    voteboat::native::runtime::JitterEntropy,
    Counter,
    voteboat::native::worker::NativeLogWorker<L>,
    voteboat::native::outbound::NativeOutbound,
    voteboat::native::snapshot_worker::NativeSnapshotWorker<
        voteboat::native::snapshot_store::NativeSnapshotStore<
            voteboat::native::snapshot_store::FileSnapshotIo,
        >,
    >,
    NativeServiceConnector,
    voteboat::native::transport::NativeTransportFactory<voteboat::native::wire::NativeWireCodec>,
>;
#[path = "benchmark/failure.rs"]
mod failure;
#[path = "benchmark/observe.rs"]
mod observe;
#[path = "benchmark/offered.rs"]
mod offered;
const WARMUP: usize = 64;
fn checked<T, E: std::fmt::Debug>(v: Result<T, E>) -> Result<T, Failure> {
    v.map_err(|e| format!("{e:?}").into())
}
fn node(n: u64) -> NodeId {
    NodeId::new(n).unwrap()
}
fn group() -> GroupIdentity {
    group_id(1)
}
fn group_id(n: usize) -> GroupIdentity {
    GroupIdentity {
        id: GroupId::new(n as u128).unwrap(),
        incarnation: GroupIncarnation::new(1).unwrap(),
    }
}
fn store(n: u64) -> StoreIdentity {
    StoreIdentity {
        id: StoreId::new(n.into()).unwrap(),
        incarnation: StoreIncarnation::new(1).unwrap(),
    }
}
fn cert(n: u64) -> &'static [u8] {
    match n {
        1 => include_bytes!("../tests/fixtures/tls/node1.der"),
        2 => include_bytes!("../tests/fixtures/tls/node2.der"),
        3 => include_bytes!("../tests/fixtures/tls/node3.der"),
        _ => unreachable!(),
    }
}
fn tls(n: u64) -> NativeTlsConfig {
    let key: &[u8] = match n {
        1 => include_bytes!("../tests/fixtures/tls/node1-key.der"),
        2 => include_bytes!("../tests/fixtures/tls/node2-key.der"),
        3 => include_bytes!("../tests/fixtures/tls/node3-key.der"),
        _ => unreachable!(),
    };
    NativeTlsConfig::new(TlsCredentials {
        roots: vec![include_bytes!("../tests/fixtures/tls/ca.der").to_vec()],
        certificate_chain: vec![cert(n).to_vec()],
        private_key: key.to_vec(),
    })
    .unwrap()
}
#[cfg(test)]
fn open(
    root: &Path,
    mode: NativeOpenMode,
    protocol: NativePeerProtocol,
    capacity: usize,
    clock: &Instant,
) -> Result<Vec<Replica>, Failure> {
    open_recorded(root, mode, protocol, capacity, clock, false).map(|(nodes, _)| nodes)
}
fn open_recorded(
    root: &Path,
    mode: NativeOpenMode,
    protocol: NativePeerProtocol,
    capacity: usize,
    clock: &Instant,
    record: bool,
) -> Result<(Vec<Replica>, Vec<observe::Trace>), Failure> {
    let reservations = (0..3)
        .map(|_| {
            (0..32)
                .find_map(|_| {
                    let tcp = TcpListener::bind("127.0.0.1:0").ok()?;
                    let udp = UdpSocket::bind(tcp.local_addr().ok()?).ok()?;
                    Some((tcp, udp))
                })
                .ok_or("no local TCP/UDP endpoint")
        })
        .collect::<Result<Vec<_>, _>>()?;
    let addresses = (1..=3)
        .zip(
            reservations
                .iter()
                .map(|(tcp, _)| tcp.local_addr().unwrap()),
        )
        .collect::<BTreeMap<_, _>>();
    let bootstrap = Bootstrap {
        group: group(),
        configuration: ConfigurationId::new(1).unwrap(),
        policy: checked(Policy::new(
            Tree::Majority((1..=3).map(|n| Tree::Voter(node(n))).collect()),
            Limits::default(),
        ))?,
        voter_stores: (1..=3).map(|n| (node(n), store(n))).collect(),
    };
    drop(reservations);
    let mut replicas = Vec::new();
    let mut traces = Vec::new();
    for n in 1..=3 {
        let config = NativeStartup {
            directory: root.join(format!("replica{n}")),
            mode,
            node: node(n),
            store: store(n),
            bootstrap: bootstrap.clone(),
            listen: addresses[&n],
            peers: (1..=3)
                .filter(|p| *p != n)
                .map(|p| {
                    (
                        node(p),
                        NativeStartupPeer {
                            address: addresses[&p],
                            certificate: cert(p).to_vec(),
                            server_name: format!("node{p}.voteboat.test"),
                        },
                    )
                })
                .collect(),
            tls: tls(n),
            entropy_seed: n + 17,
            limits: NodeLimits::default(),
        };
        let timers = TimerConfig {
            heartbeat_ms: 50,
            election_min_ms: 1000,
            election_spread_ms: 1000,
            expirations_per_poll: 32,
        };
        let app = checked(Counter::new(capacity))?;
        let wake = Arc::new(ThreadWake::current());
        let now = MonoTime(clock.elapsed().as_millis() as u64);
        let opened = if record {
            let timing = voteboat::native::log_store::JournalTimings::default();
            traces.push(observe::Trace::journal(timing.clone()));
            config.open_with_journal_timings(
                NativeStartupTimings {
                    protocol,
                    timers,
                    journal: timing,
                },
                app,
                wake,
                now,
            )
        } else {
            config.open_with_protocol_and_timers(protocol, timers, app, wake, now)
        };
        match opened {
            Ok(r) => replicas.push(r),
            Err(mut e) => {
                let deadline = Instant::now() + Duration::from_secs(10);
                while !e.try_cleanup()? {
                    if Instant::now() > deadline {
                        return Err("startup cleanup timed out".into());
                    }
                    std::thread::park_timeout(Duration::from_millis(1));
                }
                return Err(e.reason.into());
            }
        }
    }
    Ok((replicas, traces))
}
#[derive(Default)]
struct PollTotals {
    rounds: usize,
    host_ns: u128,
    max_host_ns: u128,
    persistence_batches: usize,
    worker_events: usize,
    application_deliveries: usize,
    snapshot_events: usize,
    snapshot_installs: [usize; 3],
    snapshot_send_refusals: [usize; 3],
    snapshot_supplies: usize,
}
fn poll<L: LogStore + Send + 'static>(
    replicas: &mut [Replica<L>],
    clock: &Instant,
    totals: &mut PollTotals,
) -> Result<(), Failure> {
    poll_selected(replicas, clock, totals, None)
}
fn poll_selected<L: LogStore + Send + 'static>(
    replicas: &mut [Replica<L>],
    clock: &Instant,
    totals: &mut PollTotals,
    paused: Option<usize>,
) -> Result<(), Failure> {
    let start = Instant::now();
    for (i, replica) in replicas.iter_mut().enumerate() {
        if paused == Some(i) {
            continue;
        }
        let progress = checked(replica.poll(
            MonoTime(clock.elapsed().as_millis() as u64),
            NodePollBudget::default(),
        ))?;
        if let Some(p) = progress.replica {
            totals.persistence_batches += p.persistence_batches;
            totals.worker_events += p.worker_events;
            totals.application_deliveries += p.applications;
            totals.snapshot_events += p.snapshot_events;
            totals.snapshot_installs[i] += p.snapshot_installs;
            totals.snapshot_send_refusals[i] += p.snapshot_send_refusals;
            totals.snapshot_supplies += p.snapshot_supplies;
            for step in p.steps {
                if let Some(e) = step.error {
                    return Err(format!("runtime step failed: {e:?}").into());
                }
            }
        }
    }
    let elapsed = start.elapsed().as_nanos();
    totals.rounds += 1;
    totals.host_ns += elapsed;
    totals.max_host_ns = totals.max_host_ns.max(elapsed);
    Ok(())
}
fn until<L: LogStore + Send + 'static>(
    replicas: &mut [Replica<L>],
    clock: &Instant,
    mut done: impl FnMut(&mut [Replica<L>]) -> Result<bool, Failure>,
) -> Result<PollTotals, Failure> {
    let mut totals = PollTotals::default();
    drive(replicas, clock, &mut totals, &mut done)?;
    Ok(totals)
}
fn drive<L: LogStore + Send + 'static>(
    replicas: &mut [Replica<L>],
    clock: &Instant,
    totals: &mut PollTotals,
    mut done: impl FnMut(&mut [Replica<L>]) -> Result<bool, Failure>,
) -> Result<(), Failure> {
    let deadline = Instant::now() + Duration::from_secs(120);
    loop {
        poll(replicas, clock, totals)?;
        if done(replicas)? {
            return Ok(());
        }
        if Instant::now() > deadline {
            return Err("native progress timed out; retained run is invalid".into());
        }
        std::thread::park_timeout(Duration::from_micros(100));
    }
}
fn leader<L: LogStore + Send + 'static>(
    replicas: &[Replica<L>],
    group: GroupIdentity,
) -> Result<usize, Failure> {
    replicas
        .iter()
        .position(|n| {
            let core = n.local().owner.core(group).unwrap();
            core.role() == voteboat::raft::Role::Leader
                && core.state().term_at(core.state().commit_index)
                    == Some(core.state().hard_state.term)
        })
        .ok_or_else(|| format!("no ready leader for group {}", group.id.get()).into())
}
fn campaign<L: LogStore + Send + 'static>(
    replicas: &mut [Replica<L>],
    clock: &Instant,
    groups: usize,
) -> Result<(), Failure> {
    for g in 1..=groups {
        // Leave an existing leader alone; reopening may already have elected one.
        if leader(replicas, group_id(g)).is_err() {
            checked(replicas[0].control(group_id(g), NodeControl::Campaign))?;
        }
    }
    until(replicas, clock, |ns| {
        Ok((1..=groups).all(|g| {
            let group = group_id(g);
            leader(ns, group).is_ok_and(|i| {
                let core = ns[i].local().owner.core(group).unwrap();
                ns.iter().all(|r| {
                    r.local().applications[&group].applied_index() == core.state().commit_index
                })
            })
        }))
    })?;
    Ok(())
}
fn operation_group(operation: usize, groups: usize) -> GroupIdentity {
    group_id((operation - 1) % groups + 1)
}
struct Sample {
    group: GroupIdentity,
    operation: u128,
    submitted_ns: u128,
    completed_ns: u128,
    index: u64,
    value: i64,
}
struct Measurement {
    elapsed: Duration,
    samples: Vec<Sample>,
    max_inflight: usize,
    polls: PollTotals,
}
struct Workload<'a> {
    first: usize,
    count: usize,
    window: usize,
    groups: usize,
    diagnostic: Option<(&'a Path, &'a str)>,
}
fn workload<L: LogStore + Send + 'static>(
    replicas: &mut [Replica<L>],
    clock: &Instant,
    spec: Workload<'_>,
) -> Result<Measurement, Failure> {
    let Workload {
        first,
        count,
        window,
        groups,
        diagnostic,
    } = spec;
    let start = Instant::now();
    let mut sent = 0;
    let mut pending = BTreeMap::new();
    let mut group_pending = BTreeMap::<GroupIdentity, usize>::new();
    let mut samples = Vec::with_capacity(count);
    let mut max_inflight = 0;
    let mut polls = PollTotals::default();
    let result = drive(replicas, clock, &mut polls, |ns| {
        while sent < count && pending.len() < window {
            let operation = (first + sent) as u128;
            let submitted = start.elapsed().as_nanos();
            let group = operation_group(first + sent, groups);
            if group_pending.get(&group).copied().unwrap_or(0) >= window.div_ceil(groups) {
                break;
            }
            let replica = leader(ns, group)?;
            let ticket = ns[replica]
                .propose(ClientRequest {
                    group,
                    operation: OperationId::new(operation).unwrap(),
                    bytes: 1i64.to_le_bytes().to_vec(),
                })
                .map_err(|e| format!("admission refused; invalid run: {:?}", e.reason))?;
            if pending
                .insert((replica, ticket.sequence), (ticket, submitted))
                .is_some()
            {
                return Err("duplicate live ticket".into());
            }
            sent += 1;
            *group_pending.entry(group).or_default() += 1;
            max_inflight = max_inflight.max(pending.len());
        }
        collect_workload_receipts(
            ns,
            start,
            groups,
            &mut pending,
            &mut group_pending,
            &mut samples,
        )?;
        Ok(samples.len() == count)
    });
    if let Err(error) = result {
        if let Some((root, phase)) = diagnostic {
            let unresolved = pending
                .values()
                .map(|(ticket, submitted)| {
                    (ticket.group.id.get(), ticket.operation.get(), *submitted)
                })
                .collect::<Vec<_>>();
            if let Err(retain) = failure::retain(
                root,
                phase,
                &error.to_string(),
                &samples,
                &unresolved,
                &polls,
                replicas,
            ) {
                eprintln!("failed to retain diagnostic: {retain}; original failure: {error}");
            }
        }
        return Err(error);
    }
    Ok(Measurement {
        elapsed: start.elapsed(),
        samples,
        max_inflight,
        polls,
    })
}
fn boundaries(samples: impl Iterator<Item = (GroupIdentity, u64)>) -> BTreeMap<GroupIdentity, u64> {
    let mut result = BTreeMap::new();
    for (g, index) in samples {
        result
            .entry(g)
            .and_modify(|old: &mut u64| *old = (*old).max(index))
            .or_insert(index);
    }
    result
}
fn verify<L: LogStore + Send + 'static>(
    replicas: &mut [Replica<L>],
    clock: &Instant,
    total: usize,
    groups: usize,
    last: &BTreeMap<GroupIdentity, u64>,
) -> Result<(), Failure> {
    let expected = (1..=groups)
        .map(|g| {
            (
                group_id(g),
                if total >= g {
                    ((total - g) / groups + 1) as i64
                } else {
                    0
                },
            )
        })
        .collect();
    verify_expected(replicas, clock, &expected, last)
}
fn verify_expected<L: LogStore + Send + 'static>(
    replicas: &mut [Replica<L>],
    clock: &Instant,
    expected: &BTreeMap<GroupIdentity, i64>,
    last: &BTreeMap<GroupIdentity, u64>,
) -> Result<(), Failure> {
    until(replicas, clock, |ns| {
        Ok(last.iter().all(|(g, index)| {
            ns.iter()
                .all(|n| n.local().applications[g].applied_index() >= *index)
        }))
    })?;
    for (&group, &expected) in expected {
        let g = group.id.get();
        let last_index = last[&group];
        for n in replicas.iter() {
            if checked(n.local().applications[&group].read_applied(last_index))? != expected {
                return Err(format!("replica value mismatch for group {g}").into());
            }
        }
        let replica = leader(replicas, group)?;
        let ticket = checked(replicas[replica].read(group, ()))?;
        until(replicas, clock, |ns| {
            let Some(output) = ns[replica].poll_read() else {
                return Ok(false);
            };
            if output.ticket() != ticket {
                return Err("read ticket mismatch".into());
            }
            match checked(ns[replica].complete_read(output).map_err(|e| e.reason))? {
                ReadOutcome::Read {
                    result: Ok(value), ..
                } if value == expected => Ok(true),
                other => Err(format!("group {g} quorum read failed: {other:?}").into()),
            }
        })?;
    }
    Ok(())
}
fn retry_once<L: LogStore + Send + 'static>(
    replicas: &mut [Replica<L>],
    clock: &Instant,
    operation: usize,
    expected: i64,
    groups: usize,
) -> Result<bool, Failure> {
    let group = operation_group(operation, groups);
    let replica = leader(replicas, group)?;
    let ticket = replicas[replica]
        .propose(ClientRequest {
            group,
            operation: OperationId::new(operation as u128).unwrap(),
            bytes: 1i64.to_le_bytes().to_vec(),
        })
        .map_err(|e| format!("retry refused: {:?}", e.reason))?;
    let mut changed = false;
    until(replicas, clock, |ns| {
        let Some(output) = ns[replica].poll_client() else {
            return Ok(false);
        };
        if output.ticket() != ticket {
            return Err("retry ticket mismatch".into());
        }
        match checked(ns[replica].complete_client(output).map_err(|e| e.reason))? {
            ClientOutcome::Unknown(ClientUnknown::LeadershipChanged)
            | ClientOutcome::NotProposed(voteboat::raft::RaftError::NotLeader) => {
                changed = true;
                Ok(true)
            }
            ClientOutcome::Applied { receipt, .. }
                if receipt.operation == ticket.operation
                    && receipt.duplicate
                    && receipt.outcome == CounterOutcome::Value(expected) =>
            {
                Ok(true)
            }
            other => Err(format!("retry verification failed: {other:?}").into()),
        }
    })?;
    Ok(!changed)
}
fn retry<L: LogStore + Send + 'static>(
    replicas: &mut [Replica<L>],
    clock: &Instant,
    operation: usize,
    expected: i64,
    groups: usize,
) -> Result<usize, Failure> {
    for attempt in 0..4 {
        campaign(replicas, clock, groups)?;
        if retry_once(replicas, clock, operation, expected, groups)? {
            return Ok(attempt);
        }
        eprintln!("phase=recovery-retry operation={operation} leadership_changed=true");
    }
    Err("recovery retry repeatedly lost leadership".into())
}
fn close<L: LogStore + Send + 'static>(
    mut replicas: Vec<Replica<L>>,
    clock: &Instant,
) -> Result<(), Failure> {
    for n in &mut replicas {
        n.begin_shutdown();
    }
    until(&mut replicas, clock, |ns| {
        Ok(ns.iter().all(|n| n.is_drained()))
    })?;
    for n in replicas {
        let mut parts = n.into_parts().map_err(|_| "node not drained")?;
        let mut dialer = parts
            .peers
            .take()
            .ok_or("missing peers")?
            .connector
            .into_dialer()
            .map_err(|_| "connector not drained")?;
        let mut snapshots = parts.local.snapshots.take().ok_or("missing snapshots")?;
        let deadline = Instant::now() + Duration::from_secs(10);
        let (mut log, mut snap) = (false, false);
        loop {
            let dial = match &mut dialer {
                Some(d) => checked(d.try_finish())?,
                None => true,
            };
            if !log {
                log = parts.local.persistence.try_reclaim()?.is_some();
            }
            if !snap {
                snap = snapshots.worker.try_reclaim()?.is_some();
            }
            if dial && log && snap {
                break;
            }
            if Instant::now() > deadline {
                return Err("worker join timed out".into());
            }
            std::thread::park_timeout(Duration::from_micros(100));
        }
    }
    Ok(())
}
fn exclusive(path: &Path) -> Result<File, Failure> {
    Ok(OpenOptions::new().create_new(true).write(true).open(path)?)
}
fn main() -> Result<(), Failure> {
    let mut args = std::env::args().skip(1).collect::<Vec<_>>();
    let journal_timings = args.last().is_some_and(|arg| arg == "--journal-timings");
    if journal_timings {
        args.pop();
        if args.len() != 4 {
            return Err("journal timings require startup reference mode".into());
        }
    }
    if !matches!(args.len(), 4 | 5 | 7 | 9 | 11) {
        return Err(
            "usage: native_benchmark FRESH_DIRECTORY tcp|quic OPERATIONS(1..50000) WINDOW(1..32) [--journal-timings (startup only)] [GROUPS(1..32) [--offered RATE(1..100000) [--maintenance SECONDS(1..60) [--pause-follower START:DURATION]]]]"
                .into(),
        );
    }
    let [root, protocol, operations, window] = &args[..4] else {
        unreachable!()
    };
    let shared = args.len() >= 5;
    let groups = if shared { args[4].parse::<usize>()? } else { 1 };
    if !(1..=32).contains(&groups) {
        return Err("group count out of bounds".into());
    }
    let protocol = match protocol.as_str() {
        "tcp" => NativePeerProtocol::TcpTls,
        #[cfg(feature = "quic")]
        "quic" => NativePeerProtocol::Quic,
        _ => return Err("unsupported protocol".into()),
    };
    let count = operations.parse::<usize>()?;
    let window = window.parse::<usize>()?;
    if !(1..=50000).contains(&count) || !(1..=32).contains(&window) {
        return Err("count/window out of bounds".into());
    }
    let offered_rate = if args.len() >= 7 {
        if args[5] != "--offered" {
            return Err("expected --offered RATE".into());
        }
        let rate = args[6].parse::<usize>()?;
        offered::validate(count, rate)?;
        Some(rate)
    } else {
        None
    };
    let maintenance = if args.len() >= 9 {
        if args[7] != "--maintenance" {
            return Err("expected --maintenance SECONDS".into());
        }
        let pause = if args.len() == 11 {
            if args[9] != "--pause-follower" {
                return Err("expected --pause-follower START:DURATION".into());
            }
            Some(args[10].as_str())
        } else {
            None
        };
        Some(offered::maintenance_config(
            count,
            offered_rate.unwrap(),
            args[8].parse()?,
            pause,
        )?)
    } else {
        None
    };
    let capacity = count + WARMUP;
    if capacity.div_ceil(groups) + 64 > LogLimits::default().max_entries_per_group {
        return Err(
            "workload exceeds default per-group log capacity (including control reserve)".into(),
        );
    }
    let root = Path::new(root);
    let clock = Instant::now();
    let config = RunConfig {
        root,
        protocol,
        count,
        window,
        groups,
        shared,
        offered_rate,
        maintenance,
    };
    if shared {
        run(config, &clock, |mode| {
            shared::open(root, mode, protocol, capacity, &clock, groups)
        })
    } else {
        run(config, &clock, |mode| {
            open_recorded(root, mode, protocol, capacity, &clock, journal_timings)
        })
    }
}
struct RunConfig<'a> {
    root: &'a Path,
    protocol: NativePeerProtocol,
    count: usize,
    window: usize,
    groups: usize,
    shared: bool,
    offered_rate: Option<usize>,
    maintenance: Option<offered::MaintenanceConfig>,
}
fn run<L: LogStore + Send + 'static>(
    config: RunConfig<'_>,
    clock: &Instant,
    open_cluster: impl Fn(NativeOpenMode) -> Result<(Vec<Replica<L>>, Vec<observe::Trace>), Failure>,
) -> Result<(), Failure> {
    if let Some(rate) = config.offered_rate {
        return offered::run(config, rate, clock, open_cluster);
    }
    let RunConfig {
        root,
        protocol,
        count,
        window,
        groups,
        shared,
        offered_rate: _,
        maintenance: _,
    } = config;
    let capacity = count + WARMUP;
    std::fs::create_dir(root)?;
    let mut csv = exclusive(&root.join("samples.csv"))?;
    let mut storage = Vec::new();
    let (mut replicas, traces) = open_cluster(NativeOpenMode::Create)?;
    campaign(&mut replicas, clock, groups)?;
    eprintln!("phase=warmup protocol={protocol:?} window={window}");
    let warmup = match workload(
        &mut replicas,
        clock,
        Workload {
            first: 1,
            count: WARMUP,
            window,
            groups,
            diagnostic: Some((root, "warmup")),
        },
    ) {
        Ok(result) => result,
        Err(error) => {
            if !shared {
                observe::retain_journal(root, &mut storage, "warmup_failure", &traces);
            }
            return failure::cleanup(replicas, clock, root, error);
        }
    };
    let warmup_last = boundaries(warmup.samples.iter().map(|s| (s.group, s.index)));
    verify(&mut replicas, clock, WARMUP, groups, &warmup_last)?;
    let placement = leader_placement(&replicas, groups)?;
    eprintln!("phase=measurement operations={count} groups={groups} leaders={placement}");
    observe::capture(&mut storage, "before_measurement", &traces);
    let measured = match workload(
        &mut replicas,
        clock,
        Workload {
            first: WARMUP + 1,
            count,
            window,
            groups,
            diagnostic: Some((root, "measurement")),
        },
    ) {
        Ok(result) => result,
        Err(error) => {
            if !shared {
                observe::retain_journal(root, &mut storage, "measurement_failure", &traces);
            }
            return failure::cleanup(replicas, clock, root, error);
        }
    };
    failure::write_samples(&mut csv, &measured.samples)?;
    observe::capture(&mut storage, "after_measurement", &traces);
    let last = boundaries(
        warmup
            .samples
            .iter()
            .chain(&measured.samples)
            .map(|s| (s.group, s.index)),
    );
    verify(&mut replicas, clock, capacity, groups, &last)?;
    close(replicas, clock)?;
    observe::capture(&mut storage, "after_create_join", &traces);
    eprintln!("phase=recovery-verification");
    let (mut replicas, recovered_traces) = open_cluster(NativeOpenMode::Recover)?;
    let recovery_retries = verify_recovered_workload(
        &mut replicas,
        clock,
        groups,
        &last,
        &warmup,
        &measured,
        capacity,
    )?;
    close(replicas, clock)?;
    observe::capture(&mut storage, "after_recover_join", &recovered_traces);
    if shared {
        observe::write_csv(&mut exclusive(&root.join("storage.csv"))?, &storage)?;
    } else if !traces.is_empty() {
        observe::write_csv(&mut exclusive(&root.join("journal.csv"))?, &storage)?;
    }
    report_run(
        &config,
        &placement,
        &measured,
        recovery_retries,
        !traces.is_empty(),
    )
}

fn collect_workload_receipts<L: LogStore + Send + 'static>(
    replicas: &mut [Replica<L>],
    start: Instant,
    groups: usize,
    pending: &mut BTreeMap<(usize, u64), (ClientTicket, u128)>,
    group_pending: &mut BTreeMap<GroupIdentity, usize>,
    samples: &mut Vec<Sample>,
) -> Result<(), Failure> {
    for (replica, n) in replicas.iter_mut().enumerate() {
        while let Some(output) = n.poll_client() {
            let completed = start.elapsed().as_nanos();
            let ticket = output.ticket();
            let (original, submitted) = pending
                .remove(&(replica, ticket.sequence))
                .ok_or("untracked receipt")?;
            if ticket != original {
                return Err("ticket identity mismatch".into());
            }
            *group_pending
                .get_mut(&ticket.group)
                .ok_or("untracked group")? -= 1;
            let receipt = match checked(n.complete_client(output).map_err(|e| e.reason))? {
                ClientOutcome::Applied { receipt, .. } => receipt,
                other => {
                    return Err(format!(
                        "operation {}: non-applied outcome invalidates measurement: {other:?}",
                        ticket.operation.get()
                    )
                    .into())
                }
            };
            let CounterOutcome::Value(value) = receipt.outcome else {
                return Err("non-value receipt".into());
            };
            if receipt.operation != ticket.operation || receipt.duplicate {
                return Err("invalid useful-write receipt".into());
            }
            if value != ((receipt.operation.get() - 1) / groups as u128 + 1) as i64 {
                return Err("receipt disagrees with round-robin group history".into());
            }
            samples.push(Sample {
                group: ticket.group,
                operation: receipt.operation.get(),
                submitted_ns: submitted,
                completed_ns: completed,
                index: receipt.index,
                value,
            });
        }
    }
    Ok(())
}

fn verify_recovered_workload<L: LogStore + Send + 'static>(
    replicas: &mut [Replica<L>],
    clock: &Instant,
    groups: usize,
    last: &BTreeMap<GroupIdentity, u64>,
    warmup: &Measurement,
    measured: &Measurement,
    capacity: usize,
) -> Result<usize, Failure> {
    campaign(replicas, clock, groups)?;
    verify(replicas, clock, capacity, groups, last)?;
    let first_value = warmup
        .samples
        .iter()
        .find(|s| s.operation == 1)
        .unwrap()
        .value;
    let last_value = measured
        .samples
        .iter()
        .find(|s| s.operation == capacity as u128)
        .unwrap()
        .value;
    let recovery_retries = retry(replicas, clock, 1, first_value, groups)?
        + retry(replicas, clock, capacity, last_value, groups)?;
    let retry_index = boundaries(replicas.iter().flat_map(|n| {
        n.local()
            .applications
            .iter()
            .map(|(g, a)| (*g, a.applied_index()))
    }));
    verify(replicas, clock, capacity, groups, &retry_index)?;
    Ok(recovery_retries)
}

fn report_run(
    config: &RunConfig<'_>,
    placement: &str,
    measured: &Measurement,
    recovery_retries: usize,
    journal: bool,
) -> Result<(), Failure> {
    let RunConfig {
        root,
        protocol,
        count,
        window,
        groups,
        shared,
        ..
    } = *config;
    let capacity = count + WARMUP;
    let mut latency = measured
        .samples
        .iter()
        .map(|s| s.completed_ns - s.submitted_ns)
        .collect::<Vec<_>>();
    latency.sort_unstable();
    let percentile = |p: usize| latency[(count * p).div_ceil(100) - 1] as f64 / 1000.;
    let election_ms = if shared { 10000 } else { 1000 };
    let diagnostic = if !shared && journal {
        " journal_timings=true"
    } else {
        ""
    };
    let summary = format!("protocol={protocol:?} replicas=3 groups={groups} assembly={} leader_placement={} wal_workers_per_replica=1 snapshot_workers_per_replica=1 peer_endpoints_per_replica=1 heartbeat_ms=50 election_min_ms={election_ms} election_spread_ms={election_ms} payload_bytes=8 warmup={WARMUP} operations={count} window={window} max_inflight={} elapsed_s={:.6} applied_ops_s={:.3} p50_us={:.3} p95_us={:.3} p99_us={:.3} max_us={:.3} recovered_value={capacity} recovery_retries={recovery_retries} retry_verified=true workers_joined=true poll_rounds={} host_poll_ms={:.3} max_host_poll_ms={:.3} persistence_batches={} worker_events={} application_deliveries={}{diagnostic}\n",
        if shared { "shared" } else { "startup" }, placement, measured.max_inflight, measured.elapsed.as_secs_f64(), count as f64 / measured.elapsed.as_secs_f64(), percentile(50), percentile(95), percentile(99), *latency.last().unwrap() as f64 / 1000., measured.polls.rounds, measured.polls.host_ns as f64 / 1e6, measured.polls.max_host_ns as f64 / 1e6, measured.polls.persistence_batches, measured.polls.worker_events, measured.polls.application_deliveries);
    let mut output = exclusive(&root.join("summary.txt"))?;
    output.write_all(summary.as_bytes())?;
    output.sync_all()?;
    print!("{summary}");
    Ok(())
}

fn leader_placement<L: LogStore + Send + 'static>(
    replicas: &[Replica<L>],
    groups: usize,
) -> Result<String, Failure> {
    Ok((1..=groups)
        .map(|g| {
            let group = group_id(g);
            Ok(format!(
                "{g}:{}",
                replicas[leader(replicas, group)?]
                    .local()
                    .owner
                    .core(group)
                    .unwrap()
                    .local_node()
                    .get()
            ))
        })
        .collect::<Result<Vec<_>, Failure>>()?
        .join(","))
}
