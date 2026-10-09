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
fn open(
    root: &Path,
    mode: NativeOpenMode,
    protocol: NativePeerProtocol,
    capacity: usize,
    clock: &Instant,
) -> Result<Vec<Replica>, Failure> {
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
        match config.open_with_protocol_and_timers(
            protocol,
            TimerConfig {
                heartbeat_ms: 50,
                election_min_ms: 1000,
                election_spread_ms: 1000,
                expirations_per_poll: 32,
            },
            checked(Counter::new(capacity))?,
            Arc::new(ThreadWake::current()),
            MonoTime(clock.elapsed().as_millis() as u64),
        ) {
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
    Ok(replicas)
}
#[derive(Default)]
struct PollTotals {
    rounds: usize,
    host_ns: u128,
    max_host_ns: u128,
    persistence_batches: usize,
    worker_events: usize,
    application_deliveries: usize,
}
fn poll<L: LogStore + Send + 'static>(
    replicas: &mut [Replica<L>],
    clock: &Instant,
    totals: &mut PollTotals,
) -> Result<(), Failure> {
    let start = Instant::now();
    for replica in replicas {
        let progress = checked(replica.poll(
            MonoTime(clock.elapsed().as_millis() as u64),
            NodePollBudget::default(),
        ))?;
        if let Some(p) = progress.replica {
            totals.persistence_batches += p.persistence_batches;
            totals.worker_events += p.worker_events;
            totals.application_deliveries += p.applications;
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
    let deadline = Instant::now() + Duration::from_secs(120);
    let mut totals = PollTotals::default();
    loop {
        poll(replicas, clock, &mut totals)?;
        if done(replicas)? {
            return Ok(totals);
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
fn workload<L: LogStore + Send + 'static>(
    replicas: &mut [Replica<L>],
    clock: &Instant,
    first: usize,
    count: usize,
    window: usize,
    groups: usize,
) -> Result<Measurement, Failure> {
    let start = Instant::now();
    let mut sent = 0;
    let mut pending = BTreeMap::new();
    let mut group_pending = BTreeMap::<GroupIdentity, usize>::new();
    let mut samples = Vec::with_capacity(count);
    let mut max_inflight = 0;
    let polls = until(replicas, clock, |ns| {
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
        for (replica, n) in ns.iter_mut().enumerate() {
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
        Ok(samples.len() == count)
    })?;
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
    let args = std::env::args().skip(1).collect::<Vec<_>>();
    if !matches!(args.len(), 4 | 5 | 7) {
        return Err(
            "usage: native_benchmark FRESH_DIRECTORY tcp|quic OPERATIONS(1..50000) WINDOW(1..32) [GROUPS(1..32) [--offered RATE(1..100000)]]"
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
    let offered_rate = if args.len() == 7 {
        if args[5] != "--offered" {
            return Err("expected --offered RATE".into());
        }
        let rate = args[6].parse::<usize>()?;
        offered::validate(count, rate)?;
        Some(rate)
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
    };
    if shared {
        run(config, &clock, |mode| {
            shared::open(root, mode, protocol, capacity, &clock, groups)
        })
    } else {
        run(config, &clock, |mode| {
            open(root, mode, protocol, capacity, &clock).map(|nodes| (nodes, Vec::new()))
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
    } = config;
    let capacity = count + WARMUP;
    std::fs::create_dir(root)?;
    let mut csv = exclusive(&root.join("samples.csv"))?;
    let mut storage = Vec::new();
    let (mut replicas, traces) = open_cluster(NativeOpenMode::Create)?;
    campaign(&mut replicas, clock, groups)?;
    eprintln!("phase=warmup protocol={protocol:?} window={window}");
    let warmup = workload(&mut replicas, clock, 1, WARMUP, window, groups)?;
    let warmup_last = boundaries(warmup.samples.iter().map(|s| (s.group, s.index)));
    verify(&mut replicas, clock, WARMUP, groups, &warmup_last)?;
    let placement = (1..=groups)
        .map(|g| {
            let group = group_id(g);
            Ok(format!(
                "{g}:{}",
                replicas[leader(&replicas, group)?]
                    .local()
                    .owner
                    .core(group)
                    .unwrap()
                    .local_node()
                    .get()
            ))
        })
        .collect::<Result<Vec<_>, Failure>>()?
        .join(",");
    eprintln!("phase=measurement operations={count} groups={groups} leaders={placement}");
    observe::capture(&mut storage, "before_measurement", &traces);
    let measured = workload(&mut replicas, clock, WARMUP + 1, count, window, groups)?;
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
    campaign(&mut replicas, clock, groups)?;
    verify(&mut replicas, clock, capacity, groups, &last)?;
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
    let recovery_retries = retry(&mut replicas, clock, 1, first_value, groups)?
        + retry(&mut replicas, clock, capacity, last_value, groups)?;
    let retry_index = boundaries(replicas.iter().flat_map(|n| {
        n.local()
            .applications
            .iter()
            .map(|(g, a)| (*g, a.applied_index()))
    }));
    verify(&mut replicas, clock, capacity, groups, &retry_index)?;
    close(replicas, clock)?;
    observe::capture(&mut storage, "after_recover_join", &recovered_traces);
    if shared {
        observe::write_csv(&mut exclusive(&root.join("storage.csv"))?, &storage)?;
    }
    writeln!(
        csv,
        "operation,submitted_ns,completed_ns,latency_ns,applied_index,value,group"
    )?;
    for s in &measured.samples {
        writeln!(
            csv,
            "{},{},{},{},{},{},{}",
            s.operation,
            s.submitted_ns,
            s.completed_ns,
            s.completed_ns - s.submitted_ns,
            s.index,
            s.value,
            s.group.id.get()
        )?;
    }
    csv.sync_all()?;
    let mut latency = measured
        .samples
        .iter()
        .map(|s| s.completed_ns - s.submitted_ns)
        .collect::<Vec<_>>();
    latency.sort_unstable();
    let percentile = |p: usize| latency[(count * p).div_ceil(100) - 1] as f64 / 1000.;
    let election_ms = if shared { 10000 } else { 1000 };
    let summary = format!("protocol={protocol:?} replicas=3 groups={groups} assembly={} leader_placement={} wal_workers_per_replica=1 snapshot_workers_per_replica=1 peer_endpoints_per_replica=1 heartbeat_ms=50 election_min_ms={election_ms} election_spread_ms={election_ms} payload_bytes=8 warmup={WARMUP} operations={count} window={window} max_inflight={} elapsed_s={:.6} applied_ops_s={:.3} p50_us={:.3} p95_us={:.3} p99_us={:.3} max_us={:.3} recovered_value={capacity} recovery_retries={recovery_retries} retry_verified=true workers_joined=true poll_rounds={} host_poll_ms={:.3} max_host_poll_ms={:.3} persistence_batches={} worker_events={} application_deliveries={}\n",
        if shared { "shared" } else { "startup" }, placement, measured.max_inflight, measured.elapsed.as_secs_f64(), count as f64 / measured.elapsed.as_secs_f64(), percentile(50), percentile(95), percentile(99), *latency.last().unwrap() as f64 / 1000., measured.polls.rounds, measured.polls.host_ns as f64 / 1e6, measured.polls.max_host_ns as f64 / 1e6, measured.polls.persistence_batches, measured.polls.worker_events, measured.polls.application_deliveries);
    let mut output = exclusive(&root.join("summary.txt"))?;
    output.write_all(summary.as_bytes())?;
    output.sync_all()?;
    print!("{summary}");
    Ok(())
}
