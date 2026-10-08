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
        node::*,
        startup::*,
        tls::*,
        worker::ThreadWake,
    },
    quorum::*,
    runtime::*,
};
type Failure = Box<dyn std::error::Error>;
type Replica = NativeNode<Counter, NativeServiceConnector>;
const WARMUP: usize = 64;
fn checked<T, E: std::fmt::Debug>(v: Result<T, E>) -> Result<T, Failure> {
    v.map_err(|e| format!("{e:?}").into())
}
fn node(n: u64) -> NodeId {
    NodeId::new(n).unwrap()
}
fn group() -> GroupIdentity {
    GroupIdentity {
        id: GroupId::new(1).unwrap(),
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
fn poll(replicas: &mut [Replica], clock: &Instant, totals: &mut PollTotals) -> Result<(), Failure> {
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
fn until(
    replicas: &mut [Replica],
    clock: &Instant,
    mut done: impl FnMut(&mut [Replica]) -> Result<bool, Failure>,
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
fn campaign(replicas: &mut [Replica], clock: &Instant) -> Result<(), Failure> {
    checked(replicas[0].control(group(), NodeControl::Campaign))?;
    let mut leader = None;
    until(replicas, clock, |ns| {
        leader = ns.iter().position(|n| {
            let core = n.local().owner.core(group()).unwrap();
            core.role() == voteboat::raft::Role::Leader
                && core.state().term_at(core.state().commit_index)
                    == Some(core.state().hard_state.term)
                && ns.iter().all(|r| {
                    r.local().applications[&group()].applied_index() == core.state().commit_index
                })
        });
        Ok(leader.is_some())
    })?;
    replicas.swap(0, leader.unwrap());
    Ok(())
}
struct Sample {
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
fn workload(
    replicas: &mut [Replica],
    clock: &Instant,
    first: usize,
    count: usize,
    window: usize,
) -> Result<Measurement, Failure> {
    let start = Instant::now();
    let mut sent = 0;
    let mut pending = BTreeMap::new();
    let mut samples = Vec::with_capacity(count);
    let mut max_inflight = 0;
    let polls = until(replicas, clock, |ns| {
        while sent < count && pending.len() < window {
            let operation = (first + sent) as u128;
            let submitted = start.elapsed().as_nanos();
            let ticket = ns[0]
                .propose(ClientRequest {
                    group: group(),
                    operation: OperationId::new(operation).unwrap(),
                    bytes: 1i64.to_le_bytes().to_vec(),
                })
                .map_err(|e| format!("admission refused; invalid run: {:?}", e.reason))?;
            if pending
                .insert(ticket.sequence, (ticket, submitted))
                .is_some()
            {
                return Err("duplicate live ticket".into());
            }
            sent += 1;
            max_inflight = max_inflight.max(pending.len());
        }
        while let Some(output) = ns[0].poll_client() {
            let completed = start.elapsed().as_nanos();
            let ticket = output.ticket();
            let (original, submitted) = pending
                .remove(&ticket.sequence)
                .ok_or("untracked receipt")?;
            if ticket != original {
                return Err("ticket identity mismatch".into());
            }
            let receipt = match checked(ns[0].complete_client(output).map_err(|e| e.reason))? {
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
            samples.push(Sample {
                operation: receipt.operation.get(),
                submitted_ns: submitted,
                completed_ns: completed,
                index: receipt.index,
                value,
            });
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
fn verify(
    replicas: &mut [Replica],
    clock: &Instant,
    expected: i64,
    last_index: u64,
) -> Result<(), Failure> {
    until(replicas, clock, |ns| {
        Ok(ns
            .iter()
            .all(|n| n.local().applications[&group()].applied_index() >= last_index))
    })?;
    for n in replicas.iter() {
        let app = &n.local().applications[&group()];
        if checked(app.read_applied(last_index))? != expected {
            return Err("replica value mismatch".into());
        }
    }
    checked(replicas[0].read(group(), ())).map_err(|e| format!("{e:?}"))?;
    until(replicas, clock, |ns| {
        let Some(output) = ns[0].poll_read() else {
            return Ok(false);
        };
        match checked(ns[0].complete_read(output).map_err(|e| e.reason))? {
            ReadOutcome::Read {
                result: Ok(value), ..
            } if value == expected => Ok(true),
            other => Err(format!("quorum read verification failed: {other:?}").into()),
        }
    })
    .map(|_| ())
}
fn retry_once(
    replicas: &mut [Replica],
    clock: &Instant,
    operation: usize,
    expected: i64,
) -> Result<bool, Failure> {
    let ticket = replicas[0]
        .propose(ClientRequest {
            group: group(),
            operation: OperationId::new(operation as u128).unwrap(),
            bytes: 1i64.to_le_bytes().to_vec(),
        })
        .map_err(|e| format!("retry refused: {:?}", e.reason))?;
    let mut changed = false;
    until(replicas, clock, |ns| {
        let Some(output) = ns[0].poll_client() else {
            return Ok(false);
        };
        if output.ticket() != ticket {
            return Err("retry ticket mismatch".into());
        }
        match checked(ns[0].complete_client(output).map_err(|e| e.reason))? {
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
fn retry(
    replicas: &mut [Replica],
    clock: &Instant,
    operation: usize,
    expected: i64,
) -> Result<usize, Failure> {
    for attempt in 0..4 {
        campaign(replicas, clock)?;
        if retry_once(replicas, clock, operation, expected)? {
            return Ok(attempt);
        }
        eprintln!("phase=recovery-retry operation={operation} leadership_changed=true");
    }
    Err("recovery retry repeatedly lost leadership".into())
}
fn close(mut replicas: Vec<Replica>, clock: &Instant) -> Result<(), Failure> {
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
    let [root, protocol, operations, window] = args.as_slice() else {
        return Err(
            "usage: native_benchmark FRESH_DIRECTORY tcp|quic OPERATIONS(1..50000) WINDOW(1..32)"
                .into(),
        );
    };
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
    let root = Path::new(root);
    std::fs::create_dir(root)?;
    let mut csv = exclusive(&root.join("samples.csv"))?;
    let capacity = count + WARMUP;
    let clock = Instant::now();
    let mut replicas = open(root, NativeOpenMode::Create, protocol, capacity, &clock)?;
    campaign(&mut replicas, &clock)?;
    eprintln!("phase=warmup protocol={protocol:?} window={window}");
    let warmup = workload(&mut replicas, &clock, 1, WARMUP, window)?;
    let warmup_last = warmup.samples.iter().map(|s| s.index).max().unwrap();
    verify(&mut replicas, &clock, WARMUP as i64, warmup_last)?;
    eprintln!("phase=measurement operations={count}");
    let measured = workload(&mut replicas, &clock, WARMUP + 1, count, window)?;
    let last = measured
        .samples
        .iter()
        .map(|s| s.index)
        .max()
        .ok_or("missing samples")?;
    verify(&mut replicas, &clock, capacity as i64, last)?;
    close(replicas, &clock)?;
    eprintln!("phase=recovery-verification");
    let mut replicas = open(root, NativeOpenMode::Recover, protocol, capacity, &clock)?;
    campaign(&mut replicas, &clock)?;
    verify(&mut replicas, &clock, capacity as i64, last)?;
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
    let recovery_retries = retry(&mut replicas, &clock, 1, first_value)?
        + retry(&mut replicas, &clock, capacity, last_value)?;
    let retry_index = replicas[0].local().applications[&group()].applied_index();
    verify(&mut replicas, &clock, capacity as i64, retry_index)?;
    close(replicas, &clock)?;
    writeln!(
        csv,
        "operation,submitted_ns,completed_ns,latency_ns,applied_index,value"
    )?;
    for s in &measured.samples {
        writeln!(
            csv,
            "{},{},{},{},{},{}",
            s.operation,
            s.submitted_ns,
            s.completed_ns,
            s.completed_ns - s.submitted_ns,
            s.index,
            s.value
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
    let summary = format!("protocol={protocol:?} replicas=3 groups=1 heartbeat_ms=50 election_min_ms=1000 election_spread_ms=1000 payload_bytes=8 warmup={WARMUP} operations={count} window={window} max_inflight={} elapsed_s={:.6} applied_ops_s={:.3} p50_us={:.3} p95_us={:.3} p99_us={:.3} max_us={:.3} recovered_value={capacity} recovery_retries={recovery_retries} retry_verified=true workers_joined=true poll_rounds={} host_poll_ms={:.3} max_host_poll_ms={:.3} persistence_batches={} worker_events={} application_deliveries={}\n",
        measured.max_inflight, measured.elapsed.as_secs_f64(), count as f64 / measured.elapsed.as_secs_f64(), percentile(50), percentile(95), percentile(99), *latency.last().unwrap() as f64 / 1000., measured.polls.rounds, measured.polls.host_ns as f64 / 1e6, measured.polls.max_host_ns as f64 / 1e6, measured.polls.persistence_batches, measured.polls.worker_events, measured.polls.application_deliveries);
    let mut output = exclusive(&root.join("summary.txt"))?;
    output.write_all(summary.as_bytes())?;
    output.sync_all()?;
    print!("{summary}");
    Ok(())
}
