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
//! NT1: connect generations are scoped to the store session and full-width.
//! Each process used to own `session * 10000 ..= +9999`; one peer down at the
//! 10 s retry cap exhausted that in ~27.8 h and sent the node to recovery.
use super::*;
use voteboat::{
    native::{
        connect::NativePeerProtocol,
        log_store::{FileLogIo, JournalIo, NativeLogStore},
    },
    raft::Role,
    secure::SessionBinding,
    transport::{ConnectTicket, PeerPoll},
};

/// The old per-process range held 10,000 generations.
const OLD_RANGE: u64 = 10_000;
/// Real-time ceiling for a wait that contains no protocol time (joining
/// worker threads). It only turns a deadlock into a failure; no outcome or
/// assertion depends on how fast the host is.
const HANG: Duration = Duration::from_secs(600);
/// Liveness bound of the two-node reconnect, in protocol time.
const LIVENESS_MS: u64 = 60_000;
const LOCAL: u64 = 2;
const PEER: u64 = 1;

fn pair_bootstrap() -> Bootstrap {
    let stores = (1..=2)
        .map(|n| {
            (
                NodeId::new(n).unwrap(),
                StoreIdentity {
                    id: StoreId::new(n as u128).unwrap(),
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

fn pair_config(
    directory: &std::path::Path,
    addresses: [std::net::SocketAddr; 2],
    n: u64,
    mode: NativeOpenMode,
) -> NativeStartup {
    let certificate = |n| match n {
        1 => include_bytes!("../fixtures/tls/node1.der").as_slice(),
        _ => include_bytes!("../fixtures/tls/node2.der").as_slice(),
    };
    let key = |n| match n {
        1 => include_bytes!("../fixtures/tls/node1-key.der").as_slice(),
        _ => include_bytes!("../fixtures/tls/node2-key.der").as_slice(),
    };
    let other = 3 - n;
    let mut config = config(directory.join(n.to_string()), mode);
    config.node = NodeId::new(n).unwrap();
    config.bootstrap = pair_bootstrap();
    config.store = config.bootstrap.voter_stores[&config.node];
    config.listen = addresses[n as usize - 1];
    config.peers = [(
        NodeId::new(other).unwrap(),
        NativeStartupPeer {
            address: addresses[other as usize - 1],
            certificate: certificate(other).to_vec(),
            server_name: format!("node{other}.voteboat.test"),
        },
    )]
    .into();
    config.entropy_seed += n;
    config.tls = NativeTlsConfig::new(TlsCredentials {
        roots: vec![include_bytes!("../fixtures/tls/ca.der").to_vec()],
        certificate_chain: vec![certificate(n).to_vec()],
        private_key: key(n).to_vec(),
    })
    .unwrap();
    config
}

/// No election fires on its own during the simulated soak (35,000 years), so
/// the soak measures only connection attempts; elections are explicit.
fn quiet_timers() -> TimerConfig {
    TimerConfig {
        heartbeat_ms: 50,
        election_min_ms: 1 << 50,
        election_spread_ms: 1,
        expirations_per_poll: 32,
        append_hold_ms: TimerConfig::DEFAULT_APPEND_HOLD_MS,
    }
}

fn open_pair_node(
    directory: &std::path::Path,
    addresses: [std::net::SocketAddr; 2],
    n: u64,
    mode: NativeOpenMode,
    now: MonoTime,
) -> WireNode {
    pair_config(directory, addresses, n, mode)
        .open_with_protocol_and_timers(
            NativePeerProtocol::TcpTls,
            quiet_timers(),
            app(),
            Arc::new(ThreadWake::current()),
            now,
        )
        .unwrap_or_else(|r| panic!("open node {n}: {:?}", r.reason))
}

/// Simulated-time soak driver: `LOCAL` accepts from the stopped `PEER`
/// (higher node IDs accept), so every attempt expires after the 10 s connect
/// timeout and the next is due after the 10 s retry cap. No socket traffic is
/// needed, so the run is deterministic and wall-clock free.
struct Soak {
    now: u64,
    attempts: Vec<ConnectTicket>,
}

impl Soak {
    fn new(now: u64) -> Self {
        Self {
            now,
            attempts: Vec::new(),
        }
    }
    /// Poll at simulated `now` and record every expired attempt. The node
    /// must stay in service after every poll.
    fn poll(&mut self, node: &mut WireNode) {
        let progress = node
            .poll(MonoTime(self.now), NodePollBudget::default())
            .unwrap_or_else(|e| {
                panic!(
                    "node left service after {} attempts (last generation {:?}): {e:?}",
                    self.attempts.len(),
                    self.attempts.last().map(|t| t.generation.get())
                )
            });
        assert_eq!(node.state(), NodeState::Running);
        for event in progress.peers.into_iter().flat_map(|p| p.peers) {
            if let PeerPoll::ConnectExpired(ticket) = event {
                self.attempts.push(ticket);
            }
        }
    }
    /// Step simulated time in 10 s increments until `count` attempts expired.
    /// Each attempt takes two steps (timeout, then retry cap), so a node that
    /// silently stopped issuing fails after `3 * count` polls instead of
    /// looping forever.
    fn run_until(&mut self, node: &mut WireNode, count: usize) {
        let limit = 3 * count.saturating_sub(self.attempts.len());
        let mut polls = 0;
        while self.attempts.len() < count {
            polls += 1;
            assert!(
                polls <= limit,
                "no new connect attempt after {} expired: issuing stopped",
                self.attempts.len()
            );
            self.now += 10_000;
            self.poll(node);
        }
    }
}

fn assert_strictly_increasing(attempts: &[ConnectTicket]) {
    for pair in attempts.windows(2) {
        assert_eq!(pair[0].local, pair[1].local, "one store session");
        assert!(pair[0].generation < pair[1].generation, "{pair:?}");
    }
}

fn close_node(n: WireNode, from: u64) {
    let mut n = n;
    n.begin_shutdown();
    let mut tick = from;
    while !n.is_drained() {
        tick += 1;
        n.poll(MonoTime(tick), NodePollBudget::default()).unwrap();
        assert!(tick < from + 20_000, "node did not drain");
        std::thread::park_timeout(Duration::from_millis(1));
    }
    let mut p = n.into_parts().unwrap_or_else(|_| panic!("not drained"));
    let mut d = p
        .peers
        .take()
        .unwrap()
        .connector
        .into_dialer()
        .unwrap_or_else(|_| panic!("connector"));
    let mut s = p.local.snapshots.take().unwrap();
    let guard = Instant::now();
    let (mut log, mut snap) = (false, false);
    loop {
        let dial = d.as_mut().is_none_or(|d| d.try_finish().unwrap());
        if !log {
            log = p.local.persistence.try_reclaim().unwrap().is_some();
        }
        if !snap {
            snap = s.worker.try_reclaim().unwrap().is_some();
        }
        if dial && log && snap {
            break;
        }
        assert!(
            guard.elapsed() < HANG,
            "hang guard ({HANG:?} of real time), not a timing assertion: workers did not join"
        );
        std::thread::park_timeout(Duration::from_millis(1));
    }
}

/// Fails on the base (3d552e2e): after 10,000 attempts the roster returned
/// `Exhausted`, the driver failed and the node entered `RecoveryRequired`.
#[test]
fn native_node_survives_more_connect_attempts_than_the_old_per_process_range() {
    let directory = root();
    std::fs::create_dir(&directory).unwrap();
    // No socket traffic: the peer is down and the local node only accepts.
    let (_lease, addresses) = leased_pair();
    let mut node = open_pair_node(
        &directory,
        addresses,
        LOCAL,
        NativeOpenMode::Create,
        MonoTime(0),
    );
    let mut soak = Soak::new(0);
    soak.poll(&mut node);
    let target = OLD_RANGE as usize + 2_000;
    soak.run_until(&mut node, target);
    assert_strictly_increasing(&soak.attempts);
    let last = soak.attempts.last().unwrap().generation.get();
    assert!(last >= target as u64, "{last}");
    // The cadence is the retry cap: one attempt per 20 simulated seconds,
    // so 12,000 attempts span about 2.8 simulated days.
    assert!(soak.now >= (target as u64 - 20) * 20_000, "{}", soak.now);
    close_node(node, soak.now);
    std::fs::remove_dir_all(directory).unwrap();
}

/// A recovery that dies before its session write: the manifest publication
/// fails and nothing is written, as after a crash at that point.
struct CrashBeforePublish(FileLogIo);
impl JournalIo for CrashBeforePublish {
    fn read_manifest(&mut self) -> std::io::Result<Vec<u8>> {
        self.0.read_manifest()
    }
    fn read_log(&mut self, limit: usize) -> std::io::Result<Vec<u8>> {
        self.0.read_log(limit)
    }
    fn append(&mut self, bytes: &[u8]) -> std::io::Result<()> {
        self.0.append(bytes)
    }
    fn sync_log(&mut self) -> std::io::Result<()> {
        self.0.sync_log()
    }
    fn truncate_log(&mut self, length: u64) -> std::io::Result<()> {
        self.0.truncate_log(length)
    }
    fn publish_manifest(&mut self, _: &[u8]) -> std::io::Result<()> {
        Err(std::io::Error::other("crash before the session write"))
    }
    fn publish_barrier(&mut self, _: &[u8]) -> std::io::Result<()> {
        Err(std::io::Error::other("crash before the session write"))
    }
}

/// One process lifetime of the accepting node: open, replay every earlier
/// ticket against the live roster while its first attempt is open, expire
/// 50 attempts, close. Returns this lifetime's expired tickets.
fn lifetime(
    directory: &std::path::Path,
    addresses: [std::net::SocketAddr; 2],
    mode: NativeOpenMode,
    now: &mut u64,
    earlier: &[ConnectTicket],
) -> Vec<ConnectTicket> {
    let mut node = open_pair_node(directory, addresses, LOCAL, mode, MonoTime(*now));
    let mut soak = Soak::new(*now);
    soak.poll(&mut node);
    let live = node
        .peer_drain_status(NodeId::new(PEER).unwrap())
        .and_then(|s| s.attempt)
        .expect("the first accept attempt is open");
    assert_eq!(live.generation.get(), 1, "each store session starts at 1");
    let roster = node.peers().unwrap().roster();
    assert!(roster.attempt_deadline(live).is_some());
    if let Some(first) = earlier.first() {
        // Equal numeric generation (every earlier session issued 1) and a
        // higher one from a long-lived earlier process.
        let higher = ConnectTicket {
            generation: SecureSessionGeneration::new(50_000).unwrap(),
            ..*first
        };
        assert!(earlier.iter().any(|t| t.generation == live.generation));
        for old in earlier.iter().copied().chain([higher]) {
            assert!(old.local.store.session < live.local.store.session);
            assert_eq!(roster.attempt_deadline(old), None, "{old:?}");
        }
    }
    soak.run_until(&mut node, 50);
    assert_strictly_increasing(&soak.attempts);
    assert_eq!(soak.attempts[0], live);
    assert_eq!(
        live.local.store.session,
        node.local().owner.identity().store.session
    );
    *now = soak.now + 1;
    close_node(node, *now);
    *now += 20_001;
    soak.attempts
}

/// Generations restart in each store session; the store session is persisted
/// and strictly increases on every recovery that exposes a store, so no
/// `(session, generation)` pair, ticket or binding of an earlier process can
/// be reused. Earlier tickets with an equal or a higher numeric generation
/// are replayed against each recovered roster (see also the roster,
/// connector and relay replay tests). Two interrupted recoveries run between
/// the second and third lifetimes: one dies after its session write, one
/// before it.
#[test]
fn connect_generations_are_unique_across_store_session_restarts() {
    let directory = root();
    std::fs::create_dir(&directory).unwrap();
    let (_lease, addresses) = leased_pair();
    let store_directory = directory.join(LOCAL.to_string());
    let store = pair_bootstrap().voter_stores[&NodeId::new(LOCAL).unwrap()];
    let mut now = 0;
    let mut runs: Vec<Vec<ConnectTicket>> = Vec::new();
    for mode in [NativeOpenMode::Create, NativeOpenMode::Recover] {
        let earlier = runs.concat();
        runs.push(lifetime(&directory, addresses, mode, &mut now, &earlier));
    }
    // Crash after the session write: recovery publishes a new session, then
    // the process dies before any connection work.
    let after = NativeLogStore::recover(
        FileLogIo::open(&store_directory).unwrap(),
        store,
        LogLimits::default(),
    )
    .unwrap()
    .binding()
    .session;
    assert!(after > runs[1][0].local.store.session);
    // Crash before the session write: no store is exposed and nothing is
    // published.
    let before = NativeLogStore::recover(
        CrashBeforePublish(FileLogIo::open(&store_directory).unwrap()),
        store,
        LogLimits::default(),
    );
    assert!(before.is_err(), "a failed publication exposes no store");
    let earlier = runs.concat();
    runs.push(lifetime(
        &directory,
        addresses,
        NativeOpenMode::Recover,
        &mut now,
        &earlier,
    ));
    let third = runs[2][0].local.store.session;
    // The crash after the write is skipped; the crash before it proposed the
    // same next number, which is reused here because nothing exposed it.
    assert_eq!(third.get(), after.get() + 1, "{after:?} {third:?}");
    for pair in runs.windows(2) {
        let (old, new) = (
            pair[0][0].local.store.session,
            pair[1][0].local.store.session,
        );
        assert!(
            old < new,
            "store session must strictly increase: {old:?} {new:?}"
        );
    }
    let mut seen = std::collections::BTreeSet::new();
    for ticket in runs.iter().flatten() {
        assert!(
            seen.insert((ticket.local.store.session, ticket.generation)),
            "reused {ticket:?}"
        );
    }
    // The same numeric generation recurs across sessions, but no ticket does.
    assert_eq!(runs[0][0].generation, runs[1][0].generation);
    for old in &runs[0] {
        assert!(runs[1].iter().chain(&runs[2]).all(|new| new != old));
    }
    std::fs::remove_dir_all(directory).unwrap();
}

#[path = "../support/ports.rs"]
#[allow(dead_code)]
mod ports;

/// Two TR1b leases; both probes are released before a node binds, and the
/// locks stay held until the lease is dropped.
fn leased_pair() -> (ports::PortLease, [std::net::SocketAddr; 2]) {
    let mut lease = ports::PortLease::acquire(2);
    let addresses = lease.addresses::<2>();
    lease.handoff();
    (lease, addresses)
}

fn binding(node: &WireNode, peer: u64) -> Option<SessionBinding> {
    node.peers()
        .unwrap()
        .roster()
        .binding(NodeId::new(peer).unwrap())
}

/// Two native nodes over leased ports (TR1b): the accepting node burns more
/// generations than the old per-process range while its peer is stopped, then
/// the peer starts and a real TCP/TLS session is established with a
/// generation past the old boundary, followed by an election and a write.
#[test]
fn native_pair_reconnects_with_a_generation_past_the_old_range() {
    let directory = root();
    std::fs::create_dir(&directory).unwrap();
    let (lease, addresses) = leased_pair();
    let mut accepting = open_pair_node(
        &directory,
        addresses,
        LOCAL,
        NativeOpenMode::Create,
        MonoTime(0),
    );
    let mut soak = Soak::new(0);
    soak.poll(&mut accepting);
    soak.run_until(&mut accepting, OLD_RANGE as usize + 1);
    assert_strictly_increasing(&soak.attempts);
    assert!(soak.attempts.last().unwrap().generation.get() > OLD_RANGE);
    // One more step issues the next accept attempt, open for 10 s.
    soak.now += 10_000;
    soak.poll(&mut accepting);
    // The stopped peer starts. Both nodes advance 1 ms of protocol time per
    // round from their own origins, independent of host speed; the real
    // sockets and dial worker make progress between rounds.
    let start = soak.now;
    let mut dialing = open_pair_node(
        &directory,
        addresses,
        PEER,
        NativeOpenMode::Create,
        MonoTime(0),
    );
    let guard = Instant::now();
    let mut tick = 0;
    let mut drive = |accepting: &mut WireNode,
                     dialing: &mut WireNode,
                     done: &dyn Fn(&WireNode, &WireNode) -> bool| {
        let began = tick;
        loop {
            tick += 1;
            accepting
                .poll(MonoTime(start + tick), NodePollBudget::default())
                .unwrap();
            dialing
                .poll(MonoTime(tick), NodePollBudget::default())
                .unwrap();
            if done(accepting, dialing) {
                return tick;
            }
            assert!(
                tick - began < LIVENESS_MS,
                "pair did not converge in {LIVENESS_MS} ms of protocol time"
            );
            assert!(
                guard.elapsed() < HANG,
                "hang guard ({HANG:?} of real time), not a timing assertion"
            );
            std::thread::park_timeout(Duration::from_millis(1));
        }
    };
    drive(&mut accepting, &mut dialing, &|a, d| {
        binding(a, PEER).is_some() && binding(d, LOCAL).is_some()
    });
    let accepted = binding(&accepting, PEER).unwrap();
    assert!(
        accepted.generation.get() > OLD_RANGE,
        "reconnect generation {}",
        accepted.generation.get()
    );
    assert_eq!(
        accepted.local.store.session,
        soak.attempts[0].local.store.session
    );
    // Service: elect the accepting node and commit through the reconnected pair.
    accepting.control(group(), NodeControl::Campaign).unwrap();
    let elapsed = drive(&mut accepting, &mut dialing, &|a, d| {
        a.local().owner.core(group()).unwrap().role() == Role::Leader
            && [a, d]
                .iter()
                .all(|n| n.local().applications[&group()].applied_index() > 0)
    });
    close_node(dialing, elapsed + 1);
    close_node(accepting, start + elapsed + 1);
    drop(lease);
    std::fs::remove_dir_all(directory).unwrap();
}
