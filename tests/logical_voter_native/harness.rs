// SPDX-License-Identifier: RPL-1.5
// Copyright (c) 2026 Karl Livesey
//! Host-side fixture support: protocol time, bounded waits and port leases.
//!
//! A [`SimClock`] supplies protocol time that depends only on the fixture's
//! own idle polling rounds, never on CPU speed, core count, disk latency or
//! thread scheduling. Every liveness wait keeps its original bound in
//! protocol time; real time is only a generous hang guard for deadlocks.
use super::{aliases::Bridge, cluster::ChildNode, support::ports};
use std::{
    cell::Cell,
    hash::{DefaultHasher, Hash, Hasher},
    net::{SocketAddr, TcpListener, TcpStream, UdpSocket},
    rc::Rc,
    sync::{
        atomic::{AtomicBool, Ordering},
        Arc,
    },
    thread::JoinHandle,
    time::{Duration, Instant},
};
use voteboat::{
    application::{ApplicationReceipt, ReadableStateMachine, StateMachine},
    connect::PeerConnector,
    logical_voter::{LogicalBackingBridge, LogicalBackingChild},
    native::connect::NativePeerProtocol,
    outbound::OutboundQueue,
    runtime::{ElectionEntropy, MonoTime, NodeLocalParts, ReadyScheduler, TimerService},
    snapshot_worker::SnapshotWorker,
    worker::PersistenceWorker,
};

/// Real-time ceiling for one fixture wait. It only turns a deadlock into a
/// failure. Protocol time never derives from it, so a slow host (a Raspberry
/// Pi, or a loaded 3-core CI runner) cannot change any outcome.
pub const HANG: Duration = Duration::from_secs(600);
/// The original liveness bounds, now measured in protocol time.
pub const LIVENESS_MS: u64 = 15_000;
pub const HANDSHAKE_MS: u64 = 10_000;

/// A real-time ceiling alone, for waits that contain no protocol time, such
/// as joining worker threads after close. It warns once past a quarter.
pub struct HangGuard {
    began: Instant,
    warned: Cell<bool>,
}
impl HangGuard {
    pub fn new() -> Self {
        Self {
            began: Instant::now(),
            warned: Cell::new(false),
        }
    }
    pub fn check(&self, what: &str) {
        self.check_with(|| what.to_owned());
    }
    /// `what` is only formatted for a failure or a warning.
    fn check_with(&self, what: impl FnOnce() -> String) {
        let real = self.began.elapsed();
        if real >= HANG {
            panic!(
                "hang guard ({HANG:?} of real time), not a timing assertion: {}",
                what()
            );
        }
        if real >= HANG / 4 && !self.warned.replace(true) {
            eprintln!(
                "hang-guard warning: {real:?} of real time so far: {}",
                what()
            );
        }
    }
}
impl Default for HangGuard {
    fn default() -> Self {
        Self::new()
    }
}

/// One bounded wait: the original protocol-time liveness bound plus a
/// separate real-time hang guard.
pub struct Wait {
    start: MonoTime,
    limit_ms: u64,
    guard: HangGuard,
}
impl Wait {
    pub fn new(clock: &impl FixtureClock, limit_ms: u64) -> Self {
        Self {
            start: clock.now(),
            limit_ms,
            guard: HangGuard::new(),
        }
    }
    /// Whether the wait is still within its original protocol-time bound;
    /// callers assert this with their original message. Past the hang guard
    /// this panics instead: that is a deadlock, not a timing assertion.
    pub fn within(&self, clock: &impl FixtureClock) -> bool {
        self.guard.check_with(|| clock.describe());
        clock.now().0.saturating_sub(self.start.0) < self.limit_ms
    }
}

/// Protocol time supplied to nodes for one complete fixture poll round.
pub trait FixtureClock {
    fn now(&self) -> MonoTime;
    /// Time for the next round, given what the fixture observed after the
    /// previous one.
    fn round(&self, observed: Observed) -> MonoTime;
    /// Called when a fixture opens fresh nodes on this clock. Shared `&C`
    /// handles never reopen.
    fn reopened(&mut self) {}
    /// Diagnostics for a hang-guard failure.
    fn describe(&self) -> String {
        format!("protocol time {:?}", self.now())
    }
}
/// Wall-clock time, kept for fixtures outside the deterministic binaries.
impl FixtureClock for Instant {
    fn now(&self) -> MonoTime {
        MonoTime(self.elapsed().as_millis() as u64)
    }
    fn round(&self, _: Observed) -> MonoTime {
        self.now()
    }
    fn reopened(&mut self) {
        *self = Instant::now();
    }
}
impl<C: FixtureClock> FixtureClock for &C {
    fn now(&self) -> MonoTime {
        (**self).now()
    }
    fn round(&self, observed: Observed) -> MonoTime {
        (**self).round(observed)
    }
    fn describe(&self) -> String {
        (**self).describe()
    }
}

/// Deterministic protocol time shared by every node in one test.
///
/// It advances one millisecond only after an idle round: no durable log or
/// snapshot worker I/O in flight, no connection attempt waiting for its dial
/// worker, and no change in any observed replica or application state since
/// the previous round. A slow CPU, disk or scheduler therefore adds rounds but
/// not protocol time, and protocol deadlines (heartbeats, elections, connect
/// and handshake expiry) are reached only while the fixture is quiescent.
/// Every held signal completes without protocol time: worker and dial
/// threads finish on their own, and a state change is finite committed or
/// applied progress. Holding time therefore cannot stall progress.
#[derive(Clone, Default)]
pub struct SimClock(Rc<SimState>);
#[derive(Default)]
struct SimState {
    now: Cell<u64>,
    state: Cell<Option<u64>>,
    held: Cell<u64>,
    // Durable requests, dial attempts and state change of the last hold.
    reason: Cell<(usize, usize, bool)>,
}
impl FixtureClock for SimClock {
    fn now(&self) -> MonoTime {
        MonoTime(self.0.now.get())
    }
    fn round(&self, observed: Observed) -> MonoTime {
        let state = observed.state.finish();
        let changed = self.0.state.replace(Some(state)) != Some(state);
        if observed.durable == 0 && observed.dialing == 0 && !changed {
            self.0.now.set(self.0.now.get().checked_add(1).unwrap());
            self.0.held.set(0);
        } else {
            self.0.held.set(self.0.held.get() + 1);
            self.0
                .reason
                .set((observed.durable, observed.dialing, changed));
        }
        self.now()
    }
    fn describe(&self) -> String {
        let (durable, dialing, changed) = self.0.reason.get();
        format!(
            "protocol time {:?}, held for the last {} rounds (last hold: \
             durable={durable} dialing={dialing} state_changed={changed})",
            self.now(),
            self.0.held.get(),
        )
    }
}

/// Time-independent work and replicated state seen by one fixture round.
#[derive(Default)]
pub struct Observed {
    durable: usize,
    dialing: usize,
    state: DefaultHasher,
}
impl Observed {
    pub fn nodes(nodes: &[ChildNode]) -> Self {
        nodes.iter().fold(Self::default(), Self::node)
    }
    /// Durable worker I/O, dial work and replica state of an owned node.
    pub fn node(mut self, node: &ChildNode) -> Self {
        self.durable += durable(node.local());
        self.dialing += node.peers().map_or(0, |p| p.connector_usage().dialing);
        replicas(node.local(), &mut self.state);
        self
    }
    /// The same for every native node borrowed by a bridge. A bridge exposes
    /// no connector, so dial work of borrowed nodes is not visible here.
    pub fn bridge(mut self, bridge: &Bridge<'_>) -> Self {
        for i in 0.. {
            let Ok(work) = bridge.inspect_backing(i, |local| {
                replicas(local, &mut self.state);
                durable(local)
            }) else {
                break;
            };
            self.durable += work;
        }
        self
    }
    /// Replica and application state of every node borrowed by any bridge,
    /// without holding on their persistence. A logical node's persistence
    /// completes only through its own backing group's consensus, which can
    /// itself need protocol time (an election or a retransmission), so
    /// holding on it could stall the clock that completes it. Its effect is
    /// still visible here as replica progress.
    pub fn backing<N: LogicalBackingChild>(mut self, bridge: &LogicalBackingBridge<'_, N>) -> Self {
        for i in 0.. {
            let Ok(()) = bridge.inspect_backing(i, |local| replicas(local, &mut self.state)) else {
                break;
            };
        }
        self
    }
    /// Connection attempts waiting for a dial worker outside any node or
    /// connector seen here, such as a closed peer driver's.
    pub fn dialing(mut self, attempts: usize) -> Self {
        self.dialing += attempts;
        self
    }
    /// A connector's attempts waiting for its dial worker, and its progress.
    pub fn connector(mut self, connector: &impl PeerConnector) -> Self {
        let usage = connector.usage();
        self.dialing += usage.dialing;
        (usage.requests, usage.anonymous, usage.handshaking).hash(&mut self.state);
        self
    }
    /// Replica and application state of any other polled node.
    pub fn local<S, T, E, A, W, O, H>(mut self, local: &NodeLocalParts<S, T, E, A, W, O, H>) -> Self
    where
        S: ReadyScheduler,
        T: TimerService,
        E: ElectionEntropy,
        A: StateMachine + ReadableStateMachine,
        W: PersistenceWorker,
        O: OutboundQueue,
        H: SnapshotWorker,
        A::Receipt: ApplicationReceipt,
    {
        replicas(local, &mut self.state);
        self
    }
    /// Any other fixture progress, such as established session counts.
    pub fn progress(mut self, value: impl Hash) -> Self {
        value.hash(&mut self.state);
        self
    }
}
fn durable<S, T, E, A, W, O, H>(local: &NodeLocalParts<S, T, E, A, W, O, H>) -> usize
where
    S: ReadyScheduler,
    T: TimerService,
    E: ElectionEntropy,
    A: StateMachine + ReadableStateMachine,
    W: PersistenceWorker,
    O: OutboundQueue,
    H: SnapshotWorker,
    A::Receipt: ApplicationReceipt,
{
    local.persistence.usage().requests
        + local
            .snapshots
            .as_ref()
            .map_or(0, |s| s.worker.usage().requests)
}
fn replicas<S, T, E, A, W, O, H>(local: &NodeLocalParts<S, T, E, A, W, O, H>, h: &mut DefaultHasher)
where
    S: ReadyScheduler,
    T: TimerService,
    E: ElectionEntropy,
    A: StateMachine + ReadableStateMachine,
    W: PersistenceWorker,
    O: OutboundQueue,
    H: SnapshotWorker,
    A::Receipt: ApplicationReceipt,
{
    for (group, application) in &local.applications {
        let log = local.owner.core(*group).map(|core| {
            let log = core.state();
            let ballot = log.hard_state.voted_for.map(|n| n.get());
            (
                log.hard_state.term,
                ballot,
                log.commit_index,
                log.last_index(),
            )
        });
        (group, log, application.applied_index()).hash(h);
    }
}

/// One loopback port owned by a test for its whole lifetime.
///
/// The port, its cross-process lock and the in-process registry entry come
/// from the shared [`ports::PortLease`] (TR1b), so every fixture draws from one
/// pool: outside the kernel's automatic (ephemeral) range and outside every
/// legacy fixed-port ladder (counter, routed, directory and transfer
/// services). This type only splits that lease's bound TCP/UDP pair by
/// protocol. The half the node or connector does not use stays bound here for
/// the whole lease, so no TCP+UDP pair probe can claim the port. The selected
/// half is held until the instant its owner binds or adopts it, and again from
/// the moment [`PortLease::reclaim`] proves its release until the lease ends.
/// While a TCP owner is absent the held listener closes each accepted
/// connection at once, so peers dialling an absent node fail promptly instead
/// of hanging until their deadline.
pub struct PortLease {
    address: SocketAddr,
    protocol: NativePeerProtocol,
    selected: Option<Held>,
    // The unused protocol half, bound for the lease lifetime.
    _other: Held,
    // Declared last: fields drop in order, so both halves close before the
    // lock and registry entry are released.
    _shared: ports::PortLease,
}
enum Held {
    Tcp(Refusing),
    // Read only by QUIC adoption; otherwise held for its binding alone.
    Udp { _socket: UdpSocket },
}
impl PortLease {
    pub fn acquire(protocol: NativePeerProtocol) -> Self {
        let mut shared = ports::PortLease::acquire(1);
        let [address] = shared.addresses();
        let [(tcp, udp)]: [(TcpListener, UdpSocket); 1] = shared.take_sockets().try_into().unwrap();
        let (selected, other) = match protocol {
            NativePeerProtocol::TcpTls => (Held::tcp(tcp), Held::Udp { _socket: udp }),
            #[cfg(feature = "quic")]
            NativePeerProtocol::Quic => (Held::Udp { _socket: udp }, Held::tcp(tcp)),
        };
        Self {
            address,
            protocol,
            selected: Some(selected),
            _other: other,
            _shared: shared,
        }
    }
    pub fn address(&self) -> SocketAddr {
        self.address
    }
    /// Release the node's own half immediately before the node binds it.
    pub fn handoff(&mut self) {
        drop(self.selected.take().expect("held before handoff"));
    }
    /// Move the selected TCP listener into a connector that adopts it. The
    /// unused UDP half and the lock stay here; [`PortLease::reclaim`] proves
    /// the release after the connector closes.
    pub fn adopt_tcp(&mut self) -> TcpListener {
        match self.selected.take() {
            Some(Held::Tcp(tcp)) => tcp.stop(),
            _ => panic!("a held TCP half before adoption"),
        }
    }
    /// Move the selected UDP socket into a connector that adopts it. The
    /// unused TCP half and the lock stay here; [`PortLease::reclaim`] proves
    /// the release after the connector closes.
    #[cfg(feature = "quic")]
    pub fn adopt_udp(&mut self) -> UdpSocket {
        match self.selected.take() {
            Some(Held::Udp { _socket: udp }) => udp,
            _ => panic!("a held UDP half before adoption"),
        }
    }
    /// Bind the selected half again: this proves its owner released it, and
    /// holds it for a later reopen. Only the selected half ever leaves the
    /// lease, so this is the only rebind.
    pub fn reclaim(&mut self) {
        assert!(self.selected.is_none(), "selected half already held");
        let address = self.address;
        self.selected = Some(match self.protocol {
            NativePeerProtocol::TcpTls => Held::tcp(released(address, TcpListener::bind(address))),
            #[cfg(feature = "quic")]
            NativePeerProtocol::Quic => Held::Udp {
                _socket: released(address, UdpSocket::bind(address)),
            },
        });
    }
}
fn released<T>(address: SocketAddr, bound: std::io::Result<T>) -> T {
    bound.unwrap_or_else(|error| panic!("{address} not released by its owner: {error}"))
}
impl Held {
    fn tcp(listener: TcpListener) -> Self {
        Self::Tcp(Refusing::start(listener))
    }
}
/// A held listener whose thread closes each accepted connection at once.
struct Refusing {
    listener: Option<Arc<TcpListener>>,
    stop: Arc<AtomicBool>,
    thread: Option<JoinHandle<()>>,
}
impl Refusing {
    fn start(listener: TcpListener) -> Self {
        let listener = Arc::new(listener);
        let stop = Arc::new(AtomicBool::new(false));
        let thread = std::thread::Builder::new()
            .name("voteboat-test-port".into())
            .spawn({
                let (listener, stop) = (listener.clone(), stop.clone());
                move || {
                    while !stop.load(Ordering::Acquire) {
                        // Dropping the stream closes it before any handshake.
                        drop(listener.accept());
                    }
                }
            })
            .unwrap();
        Self {
            listener: Some(listener),
            stop,
            thread: Some(thread),
        }
    }
    fn halt(&mut self) {
        let (Some(thread), Some(listener)) = (self.thread.take(), &self.listener) else {
            return;
        };
        self.stop.store(true, Ordering::Release);
        // Wake the blocking accept; the thread then sees the stop flag.
        drop(TcpStream::connect(listener.local_addr().unwrap()));
        thread.join().unwrap();
    }
    fn stop(mut self) -> TcpListener {
        self.halt();
        Arc::try_unwrap(self.listener.take().unwrap()).unwrap()
    }
}
impl Drop for Refusing {
    fn drop(&mut self) {
        self.halt();
    }
}
