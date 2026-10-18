// SPDX-License-Identifier: RPL-1.5
// Copyright (c) 2026 Karl Livesey
//! Host-side fixture support: protocol time, bounded waits and port leases.
//!
//! A [`SimClock`] supplies protocol time that depends only on the fixture's
//! own idle polling rounds, never on CPU speed, core count, disk latency or
//! thread scheduling. Every liveness wait keeps its original bound in
//! protocol time; real time is only a generous hang guard for deadlocks.
use super::{aliases::Bridge, cluster::ChildNode};
use std::{
    cell::Cell,
    fs::{File, OpenOptions},
    hash::{DefaultHasher, Hash, Hasher},
    io,
    net::{Ipv4Addr, SocketAddr, TcpListener, TcpStream, UdpSocket},
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
/// Leases come from outside the kernel's automatic (ephemeral) port range,
/// so no outgoing connection or `:0` bind can take one, and outside
/// 1024..32768, where other suites allocate fixed port blocks. Each is
/// claimed with a cross-process file lock in one namespace shared by every
/// user and `TMPDIR`. The protocol the node does not use stays bound by the
/// lease for its whole lifetime, so no TCP+UDP pair probe can claim the port.
/// The node's own half is held until the instant the node binds it, and again
/// from the moment its release is proven until the lease ends. While a TCP
/// node is absent its held listener closes each accepted connection at once,
/// so peers dialling an absent node fail promptly instead of hanging until
/// their deadline.
///
/// The lock namespace and port pool follow the shared `support::ports` lease
/// (PR 4), so this can be swapped for it once that lands.
pub struct PortLease {
    address: SocketAddr,
    protocol: NativePeerProtocol,
    selected: Option<Held>,
    // The unused protocol half, bound for the lease lifetime.
    other: Option<Held>,
    _lock: File,
}
enum Held {
    Tcp(Refusing),
    Udp(UdpSocket),
}
impl PortLease {
    pub fn acquire(protocol: NativePeerProtocol) -> Self {
        let directory = lease_directory();
        let ephemeral = ephemeral_range();
        for port in 32768..=u16::MAX {
            if ephemeral.contains(&port) {
                continue;
            }
            let Some(lock) = lock(&directory, port) else {
                continue;
            };
            let (tcp, udp) = match bind_pair(port) {
                Ok(pair) => pair,
                Err(error) if error.kind() == io::ErrorKind::AddrInUse => continue,
                Err(error) => panic!("port lease {port}: {error}"),
            };
            let address = tcp.local_addr().unwrap();
            let (selected, other) = match protocol {
                NativePeerProtocol::TcpTls => (Held::tcp(tcp), Held::Udp(udp)),
                #[cfg(feature = "quic")]
                NativePeerProtocol::Quic => (Held::Udp(udp), Held::tcp(tcp)),
            };
            return Self {
                address,
                protocol,
                selected: Some(selected),
                other: Some(other),
                _lock: lock,
            };
        }
        panic!("no TCP/UDP test port lease outside 1024..32768 and {ephemeral:?}");
    }
    pub fn address(&self) -> SocketAddr {
        self.address
    }
    /// Release the node's own half immediately before the node binds it.
    pub fn handoff(&mut self) {
        drop(self.selected.take().expect("held before handoff"));
    }
    /// Bind the node's half again: this proves the node released it, and
    /// holds it for a later reopen.
    pub fn reclaim(&mut self) {
        assert!(self.selected.is_none());
        self.selected = Some(match self.protocol {
            NativePeerProtocol::TcpTls => Held::tcp(TcpListener::bind(self.address).unwrap()),
            #[cfg(feature = "quic")]
            NativePeerProtocol::Quic => Held::Udp(UdpSocket::bind(self.address).unwrap()),
        });
    }
    /// Move both bound sockets into a connector that adopts them; the lock
    /// stays here until [`PortLease::reclaim_both`].
    pub fn take_sockets(&mut self) -> (TcpListener, UdpSocket) {
        match (self.selected.take(), self.other.take()) {
            (Some(Held::Tcp(tcp)), Some(Held::Udp(udp)))
            | (Some(Held::Udp(udp)), Some(Held::Tcp(tcp))) => (tcp.stop(), udp),
            _ => panic!("both halves held before adoption"),
        }
    }
    /// After an adopting connector closed: prove both halves were released.
    pub fn reclaim_both(&mut self) {
        assert!(self.selected.is_none() && self.other.is_none());
        let (tcp, udp) = bind_pair(self.address.port()).unwrap();
        self.selected = Some(Held::Udp(udp));
        self.other = Some(Held::tcp(tcp));
    }
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
/// One namespace for every process that shares the loopback port space,
/// independent of each user's `TMPDIR`; sticky and world-writable like `/tmp`.
fn lease_directory() -> std::path::PathBuf {
    use std::os::unix::fs::PermissionsExt;
    let directory = std::env::var_os("VOTEBOAT_TEST_PORT_LEASE_DIR").map_or_else(
        || "/tmp/voteboat-test-ports".into(),
        std::path::PathBuf::from,
    );
    std::fs::create_dir_all(&directory).unwrap();
    let mode = std::fs::metadata(&directory).unwrap().permissions().mode();
    if mode & 0o1777 != 0o1777 {
        // Only the owner can repair the mode; another user's directory that
        // is already shared needs no change.
        drop(std::fs::set_permissions(
            &directory,
            std::fs::Permissions::from_mode(0o1777),
        ));
    }
    directory
}
/// Lock files persist so every process locks the same inode. Existing files
/// are opened without `O_CREAT` (another user's file in a sticky directory
/// refuses it), and read-only when they are not writable for this user: an
/// advisory lock needs no write access.
fn lock(directory: &std::path::Path, port: u16) -> Option<File> {
    use std::os::unix::fs::{OpenOptionsExt, PermissionsExt};
    let path = directory.join(format!("{port}.lock"));
    let file = loop {
        match OpenOptions::new().read(true).write(true).open(&path) {
            Ok(file) => break file,
            Err(e) if e.kind() == io::ErrorKind::PermissionDenied => {
                break File::open(&path).ok()?
            }
            Err(e) if e.kind() == io::ErrorKind::NotFound => {}
            Err(e) => panic!("port lease {port}: {e}"),
        }
        match OpenOptions::new()
            .read(true)
            .write(true)
            .create_new(true)
            .mode(0o666)
            .open(&path)
        {
            Ok(file) => {
                // Independent of this process's umask.
                file.set_permissions(std::fs::Permissions::from_mode(0o666))
                    .unwrap();
                break file;
            }
            Err(e) if e.kind() == io::ErrorKind::AlreadyExists => continue,
            Err(e) => panic!("port lease {port}: {e}"),
        }
    };
    match file.try_lock() {
        Ok(()) => Some(file),
        Err(std::fs::TryLockError::WouldBlock) => None,
        Err(error) => panic!("port lease {port}: {error}"),
    }
}
fn bind_pair(port: u16) -> io::Result<(TcpListener, UdpSocket)> {
    let tcp = TcpListener::bind((Ipv4Addr::LOCALHOST, port))?;
    let udp = UdpSocket::bind(tcp.local_addr()?)?;
    Ok((tcp, udp))
}
fn ephemeral_range() -> std::ops::RangeInclusive<u16> {
    #[cfg(target_os = "linux")]
    {
        let text = std::fs::read_to_string("/proc/sys/net/ipv4/ip_local_port_range").unwrap();
        let mut ports = text.split_whitespace().map(|p| p.parse::<u16>().unwrap());
        ports.next().unwrap()..=ports.next().unwrap()
    }
    #[cfg(not(target_os = "linux"))]
    {
        49152..=65535
    }
}
