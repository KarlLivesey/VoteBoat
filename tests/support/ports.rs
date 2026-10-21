// SPDX-License-Identifier: RPL-1.5
// Copyright (c) 2026 Karl Livesey
//! Addresses must be known before native startup. Keep a cross-process lease
//! through startup, reopen and release assertions, not merely a probe/drop pair.
use std::{
    collections::BTreeSet,
    fs::{File, OpenOptions},
    io,
    net::{Ipv4Addr, SocketAddr, TcpListener, UdpSocket},
    os::unix::fs::{DirBuilderExt, MetadataExt, OpenOptionsExt, PermissionsExt},
    path::{Path, PathBuf},
    sync::{Mutex, PoisonError},
};

static PORTS: Mutex<BTreeSet<u16>> = Mutex::new(BTreeSet::new());

pub(crate) struct Reservation {
    pub(crate) address: SocketAddr,
    // Drop closes both probe halves, removes the registry entry, then drops
    // the file lock.
    pub(crate) tcp: Option<TcpListener>,
    pub(crate) udp: Option<UdpSocket>,
    pub(crate) _lock: File,
}

impl Drop for Reservation {
    fn drop(&mut self) {
        self.tcp.take();
        self.udp.take();
        PORTS
            .lock()
            .unwrap_or_else(PoisonError::into_inner)
            .remove(&self.address.port());
    }
}

pub struct PortLease {
    pub(crate) held: Vec<Reservation>,
    pub(crate) directory: PathBuf,
}

impl PortLease {
    pub fn acquire(count: usize) -> Self {
        Self::with_directory(|directory| Self::acquire_in(count, directory))
    }

    /// Reserve a service's fixed listener layout with the same locks and safe
    /// pool as individual ports. Every addition is checked before any bind.
    pub fn acquire_offsets(offsets: &[u16]) -> (u16, Self) {
        assert!(!offsets.is_empty());
        assert_eq!(offsets.iter().collect::<BTreeSet<_>>().len(), offsets.len());
        let lease = Self::with_directory(|directory| Self::acquire_offsets_in(offsets, directory));
        let base = lease.held[0].address.port() - offsets[0];
        (base, lease)
    }

    fn with_directory(acquire: impl Fn(&Path) -> io::Result<Self>) -> Self {
        let directory = shared_directory();
        let result = prepare_directory(&directory).and_then(|()| acquire(&directory));
        match result {
            Ok(lease) => lease,
            Err(error) if error.kind() == io::ErrorKind::AddrNotAvailable => panic!("{error}"),
            Err(error) => {
                let fallback = fallback_directory(&directory, &error);
                prepare_directory(&fallback).unwrap();
                acquire(&fallback).unwrap()
            }
        }
    }

    fn acquire_offsets_in(offsets: &[u16], directory: &Path) -> io::Result<Self> {
        let ephemeral = ephemeral_range();
        let permitted = candidates(&ephemeral).collect::<BTreeSet<_>>();
        let mut namespace_error = None;
        let mut namespace_usable = false;
        for base in candidates(&ephemeral).filter_map(|port| port.checked_sub(offsets[0])) {
            let Some(ports) = offset_ports(base, offsets) else {
                continue;
            };
            if !ports.iter().all(|port| permitted.contains(port)) {
                continue;
            }
            let mut held = Vec::new();
            for port in ports {
                match reserve(port, directory) {
                    Ok(Some(reservation)) => {
                        namespace_usable = true;
                        held.push(reservation);
                    }
                    Ok(None) => {
                        namespace_usable = true;
                        break;
                    }
                    Err(error) if error.kind() == io::ErrorKind::ReadOnlyFilesystem => {
                        return Err(error)
                    }
                    Err(error) => {
                        namespace_error = namespace_error.or(Some(error));
                        break;
                    }
                }
            }
            if held.len() == offsets.len() {
                return Ok(Self {
                    held,
                    directory: directory.to_owned(),
                });
            }
            // A collision releases this partial layout before the next base.
        }
        if let (false, Some(error)) = (namespace_usable, namespace_error) {
            return Err(error);
        }
        Err(io::Error::new(
            io::ErrorKind::AddrNotAvailable,
            "no available TCP/UDP service port layout",
        ))
    }

    fn acquire_in(count: usize, directory: &Path) -> io::Result<Self> {
        assert!(count != 0, "a port lease must reserve at least one pair");
        let ephemeral = ephemeral_range();
        let mut held = Vec::new();
        let mut namespace_error = None;
        let mut namespace_usable = false;
        for port in candidates(&ephemeral) {
            match reserve(port, directory) {
                Ok(reservation) => {
                    namespace_usable = true;
                    if let Some(reservation) = reservation {
                        held.push(reservation);
                    }
                }
                Err(error) if error.kind() == io::ErrorKind::ReadOnlyFilesystem => {
                    return Err(error)
                }
                Err(error) => {
                    namespace_error = namespace_error.or(Some(error));
                    continue;
                }
            }
            if held.len() == count {
                return Ok(Self {
                    held,
                    directory: directory.to_owned(),
                });
            }
        }
        if let (false, Some(error)) = (namespace_usable, namespace_error) {
            return Err(error);
        }
        panic!("no {count} available TCP/UDP test port leases outside ephemeral {ephemeral:?} and legacy service/routed ranges; a host reserving every permitted port for ephemeral use has no safe handoff pool");
    }

    pub fn addresses<const N: usize>(&self) -> [SocketAddr; N] {
        self.address_list().try_into().unwrap()
    }

    pub fn address_list(&self) -> Vec<SocketAddr> {
        self.held.iter().map(|r| r.address).collect()
    }

    /// Only release probes when the owner is ready to bind. Locks stay held.
    pub fn handoff(&mut self) {
        for reservation in &mut self.held {
            reservation.tcp.take();
            reservation.udp.take();
        }
    }

    /// Move both probes directly into an owner that accepts already-bound
    /// sockets. The caller retains this lease through shutdown/release checks.
    pub fn take_sockets(&mut self) -> Vec<(TcpListener, UdpSocket)> {
        self.held
            .iter_mut()
            .map(|r| {
                let tcp = r.tcp.take().expect("bound TCP socket ownership");
                let udp = r.udp.take().expect("bound UDP socket ownership");
                (tcp, udp)
            })
            .collect()
    }

    /// Move one reservation's bound TCP listener into a connector that adopts
    /// it. The unused UDP half and the lock stay here for the whole lease, so
    /// no other binder can occupy the port; [`PortLease::reclaim`] proves the
    /// listener's release after the connector closes.
    pub fn adopt_tcp(&mut self, slot: usize) -> TcpListener {
        self.held[slot]
            .tcp
            .take()
            .expect("a held TCP half before adoption")
    }

    /// Move one reservation's bound UDP socket into a connector that adopts
    /// it. The unused TCP half and the lock stay here for the whole lease;
    /// [`PortLease::reclaim`] proves the socket's release after the connector
    /// closes.
    pub fn adopt_udp(&mut self, slot: usize) -> UdpSocket {
        self.held[slot]
            .udp
            .take()
            .expect("a held UDP half before adoption")
    }

    /// After every adopting owner closed: bind each reservation's adopted half
    /// again. This proves its owner released it and holds it until the lease
    /// ends. The unused half never left the lease, so it is never rebound.
    pub fn reclaim(&mut self) {
        for reservation in &mut self.held {
            let address = reservation.address;
            match (reservation.tcp.is_some(), reservation.udp.is_some()) {
                (false, true) => {
                    reservation.tcp = Some(released(address, TcpListener::bind(address)));
                }
                (true, false) => {
                    reservation.udp = Some(released(address, UdpSocket::bind(address)));
                }
                (tcp, udp) => panic!(
                    "{address}: reclaim proves exactly one adopted half (TCP held {tcp}, UDP held {udp})"
                ),
            }
        }
    }
}

pub fn offset_ports(base: u16, offsets: &[u16]) -> Option<Vec<u16>> {
    offsets
        .iter()
        .map(|offset| base.checked_add(*offset))
        .collect()
}

fn reserve(port: u16, directory: &Path) -> io::Result<Option<Reservation>> {
    let mut local = PORTS.lock().unwrap_or_else(PoisonError::into_inner);
    if local.contains(&port) {
        return Ok(None);
    }
    // Never unlink lock files: every process must lock the same inode.
    let lock = open_lock(&directory.join(format!("{port}.lock")))?;
    match lock.try_lock() {
        Ok(()) => (),
        Err(std::fs::TryLockError::WouldBlock) => return Ok(None),
        Err(error) => panic!("port lease {port}: {error}"),
    }
    let (tcp, udp) = match bind_pair(port) {
        Ok(sockets) => sockets,
        Err(error)
            if matches!(
                error.kind(),
                io::ErrorKind::AddrInUse | io::ErrorKind::PermissionDenied
            ) =>
        {
            return Ok(None)
        }
        Err(error) => panic!("port reservation {port}: {error}"),
    };
    let address = tcp.local_addr().unwrap();
    local.insert(port);
    Ok(Some(Reservation {
        address,
        tcp: Some(tcp),
        udp: Some(udp),
        _lock: lock,
    }))
}

fn released<T>(address: SocketAddr, bound: io::Result<T>) -> T {
    bound.unwrap_or_else(|error| panic!("{address} not released by its owner: {error}"))
}

pub fn shared_directory() -> PathBuf {
    // One namespace for one loopback network namespace, regardless of TMPDIR.
    std::env::var_os("VOTEBOAT_TEST_PORT_LEASE_DIR")
        .map_or_else(|| PathBuf::from("/tmp/voteboat-test-ports"), PathBuf::from)
}

pub fn prepare_directory(directory: &Path) -> io::Result<()> {
    match std::fs::DirBuilder::new().mode(0o1777).create(directory) {
        Ok(()) => (),
        Err(error) if error.kind() == io::ErrorKind::AlreadyExists => (),
        Err(error) => return Err(error),
    }
    let metadata = directory.metadata()?;
    if !metadata.is_dir() {
        return Err(io::Error::other("lease namespace is not a directory"));
    }
    if metadata.mode() & 0o7777 != 0o1777 {
        // Explicit chmod repairs umask-stripped creation and owned legacy dirs.
        std::fs::set_permissions(directory, std::fs::Permissions::from_mode(0o1777))?;
    }
    Ok(())
}

pub fn open_lock(path: &Path) -> io::Result<File> {
    // Opening existing files without O_CREAT avoids Linux protected_regular's
    // cross-user restriction in sticky directories, even for mode 0666 files.
    let existing = || {
        OpenOptions::new()
            .read(true)
            .write(true)
            .custom_flags(libc::O_NOFOLLOW | libc::O_NONBLOCK)
            .open(path)
    };
    let file = match existing() {
        Ok(file) => file,
        Err(error) if error.kind() == io::ErrorKind::NotFound => {
            match OpenOptions::new()
                .read(true)
                .write(true)
                .create_new(true)
                .custom_flags(libc::O_NOFOLLOW | libc::O_NONBLOCK)
                .mode(0o666)
                .open(path)
            {
                Ok(file) => file,
                Err(error) if error.kind() == io::ErrorKind::AlreadyExists => existing()?,
                Err(error) => return Err(error),
            }
        }
        Err(error) => return Err(error),
    };
    let metadata = file.metadata()?;
    if !metadata.is_file() || metadata.nlink() != 1 {
        return Err(io::Error::new(
            io::ErrorKind::InvalidData,
            "lease lock must be a regular, single-linked file",
        ));
    }
    if metadata.mode() & 0o7777 != 0o666 {
        file.set_permissions(std::fs::Permissions::from_mode(0o666))?;
    }
    Ok(file)
}

pub fn fallback_directory(shared: &Path, error: &io::Error) -> PathBuf {
    // Obtain the effective owner's uid portably, without unsafe/getuid or a
    // potentially inherited USER/HOME value (notably under sudo).
    use std::sync::atomic::{AtomicUsize, Ordering};
    static PROBES: AtomicUsize = AtomicUsize::new(0);
    let (probe, file) = loop {
        let probe = Path::new("/tmp").join(format!(
            "voteboat-lease-owner-{}-{}",
            std::process::id(),
            PROBES.fetch_add(1, Ordering::Relaxed)
        ));
        match OpenOptions::new()
            .write(true)
            .create_new(true)
            .mode(0o600)
            .open(&probe)
        {
            Ok(file) => break (probe, file),
            Err(error) if error.kind() == io::ErrorKind::AlreadyExists => continue,
            Err(error) => panic!("effective-uid probe: {error}"),
        }
    };
    let uid = file.metadata().unwrap().uid();
    std::fs::remove_file(probe).unwrap();
    let fallback = PathBuf::from(format!("/tmp/voteboat-test-ports-uid-{uid}"));
    eprintln!("VoteBoat test port leases: shared namespace {} is unusable ({error}); using {}. CROSS-USER COORDINATION IS LOST during handoff/reopen; repair the shared directory/lock permissions or use a common VOTEBOAT_TEST_PORT_LEASE_DIR.", shared.display(), fallback.display());
    fallback
}

pub fn candidates(ephemeral: &std::ops::RangeInclusive<u16>) -> impl Iterator<Item = u16> + '_ {
    // Counter service binds two bands in each low 128-wide block; routed owns
    // 10000..19999. Prefer high ports, then safe low gaps for wide ephemeral ranges.
    // Directory's bases are 20000 + 128*k (including u16 wrap); transfer's
    // bases are 14000 + 1024*k with four 128-wide slots. Exclude every residue,
    // including their command ports, until TR12/TR15 convert those allocators.
    (32768..=65535)
        .chain(1024..32768)
        .chain(1..1024)
        .filter(|port| {
            !ephemeral.contains(port)
                && !(10000..20000).contains(port)
                && !((1024..32768).contains(port) && matches!(port % 128, 0..=16 | 96..=127))
                && ![5, 6, 7, 33, 34, 35].contains(&(port % 128))
                && !(0..4).any(|slot| {
                    [1, 2, 3, 101, 102, 103]
                        .iter()
                        .any(|offset| (14000 + slot * 128 + offset) % 1024 == port % 1024)
                })
        })
}

fn bind_pair(port: u16) -> io::Result<(TcpListener, UdpSocket)> {
    let tcp = TcpListener::bind((Ipv4Addr::LOCALHOST, port))?;
    let udp = UdpSocket::bind(tcp.local_addr()?)?;
    Ok((tcp, udp))
}

fn ephemeral_range() -> std::ops::RangeInclusive<u16> {
    #[cfg(target_os = "linux")]
    {
        if let Some((start, end)) =
            std::fs::read_to_string("/proc/sys/net/ipv4/ip_local_port_range")
                .ok()
                .and_then(|text| {
                    let mut ports = text.split_whitespace().map(str::parse::<u16>);
                    Some((ports.next()?.ok()?, ports.next()?.ok()?))
                })
                .filter(|(start, end)| start <= end)
        {
            return start..=end;
        }
        // Conservative union of the Linux/macOS defaults when proc is hidden.
        eprintln!("VoteBoat test port leases: Linux proc range unavailable; assuming ephemeral range 32768..=65535");
        32768..=65535
    }
    #[cfg(not(target_os = "linux"))]
    {
        eprintln!(
            "VoteBoat test port leases: assuming macOS default ephemeral range 49152..=65535"
        );
        49152..=65535
    }
}
