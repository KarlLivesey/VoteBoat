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
    // Drop closes probes, removes the registry entry, then drops the file lock.
    pub(crate) sockets: Option<(TcpListener, UdpSocket)>,
    pub(crate) _lock: File,
}

impl Drop for Reservation {
    fn drop(&mut self) {
        self.sockets.take();
        PORTS
            .lock()
            .unwrap_or_else(PoisonError::into_inner)
            .remove(&self.address.port());
    }
}

pub struct PortLease {
    pub(crate) held: Vec<Reservation>,
}

impl PortLease {
    pub fn acquire(count: usize) -> Self {
        let directory = shared_directory();
        let result =
            prepare_directory(&directory).and_then(|()| Self::acquire_in(count, &directory));
        match result {
            Ok(lease) => lease,
            Err(error) => {
                let fallback = fallback_directory(&directory, &error);
                prepare_directory(&fallback).unwrap();
                Self::acquire_in(count, &fallback).unwrap()
            }
        }
    }

    fn acquire_in(count: usize, directory: &Path) -> io::Result<Self> {
        assert!(count != 0, "a port lease must reserve at least one pair");
        let ephemeral = ephemeral_range();
        let mut held = Vec::new();
        for port in candidates(&ephemeral) {
            let mut local = PORTS.lock().unwrap_or_else(PoisonError::into_inner);
            if local.contains(&port) {
                continue;
            }
            // Never unlink lock files: every process must lock the same inode.
            let lock = open_lock(&directory.join(format!("{port}.lock")))?;
            match lock.try_lock() {
                Ok(()) => (),
                Err(std::fs::TryLockError::WouldBlock) => continue,
                Err(error) => panic!("port lease {port}: {error}"),
            }
            let sockets = match bind_pair(port) {
                Ok(sockets) => sockets,
                Err(error) if error.kind() == io::ErrorKind::AddrInUse => continue,
                Err(error) => panic!("port reservation {port}: {error}"),
            };
            let address = sockets.0.local_addr().unwrap();
            local.insert(port);
            held.push(Reservation {
                address,
                sockets: Some(sockets),
                _lock: lock,
            });
            if held.len() == count {
                return Ok(Self { held });
            }
        }
        panic!("no {count} available TCP/UDP test port leases outside ephemeral {ephemeral:?} and legacy service/routed ranges; a host reserving every unprivileged port for ephemeral use has no safe handoff pool");
    }

    pub fn addresses<const N: usize>(&self) -> [SocketAddr; N] {
        self.held
            .iter()
            .map(|r| r.address)
            .collect::<Vec<_>>()
            .try_into()
            .unwrap()
    }

    /// Only release probes when the owner is ready to bind. Locks stay held.
    pub fn handoff(&mut self) {
        for reservation in &mut self.held {
            reservation.sockets.take();
        }
    }

    /// Move probes directly into a connector that accepts already-bound sockets.
    /// The caller retains this lease through connector shutdown/release checks.
    pub fn take_sockets(&mut self) -> Vec<(TcpListener, UdpSocket)> {
        self.held
            .iter_mut()
            .map(|r| r.sockets.take().expect("bound socket ownership"))
            .collect()
    }
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
    let file = match OpenOptions::new().read(true).write(true).open(path) {
        Ok(file) => file,
        Err(error) if error.kind() == io::ErrorKind::NotFound => {
            match OpenOptions::new()
                .read(true)
                .write(true)
                .create_new(true)
                .mode(0o666)
                .open(path)
            {
                Ok(file) => file,
                Err(error) if error.kind() == io::ErrorKind::AlreadyExists => {
                    OpenOptions::new().read(true).write(true).open(path)?
                }
                Err(error) => return Err(error),
            }
        }
        Err(error) => return Err(error),
    };
    if file.metadata()?.mode() & 0o7777 != 0o666 {
        file.set_permissions(std::fs::Permissions::from_mode(0o666))?;
    }
    Ok(file)
}

pub fn fallback_directory(shared: &Path, error: &io::Error) -> PathBuf {
    // Obtain the effective owner's uid portably, without unsafe/getuid or a
    // potentially inherited USER/HOME value (notably under sudo).
    use std::sync::atomic::{AtomicU64, Ordering};
    static PROBES: AtomicU64 = AtomicU64::new(0);
    let probe = std::env::temp_dir().join(format!(
        "voteboat-lease-owner-{}-{}",
        std::process::id(),
        PROBES.fetch_add(1, Ordering::Relaxed)
    ));
    let file = OpenOptions::new()
        .write(true)
        .create_new(true)
        .mode(0o600)
        .open(&probe)
        .unwrap();
    let uid = file.metadata().unwrap().uid();
    std::fs::remove_file(probe).unwrap();
    let fallback = PathBuf::from(format!("/tmp/voteboat-test-ports-uid-{uid}"));
    eprintln!("VoteBoat test port leases: shared namespace {} is unusable ({error}); using {}. CROSS-USER COORDINATION IS LOST during handoff/reopen; repair the shared directory/lock permissions or use a common VOTEBOAT_TEST_PORT_LEASE_DIR.", shared.display(), fallback.display());
    fallback
}

pub fn candidates(ephemeral: &std::ops::RangeInclusive<u16>) -> impl Iterator<Item = u16> + '_ {
    // Counter service owns 1024..32767; routed owns 10000..19999.
    // Directory's bases are 20000 + 128*k (including u16 wrap); transfer's
    // bases are 14000 + 1024*k with four 128-wide slots. Exclude every residue,
    // including their command ports, until TR12/TR15 convert those allocators.
    (32768..=65535).filter(|port| {
        !ephemeral.contains(port)
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
    if let Some((start, end)) = std::fs::read_to_string("/proc/sys/net/ipv4/ip_local_port_range")
        .ok()
        .and_then(|text| {
            let mut ports = text.split_whitespace().map(str::parse::<u16>);
            Some((ports.next()?.ok()?, ports.next()?.ok()?))
        })
        .filter(|(start, end)| start <= end)
    {
        return start..=end;
    }
    // macOS defaults; also the documented fallback when Linux proc is absent.
    eprintln!("VoteBoat test port leases: assuming ephemeral range 49152..=65535 (macOS default / unavailable Linux proc range)");
    49152..=65535
}
