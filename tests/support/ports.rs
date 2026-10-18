// SPDX-License-Identifier: RPL-1.5
// Copyright (c) 2026 Karl Livesey
//! Addresses must be known before native startup. Keep a cross-process lease
//! through startup, reopen and release assertions, not merely a probe/drop pair.
use std::{
    collections::BTreeSet,
    fs::{File, OpenOptions},
    io,
    net::{Ipv4Addr, SocketAddr, TcpListener, UdpSocket},
    sync::{Mutex, PoisonError},
};

static PORTS: Mutex<BTreeSet<u16>> = Mutex::new(BTreeSet::new());

struct Reservation {
    address: SocketAddr,
    // Field order closes probes before releasing the lock and local registry.
    sockets: Option<(TcpListener, UdpSocket)>,
    _lock: File,
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
    held: Vec<Reservation>,
}

impl PortLease {
    pub fn acquire(count: usize) -> Self {
        let directory = std::env::temp_dir().join("voteboat-test-ports");
        Self::acquire_in(count, &directory)
    }

    /// Private namespaces are only for allocator cleanup tests, which never
    /// hand sockets to native owners. Node fixtures must all use `acquire`.
    pub fn acquire_in(count: usize, directory: &std::path::Path) -> Self {
        assert!(count != 0, "a port lease must reserve at least one pair");
        std::fs::create_dir_all(directory).unwrap();
        let ephemeral = ephemeral_range();
        let mut held = Vec::new();
        for port in 10000..32768 {
            if ephemeral.contains(&port) {
                continue;
            }
            let mut local = PORTS.lock().unwrap_or_else(PoisonError::into_inner);
            if local.contains(&port) {
                continue;
            }
            // Never remove these files: every process must lock the same inode.
            let lock = OpenOptions::new()
                .read(true)
                .write(true)
                .create(true)
                .truncate(false)
                .open(directory.join(format!("{port}.lock")))
                .unwrap();
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
                return Self { held };
            }
        }
        panic!("no {count} available TCP/UDP test port leases outside {ephemeral:?}");
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
