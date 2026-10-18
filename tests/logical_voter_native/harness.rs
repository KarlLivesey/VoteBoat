// SPDX-License-Identifier: RPL-1.5
// Copyright (c) 2026 Karl Livesey
//! Host-side fixture support: protocol time, hang guards and port reservations.
//!
//! A [`SimClock`] supplies protocol time that depends only on the fixture's
//! own polling rounds, never on CPU speed, core count or disk latency. Real
//! `Instant` deadlines remain only as generous hang guards.
use super::{aliases::Bridge, cluster::ChildNode};
use std::{
    cell::Cell,
    net::{SocketAddr, TcpListener, UdpSocket},
    rc::Rc,
    time::{Duration, Instant},
};
use voteboat::{runtime::MonoTime, snapshot_worker::SnapshotWorker, worker::PersistenceWorker};

/// Real-time ceiling for one fixture wait. It only turns a hang into a
/// failure; protocol time never derives from it, so a slow host (a Raspberry
/// Pi or a loaded 3-core CI runner) cannot change any outcome.
pub const HANG: Duration = Duration::from_secs(300);
pub fn hang_guard() -> Instant {
    Instant::now() + HANG
}

/// Protocol time supplied to nodes for one complete fixture poll round.
pub trait FixtureClock {
    fn now(&self) -> MonoTime;
    /// Time for the next round. `settled` reports that no node polled in the
    /// round has durable worker I/O in flight.
    fn round(&self, settled: bool) -> MonoTime;
    /// Called when a fixture opens fresh nodes on this clock.
    fn reopened(&mut self) {}
}
/// Wall-clock time, kept for fixtures outside the deterministic binaries.
impl FixtureClock for Instant {
    fn now(&self) -> MonoTime {
        MonoTime(self.elapsed().as_millis() as u64)
    }
    fn round(&self, _: bool) -> MonoTime {
        self.now()
    }
    fn reopened(&mut self) {
        *self = Instant::now();
    }
}
/// Deterministic protocol time shared by every node in one test. It advances
/// one millisecond per settled round and stands still while durable worker
/// I/O is in flight, so a slow disk or CPU adds rounds but not protocol time:
/// heartbeats, elections and retained authorization actions stay bounded by
/// the workflow. Worker I/O always completes without protocol time, so
/// holding time still cannot stall progress.
#[derive(Clone, Default)]
pub struct SimClock(Rc<Cell<u64>>);
impl FixtureClock for SimClock {
    fn now(&self) -> MonoTime {
        MonoTime(self.0.get())
    }
    fn round(&self, settled: bool) -> MonoTime {
        if settled {
            self.0.set(self.0.get().checked_add(1).unwrap());
        }
        self.now()
    }
}
impl<C: FixtureClock> FixtureClock for &C {
    fn now(&self) -> MonoTime {
        (**self).now()
    }
    fn round(&self, settled: bool) -> MonoTime {
        (**self).round(settled)
    }
}

/// No durable log or snapshot work is in flight on this node's workers.
pub fn node_settled(node: &ChildNode) -> bool {
    let local = node.local();
    local.persistence.usage().requests == 0
        && local
            .snapshots
            .as_ref()
            .is_none_or(|s| s.worker.usage().requests == 0)
}
pub fn nodes_settled(nodes: &[ChildNode]) -> bool {
    nodes.iter().all(node_settled)
}
/// The same predicate for every native node borrowed by a bridge.
pub fn bridge_settled(bridge: &Bridge<'_>) -> bool {
    (0..)
        .map_while(|i| {
            bridge
                .inspect_backing(i, |local| {
                    local.persistence.usage().requests == 0
                        && local
                            .snapshots
                            .as_ref()
                            .is_none_or(|s| s.worker.usage().requests == 0)
                })
                .ok()
        })
        .all(|settled| settled)
}

/// A loopback TCP listener and UDP socket on one OS-assigned port. The
/// fixture holds it until the node that owns the address binds it, so no
/// other socket can take the port while the configuration is pending.
pub struct Reservation(TcpListener, UdpSocket);
impl Reservation {
    pub fn ephemeral() -> Self {
        // The UDP half can collide with another socket's ephemeral port;
        // choose another OS-assigned pair rather than failing.
        (0..64)
            .find_map(|_| {
                let tcp = TcpListener::bind("127.0.0.1:0").ok()?;
                let udp = UdpSocket::bind(tcp.local_addr().ok()?).ok()?;
                Some(Self(tcp, udp))
            })
            .expect("an OS-assigned TCP and UDP loopback port pair")
    }
    /// Bind the exact address again; this proves the previous owner released
    /// both protocols, then holds the address for a later reopen.
    pub fn reclaim(address: SocketAddr) -> Self {
        Self(
            TcpListener::bind(address).unwrap(),
            UdpSocket::bind(address).unwrap(),
        )
    }
    pub fn address(&self) -> SocketAddr {
        self.0.local_addr().unwrap()
    }
    /// Hand both sockets to a connector that adopts pre-bound sockets.
    pub fn into_sockets(self) -> (TcpListener, UdpSocket) {
        (self.0, self.1)
    }
}
