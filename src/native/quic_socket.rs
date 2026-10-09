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
//! Internal bounded socket sharing for the native QUIC connector.
use crate::identity::NodeId;
use std::{
    collections::{BTreeMap, VecDeque},
    io,
    net::{SocketAddr, UdpSocket},
    sync::{Arc, Mutex, MutexGuard},
};
pub(super) const MTU: usize = 1200;
const QUEUED: usize = 8;
struct Mailbox {
    owner: NodeId,
    generation: u64,
    packets: VecDeque<Vec<u8>>,
}
pub(super) struct QuicSocketHub {
    socket: UdpSocket,
    address: SocketAddr,
    leases: BTreeMap<SocketAddr, Mailbox>,
}
impl QuicSocketHub {
    pub(super) fn new(socket: UdpSocket, address: SocketAddr) -> Arc<Mutex<Self>> {
        Arc::new(Mutex::new(Self {
            socket,
            address,
            leases: BTreeMap::new(),
        }))
    }
    pub(super) fn lease(
        hub: &Arc<Mutex<Self>>,
        owner: NodeId,
        remote: SocketAddr,
        generation: u64,
    ) -> io::Result<SessionSocket> {
        let mut shared = lock(hub)?;
        if shared.leases.contains_key(&remote) || shared.leases.values().any(|m| m.owner == owner) {
            return Err(io::ErrorKind::WouldBlock.into());
        }
        shared.leases.insert(
            remote,
            Mailbox {
                owner,
                generation,
                packets: VecDeque::new(),
            },
        );
        Ok(SessionSocket::Shared(SharedSocket {
            hub: hub.clone(),
            remote,
            generation,
        }))
    }
}
fn lock(hub: &Arc<Mutex<QuicSocketHub>>) -> io::Result<MutexGuard<'_, QuicSocketHub>> {
    hub.lock().map_err(|_| io::ErrorKind::Other.into())
}
pub(super) struct SharedSocket {
    hub: Arc<Mutex<QuicSocketHub>>,
    remote: SocketAddr,
    generation: u64,
}
impl Drop for SharedSocket {
    fn drop(&mut self) {
        // A poisoned mutex must still release a retired lease and its packets.
        let mut hub = self.hub.lock().unwrap_or_else(|e| e.into_inner());
        if hub
            .leases
            .get(&self.remote)
            .is_some_and(|m| m.generation == self.generation)
        {
            hub.leases.remove(&self.remote);
        }
    }
}
pub(super) enum SessionSocket {
    Dedicated(UdpSocket),
    Shared(SharedSocket),
}
impl SessionSocket {
    pub(super) fn local_addr(&self) -> io::Result<SocketAddr> {
        match self {
            Self::Dedicated(s) => s.local_addr(),
            Self::Shared(s) => Ok(lock(&s.hub)?.address),
        }
    }
    pub(super) fn set_nonblocking(&self) -> io::Result<()> {
        match self {
            Self::Dedicated(s) => s.set_nonblocking(true),
            Self::Shared(_) => Ok(()),
        }
    }
    // Charge every consumed datagram, including another peer's queued packet.
    // A queued read is conservatively charged too. At most one OS read occurs.
    pub(super) fn recv(&self, bytes: &mut [u8; MTU]) -> io::Result<(usize, Option<SocketAddr>)> {
        match self {
            Self::Dedicated(s) => s.recv_from(bytes).map(|(n, a)| (n, Some(a))),
            Self::Shared(s) => {
                let mut hub = lock(&s.hub)?;
                let mailbox = hub
                    .leases
                    .get_mut(&s.remote)
                    .ok_or(io::ErrorKind::NotConnected)?;
                if mailbox.generation != s.generation {
                    return Err(io::ErrorKind::NotConnected.into());
                }
                if let Some(packet) = mailbox.packets.pop_front() {
                    bytes[..packet.len()].copy_from_slice(&packet);
                    return Ok((packet.len(), Some(s.remote)));
                }
                let (n, remote) = hub.socket.recv_from(bytes)?;
                if remote == s.remote {
                    return Ok((n, Some(remote)));
                }
                if let Some(mailbox) = hub.leases.get_mut(&remote) {
                    if mailbox.packets.len() < QUEUED {
                        mailbox.packets.push_back(bytes[..n].to_vec());
                    }
                }
                Ok((n, None))
            }
        }
    }
    pub(super) fn send_to(&self, bytes: &[u8], remote: SocketAddr) -> io::Result<usize> {
        match self {
            Self::Dedicated(s) => s.send_to(bytes, remote),
            Self::Shared(s) => {
                let hub = lock(&s.hub)?;
                if remote != s.remote
                    || hub
                        .leases
                        .get(&remote)
                        .is_none_or(|m| m.generation != s.generation)
                {
                    return Err(io::ErrorKind::NotConnected.into());
                }
                hub.socket.send_to(bytes, remote)
            }
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    #[test]
    fn routed_datagrams_are_charged_bounded_and_discarded_with_the_retired_lease() {
        let bound = UdpSocket::bind("127.0.0.1:0").unwrap();
        let address = bound.local_addr().unwrap();
        bound.set_nonblocking(true).unwrap();
        let hub = QuicSocketHub::new(bound, address);
        let a = UdpSocket::bind("127.0.0.1:0").unwrap();
        let b = UdpSocket::bind("127.0.0.1:0").unwrap();
        let stranger = UdpSocket::bind("127.0.0.1:0").unwrap();
        let a_route =
            QuicSocketHub::lease(&hub, NodeId::new(1).unwrap(), a.local_addr().unwrap(), 1)
                .unwrap();
        let b_route =
            QuicSocketHub::lease(&hub, NodeId::new(2).unwrap(), b.local_addr().unwrap(), 1)
                .unwrap();
        assert_eq!(
            QuicSocketHub::lease(
                &hub,
                NodeId::new(1).unwrap(),
                stranger.local_addr().unwrap(),
                2
            )
            .err()
            .unwrap()
            .kind(),
            io::ErrorKind::WouldBlock
        );
        assert_eq!(
            QuicSocketHub::lease(&hub, NodeId::new(3).unwrap(), a.local_addr().unwrap(), 1)
                .err()
                .unwrap()
                .kind(),
            io::ErrorKind::WouldBlock
        );
        let mut bytes = [0; MTU];
        for n in 0..20u8 {
            b.send_to(&[n; MTU], address).unwrap();
            assert_eq!(a_route.recv(&mut bytes).unwrap(), (MTU, None));
        }
        assert_eq!(
            lock(&hub).unwrap().leases[&b.local_addr().unwrap()]
                .packets
                .len(),
            QUEUED
        );
        stranger.send_to(&[9; MTU], address).unwrap();
        assert_eq!(a_route.recv(&mut bytes).unwrap(), (MTU, None));
        assert_eq!(lock(&hub).unwrap().leases.len(), 2);
        for n in 0..QUEUED as u8 {
            assert_eq!(
                b_route.recv(&mut bytes).unwrap(),
                (MTU, Some(b.local_addr().unwrap()))
            );
            assert_eq!(bytes, [n; MTU]);
        }
        b.send_to(&[77; MTU], address).unwrap();
        assert_eq!(a_route.recv(&mut bytes).unwrap(), (MTU, None));
        drop(b_route);
        let replacement =
            QuicSocketHub::lease(&hub, NodeId::new(2).unwrap(), b.local_addr().unwrap(), 2)
                .unwrap();
        assert_eq!(
            replacement.recv(&mut bytes).unwrap_err().kind(),
            io::ErrorKind::WouldBlock
        );
        b.send_to(&[88; MTU], address).unwrap();
        assert_eq!(
            replacement.recv(&mut bytes).unwrap(),
            (MTU, Some(b.local_addr().unwrap()))
        );
        assert_eq!(bytes, [88; MTU]);
    }
}
