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
//! Caller-polled authenticated QUIC establishment on one explicitly supplied UDP socket.
use super::{connect::NativeConnectConfig, quic::*, quic_socket::QuicSocketHub, tls::*};
use crate::{connect::*, identity::*, runtime::MonoTime, secure::*, transport::ConnectTicket};
use std::{
    collections::{BTreeMap, BTreeSet},
    net::{SocketAddr, UdpSocket},
    ops::Bound,
    sync::{Arc, Mutex},
};
/// Failed construction returns the supplied bound socket. No worker is created.
pub struct QuicConnectRejected {
    pub reason: ConnectError,
    pub socket: UdpSocket,
}
impl std::fmt::Debug for QuicConnectRejected {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("QuicConnectRejected")
            .field("reason", &self.reason)
            .finish_non_exhaustive()
    }
}
struct Peer {
    remote: SocketAddr,
    pin: TlsPeer,
    generation: u64,
}
enum Stage {
    Handshake(Box<NativeQuicSession>),
    Terminal(Result<Box<NativeQuicSession>, ConnectError>),
}
struct Attempt {
    ticket: ConnectTicket,
    deadline: MonoTime,
    next: Option<MonoTime>,
    stage: Stage,
}
/// At most eight 1200-byte packets are queued per live authorized session lease.
/// Cancel/close releases owned handshakes; transferred sessions retain independent
/// leases and keep the socket alive. No anonymous peer or live migration.
/// Discovered dial addresses require explicit construction selection.
pub struct NativeQuicConnector {
    config: NativeConnectConfig,
    tls: NativeTlsConfig,
    peers: BTreeMap<NodeId, Peer>,
    hub: Option<Arc<Mutex<QuicSocketHub>>>,
    address: SocketAddr,
    attempts: BTreeMap<NodeId, Attempt>,
    cursor: Option<NodeId>,
    terminal_cursor: Option<NodeId>,
    now: MonoTime,
    closed: bool,
    discovered_dials: bool,
}
fn next<V>(map: &BTreeMap<NodeId, V>, cursor: Option<NodeId>) -> Option<NodeId> {
    cursor
        .and_then(|c| {
            map.range((Bound::Excluded(c), Bound::Unbounded))
                .next()
                .map(|(k, _)| *k)
        })
        .or_else(|| map.first_key_value().map(|(k, _)| *k))
}
impl NativeQuicConnector {
    /// Explicitly permit validated discovered Dial endpoints for already pinned
    /// peers. Accept still uses its provisioned endpoint. Accepted sessions keep
    /// their original socket lease; address changes cannot preempt them.
    pub fn new_with_discovered_dials(
        config: NativeConnectConfig,
        tls: NativeTlsConfig,
        peers: BTreeMap<NodeId, (SocketAddr, TlsPeer)>,
        socket: UdpSocket,
        now: MonoTime,
    ) -> Result<Self, Box<QuicConnectRejected>> {
        let mut connector = Self::new(config, tls, peers, socket, now)?;
        connector.discovered_dials = true;
        Ok(connector)
    }
    pub fn new(
        config: NativeConnectConfig,
        tls: NativeTlsConfig,
        peers: BTreeMap<NodeId, (SocketAddr, TlsPeer)>,
        socket: UdpSocket,
        now: MonoTime,
    ) -> Result<Self, Box<QuicConnectRejected>> {
        let validation = (|| {
            config.limits.validate()?;
            config.session.validate().map_err(ConnectError::Session)?;
            let address = socket
                .local_addr()
                .map_err(|e| ConnectError::Io(e.kind()))?;
            let mut addresses = BTreeSet::new();
            if peers.len() > 1024
                || peers.contains_key(&config.local.node)
                || peers.iter().any(|(id, (a, p))| {
                    *id != p.identity.node
                        || a.port() == 0
                        || a.ip().is_unspecified()
                        || a.ip().is_multicast()
                        || a.is_ipv4() != address.is_ipv4()
                        || *a == address
                        || !addresses.insert(*a)
                        || p.certificate.is_empty()
                        || p.certificate.len() > 65536
                        || p.server_name.is_empty()
                        || p.server_name.len() > 253
                        || rustls::pki_types::ServerName::try_from(p.server_name.as_str()).is_err()
                })
                || peers
                    .values()
                    .try_fold(0usize, |n, (_, p)| {
                        n.checked_add(p.certificate.capacity())?
                            .checked_add(p.server_name.capacity())
                    })
                    .is_none_or(|n| n > 1024 * 1024)
            {
                return Err(ConnectError::InvalidRequest);
            }
            socket
                .set_nonblocking(true)
                .map_err(|e| ConnectError::Io(e.kind()))?;
            Ok(address)
        })();
        let address = match validation {
            Ok(a) => a,
            Err(reason) => return Err(Box::new(QuicConnectRejected { reason, socket })),
        };
        let hub = QuicSocketHub::new(socket, address);
        Ok(Self {
            config,
            tls,
            peers: peers
                .into_iter()
                .map(|(id, (remote, pin))| {
                    (
                        id,
                        Peer {
                            remote,
                            pin,
                            generation: 0,
                        },
                    )
                })
                .collect(),
            hub: Some(hub),
            address,
            attempts: BTreeMap::new(),
            cursor: None,
            terminal_cursor: None,
            now,
            closed: false,
            discovered_dials: false,
        })
    }
    pub(super) fn closed_and_drained(&self) -> bool {
        self.closed && self.is_drained()
    }
    pub fn local_addr(&self) -> SocketAddr {
        self.address
    }
    fn admission(&self, r: &ConnectRequest<SocketAddr>, now: MonoTime) -> Result<(), ConnectError> {
        if now < self.now {
            return Err(ConnectError::TimeWentBack);
        }
        if self.closed {
            return Err(ConnectError::Closed);
        }
        if r.ticket.local != self.config.local {
            return Err(ConnectError::WrongBinding);
        }
        let peer = self
            .peers
            .get(&r.ticket.peer.node)
            .ok_or(ConnectError::UnknownPeer)?;
        if r.ticket.peer != peer.pin.identity {
            return Err(ConnectError::WrongBinding);
        }
        if r.ticket.generation.get() <= peer.generation {
            return Err(ConnectError::StaleConnection);
        }
        if r.deadline <= now
            || r.deadline.0 - now.0 > self.config.limits.timeout_ms
            || matches!(r.direction, ConnectDirection::Dial(a) if
                (!self.discovered_dials && a != peer.remote) || a.port() == 0
                || a.ip().is_unspecified() || a.ip().is_multicast()
                || a.is_ipv4() != self.address.is_ipv4() || a == self.address)
        {
            return Err(ConnectError::InvalidRequest);
        }
        if self.attempts.contains_key(&r.ticket.peer.node)
            || self.attempts.len() >= self.config.limits.requests
        {
            return Err(ConnectError::Overloaded);
        }
        Ok(())
    }
    fn stop(&mut self, peer: NodeId, reason: ConnectError) {
        let a = self.attempts.get_mut(&peer).unwrap();
        if !matches!(a.stage, Stage::Terminal(Err(_))) {
            a.stage = Stage::Terminal(Err(reason));
            a.next = None;
        }
    }
}
impl PeerConnector for NativeQuicConnector {
    type Endpoint = SocketAddr;
    type Session = NativeQuicSession;
    fn local(&self) -> LocalIdentity {
        self.config.local
    }
    fn supports_peer(&self, peer: PeerIdentity) -> bool {
        self.peers
            .get(&peer.node)
            .is_some_and(|p| p.pin.identity == peer)
    }
    fn limits(&self) -> ConnectLimits {
        self.config.limits
    }
    fn usage(&self) -> ConnectUsage {
        ConnectUsage {
            requests: self.attempts.len(),
            handshaking: self
                .attempts
                .values()
                .filter(|a| matches!(a.stage, Stage::Handshake(_)))
                .count(),
            ..ConnectUsage::default()
        }
    }
    fn next_deadline(&self) -> Option<MonoTime> {
        self.attempts
            .values()
            .map(|a| match a.stage {
                Stage::Terminal(_) => self.now,
                _ => a.next.map_or(a.deadline, |n| n.min(a.deadline)),
            })
            .min()
    }
    fn submit(
        &mut self,
        request: ConnectRequest<SocketAddr>,
        now: MonoTime,
    ) -> Result<(), ConnectRejected<SocketAddr>> {
        let result = (|| {
            self.admission(&request, now)?;
            let peer = &self.peers[&request.ticket.peer.node];
            let remote = match request.direction {
                ConnectDirection::Dial(address) => address,
                ConnectDirection::Accept => peer.remote,
            };
            let socket = QuicSocketHub::lease(
                self.hub.as_ref().unwrap(),
                request.ticket.peer.node,
                remote,
                request.ticket.generation.get(),
            )
            .map_err(|e| {
                if e.kind() == std::io::ErrorKind::WouldBlock {
                    ConnectError::Overloaded
                } else {
                    ConnectError::Io(e.kind())
                }
            })?;
            let mut limits = self.config.session;
            limits.handshake_timeout_ms =
                limits.handshake_timeout_ms.min(request.deadline.0 - now.0);
            NativeQuicSession::new(
                socket,
                &self.tls,
                QuicSessionOptions {
                    local: self.config.local,
                    peer: peer.pin.clone(),
                    remote,
                    generation: request.ticket.generation,
                    limits,
                },
                now,
                matches!(request.direction, ConnectDirection::Dial(_)),
            )
            .map_err(ConnectError::Session)
        })();
        let mut session = match result {
            Ok(s) => s,
            Err(reason) => {
                return Err(ConnectRejected {
                    reason,
                    request: Box::new(request),
                })
            }
        };
        let deadline = session.next_deadline();
        self.now = now;
        self.peers
            .get_mut(&request.ticket.peer.node)
            .unwrap()
            .generation = request.ticket.generation.get();
        self.attempts.insert(
            request.ticket.peer.node,
            Attempt {
                ticket: request.ticket,
                deadline: request.deadline,
                next: deadline,
                stage: Stage::Handshake(Box::new(session)),
            },
        );
        Ok(())
    }
    fn cancel(&mut self, ticket: ConnectTicket) -> bool {
        if self
            .attempts
            .get(&ticket.peer.node)
            .is_none_or(|a| a.ticket != ticket)
        {
            return false;
        }
        self.stop(ticket.peer.node, ConnectError::Cancelled);
        true
    }
    fn poll(
        &mut self,
        now: MonoTime,
        budget: ConnectPollBudget,
    ) -> Result<Vec<ConnectCompletion<Self::Session>>, ConnectError> {
        let budget = budget.validate()?;
        if now < self.now {
            return Err(ConnectError::TimeWentBack);
        }
        self.now = now;
        let expired = self
            .attempts
            .iter()
            .filter(|(_, a)| now >= a.deadline)
            .map(|(id, _)| *id)
            .collect::<Vec<_>>();
        for id in expired {
            self.stop(id, ConnectError::Timeout);
        }
        for _ in 0..budget.visits.min(self.attempts.len()) {
            let id = next(&self.attempts, self.cursor).unwrap();
            self.cursor = Some(id);
            let a = self.attempts.get_mut(&id).unwrap();
            if let Stage::Handshake(session) = &mut a.stage {
                match session.poll(now, budget.session) {
                    Err(e) => {
                        a.stage = Stage::Terminal(Err(ConnectError::Session(e)));
                        a.next = None;
                    }
                    Ok(_) => {
                        a.next = session.next_deadline();
                        if session.state() == SessionState::Ready {
                            let valid = require_authenticated(session.as_ref()).is_ok_and(|b| {
                                b.local == a.ticket.local
                                    && b.peer.node == a.ticket.peer.node
                                    && b.peer.store.identity == a.ticket.peer.store
                                    && b.generation == a.ticket.generation
                                    && b.wire_version == self.tls.wire_version()
                            });
                            if valid {
                                let Stage::Handshake(s) = std::mem::replace(
                                    &mut a.stage,
                                    Stage::Terminal(Err(ConnectError::ProviderViolation)),
                                ) else {
                                    unreachable!()
                                };
                                a.stage = Stage::Terminal(Ok(s));
                            } else {
                                a.stage = Stage::Terminal(Err(ConnectError::ProviderViolation));
                            }
                        }
                    }
                }
            }
        }
        let mut out = Vec::new();
        for _ in 0..self.attempts.len() {
            if out.len() >= budget.completions {
                break;
            }
            let id = next(&self.attempts, self.terminal_cursor).unwrap();
            self.terminal_cursor = Some(id);
            if matches!(self.attempts[&id].stage, Stage::Terminal(_)) {
                let a = self.attempts.remove(&id).unwrap();
                let Stage::Terminal(result) = a.stage else {
                    unreachable!()
                };
                out.push(ConnectCompletion {
                    ticket: a.ticket,
                    result: result.map(|s| *s),
                });
            }
        }
        Ok(out)
    }
    fn close(&mut self) {
        self.closed = true;
        let peers = self.attempts.keys().copied().collect::<Vec<_>>();
        for id in peers {
            self.stop(id, ConnectError::Cancelled);
        }
        self.hub.take();
    }
}
