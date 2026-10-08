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
//! Bounded connection coordination over construction-selected peer transports.
use super::*;
use crate::identity::*;
use std::collections::BTreeMap;

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub struct ConnectTicket {
    pub local: LocalIdentity,
    pub peer: PeerIdentity,
    pub generation: SecureSessionGeneration,
}
#[derive(Clone, Copy, Debug)]
pub struct PeerRosterLimits {
    pub peers: usize,
    pub connecting: usize,
    /// Sum of declared transport frame/decoded ceilings for reserved connections.
    /// TLS, sockets, handshake resources and outbound ownership are separate.
    pub connection_bytes: usize,
    pub connect_timeout_ms: u64,
    pub retry_min_ms: u64,
    pub retry_max_ms: u64,
}
#[derive(Clone, Copy, Debug)]
pub struct PeerRosterConfig {
    pub local: LocalIdentity,
    pub outbound: OutboundBinding,
    /// First of a host-reserved, non-overlapping generation range within the
    /// recovered local store session. Never reuse a range for another roster.
    pub first_generation: SecureSessionGeneration,
    pub last_generation: SecureSessionGeneration,
    pub wire_version: u16,
    pub limits: PeerRosterLimits,
    pub transport_limits: TransportLimits,
}
impl Default for PeerRosterLimits {
    fn default() -> Self {
        Self {
            peers: 1024,
            connecting: 16,
            connection_bytes: 256 * 1024 * 1024,
            connect_timeout_ms: 10_000,
            retry_min_ms: 100,
            retry_max_ms: 10_000,
        }
    }
}
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum PeerRosterError {
    InvalidLimits,
    WrongBinding,
    UnknownPeer,
    StaleConnection,
    Overloaded,
    Closed,
    Fenced,
    TimeWentBack,
    Exhausted,
    ProviderViolation,
    Transport(TransportError),
}
#[derive(Debug)]
pub struct ConnectionRejected<P> {
    pub reason: PeerRosterError,
    pub transport: P,
}
#[derive(Debug)]
pub struct PeerSendRejected {
    pub reason: PeerRosterError,
    pub batch: Box<OutboundBatch>,
}
#[derive(Debug)]
pub struct PeerCompletionRejected {
    pub reason: PeerRosterError,
    pub send: Box<TransportSend>,
}
#[derive(Debug)]
pub struct PeerReceiveRejected {
    pub reason: PeerRosterError,
    pub batch: Box<ReceivedBatch>,
}
#[derive(Clone, Copy, Debug, Default, Eq, PartialEq)]
pub struct PeerRosterUsage {
    pub connections: usize,
    pub connecting: usize,
    pub reserved_bytes: usize,
}
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum PeerPoll {
    Progress {
        peer: NodeId,
        progress: TransportProgress,
    },
    ConnectExpired(ConnectTicket),
    Disconnected {
        peer: NodeId,
        error: Option<TransportError>,
    },
}
struct Attempt {
    ticket: ConnectTicket,
    expires: MonoTime,
}
struct Peer<P> {
    identity: PeerIdentity,
    next: MonoTime,
    failures: u32,
    attempt: Option<Attempt>,
    transport: Option<P>,
    binding: Option<SessionBinding>,
    retiring: bool,
    accepted: Option<SendTicket>,
    session: Option<StoreSession>,
}
/// Fixed identity/lifetime coordination, not a quorum or discovery policy.
/// No sockets, executors or queues are created. The host drives bounded polls
/// and establishes authenticated transports for returned connect tickets.
pub struct PeerRoster<P: PeerTransport> {
    local: LocalIdentity,
    outbound: OutboundBinding,
    wire_version: u16,
    limits: PeerRosterLimits,
    transport_limits: TransportLimits,
    per_connection: usize,
    peers: BTreeMap<NodeId, Peer<P>>,
    connect_cursor: Option<NodeId>,
    poll_cursor: Option<NodeId>,
    sequence: u64,
    last_generation: u64,
    now: MonoTime,
    closed: bool,
    fenced: bool,
}
impl<P: PeerTransport> PeerRoster<P> {
    pub fn new(
        config: PeerRosterConfig,
        authorized: BTreeMap<NodeId, StoreIdentity>,
        now: MonoTime,
    ) -> Result<Self, PeerRosterError> {
        let PeerRosterConfig {
            local,
            outbound,
            first_generation,
            last_generation,
            wire_version,
            limits,
            transport_limits,
        } = config;
        let t = transport_limits
            .validate()
            .map_err(PeerRosterError::Transport)?;
        let per_connection = t
            .send_frame_bytes
            .checked_add(t.receive_frame_bytes)
            .and_then(|n| n.checked_add(t.decoded_bytes))
            .ok_or(PeerRosterError::InvalidLimits)?;
        if local.node != outbound.node || local.store != outbound.store {
            return Err(PeerRosterError::WrongBinding);
        }
        if limits.peers == 0
            || last_generation < first_generation
            || limits.peers > 65536
            || authorized.len() > limits.peers
            || authorized.contains_key(&local.node)
            || wire_version == 0
            || limits.connecting == 0
            || limits.connecting > limits.peers
            || limits.connection_bytes < per_connection
            || limits.connection_bytes as u128 > 4 * 1024 * 1024 * 1024u128
            || limits.connect_timeout_ms == 0
            || limits.connect_timeout_ms > 3_600_000
            || limits.retry_min_ms == 0
            || limits.retry_max_ms < limits.retry_min_ms
            || limits.retry_max_ms > 3_600_000
        {
            return Err(PeerRosterError::InvalidLimits);
        }
        Ok(Self {
            local,
            outbound,
            wire_version,
            limits,
            transport_limits: t,
            per_connection,
            peers: authorized
                .into_iter()
                .map(|(node, store)| {
                    (
                        node,
                        Peer {
                            identity: PeerIdentity { node, store },
                            next: now,
                            failures: 0,
                            attempt: None,
                            transport: None,
                            binding: None,
                            retiring: false,
                            accepted: None,
                            session: None,
                        },
                    )
                })
                .collect(),
            connect_cursor: None,
            poll_cursor: None,
            sequence: first_generation.get() - 1,
            last_generation: last_generation.get(),
            now,
            closed: false,
            fenced: false,
        })
    }
    pub fn usage(&self) -> PeerRosterUsage {
        let connections = self
            .peers
            .values()
            .filter(|p| p.transport.is_some() || p.attempt.is_some())
            .count();
        PeerRosterUsage {
            connections,
            connecting: self.peers.values().filter(|p| p.attempt.is_some()).count(),
            reserved_bytes: connections * self.per_connection,
        }
    }
    pub fn binding(&self, peer: NodeId) -> Option<SessionBinding> {
        let p = self.peers.get(&peer)?;
        if p.retiring {
            None
        } else {
            p.binding
        }
    }
    pub fn is_fenced(&self) -> bool {
        self.fenced
    }
    /// Local scheduling hint only. A due waiting peer is omitted while no
    /// connection/handshake capacity is available, preventing a busy retry loop.
    pub fn next_deadline(&self) -> Option<MonoTime> {
        if self.closed || self.fenced {
            return None;
        }
        let u = self.usage();
        let available = u.connecting < self.limits.connecting
            && self.sequence < self.last_generation
            && self.per_connection <= self.limits.connection_bytes - u.reserved_bytes;
        self.peers
            .values()
            .filter_map(|p| {
                if let Some(a) = &p.attempt {
                    Some(a.expires)
                } else if available && p.transport.is_none() {
                    Some(p.next)
                } else {
                    None
                }
            })
            .min()
    }
    /// Exact attempt deadline for a separately bounded connection provider.
    /// A missing/stale ticket grants no authority to start or attach work.
    pub fn attempt_deadline(&self, ticket: ConnectTicket) -> Option<MonoTime> {
        self.peers
            .get(&ticket.peer.node)?
            .attempt
            .as_ref()
            .filter(|a| a.ticket == ticket)
            .map(|a| a.expires)
    }
    pub fn is_drained(&self) -> bool {
        self.closed
            && self
                .peers
                .values()
                .all(|p| p.attempt.is_none() && p.transport.is_none())
    }
    /// After complete shutdown, hand off the next unused generation to a
    /// replacement roster. Concurrent rosters need disjoint host-reserved ranges.
    pub fn reclaim_generation(&self) -> Result<SecureSessionGeneration, PeerRosterError> {
        if !self.is_drained() {
            return Err(PeerRosterError::Overloaded);
        }
        self.sequence
            .checked_add(1)
            .filter(|n| *n <= self.last_generation)
            .and_then(SecureSessionGeneration::new)
            .ok_or(PeerRosterError::Exhausted)
    }
    fn time(&mut self, now: MonoTime) -> Result<(), PeerRosterError> {
        if now < self.now {
            return Err(PeerRosterError::TimeWentBack);
        }
        self.now = now;
        Ok(())
    }
    fn keys_after(&self, cursor: Option<NodeId>, limit: usize) -> Vec<NodeId> {
        use std::ops::Bound::{Excluded, Unbounded};
        match cursor {
            Some(c) => self
                .peers
                .range((Excluded(c), Unbounded))
                .chain(self.peers.range(..=c))
                .take(limit.min(self.peers.len()))
                .map(|(k, _)| *k)
                .collect(),
            None => self.peers.keys().take(limit).copied().collect(),
        }
    }
    /// Reserve a finite connection slot before the host starts dialing/handshake.
    /// At most `limit` peers are examined, with a rotating cursor across calls.
    pub fn due_connections(
        &mut self,
        now: MonoTime,
        limit: usize,
    ) -> Result<Vec<ConnectTicket>, PeerRosterError> {
        self.time(now)?;
        if self.fenced {
            return Err(PeerRosterError::Fenced);
        }
        if self.closed {
            return Err(PeerRosterError::Closed);
        }
        if limit == 0 || self.peers.is_empty() {
            return Ok(Vec::new());
        }
        // Preflight exhaustion before creating any ticket that a returned error
        // could hide from its owner.
        let expires = MonoTime(
            now.0
                .checked_add(self.limits.connect_timeout_ms)
                .ok_or(PeerRosterError::Exhausted)?,
        );
        let mut usage = self.usage();
        let mut tickets = Vec::new();
        for id in self.keys_after(self.connect_cursor, limit) {
            self.connect_cursor = Some(id);
            let p = self.peers.get_mut(&id).unwrap();
            if p.transport.is_some() || p.attempt.is_some() || now < p.next {
                continue;
            }
            if usage.connecting == self.limits.connecting
                || self.per_connection > self.limits.connection_bytes - usage.reserved_bytes
            {
                break;
            }
            let Some(sequence) = self
                .sequence
                .checked_add(1)
                .filter(|n| *n <= self.last_generation)
            else {
                if tickets.is_empty() {
                    return Err(PeerRosterError::Exhausted);
                }
                break;
            };
            let ticket = ConnectTicket {
                local: self.local,
                peer: p.identity,
                generation: SecureSessionGeneration::new(sequence).unwrap(),
            };
            self.sequence = sequence;
            p.attempt = Some(Attempt { ticket, expires });
            usage.connecting += 1;
            usage.reserved_bytes += self.per_connection;
            tickets.push(ticket);
        }
        Ok(tickets)
    }
    fn backoff(p: &mut Peer<P>, now: MonoTime, limits: PeerRosterLimits) {
        let delay = limits
            .retry_min_ms
            .saturating_mul(1u64 << p.failures.min(63))
            .min(limits.retry_max_ms);
        p.failures = p.failures.saturating_add(1);
        p.next = MonoTime(now.0.saturating_add(delay));
    }
    pub fn connect_failed(
        &mut self,
        ticket: ConnectTicket,
        now: MonoTime,
    ) -> Result<(), PeerRosterError> {
        self.time(now)?;
        let p = self
            .peers
            .get_mut(&ticket.peer.node)
            .ok_or(PeerRosterError::UnknownPeer)?;
        if p.attempt.as_ref().is_none_or(|a| a.ticket != ticket) {
            return Err(PeerRosterError::StaleConnection);
        }
        p.attempt = None;
        Self::backoff(p, now, self.limits);
        Ok(())
    }
    pub fn attach(
        &mut self,
        ticket: ConnectTicket,
        transport: P,
        now: MonoTime,
    ) -> Result<(), ConnectionRejected<P>> {
        let check = (|| {
            self.time(now)?;
            if self.fenced {
                return Err(PeerRosterError::Fenced);
            }
            if self.closed {
                return Err(PeerRosterError::Closed);
            }
            let p = self
                .peers
                .get(&ticket.peer.node)
                .ok_or(PeerRosterError::UnknownPeer)?;
            if p.attempt
                .as_ref()
                .is_none_or(|a| a.ticket != ticket || now >= a.expires)
            {
                return Err(PeerRosterError::StaleConnection);
            }
            let b = transport.binding();
            let t = transport
                .limits()
                .validate()
                .map_err(PeerRosterError::Transport)?;
            let u = transport.usage();
            if transport.security() != SessionSecurity::Authenticated
                || transport.state() != TransportState::Open
                || b.local != self.local
                || b.peer.node != p.identity.node
                || b.peer.store.identity != p.identity.store
                || b.generation != ticket.generation
                || b.wire_version != self.wire_version
                || p.session.is_some_and(|s| b.peer.store.session < s)
                || u.sending
                || u.completion
                || u.decoded_bytes != 0
                || u.send_frame_bytes > t.send_frame_bytes
                || u.receive_frame_bytes > t.receive_frame_bytes
                || t.send_frame_bytes > self.transport_limits.send_frame_bytes
                || t.receive_frame_bytes > self.transport_limits.receive_frame_bytes
                || t.decoded_bytes > self.transport_limits.decoded_bytes
            {
                return Err(PeerRosterError::WrongBinding);
            }
            Ok(b)
        })();
        let binding = match check {
            Ok(b) => b,
            Err(reason) => return Err(ConnectionRejected { reason, transport }),
        };
        let p = self.peers.get_mut(&ticket.peer.node).unwrap();
        p.attempt = None;
        p.binding = Some(binding);
        p.session = Some(binding.peer.store.session);
        p.transport = Some(transport);
        p.retiring = false;
        Ok(())
    }
    pub fn submit(&mut self, batch: OutboundBatch) -> Result<(), PeerSendRejected> {
        let check = (|| {
            if self.fenced {
                return Err(PeerRosterError::Fenced);
            }
            if self.closed {
                return Err(PeerRosterError::Closed);
            }
            if batch.ticket.binding != self.outbound || batch.ticket.sequence == 0 {
                return Err(PeerRosterError::WrongBinding);
            }
            let p = self
                .peers
                .get(&batch.ticket.peer)
                .ok_or(PeerRosterError::UnknownPeer)?;
            if p.retiring || p.transport.is_none() {
                return Err(PeerRosterError::Overloaded);
            }
            let t = p.transport.as_ref().unwrap();
            if Some(t.binding()) != p.binding || t.security() != SessionSecurity::Authenticated {
                return Err(PeerRosterError::ProviderViolation);
            }
            if p.accepted.is_some() {
                return Err(PeerRosterError::Overloaded);
            }
            Ok(())
        })();
        if let Err(reason) = check {
            return Err(PeerSendRejected {
                reason,
                batch: Box::new(batch),
            });
        }
        let p = self.peers.get_mut(&batch.ticket.peer).unwrap();
        let ticket = batch.ticket;
        match p.transport.as_mut().unwrap().submit(batch) {
            Ok(()) => {
                p.accepted = Some(ticket);
                Ok(())
            }
            Err(r) => Err(PeerSendRejected {
                reason: PeerRosterError::Transport(r.reason),
                batch: r.batch,
            }),
        }
    }
    fn retire(p: &mut Peer<P>) {
        if let Some(t) = &mut p.transport {
            t.abort();
            drop(t.take_received());
        }
        p.retiring = true;
    }
    fn finish_retire(p: &mut Peer<P>) {
        if p.retiring && p.accepted.is_none() {
            p.transport = None;
            p.binding = None;
            p.retiring = false;
        }
    }
    pub fn disconnect(&mut self, peer: NodeId, now: MonoTime) -> Result<(), PeerRosterError> {
        self.time(now)?;
        let p = self
            .peers
            .get_mut(&peer)
            .ok_or(PeerRosterError::UnknownPeer)?;
        p.attempt = None;
        Self::retire(p);
        Self::backoff(p, now, self.limits);
        Self::finish_retire(p);
        Ok(())
    }
    /// Fair bounded visits. Failed connections retain accepted send ownership
    /// until take_send; their decoded input is discarded before replacement.
    pub fn poll(
        &mut self,
        now: MonoTime,
        visits: usize,
        budget: TransportPollBudget,
    ) -> Result<Vec<PeerPoll>, PeerRosterError> {
        budget.validate().map_err(PeerRosterError::Transport)?;
        self.time(now)?;
        let mut events = Vec::new();
        for id in self.keys_after(self.poll_cursor, visits) {
            self.poll_cursor = Some(id);
            let p = self.peers.get_mut(&id).unwrap();
            if p.attempt.as_ref().is_some_and(|a| now >= a.expires) {
                let ticket = p.attempt.take().unwrap().ticket;
                Self::backoff(p, now, self.limits);
                events.push(PeerPoll::ConnectExpired(ticket));
                continue;
            }
            if p.retiring {
                continue;
            }
            if self.fenced {
                Self::retire(p);
                Self::finish_retire(p);
                continue;
            }
            let Some(t) = &mut p.transport else {
                continue;
            };
            let valid = |t: &P| {
                let u = t.usage();
                let l = t.limits();
                t.binding() == p.binding.unwrap()
                    && t.security() == SessionSecurity::Authenticated
                    && l.validate().is_ok()
                    && l.send_frame_bytes <= self.transport_limits.send_frame_bytes
                    && l.receive_frame_bytes <= self.transport_limits.receive_frame_bytes
                    && l.decoded_bytes <= self.transport_limits.decoded_bytes
                    && u.send_frame_bytes <= l.send_frame_bytes
                    && u.receive_frame_bytes <= l.receive_frame_bytes
                    && u.decoded_bytes <= l.decoded_bytes
            };
            let result = if valid(t) {
                t.poll(now, budget)
            } else {
                Err(TransportError::ProviderViolation)
            };
            let result = if valid(t) {
                result
            } else {
                Err(TransportError::ProviderViolation)
            };
            let error = result.err();
            if error.is_some()
                || matches!(t.state(), TransportState::Closed | TransportState::Failed)
            {
                Self::retire(p);
                Self::backoff(p, now, self.limits);
                Self::finish_retire(p);
                events.push(PeerPoll::Disconnected { peer: id, error });
            } else {
                events.push(PeerPoll::Progress {
                    peer: id,
                    progress: result.unwrap(),
                });
            }
        }
        Ok(events)
    }
    pub fn take_send(
        &mut self,
        peer: NodeId,
    ) -> Result<Option<TransportSend>, PeerCompletionRejected> {
        let Some(p) = self.peers.get_mut(&peer) else {
            return Ok(None);
        };
        let Some(t) = &mut p.transport else {
            return Ok(None);
        };
        let Some(send) = t.take_send() else {
            return Ok(None);
        };
        if Some(send.connection) != p.binding
            || Some(send.batch.ticket) != p.accepted
            || Some(t.binding()) != p.binding
            || t.security() != SessionSecurity::Authenticated
        {
            self.fenced = true;
            return Err(PeerCompletionRejected {
                reason: PeerRosterError::ProviderViolation,
                send: Box::new(send),
            });
        }
        p.accepted = None;
        if send.result == LocalSendResult::Sent {
            p.failures = 0;
        }
        Self::finish_retire(p);
        Ok(Some(send))
    }
    pub fn take_received(
        &mut self,
        peer: NodeId,
    ) -> Result<Option<ReceivedBatch>, PeerReceiveRejected> {
        let Some(p) = self.peers.get_mut(&peer) else {
            return Ok(None);
        };
        if p.retiring {
            return Ok(None);
        }
        let Some(t) = &mut p.transport else {
            return Ok(None);
        };
        let Some(batch) = t.take_received() else {
            return Ok(None);
        };
        let b = p.binding.unwrap();
        let limit = self
            .transport_limits
            .decoded_bytes
            .min(t.limits().decoded_bytes);
        let mut cost = batch
            .messages
            .capacity()
            .checked_mul(std::mem::size_of::<crate::raft::Message>());
        for m in &batch.messages {
            if m.from != b.peer.node || m.sender != b.peer.store || m.to != b.local.node {
                cost = None;
                break;
            }
            cost = cost.and_then(|n| {
                message_cost(m, limit).ok().and_then(|(_, extra)| {
                    n.checked_add(extra - std::mem::size_of::<crate::raft::Message>())
                })
            });
            if cost.is_none_or(|n| n > limit) {
                break;
            }
        }
        if Some(batch.connection) != p.binding
            || t.binding() != p.binding.unwrap()
            || t.security() != SessionSecurity::Authenticated
            || self.fenced
            || batch.messages.is_empty()
            || cost.is_none_or(|n| n > limit)
        {
            self.fenced = true;
            return Err(PeerReceiveRejected {
                reason: PeerRosterError::ProviderViolation,
                batch: Box::new(batch),
            });
        }
        Ok(Some(batch))
    }
    pub fn close(&mut self) {
        self.closed = true;
        for p in self.peers.values_mut() {
            p.attempt = None;
            if let Some(t) = &mut p.transport {
                t.close();
            }
        }
    }
    /// Explicitly abandon a fenced roster's unresolved send correlation after
    /// the host has retained/resolved its queue ownership. Releases no queue
    /// credits, reports no delivery, and never makes this roster usable again.
    pub fn discard_failed_send(&mut self, ticket: SendTicket) -> Result<(), PeerRosterError> {
        if !self.fenced {
            return Err(PeerRosterError::ProviderViolation);
        }
        let p = self
            .peers
            .get_mut(&ticket.peer)
            .ok_or(PeerRosterError::UnknownPeer)?;
        if p.accepted != Some(ticket) {
            return Err(PeerRosterError::StaleConnection);
        }
        Self::retire(p);
        p.accepted = None;
        Self::finish_retire(p);
        Ok(())
    }
    pub fn abort(&mut self) {
        self.closed = true;
        for p in self.peers.values_mut() {
            p.attempt = None;
            Self::retire(p);
            Self::finish_retire(p);
        }
    }
}
