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
//! Bounded peer reactor over explicitly owned connector, roster and ingress.
use super::*;
use crate::{connect::*, outbound::*, secure::*, transport::*};

pub struct PeerParts<C: PeerConnector, F: PeerTransportFactory<C::Session>> {
    pub connector: C,
    pub factory: F,
    pub roster: PeerRoster<F::Transport>,
    pub ingress: IngressRouter,
    /// Fixed address hints/directions for exactly the authorized roster peers.
    /// These cannot authorize membership, stores, sessions or application service.
    pub routes: BTreeMap<NodeId, ConnectDirection<C::Endpoint>>,
}
#[derive(Clone, Copy, Debug)]
pub struct PeerDriverLimits {
    pub staged_batches: usize,
    pub metadata_bytes: usize,
}
impl Default for PeerDriverLimits {
    fn default() -> Self {
        Self {
            staged_batches: 4096,
            metadata_bytes: 1024 * 1024,
        }
    }
}
#[derive(Clone, Copy, Debug)]
pub struct PeerDriverBudget {
    pub connection_visits: usize,
    pub connector: ConnectPollBudget,
    pub peer_visits: usize,
    pub transport: TransportPollBudget,
    pub sends: usize,
    pub ingress: usize,
}
impl Default for PeerDriverBudget {
    fn default() -> Self {
        Self {
            connection_visits: 16,
            connector: ConnectPollBudget::default(),
            peer_visits: 16,
            transport: TransportPollBudget::default(),
            sends: 32,
            ingress: 128,
        }
    }
}
impl PeerDriverBudget {
    fn validate(self) -> Result<(), PeerDriverError> {
        if self.connection_visits > 4096
            || self.peer_visits > 4096
            || self.sends > 4096
            || self.ingress > 65536
        {
            return Err(PeerDriverError::InvalidLimits);
        }
        self.connector
            .validate()
            .map_err(PeerDriverError::Connect)?;
        self.transport
            .validate()
            .map_err(PeerDriverError::Transport)?;
        Ok(())
    }
}
#[derive(Clone, Debug, Eq, PartialEq)]
pub enum PeerDriverError {
    InvalidLimits,
    WrongBinding,
    NotQuiescent,
    TimeWentBack,
    Fenced,
    ProviderViolation,
    Connect(ConnectError),
    Roster(PeerRosterError),
    Transport(TransportError),
    Outbound(OutboundError),
    Ingress(IngressError),
}
pub struct PeerDriverRejected<C: PeerConnector, F: PeerTransportFactory<C::Session>> {
    pub reason: PeerDriverError,
    pub parts: Box<PeerParts<C, F>>,
}
#[derive(Clone, Copy, Debug, Default, Eq, PartialEq)]
pub struct PeerDriverUsage {
    pub attempts: usize,
    pub staged_batches: usize,
    pub quarantined_batches: usize,
    pub failed_send: bool,
    pub failed_receive: bool,
}
#[derive(Debug, Default)]
pub struct PeerDriverProgress {
    pub connection_submissions: usize,
    pub connections: usize,
    pub connection_failures: usize,
    pub obsolete_connections: usize,
    pub sends: usize,
    pub completions: Vec<LocalSendCompletion>,
    pub received: usize,
    pub ingress_blocked: usize,
    pub ingress: IngressProgress,
    pub peers: Vec<PeerPoll>,
}
/// Explicit recovery handoff. Original queue payloads remain charged until the
/// host resolves them against their exact queue; no field establishes delivery.
/// A violating provider may already have returned an oversized allocation, which
/// is quarantined whole after fencing, never admitted as normally bounded work.
pub struct PeerDriverRecovery<C: PeerConnector, F: PeerTransportFactory<C::Session>> {
    pub parts: PeerParts<C, F>,
    pub attempts: Vec<ConnectTicket>,
    pub staged: VecDeque<OutboundBatch>,
    pub quarantined: Vec<OutboundBatch>,
    pub failed_send: Option<Box<TransportSend>>,
    pub failed_receive: Option<Box<ReceivedBatch>>,
}
/// Single serialized peer reactor. All networking components are selected and
/// transferred explicitly at construction; the core and outbound queue stay
/// borrowed. No listener, clock, executor, thread or fallback is created here.
pub struct PeerDriver<C: PeerConnector, F: PeerTransportFactory<C::Session>> {
    parts: PeerParts<C, F>,
    owner: RuntimeOwner,
    outbound: OutboundBinding,
    limits: PeerDriverLimits,
    connector_limits: ConnectLimits,
    peers: Vec<NodeId>,
    cursor: usize,
    attempts: Vec<ConnectTicket>,
    staged: VecDeque<OutboundBatch>,
    quarantine: Vec<OutboundBatch>,
    failed_send: Option<Box<TransportSend>>,
    failed_receive: Option<Box<ReceivedBatch>>,
    now: MonoTime,
    closed: bool,
    failed: Option<PeerDriverError>,
}
impl<C: PeerConnector, F: PeerTransportFactory<C::Session>> PeerDriver<C, F> {
    pub fn new<O: OutboundQueue>(
        parts: PeerParts<C, F>,
        owner: RuntimeOwner,
        outbound: &O,
        limits: PeerDriverLimits,
        now: MonoTime,
    ) -> Result<Self, PeerDriverRejected<C, F>> {
        let check = (|| {
            let local = parts.roster.local();
            if local.store != owner.store
                || local.node != outbound.binding().node
                || parts.connector.local() != local
                || parts.roster.outbound_binding() != outbound.binding()
                || parts.ingress.binding().owner != owner
                || parts.ingress.binding().local != local
            {
                return Err(PeerDriverError::WrongBinding);
            }
            parts
                .connector
                .limits()
                .validate()
                .map_err(PeerDriverError::Connect)?;
            outbound
                .limits()
                .validate()
                .map_err(PeerDriverError::Outbound)?;
            let peers = parts.roster.authorized_peers().collect::<Vec<_>>();
            if peers.len() != parts.routes.len()
                || peers.iter().any(|p| !parts.routes.contains_key(p))
            {
                return Err(PeerDriverError::WrongBinding);
            }
            if parts.roster.limits().connect_timeout_ms > parts.connector.limits().timeout_ms {
                return Err(PeerDriverError::InvalidLimits);
            }
            let metadata = peers
                .len()
                .checked_mul(size_of::<NodeId>())
                .and_then(|n| {
                    n.checked_add(parts.connector.limits().requests * size_of::<ConnectTicket>())
                })
                .and_then(|n| {
                    n.checked_add(
                        limits
                            .staged_batches
                            .checked_mul(size_of::<OutboundBatch>())?,
                    )
                });
            if limits.staged_batches < outbound.limits().node.max.batches
                || limits.staged_batches > 65536
                || limits.metadata_bytes > 64 * 1024 * 1024
                || metadata.is_none_or(|n| n > limits.metadata_bytes)
            {
                return Err(PeerDriverError::InvalidLimits);
            }
            if parts.connector.usage().requests != 0
                || parts.connector.usage().anonymous != 0
                || parts.roster.usage().connections != 0
                || parts.roster.is_fenced()
                || !parts.ingress.is_drained()
                || !outbound.is_drained()
            {
                return Err(PeerDriverError::NotQuiescent);
            }
            Ok(peers)
        })();
        let peers = match check {
            Ok(peers) => peers,
            Err(reason) => {
                return Err(PeerDriverRejected {
                    reason,
                    parts: Box::new(parts),
                })
            }
        };
        let attempts = Vec::with_capacity(parts.connector.limits().requests);
        let connector_limits = parts.connector.limits();
        Ok(Self {
            parts,
            owner,
            outbound: outbound.binding(),
            limits,
            connector_limits,
            peers,
            cursor: 0,
            attempts,
            staged: VecDeque::with_capacity(limits.staged_batches),
            quarantine: Vec::new(),
            failed_send: None,
            failed_receive: None,
            now,
            closed: false,
            failed: None,
        })
    }
    pub fn usage(&self) -> PeerDriverUsage {
        PeerDriverUsage {
            attempts: self.attempts.len(),
            staged_batches: self.staged.len(),
            quarantined_batches: self.quarantine.len(),
            failed_send: self.failed_send.is_some(),
            failed_receive: self.failed_receive.is_some(),
        }
    }
    pub fn limits(&self) -> PeerDriverLimits {
        self.limits
    }
    pub fn roster(&self) -> &PeerRoster<F::Transport> {
        &self.parts.roster
    }
    pub fn ingress(&self) -> &IngressRouter {
        &self.parts.ingress
    }
    pub fn connector_usage(&self) -> ConnectUsage {
        self.parts.connector.usage()
    }
    pub fn is_failed(&self) -> bool {
        self.failed.is_some()
    }
    pub fn next_deadline(&self) -> Option<MonoTime> {
        if self.failed.is_some() {
            return None;
        }
        [
            self.parts.connector.next_deadline(),
            self.parts
                .roster
                .next_deadline_filtered(|peer| !self.attempts.iter().any(|t| t.peer.node == peer)),
        ]
        .into_iter()
        .flatten()
        .min()
    }
    pub fn is_drained(&self) -> bool {
        self.closed
            && self.attempts.is_empty()
            && self.staged.is_empty()
            && self.quarantine.is_empty()
            && self.failed_send.is_none()
            && self.failed_receive.is_none()
            && self.parts.connector.is_drained()
            && self.parts.roster.is_drained()
            && self.parts.ingress.is_drained()
    }
    /// Cancel the exact attempt before retiring this peer. Provider slots remain
    /// charged until terminal polling, and block replacement admission meanwhile.
    pub fn disconnect(&mut self, peer: NodeId, now: MonoTime) -> Result<(), PeerDriverError> {
        if now < self.now {
            return Err(PeerDriverError::TimeWentBack);
        }
        if self.failed.is_some() {
            return Err(PeerDriverError::Fenced);
        }
        self.now = now;
        if let Some(ticket) = self.attempts.iter().find(|t| t.peer.node == peer) {
            self.parts.connector.cancel(*ticket);
        }
        self.parts
            .roster
            .disconnect(peer, now)
            .map_err(PeerDriverError::Roster)
    }
    pub fn poll<O: OutboundQueue, Q: ReadyScheduler, T: TimerService, E: ElectionEntropy>(
        &mut self,
        owner: &mut EffectOwner<Q, T, E>,
        outbound: &mut O,
        now: MonoTime,
        budget: PeerDriverBudget,
    ) -> Result<PeerDriverProgress, PeerDriverError>
    where
        C::Endpoint: Clone,
    {
        if owner.identity() != self.owner || outbound.binding() != self.outbound {
            return Err(PeerDriverError::WrongBinding);
        }
        budget.validate()?;
        if now < self.now {
            return Err(PeerDriverError::TimeWentBack);
        }
        if self.failed.is_some() || owner.is_failed() {
            return Err(PeerDriverError::Fenced);
        }
        self.now = now;
        let result = self.poll_inner(outbound, now, budget).and_then(|mut out| {
            out.ingress = self
                .parts
                .ingress
                .dispatch(&self.parts.roster, owner, budget.ingress)
                .map_err(PeerDriverError::Ingress)?;
            Ok(out)
        });
        if let Err(reason) = &result {
            let _ = owner.fail::<()>(EffectOwnerError::ProviderContract);
            self.fail_network(reason);
        }
        result
    }
    /// After close, resolve network ownership without accessing a failed core.
    /// Held ingress is canceled explicitly, never admitted or executed.
    pub fn drain<O: OutboundQueue>(
        &mut self,
        outbound: &mut O,
        now: MonoTime,
        budget: PeerDriverBudget,
    ) -> Result<PeerDriverProgress, PeerDriverError>
    where
        C::Endpoint: Clone,
    {
        if outbound.binding() != self.outbound {
            return Err(PeerDriverError::WrongBinding);
        }
        budget.validate()?;
        if now < self.now {
            return Err(PeerDriverError::TimeWentBack);
        }
        if self.failed.is_some() {
            return Err(PeerDriverError::Fenced);
        }
        if !self.closed {
            return Err(PeerDriverError::NotQuiescent);
        }
        self.now = now;
        let result = self.poll_inner(outbound, now, budget).map(|mut out| {
            out.ingress.completed = self.parts.ingress.abort();
            out.ingress.discarded = out.ingress.completed.iter().map(|c| c.discarded).sum();
            out
        });
        if let Err(reason) = &result {
            self.fail_network(reason);
        }
        result
    }
    fn fail_network(&mut self, reason: &PeerDriverError) {
        self.failed = Some(reason.clone());
        self.closed = true;
        self.parts.connector.close();
        self.parts.roster.abort();
        self.parts.ingress.close();
    }
    fn poll_inner<O: OutboundQueue>(
        &mut self,
        outbound: &mut O,
        now: MonoTime,
        b: PeerDriverBudget,
    ) -> Result<PeerDriverProgress, PeerDriverError>
    where
        C::Endpoint: Clone,
    {
        self.check_connector()?;
        if outbound.limits().validate().is_err()
            || outbound.limits().node.max.batches > self.limits.staged_batches
            || outbound.usage().batches > self.limits.staged_batches
        {
            return Err(PeerDriverError::ProviderViolation);
        }
        let peers = self
            .parts
            .roster
            .poll(now, b.peer_visits, b.transport)
            .map_err(PeerDriverError::Roster)?;
        let mut out = PeerDriverProgress {
            peers,
            ..Default::default()
        };
        for event in &out.peers {
            if let PeerPoll::ConnectExpired(ticket) = event {
                self.parts.connector.cancel(*ticket);
            }
            if matches!(
                event,
                PeerPoll::Disconnected {
                    error: Some(TransportError::ProviderViolation),
                    ..
                }
            ) {
                return Err(PeerDriverError::ProviderViolation);
            }
        }
        let events = self
            .parts
            .connector
            .poll(now, b.connector)
            .map_err(PeerDriverError::Connect)?;
        if events.len() > b.connector.completions {
            return Err(PeerDriverError::ProviderViolation);
        }
        self.check_connector()?;
        for event in events {
            let index = self
                .attempts
                .iter()
                .position(|t| *t == event.ticket)
                .ok_or(PeerDriverError::ProviderViolation)?;
            self.attempts.swap_remove(index);
            if self.closed
                || self
                    .parts
                    .roster
                    .attempt_deadline(event.ticket)
                    .is_none_or(|d| now >= d)
            {
                // Dropping a late returned session closes its independently owned channel.
                out.obsolete_connections += 1;
                continue;
            }
            match event.result {
                Err(_) => {
                    self.parts
                        .roster
                        .connect_failed(event.ticket, now)
                        .map_err(PeerDriverError::Roster)?;
                    out.connection_failures += 1;
                }
                Ok(session) => {
                    let binding = require_authenticated(&session)
                        .map_err(|_| PeerDriverError::ProviderViolation)?;
                    if binding.local != event.ticket.local
                        || binding.peer.node != event.ticket.peer.node
                        || binding.peer.store.identity != event.ticket.peer.store
                        || binding.generation != event.ticket.generation
                        || binding.wire_version != self.parts.roster.wire_version()
                    {
                        return Err(PeerDriverError::ProviderViolation);
                    }
                    match self.parts.factory.build(session, outbound) {
                        Ok(transport) => {
                            if let Err(mut rejected) =
                                self.parts.roster.attach(event.ticket, transport, now)
                            {
                                rejected.transport.abort();
                                return Err(PeerDriverError::Roster(rejected.reason));
                            }
                            out.connections += 1;
                        }
                        Err(reason) => return Err(PeerDriverError::Transport(reason)),
                    }
                }
            }
        }
        if !self.closed {
            let active = &self.attempts;
            let mut slots = self
                .parts
                .connector
                .limits()
                .requests
                .saturating_sub(active.len());
            let tickets = self
                .parts
                .roster
                .due_connections_filtered(now, b.connection_visits, |peer| {
                    if slots == 0 || active.iter().any(|t| t.peer.node == peer) {
                        false
                    } else {
                        slots -= 1;
                        true
                    }
                })
                .map_err(PeerDriverError::Roster)?;
            for ticket in tickets {
                let request = ConnectRequest {
                    ticket,
                    direction: self.parts.routes[&ticket.peer.node].clone(),
                    deadline: self.parts.roster.attempt_deadline(ticket).unwrap(),
                };
                match self.parts.connector.submit(request, now) {
                    Ok(()) => {
                        self.attempts.push(ticket);
                        out.connection_submissions += 1;
                    }
                    Err(rejected) => {
                        if rejected.request.ticket != ticket {
                            return Err(PeerDriverError::ProviderViolation);
                        }
                        self.parts
                            .roster
                            .connect_failed(ticket, now)
                            .map_err(PeerDriverError::Roster)?;
                        if rejected.reason != ConnectError::Overloaded {
                            return Err(PeerDriverError::Connect(rejected.reason));
                        }
                        out.connection_failures += 1;
                    }
                }
                self.check_connector()?;
            }
        }
        // Complete transport-owned output before admitting more outbound work.
        for _ in 0..b.peer_visits.min(self.peers.len()) {
            let peer = self.peers[self.cursor];
            self.cursor = (self.cursor + 1) % self.peers.len();
            match self.parts.roster.take_send(peer) {
                Ok(Some(send)) => {
                    let ticket = send.batch.ticket;
                    match outbound.complete(send.batch, send.result) {
                        Ok(done) => {
                            if done.ticket != ticket || done.result != send.result {
                                return Err(PeerDriverError::ProviderViolation);
                            }
                            out.completions.push(done);
                        }
                        Err(rejected) => {
                            self.failed_send = Some(Box::new(TransportSend {
                                connection: send.connection,
                                batch: *rejected.batch,
                                result: send.result,
                            }));
                            return Err(PeerDriverError::Outbound(rejected.reason));
                        }
                    }
                }
                Ok(None) => (),
                Err(rejected) => {
                    self.failed_send = Some(rejected.send);
                    return Err(PeerDriverError::Roster(rejected.reason));
                }
            }
            if !self.closed {
                match self.parts.ingress.receive(&mut self.parts.roster, peer) {
                    Ok(Some(_)) => out.received += 1,
                    Ok(None) => (),
                    Err(rejected)
                        if rejected.reason == IngressError::Overloaded
                            && rejected.batch.is_none() =>
                    {
                        out.ingress_blocked += 1
                    }
                    Err(rejected) => {
                        self.failed_receive = rejected.batch;
                        return Err(PeerDriverError::Ingress(rejected.reason));
                    }
                }
            }
        }
        for _ in 0..b.sends {
            if self.staged.len() == self.limits.staged_batches {
                break;
            }
            let mut batches = outbound.poll(1);
            if batches.len() > 1 {
                self.quarantine = batches;
                return Err(PeerDriverError::ProviderViolation);
            }
            let Some(batch) = batches.pop() else {
                break;
            };
            self.staged.push_back(batch);
        }
        for _ in 0..self.staged.len().min(b.sends) {
            let batch = self.staged.pop_front().unwrap();
            if self.closed {
                let ticket = batch.ticket;
                match outbound.complete(batch, LocalSendResult::Failed) {
                    Ok(done) => {
                        if done.ticket != ticket || done.result != LocalSendResult::Failed {
                            return Err(PeerDriverError::ProviderViolation);
                        }
                        out.completions.push(done);
                    }
                    Err(rejected) => {
                        self.staged.push_front(*rejected.batch);
                        return Err(PeerDriverError::Outbound(rejected.reason));
                    }
                }
            } else {
                match self.parts.roster.submit(batch) {
                    Ok(()) => out.sends += 1,
                    Err(rejected) => {
                        self.staged.push_back(*rejected.batch);
                        if !matches!(
                            rejected.reason,
                            PeerRosterError::Overloaded
                                | PeerRosterError::Transport(TransportError::Overloaded)
                        ) {
                            return Err(PeerDriverError::Roster(rejected.reason));
                        }
                    }
                }
            }
        }
        Ok(out)
    }
    fn check_connector(&self) -> Result<(), PeerDriverError> {
        let u = self.parts.connector.usage();
        if self.parts.connector.limits() != self.connector_limits
            || u.requests > self.connector_limits.requests
            || u.anonymous > self.connector_limits.anonymous
            || u.dialing > u.requests
            || u.handshaking > u.requests - u.dialing
        {
            return Err(PeerDriverError::ProviderViolation);
        }
        Ok(())
    }
    /// Stop connection/receive admission, drain accepted transport sends, and
    /// resolve retained unsent batches as Failed (unknown remote delivery).
    /// Host-owned outbound queues and serialized owners remain open.
    pub fn close(&mut self) {
        self.closed = true;
        self.parts.connector.close();
        self.parts.roster.close();
        self.parts.ingress.close();
    }
    pub fn into_parts(self) -> Result<PeerParts<C, F>, Box<Self>> {
        if self.is_drained() {
            Ok(self.parts)
        } else {
            Err(Box::new(self))
        }
    }
    /// Consumes a failed driver without claiming provider drain or releasing
    /// queue credits. The host receives every still-observed payload and scope.
    pub fn into_recovery(self) -> Result<PeerDriverRecovery<C, F>, Box<Self>> {
        if self.failed.is_none() {
            return Err(Box::new(self));
        }
        Ok(PeerDriverRecovery {
            parts: self.parts,
            attempts: self.attempts,
            staged: self.staged,
            quarantined: self.quarantine,
            failed_send: self.failed_send,
            failed_receive: self.failed_receive,
        })
    }
}
