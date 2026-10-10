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
//! Discovery at the ordinary authenticated connector boundary.
use super::*;
use crate::{discovery::*, identity::NodeId};
use std::{
    collections::{BTreeMap, BTreeSet, VecDeque},
    net::SocketAddr,
};
struct Attempt {
    ticket: ConnectTicket,
    hint: Option<HintGeneration>,
    cancelled: bool,
}
struct Drive<R> {
    poll: fn(&mut R, MonoTime, SessionPollBudget) -> Result<(), DiscoveryError>,
    pending: fn(&R) -> bool,
    deadline: fn(&R) -> Option<MonoTime>,
}
struct Waiting {
    request: ConnectRequest<SocketAddr>,
    cancelled: bool,
}
struct Terminal {
    ticket: ConnectTicket,
    reason: ConnectError,
}
pub struct DiscoveryConnector<C: PeerConnector<Endpoint = SocketAddr>, R: PeerDiscovery> {
    connector: C,
    resolver: R,
    limits: ConnectLimits,
    attempts: BTreeMap<NodeId, Attempt>,
    waiting: VecDeque<Waiting>,
    terminal: Option<Terminal>,
    now: MonoTime,
    closed: bool,
    drive: Option<Drive<R>>,
    discovery_turn: bool,
}
impl<C: PeerConnector<Endpoint = SocketAddr>, R: PeerDiscovery> DiscoveryConnector<C, R> {
    pub fn new(connector: C, resolver: R, now: MonoTime) -> Result<Self, (ConnectError, C, R)> {
        let limits = connector.limits();
        let check = limits.validate().and_then(|_| {
            if connector.usage().requests != 0 || connector.usage().anonymous != 0 {
                Err(ConnectError::InvalidRequest)
            } else {
                Ok(())
            }
        });
        if let Err(error) = check {
            return Err((error, connector, resolver));
        }
        Ok(Self {
            connector,
            resolver,
            limits,
            attempts: BTreeMap::new(),
            waiting: VecDeque::new(),
            terminal: None,
            now,
            closed: false,
            drive: None,
            discovery_turn: true,
        })
    }
    /// Own resolver progress inside connector/Node polling. Transient discovery
    /// misses retain the accepted request until its original deadline. The caller
    /// must not independently consume resolver completions after handing it over.
    pub fn new_driven(
        connector: C,
        resolver: R,
        now: MonoTime,
    ) -> Result<Self, (ConnectError, C, R)>
    where
        R: DiscoveryDriver,
    {
        if resolver.discovery_pending() {
            return Err((ConnectError::InvalidRequest, connector, resolver));
        }
        let mut this = Self::new(connector, resolver, now)?;
        this.drive = Some(Drive {
            poll: R::poll_discovery,
            pending: R::discovery_pending,
            deadline: R::discovery_deadline,
        });
        Ok(this)
    }
    pub fn discovery_mut(&mut self) -> &mut R {
        &mut self.resolver
    }
    pub fn connector(&self) -> &C {
        &self.connector
    }
    pub fn into_parts(self) -> Result<(C, R), Box<Self>> {
        if !self.closed || !self.is_drained() {
            return Err(Box::new(self));
        }
        Ok((self.connector, self.resolver))
    }
    fn poll_discovery(
        &mut self,
        now: MonoTime,
        budget: &mut ConnectPollBudget,
    ) -> Result<Option<Terminal>, ConnectError> {
        let Some(drive) = &self.drive else {
            return Ok(None);
        };
        if budget.visits == 0 || (!(drive.pending)(&self.resolver) && self.waiting.is_empty()) {
            return Ok(None);
        }
        let single_visit = budget.visits == 1;
        let run = !single_visit || self.discovery_turn;
        if single_visit && budget.session.io_calls > 0 {
            self.discovery_turn = !self.discovery_turn;
        }
        if run {
            if (drive.pending)(&self.resolver) {
                (drive.poll)(&mut self.resolver, now, budget.session)
                    .map_err(ConnectError::Discovery)?;
            }
            budget.visits -= 1;
            if budget.completions > 0 {
                return Ok(self.poll_waiting(now));
            }
        }
        Ok(None)
    }
    fn poll_waiting(&mut self, now: MonoTime) -> Option<Terminal> {
        let waiting = self.waiting.pop_front()?;
        let reason = if waiting.cancelled || self.closed {
            Some(ConnectError::Cancelled)
        } else if now >= waiting.request.deadline {
            Some(ConnectError::Timeout)
        } else {
            None
        };
        if let Some(reason) = reason {
            return Some(Terminal {
                ticket: waiting.request.ticket,
                reason,
            });
        }
        let hint = self
            .resolver
            .resolve(waiting.request.ticket.peer, now)
            .and_then(|h| h.validate(waiting.request.ticket.peer, now));
        let result = match hint {
            Ok(hint) => self.submit_resolved(waiting.request, Some(hint), now),
            Err(error) if error.retryable() => {
                self.waiting.push_back(waiting);
                return None;
            }
            Err(error) => {
                return Some(Terminal {
                    ticket: waiting.request.ticket,
                    reason: ConnectError::Discovery(error),
                })
            }
        };
        match result {
            Ok(()) => None,
            Err(rejected) if rejected.reason == ConnectError::Overloaded => {
                self.waiting.push_back(Waiting {
                    request: *rejected.request,
                    cancelled: false,
                });
                None
            }
            Err(rejected) => Some(Terminal {
                ticket: rejected.request.ticket,
                reason: rejected.reason,
            }),
        }
    }
    fn admission(
        &self,
        request: &ConnectRequest<SocketAddr>,
        now: MonoTime,
    ) -> Result<(), ConnectError> {
        if self.closed {
            return Err(ConnectError::Closed);
        }
        if now < self.now {
            return Err(ConnectError::TimeWentBack);
        }
        if request.ticket.local != self.connector.local() {
            return Err(ConnectError::WrongBinding);
        }
        if !self.connector.supports_peer(request.ticket.peer) {
            return Err(ConnectError::UnknownPeer);
        }
        if request.deadline <= now || request.deadline.0 - now.0 > self.limits.timeout_ms {
            return Err(ConnectError::InvalidRequest);
        }
        if self.attempts.len() + self.waiting.len() + usize::from(self.terminal.is_some())
            >= self.limits.requests
            || self.attempts.contains_key(&request.ticket.peer.node)
            || self
                .waiting
                .iter()
                .any(|w| w.request.ticket.peer.node == request.ticket.peer.node)
            || self
                .terminal
                .as_ref()
                .is_some_and(|t| t.ticket.peer.node == request.ticket.peer.node)
        {
            return Err(ConnectError::Overloaded);
        }
        Ok(())
    }
    fn submit_resolved(
        &mut self,
        request: ConnectRequest<SocketAddr>,
        hint: Option<PeerEndpointHint>,
        now: MonoTime,
    ) -> Result<(), ConnectRejected<SocketAddr>> {
        let ticket = request.ticket;
        let original_direction = request.direction.clone();
        let direction = hint.map_or(ConnectDirection::Accept, |h| {
            ConnectDirection::Dial(h.endpoint)
        });
        let resolved = ConnectRequest {
            ticket,
            direction,
            deadline: request.deadline,
        };
        if let Err(mut rejected) = self.connector.submit(resolved, now) {
            rejected.request.direction = original_direction;
            return Err(rejected);
        }
        self.attempts.insert(
            ticket.peer.node,
            Attempt {
                ticket,
                hint: hint.map(|h| h.generation),
                cancelled: false,
            },
        );
        self.now = now;
        Ok(())
    }
    fn cancel_submitted(&mut self, ticket: ConnectTicket) -> bool {
        let Some(attempt) = self
            .attempts
            .get_mut(&ticket.peer.node)
            .filter(|a| a.ticket == ticket)
        else {
            return false;
        };
        if !self.connector.cancel(ticket) {
            return false;
        }
        attempt.cancelled = true;
        true
    }
    fn poll_submitted(
        &mut self,
        now: MonoTime,
        budget: ConnectPollBudget,
    ) -> Result<Vec<ConnectCompletion<C::Session>>, ConnectError> {
        let events = self.connector.poll(now, budget)?;
        if events.len() > budget.completions {
            self.close();
            return Err(ConnectError::ProviderViolation);
        }
        let mut seen = BTreeSet::new();
        for event in &events {
            if !seen.insert(event.ticket.peer.node)
                || self
                    .attempts
                    .get(&event.ticket.peer.node)
                    .is_none_or(|a| a.ticket != event.ticket)
            {
                self.close();
                return Err(ConnectError::ProviderViolation);
            }
        }
        for event in &events {
            let attempt = self.attempts.remove(&event.ticket.peer.node).unwrap();
            if event.result.is_err() && !attempt.cancelled {
                if let Some(generation) = attempt.hint {
                    self.resolver.invalidate(event.ticket.peer, generation);
                }
            }
        }
        Ok(events)
    }
}
impl<C: PeerConnector<Endpoint = SocketAddr>, R: PeerDiscovery> PeerConnector
    for DiscoveryConnector<C, R>
{
    type Endpoint = SocketAddr;
    type Session = C::Session;
    fn local(&self) -> LocalIdentity {
        self.connector.local()
    }
    fn supports_peer(&self, peer: PeerIdentity) -> bool {
        self.connector.supports_peer(peer)
    }
    fn limits(&self) -> ConnectLimits {
        self.limits
    }
    fn usage(&self) -> ConnectUsage {
        let mut usage = self.connector.usage();
        usage.requests += self.waiting.len() + usize::from(self.terminal.is_some());
        usage
    }
    fn next_deadline(&self) -> Option<MonoTime> {
        [
            self.connector.next_deadline(),
            self.drive
                .as_ref()
                .filter(|d| (d.pending)(&self.resolver) || !self.waiting.is_empty())
                .and_then(|d| (d.deadline)(&self.resolver)),
            self.waiting
                .iter()
                .map(|w| {
                    if self.closed || w.cancelled {
                        self.now
                    } else {
                        w.request.deadline
                    }
                })
                .min(),
            self.terminal.as_ref().map(|_| self.now),
        ]
        .into_iter()
        .flatten()
        .min()
    }
    fn submit(
        &mut self,
        request: ConnectRequest<SocketAddr>,
        now: MonoTime,
    ) -> Result<(), ConnectRejected<SocketAddr>> {
        let hint = self.admission(&request, now).and_then(|_| {
            if matches!(request.direction, ConnectDirection::Accept) {
                return Ok(None);
            }
            self.resolver
                .resolve(request.ticket.peer, now)
                .and_then(|h| h.validate(request.ticket.peer, now))
                .map(Some)
                .map_err(ConnectError::Discovery)
        });
        let hint = match hint {
            Ok(hint) => hint,
            Err(reason) => {
                if self.drive.is_some()
                    && matches!(reason, ConnectError::Discovery(e) if e.retryable())
                {
                    self.waiting.push_back(Waiting {
                        request,
                        cancelled: false,
                    });
                    self.now = now;
                    return Ok(());
                }
                return Err(ConnectRejected {
                    reason,
                    request: Box::new(request),
                });
            }
        };
        self.submit_resolved(request, hint, now)
    }
    fn cancel(&mut self, ticket: ConnectTicket) -> bool {
        if let Some(waiting) = self.waiting.iter_mut().find(|w| w.request.ticket == ticket) {
            waiting.cancelled = true;
            return true;
        }
        self.cancel_submitted(ticket)
    }
    fn poll(
        &mut self,
        now: MonoTime,
        mut budget: ConnectPollBudget,
    ) -> Result<Vec<ConnectCompletion<C::Session>>, ConnectError> {
        budget.validate()?;
        if now < self.now {
            return Err(ConnectError::TimeWentBack);
        }
        self.now = now;
        if self.terminal.is_none() {
            self.terminal = self.poll_discovery(now, &mut budget)?;
        }
        let deliver = self.terminal.is_some() && budget.completions > 0;
        budget.completions -= usize::from(deliver);
        let mut events = self.poll_submitted(now, budget)?;
        if deliver {
            let terminal = self.terminal.take().unwrap();
            events.push(ConnectCompletion {
                ticket: terminal.ticket,
                result: Err(terminal.reason),
            });
        }
        Ok(events)
    }
    fn is_drained(&self) -> bool {
        self.waiting.is_empty()
            && self.terminal.is_none()
            && self.attempts.is_empty()
            && self.connector.is_drained()
            && self
                .drive
                .as_ref()
                .is_none_or(|d| !(d.pending)(&self.resolver))
    }
    fn close(&mut self) {
        self.closed = true;
        for attempt in self.attempts.values_mut() {
            attempt.cancelled = true;
        }
        self.resolver.close();
        self.connector.close();
    }
}

impl<C: PeerCredentialControl<Endpoint = SocketAddr>, R: PeerDiscovery> PeerCredentialControl
    for DiscoveryConnector<C, R>
{
    type Credentials = C::Credentials;
    fn credential_generation(&self) -> Option<crate::authorization::CredentialGeneration> {
        self.connector.credential_generation()
    }
    fn replace_peer_credentials(
        &mut self,
        expected: crate::authorization::CredentialGeneration,
        replacement: crate::authorization::CredentialGeneration,
        material: Self::Credentials,
    ) -> Result<Self::Credentials, (ConnectError, Self::Credentials)> {
        if self.closed {
            return Err((ConnectError::Closed, material));
        }
        self.connector
            .replace_peer_credentials(expected, replacement, material)
    }
}
