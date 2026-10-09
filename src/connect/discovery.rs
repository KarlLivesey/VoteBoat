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
    collections::{BTreeMap, BTreeSet},
    net::SocketAddr,
};
struct Attempt {
    ticket: ConnectTicket,
    hint: Option<HintGeneration>,
    cancelled: bool,
}
pub struct DiscoveryConnector<C: PeerConnector<Endpoint = SocketAddr>, R: PeerDiscovery> {
    connector: C,
    resolver: R,
    limits: ConnectLimits,
    attempts: BTreeMap<NodeId, Attempt>,
    now: MonoTime,
    closed: bool,
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
            now,
            closed: false,
        })
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
        if self.attempts.len() >= self.limits.requests
            || self.attempts.contains_key(&request.ticket.peer.node)
        {
            return Err(ConnectError::Overloaded);
        }
        Ok(())
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
        self.connector.usage()
    }
    fn next_deadline(&self) -> Option<MonoTime> {
        self.connector.next_deadline()
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
            let hint = self
                .resolver
                .resolve(request.ticket.peer, now)
                .map_err(ConnectError::Discovery)?;
            hint.validate(request.ticket.peer, now)
                .map(Some)
                .map_err(ConnectError::Discovery)
        });
        let hint = match hint {
            Ok(hint) => hint,
            Err(reason) => {
                return Err(ConnectRejected {
                    reason,
                    request: Box::new(request),
                })
            }
        };
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
    fn cancel(&mut self, ticket: ConnectTicket) -> bool {
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
    fn poll(
        &mut self,
        now: MonoTime,
        budget: ConnectPollBudget,
    ) -> Result<Vec<ConnectCompletion<C::Session>>, ConnectError> {
        budget.validate()?;
        if now < self.now {
            return Err(ConnectError::TimeWentBack);
        }
        self.now = now;
        let events = self.connector.poll(now, budget)?;
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
    fn is_drained(&self) -> bool {
        self.attempts.is_empty() && self.connector.is_drained()
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
