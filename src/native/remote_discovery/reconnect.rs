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
use super::NativeRemotePeerDiscovery;
use crate::{
    connect::*, discovery::*, identity::SecureSessionGeneration, runtime::MonoTime, secure::*,
    transport::ConnectTicket,
};
use std::net::SocketAddr;

#[derive(Clone, Copy, Debug)]
pub struct SourceReconnectConfig {
    /// Explicit numeric source address; this connector must not resolve itself.
    pub endpoint: SocketAddr,
    /// Dedicated host-reserved range in the same recovered local store session.
    pub first_generation: SecureSessionGeneration,
    /// Inclusive bound. Exhaustion stops retries; the host chooses a new range
    /// on reconstruction, without reusing any generation in this store session.
    pub last_generation: SecureSessionGeneration,
    /// Delay between failed attempts, from 1 through 60,000 milliseconds.
    pub retry_ms: u64,
}
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum SourceReconnectStatus {
    Ready,
    Backoff {
        retry_at: MonoTime,
    },
    Connecting {
        ticket: ConnectTicket,
        deadline: MonoTime,
    },
    Exhausted,
    Closed,
}
/// Construction failure returns both unmodified owners without closing either.
pub struct SourceReconnectRejected<C: PeerConnector> {
    pub reason: DiscoveryError,
    pub remote: NativeRemotePeerDiscovery<C::Session>,
    pub connector: C,
}
struct Pending {
    ticket: ConnectTicket,
    deadline: MonoTime,
    cancelled: bool,
}
/// Owns one authenticated discovery view and one dedicated source connector.
/// Cache floors survive session replacement; restart still requires host input.
/// All progress occurs through the existing DiscoveryDriver budget and owner.
/// The connector is dedicated to the provisioned source; it owns no peer-data
/// traffic. Even rejected submissions consume a reserved generation. Malformed
/// provider receipts stop reconnects without releasing unidentified work.
pub struct ReconnectingPeerDiscovery<C: PeerConnector<Endpoint = SocketAddr>> {
    remote: NativeRemotePeerDiscovery<C::Session>,
    connector: C,
    limits: ConnectLimits,
    config: SourceReconnectConfig,
    next: Option<SecureSessionGeneration>,
    retry_at: MonoTime,
    pending: Option<Pending>,
    last_error: Option<ConnectError>,
    now: MonoTime,
    closed: bool,
    broken: bool,
}
impl<C: PeerConnector<Endpoint = SocketAddr>> ReconnectingPeerDiscovery<C> {
    pub fn new(
        remote: NativeRemotePeerDiscovery<C::Session>,
        connector: C,
        config: SourceReconnectConfig,
        now: MonoTime,
    ) -> Result<Self, Box<SourceReconnectRejected<C>>> {
        let (binding, remote_now) = remote.reconnect_binding();
        let source = PeerIdentity {
            node: binding.peer.node,
            store: binding.peer.store.identity,
        };
        let check = (|| {
            if remote.is_closed() {
                return Err(DiscoveryError::Closed);
            }
            connector
                .limits()
                .validate()
                .map_err(|_| DiscoveryError::InvalidLimits)?;
            if config.first_generation <= binding.generation
                || config.last_generation < config.first_generation
                || config.retry_ms == 0
                || config.retry_ms > 60_000
                || config.endpoint.port() == 0
                || config.endpoint.ip().is_unspecified()
                || config.endpoint.ip().is_multicast()
            {
                return Err(DiscoveryError::InvalidLimits);
            }
            if now < remote_now {
                return Err(DiscoveryError::TimeWentBack);
            }
            if connector.local() != binding.local || !connector.supports_peer(source) {
                return Err(DiscoveryError::WrongBinding);
            }
            if remote.pending().is_some()
                || connector.usage().requests != 0
                || connector.usage().anonymous != 0
                || !connector.is_drained()
            {
                return Err(DiscoveryError::Unavailable);
            }
            Ok(())
        })();
        if let Err(error) = check {
            return Err(Box::new(SourceReconnectRejected {
                reason: error,
                remote,
                connector,
            }));
        }
        let limits = connector.limits();
        Ok(Self {
            remote,
            connector,
            limits,
            config,
            next: Some(config.first_generation),
            retry_at: now,
            pending: None,
            last_error: None,
            now,
            closed: false,
            broken: false,
        })
    }
    pub fn source(&self) -> &NativeRemotePeerDiscovery<C::Session> {
        &self.remote
    }
    pub fn last_connect_error(&self) -> Option<ConnectError> {
        self.last_error
    }
    pub fn status(&self) -> SourceReconnectStatus {
        if self.closed {
            return SourceReconnectStatus::Closed;
        }
        if self.broken {
            return SourceReconnectStatus::Exhausted;
        }
        if let Some(p) = &self.pending {
            return SourceReconnectStatus::Connecting {
                ticket: p.ticket,
                deadline: p.deadline,
            };
        }
        if self.remote.source_failed() && self.next.is_none() {
            return SourceReconnectStatus::Exhausted;
        }
        if self.remote.source_failed() {
            SourceReconnectStatus::Backoff {
                retry_at: self.retry_at,
            }
        } else {
            SourceReconnectStatus::Ready
        }
    }
    pub fn is_drained(&self) -> bool {
        self.pending.is_none() && self.remote.pending().is_none() && self.connector.is_drained()
    }
    pub fn into_parts(self) -> Result<(NativeRemotePeerDiscovery<C::Session>, C), Box<Self>> {
        if !self.closed || !self.is_drained() {
            return Err(Box::new(self));
        }
        Ok((self.remote, self.connector))
    }
    fn time(&mut self, now: MonoTime) -> Result<(), DiscoveryError> {
        if now < self.now {
            return Err(DiscoveryError::TimeWentBack);
        }
        self.now = now;
        Ok(())
    }
    fn retry(&mut self, error: ConnectError) {
        self.last_error = Some(error);
        self.retry_at = MonoTime(self.now.0.saturating_add(self.config.retry_ms));
    }
    fn poison(&mut self) -> DiscoveryError {
        self.broken = true;
        self.last_error = Some(ConnectError::ProviderViolation);
        self.connector.close();
        DiscoveryError::WrongBinding
    }
    fn start(&mut self) -> Result<(), DiscoveryError> {
        self.remote.retire_failed_session();
        let Some(generation) = self.next else {
            return Ok(());
        };
        self.next = if generation < self.config.last_generation {
            generation
                .get()
                .checked_add(1)
                .and_then(SecureSessionGeneration::new)
        } else {
            None
        };
        let Some(end) = self.now.0.checked_add(self.limits.timeout_ms) else {
            self.next = None;
            self.retry(ConnectError::InvalidRequest);
            return Ok(());
        };
        let (binding, _) = self.remote.reconnect_binding();
        let ticket = ConnectTicket {
            local: binding.local,
            peer: PeerIdentity {
                node: binding.peer.node,
                store: binding.peer.store.identity,
            },
            generation,
        };
        let deadline = MonoTime(end);
        let request = ConnectRequest {
            ticket,
            direction: ConnectDirection::Dial(self.config.endpoint),
            deadline,
        };
        match self.connector.submit(request, self.now) {
            Ok(()) => {
                self.pending = Some(Pending {
                    ticket,
                    deadline,
                    cancelled: false,
                })
            }
            Err(rejected) => {
                if rejected.request.ticket != ticket
                    || rejected.request.deadline != deadline
                    || !matches!(rejected.request.direction, ConnectDirection::Dial(a) if a == self.config.endpoint)
                {
                    return Err(self.poison());
                }
                self.retry(rejected.reason);
            }
        }
        Ok(())
    }
    fn poll_connector(&mut self, budget: SessionPollBudget) -> Result<(), DiscoveryError> {
        if let Some(p) = &mut self.pending {
            if self.now >= p.deadline && !p.cancelled {
                p.cancelled = self.connector.cancel(p.ticket);
            }
        }
        let budget = ConnectPollBudget {
            visits: 1,
            socket_calls: budget.io_calls,
            completions: 1,
            session: budget,
        };
        let events = match self.connector.poll(self.now, budget) {
            Ok(events) => events,
            Err(error) => {
                self.last_error = Some(error);
                return Ok(());
            }
        };
        if events.len() > 1
            || events
                .iter()
                .any(|e| self.pending.as_ref().is_none_or(|p| p.ticket != e.ticket))
        {
            for event in events {
                if let Ok(mut session) = event.result {
                    session.close();
                }
            }
            return Err(self.poison());
        }
        if let Some(event) = events.into_iter().next() {
            self.complete(event)?;
        }
        Ok(())
    }
    fn complete(&mut self, event: ConnectCompletion<C::Session>) -> Result<(), DiscoveryError> {
        let pending = self.pending.take().expect("checked exact connection");
        let mut session = match event.result {
            Ok(session) => session,
            Err(error) => {
                self.retry(error);
                return Ok(());
            }
        };
        if self.closed || self.now >= pending.deadline {
            session.close();
            self.retry(if self.closed {
                ConnectError::Cancelled
            } else {
                ConnectError::Timeout
            });
            return Ok(());
        }
        let (old, _) = self.remote.reconnect_binding();
        let valid = require_authenticated(&session).is_ok_and(|b| {
            b.local == pending.ticket.local
                && b.peer.node == pending.ticket.peer.node
                && b.peer.store.identity == pending.ticket.peer.store
                && b.generation == pending.ticket.generation
                && b.wire_version == old.wire_version
        });
        if !valid {
            session.close();
            return Err(self.poison());
        }
        match self.remote.attach_session(session) {
            Ok(old) => {
                if let Some(mut old) = old {
                    old.close();
                }
                self.last_error = None;
            }
            Err((_, mut returned)) => {
                returned.close();
                return Err(self.poison());
            }
        }
        Ok(())
    }
}
impl<C: PeerConnector<Endpoint = SocketAddr>> PeerDiscovery for ReconnectingPeerDiscovery<C> {
    fn resolve(
        &mut self,
        peer: PeerIdentity,
        now: MonoTime,
    ) -> Result<PeerEndpointHint, DiscoveryError> {
        self.time(now)?;
        self.remote.resolve(peer, now)
    }
    fn invalidate(&mut self, peer: PeerIdentity, generation: HintGeneration) -> bool {
        self.remote.invalidate(peer, generation)
    }
    fn close(&mut self) {
        self.closed = true;
        self.remote.close();
        self.connector.close();
    }
}
impl<C: PeerConnector<Endpoint = SocketAddr>> DiscoveryDriver for ReconnectingPeerDiscovery<C> {
    fn poll_discovery(
        &mut self,
        now: MonoTime,
        budget: SessionPollBudget,
    ) -> Result<(), DiscoveryError> {
        budget
            .validate()
            .map_err(|_| DiscoveryError::InvalidLimits)?;
        self.time(now)?;
        if self.connector.limits() != self.limits {
            return Err(self.poison());
        }
        if self.broken && !self.closed {
            return Err(DiscoveryError::WrongBinding);
        }
        if self.closed {
            if self.remote.pending().is_some() {
                self.remote.poll_discovery(now, budget)
            } else {
                self.poll_connector(budget)
            }
        } else if self.remote.source_failed() {
            if self.pending.is_some() {
                self.poll_connector(budget)
            } else if now >= self.retry_at && budget.io_calls > 0 {
                self.start()
            } else {
                Ok(())
            }
        } else {
            self.remote.poll_discovery(now, budget)?;
            if self.remote.source_failed() {
                self.retry_at = MonoTime(now.0.saturating_add(self.config.retry_ms));
            }
            Ok(())
        }
    }
    fn discovery_pending(&self) -> bool {
        self.remote.pending().is_some()
            || self.pending.is_some()
            || (!self.closed && !self.broken && self.remote.source_failed() && self.next.is_some())
    }
    fn discovery_deadline(&self) -> Option<MonoTime> {
        if self.closed {
            return self.discovery_pending().then_some(self.now);
        }
        if let Some(p) = &self.pending {
            return Some(
                self.connector
                    .next_deadline()
                    .map_or(p.deadline, |d| d.min(p.deadline)),
            );
        }
        if !self.broken && self.remote.source_failed() && self.next.is_some() {
            return Some(self.retry_at);
        }
        self.remote.discovery_deadline()
    }
}
