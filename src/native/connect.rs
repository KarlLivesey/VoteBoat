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
//! Bounded caller-driven TCP listener, routing preface and TLS establishment.
//! The supplied PeerDialer is the sole address executor. No hidden listener or
//! thread is created here. Routing hints select expectations, never authority.
use super::{dial::NativeTcpDialer, tls::*};
pub use crate::connect::AcceptStatus;
use crate::{
    connect::*, dial::*, identity::*, runtime::MonoTime, secure::*, transport::ConnectTicket,
};
use std::{
    collections::BTreeMap,
    io::{self, Read, Write},
    net::{SocketAddr, TcpListener, TcpStream},
    ops::Bound,
};
const MAGIC: &[u8; 8] = b"VBCONN01";
const PREFACE: usize = 16;
type Session = NativeTlsSession<TcpStream>;
#[derive(Clone, Copy, Debug)]
pub struct NativeConnectConfig {
    pub local: LocalIdentity,
    pub limits: ConnectLimits,
    pub session: SessionLimits,
}
/// Failed construction returns caller-owned live resources for explicit cleanup.
/// TLS config and public peer material are consumed, never printed.
pub struct ConnectConstructionRejected<D> {
    pub reason: ConnectError,
    pub dialer: D,
    pub listener: Option<TcpListener>,
}
impl<D> std::fmt::Debug for ConnectConstructionRejected<D> {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("ConnectConstructionRejected")
            .field("reason", &self.reason)
            .finish_non_exhaustive()
    }
}
struct Peer {
    pin: TlsPeer,
    generation: u64,
}
enum Stage {
    Dialing(Option<ConnectError>),
    Accept,
    Preface { stream: TcpStream, written: usize },
    Handshake(Box<Session>),
    Terminal(Result<Box<Session>, ConnectError>),
}
struct Attempt {
    ticket: ConnectTicket,
    deadline: MonoTime,
    stage: Stage,
}
struct Anonymous {
    stream: TcpStream,
    bytes: [u8; PREFACE],
    read: usize,
    deadline: MonoTime,
}
#[derive(Clone, Copy)]
enum IoPhase {
    Prefaces,
    Attempts,
    Accept,
}
impl IoPhase {
    fn next(self) -> Self {
        match self {
            Self::Prefaces => Self::Attempts,
            Self::Attempts => Self::Accept,
            Self::Accept => Self::Prefaces,
        }
    }
}
pub struct NativePeerConnector<
    D: PeerDialer<Endpoint = SocketAddr, Channel = TcpStream> = NativeTcpDialer,
> {
    config: NativeConnectConfig,
    tls: NativeTlsConfig,
    peers: BTreeMap<NodeId, Peer>,
    dialer: D,
    dial_limits: DialLimits,
    listener: Option<TcpListener>,
    attempts: BTreeMap<NodeId, Attempt>,
    anonymous: Vec<Option<Anonymous>>,
    cursor: Option<NodeId>,
    terminal_cursor: Option<NodeId>,
    anonymous_cursor: usize,
    io_phase: IoPhase,
    now: MonoTime,
    closed: bool,
    /// Accept phases to skip after the listener reported no pending
    /// connection; bounds idle `accept` syscalls without delaying a ready one
    /// by more than `ACCEPT_IDLE_PHASES` accept phases.
    accept_idle: u8,
    /// Consecutive resource-class accept failures; sets the backoff delay.
    accept_failures: u32,
    accept_status: AcceptStatus,
    #[cfg(test)]
    injected_accept_errors: std::collections::VecDeque<io::Error>,
}
const ACCEPT_IDLE_PHASES: u8 = 7;
const ACCEPT_BACKOFF_MIN_MS: u64 = 10;
const ACCEPT_BACKOFF_MAX_MS: u64 = 1000;

/// What the listener does after an `accept()` error. Environmental errors are
/// never node-fatal; only a listener that is gone or misused is.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum AcceptAction {
    /// No pending connection: skip a few accept phases.
    Idle,
    /// This connection failed before it was accepted (aborted, reset, a
    /// pending network error): keep accepting within the call budget.
    Retry,
    /// Out of descriptors, buffers or memory, or an unrecognised error: stop
    /// accepting for a bounded, doubling delay, then try again.
    Backoff,
    /// The listener is gone or misused (`EBADF`, `EINVAL`, `ENOTSOCK`,
    /// `EFAULT`): a bug, reported as `ConnectError::Io`.
    Fatal,
}

/// Classify an `accept()` error. Linux `accept(2)` asks callers to treat the
/// pending network errors (`EPROTO`, `ENETDOWN`, ...) like `EAGAIN`.
pub fn classify_accept(error: &io::Error) -> AcceptAction {
    #[cfg(unix)]
    if let Some(code) = error.raw_os_error() {
        return match code {
            libc::EBADF | libc::EINVAL | libc::ENOTSOCK | libc::EFAULT => AcceptAction::Fatal,
            libc::EMFILE | libc::ENFILE | libc::ENOBUFS | libc::ENOMEM => AcceptAction::Backoff,
            libc::EINTR
            | libc::ECONNABORTED
            | libc::ECONNRESET
            | libc::EPERM
            | libc::ETIMEDOUT
            | libc::EPROTO
            | libc::ENETDOWN
            | libc::ENOPROTOOPT
            | libc::EHOSTDOWN
            | libc::EHOSTUNREACH
            | libc::EOPNOTSUPP
            | libc::ENETUNREACH
            | libc::ESOCKTNOSUPPORT
            | libc::EPROTONOSUPPORT => AcceptAction::Retry,
            #[cfg(target_os = "linux")]
            libc::ENONET => AcceptAction::Retry,
            // Linux `accept(2)` also lists these per-connection errors.
            #[cfg(any(target_os = "linux", target_vendor = "apple"))]
            libc::ENOSR => AcceptAction::Retry,
            _ if code == libc::EAGAIN || code == libc::EWOULDBLOCK => AcceptAction::Idle,
            _ => AcceptAction::Backoff,
        };
    }
    match error.kind() {
        io::ErrorKind::WouldBlock => AcceptAction::Idle,
        io::ErrorKind::Interrupted
        | io::ErrorKind::ConnectionAborted
        | io::ErrorKind::ConnectionReset => AcceptAction::Retry,
        _ => AcceptAction::Backoff,
    }
}

fn next_key<V>(map: &BTreeMap<NodeId, V>, cursor: Option<NodeId>) -> Option<NodeId> {
    cursor
        .and_then(|c| {
            map.range((Bound::Excluded(c), Bound::Unbounded))
                .next()
                .map(|(k, _)| *k)
        })
        .or_else(|| map.first_key_value().map(|(k, _)| *k))
}
impl<D: PeerDialer<Endpoint = SocketAddr, Channel = TcpStream>> NativePeerConnector<D> {
    pub fn new(
        config: NativeConnectConfig,
        tls: NativeTlsConfig,
        peers: BTreeMap<NodeId, TlsPeer>,
        dialer: D,
        listener: Option<TcpListener>,
        now: MonoTime,
    ) -> Result<Self, Box<ConnectConstructionRejected<D>>> {
        let validation = (|| {
            config.limits.validate()?;
            config.session.validate().map_err(ConnectError::Session)?;
            dialer.limits().validate().map_err(ConnectError::Dial)?;
            if dialer.local() != config.local || dialer.outstanding() != 0 {
                return Err(ConnectError::WrongBinding);
            }
            if peers.len() > 1024
                || peers.contains_key(&config.local.node)
                || peers.iter().any(|(node, p)| {
                    *node != p.identity.node
                        || p.certificate.is_empty()
                        || p.certificate.len() > 64 * 1024
                        || p.server_name.is_empty()
                        || p.server_name.len() > 253
                        || rustls::pki_types::ServerName::try_from(p.server_name.as_str()).is_err()
                })
                || peers
                    .values()
                    .try_fold(0usize, |bytes, p| {
                        bytes
                            .checked_add(p.certificate.capacity())?
                            .checked_add(p.server_name.capacity())
                    })
                    .is_none_or(|bytes| bytes > 1024 * 1024)
            {
                return Err(ConnectError::InvalidRequest);
            }
            if listener.is_some() && config.limits.anonymous == 0 {
                return Err(ConnectError::InvalidLimits);
            }
            if let Some(listener) = &listener {
                listener
                    .set_nonblocking(true)
                    .map_err(|e| ConnectError::Io(e.kind()))?;
            }
            Ok(())
        })();
        if let Err(reason) = validation {
            return Err(Box::new(ConnectConstructionRejected {
                reason,
                dialer,
                listener,
            }));
        }
        let dial_limits = dialer.limits();
        Ok(Self {
            config,
            tls,
            peers: peers
                .into_iter()
                .map(|(id, pin)| (id, Peer { pin, generation: 0 }))
                .collect(),
            dialer,
            dial_limits,
            listener,
            attempts: BTreeMap::new(),
            anonymous: (0..config.limits.anonymous).map(|_| None).collect(),
            cursor: None,
            terminal_cursor: None,
            anonymous_cursor: 0,
            io_phase: IoPhase::Prefaces,
            accept_idle: 0,
            accept_failures: 0,
            accept_status: AcceptStatus::default(),
            #[cfg(test)]
            injected_accept_errors: std::collections::VecDeque::new(),
            now,
            closed: false,
        })
    }
    pub fn listener_addr(&self) -> io::Result<Option<SocketAddr>> {
        self.listener
            .as_ref()
            .map(TcpListener::local_addr)
            .transpose()
    }
    /// Explicitly reclaim the closed, drained dial provider; the caller can then
    /// join a native worker via try_finish. No shared host reactor is stopped.
    pub fn into_dialer(self) -> Result<D, Box<Self>> {
        if !self.closed || !self.is_drained() || !self.dialer.is_drained() {
            return Err(Box::new(self));
        }
        Ok(self.dialer)
    }
    fn check_time(&self, now: MonoTime) -> Result<(), ConnectError> {
        if now < self.now {
            Err(ConnectError::TimeWentBack)
        } else {
            Ok(())
        }
    }
    fn check_dialer(&self) -> Result<(), ConnectError> {
        if self.dialer.local() != self.config.local
            || self.dialer.limits() != self.dial_limits
            || self.dialer.outstanding()
                != self
                    .attempts
                    .values()
                    .filter(|a| matches!(a.stage, Stage::Dialing(_)))
                    .count()
        {
            return Err(ConnectError::ProviderViolation);
        }
        Ok(())
    }
    fn stop(&mut self, peer: NodeId, reason: ConnectError) {
        let attempt = self.attempts.get_mut(&peer).unwrap();
        match &mut attempt.stage {
            Stage::Dialing(stopped) => {
                if stopped.is_none() {
                    *stopped = Some(reason);
                    self.dialer.cancel(attempt.ticket);
                }
            }
            Stage::Terminal(Err(_)) => (),
            _ => attempt.stage = Stage::Terminal(Err(reason)),
        }
    }
    fn admission(&self, r: &ConnectRequest<SocketAddr>, now: MonoTime) -> Result<(), ConnectError> {
        self.check_time(now)?;
        if self.closed {
            return Err(ConnectError::Closed);
        }
        self.check_dialer()?;
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
        if r.deadline <= now || r.deadline.0 - now.0 > self.config.limits.timeout_ms {
            return Err(ConnectError::InvalidRequest);
        }
        if matches!(r.direction, ConnectDirection::Accept) && self.listener.is_none() {
            return Err(ConnectError::InvalidRequest);
        }
        if self.attempts.contains_key(&r.ticket.peer.node)
            || self.attempts.len() >= self.config.limits.requests
        {
            return Err(ConnectError::Overloaded);
        }
        Ok(())
    }
    fn ingest_dials(&mut self, limit: usize) -> Result<(), ConnectError> {
        let events = self.dialer.poll(limit);
        if events.len() > limit {
            self.close();
            return Err(ConnectError::ProviderViolation);
        }
        for event in events {
            let Some(a) = self.attempts.get_mut(&event.connection.peer.node) else {
                self.close();
                return Err(ConnectError::ProviderViolation);
            };
            let Stage::Dialing(stopped) = a.stage else {
                self.close();
                return Err(ConnectError::ProviderViolation);
            };
            if a.ticket != event.connection {
                self.close();
                return Err(ConnectError::ProviderViolation);
            }
            a.stage = if let Some(reason) = stopped {
                Stage::Terminal(Err(reason))
            } else {
                match event.result {
                    Ok(stream) => match stream
                        .set_nonblocking(true)
                        .and_then(|_| stream.set_nodelay(true))
                    {
                        Ok(()) => Stage::Preface { stream, written: 0 },
                        Err(e) => Stage::Terminal(Err(ConnectError::Io(e.kind()))),
                    },
                    Err(e) => Stage::Terminal(Err(ConnectError::Dial(e))),
                }
            };
        }
        if let Err(e) = self.check_dialer() {
            self.close();
            return Err(e);
        }
        Ok(())
    }
    /// Whether an accept backoff (if any) has elapsed; clears it once due.
    fn accept_ready(&mut self) -> bool {
        match self.accept_status.retry_at {
            Some(at) if self.now < at => false,
            Some(_) => {
                self.accept_status.retry_at = None;
                true
            }
            None => true,
        }
    }
    fn accept_one(&mut self) -> io::Result<TcpStream> {
        #[cfg(test)]
        if let Some(error) = self.injected_accept_errors.pop_front() {
            return Err(error);
        }
        match &self.listener {
            Some(listener) => listener.accept().map(|(stream, _)| stream),
            None => Err(io::ErrorKind::NotConnected.into()),
        }
    }
    fn accept_failed(&mut self, error: &io::Error) {
        let status = &mut self.accept_status;
        status.transient_errors = status.transient_errors.saturating_add(1);
        status.last_error = Some(error.kind());
        status.last_os_error = error.raw_os_error();
    }
    fn back_off_accept(&mut self) {
        let delay =
            (ACCEPT_BACKOFF_MIN_MS << self.accept_failures.min(7)).min(ACCEPT_BACKOFF_MAX_MS);
        self.accept_failures = self.accept_failures.saturating_add(1);
        self.accept_status.backoffs = self.accept_status.backoffs.saturating_add(1);
        self.accept_status.retry_at = Some(MonoTime(self.now.0.saturating_add(delay)));
    }
    fn accept(&mut self, calls: &mut usize) -> Result<(), ConnectError> {
        if self.listener.is_none() || !self.accept_ready() {
            return Ok(());
        }
        if self.accept_idle != 0 {
            self.accept_idle -= 1;
            return Ok(());
        }
        while *calls != 0 {
            let Some(index) = self.anonymous.iter().position(Option::is_none) else {
                break;
            };
            let Some(deadline) = self
                .now
                .0
                .checked_add(self.config.limits.timeout_ms)
                .map(MonoTime)
            else {
                break;
            };
            *calls -= 1;
            match self.accept_one() {
                Ok(stream) => {
                    self.accept_failures = 0;
                    if stream
                        .set_nonblocking(true)
                        .and_then(|_| stream.set_nodelay(true))
                        .is_err()
                    {
                        continue;
                    }
                    self.anonymous[index] = Some(Anonymous {
                        stream,
                        bytes: [0; PREFACE],
                        read: 0,
                        deadline,
                    });
                }
                Err(e) => match classify_accept(&e) {
                    AcceptAction::Idle => {
                        self.accept_failures = 0;
                        self.accept_idle = ACCEPT_IDLE_PHASES;
                        break;
                    }
                    AcceptAction::Retry => self.accept_failed(&e),
                    AcceptAction::Backoff => {
                        self.accept_failed(&e);
                        self.back_off_accept();
                        break;
                    }
                    AcceptAction::Fatal => return Err(ConnectError::Io(e.kind())),
                },
            }
        }
        Ok(())
    }
    fn read_prefaces(&mut self, visits: usize, calls: &mut usize) {
        for _ in 0..visits.min(self.anonymous.len()) {
            if *calls == 0 {
                break;
            }
            let index = self.anonymous_cursor;
            self.anonymous_cursor = (index + 1) % self.anonymous.len();
            let Some(mut incoming) = self.anonymous[index].take() else {
                continue;
            };
            *calls -= 1;
            match incoming.stream.read(&mut incoming.bytes[incoming.read..]) {
                Ok(0) => continue,
                Ok(n) => incoming.read += n,
                Err(e)
                    if matches!(
                        e.kind(),
                        io::ErrorKind::WouldBlock | io::ErrorKind::Interrupted
                    ) => {}
                Err(_) => continue,
            }
            if incoming.read < PREFACE {
                self.anonymous[index] = Some(incoming);
                continue;
            }
            if &incoming.bytes[..8] != MAGIC {
                continue;
            }
            let Some(peer) =
                NodeId::new(u64::from_le_bytes(incoming.bytes[8..].try_into().unwrap()))
            else {
                continue;
            };
            let Some(attempt) = self.attempts.get_mut(&peer) else {
                continue;
            };
            if !matches!(attempt.stage, Stage::Accept) {
                continue;
            }
            let mut limits = self.config.session;
            limits.handshake_timeout_ms = limits
                .handshake_timeout_ms
                .min(attempt.deadline.0 - self.now.0);
            attempt.stage = match Session::server(
                incoming.stream,
                &self.tls,
                self.config.local,
                self.peers[&peer].pin.clone(),
                attempt.ticket.generation,
                limits,
                self.now,
            ) {
                Ok(session) => Stage::Handshake(Box::new(session)),
                Err(e) => Stage::Terminal(Err(ConnectError::Session(e))),
            };
        }
    }
    fn drive(&mut self, visits: usize, calls: &mut usize, budget: SessionPollBudget) {
        for _ in 0..visits.min(self.attempts.len()) {
            let peer = next_key(&self.attempts, self.cursor).unwrap();
            if *calls == 0 && matches!(self.attempts[&peer].stage, Stage::Preface { .. }) {
                break;
            }
            self.cursor = Some(peer);
            let attempt = self.attempts.get_mut(&peer).unwrap();
            match &mut attempt.stage {
                Stage::Preface { stream, written } if *calls != 0 => {
                    let mut bytes = [0; PREFACE];
                    bytes[..8].copy_from_slice(MAGIC);
                    bytes[8..].copy_from_slice(&self.config.local.node.get().to_le_bytes());
                    *calls -= 1;
                    match stream.write(&bytes[*written..]) {
                        Ok(0) => {
                            attempt.stage =
                                Stage::Terminal(Err(ConnectError::Io(io::ErrorKind::WriteZero)))
                        }
                        Ok(n) => *written += n,
                        Err(e)
                            if matches!(
                                e.kind(),
                                io::ErrorKind::WouldBlock | io::ErrorKind::Interrupted
                            ) => {}
                        Err(e) => attempt.stage = Stage::Terminal(Err(ConnectError::Io(e.kind()))),
                    }
                    if matches!(
                        attempt.stage,
                        Stage::Preface {
                            written: PREFACE,
                            ..
                        }
                    ) {
                        let Stage::Preface { stream, .. } =
                            std::mem::replace(&mut attempt.stage, Stage::Accept)
                        else {
                            unreachable!()
                        };
                        let mut limits = self.config.session;
                        limits.handshake_timeout_ms = limits
                            .handshake_timeout_ms
                            .min(attempt.deadline.0 - self.now.0);
                        attempt.stage = match Session::client(
                            stream,
                            &self.tls,
                            self.config.local,
                            self.peers[&peer].pin.clone(),
                            attempt.ticket.generation,
                            limits,
                            self.now,
                        ) {
                            Ok(session) => Stage::Handshake(Box::new(session)),
                            Err(e) => Stage::Terminal(Err(ConnectError::Session(e))),
                        };
                    }
                }
                Stage::Handshake(session) => {
                    let result = session.poll(self.now, budget);
                    if let Err(e) = result {
                        attempt.stage = Stage::Terminal(Err(ConnectError::Session(e)));
                    } else if session.state() == SessionState::Ready {
                        let valid = require_authenticated(session.as_ref()).is_ok_and(|b| {
                            b.local == attempt.ticket.local
                                && b.peer.node == attempt.ticket.peer.node
                                && b.peer.store.identity == attempt.ticket.peer.store
                                && b.generation == attempt.ticket.generation
                                && b.wire_version == self.tls.wire_version()
                        });
                        if valid {
                            let Stage::Handshake(session) =
                                std::mem::replace(&mut attempt.stage, Stage::Accept)
                            else {
                                unreachable!()
                            };
                            attempt.stage = Stage::Terminal(Ok(session));
                        } else {
                            attempt.stage = Stage::Terminal(Err(ConnectError::ProviderViolation));
                        }
                    }
                }
                _ => (),
            }
        }
    }
    fn poll_io(&mut self, budget: ConnectPollBudget) -> Result<(), ConnectError> {
        let mut phase = self.io_phase;
        if budget.socket_calls != 0 {
            self.io_phase = phase.next();
        }
        let mut calls = budget.socket_calls;
        // Rotate first access to shared socket credits. A blocked stream must
        // not consume every poll before another phase gets a chance to run.
        for _ in 0..3 {
            match phase {
                IoPhase::Prefaces => self.read_prefaces(budget.visits, &mut calls),
                IoPhase::Attempts => self.drive(budget.visits, &mut calls, budget.session),
                IoPhase::Accept => self.accept(&mut calls)?,
            }
            phase = phase.next();
        }
        Ok(())
    }
}
impl<D: PeerDialer<Endpoint = SocketAddr, Channel = TcpStream>> PeerConnector
    for NativePeerConnector<D>
{
    #[cfg(unix)]
    fn readiness_fds(&self, out: &mut Vec<crate::worker::ReadinessFd>) {
        use crate::worker::ReadinessFd;
        use std::os::fd::AsRawFd;
        // A backing-off listener stays out of the wait set, so a readable
        // backlog cannot spin the host; `next_deadline` wakes it instead.
        if let Some(listener) = self
            .listener
            .as_ref()
            .filter(|_| self.accept_status.retry_at.is_none())
        {
            out.push(ReadinessFd::read(listener.as_raw_fd()));
        }
        out.extend(
            self.anonymous
                .iter()
                .flatten()
                .map(|a| ReadinessFd::read(a.stream.as_raw_fd())),
        );
        for attempt in self.attempts.values() {
            match &attempt.stage {
                Stage::Preface { stream, .. } => out.push(ReadinessFd {
                    fd: stream.as_raw_fd(),
                    write: true,
                    immediate: false,
                }),
                Stage::Handshake(session) => session.readiness_fds(out),
                _ => {}
            }
        }
    }
    type Endpoint = SocketAddr;
    type Session = Session;
    /// Transient accept errors survived and any current accept backoff.
    fn accept_status(&self) -> Option<AcceptStatus> {
        Some(self.accept_status)
    }
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
            anonymous: self.anonymous.iter().flatten().count(),
            dialing: self
                .attempts
                .values()
                .filter(|a| matches!(a.stage, Stage::Dialing(_)))
                .count(),
            handshaking: self
                .attempts
                .values()
                .filter(|a| matches!(a.stage, Stage::Handshake(_)))
                .count(),
        }
    }
    fn next_deadline(&self) -> Option<MonoTime> {
        self.attempts
            .values()
            .filter_map(|a| match a.stage {
                Stage::Terminal(_) => Some(self.now),
                Stage::Dialing(Some(_)) => None,
                _ => Some(a.deadline),
            })
            .chain(self.anonymous.iter().flatten().map(|a| a.deadline))
            .chain(
                self.accept_status
                    .retry_at
                    .filter(|_| self.listener.is_some()),
            )
            .min()
    }
    fn submit(
        &mut self,
        request: ConnectRequest<SocketAddr>,
        now: MonoTime,
    ) -> Result<(), ConnectRejected<SocketAddr>> {
        if let Err(reason) = self.admission(&request, now) {
            return Err(ConnectRejected {
                reason,
                request: Box::new(request),
            });
        }
        let stage = match request.direction {
            ConnectDirection::Accept => Stage::Accept,
            ConnectDirection::Dial(endpoint) => {
                let dial = DialRequest {
                    connection: request.ticket,
                    endpoint,
                    timeout_ms: self
                        .dial_limits
                        .connect_timeout_ms
                        .min(request.deadline.0 - now.0),
                };
                if let Err(rejected) = self.dialer.submit(dial) {
                    return Err(ConnectRejected {
                        reason: ConnectError::Dial(rejected.reason),
                        request: Box::new(request),
                    });
                }
                Stage::Dialing(None)
            }
        };
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
                stage,
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
    ) -> Result<Vec<ConnectCompletion<Session>>, ConnectError> {
        let budget = budget.validate()?;
        self.check_time(now)?;
        if let Err(e) = self.check_dialer() {
            self.close();
            return Err(e);
        }
        self.now = now;
        // Metadata expiry visits are bounded by fixed construction capacities,
        // independent of I/O budgets; expiry never starts more socket work.
        let expired = self
            .attempts
            .iter()
            .filter(|(_, a)| now >= a.deadline)
            .map(|(k, _)| *k)
            .collect::<Vec<_>>();
        for peer in expired {
            self.stop(peer, ConnectError::Timeout);
        }
        for slot in &mut self.anonymous {
            if slot.as_ref().is_some_and(|a| now >= a.deadline) {
                *slot = None;
            }
        }
        self.ingest_dials(budget.completions)?;
        self.poll_io(budget)?;
        let mut result = Vec::new();
        // Terminal scans also have fixed request capacity and a rotating cursor.
        for _ in 0..self.attempts.len() {
            if result.len() >= budget.completions {
                break;
            }
            let peer = next_key(&self.attempts, self.terminal_cursor).unwrap();
            self.terminal_cursor = Some(peer);
            if matches!(self.attempts[&peer].stage, Stage::Terminal(_)) {
                let a = self.attempts.remove(&peer).unwrap();
                let Stage::Terminal(output) = a.stage else {
                    unreachable!()
                };
                result.push(ConnectCompletion {
                    ticket: a.ticket,
                    result: output.map(|s| *s),
                });
            }
        }
        Ok(result)
    }
    fn close(&mut self) {
        self.closed = true;
        self.listener.take();
        for slot in &mut self.anonymous {
            *slot = None;
        }
        let peers = self.attempts.keys().copied().collect::<Vec<_>>();
        for peer in peers {
            self.stop(peer, ConnectError::Cancelled);
        }
        self.dialer.close();
    }
}

impl<D: PeerDialer<Endpoint = SocketAddr, Channel = TcpStream>>
    super::peer_credentials::NativePeerMaterialProvider for NativePeerConnector<D>
{
    fn replace_material(
        &mut self,
        material: super::peer_credentials::NativePeerMaterial,
    ) -> Result<
        super::peer_credentials::NativePeerMaterial,
        (ConnectError, super::peer_credentials::NativePeerMaterial),
    > {
        if self.closed {
            return Err((ConnectError::Closed, material));
        }
        if let Err(error) = super::peer_credentials::validate_material(
            &material,
            self.tls.wire_version(),
            self.peers.iter().map(|(id, p)| (*id, p.pin.identity)),
        ) {
            return Err((error, material));
        }
        let mut previous = BTreeMap::new();
        for (id, pin) in material.peers {
            let peer = self
                .peers
                .get_mut(&id)
                .expect("validated peer identity set");
            previous.insert(id, std::mem::replace(&mut peer.pin, pin));
        }
        Ok(super::peer_credentials::NativePeerMaterial {
            tls: std::mem::replace(&mut self.tls, material.tls),
            peers: previous,
        })
    }
}

/// Explicit native service transport selection; TCP remains available without QUIC.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum NativePeerProtocol {
    TcpTls,
    #[cfg(feature = "quic")]
    Quic,
}
/// Construction-selected connector for applications offering both native protocols.
/// The boxed session still implements the same public SecureSession contract.
pub type NativeDiscoveredConnector =
    DiscoveryConnector<NativeServiceConnector, Box<dyn crate::discovery::DiscoveryDriver>>;
pub enum NativeServiceConnector {
    Discovered(Box<NativeDiscoveredConnector>),
    Tcp(Box<NativePeerConnector>),
    RotatingTcp(Box<super::peer_credentials::RotatingPeerConnector<NativePeerConnector>>),
    #[cfg(feature = "quic")]
    Quic(Box<super::quic_connect::NativeQuicConnector>),
    #[cfg(feature = "quic")]
    RotatingQuic(
        Box<
            super::peer_credentials::RotatingPeerConnector<
                super::quic_connect::NativeQuicConnector,
            >,
        >,
    ),
}
impl super::peer_credentials::NativePeerMaterialProvider for NativeServiceConnector {
    fn replace_material(
        &mut self,
        material: super::peer_credentials::NativePeerMaterial,
    ) -> Result<
        super::peer_credentials::NativePeerMaterial,
        (ConnectError, super::peer_credentials::NativePeerMaterial),
    > {
        match self {
            Self::Discovered(_) | Self::RotatingTcp(_) => {
                Err((ConnectError::InvalidRequest, material))
            }
            #[cfg(feature = "quic")]
            Self::RotatingQuic(_) => Err((ConnectError::InvalidRequest, material)),
            Self::Tcp(connector) => connector.replace_material(material),
            #[cfg(feature = "quic")]
            Self::Quic(connector) => connector.replace_material(material),
        }
    }
}
impl NativeServiceConnector {
    /// Attach an explicitly owned source before any data requests are accepted.
    /// Rejection returns both owners; closed sources must drain their own work.
    pub fn with_discovery(
        self,
        source: Box<dyn crate::discovery::DiscoveryDriver>,
        now: MonoTime,
    ) -> Result<
        Self,
        (
            ConnectError,
            Self,
            Box<dyn crate::discovery::DiscoveryDriver>,
        ),
    > {
        if matches!(self, Self::Discovered(_)) {
            return Err((ConnectError::InvalidRequest, self, source));
        }
        DiscoveryConnector::new_driven(self, source, now).map(|c| Self::Discovered(Box::new(c)))
    }

    /// Enable generation guards before any connections are admitted.
    /// A rejected connector is returned unchanged for normal owner cleanup.
    pub fn with_peer_rotation(
        self,
        generation: crate::authorization::CredentialGeneration,
    ) -> Result<Self, Box<Self>> {
        use super::peer_credentials::RotatingPeerConnector;
        match self {
            Self::Tcp(c) => RotatingPeerConnector::new(*c, generation)
                .map(|c| Self::RotatingTcp(Box::new(c)))
                .map_err(|c| Box::new(Self::Tcp(Box::new(c)))),
            #[cfg(feature = "quic")]
            Self::Quic(c) => RotatingPeerConnector::new(*c, generation)
                .map(|c| Self::RotatingQuic(Box::new(c)))
                .map_err(|c| Box::new(Self::Quic(Box::new(c)))),
            other => Err(Box::new(other)),
        }
    }

    /// Reclaim the TCP worker, if selected, only after close and drain.
    /// QUIC has no connector worker; transferred sessions own their socket leases.
    pub fn into_dialer(self) -> Result<Option<NativeTcpDialer>, Box<Self>> {
        match self {
            Self::Discovered(c) => match c.into_parts() {
                Ok((connector, source)) => {
                    drop(source);
                    connector.into_dialer()
                }
                Err(c) => Err(Box::new(Self::Discovered(c))),
            },
            Self::RotatingTcp(c) => match c.into_inner() {
                Ok(c) => Self::Tcp(Box::new(c)).into_dialer(),
                Err(c) => Err(Box::new(Self::RotatingTcp(Box::new(c)))),
            },
            #[cfg(feature = "quic")]
            Self::RotatingQuic(c) => match c.into_inner() {
                Ok(c) => Self::Quic(Box::new(c)).into_dialer(),
                Err(c) => Err(Box::new(Self::RotatingQuic(Box::new(c)))),
            },
            Self::Tcp(c) => c
                .into_dialer()
                .map(Some)
                .map_err(|c| Box::new(Self::Tcp(c))),
            #[cfg(feature = "quic")]
            Self::Quic(c) => {
                if c.closed_and_drained() {
                    Ok(None)
                } else {
                    Err(Box::new(Self::Quic(c)))
                }
            }
        }
    }
}
impl PeerConnector for NativeServiceConnector {
    fn is_drained(&self) -> bool {
        match self {
            Self::Discovered(c) => c.is_drained(),
            _ => self.usage().requests == 0 && self.usage().anonymous == 0,
        }
    }
    #[cfg(unix)]
    fn readiness_fds(&self, out: &mut Vec<crate::worker::ReadinessFd>) {
        match self {
            Self::Discovered(c) => c.readiness_fds(out),
            Self::Tcp(c) => c.readiness_fds(out),
            Self::RotatingTcp(c) => c.readiness_fds(out),
            #[cfg(feature = "quic")]
            Self::Quic(c) => c.readiness_fds(out),
            #[cfg(feature = "quic")]
            Self::RotatingQuic(c) => c.readiness_fds(out),
        }
    }
    type Endpoint = SocketAddr;
    type Session = Box<dyn SecureSession>;
    /// Listener status of the TCP connector inside any variant (including
    /// `Discovered`); QUIC has no accept loop and reports `None`.
    fn accept_status(&self) -> Option<AcceptStatus> {
        match self {
            Self::Discovered(c) => c.accept_status(),
            Self::Tcp(c) => c.accept_status(),
            Self::RotatingTcp(c) => c.accept_status(),
            #[cfg(feature = "quic")]
            Self::Quic(c) => c.accept_status(),
            #[cfg(feature = "quic")]
            Self::RotatingQuic(c) => c.accept_status(),
        }
    }
    fn supports_peer(&self, peer: PeerIdentity) -> bool {
        match self {
            Self::Discovered(c) => c.supports_peer(peer),
            Self::Tcp(c) => c.supports_peer(peer),
            Self::RotatingTcp(c) => c.supports_peer(peer),
            #[cfg(feature = "quic")]
            Self::Quic(c) => c.supports_peer(peer),
            #[cfg(feature = "quic")]
            Self::RotatingQuic(c) => c.supports_peer(peer),
        }
    }
    fn local(&self) -> LocalIdentity {
        match self {
            Self::Discovered(c) => c.local(),
            Self::Tcp(c) => c.local(),
            Self::RotatingTcp(c) => c.local(),
            #[cfg(feature = "quic")]
            Self::Quic(c) => c.local(),
            #[cfg(feature = "quic")]
            Self::RotatingQuic(c) => c.local(),
        }
    }
    fn limits(&self) -> ConnectLimits {
        match self {
            Self::Discovered(c) => c.limits(),
            Self::Tcp(c) => c.limits(),
            Self::RotatingTcp(c) => c.limits(),
            #[cfg(feature = "quic")]
            Self::Quic(c) => c.limits(),
            #[cfg(feature = "quic")]
            Self::RotatingQuic(c) => c.limits(),
        }
    }
    fn usage(&self) -> ConnectUsage {
        match self {
            Self::Discovered(c) => c.usage(),
            Self::Tcp(c) => c.usage(),
            Self::RotatingTcp(c) => c.usage(),
            #[cfg(feature = "quic")]
            Self::Quic(c) => c.usage(),
            #[cfg(feature = "quic")]
            Self::RotatingQuic(c) => c.usage(),
        }
    }
    fn next_deadline(&self) -> Option<MonoTime> {
        match self {
            Self::Discovered(c) => c.next_deadline(),
            Self::Tcp(c) => c.next_deadline(),
            Self::RotatingTcp(c) => c.next_deadline(),
            #[cfg(feature = "quic")]
            Self::Quic(c) => c.next_deadline(),
            #[cfg(feature = "quic")]
            Self::RotatingQuic(c) => c.next_deadline(),
        }
    }
    fn submit(
        &mut self,
        r: ConnectRequest<SocketAddr>,
        now: MonoTime,
    ) -> Result<(), ConnectRejected<SocketAddr>> {
        match self {
            Self::Discovered(c) => c.submit(r, now),
            Self::Tcp(c) => c.submit(r, now),
            Self::RotatingTcp(c) => c.submit(r, now),
            #[cfg(feature = "quic")]
            Self::Quic(c) => c.submit(r, now),
            #[cfg(feature = "quic")]
            Self::RotatingQuic(c) => c.submit(r, now),
        }
    }
    fn cancel(&mut self, t: ConnectTicket) -> bool {
        match self {
            Self::Discovered(c) => c.cancel(t),
            Self::Tcp(c) => c.cancel(t),
            Self::RotatingTcp(c) => c.cancel(t),
            #[cfg(feature = "quic")]
            Self::Quic(c) => c.cancel(t),
            #[cfg(feature = "quic")]
            Self::RotatingQuic(c) => c.cancel(t),
        }
    }
    fn poll(
        &mut self,
        now: MonoTime,
        b: ConnectPollBudget,
    ) -> Result<Vec<ConnectCompletion<Self::Session>>, ConnectError> {
        fn boxed<S: SecureSession + 'static>(
            events: Vec<ConnectCompletion<S>>,
        ) -> Vec<ConnectCompletion<Box<dyn SecureSession>>> {
            events
                .into_iter()
                .map(|e| ConnectCompletion {
                    ticket: e.ticket,
                    result: e.result.map(|s| Box::new(s) as Box<dyn SecureSession>),
                })
                .collect()
        }
        match self {
            Self::Discovered(c) => c.poll(now, b),
            Self::Tcp(c) => c.poll(now, b).map(boxed),
            Self::RotatingTcp(c) => c.poll(now, b).map(boxed),
            #[cfg(feature = "quic")]
            Self::Quic(c) => c.poll(now, b).map(boxed),
            #[cfg(feature = "quic")]
            Self::RotatingQuic(c) => c.poll(now, b).map(boxed),
        }
    }
    fn close(&mut self) {
        match self {
            Self::Discovered(c) => c.close(),
            Self::Tcp(c) => c.close(),
            Self::RotatingTcp(c) => c.close(),
            #[cfg(feature = "quic")]
            Self::Quic(c) => c.close(),
            #[cfg(feature = "quic")]
            Self::RotatingQuic(c) => c.close(),
        }
    }
}

impl PeerCredentialControl for NativeServiceConnector {
    type Credentials = super::peer_credentials::NativePeerMaterial;
    fn credential_generation(&self) -> Option<crate::authorization::CredentialGeneration> {
        match self {
            Self::Discovered(c) => c.credential_generation(),
            Self::RotatingTcp(c) => c.credential_generation(),
            #[cfg(feature = "quic")]
            Self::RotatingQuic(c) => c.credential_generation(),
            _ => None,
        }
    }
    fn replace_peer_credentials(
        &mut self,
        expected: crate::authorization::CredentialGeneration,
        replacement: crate::authorization::CredentialGeneration,
        material: Self::Credentials,
    ) -> Result<Self::Credentials, (ConnectError, Self::Credentials)> {
        match self {
            Self::Discovered(c) => c.replace_peer_credentials(expected, replacement, material),
            Self::RotatingTcp(c) => c.replace_peer_credentials(expected, replacement, material),
            #[cfg(feature = "quic")]
            Self::RotatingQuic(c) => c.replace_peer_credentials(expected, replacement, material),
            _ => Err((ConnectError::InvalidRequest, material)),
        }
    }
}

#[cfg(test)]
mod health_tests;
#[cfg(test)]
mod accept_tests {
    use super::*;
    use crate::native::{tls::TlsCredentials, worker::ThreadWake};
    use std::{sync::Arc, time::Duration};

    fn local() -> LocalIdentity {
        LocalIdentity {
            node: NodeId::new(1).unwrap(),
            store: StoreBinding {
                identity: StoreIdentity {
                    id: StoreId::new(1).unwrap(),
                    incarnation: StoreIncarnation::new(1).unwrap(),
                },
                session: StoreSession::new(1).unwrap(),
            },
        }
    }
    fn connector() -> NativePeerConnector {
        connector_with(local(), BTreeMap::new())
    }
    fn connector_with(
        local: LocalIdentity,
        peers: BTreeMap<NodeId, TlsPeer>,
    ) -> NativePeerConnector {
        let tls = NativeTlsConfig::new(TlsCredentials {
            roots: vec![include_bytes!("../../tests/fixtures/tls/ca.der").to_vec()],
            certificate_chain: vec![include_bytes!("../../tests/fixtures/tls/node1.der").to_vec()],
            private_key: include_bytes!("../../tests/fixtures/tls/node1-key.der").to_vec(),
        })
        .unwrap();
        let dialer = NativeTcpDialer::spawn(
            local,
            BTreeMap::new(),
            DialLimits::default(),
            Arc::new(ThreadWake::current()),
        )
        .unwrap();
        NativePeerConnector::new(
            NativeConnectConfig {
                local,
                limits: ConnectLimits {
                    requests: 4,
                    anonymous: 4,
                    timeout_ms: 60_000,
                },
                session: SessionLimits::default(),
            },
            tls,
            peers,
            dialer,
            Some(TcpListener::bind("127.0.0.1:0").unwrap()),
            MonoTime(0),
        )
        .unwrap_or_else(|r| panic!("construct: {:?}", r.reason))
    }
    fn os(code: i32) -> io::Error {
        io::Error::from_raw_os_error(code)
    }
    fn listener_polled(c: &NativePeerConnector) -> bool {
        let mut fds = Vec::new();
        c.readiness_fds(&mut fds);
        fds.len() == 1
    }
    /// Poll at `now` until a real connection occupies an anonymous slot.
    fn accept_real_connection(c: &mut NativePeerConnector, now: u64) -> TcpStream {
        let address = c.listener_addr().unwrap().unwrap();
        let client = TcpStream::connect(address).unwrap();
        for _ in 0..10_000 {
            c.poll(MonoTime(now), ConnectPollBudget::default()).unwrap();
            if c.usage().anonymous == 1 {
                return client;
            }
            std::thread::park_timeout(Duration::from_millis(1));
        }
        panic!(
            "listener stopped accepting: {:?}",
            c.accept_status().unwrap()
        );
    }
    fn finish(mut c: NativePeerConnector) {
        c.close();
        let mut dialer = c.into_dialer().unwrap_or_else(|_| panic!("drained"));
        for _ in 0..10_000 {
            if dialer.try_finish().unwrap() {
                return;
            }
            std::thread::park_timeout(Duration::from_millis(1));
        }
        panic!("dialer did not finish");
    }

    #[test]
    fn accept_errors_are_classified_by_what_they_mean_for_the_listener() {
        use AcceptAction::*;
        for (code, action) in [
            (libc::EAGAIN, Idle),
            (libc::EWOULDBLOCK, Idle),
            (libc::EINTR, Retry),
            (libc::ECONNABORTED, Retry),
            (libc::ECONNRESET, Retry),
            (libc::EPERM, Retry),
            (libc::ETIMEDOUT, Retry),
            (libc::EPROTO, Retry),
            (libc::ENETDOWN, Retry),
            (libc::ENOPROTOOPT, Retry),
            (libc::EHOSTDOWN, Retry),
            (libc::EHOSTUNREACH, Retry),
            (libc::EOPNOTSUPP, Retry),
            (libc::ENETUNREACH, Retry),
            (libc::ESOCKTNOSUPPORT, Retry),
            (libc::EPROTONOSUPPORT, Retry),
            (libc::ENOSR, Retry),
            (libc::EMFILE, Backoff),
            (libc::ENFILE, Backoff),
            (libc::ENOBUFS, Backoff),
            (libc::ENOMEM, Backoff),
            (libc::EIO, Backoff),
            (libc::EBADF, Fatal),
            (libc::EINVAL, Fatal),
            (libc::ENOTSOCK, Fatal),
            (libc::EFAULT, Fatal),
        ] {
            assert_eq!(classify_accept(&os(code)), action, "errno {code}");
        }
        #[cfg(target_os = "linux")]
        assert_eq!(classify_accept(&os(libc::ENONET)), Retry);
        for (kind, action) in [
            (io::ErrorKind::WouldBlock, Idle),
            (io::ErrorKind::Interrupted, Retry),
            (io::ErrorKind::ConnectionAborted, Retry),
            (io::ErrorKind::ConnectionReset, Retry),
            (io::ErrorKind::Other, Backoff),
        ] {
            assert_eq!(classify_accept(&io::Error::from(kind)), action, "{kind:?}");
        }
    }

    /// Fails on the base: the first ECONNABORTED returned `ConnectError::Io`,
    /// which the peer driver turned into node recovery.
    #[test]
    fn injected_aborted_and_network_accept_errors_keep_the_listener_accepting() {
        let mut c = connector();
        c.injected_accept_errors.extend(
            [
                libc::ECONNABORTED,
                libc::EINTR,
                libc::EPROTO,
                libc::ECONNRESET,
            ]
            .map(os),
        );
        c.poll(MonoTime(1), ConnectPollBudget::default()).unwrap();
        assert!(c.injected_accept_errors.is_empty());
        let status = c.accept_status().unwrap();
        assert_eq!(status.transient_errors, 4);
        assert_eq!(status.last_os_error, Some(libc::ECONNRESET));
        assert_eq!(status.retry_at, None);
        assert!(listener_polled(&c));
        let _client = accept_real_connection(&mut c, 1);
        finish(c);
    }

    #[test]
    fn injected_resource_accept_errors_back_off_then_accept_again() {
        let mut c = connector();
        c.injected_accept_errors
            .extend([libc::EMFILE, libc::ENFILE, libc::ENOBUFS].map(os));
        c.poll(MonoTime(100), ConnectPollBudget::default()).unwrap();
        // One resource error stops this accept phase for 10 ms.
        assert_eq!(c.injected_accept_errors.len(), 2);
        assert_eq!(c.accept_status().unwrap().retry_at, Some(MonoTime(110)));
        assert_eq!(c.next_deadline(), Some(MonoTime(110)));
        assert!(
            !listener_polled(&c),
            "a backing-off listener must not spin the host"
        );
        c.poll(MonoTime(109), ConnectPollBudget::default()).unwrap();
        assert_eq!(
            c.injected_accept_errors.len(),
            2,
            "no accept before the deadline"
        );
        // Consecutive failures double the delay, capped at one second.
        c.poll(MonoTime(110), ConnectPollBudget::default()).unwrap();
        assert_eq!(c.accept_status().unwrap().retry_at, Some(MonoTime(130)));
        c.poll(MonoTime(130), ConnectPollBudget::default()).unwrap();
        assert_eq!(c.accept_status().unwrap().retry_at, Some(MonoTime(170)));
        for _ in 0..10 {
            c.injected_accept_errors.push_back(os(libc::EMFILE));
            let at = c.accept_status().unwrap().retry_at.unwrap();
            c.poll(at, ConnectPollBudget::default()).unwrap();
        }
        let at = c.accept_status().unwrap().retry_at.unwrap();
        assert_eq!(c.accept_status().unwrap().transient_errors, 13);
        assert_eq!(c.accept_status().unwrap().last_os_error, Some(libc::EMFILE));
        // After the delay the listener is polled again and accepts for real.
        c.poll(at, ConnectPollBudget::default()).unwrap();
        assert_eq!(c.accept_status().unwrap().retry_at, None);
        assert!(listener_polled(&c));
        let _client = accept_real_connection(&mut c, at.0);
        // A success resets the delay to its minimum. Idle accept phases may
        // be skipped first, so poll until the injected error is consumed.
        c.injected_accept_errors.push_back(os(libc::EMFILE));
        for _ in 0..=ACCEPT_IDLE_PHASES {
            if c.injected_accept_errors.is_empty() {
                break;
            }
            c.poll(at, ConnectPollBudget::default()).unwrap();
        }
        assert_eq!(
            c.accept_status().unwrap().retry_at,
            Some(MonoTime(at.0 + 10))
        );
        finish(c);
    }

    #[test]
    fn delay_doubling_is_capped_at_one_second() {
        let mut c = connector();
        let mut now = 0;
        let mut delays = Vec::new();
        for _ in 0..12 {
            c.injected_accept_errors.push_back(os(libc::ENOMEM));
            c.poll(MonoTime(now), ConnectPollBudget::default()).unwrap();
            let at = c.accept_status().unwrap().retry_at.unwrap().0;
            delays.push(at - now);
            now = at;
        }
        assert_eq!(
            delays,
            [10, 20, 40, 80, 160, 320, 640, 1000, 1000, 1000, 1000, 1000]
        );
        finish(c);
    }

    #[test]
    fn a_listener_that_is_gone_is_still_reported() {
        let mut c = connector();
        c.injected_accept_errors.push_back(os(libc::EBADF));
        assert!(matches!(
            c.poll(MonoTime(1), ConnectPollBudget::default()),
            Err(ConnectError::Io(_))
        ));
        assert_eq!(c.accept_status().unwrap().transient_errors, 0);
        finish(c);
    }

    /// NT1 round 2 (Sol 2): generations restart at 1 in every store session,
    /// so a ticket of the previous process can carry an equal or a higher
    /// numeric generation than the recovered connector's. Neither is
    /// admitted: the complete local identity, including the store session, is
    /// compared before the per-peer generation order.
    #[test]
    fn a_recovered_connector_refuses_tickets_of_the_previous_session() {
        let session = |n| LocalIdentity {
            store: StoreBinding {
                session: StoreSession::new(n).unwrap(),
                ..local().store
            },
            ..local()
        };
        let peer = PeerIdentity {
            node: NodeId::new(2).unwrap(),
            store: local().store.identity,
        };
        let pin = TlsPeer {
            identity: peer,
            certificate: include_bytes!("../../tests/fixtures/tls/node2.der").to_vec(),
            server_name: "node2.voteboat.test".into(),
        };
        let mut c = connector_with(session(2), [(peer.node, pin)].into());
        let request = |local, generation| ConnectRequest {
            ticket: ConnectTicket {
                local,
                peer,
                generation: SecureSessionGeneration::new(generation).unwrap(),
            },
            direction: ConnectDirection::Accept,
            deadline: MonoTime(1_000),
        };
        let refused = |c: &mut NativePeerConnector, r| {
            c.submit(r, MonoTime(1)).map_err(|e| e.reason).unwrap_err()
        };
        // Higher and equal numeric generations of the old session.
        for generation in [50_000, 1] {
            let old = request(session(1), generation);
            assert_eq!(refused(&mut c, old), ConnectError::WrongBinding);
        }
        // The recovered session's own first generation is admitted.
        c.submit(request(session(2), 1), MonoTime(1)).unwrap();
        assert_eq!(c.usage().requests, 1);
        // An old ticket replayed while that attempt is live is still refused
        // for its binding, not merely for overload.
        let old = request(session(1), 2);
        assert_eq!(refused(&mut c, old), ConnectError::WrongBinding);
        let live = request(session(2), 1).ticket;
        assert!(c.cancel(live));
        let done = c.poll(MonoTime(2), ConnectPollBudget::default()).unwrap();
        assert_eq!(done.len(), 1);
        assert_eq!(done[0].ticket, live);
        finish(c);
    }
}
