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
//! Caller-polled, single-peer QUIC endpoint. Reliable stream chunks implement
//! the existing authenticated byte-channel seam; QUIC ACKs are not durability.
use super::tls::{decode_hello, encode_hello, NativeTlsConfig, TlsPeer, HELLO};
use crate::{identity::SecureSessionGeneration, runtime::MonoTime, secure::*};
use bytes::{Bytes, BytesMut};
use quinn_proto::{
    crypto::rustls::{HandshakeData, QuicClientConfig, QuicServerConfig},
    *,
};
use std::{
    io,
    net::{SocketAddr, UdpSocket},
    sync::Arc,
    time::{Duration, Instant},
};
const MTU: usize = 1200;
const ALPN: &[u8] = b"voteboat-quic/1";

/// One exact, construction-authorized peer and one fresh local generation.
/// The host supplies a dedicated bound socket; discovery/shared listeners are
/// separate connector concerns. Remote address migration is disabled.
pub struct QuicSessionOptions {
    pub local: LocalIdentity,
    pub peer: TlsPeer,
    pub remote: SocketAddr,
    pub generation: SecureSessionGeneration,
    pub limits: SessionLimits,
}
pub struct NativeQuicSession {
    socket: UdpSocket,
    endpoint: Endpoint,
    connection: Option<(ConnectionHandle, Connection)>,
    options: QuicSessionOptions,
    state: SessionState,
    failure: Option<SessionError>,
    binding: Option<SessionBinding>,
    origin: Instant,
    initial: MonoTime,
    now: MonoTime,
    deadline: MonoTime,
    sending: Option<StreamId>,
    writing: Option<StreamId>,
    receiving: Option<StreamId>,
    hello_out: [u8; HELLO],
    written: usize,
    hello_in: [u8; HELLO],
    read: usize,
    authenticated: bool,
    handshake_read: usize,
    handshake_written: usize,
    pending: Vec<u8>,
    scratch: Vec<u8>,
    prefer_write: bool,
    receive: [u8; MTU],
    close_started: bool,
    peer_closed: bool,
}
impl NativeQuicSession {
    pub fn client(
        socket: UdpSocket,
        config: &NativeTlsConfig,
        options: QuicSessionOptions,
        now: MonoTime,
    ) -> Result<Self, SessionError> {
        Self::new(socket, config, options, now, true)
    }
    pub fn server(
        socket: UdpSocket,
        config: &NativeTlsConfig,
        options: QuicSessionOptions,
        now: MonoTime,
    ) -> Result<Self, SessionError> {
        Self::new(socket, config, options, now, false)
    }
    fn new(
        socket: UdpSocket,
        config: &NativeTlsConfig,
        options: QuicSessionOptions,
        now: MonoTime,
        client: bool,
    ) -> Result<Self, SessionError> {
        let limits = options.limits.validate()?;
        if options.local.node == options.peer.identity.node
            || options.peer.certificate.is_empty()
            || options.peer.certificate.len() > 65536
            || options.peer.server_name.is_empty()
            || options.peer.server_name.len() > 253
            || options.remote.port() == 0
            || options.remote.ip().is_unspecified()
            || options.remote.ip().is_multicast()
            || socket
                .local_addr()
                .map_err(|e| SessionError::Io(e.kind()))?
                .is_ipv4()
                != options.remote.is_ipv4()
        {
            return Err(SessionError::WrongPeer);
        }
        let deadline = MonoTime(
            now.0
                .checked_add(limits.handshake_timeout_ms)
                .ok_or(SessionError::InvalidLimits)?,
        );
        let mut transport = TransportConfig::default();
        transport
            .max_concurrent_bidi_streams(0u32.into())
            .max_concurrent_uni_streams(1u32.into())
            .stream_receive_window((limits.write_buffer_bytes as u32).into())
            .receive_window((limits.write_buffer_bytes as u32).into())
            .send_window(limits.write_buffer_bytes as u64)
            .crypto_buffer_size(limits.handshake_bytes)
            .initial_mtu(MTU as u16)
            .mtu_discovery_config(None)
            .enable_segmentation_offload(false)
            .datagram_receive_buffer_size(None)
            .datagram_send_buffer_size(0)
            .allow_spin(false);
        let transport = Arc::new(transport);
        let mut tls_client = (*config.client).clone();
        tls_client.alpn_protocols = vec![ALPN.to_vec()];
        let mut tls_server = (*config.server).clone();
        tls_server.alpn_protocols = vec![ALPN.to_vec()];
        let mut server_config = ServerConfig::with_crypto(Arc::new(
            QuicServerConfig::try_from(tls_server).map_err(|_| SessionError::InvalidCredentials)?,
        ));
        server_config
            .transport_config(transport.clone())
            .migration(false)
            .incoming_buffer_size(limits.handshake_bytes as u64)
            .incoming_buffer_size_total(limits.handshake_bytes as u64);
        let mut endpoint_config = EndpointConfig::default();
        endpoint_config
            .max_udp_payload_size(MTU as u16)
            .map_err(|_| SessionError::InvalidLimits)?;
        let mut endpoint = Endpoint::new(
            Arc::new(endpoint_config),
            (!client).then(|| Arc::new(server_config)),
            false,
            None,
        );
        let origin = Instant::now();
        let connection = if client {
            let mut client_config = ClientConfig::new(Arc::new(
                QuicClientConfig::try_from(tls_client)
                    .map_err(|_| SessionError::InvalidCredentials)?,
            ));
            client_config.transport_config(transport);
            Some(
                endpoint
                    .connect(
                        origin,
                        client_config,
                        options.remote,
                        &options.peer.server_name,
                    )
                    .map_err(|_| SessionError::InvalidCredentials)?,
            )
        } else {
            None
        };
        socket
            .set_nonblocking(true)
            .map_err(|e| SessionError::Io(e.kind()))?;
        let hello_out = encode_hello(options.local);
        Ok(Self {
            socket,
            endpoint,
            connection,
            options,
            state: SessionState::Handshaking,
            failure: None,
            binding: None,
            origin,
            initial: now,
            now,
            deadline,
            sending: None,
            writing: None,
            receiving: None,
            hello_out,
            written: 0,
            hello_in: [0; HELLO],
            read: 0,
            authenticated: false,
            handshake_read: 0,
            handshake_written: 0,
            pending: Vec::with_capacity(MTU),
            scratch: Vec::with_capacity(MTU),
            prefer_write: false,
            receive: [0; MTU],
            close_started: false,
            peer_closed: false,
        })
    }
    pub fn local_addr(&self) -> io::Result<SocketAddr> {
        self.socket.local_addr()
    }
    /// Deadline in the caller's initial monotonic domain; no hidden timer/reactor.
    pub fn next_deadline(&mut self) -> Option<MonoTime> {
        if matches!(self.state, SessionState::Closed | SessionState::Failed) {
            return None;
        }
        let protocol = self
            .connection
            .as_mut()
            .and_then(|(_, c)| c.poll_timeout())
            .and_then(|t| {
                self.initial
                    .0
                    .checked_add(
                        u64::try_from(
                            t.saturating_duration_since(self.origin)
                                .as_nanos()
                                .div_ceil(1_000_000),
                        )
                        .ok()?,
                    )
                    .map(MonoTime)
            });
        if self.state == SessionState::Handshaking {
            Some(protocol.map_or(self.deadline, |p| p.min(self.deadline)))
        } else {
            protocol
        }
    }
    fn instant(&self) -> Result<Instant, SessionError> {
        self.origin
            .checked_add(Duration::from_millis(self.now.0 - self.initial.0))
            .ok_or(SessionError::InvalidLimits)
    }
    fn fail<T>(&mut self, error: SessionError) -> Result<T, SessionError> {
        self.failure = Some(error);
        self.state = SessionState::Failed;
        Err(error)
    }
    fn events(&mut self, instant: Instant) -> Result<(), SessionError> {
        let Some((handle, connection)) = &mut self.connection else {
            return Ok(());
        };
        if connection
            .poll_timeout()
            .is_some_and(|deadline| deadline <= instant)
        {
            connection.handle_timeout(instant);
        }
        for _ in 0..64 {
            let Some(event) = connection.poll_endpoint_events() else {
                break;
            };
            if let Some(event) = self.endpoint.handle_event(*handle, event) {
                connection.handle_event(event);
            }
        }
        for _ in 0..64 {
            let Some(event) = connection.poll() else {
                break;
            };
            match event {
                Event::Stream(StreamEvent::Finished { id }) if self.sending == Some(id) => {
                    self.sending = None
                }
                Event::Stream(StreamEvent::Stopped { .. }) => return Err(SessionError::Truncated),
                Event::Connected => {
                    let certs = connection
                        .crypto_session()
                        .peer_identity()
                        .and_then(|v| {
                            v.downcast::<Vec<rustls::pki_types::CertificateDer<'static>>>()
                                .ok()
                        })
                        .ok_or(SessionError::Authentication)?;
                    if certs
                        .first()
                        .is_none_or(|cert| cert.as_ref() != self.options.peer.certificate)
                    {
                        return Err(SessionError::WrongPeer);
                    }
                    let data = connection
                        .crypto_session()
                        .handshake_data()
                        .and_then(|v| v.downcast::<HandshakeData>().ok())
                        .ok_or(SessionError::Authentication)?;
                    if data.protocol.as_deref() != Some(ALPN) || connection.accepted_0rtt() {
                        return Err(SessionError::IncompatibleProtocol);
                    }
                    self.authenticated = true;
                }
                Event::ConnectionLost { reason } => {
                    if matches!(&reason, ConnectionError::ApplicationClosed(close) if close.error_code == VarInt::from_u32(0))
                        && self.authenticated
                        && self.binding.is_some()
                        && self.sending.is_none()
                    {
                        self.peer_closed = true;
                        self.state = SessionState::Closed;
                    } else if !(self.close_started
                        && matches!(reason, ConnectionError::LocallyClosed))
                    {
                        return Err(if self.authenticated {
                            SessionError::Truncated
                        } else {
                            SessionError::Authentication
                        });
                    }
                }
                _ => (),
            }
        }
        Ok(())
    }
    fn write_chunk(&mut self, bytes: &[u8]) -> Result<usize, SessionError> {
        if bytes.is_empty() {
            return Ok(0);
        }
        if self.sending.is_some() {
            return Err(SessionError::WouldBlock);
        }
        let (_, c) = self.connection.as_mut().ok_or(SessionError::WouldBlock)?;
        let stream = if let Some(stream) = self.writing {
            stream
        } else {
            let stream = c.streams().open(Dir::Uni).ok_or(SessionError::WouldBlock)?;
            self.writing = Some(stream);
            stream
        };
        let n = c
            .send_stream(stream)
            .write(&bytes[..bytes.len().min(self.options.limits.write_buffer_bytes)])
            .map_err(|e| match e {
                WriteError::Blocked => SessionError::WouldBlock,
                _ => SessionError::Truncated,
            })?;
        c.send_stream(stream)
            .finish()
            .map_err(|_| SessionError::Truncated)?;
        self.sending = Some(stream);
        self.writing = None;
        Ok(n)
    }
    fn read_chunk(&mut self, bytes: &mut [u8]) -> Result<usize, SessionError> {
        if bytes.is_empty() {
            return Ok(0);
        }
        let Some((_, c)) = &mut self.connection else {
            return Err(SessionError::WouldBlock);
        };
        // At most one peer stream can be open. Accept/read in stream-ID order;
        // FIN ends a chunk, not the logical byte channel.
        for _ in 0..2 {
            let stream = match self.receiving {
                Some(id) => id,
                None => match c.streams().accept(Dir::Uni) {
                    Some(id) => {
                        self.receiving = Some(id);
                        id
                    }
                    None => {
                        return if self.peer_closed {
                            Ok(0)
                        } else {
                            Err(SessionError::WouldBlock)
                        }
                    }
                },
            };
            let mut recv = c.recv_stream(stream);
            let mut chunks = recv.read(true).map_err(|_| SessionError::Truncated)?;
            let result = chunks.next(bytes.len());
            let _ = chunks.finalize(); // The next poll sends flow-control updates.
            match result {
                Ok(Some(chunk)) => {
                    bytes[..chunk.bytes.len()].copy_from_slice(&chunk.bytes);
                    return Ok(chunk.bytes.len());
                }
                Ok(None) => self.receiving = None,
                Err(ReadError::Blocked) => return Err(SessionError::WouldBlock),
                _ => return Err(SessionError::Truncated),
            }
        }
        Err(SessionError::WouldBlock)
    }
    fn hello(&mut self) -> Result<(), SessionError> {
        if !self.authenticated || self.state != SessionState::Handshaking {
            return Ok(());
        }
        if self.written < HELLO {
            let hello = self.hello_out;
            match self.write_chunk(&hello[self.written..]) {
                Ok(n) => self.written += n,
                Err(SessionError::WouldBlock) => (),
                Err(e) => return Err(e),
            }
        }
        if self.read < HELLO {
            let mut bytes = [0; HELLO];
            match self.read_chunk(&mut bytes[..HELLO - self.read]) {
                Ok(0) => return Err(SessionError::Truncated),
                Ok(n) => {
                    self.hello_in[self.read..self.read + n].copy_from_slice(&bytes[..n]);
                    self.read += n;
                }
                Err(SessionError::WouldBlock) => (),
                Err(e) => return Err(e),
            }
        }
        if self.read == HELLO {
            if let Some(stream) = self.receiving {
                let (_, connection) = self.connection.as_mut().unwrap();
                let mut recv = connection.recv_stream(stream);
                let mut chunks = recv.read(true).map_err(|_| SessionError::Truncated)?;
                let end = chunks.next(1);
                let _ = chunks.finalize();
                match end {
                    Ok(None) => self.receiving = None,
                    Err(ReadError::Blocked) => (),
                    _ => return Err(SessionError::IncompatibleProtocol),
                }
            }
        }
        if self.written == HELLO
            && self.read == HELLO
            && self.receiving.is_none()
            && self.sending.is_none()
        {
            self.binding = Some(decode_hello(
                &self.hello_in,
                self.options.local,
                self.options.peer.identity,
                self.options.generation,
            )?);
            self.state = SessionState::Ready;
        }
        Ok(())
    }
    fn poll_inner(
        &mut self,
        now: MonoTime,
        budget: SessionPollBudget,
    ) -> Result<SessionProgress, SessionError> {
        if let Some(error) = self.failure {
            return Err(error);
        }
        let budget = budget.validate()?;
        if now < self.now {
            return Err(SessionError::TimeWentBack);
        }
        self.now = now;
        if self.state == SessionState::Closed {
            return Ok(SessionProgress::default());
        }
        if self.state == SessionState::Handshaking && now >= self.deadline {
            return Err(SessionError::Timeout);
        }
        let instant = self.instant()?;
        let was_ready = self.binding.is_some();
        self.events(instant)?;
        self.hello()?;
        let mut progress = SessionProgress::default();
        for _ in 0..budget.io_calls {
            // Alternate read/write; at most one datagram is retained on EAGAIN.
            let write = self.prefer_write;
            self.prefer_write = !self.prefer_write;
            if !write {
                if budget.read_bytes - progress.read_bytes < MTU {
                    continue;
                }
                progress.io_calls += 1;
                match self.socket.recv_from(&mut self.receive) {
                    Ok((n, remote)) => {
                        progress.read_bytes += n;
                        if remote != self.options.remote {
                            continue;
                        }
                        if self.state == SessionState::Handshaking {
                            self.handshake_read += n;
                            if self.handshake_read > self.options.limits.handshake_bytes {
                                return Err(SessionError::HandshakeTooLarge);
                            }
                        }
                        self.scratch.clear();
                        let event = self.endpoint.handle(
                            instant,
                            remote,
                            None,
                            None,
                            BytesMut::from(&self.receive[..n]),
                            &mut self.scratch,
                        );
                        match event {
                            Some(DatagramEvent::ConnectionEvent(handle, event)) => {
                                if let Some((active, c)) = &mut self.connection {
                                    if *active == handle {
                                        c.handle_event(event);
                                    }
                                }
                            }
                            Some(DatagramEvent::NewConnection(incoming))
                                if self.connection.is_none() =>
                            {
                                self.connection = Some(
                                    self.endpoint
                                        .accept(incoming, instant, &mut self.scratch, None)
                                        .map_err(|_| SessionError::Authentication)?,
                                );
                            }
                            Some(DatagramEvent::NewConnection(incoming)) => {
                                self.endpoint.ignore(incoming)
                            }
                            Some(DatagramEvent::Response(_)) => (),
                            None => (),
                        }
                        self.events(instant)?;
                        self.hello()?;
                    }
                    Err(e) if e.kind() == io::ErrorKind::WouldBlock => (),
                    Err(e) => return Err(SessionError::Io(e.kind())),
                }
            } else {
                if self.pending.is_empty() {
                    if let Some((_, c)) = &mut self.connection {
                        if let Some(transmit) = c.poll_transmit(instant, 1, &mut self.pending) {
                            if transmit.destination != self.options.remote
                                || transmit.size > MTU
                                || transmit.segment_size.is_some()
                            {
                                return Err(SessionError::Failed);
                            }
                            self.pending.truncate(transmit.size);
                        }
                    }
                }
                if self.pending.is_empty()
                    || self.pending.len() > budget.write_bytes - progress.written_bytes
                {
                    continue;
                }
                progress.io_calls += 1;
                match self.socket.send_to(&self.pending, self.options.remote) {
                    Ok(n) if n == self.pending.len() => {
                        progress.written_bytes += n;
                        if self.state == SessionState::Handshaking {
                            self.handshake_written += n;
                        }
                        self.pending.clear();
                    }
                    Ok(_) => return Err(SessionError::Failed),
                    Err(e) if e.kind() == io::ErrorKind::WouldBlock => (),
                    Err(e) => return Err(SessionError::Io(e.kind())),
                }
            }
            if self.handshake_read > self.options.limits.handshake_bytes
                || self.handshake_written > self.options.limits.handshake_bytes
            {
                return Err(SessionError::HandshakeTooLarge);
            }
        }
        if self.state == SessionState::Closing && self.sending.is_none() && !self.close_started {
            if let Some((_, c)) = &mut self.connection {
                c.close(instant, 0u32.into(), Bytes::new());
            }
            self.close_started = true;
        }
        if self.close_started
            && self.connection.as_ref().is_none_or(|(_, c)| c.is_drained())
            && self.pending.is_empty()
        {
            self.state = SessionState::Closed;
        }
        progress.became_ready = !was_ready && self.binding.is_some();
        Ok(progress)
    }
}
impl SecureSession for NativeQuicSession {
    fn security(&self) -> SessionSecurity {
        SessionSecurity::Authenticated
    }
    fn state(&self) -> SessionState {
        self.state
    }
    fn binding(&self) -> Option<SessionBinding> {
        self.binding
    }
    fn limits(&self) -> SessionLimits {
        self.options.limits
    }
    fn poll(
        &mut self,
        now: MonoTime,
        budget: SessionPollBudget,
    ) -> Result<SessionProgress, SessionError> {
        budget.validate()?;
        match self.poll_inner(now, budget) {
            Ok(p) => Ok(p),
            Err(e) => self.fail(e),
        }
    }
    fn read_plaintext(&mut self, bytes: &mut [u8]) -> Result<usize, SessionError> {
        if let Some(e) = self.failure {
            return Err(e);
        }
        if self.binding.is_none() {
            return Err(SessionError::NotReady);
        }
        match self.read_chunk(bytes) {
            Err(error @ SessionError::Truncated) => self.fail(error),
            other => other,
        }
    }
    fn write_plaintext(&mut self, bytes: &[u8]) -> Result<usize, SessionError> {
        if let Some(e) = self.failure {
            return Err(e);
        }
        if self.state != SessionState::Ready {
            return Err(if self.binding.is_some() {
                SessionError::Closed
            } else {
                SessionError::NotReady
            });
        }
        match self.write_chunk(bytes) {
            Err(error @ SessionError::Truncated) => self.fail(error),
            other => other,
        }
    }
    fn is_flushed(&self) -> bool {
        self.failure.is_none()
            && self.sending.is_none()
            && self.writing.is_none()
            && self.pending.is_empty()
    }
    fn close(&mut self) {
        if matches!(self.state, SessionState::Handshaking | SessionState::Ready) {
            self.state = SessionState::Closing;
        }
    }
    fn revoke(&mut self) {
        let _: Result<(), _> = self.fail(SessionError::Revoked);
    }
}
