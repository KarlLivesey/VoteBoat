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
//! TLS 1.3 mutual authentication using pinned Rustls/ring. No global crypto
//! provider, executor, listener or connection pool is constructed.
use crate::{identity::*, runtime::MonoTime, secure::*};
use rustls::{
    pki_types::{CertificateDer, PrivateKeyDer, ServerName},
    ClientConfig, ClientConnection, Connection, RootCertStore, ServerConfig, ServerConnection,
};
use std::{
    io::{self, Read, Write},
    net::TcpStream,
    sync::Arc,
};

pub(crate) const HELLO: usize = 52;
const ALPN: &[u8] = b"voteboat/1";
/// Public DER material; private keys are consumed and never printed by this API.
pub struct TlsCredentials {
    pub roots: Vec<Vec<u8>>,
    pub certificate_chain: Vec<Vec<u8>>,
    pub private_key: Vec<u8>,
}
#[derive(Clone)]
pub struct NativeTlsConfig {
    pub(crate) client: Arc<ClientConfig>,
    pub(crate) server: Arc<ServerConfig>,
    wire_version: u16,
}
impl NativeTlsConfig {
    pub fn new(material: TlsCredentials) -> Result<Self, SessionError> {
        if material.roots.is_empty()
            || material.roots.len() > 64
            || material.certificate_chain.is_empty()
            || material.certificate_chain.len() > 8
            || material.private_key.is_empty()
            || material.private_key.len() > 64 * 1024
            || material
                .roots
                .iter()
                .chain(&material.certificate_chain)
                .any(|c| c.is_empty() || c.len() > 64 * 1024)
            || material
                .roots
                .iter()
                .chain(&material.certificate_chain)
                .map(Vec::len)
                .sum::<usize>()
                > 1024 * 1024
        {
            return Err(SessionError::InvalidCredentials);
        }
        let provider = Arc::new(rustls::crypto::ring::default_provider());
        let mut roots = RootCertStore::empty();
        for der in material.roots {
            roots
                .add(CertificateDer::from(der))
                .map_err(|_| SessionError::InvalidCredentials)?;
        }
        let roots = Arc::new(roots);
        let chain = material
            .certificate_chain
            .into_iter()
            .map(CertificateDer::from)
            .collect::<Vec<_>>();
        let key = PrivateKeyDer::try_from(material.private_key)
            .map_err(|_| SessionError::InvalidCredentials)?;
        let mut client = ClientConfig::builder_with_provider(provider.clone())
            .with_protocol_versions(&[&rustls::version::TLS13])
            .map_err(|_| SessionError::InvalidCredentials)?
            .with_root_certificates(roots.clone())
            .with_client_auth_cert(chain.clone(), key.clone_key())
            .map_err(|_| SessionError::InvalidCredentials)?;
        client.alpn_protocols = vec![ALPN.to_vec()];
        client.enable_early_data = false;
        client.resumption = rustls::client::Resumption::disabled();
        let verifier =
            rustls::server::WebPkiClientVerifier::builder_with_provider(roots, provider.clone())
                .build()
                .map_err(|_| SessionError::InvalidCredentials)?;
        let mut server = ServerConfig::builder_with_provider(provider)
            .with_protocol_versions(&[&rustls::version::TLS13])
            .map_err(|_| SessionError::InvalidCredentials)?
            .with_client_cert_verifier(verifier)
            .with_single_cert(chain, key)
            .map_err(|_| SessionError::InvalidCredentials)?;
        server.alpn_protocols = vec![ALPN.to_vec()];
        server.send_tls13_tickets = 0;
        server.max_early_data_size = 0;
        Ok(Self {
            client: Arc::new(client),
            server: Arc::new(server),
            wire_version: 1,
        })
    }
    /// Select one exact native message format (1–3) for future sessions.
    /// Peers must select the same version; there is no automatic downgrade.
    pub fn with_wire_version(mut self, version: u16) -> Result<Self, SessionError> {
        if !(1..=3).contains(&version) {
            return Err(SessionError::IncompatibleProtocol);
        }
        self.wire_version = version;
        Ok(self)
    }
    pub fn wire_version(&self) -> u16 {
        self.wire_version
    }
}
/// Trusted construction-time certificate pin and stable node/store identity.
/// Certificate bytes are public, not a private key. The server name is verified
/// by Rustls on client connections in addition to the exact peer certificate pin.
#[derive(Clone)]
pub struct TlsPeer {
    pub identity: PeerIdentity,
    pub certificate: Vec<u8>,
    pub server_name: String,
}

pub struct NativeTlsSession<I: Read + Write> {
    io: Option<I>,
    connection: Connection,
    local: LocalIdentity,
    peer: TlsPeer,
    generation: SecureSessionGeneration,
    wire_version: u16,
    binding: Option<SessionBinding>,
    limits: SessionLimits,
    state: SessionState,
    failure: Option<SessionError>,
    deadline: MonoTime,
    now: MonoTime,
    hello_out: [u8; HELLO],
    written: usize,
    hello_in: [u8; HELLO],
    read: usize,
    authenticated: bool,
    handshake_read: usize,
    handshake_written: usize,
    peer_closed: bool,
}
impl<I: Read + Write> NativeTlsSession<I> {
    /// Generic I/O must be nonblocking and honor Read/Write byte counts. Writes
    /// must advance the stream without an additional host flush. Any host-side
    /// buffers need separate accounting. It may share its host reactor; this
    /// session owns only the supplied logical handle.
    pub fn client(
        io: I,
        config: &NativeTlsConfig,
        local: LocalIdentity,
        peer: TlsPeer,
        generation: SecureSessionGeneration,
        limits: SessionLimits,
        now: MonoTime,
    ) -> Result<Self, SessionError> {
        let name = ServerName::try_from(peer.server_name.clone())
            .map_err(|_| SessionError::InvalidCredentials)?;
        let connection = ClientConnection::new(config.client.clone(), name)
            .map_err(|_| SessionError::InvalidCredentials)?;
        Self::new(
            io,
            connection.into(),
            local,
            peer,
            generation,
            limits,
            now,
            config.wire_version(),
        )
    }
    pub fn server(
        io: I,
        config: &NativeTlsConfig,
        local: LocalIdentity,
        peer: TlsPeer,
        generation: SecureSessionGeneration,
        limits: SessionLimits,
        now: MonoTime,
    ) -> Result<Self, SessionError> {
        let connection = ServerConnection::new(config.server.clone())
            .map_err(|_| SessionError::InvalidCredentials)?;
        Self::new(
            io,
            connection.into(),
            local,
            peer,
            generation,
            limits,
            now,
            config.wire_version(),
        )
    }
    #[allow(clippy::too_many_arguments)] // Existing session scope plus private selected format.
    fn new(
        io: I,
        mut connection: Connection,
        local: LocalIdentity,
        peer: TlsPeer,
        generation: SecureSessionGeneration,
        limits: SessionLimits,
        now: MonoTime,
        wire_version: u16,
    ) -> Result<Self, SessionError> {
        let limits = limits.validate()?;
        if local.node == peer.identity.node
            || peer.certificate.is_empty()
            || peer.certificate.len() > 64 * 1024
            || peer.server_name.len() > 253
        {
            return Err(SessionError::WrongPeer);
        }
        let deadline = MonoTime(
            now.0
                .checked_add(limits.handshake_timeout_ms)
                .ok_or(SessionError::InvalidLimits)?,
        );
        connection.set_buffer_limit(Some(limits.write_buffer_bytes));
        let hello = encode_hello(local, wire_version);
        Ok(Self {
            io: Some(io),
            connection,
            local,
            peer,
            generation,
            wire_version,
            binding: None,
            limits,
            state: SessionState::Handshaking,
            failure: None,
            deadline,
            now,
            hello_out: hello,
            written: 0,
            hello_in: [0; HELLO],
            read: 0,
            authenticated: false,
            handshake_read: 0,
            handshake_written: 0,
            peer_closed: false,
        })
    }
    fn fail<T>(&mut self, error: SessionError) -> Result<T, SessionError> {
        self.state = SessionState::Failed;
        self.failure = Some(error);
        Err(error)
    }
    fn advance_hello(&mut self) -> Result<bool, SessionError> {
        if self.state != SessionState::Handshaking || self.connection.is_handshaking() {
            return Ok(false);
        }
        if !self.authenticated {
            if self.connection.protocol_version() != Some(rustls::ProtocolVersion::TLSv1_3)
                || self.connection.alpn_protocol() != Some(ALPN)
            {
                return Err(SessionError::IncompatibleProtocol);
            }
            let cert = self
                .connection
                .peer_certificates()
                .and_then(|chain| chain.first())
                .ok_or(SessionError::Authentication)?;
            if cert.as_ref() != self.peer.certificate {
                return Err(SessionError::WrongPeer);
            }
            self.authenticated = true;
        }
        let mut progress = false;
        if self.written < HELLO {
            match self
                .connection
                .writer()
                .write(&self.hello_out[self.written..])
            {
                Ok(n) => {
                    self.written += n;
                    progress |= n > 0;
                }
                Err(e) if e.kind() == io::ErrorKind::WouldBlock => (),
                Err(e) => return Err(SessionError::Io(e.kind())),
            }
        }
        if self.read < HELLO {
            match self
                .connection
                .reader()
                .read(&mut self.hello_in[self.read..])
            {
                Ok(0) => return Err(SessionError::Truncated),
                Ok(n) => {
                    self.read += n;
                    progress = true;
                }
                Err(e) if e.kind() == io::ErrorKind::WouldBlock => (),
                Err(e) => return Err(SessionError::Io(e.kind())),
            }
        }
        if self.read == HELLO && self.written == HELLO && !self.connection.wants_write() {
            self.binding = Some(decode_hello(
                &self.hello_in,
                self.local,
                self.peer.identity,
                self.generation,
                self.wire_version,
            )?);
            self.state = SessionState::Ready;
            progress = true;
        }
        Ok(progress)
    }
    /// Returns only this owned I/O handle after close/failure. It does not close
    /// a host listener/reactor. Failed external work remains uncertain.
    pub fn take_io(&mut self) -> Result<I, SessionError> {
        if !matches!(self.state, SessionState::Closed | SessionState::Failed) {
            return Err(SessionError::NotReady);
        }
        self.io.take().ok_or(SessionError::Closed)
    }
}
impl NativeTlsSession<TcpStream> {
    pub fn client_tcp(
        stream: TcpStream,
        config: &NativeTlsConfig,
        local: LocalIdentity,
        peer: TlsPeer,
        generation: SecureSessionGeneration,
        limits: SessionLimits,
        now: MonoTime,
    ) -> Result<Self, SessionError> {
        stream
            .set_nonblocking(true)
            .map_err(|e| SessionError::Io(e.kind()))?;
        Self::client(stream, config, local, peer, generation, limits, now)
    }
    pub fn server_tcp(
        stream: TcpStream,
        config: &NativeTlsConfig,
        local: LocalIdentity,
        peer: TlsPeer,
        generation: SecureSessionGeneration,
        limits: SessionLimits,
        now: MonoTime,
    ) -> Result<Self, SessionError> {
        stream
            .set_nonblocking(true)
            .map_err(|e| SessionError::Io(e.kind()))?;
        Self::server(stream, config, local, peer, generation, limits, now)
    }
}
struct LimitedIo<'a, I> {
    io: &'a mut I,
    budget: SessionPollBudget,
    progress: SessionProgress,
    blocked_read: bool,
    blocked_write: bool,
}
impl<I: Read> Read for LimitedIo<'_, I> {
    fn read(&mut self, bytes: &mut [u8]) -> io::Result<usize> {
        let count = bytes.len().min(
            self.budget
                .read_bytes
                .saturating_sub(self.progress.read_bytes),
        );
        if self.blocked_read || self.progress.io_calls == self.budget.io_calls || count == 0 {
            return Err(io::ErrorKind::WouldBlock.into());
        }
        self.progress.io_calls += 1;
        match self.io.read(&mut bytes[..count]) {
            Ok(n) if n <= count => {
                self.progress.read_bytes += n;
                Ok(n)
            }
            Ok(_) => Err(io::ErrorKind::InvalidData.into()),
            Err(e) => {
                if e.kind() == io::ErrorKind::WouldBlock {
                    self.blocked_read = true;
                }
                Err(e)
            }
        }
    }
}
impl<I: Write> Write for LimitedIo<'_, I> {
    fn write(&mut self, bytes: &[u8]) -> io::Result<usize> {
        let count = bytes.len().min(
            self.budget
                .write_bytes
                .saturating_sub(self.progress.written_bytes),
        );
        if self.blocked_write || self.progress.io_calls == self.budget.io_calls || count == 0 {
            return Err(io::ErrorKind::WouldBlock.into());
        }
        self.progress.io_calls += 1;
        match self.io.write(&bytes[..count]) {
            Ok(n) if n <= count => {
                self.progress.written_bytes += n;
                Ok(n)
            }
            Ok(_) => Err(io::ErrorKind::InvalidData.into()),
            Err(e) => {
                if e.kind() == io::ErrorKind::WouldBlock {
                    self.blocked_write = true;
                }
                Err(e)
            }
        }
    }
    fn flush(&mut self) -> io::Result<()> {
        Ok(())
    }
}
impl<I: Read + Write> SecureSession for NativeTlsSession<I> {
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
        self.limits
    }
    fn poll(
        &mut self,
        now: MonoTime,
        mut budget: SessionPollBudget,
    ) -> Result<SessionProgress, SessionError> {
        budget.validate()?;
        if let Some(error) = self.failure {
            return Err(error);
        }
        if now < self.now {
            return self.fail(SessionError::TimeWentBack);
        }
        self.now = now;
        if self.state == SessionState::Handshaking && now >= self.deadline {
            return self.fail(SessionError::Timeout);
        }
        if self.state == SessionState::Closed {
            return Ok(SessionProgress::default());
        }
        let before = self.state;
        if let Err(e) = self.advance_hello() {
            return self.fail(e);
        }
        if self.state == SessionState::Handshaking {
            budget.read_bytes = budget.read_bytes.min(
                self.limits
                    .handshake_bytes
                    .saturating_sub(self.handshake_read),
            );
            budget.write_bytes = budget.write_bytes.min(
                self.limits
                    .handshake_bytes
                    .saturating_sub(self.handshake_written),
            );
        }
        let result = (|| {
            let mut io = LimitedIo {
                io: self.io.as_mut().ok_or(SessionError::Closed)?,
                budget,
                progress: SessionProgress::default(),
                blocked_read: false,
                blocked_write: false,
            };
            for _ in 0..budget.io_calls {
                let calls = io.progress.io_calls;
                if self.connection.wants_write() {
                    match self.connection.write_tls(&mut io) {
                        Ok(0) => return Err(SessionError::Io(io::ErrorKind::WriteZero)),
                        Ok(_) => (),
                        Err(e) if e.kind() == io::ErrorKind::WouldBlock => (),
                        Err(e) if e.kind() == io::ErrorKind::Interrupted => (),
                        Err(e) => return Err(SessionError::Io(e.kind())),
                    }
                }
                if self.connection.wants_read() {
                    match self.connection.read_tls(&mut io) {
                        Ok(0) => return Err(SessionError::Truncated),
                        Ok(_) => {
                            let state = self
                                .connection
                                .process_new_packets()
                                .map_err(|_| SessionError::Authentication)?;
                            self.peer_closed |= state.peer_has_closed();
                        }
                        Err(e)
                            if matches!(
                                e.kind(),
                                io::ErrorKind::WouldBlock | io::ErrorKind::Interrupted
                            ) => {}
                        Err(e) => return Err(SessionError::Io(e.kind())),
                    }
                }
                if calls == io.progress.io_calls || io.progress.io_calls == budget.io_calls {
                    break;
                }
            }
            Ok(io.progress)
        })();
        let mut progress = match result {
            Ok(p) => p,
            Err(e) => return self.fail(e),
        };
        if before == SessionState::Handshaking {
            self.handshake_read += progress.read_bytes;
            self.handshake_written += progress.written_bytes;
        }
        if let Err(e) = self.advance_hello() {
            return self.fail(e);
        }
        if self.state == SessionState::Handshaking
            && (self.handshake_read >= self.limits.handshake_bytes
                || self.handshake_written >= self.limits.handshake_bytes)
        {
            return self.fail(SessionError::HandshakeTooLarge);
        }
        if self.peer_closed && self.state == SessionState::Ready {
            self.close();
        }
        if self.state == SessionState::Closing && !self.connection.wants_write() {
            self.state = SessionState::Closed;
        }
        progress.became_ready = before != SessionState::Ready && self.state == SessionState::Ready;
        Ok(progress)
    }
    fn read_plaintext(&mut self, bytes: &mut [u8]) -> Result<usize, SessionError> {
        if let Some(e) = self.failure {
            return Err(e);
        }
        if self.binding.is_none() {
            return Err(SessionError::NotReady);
        }
        match self.connection.reader().read(bytes) {
            Ok(n) => Ok(n),
            Err(e) if e.kind() == io::ErrorKind::WouldBlock => Err(SessionError::WouldBlock),
            Err(e) if e.kind() == io::ErrorKind::UnexpectedEof => {
                self.fail(SessionError::Truncated)
            }
            Err(e) => self.fail(SessionError::Io(e.kind())),
        }
    }
    fn write_plaintext(&mut self, bytes: &[u8]) -> Result<usize, SessionError> {
        if let Some(e) = self.failure {
            return Err(e);
        }
        if self.state != SessionState::Ready || self.peer_closed {
            return Err(
                if matches!(self.state, SessionState::Closed | SessionState::Closing)
                    || self.peer_closed
                {
                    SessionError::Closed
                } else {
                    SessionError::NotReady
                },
            );
        }
        match self.connection.writer().write(bytes) {
            Ok(0) if !bytes.is_empty() => Err(SessionError::WouldBlock),
            Ok(n) => Ok(n),
            Err(e) if e.kind() == io::ErrorKind::WouldBlock => Err(SessionError::WouldBlock),
            Err(e) => self.fail(SessionError::Io(e.kind())),
        }
    }
    fn is_flushed(&self) -> bool {
        !self.connection.wants_write()
    }
    fn close(&mut self) {
        if !matches!(
            self.state,
            SessionState::Closed | SessionState::Failed | SessionState::Closing
        ) {
            self.connection.send_close_notify();
            self.state = SessionState::Closing;
        }
    }
    fn revoke(&mut self) {
        self.state = SessionState::Failed;
        self.failure = Some(SessionError::Revoked);
    }
}

pub(crate) fn encode_hello(local: LocalIdentity, wire_version: u16) -> [u8; HELLO] {
    let mut hello = [0; HELLO];
    hello[..8].copy_from_slice(b"VBSESS01");
    hello[8..10].copy_from_slice(&wire_version.to_le_bytes());
    hello[12..20].copy_from_slice(&local.node.get().to_le_bytes());
    hello[20..36].copy_from_slice(&local.store.identity.id.get().to_le_bytes());
    hello[36..44].copy_from_slice(&local.store.identity.incarnation.get().to_le_bytes());
    hello[44..52].copy_from_slice(&local.store.session.get().to_le_bytes());
    hello
}

pub(crate) fn decode_hello(
    h: &[u8; HELLO],
    local: LocalIdentity,
    peer: PeerIdentity,
    generation: SecureSessionGeneration,
    wire_version: u16,
) -> Result<SessionBinding, SessionError> {
    if &h[..8] != b"VBSESS01"
        || u16::from_le_bytes(h[8..10].try_into().unwrap()) != wire_version
        || h[10..12] != [0, 0]
    {
        return Err(SessionError::IncompatibleProtocol);
    }
    let node = NodeId::new(u64::from_le_bytes(h[12..20].try_into().unwrap()))
        .ok_or(SessionError::WrongPeer)?;
    let store = StoreIdentity {
        id: StoreId::new(u128::from_le_bytes(h[20..36].try_into().unwrap()))
            .ok_or(SessionError::WrongPeer)?,
        incarnation: StoreIncarnation::new(u64::from_le_bytes(h[36..44].try_into().unwrap()))
            .ok_or(SessionError::WrongPeer)?,
    };
    if peer != (PeerIdentity { node, store }) {
        return Err(SessionError::WrongPeer);
    }
    let session = StoreSession::new(u64::from_le_bytes(h[44..52].try_into().unwrap()))
        .ok_or(SessionError::WrongPeer)?;
    Ok(SessionBinding {
        local,
        peer: LocalIdentity {
            node,
            store: StoreBinding {
                identity: store,
                session,
            },
        },
        generation,
        wire_version,
    })
}
