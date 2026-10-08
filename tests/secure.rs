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
use voteboat::{identity::*, runtime::MonoTime, secure::*};
fn identity(n: u64, session: u64) -> LocalIdentity {
    LocalIdentity {
        node: NodeId::new(n).unwrap(),
        store: StoreBinding {
            identity: StoreIdentity {
                id: StoreId::new(n.into()).unwrap(),
                incarnation: StoreIncarnation::new(1).unwrap(),
            },
            session: StoreSession::new(session).unwrap(),
        },
    }
}
/// A deliberately insecure host fixture exercises selection, not cryptography.
struct Simulator;
impl SecureSession for Simulator {
    fn security(&self) -> SessionSecurity {
        SessionSecurity::SimulatorOnly
    }
    fn state(&self) -> SessionState {
        SessionState::Ready
    }
    fn binding(&self) -> Option<SessionBinding> {
        Some(SessionBinding {
            local: identity(1, 2),
            peer: identity(2, 7),
            generation: SecureSessionGeneration::new(1).unwrap(),
            wire_version: 1,
        })
    }
    fn limits(&self) -> SessionLimits {
        SessionLimits::default()
    }
    fn poll(
        &mut self,
        _: MonoTime,
        budget: SessionPollBudget,
    ) -> Result<SessionProgress, SessionError> {
        budget.validate()?;
        Ok(SessionProgress::default())
    }
    fn read_plaintext(&mut self, _: &mut [u8]) -> Result<usize, SessionError> {
        Err(SessionError::WouldBlock)
    }
    fn write_plaintext(&mut self, _: &[u8]) -> Result<usize, SessionError> {
        Err(SessionError::WouldBlock)
    }
    fn is_flushed(&self) -> bool {
        true
    }
    fn close(&mut self) {}
    fn revoke(&mut self) {}
}
#[test]
fn production_rejects_simulation_even_with_ready_identity() {
    assert_eq!(
        require_authenticated(&Simulator),
        Err(SessionError::InsecureProvider)
    );
    let binding = Simulator.binding().unwrap();
    assert_eq!(binding.outgoing().from, binding.incoming().to);
    let erased: &dyn SecureSession = &Simulator;
    assert_eq!(
        require_authenticated(erased),
        Err(SessionError::InsecureProvider)
    );
}
#[test]
fn session_limits_and_poll_budgets_are_checked_without_native_provider() {
    assert!(SessionLimits::default().validate().is_ok());
    assert_eq!(
        SessionLimits {
            handshake_bytes: 0,
            ..SessionLimits::default()
        }
        .validate()
        .unwrap_err(),
        SessionError::InvalidLimits
    );
    assert_eq!(
        SessionPollBudget {
            io_calls: 1025,
            ..SessionPollBudget::default()
        }
        .validate()
        .unwrap_err(),
        SessionError::InvalidLimits
    );
}

#[cfg(feature = "tls")]
mod tls {
    use super::*;
    use std::{
        collections::VecDeque,
        io::{self, Read, Write},
        net::{TcpListener, TcpStream},
        sync::{Arc, Mutex},
        time::{Duration, Instant},
    };
    use voteboat::{
        native::{tls::*, wire::NativeWireCodec},
        raft::{Message, RequestContext, Rpc},
        wire::*,
    };
    const ROOT: &[u8] = include_bytes!("fixtures/tls/ca.der");
    fn cert(n: u64) -> &'static [u8] {
        match n {
            1 => include_bytes!("fixtures/tls/node1.der"),
            2 => include_bytes!("fixtures/tls/node2.der"),
            _ => include_bytes!("fixtures/tls/node3.der"),
        }
    }
    fn credentials(n: u64) -> TlsCredentials {
        let key: &[u8] = match n {
            1 => include_bytes!("fixtures/tls/node1-key.der"),
            2 => include_bytes!("fixtures/tls/node2-key.der"),
            _ => include_bytes!("fixtures/tls/node3-key.der"),
        };
        TlsCredentials {
            roots: vec![ROOT.to_vec()],
            certificate_chain: vec![cert(n).to_vec()],
            private_key: key.to_vec(),
        }
    }
    fn config(n: u64) -> NativeTlsConfig {
        NativeTlsConfig::new(credentials(n)).unwrap()
    }
    fn peer(n: u64) -> TlsPeer {
        TlsPeer {
            identity: PeerIdentity {
                node: identity(n, 1).node,
                store: identity(n, 1).store.identity,
            },
            certificate: cert(n).to_vec(),
            server_name: format!("node{n}.voteboat.test"),
        }
    }
    struct Channel {
        bytes: VecDeque<u8>,
        closed: bool,
    }
    struct Memory {
        incoming: Arc<Mutex<Channel>>,
        outgoing: Arc<Mutex<Channel>>,
        chunk: usize,
        interrupt: bool,
    }
    impl Drop for Memory {
        fn drop(&mut self) {
            self.outgoing.lock().unwrap().closed = true;
        }
    }
    impl Read for Memory {
        fn read(&mut self, bytes: &mut [u8]) -> io::Result<usize> {
            if self.interrupt {
                self.interrupt = false;
                return Err(io::ErrorKind::Interrupted.into());
            }
            let mut input = self.incoming.lock().unwrap();
            let n = bytes.len().min(self.chunk).min(input.bytes.len());
            if n == 0 && !input.closed {
                return Err(io::ErrorKind::WouldBlock.into());
            }
            for byte in &mut bytes[..n] {
                *byte = input.bytes.pop_front().unwrap();
            }
            Ok(n)
        }
    }
    impl Write for Memory {
        fn write(&mut self, bytes: &[u8]) -> io::Result<usize> {
            let mut output = self.outgoing.lock().unwrap();
            if output.closed {
                return Err(io::ErrorKind::BrokenPipe.into());
            }
            let n = bytes.len().min(self.chunk).min(32768 - output.bytes.len());
            if n == 0 {
                return Err(io::ErrorKind::WouldBlock.into());
            }
            output.bytes.extend(&bytes[..n]);
            Ok(n)
        }
        fn flush(&mut self) -> io::Result<()> {
            Ok(())
        }
    }
    fn duplex(chunk: usize) -> (Memory, Memory) {
        let a = Arc::new(Mutex::new(Channel {
            bytes: VecDeque::new(),
            closed: false,
        }));
        let b = Arc::new(Mutex::new(Channel {
            bytes: VecDeque::new(),
            closed: false,
        }));
        (
            Memory {
                incoming: a.clone(),
                outgoing: b.clone(),
                chunk,
                interrupt: true,
            },
            Memory {
                incoming: b,
                outgoing: a,
                chunk,
                interrupt: true,
            },
        )
    }
    fn sessions(chunk: usize) -> (NativeTlsSession<Memory>, NativeTlsSession<Memory>) {
        let (a, b) = duplex(chunk);
        let limits = SessionLimits {
            write_buffer_bytes: 128,
            ..SessionLimits::default()
        };
        (
            NativeTlsSession::client(
                a,
                &config(1),
                identity(1, 2),
                peer(2),
                SecureSessionGeneration::new(1).unwrap(),
                limits,
                MonoTime(0),
            )
            .unwrap(),
            NativeTlsSession::server(
                b,
                &config(2),
                identity(2, 7),
                peer(1),
                SecureSessionGeneration::new(9).unwrap(),
                limits,
                MonoTime(0),
            )
            .unwrap(),
        )
    }
    fn poll(s: &mut impl SecureSession) -> Result<SessionProgress, SessionError> {
        let budget = SessionPollBudget {
            io_calls: 4,
            read_bytes: 128,
            write_bytes: 128,
        };
        let p = s.poll(MonoTime(0), budget)?;
        assert!(
            p.io_calls <= budget.io_calls
                && p.read_bytes <= budget.read_bytes
                && p.written_bytes <= budget.write_bytes
        );
        Ok(p)
    }
    fn ready(a: &mut impl SecureSession, b: &mut impl SecureSession) {
        for _ in 0..10000 {
            poll(a).unwrap();
            poll(b).unwrap();
            if a.state() == SessionState::Ready && b.state() == SessionState::Ready {
                return;
            }
        }
        panic!("handshake made no bounded progress");
    }
    fn rejected(a: &mut impl SecureSession, b: &mut impl SecureSession) -> SessionError {
        for _ in 0..10000 {
            if let Err(e) = poll(a) {
                assert_ne!(a.state(), SessionState::Ready);
                return e;
            }
            if let Err(e) = poll(b) {
                assert_ne!(b.state(), SessionState::Ready);
                return e;
            }
        }
        panic!("invalid peers accepted or hung");
    }
    fn transfer(a: &mut impl SecureSession, b: &mut impl SecureSession, bytes: &[u8]) -> Vec<u8> {
        let mut sent = 0;
        let mut received = Vec::new();
        for _ in 0..100000 {
            if sent < bytes.len() {
                match a.write_plaintext(&bytes[sent..]) {
                    Ok(n) => sent += n,
                    Err(SessionError::WouldBlock) => (),
                    e => panic!("write: {e:?}"),
                }
            }
            poll(a).unwrap();
            poll(b).unwrap();
            let mut chunk = [0; 37];
            match b.read_plaintext(&mut chunk) {
                Ok(n) => received.extend_from_slice(&chunk[..n]),
                Err(SessionError::WouldBlock) => (),
                e => panic!("read: {e:?}"),
            }
            if received.len() == bytes.len() && a.is_flushed() {
                assert_eq!(sent, bytes.len());
                return received;
            }
        }
        panic!("plaintext transfer hung");
    }
    #[test]
    fn mutual_tls_short_io_binds_recovered_sessions_and_bounds_plaintext() {
        let (mut a, mut b) = sessions(7);
        assert_eq!(a.binding(), None);
        assert_eq!(a.write_plaintext(b"premature"), Err(SessionError::NotReady));
        assert_eq!(require_authenticated(&a), Err(SessionError::NotReady));
        ready(&mut a, &mut b);
        let binding = require_authenticated(&a).unwrap();
        assert_eq!(binding.local, identity(1, 2));
        assert_eq!(binding.peer, identity(2, 7));
        assert_eq!(binding.outgoing(), b.binding().unwrap().incoming());
        let bytes: Vec<u8> = (0..8192).map(|n| (n % 251) as u8).collect();
        let accepted = a.write_plaintext(&bytes).unwrap();
        assert!(accepted > 0 && accepted <= 128);
        assert!(!a.is_flushed());
        assert_eq!(a.write_plaintext(&bytes), Err(SessionError::WouldBlock));
        // Include the accepted prefix when checking the exact stream.
        let mut received = Vec::new();
        for _ in 0..1000 {
            poll(&mut a).unwrap();
            poll(&mut b).unwrap();
            let mut buf = [0; 128];
            if let Ok(n) = b.read_plaintext(&mut buf) {
                received.extend_from_slice(&buf[..n]);
            }
            if received.len() == accepted {
                break;
            }
        }
        assert_eq!(received, bytes[..accepted]);
        assert_eq!(
            transfer(&mut a, &mut b, &bytes[accepted..]),
            bytes[accepted..]
        );
    }
    #[test]
    fn exact_certificate_pin_is_required_beyond_valid_ca() {
        let (a, b) = duplex(31);
        let mut wrong = peer(2);
        wrong.certificate = cert(3).to_vec();
        let mut a = NativeTlsSession::client(
            a,
            &config(1),
            identity(1, 2),
            wrong,
            SecureSessionGeneration::new(1).unwrap(),
            SessionLimits::default(),
            MonoTime(0),
        )
        .unwrap();
        let mut b = NativeTlsSession::server(
            b,
            &config(2),
            identity(2, 7),
            peer(1),
            SecureSessionGeneration::new(2).unwrap(),
            SessionLimits::default(),
            MonoTime(0),
        )
        .unwrap();
        assert_eq!(rejected(&mut a, &mut b), SessionError::WrongPeer);
        assert_eq!(a.binding(), None);
    }
    #[test]
    fn tls_rejects_untrusted_root_and_wrong_dns_name() {
        for wrong_root in [false, true] {
            let (a, b) = duplex(31);
            let mut creds = credentials(1);
            if wrong_root {
                creds.roots = vec![include_bytes!("fixtures/tls/other-ca.der").to_vec()];
            }
            let mut expected = peer(2);
            if !wrong_root {
                expected.server_name = "wrong.voteboat.test".into();
            }
            let mut a = NativeTlsSession::client(
                a,
                &NativeTlsConfig::new(creds).unwrap(),
                identity(1, 2),
                expected,
                SecureSessionGeneration::new(1).unwrap(),
                SessionLimits::default(),
                MonoTime(0),
            )
            .unwrap();
            let mut b = NativeTlsSession::server(
                b,
                &config(2),
                identity(2, 7),
                peer(1),
                SecureSessionGeneration::new(2).unwrap(),
                SessionLimits::default(),
                MonoTime(0),
            )
            .unwrap();
            assert_eq!(rejected(&mut a, &mut b), SessionError::Authentication);
        }
    }
    #[test]
    fn authenticated_hello_cannot_choose_a_different_node_or_store() {
        for wrong_node in [false, true] {
            let (a, b) = duplex(31);
            let mut expected = peer(2);
            if wrong_node {
                expected.identity.node = NodeId::new(3).unwrap();
            } else {
                expected.identity.store.incarnation = StoreIncarnation::new(2).unwrap();
            }
            let mut a = NativeTlsSession::client(
                a,
                &config(1),
                identity(1, 2),
                expected,
                SecureSessionGeneration::new(1).unwrap(),
                SessionLimits::default(),
                MonoTime(0),
            )
            .unwrap();
            let mut b = NativeTlsSession::server(
                b,
                &config(2),
                identity(2, 7),
                peer(1),
                SecureSessionGeneration::new(2).unwrap(),
                SessionLimits::default(),
                MonoTime(0),
            )
            .unwrap();
            assert_eq!(rejected(&mut a, &mut b), SessionError::WrongPeer);
            assert_eq!(a.read_plaintext(&mut [0; 8]), Err(SessionError::WrongPeer));
        }
    }
    #[test]
    fn deadline_backward_time_zero_budget_and_revocation_latch() {
        let (mut a, _) = sessions(31);
        assert_eq!(
            a.poll(
                MonoTime(1),
                SessionPollBudget {
                    io_calls: 0,
                    read_bytes: 0,
                    write_bytes: 0
                }
            )
            .unwrap(),
            SessionProgress::default()
        );
        assert_eq!(
            a.poll(MonoTime(0), SessionPollBudget::default()),
            Err(SessionError::TimeWentBack)
        );
        assert_eq!(a.write_plaintext(b"x"), Err(SessionError::TimeWentBack));
        let (mut a, _) = sessions(31);
        assert_eq!(
            a.poll(MonoTime(10000), SessionPollBudget::default()),
            Err(SessionError::Timeout)
        );
        let (mut a, mut b) = sessions(31);
        ready(&mut a, &mut b);
        a.revoke();
        assert_eq!(a.state(), SessionState::Failed);
        assert_eq!(a.read_plaintext(&mut [0; 8]), Err(SessionError::Revoked));
        assert_eq!(a.write_plaintext(b"x"), Err(SessionError::Revoked));
        assert_eq!(poll(&mut a), Err(SessionError::Revoked));
        drop(a.take_io().unwrap());
        assert!(matches!(a.take_io(), Err(SessionError::Closed)));
    }
    #[test]
    fn clean_closure_drains_data_and_unclean_eof_fails() {
        let (mut a, mut b) = sessions(31);
        ready(&mut a, &mut b);
        a.write_plaintext(b"last data").unwrap();
        a.close();
        assert_eq!(a.write_plaintext(b"late"), Err(SessionError::Closed));
        let mut received = Vec::new();
        let mut eof = false;
        for _ in 0..1000 {
            poll(&mut a).unwrap();
            poll(&mut b).unwrap();
            let mut buf = [0; 17];
            match b.read_plaintext(&mut buf) {
                Ok(0) => eof = true,
                Ok(n) => received.extend_from_slice(&buf[..n]),
                Err(SessionError::WouldBlock) => (),
                e => panic!("close read: {e:?}"),
            }
            if eof && b.state() == SessionState::Closed {
                break;
            }
        }
        assert!(eof);
        assert_eq!(received, b"last data");
        assert_eq!(b.state(), SessionState::Closed);
        assert_eq!(require_authenticated(&b), Err(SessionError::NotReady));
        let (mut a, mut b) = sessions(31);
        ready(&mut a, &mut b);
        drop(a);
        assert_eq!(poll(&mut b), Err(SessionError::Truncated));
    }
    #[test]
    fn invalid_credentials_and_limits_fail_before_session_admission() {
        let mut creds = credentials(1);
        creds.private_key = vec![0; 8];
        assert!(matches!(
            NativeTlsConfig::new(creds),
            Err(SessionError::InvalidCredentials)
        ));
        let mut creds = credentials(1);
        creds.roots.clear();
        assert!(matches!(
            NativeTlsConfig::new(creds),
            Err(SessionError::InvalidCredentials)
        ));
        let (a, _) = duplex(31);
        let closed = a.outgoing.clone();
        assert!(matches!(
            NativeTlsSession::client(
                a,
                &config(1),
                identity(1, 1),
                peer(2),
                SecureSessionGeneration::new(1).unwrap(),
                SessionLimits {
                    write_buffer_bytes: 1,
                    ..SessionLimits::default()
                },
                MonoTime(0)
            ),
            Err(SessionError::InvalidLimits)
        ));
        assert!(closed.lock().unwrap().closed);
    }
    #[test]
    fn oversized_fragmented_handshake_stops_at_exact_ciphertext_ceiling() {
        let (mut raw, io) = duplex(4096);
        let observed = io.incoming.clone();
        let limit = 1024;
        let mut server = NativeTlsSession::server(
            io,
            &config(2),
            identity(2, 7),
            peer(1),
            SecureSessionGeneration::new(1).unwrap(),
            SessionLimits {
                handshake_bytes: limit,
                ..SessionLimits::default()
            },
            MonoTime(0),
        )
        .unwrap();
        // A ClientHello claiming 65535 bytes, fragmented across legal TLS record
        // sizes. It exceeds the channel budget before a complete hello exists.
        let mut payload = vec![0; 65539];
        payload[..4].copy_from_slice(&[1, 0, 255, 255]);
        let mut records = Vec::new();
        for part in payload.chunks(4096) {
            records.extend_from_slice(&[22, 3, 3]);
            records.extend_from_slice(&(part.len() as u16).to_be_bytes());
            records.extend_from_slice(part);
        }
        let mut sent = 0;
        let mut failed = false;
        for _ in 0..1000 {
            if sent < records.len() {
                match raw.write(&records[sent..]) {
                    Ok(n) => sent += n,
                    Err(e) if e.kind() == io::ErrorKind::WouldBlock => (),
                    e => panic!("raw write: {e:?}"),
                }
            }
            match server.poll(MonoTime(0), SessionPollBudget::default()) {
                Ok(_) => (),
                Err(e) => {
                    assert_eq!(e, SessionError::HandshakeTooLarge);
                    failed = true;
                    break;
                }
            }
        }
        assert!(failed);
        assert_eq!(sent - observed.lock().unwrap().bytes.len(), limit);
        assert_eq!(server.binding(), None);
    }
    #[test]
    fn server_requires_a_client_certificate() {
        let (mut io, server_io) = duplex(31);
        let mut roots = rustls::RootCertStore::empty();
        roots
            .add(rustls::pki_types::CertificateDer::from(ROOT.to_vec()))
            .unwrap();
        let mut config = rustls::ClientConfig::builder_with_provider(Arc::new(
            rustls::crypto::ring::default_provider(),
        ))
        .with_protocol_versions(&[&rustls::version::TLS13])
        .unwrap()
        .with_root_certificates(roots)
        .with_no_client_auth();
        config.alpn_protocols = vec![b"voteboat/1".to_vec()];
        let mut client = rustls::ClientConnection::new(
            Arc::new(config),
            rustls::pki_types::ServerName::try_from("node2.voteboat.test").unwrap(),
        )
        .unwrap();
        let mut server = NativeTlsSession::server(
            server_io,
            &self::config(2),
            identity(2, 7),
            peer(1),
            SecureSessionGeneration::new(1).unwrap(),
            SessionLimits::default(),
            MonoTime(0),
        )
        .unwrap();
        let mut rejected = false;
        for _ in 0..10000 {
            if client.wants_write() {
                let _ = client.write_tls(&mut io);
            }
            if client.wants_read() && matches!(client.read_tls(&mut io), Ok(n) if n > 0) {
                let _ = client.process_new_packets();
            }
            if let Err(e) = poll(&mut server) {
                assert_eq!(e, SessionError::Authentication);
                rejected = true;
                break;
            }
        }
        assert!(rejected);
        assert_eq!(server.binding(), None);
    }
    fn sockets(
        listener: &TcpListener,
        c1: &NativeTlsConfig,
        c2: &NativeTlsConfig,
        generation: u64,
        peer_session: u64,
    ) -> (NativeTlsSession<TcpStream>, NativeTlsSession<TcpStream>) {
        let a = TcpStream::connect(listener.local_addr().unwrap()).unwrap();
        let (b, _) = listener.accept().unwrap();
        a.set_nodelay(true).unwrap();
        b.set_nodelay(true).unwrap();
        let generation = SecureSessionGeneration::new(generation).unwrap();
        (
            NativeTlsSession::client_tcp(
                a,
                c1,
                identity(1, 2),
                peer(2),
                generation,
                SessionLimits::default(),
                MonoTime(0),
            )
            .unwrap(),
            NativeTlsSession::server_tcp(
                b,
                c2,
                identity(2, peer_session),
                peer(1),
                generation,
                SessionLimits::default(),
                MonoTime(0),
            )
            .unwrap(),
        )
    }
    #[test]
    fn real_tcp_tls_transfers_wire_frames_and_connection_rotation_is_isolated() {
        let listener = TcpListener::bind("127.0.0.1:0").unwrap();
        let c1 = config(1);
        let c2 = config(2);
        let (mut shared_a, mut shared_b) = sockets(&listener, &c1, &c2, 77, 7);
        ready(&mut shared_a, &mut shared_b);
        for (generation, peer_session) in [(1, 7), (2, 8)] {
            let (mut a, mut b) = sockets(&listener, &c1, &c2, generation, peer_session);
            let deadline = Instant::now() + Duration::from_secs(5);
            while a.state() != SessionState::Ready || b.state() != SessionState::Ready {
                poll(&mut a).unwrap();
                poll(&mut b).unwrap();
                assert!(Instant::now() < deadline);
                std::thread::sleep(Duration::from_millis(1));
            }
            let binding = require_authenticated(&a).unwrap();
            assert_eq!(binding.generation.get(), generation);
            assert_eq!(binding.peer.store.session.get(), peer_session);
            let messages: Vec<_> = (1..=20)
                .map(|id| Message {
                    group: GroupIdentity {
                        id: GroupId::new(id).unwrap(),
                        incarnation: GroupIncarnation::new(1).unwrap(),
                    },
                    configuration: ConfigurationId::new(1).unwrap(),
                    from: binding.local.node,
                    sender: binding.local.store,
                    to: binding.peer.node,
                    term: 3,
                    context: RequestContext {
                        origin: binding.local.store,
                        sequence: id as u64,
                    },
                    rpc: Rpc::ReadProbe,
                })
                .collect();
            let codec = NativeWireCodec::new(WireLimits::default()).unwrap();
            let encoded = codec.encode_batch(binding.outgoing(), &messages).unwrap();
            let received = transfer(&mut a, &mut b, &encoded);
            assert_eq!(
                codec
                    .decode_batch(b.binding().unwrap().incoming(), &received)
                    .unwrap(),
                messages
            );
            a.close();
            while a.state() != SessionState::Closed {
                poll(&mut a).unwrap();
                poll(&mut b).unwrap();
                assert!(Instant::now() < deadline);
            }
            drop(a.take_io().unwrap());
            assert_eq!(
                transfer(&mut shared_a, &mut shared_b, b"still live"),
                b"still live"
            );
            // Next iteration reuses the same listener/configs with a fresh generation and store session.
        }
    }
}
