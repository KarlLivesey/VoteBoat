// SPDX-License-Identifier: RPL-1.5
// Copyright (c) 2026 Karl Livesey
#![cfg(feature = "tls")]
#[path = "peer_rotation/host.rs"]
mod host;
mod support;
use std::{
    collections::BTreeMap,
    net::{SocketAddr, TcpListener},
    sync::Arc,
    time::{Duration, Instant},
};
use voteboat::{
    authorization::CredentialGeneration,
    connect::*,
    dial::*,
    identity::*,
    native::{connect::*, dial::NativeTcpDialer, peer_credentials::*, worker::ThreadWake},
    runtime::MonoTime,
    secure::*,
    transport::ConnectTicket,
};
type Connector = RotatingPeerConnector<NativeServiceConnector>;
type Session = <Connector as PeerConnector>::Session;
fn generation(n: u64) -> CredentialGeneration {
    CredentialGeneration::new(n).unwrap()
}
fn local(n: u64) -> LocalIdentity {
    LocalIdentity {
        node: support::node(n),
        store: StoreBinding {
            identity: support::identity(n.into()),
            session: StoreSession::new(n + 10).unwrap(),
        },
    }
}
fn material(node: u64, key: u64) -> NativePeerMaterial {
    let other = 3 - node;
    let mut peer = support::tls::peer(local(other));
    if other == 2 {
        peer.certificate = support::tls::peer(local(key)).certificate;
        peer.server_name = format!("node{key}.voteboat.test");
    }
    NativePeerMaterial {
        tls: support::tls::configuration(if node == 2 { key } else { 1 }),
        peers: [(support::node(other), peer)].into(),
    }
}
fn config(n: u64) -> NativeConnectConfig {
    NativeConnectConfig {
        local: local(n),
        limits: ConnectLimits {
            requests: 1,
            anonymous: 1,
            timeout_ms: 10000,
        },
        session: SessionLimits::default(),
    }
}
struct Rig {
    nodes: Vec<Connector>,
    addresses: Vec<SocketAddr>,
    now: u64,
}
impl Rig {
    fn new(quic: bool) -> Self {
        let (inner, addresses) = if quic { quic_nodes() } else { tcp_nodes() };
        Self {
            nodes: inner
                .into_iter()
                .map(|c| RotatingPeerConnector::new(c, generation(1)).ok().unwrap())
                .collect(),
            addresses,
            now: 0,
        }
    }
    fn submit(&mut self, sequence: u64) {
        for (index, node) in self.nodes.iter_mut().enumerate() {
            let from = index as u64 + 1;
            let to = 3 - from;
            node.submit(
                ConnectRequest {
                    ticket: ConnectTicket {
                        local: local(from),
                        peer: support::tls::peer(local(to)).identity,
                        generation: SecureSessionGeneration::new(sequence).unwrap(),
                    },
                    direction: if from == 1 {
                        ConnectDirection::Dial(self.addresses[1])
                    } else {
                        ConnectDirection::Accept
                    },
                    deadline: MonoTime(self.now + 10000),
                },
                MonoTime(self.now),
            )
            .unwrap();
        }
    }
    fn outcomes(&mut self) -> Vec<Result<Session, ConnectError>> {
        let end = Instant::now() + Duration::from_secs(15);
        let mut results: BTreeMap<usize, Result<Session, ConnectError>> = BTreeMap::new();
        while results.len() != 2 {
            self.now += 1;
            for (index, node) in self.nodes.iter_mut().enumerate() {
                for outcome in node
                    .poll(MonoTime(self.now), ConnectPollBudget::default())
                    .unwrap()
                {
                    assert_eq!(outcome.ticket.local, local(index as u64 + 1));
                    assert!(results.insert(index, outcome.result).is_none());
                }
            }
            for session in results.values_mut().filter_map(|r| r.as_mut().ok()) {
                let _ = session.poll(MonoTime(self.now), SessionPollBudget::default());
            }
            assert!(Instant::now() < end, "connection results stalled");
            std::thread::park_timeout(Duration::from_millis(1));
        }
        results.into_values().collect()
    }
    fn establish(&mut self, sequence: u64) -> Vec<Session> {
        self.submit(sequence);
        self.outcomes().into_iter().map(Result::unwrap).collect()
    }
    fn rotate(&mut self, expected: u64, next: u64, key: u64) {
        for (index, node) in self.nodes.iter_mut().enumerate() {
            node.replace_peer_credentials(
                generation(expected),
                generation(next),
                material(index as u64 + 1, key),
            )
            .ok()
            .unwrap();
        }
    }
}
impl Drop for Rig {
    fn drop(&mut self) {
        for node in &mut self.nodes {
            node.close();
        }
        let end = Instant::now() + Duration::from_secs(5);
        for node in self.nodes.drain(..) {
            let mut node = node;
            while !node.is_drained() {
                let _ = node.poll(MonoTime(self.now), ConnectPollBudget::default());
                assert!(Instant::now() < end, "connector did not drain");
                std::thread::park_timeout(Duration::from_millis(1));
            }
            let inner = node.into_inner().ok().unwrap();
            if let Some(mut dialer) = inner.into_dialer().ok().unwrap() {
                while !dialer.try_finish().unwrap() {
                    assert!(Instant::now() < end);
                    std::thread::yield_now();
                }
            }
        }
    }
}
fn tcp_nodes() -> (Vec<NativeServiceConnector>, Vec<SocketAddr>) {
    let listeners = (0..2)
        .map(|_| TcpListener::bind("127.0.0.1:0").unwrap())
        .collect::<Vec<_>>();
    let addresses = listeners.iter().map(|l| l.local_addr().unwrap()).collect();
    let nodes = listeners
        .into_iter()
        .enumerate()
        .map(|(i, listener)| {
            let n = i as u64 + 1;
            let m = material(n, 2);
            let dialer = NativeTcpDialer::spawn(
                local(n),
                [(support::node(3 - n), local(3 - n).store.identity)].into(),
                DialLimits::default(),
                Arc::new(ThreadWake::current()),
            )
            .unwrap();
            NativeServiceConnector::Tcp(Box::new(
                NativePeerConnector::new(
                    config(n),
                    m.tls,
                    m.peers,
                    dialer,
                    Some(listener),
                    MonoTime(0),
                )
                .unwrap(),
            ))
        })
        .collect();
    (nodes, addresses)
}
fn quic_nodes() -> (Vec<NativeServiceConnector>, Vec<SocketAddr>) {
    #[cfg(feature = "quic")]
    {
        use std::net::UdpSocket;
        use voteboat::native::quic_connect::NativeQuicConnector;
        let sockets = (0..2)
            .map(|_| UdpSocket::bind("127.0.0.1:0").unwrap())
            .collect::<Vec<_>>();
        let addresses = sockets
            .iter()
            .map(|s| s.local_addr().unwrap())
            .collect::<Vec<_>>();
        let nodes = sockets
            .into_iter()
            .enumerate()
            .map(|(i, socket)| {
                let n = i as u64 + 1;
                let m = material(n, 2);
                let peers = m
                    .peers
                    .into_iter()
                    .map(|(id, pin)| (id, (addresses[(3 - n) as usize - 1], pin)))
                    .collect();
                NativeServiceConnector::Quic(Box::new(
                    NativeQuicConnector::new(config(n), m.tls, peers, socket, MonoTime(0)).unwrap(),
                ))
            })
            .collect();
        (nodes, addresses)
    }
    #[cfg(not(feature = "quic"))]
    panic!("QUIC feature required");
}
fn exchange(sessions: &mut [Session], now: u64) {
    assert_eq!(sessions[0].write_plaintext(b"new keys").unwrap(), 8);
    let end = Instant::now() + Duration::from_secs(5);
    let mut bytes = [0; 8];
    let mut received = 0;
    while received < bytes.len() {
        for s in sessions.iter_mut() {
            s.poll(MonoTime(now), SessionPollBudget::default()).unwrap();
        }
        match sessions[1].read_plaintext(&mut bytes[received..]) {
            Ok(n) => received += n,
            Err(SessionError::WouldBlock) => (),
            other => panic!("{other:?}"),
        }
        assert!(Instant::now() < end);
        std::thread::park_timeout(Duration::from_millis(1));
    }
    assert_eq!(&bytes, b"new keys");
}
fn history(quic: bool) {
    let mut rig = Rig::new(quic);
    let mut original = rig.establish(1);
    exchange(&mut original, rig.now);
    invalid_material(&mut rig, &mut original[0]);
    for (expected, next) in [(2, 3), (1, 1)] {
        let m = material(1, 3);
        let pointer = m.peers[&support::node(2)].certificate.as_ptr();
        let (error, returned) = rig.nodes[0]
            .replace_peer_credentials(generation(expected), generation(next), m)
            .err()
            .unwrap();
        assert_eq!(
            error,
            ConnectError::Session(SessionError::InvalidCredentials)
        );
        assert_eq!(
            pointer,
            returned.peers[&support::node(2)].certificate.as_ptr()
        );
        assert_eq!(original[0].state(), SessionState::Ready);
    }
    rig.nodes[0]
        .replace_peer_credentials(generation(1), generation(2), material(1, 3))
        .ok()
        .unwrap();
    assert_eq!(original[0].state(), SessionState::Failed);
    assert_eq!(original[1].state(), SessionState::Ready);
    rig.nodes[1]
        .replace_peer_credentials(generation(1), generation(2), material(2, 3))
        .ok()
        .unwrap();
    for s in &mut original {
        assert_eq!(s.write_plaintext(b"old"), Err(SessionError::Revoked));
    }
    drop(original);
    let mut fresh = rig.establish(2);
    for (i, s) in fresh.iter().enumerate() {
        let b = s.binding().unwrap();
        assert_eq!(b.local, local(i as u64 + 1));
        assert_eq!(b.peer.node, local(2 - i as u64).node);
    }
    exchange(&mut fresh, rig.now);
    drop(fresh);
    rig.submit(3);
    rig.rotate(2, 3, 3);
    for result in rig.outcomes() {
        assert!(matches!(
            result,
            Err(ConnectError::Session(SessionError::Revoked))
        ));
    }
    for (index, node) in rig.nodes.iter_mut().enumerate() {
        let from = index as u64 + 1;
        let stale = ConnectRequest {
            ticket: ConnectTicket {
                local: local(from),
                peer: support::tls::peer(local(3 - from)).identity,
                generation: SecureSessionGeneration::new(3).unwrap(),
            },
            direction: if from == 1 {
                ConnectDirection::Dial(rig.addresses[1])
            } else {
                ConnectDirection::Accept
            },
            deadline: MonoTime(rig.now + 10000),
        };
        assert_eq!(
            node.submit(stale, MonoTime(rig.now)).unwrap_err().reason,
            ConnectError::StaleConnection
        );
    }
    let mut fresh = rig.establish(4);
    exchange(&mut fresh, rig.now);
}
fn invalid_material(rig: &mut Rig, session: &mut Session) {
    let mut absent = material(1, 3);
    absent.peers.clear();
    let mut identity = material(1, 3);
    identity
        .peers
        .get_mut(&support::node(2))
        .unwrap()
        .identity
        .store
        .incarnation = StoreIncarnation::new(99).unwrap();
    let mut wire = material(1, 3);
    wire.tls = wire.tls.with_wire_version(2).unwrap();
    let mut empty = material(1, 3);
    empty
        .peers
        .get_mut(&support::node(2))
        .unwrap()
        .certificate
        .clear();
    let mut name = material(1, 3);
    name.peers.get_mut(&support::node(2)).unwrap().server_name = "invalid name".into();
    for (material, expected) in [
        (absent, ConnectError::WrongBinding),
        (identity, ConnectError::WrongBinding),
        (wire, ConnectError::WrongBinding),
        (empty, ConnectError::InvalidRequest),
        (name, ConnectError::InvalidRequest),
    ] {
        let (error, _) = rig.nodes[0]
            .replace_peer_credentials(generation(1), generation(2), material)
            .err()
            .unwrap();
        assert_eq!(error, expected);
        assert_eq!(rig.nodes[0].credential_generation(), Some(generation(1)));
        session
            .poll(MonoTime(rig.now), SessionPollBudget::default())
            .unwrap();
    }
}

fn stale_pin(quic: bool) {
    let mut rig = Rig::new(quic);
    rig.nodes[0]
        .replace_peer_credentials(generation(1), generation(2), material(1, 2))
        .ok()
        .unwrap();
    rig.nodes[1]
        .replace_peer_credentials(generation(1), generation(2), material(2, 3))
        .ok()
        .unwrap();
    rig.submit(1);
    let results = rig.outcomes();
    assert!(results[0].is_err(), "old pin authenticated replacement key");
    for mut session in results.into_iter().flatten() {
        session.revoke();
    }
    rig.rotate(2, 3, 3);
    let mut sessions = rig.establish(2);
    exchange(&mut sessions, rig.now);
}
#[test]
fn tcp_stale_pin_refuses_changed_key_and_corrected_pin_reconnects() {
    stale_pin(false);
}
#[cfg(feature = "quic")]
#[test]
fn quic_stale_pin_refuses_changed_key_and_corrected_pin_reconnects() {
    stale_pin(true);
}
#[test]
fn tcp_peer_keys_rotate_and_old_attempts_and_sessions_are_revoked() {
    history(false);
}
#[cfg(feature = "quic")]
#[test]
fn quic_peer_keys_rotate_and_old_attempts_and_sessions_are_revoked() {
    history(true);
}
