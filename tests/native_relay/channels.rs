// SPDX-License-Identifier: RPL-1.5
// Copyright (c) 2026 Karl Livesey
//! Actual authenticated native sessions; helper9 has an explicit certificate pin.
use std::{
    cell::Cell,
    net::{TcpListener, TcpStream},
    rc::Rc,
    time::{Duration, Instant},
};
use voteboat::{
    identity::SecureSessionGeneration,
    native::{connect::NativePeerProtocol, tls::*},
    runtime::MonoTime,
    secure::*,
};
#[derive(Clone, Default)]
pub struct Gate {
    pub reads: Rc<Cell<bool>>,
    pub writes: Rc<Cell<bool>>,
}
pub struct Gated {
    pub actual: Box<dyn SecureSession>,
    pub gate: Gate,
}
impl SecureSession for Gated {
    fn security(&self) -> SessionSecurity {
        self.actual.security()
    }
    fn state(&self) -> SessionState {
        self.actual.state()
    }
    fn binding(&self) -> Option<SessionBinding> {
        self.actual.binding()
    }
    fn limits(&self) -> SessionLimits {
        self.actual.limits()
    }
    fn poll(&mut self, n: MonoTime, b: SessionPollBudget) -> Result<SessionProgress, SessionError> {
        self.actual.poll(n, b)
    }
    fn read_plaintext(&mut self, b: &mut [u8]) -> Result<usize, SessionError> {
        if self.gate.reads.get() {
            Err(SessionError::WouldBlock)
        } else {
            self.actual.read_plaintext(b)
        }
    }
    fn write_plaintext(&mut self, b: &[u8]) -> Result<usize, SessionError> {
        if self.gate.writes.get() {
            Err(SessionError::WouldBlock)
        } else {
            self.actual.write_plaintext(b)
        }
    }
    fn is_flushed(&self) -> bool {
        self.actual.is_flushed()
    }
    fn drain_status(&mut self) -> SessionDrainStatus {
        self.actual.drain_status()
    }
    fn close(&mut self) {
        self.actual.close();
    }
    fn revoke(&mut self) {
        self.actual.revoke();
    }
}
fn credential_slot(node: u64) -> usize {
    if node >= 9 {
        3
    } else {
        node as usize - 1
    }
}
pub fn certificate(node: u64) -> &'static [u8] {
    [
        include_bytes!("../fixtures/four-node-tls/node1.der").as_slice(),
        include_bytes!("../fixtures/four-node-tls/node2.der"),
        include_bytes!("../fixtures/four-node-tls/node3.der"),
        include_bytes!("../fixtures/four-node-tls/node4.der"),
    ][credential_slot(node)]
}
pub fn config(node: u64) -> NativeTlsConfig {
    let key: [&[u8]; 4] = [
        include_bytes!("../fixtures/four-node-tls/node1-key.der"),
        include_bytes!("../fixtures/four-node-tls/node2-key.der"),
        include_bytes!("../fixtures/four-node-tls/node3-key.der"),
        include_bytes!("../fixtures/four-node-tls/node4-key.der"),
    ];
    NativeTlsConfig::new(TlsCredentials {
        roots: vec![include_bytes!("../fixtures/four-node-tls/ca.der").to_vec()],
        certificate_chain: vec![certificate(node).to_vec()],
        private_key: key[credential_slot(node)].to_vec(),
    })
    .unwrap()
}
fn peer(local: LocalIdentity) -> TlsPeer {
    TlsPeer {
        identity: PeerIdentity {
            node: local.node,
            store: local.store.identity,
        },
        certificate: certificate(local.node.get()).to_vec(),
        server_name: format!(
            "node{}.voteboat.test",
            credential_slot(local.node.get()) + 1
        ),
    }
}
pub fn pair(
    protocol: NativePeerProtocol,
    leaf: LocalIdentity,
    helper: LocalIdentity,
    generation: u64,
    clock: Instant,
) -> (Box<dyn SecureSession>, Gated, Gate) {
    let limits = SessionLimits {
        write_buffer_bytes: 256,
        ..Default::default()
    };
    let now = MonoTime(clock.elapsed().as_millis() as u64);
    let generation = SecureSessionGeneration::new(generation).unwrap();
    let (mut a, mut b): (Box<dyn SecureSession>, Box<dyn SecureSession>) = match protocol {
        NativePeerProtocol::TcpTls => {
            let listener = TcpListener::bind("127.0.0.1:0").unwrap();
            let a =
                TcpStream::connect_timeout(&listener.local_addr().unwrap(), Duration::from_secs(1))
                    .unwrap();
            let (b, _) = listener.accept().unwrap();
            a.set_nodelay(true).unwrap();
            b.set_nodelay(true).unwrap();
            (
                Box::new(
                    NativeTlsSession::client_tcp(
                        a,
                        &config(leaf.node.get()).with_relay_profile(),
                        leaf,
                        peer(helper),
                        generation,
                        limits,
                        now,
                    )
                    .unwrap(),
                ),
                Box::new(
                    NativeTlsSession::server_tcp(
                        b,
                        &config(9).with_relay_profile(),
                        helper,
                        peer(leaf),
                        generation,
                        limits,
                        now,
                    )
                    .unwrap(),
                ),
            )
        }
        #[cfg(feature = "quic")]
        NativePeerProtocol::Quic => {
            use voteboat::native::quic::*;
            let a = std::net::UdpSocket::bind("127.0.0.1:0").unwrap();
            let b = std::net::UdpSocket::bind("127.0.0.1:0").unwrap();
            let aa = a.local_addr().unwrap();
            let ba = b.local_addr().unwrap();
            let options = |local, remote_peer, remote| QuicSessionOptions {
                local,
                peer: peer(remote_peer),
                remote,
                generation,
                limits,
            };
            (
                Box::new(
                    NativeQuicSession::client(
                        a,
                        &config(leaf.node.get()).with_relay_profile(),
                        options(leaf, helper, ba),
                        now,
                    )
                    .unwrap(),
                ),
                Box::new(
                    NativeQuicSession::server(
                        b,
                        &config(9).with_relay_profile(),
                        options(helper, leaf, aa),
                        now,
                    )
                    .unwrap(),
                ),
            )
        }
    };
    handshake(a.as_mut(), b.as_mut(), clock);
    assert_eq!(require_authenticated(a.as_ref()).unwrap().peer, helper);
    assert_eq!(require_authenticated(b.as_ref()).unwrap().peer, leaf);
    let gate = Gate::default();
    (
        a,
        Gated {
            actual: b,
            gate: gate.clone(),
        },
        gate,
    )
}

fn handshake(a: &mut dyn SecureSession, b: &mut dyn SecureSession, clock: Instant) {
    let deadline = Instant::now() + Duration::from_secs(5);
    while a.state() != SessionState::Ready || b.state() != SessionState::Ready {
        let now = MonoTime(clock.elapsed().as_millis() as u64);
        a.poll(now, SessionPollBudget::default()).unwrap();
        b.poll(now, SessionPollBudget::default()).unwrap();
        assert!(
            Instant::now() < deadline,
            "actual relay handshake {:?}/{:?}",
            a.state(),
            b.state()
        );
        std::thread::park_timeout(Duration::from_millis(1));
    }
}
