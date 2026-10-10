// SPDX-License-Identifier: RPL-1.5
// Copyright (c) 2026 Karl Livesey
use std::{
    fs,
    io::Write,
    net::TcpStream,
    path::Path,
    time::{Duration, Instant},
};
use voteboat::{identity::*, native::tls::*, runtime::MonoTime, secure::*};

fn identity(number: u64, client: bool) -> PeerIdentity {
    let namespace = if client { 1u64 << 63 } else { 1u64 << 62 };
    PeerIdentity {
        node: NodeId::new(namespace | number).unwrap(),
        store: StoreIdentity {
            id: StoreId::new((1u128 << 127) | u128::from(namespace | number)).unwrap(),
            incarnation: StoreIncarnation::new(1).unwrap(),
        },
    }
}
/// Flush an authenticated request while deliberately never reading its reply.
pub fn send(base: u16, node: u64, tls: &Path, command: &str) -> NativeTlsSession<TcpStream> {
    let config = NativeTlsConfig::new(TlsCredentials {
        roots: vec![fs::read(tls.join("ca.der")).unwrap()],
        certificate_chain: vec![fs::read(tls.join("node3.der")).unwrap()],
        private_key: fs::read(tls.join("node3-key.der")).unwrap(),
    })
    .unwrap();
    let mut stream = TcpStream::connect(("127.0.0.1", base + 100 + node as u16)).unwrap();
    stream.write_all(b"3\n").unwrap();
    stream.set_nonblocking(true).unwrap();
    let local = identity(3, true);
    let mut session = NativeTlsSession::client(
        stream,
        &config,
        LocalIdentity {
            node: local.node,
            store: StoreBinding {
                identity: local.store,
                session: StoreSession::new(1).unwrap(),
            },
        },
        TlsPeer {
            identity: identity(node, false),
            certificate: fs::read(tls.join(format!("node{node}.der"))).unwrap(),
            server_name: format!("node{node}.voteboat.test"),
        },
        SecureSessionGeneration::new(1).unwrap(),
        SessionLimits::default(),
        MonoTime(0),
    )
    .unwrap();
    let bytes = format!("{command}\n").into_bytes();
    assert!(bytes.len() <= 256);
    let start = Instant::now();
    let mut sent = 0;
    loop {
        assert!(
            start.elapsed() < Duration::from_secs(5),
            "request flush timeout"
        );
        session
            .poll(
                MonoTime(start.elapsed().as_millis() as u64),
                SessionPollBudget::default(),
            )
            .unwrap();
        if session.state() == SessionState::Ready && sent < bytes.len() {
            match session.write_plaintext(&bytes[sent..]) {
                Ok(n) => sent += n,
                Err(SessionError::WouldBlock) => (),
                other => panic!("command send failed: {other:?}"),
            }
        }
        if sent == bytes.len() && session.is_flushed() {
            return session;
        }
        std::thread::park_timeout(Duration::from_millis(1));
    }
}
pub fn record(path: &Path) {
    let end = Instant::now() + Duration::from_secs(5);
    while !path.exists() {
        assert!(Instant::now() < end, "missing durable record: {path:?}");
        std::thread::park_timeout(Duration::from_millis(1));
    }
}
