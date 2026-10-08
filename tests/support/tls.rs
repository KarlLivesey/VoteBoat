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
//! Real TLS fixtures over caller-owned loopback TCP, with no background runtime.
use std::{
    net::{TcpListener, TcpStream},
    thread,
    time::{Duration, Instant},
};
use voteboat::{identity::SecureSessionGeneration, native::tls::*, runtime::MonoTime, secure::*};
fn certificate(n: u64) -> &'static [u8] {
    match n {
        1 => include_bytes!("../fixtures/tls/node1.der"),
        2 => include_bytes!("../fixtures/tls/node2.der"),
        3 => include_bytes!("../fixtures/tls/node3.der"),
        _ => panic!("unknown test node"),
    }
}
fn configuration(n: u64) -> NativeTlsConfig {
    let key: &[u8] = match n {
        1 => include_bytes!("../fixtures/tls/node1-key.der"),
        2 => include_bytes!("../fixtures/tls/node2-key.der"),
        3 => include_bytes!("../fixtures/tls/node3-key.der"),
        _ => panic!("unknown test node"),
    };
    NativeTlsConfig::new(TlsCredentials {
        roots: vec![include_bytes!("../fixtures/tls/ca.der").to_vec()],
        certificate_chain: vec![certificate(n).to_vec()],
        private_key: key.to_vec(),
    })
    .unwrap()
}
fn peer(identity: LocalIdentity) -> TlsPeer {
    TlsPeer {
        identity: PeerIdentity {
            node: identity.node,
            store: identity.store.identity,
        },
        certificate: certificate(identity.node.get()).to_vec(),
        server_name: format!("node{}.voteboat.test", identity.node.get()),
    }
}
pub fn pair(
    a: LocalIdentity,
    b: LocalIdentity,
    generation: u64,
) -> (NativeTlsSession<TcpStream>, NativeTlsSession<TcpStream>) {
    pair_generations(a, b, generation, generation)
}
pub fn pair_generations(
    a: LocalIdentity,
    b: LocalIdentity,
    a_generation: u64,
    b_generation: u64,
) -> (NativeTlsSession<TcpStream>, NativeTlsSession<TcpStream>) {
    let listener = TcpListener::bind("127.0.0.1:0").unwrap();
    let stream_a = TcpStream::connect(listener.local_addr().unwrap()).unwrap();
    let (stream_b, _) = listener.accept().unwrap();
    stream_a.set_nodelay(true).unwrap();
    stream_b.set_nodelay(true).unwrap();
    let limits = SessionLimits {
        write_buffer_bytes: 256,
        ..SessionLimits::default()
    };
    let mut a_session = NativeTlsSession::client_tcp(
        stream_a,
        &configuration(a.node.get()),
        a,
        peer(b),
        SecureSessionGeneration::new(a_generation).unwrap(),
        limits,
        MonoTime(0),
    )
    .unwrap();
    let mut b_session = NativeTlsSession::server_tcp(
        stream_b,
        &configuration(b.node.get()),
        b,
        peer(a),
        SecureSessionGeneration::new(b_generation).unwrap(),
        limits,
        MonoTime(0),
    )
    .unwrap();
    let deadline = Instant::now() + Duration::from_secs(5);
    while a_session.state() != SessionState::Ready || b_session.state() != SessionState::Ready {
        a_session
            .poll(MonoTime(0), SessionPollBudget::default())
            .unwrap();
        b_session
            .poll(MonoTime(0), SessionPollBudget::default())
            .unwrap();
        assert!(Instant::now() < deadline, "TLS handshake timed out");
        thread::sleep(Duration::from_millis(1));
    }
    (a_session, b_session)
}
