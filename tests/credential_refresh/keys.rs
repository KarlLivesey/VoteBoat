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
use super::*;
use std::{
    net::{TcpListener, TcpStream},
    time::{Duration, Instant},
};
use voteboat::{
    authorization::CredentialGeneration,
    native::{credentials::NativeCredentialSet, tls::*},
};
type Pair = (NativeTlsSession<TcpStream>, NativeTlsSession<TcpStream>);
fn establish(
    client: &NativeTlsConfig,
    server: &NativeTlsConfig,
    peer: TlsPeer,
    generation: u64,
) -> Result<Pair, SessionError> {
    let listener = TcpListener::bind("127.0.0.1:0").unwrap();
    let a = TcpStream::connect(listener.local_addr().unwrap()).unwrap();
    let (b, _) = listener.accept().unwrap();
    a.set_nodelay(true).unwrap();
    b.set_nodelay(true).unwrap();
    let generation = SecureSessionGeneration::new(generation).unwrap();
    let mut a = NativeTlsSession::client_tcp(
        a,
        client,
        local(1),
        peer,
        generation,
        SessionLimits::default(),
        MonoTime(0),
    )?;
    let mut b = NativeTlsSession::server_tcp(
        b,
        server,
        local(2),
        support::tls::peer(local(1)),
        generation,
        SessionLimits::default(),
        MonoTime(0),
    )?;
    let deadline = Instant::now() + Duration::from_secs(5);
    while a.state() != SessionState::Ready || b.state() != SessionState::Ready {
        a.poll(MonoTime(0), SessionPollBudget::default())?;
        b.poll(MonoTime(0), SessionPollBudget::default())?;
        if Instant::now() >= deadline {
            return Err(SessionError::Timeout);
        }
        std::thread::yield_now();
    }
    Ok((a, b))
}
#[test]
fn replacement_certificate_requires_new_pin_but_preserves_stable_peer_identity() {
    let client = support::tls::configuration(1);
    let old_server = support::tls::configuration(2);
    let old_pin = support::tls::peer(local(2));
    let mut bundle = NativeCredentialSet::new(
        CredentialGeneration::new(1).unwrap(),
        (client.clone(), old_pin.clone()),
    );
    let old_lease = bundle.lease().unwrap();
    let (a, _b) = establish(&client, &old_server, old_pin.clone(), 1).unwrap();
    let mut old = GuardedSession::new(a, old_lease).ok().unwrap();
    // The new certificate/key is independently authenticated, while the host's
    // explicit pin maps it to the same durable node/store identity.
    let new_server = support::tls::configuration(3);
    let new_pin = TlsPeer {
        identity: old_pin.identity,
        ..support::tls::peer(local(3))
    };
    bundle
        .replace(
            CredentialGeneration::new(2).unwrap(),
            (client.clone(), new_pin.clone()),
        )
        .ok()
        .unwrap();
    assert_eq!(
        old.poll(MonoTime(0), SessionPollBudget::default()),
        Err(SessionError::Revoked)
    );
    let stale = establish(&client, &new_server, old_pin, 2).err().unwrap();
    assert!(matches!(
        stale,
        SessionError::WrongPeer | SessionError::Authentication
    ));
    let retired = establish(&client, &old_server, new_pin.clone(), 3)
        .err()
        .unwrap();
    assert!(matches!(
        retired,
        SessionError::WrongPeer | SessionError::Authentication
    ));
    let fresh_lease = bundle.lease().unwrap();
    let (a, mut b) = establish(&client, &new_server, new_pin, 4).unwrap();
    let mut a = GuardedSession::new(a, fresh_lease).ok().unwrap();
    assert_eq!(require_authenticated(&a).unwrap().peer, local(2));
    a.write_plaintext(b"rotated").unwrap();
    for _ in 0..1000 {
        a.poll(MonoTime(0), SessionPollBudget::default()).unwrap();
        b.poll(MonoTime(0), SessionPollBudget::default()).unwrap();
        let mut bytes = [0; 7];
        match b.read_plaintext(&mut bytes) {
            Ok(n) => {
                assert_eq!(n, 7);
                assert_eq!(&bytes, b"rotated");
                return;
            }
            Err(SessionError::WouldBlock) => (),
            other => panic!("rotated transfer: {other:?}"),
        }
    }
    panic!("rotated channel stalled");
}
