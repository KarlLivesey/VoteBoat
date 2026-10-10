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
use std::net::SocketAddr;
use voteboat::{connect::*, native::discovery::NativePeerDiscovery, transport::ConnectTicket};

pub(super) fn exercise<C: PeerConnector<Endpoint = SocketAddr>>(
    a: C::Session,
    b: C::Session,
    connector: C,
    mut acceptor: C,
    endpoint: SocketAddr,
    start: MonoTime,
) -> (C, C) {
    let config = RemoteDiscoveryConfig {
        retry_ms: 10,
        ..Default::default()
    };
    let mut hints = NativePeerDiscovery::new(3, start).unwrap();
    hints.publish(hint(3, 2, start.0 + 60_000), start).unwrap();
    hints.publish(hint(4, 1, start.0 + 60_000), start).unwrap();
    let mut source = NativeDiscoveryResponder::new(b, peer(1), hints, config, start)
        .ok()
        .unwrap();
    let mut client = NativeRemotePeerDiscovery::new(a, peer(2), config, start)
        .ok()
        .unwrap();
    client.resolve(peer(3), start).unwrap_err();
    let (done, now) = scenario::drive(&mut client, &mut source, start);
    let old = done.result.unwrap();
    client.resolve(peer(4), now).unwrap_err();
    let (done, now) = scenario::drive(&mut client, &mut source, now);
    let unrelated = done.result.unwrap();
    assert!(client.invalidate(peer(3), old.generation));
    source.close();
    drop(source);
    let generation = voteboat::identity::SecureSessionGeneration::new(2).unwrap();
    let mut driver = ReconnectingPeerDiscovery::new(
        client,
        connector,
        SourceReconnectConfig {
            endpoint,
            first_generation: generation,
            last_generation: generation,
            retry_ms: 10,
        },
        now,
    )
    .ok()
    .unwrap();
    driver.resolve(peer(5), now).unwrap_err();
    let failed_at = driver.source().next_deadline().unwrap();
    driver
        .poll_discovery(failed_at, SessionPollBudget::default())
        .unwrap();
    assert!(driver.source().source_failed());
    assert_eq!(driver.resolve(peer(4), failed_at), Ok(unrelated));
    let resumed_at = MonoTime(failed_at.0 + 10);
    acceptor
        .submit(
            ConnectRequest {
                ticket: ConnectTicket {
                    local: local(2),
                    peer: peer(1),
                    generation,
                },
                direction: ConnectDirection::Accept,
                deadline: MonoTime(resumed_at.0 + 10_000),
            },
            resumed_at,
        )
        .unwrap();
    let (mut source, now) = reconnect(&mut driver, &mut acceptor, config, resumed_at);
    assert_eq!(driver.resolve(peer(4), now), Ok(unrelated));
    // An authenticated replacement is still forbidden to lower an existing floor.
    driver.resolve(peer(3), now).unwrap_err();
    assert_eq!(
        driver.source().pending().unwrap().binding.generation,
        generation
    );
    let now = refresh(&mut driver, &mut source, now);
    assert_eq!(
        driver.resolve(peer(3), now),
        Err(DiscoveryError::StaleGeneration)
    );
    let later = MonoTime(now.0 + 10);
    source
        .source_mut()
        .publish(hint(3, 3, later.0 + 20_000), later)
        .unwrap();
    driver.resolve(peer(3), later).unwrap_err();
    let now = refresh(&mut driver, &mut source, later);
    let renewed = driver.resolve(peer(3), now).unwrap();
    assert_eq!(renewed.generation, HintGeneration::new(3).unwrap());
    assert_ne!(renewed.endpoint, old.endpoint);
    assert_eq!(driver.resolve(peer(4), now), Ok(unrelated));
    driver.resolve(peer(6), now).unwrap_err();
    driver.close();
    driver
        .poll_discovery(now, SessionPollBudget::default())
        .unwrap();
    assert!(driver.is_drained());
    let (remote, connector) = driver.into_parts().ok().unwrap();
    assert!(remote.into_session().is_ok());
    source.close();
    acceptor.close();
    for offset in 0..1000 {
        acceptor
            .poll(MonoTime(now.0 + offset), ConnectPollBudget::default())
            .unwrap();
        if acceptor.is_drained() {
            return (connector, acceptor);
        }
    }
    panic!("source acceptor did not drain");
}
fn reconnect<C: PeerConnector<Endpoint = SocketAddr>>(
    driver: &mut ReconnectingPeerDiscovery<C>,
    acceptor: &mut C,
    config: RemoteDiscoveryConfig,
    start: MonoTime,
) -> (
    NativeDiscoveryResponder<C::Session, NativePeerDiscovery>,
    MonoTime,
) {
    let mut source = None;
    for offset in 0..9000 {
        let now = MonoTime(start.0 + offset);
        for done in acceptor.poll(now, ConnectPollBudget::default()).unwrap() {
            assert_eq!(done.ticket.generation.get(), 2);
            let mut hints = NativePeerDiscovery::new(3, now).unwrap();
            hints.publish(hint(3, 1, now.0 + 20_000), now).unwrap();
            assert!(source.is_none());
            source = Some(
                NativeDiscoveryResponder::new(done.result.unwrap(), peer(1), hints, config, now)
                    .ok()
                    .unwrap(),
            );
        }
        if let Some(source) = &mut source {
            source.poll(now, SessionPollBudget::default()).unwrap();
        }
        driver
            .poll_discovery(now, SessionPollBudget::default())
            .unwrap();
        if driver.status() == SourceReconnectStatus::Ready {
            if let Some(source) = source.take() {
                return (source, now);
            }
        }
        std::thread::sleep(std::time::Duration::from_micros(100));
    }
    panic!(
        "source reconnect stalled: {:?}, {:?}",
        driver.status(),
        driver.last_connect_error()
    );
}
fn refresh<C: PeerConnector<Endpoint = SocketAddr>>(
    driver: &mut ReconnectingPeerDiscovery<C>,
    source: &mut NativeDiscoveryResponder<C::Session, NativePeerDiscovery>,
    start: MonoTime,
) -> MonoTime {
    for offset in 0..2000 {
        let now = MonoTime(start.0 + offset);
        source.poll(now, SessionPollBudget::default()).unwrap();
        driver
            .poll_discovery(now, SessionPollBudget::default())
            .unwrap();
        if driver.source().pending().is_none() {
            return now;
        }
    }
    panic!("refresh after source reconnect stalled");
}
