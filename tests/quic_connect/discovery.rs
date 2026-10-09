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
use voteboat::{discovery::*, native::discovery::NativePeerDiscovery};
fn connector(
    id: u64,
    peer: u64,
    address: SocketAddr,
    socket: UdpSocket,
    discovered: bool,
) -> NativeQuicConnector {
    let peers = [(
        support::node(peer),
        (address, support::tls::peer(local(peer))),
    )]
    .into();
    if discovered {
        NativeQuicConnector::new_with_discovered_dials(
            config(id),
            support::tls::configuration(id),
            peers,
            socket,
            MonoTime(0),
        )
        .unwrap()
    } else {
        NativeQuicConnector::new(
            config(id),
            support::tls::configuration(id),
            peers,
            socket,
            MonoTime(0),
        )
        .unwrap()
    }
}
fn req(g: u64, address: SocketAddr, now: u64) -> ConnectRequest<SocketAddr> {
    ConnectRequest {
        ticket: ticket(1, 2, g),
        direction: ConnectDirection::Dial(address),
        deadline: MonoTime(now + 10000),
    }
}
fn pair<A: PeerConnector<Endpoint = SocketAddr, Session = NativeQuicSession>>(
    a: &mut A,
    b: &mut NativeQuicConnector,
    start: u64,
) -> (BTreeMap<(u64, u64), NativeQuicSession>, u64) {
    let mut sessions = BTreeMap::new();
    for now in start..start + 9000 {
        for (id, events) in [
            (
                1,
                a.poll(MonoTime(now), ConnectPollBudget::default()).unwrap(),
            ),
            (
                2,
                b.poll(MonoTime(now), ConnectPollBudget::default()).unwrap(),
            ),
        ] {
            for event in events {
                let session = event.result.unwrap();
                let binding = require_authenticated(&session).unwrap();
                assert_eq!(binding.local, event.ticket.local);
                assert_eq!(binding.peer.node, event.ticket.peer.node);
                assert_eq!(binding.peer.store.identity, event.ticket.peer.store);
                assert_eq!(binding.generation, event.ticket.generation);
                sessions.insert((id, event.ticket.peer.node.get()), session);
            }
        }
        for session in sessions.values_mut() {
            session
                .poll(MonoTime(now), SessionPollBudget::default())
                .unwrap();
        }
        if sessions.len() == 2 {
            return (sessions, now);
        }
    }
    panic!("discovered QUIC establishment stalled");
}
#[test]
fn refreshed_quic_hint_selects_new_address_and_retained_peer_lease_bounds_reconnect() {
    let sa = UdpSocket::bind("127.0.0.1:0").unwrap();
    let sb = UdpSocket::bind("127.0.0.1:0").unwrap();
    let stale = UdpSocket::bind("127.0.0.1:0").unwrap();
    let aa = sa.local_addr().unwrap();
    let ab = sb.local_addr().unwrap();
    let old = stale.local_addr().unwrap();
    let mut hints = NativePeerDiscovery::new(1, MonoTime(0)).unwrap();
    let first = PeerEndpointHint {
        peer: ticket(1, 2, 1).peer,
        generation: HintGeneration::new(1).unwrap(),
        endpoint: old,
        expires_at: MonoTime(30000),
    };
    hints.publish(first, MonoTime(0)).unwrap();
    let a = connector(1, 2, old, sa, true);
    let mut b = connector(2, 1, aa, sb, false);
    let mut a = DiscoveryConnector::new(a, hints, MonoTime(0))
        .unwrap_or_else(|_| panic!("discovery construction"));
    let mut timed = req(1, old, 0);
    timed.deadline = MonoTime(5);
    a.submit(timed, MonoTime(0)).unwrap();
    let events = a.poll(MonoTime(5), ConnectPollBudget::default()).unwrap();
    assert_eq!(events.len(), 1);
    assert_eq!(events[0].ticket, ticket(1, 2, 1));
    assert!(matches!(events[0].result, Err(ConnectError::Timeout)));
    assert_eq!(
        a.discovery_mut().resolve(first.peer, MonoTime(5)),
        Err(DiscoveryError::Missing)
    );
    a.discovery_mut()
        .publish(
            PeerEndpointHint {
                generation: HintGeneration::new(2).unwrap(),
                endpoint: ab,
                ..first
            },
            MonoTime(5),
        )
        .unwrap();
    // Original stale input cannot explain success: the refreshed hint selects ab.
    a.submit(req(2, old, 5), MonoTime(5)).unwrap();
    request(&mut b, 2, 1, 2, aa, 5);
    let (mut sessions, now) = pair(&mut a, &mut b, 5);
    exchange(&mut sessions, now);
    // A different remote address cannot bypass the accepted same-peer lease.
    a.discovery_mut()
        .publish(
            PeerEndpointHint {
                generation: HintGeneration::new(3).unwrap(),
                endpoint: old,
                ..first
            },
            MonoTime(now),
        )
        .unwrap();
    let rejected = a.submit(req(3, ab, now), MonoTime(now)).unwrap_err();
    assert_eq!(rejected.reason, ConnectError::Overloaded);
    assert_eq!(rejected.request.ticket, ticket(1, 2, 3));
    assert!(matches!(rejected.request.direction, ConnectDirection::Dial(address) if address == ab));
    assert_eq!(a.usage().requests, 0);
    assert!(sessions.values().all(|s| require_authenticated(s).is_ok()));
    drop(sessions);
    a.discovery_mut()
        .publish(
            PeerEndpointHint {
                generation: HintGeneration::new(4).unwrap(),
                endpoint: ab,
                ..first
            },
            MonoTime(now),
        )
        .unwrap();
    // Refusal did not consume connection generation3; release permits reuse.
    a.submit(req(3, old, now), MonoTime(now)).unwrap();
    request(&mut b, 2, 1, 3, aa, now);
    let (mut sessions, now) = pair(&mut a, &mut b, now);
    a.close();
    b.close();
    assert!(a.is_drained() && b.is_drained());
    drop((a, b));
    exchange(&mut sessions, now);
    drop(sessions);
    UdpSocket::bind(aa).unwrap();
    UdpSocket::bind(ab).unwrap();
}
#[test]
fn discovered_quic_dial_validates_endpoint_and_cannot_replace_peer_pin() {
    let sa = UdpSocket::bind("127.0.0.1:0").unwrap();
    let sb = UdpSocket::bind("127.0.0.1:0").unwrap();
    let aa = sa.local_addr().unwrap();
    let ab = sb.local_addr().unwrap();
    let mut a = connector(1, 2, ab, sa, true);
    for endpoint in [
        aa,
        "127.0.0.1:0".parse().unwrap(),
        "0.0.0.0:1234".parse().unwrap(),
        "224.0.0.1:1234".parse().unwrap(),
        "[::1]:1234".parse().unwrap(),
    ] {
        let original = req(1, endpoint, 0);
        let refused = a.submit(original, MonoTime(0)).unwrap_err();
        assert_eq!(refused.reason, ConnectError::InvalidRequest);
        assert!(
            matches!(refused.request.direction, ConnectDirection::Dial(address) if address == endpoint)
        );
        assert_eq!(a.usage().requests, 0);
    }
    // Server3 is reachable but cannot satisfy the construction-provisioned pin2.
    let mut b = connector(3, 1, aa, sb, false);
    let mut hints = NativePeerDiscovery::new(1, MonoTime(0)).unwrap();
    let hint = PeerEndpointHint {
        peer: ticket(1, 2, 1).peer,
        generation: HintGeneration::new(1).unwrap(),
        endpoint: ab,
        expires_at: MonoTime(20000),
    };
    hints.publish(hint, MonoTime(0)).unwrap();
    let mut a = DiscoveryConnector::new(a, hints, MonoTime(0))
        .unwrap_or_else(|_| panic!("discovery construction"));
    a.submit(req(1, ab, 0), MonoTime(0)).unwrap();
    request(&mut b, 3, 1, 1, aa, 0);
    let mut failure = false;
    for now in 0..10001 {
        for event in a.poll(MonoTime(now), ConnectPollBudget::default()).unwrap() {
            assert_eq!(event.ticket, ticket(1, 2, 1));
            assert!(matches!(event.result, Err(ConnectError::Session(_))));
            failure = true;
        }
        for event in b.poll(MonoTime(now), ConnectPollBudget::default()).unwrap() {
            assert!(event.result.is_err());
        }
        if failure {
            assert_eq!(
                a.discovery_mut().resolve(hint.peer, MonoTime(now)),
                Err(DiscoveryError::Missing)
            );
            break;
        }
    }
    assert!(failure, "untrusted peer was not rejected");
    a.close();
    b.close();
    for now in 10001..10004 {
        for event in b.poll(MonoTime(now), ConnectPollBudget::default()).unwrap() {
            assert!(event.result.is_err());
        }
    }
    assert!(a.is_drained() && b.is_drained());
    drop((a, b));
    UdpSocket::bind(aa).unwrap();
    UdpSocket::bind(ab).unwrap();
}

// Independent host resolver: routing input only, no native cache internals.
struct HostHint {
    hint: Option<PeerEndpointHint>,
    closed: bool,
}
impl PeerDiscovery for HostHint {
    fn resolve(
        &mut self,
        peer: PeerIdentity,
        now: MonoTime,
    ) -> Result<PeerEndpointHint, DiscoveryError> {
        if self.closed {
            return Err(DiscoveryError::Closed);
        }
        self.hint
            .ok_or(DiscoveryError::Missing)?
            .validate(peer, now)
    }
    fn invalidate(&mut self, peer: PeerIdentity, generation: HintGeneration) -> bool {
        if !self.closed
            && self
                .hint
                .is_some_and(|h| h.peer == peer && h.generation == generation)
        {
            self.hint = None;
            true
        } else {
            false
        }
    }
    fn close(&mut self) {
        self.closed = true;
    }
}
#[test]
fn downstream_hint_provider_selects_actual_authenticated_quic_endpoint() {
    let sa = UdpSocket::bind("127.0.0.1:0").unwrap();
    let sb = UdpSocket::bind("127.0.0.1:0").unwrap();
    let stale = UdpSocket::bind("127.0.0.1:0").unwrap();
    let aa = sa.local_addr().unwrap();
    let ab = sb.local_addr().unwrap();
    let old = stale.local_addr().unwrap();
    let hints = HostHint {
        hint: Some(PeerEndpointHint {
            peer: ticket(1, 2, 1).peer,
            generation: HintGeneration::new(1).unwrap(),
            endpoint: ab,
            expires_at: MonoTime(20000),
        }),
        closed: false,
    };
    let mut a = DiscoveryConnector::new(connector(1, 2, old, sa, true), hints, MonoTime(0))
        .unwrap_or_else(|_| panic!("host discovery construction"));
    let mut b = connector(2, 1, aa, sb, false);
    a.submit(req(1, old, 0), MonoTime(0)).unwrap();
    request(&mut b, 2, 1, 1, aa, 0);
    let (mut sessions, now) = pair(&mut a, &mut b, 0);
    a.close();
    b.close();
    assert!(a.is_drained() && b.is_drained());
    assert_eq!(
        a.discovery_mut()
            .resolve(ticket(1, 2, 1).peer, MonoTime(now)),
        Err(DiscoveryError::Closed)
    );
    drop((a, b));
    exchange(&mut sessions, now);
    drop(sessions);
    UdpSocket::bind(aa).unwrap();
    UdpSocket::bind(ab).unwrap();
}
