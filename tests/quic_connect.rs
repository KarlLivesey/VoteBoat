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
#![cfg(feature = "quic")]
#[path = "quic_connect/discovery.rs"]
mod discovered;
#[path = "quic_connect/remote_discovery.rs"]
mod remote_discovery;
mod support;
use std::{
    collections::BTreeMap,
    net::{SocketAddr, UdpSocket},
};
use voteboat::{
    connect::*,
    identity::*,
    native::{connect::NativeConnectConfig, quic::NativeQuicSession, quic_connect::*},
    runtime::MonoTime,
    secure::*,
    transport::ConnectTicket,
};
fn local(n: u64) -> LocalIdentity {
    LocalIdentity {
        node: support::node(n),
        store: StoreBinding {
            identity: support::identity(n.into()),
            session: StoreSession::new(n + 10).unwrap(),
        },
    }
}
fn ticket(from: u64, to: u64, g: u64) -> ConnectTicket {
    ConnectTicket {
        local: local(from),
        peer: support::tls::peer(local(to)).identity,
        generation: SecureSessionGeneration::new(g).unwrap(),
    }
}
fn config(n: u64) -> NativeConnectConfig {
    NativeConnectConfig {
        local: local(n),
        limits: ConnectLimits {
            requests: 2,
            anonymous: 0,
            timeout_ms: 10000,
        },
        session: SessionLimits::default(),
    }
}
fn connectors() -> Vec<NativeQuicConnector> {
    versioned_connectors([1, 1, 1])
}
fn versioned_connectors(versions: [u16; 3]) -> Vec<NativeQuicConnector> {
    let sockets = (0..3)
        .map(|_| UdpSocket::bind("127.0.0.1:0").unwrap())
        .collect::<Vec<_>>();
    let addresses = sockets
        .iter()
        .map(|s| s.local_addr().unwrap())
        .collect::<Vec<_>>();
    sockets
        .into_iter()
        .enumerate()
        .map(|(i, s)| {
            let n = i as u64 + 1;
            NativeQuicConnector::new(
                config(n),
                support::tls::configuration(n)
                    .with_wire_version(versions[i])
                    .unwrap(),
                (1..=3)
                    .filter(|p| *p != n)
                    .map(|p| {
                        (
                            support::node(p),
                            (addresses[p as usize - 1], support::tls::peer(local(p))),
                        )
                    })
                    .collect(),
                s,
                MonoTime(0),
            )
            .unwrap()
        })
        .collect()
}
fn request(c: &mut NativeQuicConnector, from: u64, to: u64, g: u64, remote: SocketAddr, now: u64) {
    c.submit(
        ConnectRequest {
            ticket: ticket(from, to, g),
            direction: if from < to {
                ConnectDirection::Dial(remote)
            } else {
                ConnectDirection::Accept
            },
            deadline: MonoTime(now + 10000),
        },
        MonoTime(now),
    )
    .unwrap();
}
fn establish(
    c: &mut [NativeQuicConnector],
    start: u64,
    g: u64,
) -> (BTreeMap<(u64, u64), NativeQuicSession>, u64) {
    let addresses = c.iter().map(|c| c.local_addr()).collect::<Vec<_>>();
    for from in 1..=3 {
        for to in 1..=3 {
            if from != to {
                request(
                    &mut c[from as usize - 1],
                    from,
                    to,
                    g,
                    addresses[to as usize - 1],
                    start,
                );
            }
        }
    }
    let mut sessions = BTreeMap::new();
    let tiny = ConnectPollBudget {
        visits: 1,
        completions: 1,
        session: SessionPollBudget {
            io_calls: 1,
            read_bytes: 1200,
            write_bytes: 1200,
        },
        ..ConnectPollBudget::default()
    };
    for now in start..start + 9000 {
        for from in 1..=3 {
            for event in c[from - 1].poll(MonoTime(now), tiny).unwrap() {
                let session = event.result.unwrap();
                assert_eq!(
                    session.binding().unwrap().generation,
                    event.ticket.generation
                );
                sessions.insert((from as u64, event.ticket.peer.node.get()), session);
            }
        }
        // Transferred sessions must continue protocol ACKs while other peers establish.
        for s in sessions.values_mut() {
            s.poll(MonoTime(now), SessionPollBudget::default()).unwrap();
        }
        if sessions.len() == 6 {
            return (sessions, now);
        }
    }
    panic!("shared UDP establishment stalled: {}", sessions.len());
}
fn exchange(sessions: &mut BTreeMap<(u64, u64), NativeQuicSession>, start: u64) {
    for (key, s) in sessions.iter_mut() {
        assert_eq!(s.write_plaintext(&[key.0 as u8, key.1 as u8]), Ok(2));
    }
    let mut received = BTreeMap::new();
    for now in start..start + 4000 {
        for (key, s) in sessions.iter_mut() {
            s.poll(MonoTime(now), SessionPollBudget::default()).unwrap();
            if !received.contains_key(key) {
                let mut bytes = [0; 2];
                match s.read_plaintext(&mut bytes) {
                    Ok(2) => {
                        assert_eq!(bytes, [key.1 as u8, key.0 as u8]);
                        received.insert(*key, ());
                    }
                    Err(SessionError::WouldBlock) => (),
                    other => panic!("unexpected read {other:?}"),
                }
            }
        }
        if received.len() == sessions.len() {
            return;
        }
    }
    panic!("shared UDP data stalled");
}
#[test]
fn one_socket_per_node_establishes_multiple_peers_and_close_preserves_transferred_sessions() {
    let mut c = connectors();
    let addresses = c.iter().map(|c| c.local_addr()).collect::<Vec<_>>();
    let (mut sessions, now) = establish(&mut c, 0, 1);
    for (i, connector) in c.iter_mut().enumerate() {
        assert!(connector.is_drained());
        let to = if i == 0 { 2 } else { 1 };
        let r = ConnectRequest {
            ticket: ticket(i as u64 + 1, to, 2),
            direction: ConnectDirection::Dial(addresses[to as usize - 1]),
            deadline: MonoTime(now + 10000),
        };
        assert_eq!(
            connector.submit(r, MonoTime(now)).unwrap_err().reason,
            ConnectError::Overloaded
        );
        connector.close();
        assert!(connector.is_drained());
    }
    drop(c);
    exchange(&mut sessions, now + 1);
    drop(sessions);
    for address in addresses {
        UdpSocket::bind(address).unwrap();
    }
}
#[test]
fn retired_leases_release_socket_routes_for_fresh_generations() {
    let mut c = connectors();
    let (sessions, now) = establish(&mut c, 0, 7);
    drop(sessions);
    let (mut sessions, now) = establish(&mut c, now + 1, 8);
    exchange(&mut sessions, now + 1);
}
#[test]
fn cancellation_and_expiry_keep_exact_terminal_slots_and_reject_stale_or_wrong_requests() {
    let mut c = connectors();
    let remote = c[1].local_addr();
    request(&mut c[0], 1, 2, 1, remote, 0);
    assert!(!c[0].cancel(ticket(1, 2, 2)));
    assert!(c[0].cancel(ticket(1, 2, 1)));
    assert_eq!(c[0].usage().requests, 1);
    assert_eq!(c[0].usage().handshaking, 0);
    assert_eq!(c[0].next_deadline(), Some(MonoTime(0)));
    let no_receipts = ConnectPollBudget {
        visits: 0,
        completions: 0,
        socket_calls: 0,
        ..ConnectPollBudget::default()
    };
    assert!(c[0].poll(MonoTime(1), no_receipts).unwrap().is_empty());
    assert_eq!(c[0].usage().requests, 1);
    let event = c[0]
        .poll(MonoTime(2), ConnectPollBudget::default())
        .unwrap()
        .pop()
        .unwrap();
    assert_eq!(event.ticket, ticket(1, 2, 1));
    assert!(matches!(event.result, Err(ConnectError::Cancelled)));
    let make = |g| ConnectRequest {
        ticket: ticket(1, 2, g),
        direction: ConnectDirection::Dial(remote),
        deadline: MonoTime(10002),
    };
    assert_eq!(
        c[0].submit(make(1), MonoTime(2)).unwrap_err().reason,
        ConnectError::StaleConnection
    );
    let mut wrong = make(2);
    wrong.ticket.peer.store = support::identity(999);
    assert_eq!(
        c[0].submit(wrong, MonoTime(2)).unwrap_err().reason,
        ConnectError::WrongBinding
    );
    let mut wrong = make(2);
    wrong.direction = ConnectDirection::Dial(c[2].local_addr());
    assert_eq!(
        c[0].submit(wrong, MonoTime(2)).unwrap_err().reason,
        ConnectError::InvalidRequest
    );
    c[0].submit(make(2), MonoTime(2)).unwrap();
    let bad = ConnectPollBudget {
        visits: 1025,
        ..ConnectPollBudget::default()
    };
    assert_eq!(
        c[0].poll(MonoTime(10002), bad).err(),
        Some(ConnectError::InvalidLimits)
    );
    assert_eq!(c[0].usage().handshaking, 1);
    assert_eq!(
        c[0].poll(MonoTime(1), no_receipts).err(),
        Some(ConnectError::TimeWentBack)
    );
    let event = c[0]
        .poll(MonoTime(10002), ConnectPollBudget::default())
        .unwrap()
        .pop()
        .unwrap();
    assert!(matches!(event.result, Err(ConnectError::Timeout)));
    assert!(c[0].is_drained());
    c[0].close();
    assert_eq!(
        c[0].submit(make(3), MonoTime(10002)).unwrap_err().reason,
        ConnectError::Closed
    );
}
#[test]
fn failed_construction_returns_socket_without_starting_protocol_work() {
    let socket = UdpSocket::bind("127.0.0.1:0").unwrap();
    let address = socket.local_addr().unwrap();
    let remote = UdpSocket::bind("127.0.0.1:0")
        .unwrap()
        .local_addr()
        .unwrap();
    let peers = [
        (support::node(2), (remote, support::tls::peer(local(2)))),
        (support::node(3), (remote, support::tls::peer(local(3)))),
    ]
    .into();
    let rejection = match NativeQuicConnector::new(
        config(1),
        support::tls::configuration(1),
        peers,
        socket,
        MonoTime(0),
    ) {
        Ok(_) => panic!("duplicate address accepted"),
        Err(r) => r,
    };
    assert_eq!(rejection.reason, ConnectError::InvalidRequest);
    assert_eq!(rejection.socket.local_addr().unwrap(), address);
    drop(rejection);
    UdpSocket::bind(address).unwrap();
}

#[test]
fn shared_quic_connector_preserves_selected_versions_and_rejects_mismatches() {
    for version in [2, 3, 4] {
        let mut connectors = versioned_connectors([version; 3]);
        let (mut sessions, now) = establish(&mut connectors, 0, 1);
        assert!(sessions
            .values()
            .all(|s| require_authenticated(s).unwrap().wire_version == version));
        exchange(&mut sessions, now);
        for connector in &mut connectors {
            connector.close();
            assert!(connector.is_drained());
        }
    }
    let mut connectors = versioned_connectors([1, 3, 3]);
    let address = connectors[1].local_addr();
    request(&mut connectors[0], 1, 2, 1, address, 0);
    request(&mut connectors[1], 2, 1, 1, address, 0);
    let mut results = Vec::new();
    for now in 0..10001 {
        for c in &mut connectors {
            results.extend(c.poll(MonoTime(now), ConnectPollBudget::default()).unwrap());
        }
        if results.len() == 2 {
            break;
        }
    }
    assert_eq!(results.len(), 2);
    assert!(results.iter().all(|event| event.result.is_err()));
    assert!(results.iter().any(|event| matches!(
        event.result,
        Err(ConnectError::Session(SessionError::IncompatibleProtocol))
    )));
    for c in &mut connectors {
        c.close();
        assert!(c.is_drained());
    }
}
