// SPDX-License-Identifier: RPL-1.5
// Copyright (c) 2026 Karl Livesey
use super::{
    native::{finish, make, req, step},
    *,
};
use std::{
    io::Write,
    net::TcpStream,
    thread,
    time::{Duration, Instant},
};
use voteboat::native::tls::NativeTlsSession;

fn budget(visits: usize) -> ConnectPollBudget {
    ConnectPollBudget {
        visits,
        socket_calls: 1,
        completions: 1,
        session: SessionPollBudget {
            io_calls: 2,
            read_bytes: 512,
            write_bytes: 512,
        },
    }
}
fn incoming(visits: usize, preload: bool) {
    let mut connector = make(1, &[2], 2);
    let address = connector.listener_addr().unwrap().unwrap();
    let mut stalled = TcpStream::connect(address).unwrap();
    stalled.write_all(b"V").unwrap();
    if !preload {
        assert!(step(&mut connector, 0).is_empty());
        assert_eq!(connector.usage().anonymous, 1);
    }
    let expected = ticket(1, 2, 9);
    connector
        .submit(req(expected, ConnectDirection::Accept), MonoTime(0))
        .unwrap();
    let mut stream = TcpStream::connect(address).unwrap();
    stream.write_all(b"VBCONN01").unwrap();
    stream.write_all(&2u64.to_le_bytes()).unwrap();
    let mut client = NativeTlsSession::client_tcp(
        stream,
        &support::tls::configuration(2),
        local(2),
        support::tls::peer(local(1)),
        expected.generation,
        SessionLimits::default(),
        MonoTime(0),
    )
    .unwrap();
    if preload {
        let accepted = connector
            .poll(
                MonoTime(0),
                ConnectPollBudget {
                    visits: 0,
                    socket_calls: 2,
                    completions: 0,
                    ..budget(0)
                },
            )
            .unwrap();
        assert!(accepted.is_empty());
        assert_eq!(connector.usage().anonymous, 2);
    }
    let deadline = Instant::now() + Duration::from_secs(3);
    let mut accepted = None;
    while accepted.is_none() || client.state() != SessionState::Ready {
        client
            .poll(MonoTime(0), SessionPollBudget::default())
            .unwrap();
        for completion in connector.poll(MonoTime(0), budget(visits)).unwrap() {
            assert_eq!(completion.ticket, expected);
            assert!(accepted.is_none(), "duplicate terminal connection");
            let session = completion.result.unwrap();
            let binding = require_authenticated(&session).unwrap();
            assert_eq!(binding.local, expected.local);
            assert_eq!(binding.peer.node, expected.peer.node);
            assert_eq!(binding.peer.store.identity, expected.peer.store);
            accepted = Some(session);
        }
        assert!(connector.usage().anonymous <= 2);
        assert!(
            Instant::now() < deadline,
            "stalled anonymous stream starved incoming peer: {:?}",
            connector.usage()
        );
        thread::park_timeout(Duration::from_millis(1));
    }
    assert_eq!(connector.usage().anonymous, 1);
    assert_eq!(connector.usage().requests, 0);
    finish(connector, 0);
    assert!(require_authenticated(accepted.as_ref().unwrap()).is_ok());
}
#[test]
fn incomplete_anonymous_preface_cannot_starve_new_accept_with_one_socket_call() {
    incoming(2, false);
}
#[test]
fn exhausted_budget_does_not_skip_later_anonymous_slots() {
    incoming(2, true);
}
#[test]
fn incomplete_anonymous_preface_cannot_starve_authorized_outbound_preface() {
    outgoing(false);
}
#[test]
fn zero_socket_budget_polls_do_not_consume_a_fairness_turn() {
    outgoing(true);
}
fn no_socket_work(connector: &mut voteboat::native::connect::NativePeerConnector) {
    for _ in 0..2 {
        assert!(connector
            .poll(
                MonoTime(0),
                ConnectPollBudget {
                    visits: 0,
                    socket_calls: 0,
                    completions: 0,
                    ..ConnectPollBudget::default()
                },
            )
            .unwrap()
            .is_empty());
    }
}
fn outgoing(interleave_zero: bool) {
    let mut left = make(1, &[2], 1);
    let mut right = make(2, &[1], 1);
    let _stalled = TcpStream::connect(left.listener_addr().unwrap().unwrap()).unwrap();
    assert!(step(&mut left, 0).is_empty());
    assert_eq!(left.usage().anonymous, 1);
    if interleave_zero {
        no_socket_work(&mut left);
    }
    let address = right.listener_addr().unwrap().unwrap();
    left.submit(
        req(ticket(1, 2, 1), ConnectDirection::Dial(address)),
        MonoTime(0),
    )
    .unwrap();
    right
        .submit(req(ticket(2, 1, 1), ConnectDirection::Accept), MonoTime(0))
        .unwrap();
    let deadline = Instant::now() + Duration::from_secs(3);
    let mut sessions = Vec::new();
    while sessions.len() < 2 {
        for (connector, expected) in [(&mut left, ticket(1, 2, 1)), (&mut right, ticket(2, 1, 1))] {
            for completion in step(connector, 0) {
                assert_eq!(completion.ticket, expected);
                let session = completion.result.unwrap();
                let binding = require_authenticated(&session).unwrap();
                assert_eq!(binding.local, expected.local);
                assert_eq!(binding.peer.node, expected.peer.node);
                sessions.push(session);
            }
            if interleave_zero {
                no_socket_work(connector);
            }
        }
        assert!(
            Instant::now() < deadline,
            "stalled anonymous stream starved outbound peer: {:?}",
            left.usage()
        );
        thread::park_timeout(Duration::from_millis(1));
    }
    assert_eq!(left.usage().anonymous, 1);
    finish(left, 0);
    finish(right, 0);
    assert!(sessions.iter().all(|s| require_authenticated(s).is_ok()));
}
