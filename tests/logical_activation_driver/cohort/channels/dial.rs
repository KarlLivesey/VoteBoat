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
//! A dial completion delayed past the connect deadline's worth of host rounds.
//!
//! The native connector expires attempts before it ingests dial completions,
//! so a host clock that kept advancing while dial work was in flight could
//! expire an attempt whose connection already exists. The fixture clock holds
//! protocol time while any attempt waits for its dial worker.
use super::*;
use std::net::TcpListener;
use voteboat::native::tls::NativeTlsSession;

fn local(id: u64) -> LocalIdentity {
    LocalIdentity {
        node: NodeId::new(id).unwrap(),
        store: StoreBinding {
            identity: support::identity(id.into()),
            session: StoreSession::new(1).unwrap(),
        },
    }
}
fn make(id: u64, peer: u64, now: MonoTime) -> NativePeerConnector {
    let dialer = NativeTcpDialer::spawn(
        local(id),
        BTreeMap::from([(local(peer).node, local(peer).store.identity)]),
        DialLimits::default(),
        Arc::new(ThreadWake::current()),
    )
    .unwrap();
    NativePeerConnector::new(
        NativeConnectConfig {
            local: local(id),
            limits: ConnectLimits::default(),
            session: SessionLimits::default(),
        },
        support::tls::configuration(id),
        BTreeMap::from([(local(peer).node, support::tls::peer(local(peer)))]),
        dialer,
        Some(TcpListener::bind("127.0.0.1:0").unwrap()),
        now,
    )
    .unwrap_or_else(|r| panic!("construct: {:?}", r.reason))
}
/// A dialling and an accepting connector whose TCP connection has already
/// been established by the dial worker, with the completion not yet ingested.
fn dialled(clock: &SimClock) -> [NativePeerConnector; 2] {
    let now = clock.now();
    let mut pair = [make(1, 2, now), make(2, 1, now)];
    let address = pair[1].listener_addr().unwrap().unwrap();
    for (from, to, direction) in [
        (1, 2, ConnectDirection::Dial(address)),
        (2, 1, ConnectDirection::Accept),
    ] {
        let ticket = ConnectTicket {
            local: local(from),
            peer: PeerIdentity {
                node: local(to).node,
                store: local(to).store.identity,
            },
            generation: SecureSessionGeneration::new(1).unwrap(),
        };
        pair[from as usize - 1]
            .submit(
                ConnectRequest {
                    ticket,
                    direction,
                    deadline: MonoTime(now.0 + harness::HANDSHAKE_MS),
                },
                now,
            )
            .unwrap();
    }
    // Only the accepting side is polled until it holds the dialled stream,
    // so the dialer's completion is real but still queued.
    let guard = harness::HangGuard::new();
    while pair[1].usage().anonymous == 0 && pair[1].usage().handshaking == 0 {
        pair[1].poll(now, ConnectPollBudget::default()).unwrap();
        guard.check("dial worker connects");
        std::thread::park_timeout(Duration::from_millis(1));
    }
    assert_eq!(pair[0].usage().dialing, 1);
    pair
}
fn close(mut connectors: [NativePeerConnector; 2], now: MonoTime) {
    let guard = harness::HangGuard::new();
    for c in &mut connectors {
        c.close();
    }
    while !connectors.iter().all(PeerConnector::is_drained) {
        for c in &mut connectors {
            for output in c.poll(now, ConnectPollBudget::default()).unwrap() {
                assert!(output.result.is_err());
            }
        }
        guard.check("connector drain");
        std::thread::park_timeout(Duration::from_millis(1));
    }
    for c in connectors {
        let mut dialer = c.into_dialer().unwrap_or_else(|_| panic!("drained"));
        while !dialer.try_finish().unwrap() {
            guard.check("dialer join");
            std::thread::park_timeout(Duration::from_millis(1));
        }
    }
}
#[test]
fn delayed_dial_completion_holds_protocol_time_until_ingested() {
    // Control: advancing time per round regardless of dial work, as the
    // round-1 fixture did, expires the attempt although its connection exists.
    let clock = SimClock::default();
    let mut unheld = dialled(&clock);
    let late = MonoTime(clock.now().0 + 2 * harness::HANDSHAKE_MS);
    let output = unheld[0]
        .poll(late, ConnectPollBudget::default())
        .unwrap()
        .pop()
        .unwrap();
    assert_eq!(output.result.err(), Some(ConnectError::Timeout));
    close(unheld, late);

    // The fixture clock: twice the deadline's worth of host rounds pass with
    // the completion still queued, and protocol time does not move.
    let clock = SimClock::default();
    let mut pair = dialled(&clock);
    let start = clock.now();
    for _ in 0..2 * harness::HANDSHAKE_MS {
        clock.round(pair.iter().fold(Observed::default(), Observed::connector));
    }
    assert_eq!(clock.now(), start);
    let end = harness::Wait::new(&clock, harness::HANDSHAKE_MS);
    let mut sessions: Vec<NativeTlsSession<_>> = Vec::new();
    while sessions.len() < 2 {
        let now = clock.round(pair.iter().fold(Observed::default(), Observed::connector));
        for c in &mut pair {
            for output in c.poll(now, ConnectPollBudget::default()).unwrap() {
                sessions.push(output.result.unwrap());
            }
        }
        assert!(end.within(&clock), "delayed dial handshake");
        std::thread::park_timeout(Duration::from_millis(1));
    }
    assert!(sessions.iter().all(|s| require_authenticated(s).is_ok()));
    close(pair, clock.now());
}
