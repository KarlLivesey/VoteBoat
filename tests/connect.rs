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
#[cfg(feature = "tls")]
#[path = "connect/fairness.rs"]
mod fairness;
mod support;
use voteboat::{connect::*, identity::*, runtime::MonoTime, secure::*, transport::ConnectTicket};
fn local(id: u64) -> LocalIdentity {
    LocalIdentity {
        node: NodeId::new(id).unwrap(),
        store: StoreBinding {
            identity: support::identity(id as u128),
            session: StoreSession::new(1).unwrap(),
        },
    }
}
fn ticket(from: u64, to: u64, generation: u64) -> ConnectTicket {
    ConnectTicket {
        local: local(from),
        peer: PeerIdentity {
            node: local(to).node,
            store: local(to).store.identity,
        },
        generation: SecureSessionGeneration::new(generation).unwrap(),
    }
}
/// Trusted host's independent session, not cryptographic test evidence.
struct HostSession(SessionBinding);
impl SecureSession for HostSession {
    fn security(&self) -> SessionSecurity {
        SessionSecurity::Authenticated
    }
    fn state(&self) -> SessionState {
        SessionState::Ready
    }
    fn binding(&self) -> Option<SessionBinding> {
        Some(self.0)
    }
    fn limits(&self) -> SessionLimits {
        SessionLimits::default()
    }
    fn poll(&mut self, _: MonoTime, b: SessionPollBudget) -> Result<SessionProgress, SessionError> {
        b.validate()?;
        Ok(SessionProgress::default())
    }
    fn read_plaintext(&mut self, _: &mut [u8]) -> Result<usize, SessionError> {
        Err(SessionError::WouldBlock)
    }
    fn write_plaintext(&mut self, _: &[u8]) -> Result<usize, SessionError> {
        Err(SessionError::WouldBlock)
    }
    fn is_flushed(&self) -> bool {
        true
    }
    fn close(&mut self) {}
    fn revoke(&mut self) {}
}
struct HostConnector {
    pending: Option<(ConnectRequest<()>, bool)>,
    generation: u64,
    closed: bool,
    now: MonoTime,
}
impl PeerConnector for HostConnector {
    type Endpoint = ();
    type Session = HostSession;
    fn local(&self) -> LocalIdentity {
        local(1)
    }
    fn limits(&self) -> ConnectLimits {
        ConnectLimits {
            requests: 1,
            anonymous: 0,
            timeout_ms: 100,
        }
    }
    fn usage(&self) -> ConnectUsage {
        ConnectUsage {
            requests: usize::from(self.pending.is_some()),
            ..ConnectUsage::default()
        }
    }
    fn next_deadline(&self) -> Option<MonoTime> {
        self.pending
            .as_ref()
            .map(|(r, c)| if *c { self.now } else { r.deadline })
    }
    fn submit(&mut self, r: ConnectRequest<()>, now: MonoTime) -> Result<(), ConnectRejected<()>> {
        let reason = if now < self.now {
            Some(ConnectError::TimeWentBack)
        } else if self.closed {
            Some(ConnectError::Closed)
        } else if r.ticket.local != local(1) || r.ticket.peer != ticket(1, 2, 1).peer {
            Some(ConnectError::WrongBinding)
        } else if r.ticket.generation.get() <= self.generation {
            Some(ConnectError::StaleConnection)
        } else if r.deadline <= now || r.deadline.0 - now.0 > 100 {
            Some(ConnectError::InvalidRequest)
        } else if self.pending.is_some() {
            Some(ConnectError::Overloaded)
        } else {
            None
        };
        if let Some(reason) = reason {
            return Err(ConnectRejected {
                reason,
                request: Box::new(r),
            });
        }
        self.generation = r.ticket.generation.get();
        self.now = now;
        self.pending = Some((r, false));
        Ok(())
    }
    fn cancel(&mut self, t: ConnectTicket) -> bool {
        if let Some((r, c)) = &mut self.pending {
            if r.ticket == t {
                *c = true;
                return true;
            }
        }
        false
    }
    fn poll(
        &mut self,
        now: MonoTime,
        b: ConnectPollBudget,
    ) -> Result<Vec<ConnectCompletion<HostSession>>, ConnectError> {
        b.validate()?;
        if now < self.now {
            return Err(ConnectError::TimeWentBack);
        }
        self.now = now;
        if b.completions == 0 {
            return Ok(vec![]);
        }
        Ok(self
            .pending
            .take()
            .map(|(r, c)| ConnectCompletion {
                ticket: r.ticket,
                result: if c {
                    Err(ConnectError::Cancelled)
                } else if now >= r.deadline {
                    Err(ConnectError::Timeout)
                } else {
                    Ok(HostSession(SessionBinding {
                        local: r.ticket.local,
                        peer: local(2),
                        generation: r.ticket.generation,
                        wire_version: 1,
                    }))
                },
            })
            .into_iter()
            .collect())
    }
    fn close(&mut self) {
        self.closed = true;
        if let Some((_, c)) = &mut self.pending {
            *c = true;
        }
    }
}
#[test]
fn downstream_connector_uses_public_tickets_and_local_secure_session_type() {
    let mut host = HostConnector {
        pending: None,
        generation: 0,
        closed: false,
        now: MonoTime(0),
    };
    let provider: &mut dyn PeerConnector<Endpoint = (), Session = HostSession> = &mut host;
    let request = |generation| ConnectRequest {
        ticket: ticket(1, 2, generation),
        direction: ConnectDirection::Accept,
        deadline: MonoTime(100),
    };
    provider.submit(request(1), MonoTime(0)).unwrap();
    assert!(!provider.cancel(ticket(1, 2, 2)));
    let rejected = provider.submit(request(2), MonoTime(0)).unwrap_err();
    assert_eq!(rejected.reason, ConnectError::Overloaded);
    assert_eq!(rejected.request.ticket, ticket(1, 2, 2));
    assert!(provider.cancel(ticket(1, 2, 1)));
    assert_eq!(provider.usage().requests, 1);
    assert!(provider
        .poll(
            MonoTime(0),
            ConnectPollBudget {
                completions: 0,
                ..ConnectPollBudget::default()
            }
        )
        .unwrap()
        .is_empty());
    let event = provider
        .poll(MonoTime(0), ConnectPollBudget::default())
        .unwrap()
        .pop()
        .unwrap();
    assert_eq!(event.ticket, ticket(1, 2, 1));
    assert!(matches!(event.result, Err(ConnectError::Cancelled)));
    provider.submit(*rejected.request, MonoTime(0)).unwrap();
    let event = provider
        .poll(MonoTime(0), ConnectPollBudget::default())
        .unwrap()
        .pop()
        .unwrap();
    let binding = require_authenticated(&event.result.unwrap()).unwrap();
    assert_eq!(binding.local, ticket(1, 2, 2).local);
    assert_eq!(binding.generation, ticket(1, 2, 2).generation);
    provider.close();
    assert!(provider.is_drained());
}
#[test]
fn connection_limits_and_poll_budgets_reject_excessive_work() {
    assert!(ConnectLimits::default().validate().is_ok());
    for limits in [
        ConnectLimits {
            requests: 0,
            ..ConnectLimits::default()
        },
        ConnectLimits {
            requests: 1025,
            ..ConnectLimits::default()
        },
        ConnectLimits {
            anonymous: 1025,
            ..ConnectLimits::default()
        },
        ConnectLimits {
            timeout_ms: 0,
            ..ConnectLimits::default()
        },
        ConnectLimits {
            timeout_ms: 60001,
            ..ConnectLimits::default()
        },
    ] {
        assert_eq!(limits.validate(), Err(ConnectError::InvalidLimits));
    }
    assert!(ConnectPollBudget {
        visits: 1025,
        ..ConnectPollBudget::default()
    }
    .validate()
    .is_err());
    assert!(ConnectPollBudget {
        socket_calls: 1025,
        ..ConnectPollBudget::default()
    }
    .validate()
    .is_err());
    assert!(ConnectPollBudget {
        completions: 1025,
        ..ConnectPollBudget::default()
    }
    .validate()
    .is_err());
}
#[cfg(feature = "tls")]
mod native {
    use super::*;
    use std::{
        collections::BTreeMap,
        io::{Read, Write},
        net::{SocketAddr, TcpListener, TcpStream},
        sync::{Arc, Mutex},
        thread,
        time::{Duration, Instant},
    };
    use voteboat::{
        dial::*,
        native::{connect::*, dial::NativeTcpDialer, tls::NativeTlsSession, worker::ThreadWake},
    };
    pub(super) fn make(id: u64, peers: &[u64], limit: usize) -> NativePeerConnector {
        versioned_make(id, peers, limit, 1)
    }
    fn versioned_make(id: u64, peers: &[u64], limit: usize, version: u16) -> NativePeerConnector {
        let map = peers
            .iter()
            .map(|id| (local(*id).node, local(*id).store.identity))
            .collect();
        let dialer = NativeTcpDialer::spawn(
            local(id),
            map,
            DialLimits::default(),
            Arc::new(ThreadWake::current()),
        )
        .unwrap();
        NativePeerConnector::new(
            NativeConnectConfig {
                local: local(id),
                limits: ConnectLimits {
                    requests: limit,
                    anonymous: limit,
                    timeout_ms: 100,
                },
                session: SessionLimits {
                    write_buffer_bytes: 256,
                    ..SessionLimits::default()
                },
            },
            support::tls::configuration(id)
                .with_wire_version(version)
                .unwrap(),
            peers
                .iter()
                .map(|id| (local(*id).node, support::tls::peer(local(*id))))
                .collect(),
            dialer,
            Some(TcpListener::bind("127.0.0.1:0").unwrap()),
            MonoTime(0),
        )
        .unwrap_or_else(|r| panic!("construct: {:?}", r.reason))
    }
    pub(super) fn req(
        t: ConnectTicket,
        direction: ConnectDirection<SocketAddr>,
    ) -> ConnectRequest<SocketAddr> {
        ConnectRequest {
            ticket: t,
            direction,
            deadline: MonoTime(100),
        }
    }
    pub(super) fn step(
        c: &mut NativePeerConnector,
        now: u64,
    ) -> Vec<ConnectCompletion<NativeTlsSession<TcpStream>>> {
        c.poll(
            MonoTime(now),
            ConnectPollBudget {
                visits: 1,
                socket_calls: 1,
                completions: 1,
                session: SessionPollBudget {
                    io_calls: 2,
                    read_bytes: 512,
                    write_bytes: 512,
                },
            },
        )
        .unwrap()
    }
    pub(super) fn finish(mut c: NativePeerConnector, now: u64) {
        c.close();
        let deadline = Instant::now() + Duration::from_secs(5);
        while !c.is_drained() {
            for e in step(&mut c, now) {
                assert!(e.result.is_err());
            }
            assert!(Instant::now() < deadline);
            thread::park_timeout(Duration::from_millis(1));
        }
        assert_eq!(c.listener_addr().unwrap(), None);
        let mut dialer = c.into_dialer().unwrap_or_else(|_| panic!("not drained"));
        while !dialer.try_finish().unwrap() {
            assert!(Instant::now() < deadline);
            thread::yield_now();
        }
    }
    #[test]
    fn one_listener_establishes_multiple_peers_with_bounded_fair_polls() {
        let mut nodes = [
            make(1, &[2, 3], 2),
            make(2, &[1, 3], 2),
            make(3, &[1, 2], 2),
        ];
        let addresses = nodes
            .iter()
            .map(|c| c.listener_addr().unwrap().unwrap())
            .collect::<Vec<_>>();
        // Submit distinct peer generations in descending order, never a global floor.
        for (i, c) in nodes.iter_mut().enumerate() {
            for j in (0..3).rev().filter(|j| *j != i) {
                c.submit(
                    req(
                        ticket(i as u64 + 1, j as u64 + 1, j as u64 + 1),
                        if i < j {
                            ConnectDirection::Dial(addresses[j])
                        } else {
                            ConnectDirection::Accept
                        },
                    ),
                    MonoTime(0),
                )
                .unwrap();
            }
        }
        let deadline = Instant::now() + Duration::from_secs(5);
        let mut sessions = Vec::new();
        while sessions.len() != 6 {
            for c in &mut nodes {
                for e in step(c, 0) {
                    let binding = require_authenticated(e.result.as_ref().unwrap()).unwrap();
                    assert_eq!(binding.local, e.ticket.local);
                    assert_eq!(binding.peer.node, e.ticket.peer.node);
                    assert_eq!(binding.generation, e.ticket.generation);
                    sessions.push(e.result.unwrap());
                }
                assert!(c.usage().requests <= 2);
                assert!(c.usage().anonymous <= 2);
            }
            assert!(Instant::now() < deadline);
            thread::park_timeout(Duration::from_millis(1));
        }
        assert!(nodes.iter().all(PeerConnector::is_drained));
        for c in nodes {
            finish(c, 0);
        }
        // Returned sessions remain owned by the caller after connectors close.
        assert!(sessions.iter().all(|s| require_authenticated(s).is_ok()));
    }
    #[test]
    fn anonymous_prefaces_are_fixed_bounded_and_expire_without_granting_a_ticket() {
        let mut c = make(1, &[2], 1);
        let address = c.listener_addr().unwrap().unwrap();
        c.submit(req(ticket(1, 2, 1), ConnectDirection::Accept), MonoTime(0))
            .unwrap();
        let mut incoming = TcpStream::connect(address).unwrap();
        incoming.write_all(b"V").unwrap();
        assert!(step(&mut c, 0).is_empty());
        assert_eq!(c.usage().anonymous, 1);
        let _extra = TcpStream::connect(address).unwrap();
        for _ in 0..3 {
            assert!(step(&mut c, 0).is_empty());
            assert_eq!(c.usage().anonymous, 1);
            assert_eq!(c.usage().handshaking, 0);
        }
        let events = step(&mut c, 100);
        assert_eq!(events.len(), 1);
        assert!(matches!(events[0].result, Err(ConnectError::Timeout)));
        // The old anonymous socket is closed; queued sockets may consume a fresh bounded slot.
        incoming
            .set_read_timeout(Some(Duration::from_secs(5)))
            .unwrap();
        assert_eq!(incoming.read(&mut [0]).unwrap(), 0);
        finish(c, 100);
    }
    #[test]
    fn invalid_hint_drops_socket_without_consuming_authorized_attempt() {
        let mut c = make(1, &[2], 1);
        c.submit(req(ticket(1, 2, 1), ConnectDirection::Accept), MonoTime(0))
            .unwrap();
        for (magic, peer) in [
            (b"BADMAGIC".as_slice(), 2u64),
            (b"VBCONN01".as_slice(), 99),
            (b"VBCONN01".as_slice(), 0),
        ] {
            let mut stream = TcpStream::connect(c.listener_addr().unwrap().unwrap()).unwrap();
            stream.write_all(magic).unwrap();
            stream.write_all(&peer.to_le_bytes()).unwrap();
            stream.set_nonblocking(true).unwrap();
            let deadline = Instant::now() + Duration::from_secs(2);
            loop {
                assert!(step(&mut c, 0).is_empty());
                assert_eq!(c.usage().requests, 1);
                assert_eq!(c.usage().handshaking, 0);
                match stream.read(&mut [0]) {
                    Ok(0) => break,
                    Err(error) if error.kind() == std::io::ErrorKind::WouldBlock => (),
                    result => panic!("invalid hint socket was not closed cleanly: {result:?}"),
                }
                assert!(Instant::now() < deadline, "invalid hint socket stayed open");
                thread::park_timeout(Duration::from_millis(1));
            }
            assert_eq!(c.usage().anonymous, 0);
            assert_eq!(c.usage().requests, 1);
            assert_eq!(c.usage().handshaking, 0);
        }
        assert!(c.cancel(ticket(1, 2, 1)));
        assert!(matches!(
            step(&mut c, 0).pop().unwrap().result,
            Err(ConnectError::Cancelled)
        ));
        finish(c, 0);
    }
    #[test]
    fn forged_authorized_hint_still_fails_tls_certificate_pin() {
        let mut c = make(1, &[2], 1);
        c.submit(req(ticket(1, 2, 1), ConnectDirection::Accept), MonoTime(0))
            .unwrap();
        let mut stream = TcpStream::connect(c.listener_addr().unwrap().unwrap()).unwrap();
        stream.write_all(b"VBCONN01").unwrap();
        stream.write_all(&2u64.to_le_bytes()).unwrap();
        let mut attacker = NativeTlsSession::client_tcp(
            stream,
            &support::tls::configuration(3),
            local(3),
            support::tls::peer(local(1)),
            SecureSessionGeneration::new(9).unwrap(),
            SessionLimits::default(),
            MonoTime(0),
        )
        .unwrap();
        let deadline = Instant::now() + Duration::from_secs(5);
        let event = loop {
            let _ = attacker.poll(MonoTime(0), SessionPollBudget::default());
            if let Some(e) = step(&mut c, 0).pop() {
                break e;
            }
            assert!(Instant::now() < deadline);
            thread::park_timeout(Duration::from_millis(1));
        };
        assert_eq!(event.ticket, ticket(1, 2, 1));
        assert!(matches!(
            event.result,
            Err(ConnectError::Session(SessionError::WrongPeer))
        ));
        finish(c, 0);
    }
    #[test]
    fn request_scope_budget_time_and_exact_cancellation_are_checked_before_progress() {
        let mut c = make(1, &[2], 1);
        let mut wrong = ticket(1, 2, 1);
        wrong.local.store.session = StoreSession::new(2).unwrap();
        assert_eq!(
            c.submit(req(wrong, ConnectDirection::Accept), MonoTime(0))
                .unwrap_err()
                .reason,
            ConnectError::WrongBinding
        );
        assert_eq!(
            c.submit(req(ticket(1, 3, 1), ConnectDirection::Accept), MonoTime(0))
                .unwrap_err()
                .reason,
            ConnectError::UnknownPeer
        );
        c.submit(req(ticket(1, 2, 1), ConnectDirection::Accept), MonoTime(0))
            .unwrap();
        assert!(c
            .poll(
                MonoTime(1),
                ConnectPollBudget {
                    socket_calls: 1025,
                    ..ConnectPollBudget::default()
                }
            )
            .is_err());
        assert_eq!(c.usage().anonymous, 0);
        assert_eq!(c.usage().requests, 1);
        assert!(step(&mut c, 1).is_empty());
        assert!(matches!(
            c.poll(MonoTime(0), ConnectPollBudget::default()),
            Err(ConnectError::TimeWentBack)
        ));
        let rejected = c
            .submit(req(ticket(1, 2, 2), ConnectDirection::Accept), MonoTime(1))
            .unwrap_err();
        assert_eq!(rejected.reason, ConnectError::Overloaded);
        assert!(!c.cancel(ticket(1, 2, 2)));
        assert!(c.cancel(ticket(1, 2, 1)));
        assert_eq!(c.usage().requests, 1);
        assert!(c
            .poll(
                MonoTime(1),
                ConnectPollBudget {
                    completions: 0,
                    socket_calls: 0,
                    visits: 0,
                    ..ConnectPollBudget::default()
                }
            )
            .unwrap()
            .is_empty());
        assert_eq!(c.next_deadline(), Some(MonoTime(1)));
        assert!(matches!(
            step(&mut c, 1).pop().unwrap().result,
            Err(ConnectError::Cancelled)
        ));
        assert_eq!(
            c.submit(req(ticket(1, 2, 1), ConnectDirection::Accept), MonoTime(1))
                .unwrap_err()
                .reason,
            ConnectError::StaleConnection
        );
        c.submit(*rejected.request, MonoTime(1)).unwrap();
        finish(c, 1);
    }
    #[derive(Default)]
    struct Control {
        pending: Option<ConnectTicket>,
        cancelled: bool,
        release: bool,
        mis_scope: bool,
    }
    #[test]
    fn fragmented_routing_then_stalled_handshake_closes_on_timeout_or_cancel() {
        for cancel in [false, true] {
            let mut c = make(1, &[2], 1);
            c.submit(req(ticket(1, 2, 1), ConnectDirection::Accept), MonoTime(0))
                .unwrap();
            let mut stream = TcpStream::connect(c.listener_addr().unwrap().unwrap()).unwrap();
            stream.write_all(b"V").unwrap();
            let deadline = std::time::Instant::now() + Duration::from_secs(2);
            while c.usage().anonymous != 1 {
                assert!(step(&mut c, 0).is_empty());
                assert!(std::time::Instant::now() < deadline, "accept stalled");
                std::thread::park_timeout(Duration::from_millis(1));
            }
            assert_eq!(c.usage().anonymous, 1);
            assert_eq!(c.usage().handshaking, 0);
            stream.write_all(b"BCONN01").unwrap();
            stream.write_all(&2u64.to_le_bytes()).unwrap();
            let deadline = std::time::Instant::now() + Duration::from_secs(2);
            while c.usage().handshaking != 1 {
                assert!(step(&mut c, 0).is_empty());
                assert!(std::time::Instant::now() < deadline, "preface stalled");
                std::thread::park_timeout(Duration::from_millis(1));
            }
            assert_eq!(c.usage().anonymous, 0);
            assert_eq!(c.usage().handshaking, 1);
            if cancel {
                assert!(c.cancel(ticket(1, 2, 1)));
            }
            let now = if cancel { 0 } else { 100 };
            let event = step(&mut c, now).pop().unwrap();
            assert_eq!(event.ticket, ticket(1, 2, 1));
            assert!(matches!(
                (cancel, event.result),
                (true, Err(ConnectError::Cancelled)) | (false, Err(ConnectError::Timeout))
            ));
            stream
                .set_read_timeout(Some(Duration::from_secs(5)))
                .unwrap();
            assert_eq!(stream.read(&mut [0]).unwrap(), 0);
            finish(c, now);
        }
    }
    #[test]
    fn cancellation_of_ready_unpolled_session_suppresses_transfer() {
        let mut left = make(1, &[2], 1);
        let mut right = make(2, &[1], 1);
        left.submit(
            req(
                ticket(1, 2, 1),
                ConnectDirection::Dial(right.listener_addr().unwrap().unwrap()),
            ),
            MonoTime(0),
        )
        .unwrap();
        right
            .submit(req(ticket(2, 1, 1), ConnectDirection::Accept), MonoTime(0))
            .unwrap();
        let deadline = Instant::now() + Duration::from_secs(5);
        let mut returned = None;
        while returned.is_none()
            || right.usage().handshaking != 0
            || right.next_deadline() != Some(MonoTime(0))
        {
            if returned.is_none() {
                if let Some(e) = step(&mut left, 0).pop() {
                    returned = Some(e.result.unwrap());
                }
            }
            assert!(right
                .poll(
                    MonoTime(0),
                    ConnectPollBudget {
                        completions: 0,
                        ..ConnectPollBudget::default()
                    }
                )
                .unwrap()
                .is_empty());
            assert!(Instant::now() < deadline, "TLS sessions never ready");
            thread::park_timeout(Duration::from_millis(1));
        }
        assert_eq!(right.usage().requests, 1);
        assert!(right.cancel(ticket(2, 1, 1)));
        let event = step(&mut right, 0).pop().unwrap();
        assert!(matches!(event.result, Err(ConnectError::Cancelled)));
        assert!(right.is_drained());
        // The already transferred left session remains caller-owned; the
        // canceled right session closes its stream instead of escaping.
        assert!(require_authenticated(returned.as_ref().unwrap()).is_ok());
        finish(left, 0);
        finish(right, 0);
    }
    #[test]
    fn rejected_construction_returns_live_caller_resources_for_cleanup() {
        let listener = TcpListener::bind("127.0.0.1:0").unwrap();
        let address = listener.local_addr().unwrap();
        let dialer = NativeTcpDialer::spawn(
            local(1),
            [(local(2).node, local(2).store.identity)].into(),
            DialLimits::default(),
            Arc::new(ThreadWake::current()),
        )
        .unwrap();
        let result = NativePeerConnector::new(
            NativeConnectConfig {
                local: local(1),
                limits: ConnectLimits {
                    requests: 0,
                    ..ConnectLimits::default()
                },
                session: SessionLimits::default(),
            },
            support::tls::configuration(1),
            [(local(2).node, support::tls::peer(local(2)))].into(),
            dialer,
            Some(listener),
            MonoTime(0),
        );
        let mut rejected = result.err().expect("invalid config accepted");
        assert_eq!(rejected.reason, ConnectError::InvalidLimits);
        assert_eq!(
            rejected.listener.as_ref().unwrap().local_addr().unwrap(),
            address
        );
        assert!(rejected.dialer.is_drained());
        rejected.dialer.close();
        let deadline = Instant::now() + Duration::from_secs(5);
        while !rejected.dialer.try_finish().unwrap() {
            assert!(Instant::now() < deadline);
            thread::yield_now();
        }
    }
    struct HeldDialer(Arc<Mutex<Control>>);
    impl PeerDialer for HeldDialer {
        type Endpoint = SocketAddr;
        type Channel = TcpStream;
        fn local(&self) -> LocalIdentity {
            local(1)
        }
        fn limits(&self) -> DialLimits {
            DialLimits::default()
        }
        fn outstanding(&self) -> usize {
            usize::from(self.0.lock().unwrap().pending.is_some())
        }
        fn submit(&mut self, r: DialRequest<SocketAddr>) -> Result<(), DialRejected<SocketAddr>> {
            self.0.lock().unwrap().pending = Some(r.connection);
            Ok(())
        }
        fn cancel(&mut self, t: ConnectTicket) -> bool {
            let mut c = self.0.lock().unwrap();
            if c.pending == Some(t) {
                c.cancelled = true;
                true
            } else {
                false
            }
        }
        fn poll(&mut self, limit: usize) -> Vec<DialCompletion<TcpStream>> {
            let mut c = self.0.lock().unwrap();
            if limit != 0 && c.release {
                let mis_scope = c.mis_scope;
                c.pending
                    .take()
                    .map(|mut connection| {
                        if mis_scope {
                            connection.generation =
                                SecureSessionGeneration::new(connection.generation.get() + 1)
                                    .unwrap();
                        }
                        DialCompletion {
                            connection,
                            result: Err(DialError::Cancelled),
                        }
                    })
                    .into_iter()
                    .collect()
            } else {
                vec![]
            }
        }
        fn close(&mut self) {
            self.0.lock().unwrap().cancelled = true;
        }
    }
    #[test]
    fn timeout_retains_dial_credits_until_host_provider_actual_terminal_receipt() {
        let control = Arc::new(Mutex::new(Control::default()));
        let mut c = NativePeerConnector::new(
            NativeConnectConfig {
                local: local(1),
                limits: ConnectLimits::default(),
                session: SessionLimits::default(),
            },
            support::tls::configuration(1),
            BTreeMap::from([(local(2).node, support::tls::peer(local(2)))]),
            HeldDialer(control.clone()),
            None,
            MonoTime(0),
        )
        .unwrap_or_else(|r| panic!("construct: {:?}", r.reason));
        c.submit(
            req(
                ticket(1, 2, 1),
                ConnectDirection::Dial("127.0.0.1:1".parse().unwrap()),
            ),
            MonoTime(0),
        )
        .unwrap();
        assert!(c
            .poll(MonoTime(100), ConnectPollBudget::default())
            .unwrap()
            .is_empty());
        assert!(control.lock().unwrap().cancelled);
        assert_eq!(c.usage().dialing, 1);
        assert_eq!(c.usage().requests, 1);
        assert_eq!(c.next_deadline(), None);
        c.close();
        assert!(!c.is_drained());
        assert!(c
            .poll(MonoTime(100), ConnectPollBudget::default())
            .unwrap()
            .is_empty());
        control.lock().unwrap().release = true;
        let event = c
            .poll(MonoTime(100), ConnectPollBudget::default())
            .unwrap()
            .pop()
            .unwrap();
        assert_eq!(event.ticket, ticket(1, 2, 1));
        assert!(matches!(event.result, Err(ConnectError::Timeout)));
        assert!(c.is_drained());
        assert!(c.into_dialer().is_ok());
    }
    #[test]
    fn mis_scoped_host_dial_receipt_stops_admission_without_releasing_real_ticket() {
        let control = Arc::new(Mutex::new(Control::default()));
        let mut c = NativePeerConnector::new(
            NativeConnectConfig {
                local: local(1),
                limits: ConnectLimits::default(),
                session: SessionLimits::default(),
            },
            support::tls::configuration(1),
            [(local(2).node, support::tls::peer(local(2)))].into(),
            HeldDialer(control.clone()),
            None,
            MonoTime(0),
        )
        .unwrap_or_else(|r| panic!("construct: {:?}", r.reason));
        c.submit(
            req(
                ticket(1, 2, 1),
                ConnectDirection::Dial("127.0.0.1:1".parse().unwrap()),
            ),
            MonoTime(0),
        )
        .unwrap();
        {
            let mut state = control.lock().unwrap();
            state.release = true;
            state.mis_scope = true;
        }
        assert!(matches!(
            c.poll(MonoTime(0), ConnectPollBudget::default()),
            Err(ConnectError::ProviderViolation)
        ));
        assert_eq!(
            c.usage().requests,
            1,
            "an alien receipt cannot terminate the actual ticket"
        );
        assert!(control.lock().unwrap().cancelled);
        assert_eq!(
            c.submit(
                req(
                    ticket(1, 2, 2),
                    ConnectDirection::Dial("127.0.0.1:1".parse().unwrap())
                ),
                MonoTime(0)
            )
            .unwrap_err()
            .reason,
            ConnectError::Closed
        );
        // This deliberately broken provider lost its real receipt. Observation
        // must be abandoned; no result can be passed to a roster as evidence.
        assert!(c.into_dialer().is_err());
    }

    #[test]
    fn connector_transfers_selected_versions_and_rejects_mismatch_without_ready() {
        for (av, bv) in [(2, 2), (3, 3), (1, 3), (3, 2)] {
            let mut a = versioned_make(1, &[2], 1, av);
            let mut b = versioned_make(2, &[1], 1, bv);
            let address = b.listener_addr().unwrap().unwrap();
            a.submit(
                req(ticket(1, 2, 1), ConnectDirection::Dial(address)),
                MonoTime(0),
            )
            .unwrap();
            b.submit(req(ticket(2, 1, 1), ConnectDirection::Accept), MonoTime(0))
                .unwrap();
            let deadline = Instant::now() + Duration::from_secs(5);
            let mut events = Vec::new();
            while events.len() < 2 {
                events.extend(step(&mut a, 0));
                events.extend(step(&mut b, 0));
                assert!(Instant::now() < deadline);
                thread::park_timeout(Duration::from_millis(1));
            }
            for event in events {
                if av == bv {
                    let session = event.result.unwrap();
                    assert_eq!(require_authenticated(&session).unwrap().wire_version, av);
                } else {
                    assert!(matches!(
                        event.result,
                        Err(ConnectError::Session(SessionError::IncompatibleProtocol))
                    ));
                }
            }
            assert!(a.is_drained() && b.is_drained());
            finish(a, 0);
            finish(b, 0);
        }
    }
    #[test]
    fn remote_discovery_drives_real_pinned_target_dial() {
        use voteboat::{
            discovery::*,
            native::{discovery::NativePeerDiscovery, remote_discovery::*},
        };
        let a = make(1, &[2], 1);
        let mut b = make(2, &[1], 1);
        let target = b.listener_addr().unwrap().unwrap();
        let (lookup, serve) = support::tls::pair(local(1), local(3), 1);
        let config = RemoteDiscoveryConfig::default();
        let mut source = NativePeerDiscovery::new(1, MonoTime(0)).unwrap();
        source
            .publish(
                PeerEndpointHint {
                    peer: ticket(1, 2, 1).peer,
                    generation: HintGeneration::new(1).unwrap(),
                    endpoint: target,
                    expires_at: MonoTime(1000),
                },
                MonoTime(0),
            )
            .unwrap();
        let mut server =
            NativeDiscoveryResponder::new(serve, ticket(3, 1, 1).peer, source, config, MonoTime(0))
                .ok()
                .unwrap();
        let remote =
            NativeRemotePeerDiscovery::new(lookup, ticket(1, 3, 1).peer, config, MonoTime(0))
                .ok()
                .unwrap();
        let mut a = DiscoveryConnector::new(a, remote, MonoTime(0))
            .ok()
            .unwrap();
        let stale = "127.0.0.1:1".parse().unwrap();
        let request = req(ticket(1, 2, 1), ConnectDirection::Dial(stale));
        let refused = a.submit(request, MonoTime(0)).unwrap_err();
        assert_eq!(
            refused.reason,
            ConnectError::Discovery(DiscoveryError::Unavailable)
        );
        assert!(matches!(refused.request.direction,ConnectDirection::Dial(v) if v==stale));
        let deadline = Instant::now() + Duration::from_secs(5);
        loop {
            server
                .poll(MonoTime(0), SessionPollBudget::default())
                .unwrap();
            if let Some(done) = a
                .discovery_mut()
                .poll(MonoTime(0), SessionPollBudget::default())
                .unwrap()
            {
                assert_eq!(done.result.unwrap().endpoint, target);
                break;
            }
            assert!(Instant::now() < deadline);
            thread::yield_now();
        }
        a.submit(*refused.request, MonoTime(0)).unwrap();
        b.submit(req(ticket(2, 1, 1), ConnectDirection::Accept), MonoTime(0))
            .unwrap();
        let mut sessions = Vec::new();
        while sessions.len() < 2 {
            for event in a
                .poll(MonoTime(0), ConnectPollBudget::default())
                .unwrap()
                .into_iter()
                .chain(step(&mut b, 0))
            {
                let session = event.result.unwrap();
                let binding = require_authenticated(&session).unwrap();
                assert_eq!(binding.peer.node, event.ticket.peer.node);
                assert_eq!(binding.peer.store.identity, event.ticket.peer.store);
                sessions.push(session);
            }
            assert!(Instant::now() < deadline);
            thread::park_timeout(Duration::from_millis(1));
        }
        a.close();
        let (a, remote) = a.into_parts().ok().unwrap();
        finish(a, 0);
        finish(b, 0);
        let _lookup = remote.into_session().ok().unwrap();
        server.close();
        assert!(sessions.iter().all(|s| require_authenticated(s).is_ok()));
    }
    #[test]
    fn native_discovery_invalidates_failed_hint_then_dials_refreshed_address_and_authenticates() {
        use voteboat::{discovery::*, native::discovery::NativePeerDiscovery};
        let a = make(1, &[2], 1);
        let mut b = make(2, &[1], 1);
        // This held listener accepts no TLS work. It is also the stale address
        // passed on the refreshed request, so success proves substitution.
        let stale = TcpListener::bind("127.0.0.1:0").unwrap();
        let stale_address = stale.local_addr().unwrap();
        let mut resolver = NativePeerDiscovery::new(1, MonoTime(0)).unwrap();
        let first = PeerEndpointHint {
            peer: ticket(1, 2, 1).peer,
            generation: HintGeneration::new(1).unwrap(),
            endpoint: stale_address,
            expires_at: MonoTime(1000),
        };
        resolver.publish(first, MonoTime(0)).unwrap();
        let mut a = DiscoveryConnector::new(a, resolver, MonoTime(0))
            .unwrap_or_else(|_| panic!("construct discovery"));
        a.submit(
            req(ticket(1, 2, 1), ConnectDirection::Dial(stale_address)),
            MonoTime(0),
        )
        .unwrap();
        let deadline = Instant::now() + Duration::from_secs(5);
        loop {
            let events = a.poll(MonoTime(100), ConnectPollBudget::default()).unwrap();
            if let Some(event) = events.into_iter().next() {
                assert_eq!(event.result.err(), Some(ConnectError::Timeout));
                break;
            }
            assert!(Instant::now() < deadline);
            thread::park_timeout(Duration::from_millis(1));
        }
        assert_eq!(
            a.discovery_mut().resolve(first.peer, MonoTime(100)),
            Err(DiscoveryError::Missing)
        );
        a.discovery_mut()
            .publish(
                PeerEndpointHint {
                    generation: HintGeneration::new(2).unwrap(),
                    endpoint: b.listener_addr().unwrap().unwrap(),
                    ..first
                },
                MonoTime(100),
            )
            .unwrap();
        a.submit(
            ConnectRequest {
                ticket: ticket(1, 2, 2),
                direction: ConnectDirection::Dial(stale_address),
                deadline: MonoTime(200),
            },
            MonoTime(100),
        )
        .unwrap();
        b.submit(
            ConnectRequest {
                ticket: ticket(2, 1, 1),
                direction: ConnectDirection::Accept,
                deadline: MonoTime(200),
            },
            MonoTime(100),
        )
        .unwrap();
        let mut sessions = Vec::new();
        while sessions.len() < 2 {
            for event in a
                .poll(MonoTime(100), ConnectPollBudget::default())
                .unwrap()
                .into_iter()
                .chain(step(&mut b, 100))
            {
                let session = event.result.unwrap();
                let binding = require_authenticated(&session).unwrap();
                assert_eq!(binding.local, event.ticket.local);
                assert_eq!(binding.peer.node, event.ticket.peer.node);
                assert_eq!(binding.peer.store.identity, event.ticket.peer.store);
                sessions.push(session);
            }
            assert!(Instant::now() < deadline);
            thread::park_timeout(Duration::from_millis(1));
        }
        a.close();
        assert!(a.is_drained());
        let (a, _) = a.into_parts().unwrap_or_else(|_| panic!("discovery drain"));
        finish(a, 100);
        finish(b, 100);
        assert!(sessions.iter().all(|s| require_authenticated(s).is_ok()));
    }
}
