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
use voteboat::{connect::*, identity::SecureSessionGeneration, transport::ConnectTicket};

#[derive(Default)]
struct Control {
    tickets: Vec<ConnectTicket>,
    cancelled: Vec<ConnectTicket>,
    reject: bool,
    corrupt_rejection: bool,
    hold: bool,
    poll_error: bool,
    wrong_ticket: bool,
    wrong_binding: u8,
    success: Option<HostSession>,
    closed: bool,
}
struct Connector {
    control: Rc<RefCell<Control>>,
    pending: Option<ConnectRequest<SocketAddr>>,
    owner: LocalIdentity,
    limits: ConnectLimits,
}
impl Connector {
    fn new() -> Self {
        Self {
            control: Rc::new(RefCell::new(Control::default())),
            pending: None,
            owner: local(1),
            limits: ConnectLimits {
                requests: 1,
                anonymous: 0,
                timeout_ms: 50,
            },
        }
    }
}
impl PeerConnector for Connector {
    type Endpoint = SocketAddr;
    type Session = HostSession;
    fn local(&self) -> LocalIdentity {
        self.owner
    }
    fn limits(&self) -> ConnectLimits {
        self.limits
    }
    fn usage(&self) -> ConnectUsage {
        ConnectUsage {
            requests: usize::from(self.pending.is_some()),
            ..Default::default()
        }
    }
    fn supports_peer(&self, p: PeerIdentity) -> bool {
        p == peer(2)
    }
    fn next_deadline(&self) -> Option<MonoTime> {
        self.pending.as_ref().map(|r| r.deadline)
    }
    fn submit(
        &mut self,
        mut r: ConnectRequest<SocketAddr>,
        _: MonoTime,
    ) -> Result<(), ConnectRejected<SocketAddr>> {
        let mut c = self.control.borrow_mut();
        c.tickets.push(r.ticket);
        if c.reject {
            if c.corrupt_rejection {
                r.ticket.peer = peer(9);
            }
            return Err(ConnectRejected {
                request: Box::new(r),
                reason: ConnectError::Overloaded,
            });
        }
        assert!(self.pending.replace(r).is_none());
        Ok(())
    }
    fn cancel(&mut self, t: ConnectTicket) -> bool {
        if self.pending.as_ref().is_some_and(|r| r.ticket == t) {
            self.control.borrow_mut().cancelled.push(t);
            true
        } else {
            false
        }
    }
    fn close(&mut self) {
        self.control.borrow_mut().closed = true;
        if let Some(r) = &self.pending {
            self.control.borrow_mut().cancelled.push(r.ticket);
        }
    }
    fn poll(
        &mut self,
        _: MonoTime,
        b: ConnectPollBudget,
    ) -> Result<Vec<ConnectCompletion<HostSession>>, ConnectError> {
        b.validate()?;
        let mut c = self.control.borrow_mut();
        if std::mem::take(&mut c.poll_error) {
            return Err(ConnectError::Io(std::io::ErrorKind::Interrupted));
        }
        if c.hold || b.completions == 0 {
            return Ok(vec![]);
        }
        let Some(r) = self.pending.take() else {
            return Ok(vec![]);
        };
        let mut t = r.ticket;
        if c.wrong_ticket {
            t.peer = peer(9);
        }
        let result = c
            .success
            .take()
            .map_or(Err(ConnectError::Timeout), |mut s| {
                match c.wrong_binding {
                    1 => s.binding.local = local(9),
                    2 => s.binding.peer = local(9),
                    3 => s.binding.generation = SecureSessionGeneration::new(9).unwrap(),
                    4 => s.binding.wire_version = 9,
                    5 => s.closed = true,
                    _ => (),
                }
                Ok(s)
            });
        Ok(vec![ConnectCompletion { ticket: t, result }])
    }
}
type Driver = ReconnectingPeerDiscovery<Connector>;
fn config() -> SourceReconnectConfig {
    SourceReconnectConfig {
        endpoint: "127.0.0.1:4567".parse().unwrap(),
        first_generation: SecureSessionGeneration::new(2).unwrap(),
        last_generation: SecureSessionGeneration::new(3).unwrap(),
        retry_ms: 10,
    }
}
fn failed() -> (NativeRemotePeerDiscovery<HostSession>, MonoTime) {
    let (mut client, _) = client_server();
    client.resolve(peer(3), MonoTime(0)).unwrap_err();
    let now = client.next_deadline().unwrap();
    client.poll(now, SessionPollBudget::default()).unwrap();
    assert!(client.source_failed());
    (client, now)
}
fn fixture() -> (Driver, Rc<RefCell<Control>>, MonoTime) {
    let (client, now) = failed();
    let connector = Connector::new();
    let c = connector.control.clone();
    (
        Driver::new(client, connector, config(), now).ok().unwrap(),
        c,
        now,
    )
}
fn poll(d: &mut Driver, now: MonoTime) {
    d.poll_discovery(now, SessionPollBudget::default()).unwrap();
}

#[test]
fn retry_consumes_only_reserved_generations_and_stops_at_exhaustion() {
    let (mut d, c, now) = fixture();
    c.borrow_mut().reject = true;
    poll(&mut d, now);
    assert_eq!(
        d.status(),
        SourceReconnectStatus::Backoff {
            retry_at: MonoTime(now.0 + 10)
        }
    );
    poll(&mut d, MonoTime(now.0 + 9));
    assert_eq!(c.borrow().tickets.len(), 1);
    poll(&mut d, MonoTime(now.0 + 10));
    assert_eq!(
        c.borrow()
            .tickets
            .iter()
            .map(|t| t.generation.get())
            .collect::<Vec<_>>(),
        [2, 3]
    );
    assert_eq!(d.status(), SourceReconnectStatus::Exhausted);
    assert!(!d.discovery_pending());
    assert_eq!(d.discovery_deadline(), None);
    poll(&mut d, MonoTime(now.0 + 100));
    assert_eq!(c.borrow().tickets.len(), 2);
}
#[test]
fn deadline_and_poll_error_retain_original_ticket_until_actual_receipt() {
    let (mut d, c, now) = fixture();
    poll(&mut d, now);
    let t = c.borrow().tickets[0];
    c.borrow_mut().hold = true;
    c.borrow_mut().poll_error = true;
    poll(&mut d, MonoTime(now.0 + 50));
    assert_eq!(c.borrow().cancelled, [t]);
    assert!(matches!(d.status(), SourceReconnectStatus::Connecting { ticket, .. } if ticket == t));
    poll(&mut d, MonoTime(now.0 + 60));
    assert_eq!(c.borrow().tickets, [t]);
    c.borrow_mut().hold = false;
    poll(&mut d, MonoTime(now.0 + 60));
    assert_eq!(
        d.status(),
        SourceReconnectStatus::Backoff {
            retry_at: MonoTime(now.0 + 70)
        }
    );
    poll(&mut d, MonoTime(now.0 + 70));
    assert_eq!(c.borrow().tickets[1].generation.get(), 3);
}
#[test]
fn close_drains_accepted_ticket_without_publishing_late_success() {
    let (mut d, c, now) = fixture();
    let (mut a, _) = pair();
    a.binding.generation = config().first_generation;
    c.borrow_mut().success = Some(a);
    poll(&mut d, now);
    c.borrow_mut().hold = true;
    d.close();
    assert!(!d.is_drained());
    assert_eq!(d.status(), SourceReconnectStatus::Closed);
    poll(&mut d, now);
    assert!(!d.is_drained());
    c.borrow_mut().hold = false;
    poll(&mut d, now);
    assert!(d.is_drained());
    assert!(!d.discovery_pending());
    let (r, connector) = d.into_parts().ok().unwrap();
    assert!(r.into_optional_session().ok().unwrap().is_none());
    assert!(connector.control.borrow().closed);
}
#[test]
fn wrong_completion_ticket_or_session_binding_fences_the_source() {
    for mismatch in 0..6 {
        let (mut d, c, now) = fixture();
        let (mut a, _) = pair();
        a.binding.generation = config().first_generation;
        c.borrow_mut().success = Some(a);
        c.borrow_mut().wrong_ticket = mismatch == 0;
        c.borrow_mut().wrong_binding = mismatch;
        poll(&mut d, now);
        assert_eq!(
            d.poll_discovery(now, SessionPollBudget::default()),
            Err(DiscoveryError::WrongBinding)
        );
        assert_eq!(
            d.last_connect_error(),
            Some(ConnectError::ProviderViolation)
        );
        assert_eq!(d.status(), SourceReconnectStatus::Exhausted);
        assert!(c.borrow().closed);
    }
}
#[test]
fn malformed_rejection_is_not_retried() {
    let (mut d, c, now) = fixture();
    c.borrow_mut().reject = true;
    c.borrow_mut().corrupt_rejection = true;
    assert_eq!(
        d.poll_discovery(now, SessionPollBudget::default()),
        Err(DiscoveryError::WrongBinding)
    );
    assert_eq!(c.borrow().tickets.len(), 1);
    assert_eq!(
        d.poll_discovery(now, SessionPollBudget::default()),
        Err(DiscoveryError::WrongBinding)
    );
}
#[test]
fn invalid_time_and_zero_budget_do_not_submit_or_change_pending_work() {
    let (mut d, c, now) = fixture();
    assert_eq!(
        d.poll_discovery(MonoTime(now.0 - 1), SessionPollBudget::default()),
        Err(DiscoveryError::TimeWentBack)
    );
    d.poll_discovery(
        now,
        SessionPollBudget {
            io_calls: 0,
            read_bytes: 0,
            write_bytes: 0,
        },
    )
    .unwrap();
    assert!(c.borrow().tickets.is_empty());
    poll(&mut d, now);
    let status = d.status();
    assert_eq!(
        d.poll_discovery(
            now,
            SessionPollBudget {
                io_calls: usize::MAX,
                ..Default::default()
            }
        ),
        Err(DiscoveryError::InvalidLimits)
    );
    assert_eq!(d.status(), status);
}
#[test]
fn constructor_returns_original_owners_for_invalid_configuration_and_binding() {
    for case in 0..8 {
        let (mut client, now) = failed();
        let mut c = Connector::new();
        let original = c.control.clone();
        let mut cfg = config();
        match case {
            0 => cfg.first_generation = SecureSessionGeneration::new(1).unwrap(),
            1 => cfg.last_generation = SecureSessionGeneration::new(1).unwrap(),
            2 => cfg.retry_ms = 0,
            3 => cfg.endpoint = "0.0.0.0:4567".parse().unwrap(),
            4 => c.owner = local(9),
            5 => c.limits.requests = 0,
            6 => client.close(),
            _ => cfg.endpoint.set_port(0),
        }
        let rejected = Driver::new(client, c, cfg, now).err().unwrap();
        let (client, c) = (rejected.remote, rejected.connector);
        assert!(Rc::ptr_eq(&original, &c.control));
        assert!(!original.borrow().closed);
        assert!(client.source_failed());
    }
}

#[test]
fn constructor_rejection_preserves_pending_owners_and_monotonic_time() {
    let (mut remote, _) = client_server();
    remote.resolve(peer(3), MonoTime(1)).unwrap_err();
    let original = remote.pending().unwrap();
    let rejected = Driver::new(remote, Connector::new(), config(), MonoTime(1))
        .err()
        .unwrap();
    assert_eq!(rejected.reason, DiscoveryError::Unavailable);
    assert_eq!(rejected.remote.pending(), Some(original));
    assert!(!rejected.connector.control.borrow().closed);
    let (remote, now) = failed();
    let mut connector = Connector::new();
    let request = ConnectRequest {
        ticket: ConnectTicket {
            local: local(1),
            peer: peer(2),
            generation: config().first_generation,
        },
        direction: ConnectDirection::Dial(config().endpoint),
        deadline: MonoTime(now.0 + 50),
    };
    let original = request.ticket;
    connector.submit(request, now).unwrap();
    let rejected = Driver::new(remote, connector, config(), now).err().unwrap();
    assert_eq!(rejected.reason, DiscoveryError::Unavailable);
    assert_eq!(
        rejected.connector.pending.as_ref().unwrap().ticket,
        original
    );
    let (remote, now) = failed();
    let rejected = Driver::new(remote, Connector::new(), config(), MonoTime(now.0 - 1))
        .err()
        .unwrap();
    assert_eq!(rejected.reason, DiscoveryError::TimeWentBack);
    assert!(!rejected.connector.control.borrow().closed);
}

#[test]
fn owning_discovery_connector_reconnects_source_without_manual_resolver_polling() {
    let (initial, _) = pair();
    let remote = NativeRemotePeerDiscovery::new(
        initial,
        peer(2),
        RemoteDiscoveryConfig {
            timeout_ms: 1000,
            retry_ms: 1,
            ..Default::default()
        },
        MonoTime(0),
    )
    .ok()
    .unwrap();
    let source_connector = Connector::new();
    let source_control = source_connector.control.clone();
    let (mut source_client, mut source_server) = pair();
    source_client.binding.generation = config().first_generation;
    source_server.binding.generation = config().first_generation;
    source_control.borrow_mut().success = Some(source_client);
    let resolver = Driver::new(remote, source_connector, config(), MonoTime(0))
        .ok()
        .unwrap();
    let mut target = Connector::new();
    target.limits.timeout_ms = 10_000;
    let target_control = target.control.clone();
    let (mut session, _) = pair();
    let generation = SecureSessionGeneration::new(8).unwrap();
    session.binding.generation = generation;
    target_control.borrow_mut().success = Some(session);
    let mut owned = DiscoveryConnector::new_driven(target, resolver, MonoTime(0))
        .ok()
        .unwrap();
    let ticket = ConnectTicket {
        local: local(1),
        peer: peer(2),
        generation,
    };
    owned
        .submit(
            ConnectRequest {
                ticket,
                direction: ConnectDirection::Dial(config().endpoint),
                deadline: MonoTime(10_000),
            },
            MonoTime(0),
        )
        .unwrap();
    let mut server = NativeDiscoveryResponder::new(
        source_server,
        peer(1),
        HostHints {
            hint: Some(hint(2, 1, 30_000)),
            calls: 0,
            closed: false,
        },
        RemoteDiscoveryConfig::default(),
        MonoTime(0),
    )
    .ok()
    .unwrap();
    let budget = ConnectPollBudget {
        visits: 1,
        ..Default::default()
    };
    let mut completed = false;
    for elapsed in 0..2000 {
        let now = MonoTime(elapsed);
        server.poll(now, SessionPollBudget::default()).unwrap();
        for done in owned.poll(now, budget).unwrap() {
            assert_eq!(done.ticket, ticket);
            assert_eq!(
                require_authenticated(&done.result.unwrap())
                    .unwrap()
                    .generation,
                generation
            );
            completed = true;
        }
        if completed {
            break;
        }
    }
    assert!(
        completed,
        "source repair did not unblock the original target request"
    );
    assert_eq!(source_control.borrow().tickets.len(), 1);
    assert_eq!(target_control.borrow().tickets, [ticket]);
    owned.close();
    assert!(owned.is_drained());
    assert!(owned.into_parts().is_ok());
}
