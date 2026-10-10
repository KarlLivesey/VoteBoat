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
use std::cell::Cell;

struct Driver {
    pending: bool,
    ready: bool,
    closed: bool,
    polls: usize,
    hold: bool,
}
impl Driver {
    fn new() -> Self {
        Self {
            pending: false,
            ready: false,
            closed: false,
            polls: 0,
            hold: false,
        }
    }
}
impl PeerDiscovery for Driver {
    fn resolve(
        &mut self,
        p: PeerIdentity,
        _: MonoTime,
    ) -> Result<PeerEndpointHint, DiscoveryError> {
        if self.closed {
            return Err(DiscoveryError::Closed);
        }
        if self.ready {
            return Ok(hint(p.node.get(), 1));
        }
        self.pending = true;
        Err(DiscoveryError::Unavailable)
    }
    fn invalidate(&mut self, _: PeerIdentity, _: HintGeneration) -> bool {
        self.ready = false;
        true
    }
    fn close(&mut self) {
        self.closed = true;
    }
}
impl DiscoveryDriver for Driver {
    fn poll_discovery(
        &mut self,
        _: MonoTime,
        budget: SessionPollBudget,
    ) -> Result<(), DiscoveryError> {
        budget
            .validate()
            .map_err(|_| DiscoveryError::InvalidLimits)?;
        if budget.io_calls == 0 && !self.closed {
            return Ok(());
        }
        self.polls += 1;
        if !self.hold || self.closed {
            self.pending = false;
            self.ready = !self.closed;
        }
        Ok(())
    }
    fn discovery_pending(&self) -> bool {
        self.pending
    }
    fn discovery_deadline(&self) -> Option<MonoTime> {
        self.pending
            .then_some(MonoTime(if self.closed { 0 } else { 30 }))
    }
}
struct Connector {
    inner: HostConnector,
    visits: Rc<RefCell<Vec<usize>>>,
    fail_poll: Cell<bool>,
}
impl PeerConnector for Connector {
    type Endpoint = SocketAddr;
    type Session = Box<dyn SecureSession>;
    fn local(&self) -> LocalIdentity {
        self.inner.local()
    }
    fn supports_peer(&self, peer: PeerIdentity) -> bool {
        self.inner.supports_peer(peer)
    }
    fn limits(&self) -> ConnectLimits {
        self.inner.limits()
    }
    fn usage(&self) -> ConnectUsage {
        self.inner.usage()
    }
    fn next_deadline(&self) -> Option<MonoTime> {
        self.inner.next_deadline()
    }
    fn submit(
        &mut self,
        r: ConnectRequest<SocketAddr>,
        now: MonoTime,
    ) -> Result<(), ConnectRejected<SocketAddr>> {
        self.inner.submit(r, now)
    }
    fn cancel(&mut self, ticket: ConnectTicket) -> bool {
        self.inner.cancel(ticket)
    }
    fn close(&mut self) {
        self.inner.close();
    }
    fn poll(
        &mut self,
        now: MonoTime,
        mut b: ConnectPollBudget,
    ) -> Result<Vec<ConnectCompletion<Self::Session>>, ConnectError> {
        if self.fail_poll.replace(false) {
            return Err(ConnectError::Io(std::io::ErrorKind::Other));
        }
        self.visits.borrow_mut().push(b.visits);
        if b.visits == 0 {
            b.completions = 0;
        }
        self.inner.poll(now, b)
    }
}
fn connector(visits: &Rc<RefCell<Vec<usize>>>) -> Connector {
    Connector {
        inner: HostConnector(Rc::new(RefCell::new(Control::default()))),
        visits: visits.clone(),
        fail_poll: Cell::new(false),
    }
}

#[test]
fn driven_lookup_progresses_without_separate_poll_and_drains_on_close() {
    let visits = Rc::new(RefCell::new(Vec::new()));
    let mut c = DiscoveryConnector::new_driven(connector(&visits), Driver::new(), MonoTime(0))
        .unwrap_or_else(|_| panic!("construction"));
    c.submit(request(1), MonoTime(0)).unwrap();
    assert_eq!(c.usage().requests, 1);
    let rejected = c.submit(request(2), MonoTime(0)).unwrap_err();
    assert_eq!(rejected.reason, ConnectError::Overloaded);
    assert_eq!(rejected.request.ticket, request(2).ticket);
    assert_eq!(c.next_deadline(), Some(MonoTime(30)));
    c.poll(
        MonoTime(0),
        ConnectPollBudget {
            completions: 0,
            ..Default::default()
        },
    )
    .unwrap();
    assert_eq!(c.discovery_mut().polls, 1);
    assert_eq!(visits.borrow().as_slice(), &[15]);
    let done = c.poll(MonoTime(0), ConnectPollBudget::default()).unwrap();
    assert_eq!(done.len(), 1);
    assert_eq!(done[0].ticket, request(1).ticket);
    c.submit(*rejected.request, MonoTime(0)).unwrap();
    c.close();
    assert!(!c.is_drained());
    assert_eq!(c.next_deadline(), Some(MonoTime(0)));
    let done = c.poll(MonoTime(0), ConnectPollBudget::default()).unwrap();
    assert_eq!(done.len(), 1);
    assert_eq!(done[0].ticket, request(2).ticket);
    assert!(matches!(done[0].result, Err(ConnectError::Cancelled)));
    assert!(c.is_drained());
    let (_, driver) = c.into_parts().ok().unwrap();
    assert!(driver.closed && !driver.ready && !driver.pending);
}

#[test]
fn single_visit_alternates_and_zero_io_preserves_the_next_discovery_turn() {
    let visits = Rc::new(RefCell::new(Vec::new()));
    let mut driver = Driver::new();
    driver.hold = true;
    let mut c = DiscoveryConnector::new_driven(connector(&visits), driver, MonoTime(0))
        .unwrap_or_else(|_| panic!("construction"));
    c.submit(request(1), MonoTime(0)).unwrap();
    let budget = ConnectPollBudget {
        visits: 1,
        ..Default::default()
    };
    for _ in 0..4 {
        c.poll(
            MonoTime(0),
            ConnectPollBudget {
                session: SessionPollBudget {
                    io_calls: 0,
                    read_bytes: 0,
                    write_bytes: 0,
                },
                ..budget
            },
        )
        .unwrap();
        c.poll(MonoTime(0), budget).unwrap();
    }
    assert_eq!(c.discovery_mut().polls, 2);
    assert_eq!(visits.borrow().as_slice(), &[0, 0, 1, 1, 0, 0, 1, 1]);
    c.poll(
        MonoTime(0),
        ConnectPollBudget {
            visits: 0,
            ..budget
        },
    )
    .unwrap();
    c.poll(MonoTime(0), budget).unwrap();
    assert_eq!(c.discovery_mut().polls, 3);
}

#[test]
fn pending_construction_returns_owners_and_manual_mode_does_not_drive() {
    let visits = Rc::new(RefCell::new(Vec::new()));
    let mut driver = Driver::new();
    driver.pending = true;
    let (error, c, driver) =
        DiscoveryConnector::new_driven(connector(&visits), driver, MonoTime(0))
            .err()
            .unwrap();
    assert_eq!(error, ConnectError::InvalidRequest);
    assert!(Rc::ptr_eq(&visits, &c.visits));
    assert!(driver.pending && !driver.closed && driver.polls == 0);
    let mut manual = DiscoveryConnector::new(c, driver, MonoTime(0))
        .ok()
        .unwrap();
    manual
        .poll(MonoTime(0), ConnectPollBudget::default())
        .unwrap();
    assert_eq!(manual.discovery_mut().polls, 0);
    assert_eq!(manual.next_deadline(), None);
}

#[test]
fn waiting_cancel_and_deadline_keep_exact_terminal_ownership_across_poll_failure() {
    for cancelled in [false, true] {
        let visits = Rc::new(RefCell::new(Vec::new()));
        let mut driver = Driver::new();
        driver.hold = true;
        let mut c = DiscoveryConnector::new_driven(connector(&visits), driver, MonoTime(0))
            .ok()
            .unwrap();
        c.submit(request(1), MonoTime(0)).unwrap();
        assert!(!c.cancel(request(2).ticket));
        let now = if cancelled {
            assert!(c.cancel(request(1).ticket));
            MonoTime(1)
        } else {
            MonoTime(50)
        };
        assert!(c
            .poll(
                now,
                ConnectPollBudget {
                    completions: 0,
                    ..Default::default()
                }
            )
            .unwrap()
            .is_empty());
        assert_eq!(c.usage().requests, 1);
        c.connector().fail_poll.set(true);
        assert!(matches!(
            c.poll(now, ConnectPollBudget::default()),
            Err(ConnectError::Io(_))
        ));
        assert_eq!(c.usage().requests, 1);
        assert_eq!(
            c.submit(request(2), now).unwrap_err().reason,
            if cancelled {
                ConnectError::Overloaded
            } else {
                ConnectError::InvalidRequest
            }
        );
        let done = c.poll(now, ConnectPollBudget::default()).unwrap();
        assert_eq!(done.len(), 1);
        assert_eq!(done[0].ticket, request(1).ticket);
        assert!(
            matches!(&done[0].result, Err(e) if *e == if cancelled { ConnectError::Cancelled } else { ConnectError::Timeout })
        );
        assert!(c
            .poll(now, ConnectPollBudget::default())
            .unwrap()
            .is_empty());
        c.close();
        c.poll(now, ConnectPollBudget::default()).unwrap();
        assert!(c.is_drained());
    }
}
