// SPDX-License-Identifier: RPL-1.5
// Copyright (c) 2026 Karl Livesey
use super::*;
use std::{
    cell::{Cell, RefCell},
    rc::Rc,
};

struct HostSession {
    binding: SessionBinding,
    revoked: Rc<Cell<usize>>,
}
impl SecureSession for HostSession {
    fn security(&self) -> SessionSecurity {
        SessionSecurity::Authenticated
    }
    fn state(&self) -> SessionState {
        SessionState::Ready
    }
    fn binding(&self) -> Option<SessionBinding> {
        Some(self.binding)
    }
    fn limits(&self) -> SessionLimits {
        SessionLimits::default()
    }
    fn poll(&mut self, _: MonoTime, _: SessionPollBudget) -> Result<SessionProgress, SessionError> {
        Ok(SessionProgress::default())
    }
    fn read_plaintext(&mut self, _: &mut [u8]) -> Result<usize, SessionError> {
        Err(SessionError::WouldBlock)
    }
    fn write_plaintext(&mut self, bytes: &[u8]) -> Result<usize, SessionError> {
        Ok(bytes.len())
    }
    fn is_flushed(&self) -> bool {
        true
    }
    fn close(&mut self) {}
    fn revoke(&mut self) {
        self.revoked.set(self.revoked.get() + 1);
    }
}
#[derive(Default)]
struct State {
    pending: Option<ConnectTicket>,
    release: bool,
    reject: bool,
    cancelled: usize,
    closed: bool,
    revoked: Rc<Cell<usize>>,
}
struct Host {
    state: Rc<RefCell<State>>,
    material: NativePeerMaterial,
}
impl NativePeerMaterialProvider for Host {
    fn replace_material(
        &mut self,
        material: NativePeerMaterial,
    ) -> Result<NativePeerMaterial, (ConnectError, NativePeerMaterial)> {
        if self.state.borrow().reject {
            return Err((ConnectError::InvalidRequest, material));
        }
        Ok(std::mem::replace(&mut self.material, material))
    }
}
impl PeerConnector for Host {
    type Endpoint = ();
    type Session = HostSession;
    fn local(&self) -> LocalIdentity {
        local(1)
    }
    fn limits(&self) -> ConnectLimits {
        ConnectLimits {
            requests: 1,
            ..ConnectLimits::default()
        }
    }
    fn usage(&self) -> ConnectUsage {
        ConnectUsage {
            requests: usize::from(self.state.borrow().pending.is_some()),
            ..ConnectUsage::default()
        }
    }
    fn next_deadline(&self) -> Option<MonoTime> {
        None
    }
    fn submit(
        &mut self,
        request: ConnectRequest<()>,
        _: MonoTime,
    ) -> Result<(), ConnectRejected<()>> {
        self.state.borrow_mut().pending = Some(request.ticket);
        Ok(())
    }
    fn cancel(&mut self, _: ConnectTicket) -> bool {
        // Models a successful session already queued when cancellation arrives.
        self.state.borrow_mut().cancelled += 1;
        false
    }
    fn poll(
        &mut self,
        _: MonoTime,
        budget: ConnectPollBudget,
    ) -> Result<Vec<ConnectCompletion<HostSession>>, ConnectError> {
        let mut state = self.state.borrow_mut();
        if !state.release || budget.completions == 0 {
            return Ok(Vec::new());
        }
        Ok(state
            .pending
            .take()
            .map(|ticket| ConnectCompletion {
                ticket,
                result: Ok(HostSession {
                    binding: SessionBinding {
                        local: ticket.local,
                        peer: LocalIdentity {
                            node: ticket.peer.node,
                            store: StoreBinding {
                                identity: ticket.peer.store,
                                session: StoreSession::new(42).unwrap(),
                            },
                        },
                        generation: ticket.generation,
                        wire_version: 1,
                    },
                    revoked: state.revoked.clone(),
                }),
            })
            .into_iter()
            .collect())
    }
    fn close(&mut self) {
        self.state.borrow_mut().closed = true;
    }
}
fn connector() -> (RotatingPeerConnector<Host>, Rc<RefCell<State>>) {
    let state = Rc::new(RefCell::new(State::default()));
    let host = Host {
        state: state.clone(),
        material: material(1, 2),
    };
    (
        RotatingPeerConnector::new(host, generation(1))
            .ok()
            .unwrap(),
        state,
    )
}
fn request(sequence: u64) -> ConnectRequest<()> {
    ConnectRequest {
        ticket: ConnectTicket {
            local: local(1),
            peer: support::tls::peer(local(2)).identity,
            generation: SecureSessionGeneration::new(sequence).unwrap(),
        },
        direction: ConnectDirection::Accept,
        deadline: MonoTime(100),
    }
}
#[test]
fn rotation_retains_old_ticket_until_terminal_and_revokes_late_success() {
    let (mut c, state) = connector();
    let ticket = request(1).ticket;
    c.submit(request(1), MonoTime(0)).unwrap();
    c.replace_peer_credentials(generation(1), generation(2), material(1, 3))
        .ok()
        .unwrap();
    assert_eq!(state.borrow().cancelled, 1);
    assert_eq!(c.usage().requests, 1);
    let mut other = request(2);
    other.ticket.peer.node = support::node(3);
    let pointer = other.ticket;
    let rejected = c.submit(other, MonoTime(0)).unwrap_err();
    assert_eq!(rejected.reason, ConnectError::Overloaded);
    assert_eq!(rejected.request.ticket, pointer);
    assert_eq!(
        c.submit(request(2), MonoTime(0)).unwrap_err().reason,
        ConnectError::Overloaded
    );
    state.borrow_mut().release = true;
    let events = c
        .poll(
            MonoTime(0),
            ConnectPollBudget {
                completions: 0,
                ..Default::default()
            },
        )
        .unwrap();
    assert!(events.is_empty());
    assert_eq!(c.usage().requests, 1);
    let events = c.poll(MonoTime(0), ConnectPollBudget::default()).unwrap();
    assert_eq!(events.len(), 1);
    assert_eq!(events[0].ticket, ticket);
    assert!(matches!(
        events[0].result,
        Err(ConnectError::Session(SessionError::Revoked))
    ));
    assert_eq!(state.borrow().revoked.get(), 1);
    assert!(c.is_drained());
    assert!(c
        .poll(MonoTime(0), ConnectPollBudget::default())
        .unwrap()
        .is_empty());
    c.close();
    assert!(c.into_inner().is_ok());
    assert!(state.borrow().closed);
}
#[test]
fn rejected_material_preserves_live_sessions_and_drop_revokes_them() {
    let (mut c, state) = connector();
    c.submit(request(1), MonoTime(0)).unwrap();
    state.borrow_mut().release = true;
    let mut session = c
        .poll(MonoTime(0), ConnectPollBudget::default())
        .unwrap()
        .pop()
        .unwrap()
        .result
        .ok()
        .unwrap();
    state.borrow_mut().reject = true;
    let input = material(1, 3);
    let pointer = input.peers[&support::node(2)].certificate.as_ptr();
    let (error, returned) = c
        .replace_peer_credentials(generation(1), generation(2), input)
        .err()
        .unwrap();
    assert_eq!(error, ConnectError::InvalidRequest);
    assert_eq!(
        returned.peers[&support::node(2)].certificate.as_ptr(),
        pointer
    );
    assert_eq!(c.credential_generation(), Some(generation(1)));
    assert_eq!(session.write_plaintext(b"live"), Ok(4));
    drop(c);
    assert_eq!(
        session.write_plaintext(b"revoked"),
        Err(SessionError::Revoked)
    );
}

#[test]
fn wrapping_active_provider_returns_original_owner_without_consuming_its_attempt() {
    let state = Rc::new(RefCell::new(State::default()));
    let mut original = Host {
        state: state.clone(),
        material: material(1, 2),
    };
    let ticket = request(1).ticket;
    original.submit(request(1), MonoTime(0)).unwrap();
    let returned = RotatingPeerConnector::new(original, generation(1))
        .err()
        .unwrap();
    assert!(Rc::ptr_eq(&state, &returned.state));
    assert_eq!(state.borrow().pending, Some(ticket));
    assert_eq!(state.borrow().cancelled, 0);
}
