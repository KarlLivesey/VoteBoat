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
    cell::{Cell, RefCell},
    rc::Rc,
};
#[derive(Clone)]
struct Validity(Rc<Cell<bool>>);
impl SessionValidity for Validity {
    fn validate(&self) -> Result<(), SessionError> {
        if self.0.get() {
            Ok(())
        } else {
            Err(SessionError::Revoked)
        }
    }
}
#[derive(Default)]
struct Calls {
    poll: usize,
    read: usize,
    write: usize,
    revoked: usize,
}
struct HostSession {
    binding: SessionBinding,
    state: SessionState,
    security: SessionSecurity,
    calls: Rc<RefCell<Calls>>,
    invalidate: Option<Rc<Cell<bool>>>,
}
fn session() -> (HostSession, Rc<RefCell<Calls>>) {
    let calls = Rc::new(RefCell::new(Calls::default()));
    (
        HostSession {
            binding: SessionBinding {
                local: local(1),
                peer: local(2),
                generation: SecureSessionGeneration::new(1).unwrap(),
                wire_version: 1,
            },
            state: SessionState::Ready,
            security: SessionSecurity::Authenticated,
            calls: calls.clone(),
            invalidate: None,
        },
        calls,
    )
}
impl HostSession {
    fn effect(&self) {
        if let Some(valid) = &self.invalidate {
            valid.set(false);
        }
    }
}
impl SecureSession for HostSession {
    fn security(&self) -> SessionSecurity {
        self.security
    }
    fn state(&self) -> SessionState {
        self.state
    }
    fn binding(&self) -> Option<SessionBinding> {
        Some(self.binding)
    }
    fn limits(&self) -> SessionLimits {
        SessionLimits::default()
    }
    fn poll(&mut self, _: MonoTime, _: SessionPollBudget) -> Result<SessionProgress, SessionError> {
        self.calls.borrow_mut().poll += 1;
        self.effect();
        Ok(SessionProgress::default())
    }
    fn read_plaintext(&mut self, b: &mut [u8]) -> Result<usize, SessionError> {
        self.calls.borrow_mut().read += 1;
        self.effect();
        b.fill(17);
        Ok(b.len())
    }
    fn write_plaintext(&mut self, b: &[u8]) -> Result<usize, SessionError> {
        self.calls.borrow_mut().write += 1;
        self.effect();
        Ok(b.len())
    }
    fn is_flushed(&self) -> bool {
        true
    }
    fn close(&mut self) {
        self.state = SessionState::Closed;
    }
    fn revoke(&mut self) {
        self.calls.borrow_mut().revoked += 1;
        self.state = SessionState::Failed;
    }
}
#[test]
fn downstream_validity_blocks_io_and_one_channel_close_preserves_others() {
    let valid = Validity(Rc::new(Cell::new(true)));
    let (a, counts) = session();
    let (b, _) = session();
    let validity: Box<dyn SessionValidity> = Box::new(valid.clone());
    let mut a = GuardedSession::new(a, validity).ok().unwrap();
    let mut b = GuardedSession::new(b, valid.clone()).ok().unwrap();
    b.close();
    assert_eq!(a.write_plaintext(&[1]), Ok(1));
    valid.0.set(false);
    assert_eq!(a.state(), SessionState::Failed);
    assert_eq!(a.binding(), None);
    assert_eq!(a.read_plaintext(&mut [0; 2]), Err(SessionError::Revoked));
    assert_eq!(
        a.poll(MonoTime(0), SessionPollBudget::default()),
        Err(SessionError::Revoked)
    );
    assert_eq!(a.write_plaintext(&[2]), Err(SessionError::Revoked));
    assert!(!a.is_flushed());
    let calls = counts.borrow();
    assert_eq!(
        (calls.poll, calls.read, calls.write, calls.revoked),
        (0, 0, 1, 1)
    );
}
#[test]
fn failed_construction_returns_unchanged_session_and_validity() {
    let valid = Validity(Rc::new(Cell::new(false)));
    let (a, counts) = session();
    let (e, a, returned) = GuardedSession::new(a, valid.clone()).err().unwrap();
    assert_eq!(e, SessionError::Revoked);
    assert!(Rc::ptr_eq(&valid.0, &returned.0));
    assert_eq!(a.state, SessionState::Ready);
    assert_eq!(counts.borrow().revoked, 0);
}
#[test]
fn invalidation_during_provider_io_suppresses_success_and_scrubs_read_output() {
    for action in 0..3 {
        let valid = Validity(Rc::new(Cell::new(true)));
        let (mut a, counts) = session();
        a.invalidate = Some(valid.0.clone());
        let mut a = GuardedSession::new(a, valid).ok().unwrap();
        let mut bytes = [0; 4];
        let error = match action {
            0 => a.poll(MonoTime(0), SessionPollBudget::default()).err(),
            1 => a.read_plaintext(&mut bytes).err(),
            _ => a.write_plaintext(&bytes).err(),
        };
        assert_eq!(error, Some(SessionError::Revoked));
        assert_eq!(bytes, [0; 4]);
        assert_eq!(counts.borrow().revoked, 1);
    }
}
#[test]
fn extracting_parts_always_revokes_before_returning_raw_session() {
    let valid = Validity(Rc::new(Cell::new(true)));
    let (a, counts) = session();
    let a = GuardedSession::new(a, valid).ok().unwrap();
    let (a, _) = a.into_revoked_parts();
    assert_eq!(a.state, SessionState::Failed);
    assert_eq!(counts.borrow().revoked, 1);
}

#[test]
fn simulator_or_unready_session_is_returned_before_guarding() {
    for insecure in [true, false] {
        let (mut a, counts) = session();
        if insecure {
            a.security = SessionSecurity::SimulatorOnly;
        } else {
            a.state = SessionState::Handshaking;
        }
        let expected = if insecure {
            SessionError::InsecureProvider
        } else {
            SessionError::NotReady
        };
        let (error, a, _) = GuardedSession::new(a, Validity(Rc::new(Cell::new(true))))
            .err()
            .unwrap();
        assert_eq!(error, expected);
        assert_eq!(counts.borrow().revoked, 0);
        assert_eq!(
            a.state,
            if insecure {
                SessionState::Ready
            } else {
                SessionState::Handshaking
            }
        );
    }
}
