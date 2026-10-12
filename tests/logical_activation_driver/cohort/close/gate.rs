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
//! Seven original plaintext bytes, then bounded waiting until real native closure.
use std::{cell::RefCell, rc::Rc};
use voteboat::{runtime::MonoTime, secure::*};
#[derive(Default, Debug)]
pub struct Observation {
    pub accepted: usize,
    pub offered: usize,
    pub prefix: [u8; 7],
    pub terminal_writes: usize,
}
pub type Observed = Rc<RefCell<Observation>>;
pub struct Gate {
    session: Box<dyn SecureSession>,
    observed: Observed,
}
impl Gate {
    pub fn wrap(session: Box<dyn SecureSession>, observed: Observed) -> Box<dyn SecureSession> {
        assert_eq!(session.security(), SessionSecurity::Authenticated);
        assert_eq!(session.state(), SessionState::Ready);
        assert!(session.binding().is_some());
        Box::new(Self { session, observed })
    }
}
impl SecureSession for Gate {
    fn security(&self) -> SessionSecurity {
        self.session.security()
    }
    fn state(&self) -> SessionState {
        self.session.state()
    }
    fn binding(&self) -> Option<SessionBinding> {
        self.session.binding()
    }
    fn limits(&self) -> SessionLimits {
        self.session.limits()
    }
    fn poll(
        &mut self,
        now: MonoTime,
        budget: SessionPollBudget,
    ) -> Result<SessionProgress, SessionError> {
        self.session.poll(now, budget)
    }
    fn read_plaintext(&mut self, bytes: &mut [u8]) -> Result<usize, SessionError> {
        self.session.read_plaintext(bytes)
    }
    fn write_plaintext(&mut self, bytes: &[u8]) -> Result<usize, SessionError> {
        if self.session.state() != SessionState::Ready {
            self.observed.borrow_mut().terminal_writes += 1;
            return self.session.write_plaintext(bytes);
        }
        let remaining = 7 - self.observed.borrow().accepted;
        if remaining == 0 {
            return Err(SessionError::WouldBlock);
        }
        let offered = bytes.len();
        let count = self
            .session
            .write_plaintext(&bytes[..remaining.min(offered)])?;
        let mut observed = self.observed.borrow_mut();
        if observed.offered == 0 {
            observed.offered = offered;
        }
        let offset = observed.accepted;
        observed.prefix[offset..offset + count].copy_from_slice(&bytes[..count]);
        observed.accepted += count;
        Ok(count)
    }
    fn is_flushed(&self) -> bool {
        self.session.is_flushed()
    }
    fn drain_status(&mut self) -> SessionDrainStatus {
        self.session.drain_status()
    }
    fn close(&mut self) {
        self.session.close();
    }
    fn revoke(&mut self) {
        self.session.revoke();
    }
}
