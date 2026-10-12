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
//! One explicit negative host-provider poll; successful operations stay native.
use std::{cell::Cell, io::ErrorKind, rc::Rc};
use voteboat::{runtime::MonoTime, secure::*};
#[derive(Default, Clone)]
pub struct Fault {
    pub armed: Rc<Cell<bool>>,
    pub injected: Rc<Cell<usize>>,
}
struct FaultSession {
    session: Box<dyn SecureSession>,
    fault: Fault,
}
pub fn wrap(session: Box<dyn SecureSession>, fault: Fault) -> Box<dyn SecureSession> {
    Box::new(FaultSession { session, fault })
}
impl SecureSession for FaultSession {
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
        if self.fault.armed.replace(false) {
            self.fault.injected.set(self.fault.injected.get() + 1);
            return Err(SessionError::Io(ErrorKind::Other));
        }
        self.session.poll(now, budget)
    }
    fn read_plaintext(&mut self, bytes: &mut [u8]) -> Result<usize, SessionError> {
        self.session.read_plaintext(bytes)
    }
    fn write_plaintext(&mut self, bytes: &[u8]) -> Result<usize, SessionError> {
        self.session.write_plaintext(bytes)
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
