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
//! Explicit authenticated host-provider double; real child provenance comes from fixture Nodes.
use std::{cell::RefCell, collections::VecDeque, rc::Rc};
use voteboat::{runtime::MonoTime, secure::*};
pub struct State {
    pub input: VecDeque<u8>,
    pub output: Vec<u8>,
    pub flushed: bool,
    pub eof: bool,
    pub revoked: bool,
    pub state: SessionState,
}
#[derive(Clone)]
pub struct Host {
    pub binding: SessionBinding,
    pub shared: Rc<RefCell<State>>,
}
impl Host {
    pub fn new(binding: SessionBinding) -> Self {
        Self {
            binding,
            shared: Rc::new(RefCell::new(State {
                input: VecDeque::new(),
                output: Vec::new(),
                flushed: false,
                eof: false,
                revoked: false,
                state: SessionState::Ready,
            })),
        }
    }
}
impl SecureSession for Host {
    fn security(&self) -> SessionSecurity {
        SessionSecurity::Authenticated
    }
    fn state(&self) -> SessionState {
        self.shared.borrow().state
    }
    fn binding(&self) -> Option<SessionBinding> {
        Some(self.binding)
    }
    fn limits(&self) -> SessionLimits {
        SessionLimits::default()
    }
    fn poll(
        &mut self,
        _now: MonoTime,
        _budget: SessionPollBudget,
    ) -> Result<SessionProgress, SessionError> {
        Ok(SessionProgress::default())
    }
    fn read_plaintext(&mut self, bytes: &mut [u8]) -> Result<usize, SessionError> {
        let mut s = self.shared.borrow_mut();
        if s.revoked {
            return Err(SessionError::Revoked);
        }
        if s.input.is_empty() {
            return if s.eof {
                Ok(0)
            } else {
                Err(SessionError::WouldBlock)
            };
        }
        let count = bytes.len().min(3).min(s.input.len());
        for byte in bytes.iter_mut().take(count) {
            *byte = s.input.pop_front().unwrap();
        }
        Ok(count)
    }
    fn write_plaintext(&mut self, bytes: &[u8]) -> Result<usize, SessionError> {
        let mut s = self.shared.borrow_mut();
        if s.revoked {
            return Err(SessionError::Revoked);
        }
        if s.state != SessionState::Ready {
            return Err(SessionError::Closed);
        }
        let count = bytes.len().min(5);
        s.output.extend_from_slice(&bytes[..count]);
        Ok(count)
    }
    fn is_flushed(&self) -> bool {
        self.shared.borrow().flushed
    }
    fn close(&mut self) {
        self.shared.borrow_mut().state = SessionState::Closed;
    }
    fn revoke(&mut self) {
        let mut s = self.shared.borrow_mut();
        s.revoked = true;
        s.state = SessionState::Failed;
    }
}
