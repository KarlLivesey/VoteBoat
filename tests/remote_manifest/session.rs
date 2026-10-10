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
use std::{cell::RefCell, collections::VecDeque, rc::Rc};
type Bytes = Rc<RefCell<VecDeque<u8>>>;
pub(super) struct Session {
    pub binding: SessionBinding,
    pub input: Bytes,
    pub output: Bytes,
    pub closed: bool,
}
pub(super) fn peer(id: u64) -> PeerIdentity {
    PeerIdentity {
        node: support::node(id),
        store: support::identity(id.into()),
    }
}
fn local(id: u64) -> LocalIdentity {
    LocalIdentity {
        node: peer(id).node,
        store: StoreBinding {
            identity: peer(id).store,
            session: StoreSession::new(1).unwrap(),
        },
    }
}
pub(super) fn pair(generation: u64) -> (Session, Session) {
    let a = Bytes::default();
    let b = Bytes::default();
    let binding = SessionBinding {
        local: local(1),
        peer: local(2),
        generation: SecureSessionGeneration::new(generation).unwrap(),
        wire_version: 1,
    };
    (
        Session {
            binding,
            input: a.clone(),
            output: b.clone(),
            closed: false,
        },
        Session {
            binding: SessionBinding {
                local: local(2),
                peer: local(1),
                ..binding
            },
            input: b,
            output: a,
            closed: false,
        },
    )
}
impl SecureSession for Session {
    fn security(&self) -> SessionSecurity {
        SessionSecurity::Authenticated
    }
    fn state(&self) -> SessionState {
        if self.closed {
            SessionState::Closed
        } else {
            SessionState::Ready
        }
    }
    fn binding(&self) -> Option<SessionBinding> {
        Some(self.binding)
    }
    fn limits(&self) -> SessionLimits {
        SessionLimits::default()
    }
    fn poll(&mut self, _: MonoTime, b: SessionPollBudget) -> Result<SessionProgress, SessionError> {
        b.validate()?;
        Ok(SessionProgress::default())
    }
    fn read_plaintext(&mut self, b: &mut [u8]) -> Result<usize, SessionError> {
        let mut input = self.input.borrow_mut();
        if input.is_empty() {
            return Err(SessionError::WouldBlock);
        }
        let count = input.len().min(b.len()).min(5);
        for item in &mut b[..count] {
            *item = input.pop_front().unwrap();
        }
        Ok(count)
    }
    fn write_plaintext(&mut self, b: &[u8]) -> Result<usize, SessionError> {
        let count = b.len().min(7);
        self.output.borrow_mut().extend(&b[..count]);
        Ok(count)
    }
    fn is_flushed(&self) -> bool {
        true
    }
    fn close(&mut self) {
        self.closed = true;
    }
    fn revoke(&mut self) {
        self.closed = true;
    }
}
