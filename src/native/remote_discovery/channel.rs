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
use super::{
    codec::{Frame, LENGTH},
    RemoteDiscoveryError as Error,
};
use crate::{runtime::MonoTime, secure::*};
/// One fixed input and output; each call moves at most one prefix in each direction.
pub(super) struct Channel<S> {
    session: Option<S>,
    pub binding: SessionBinding,
    input: [u8; LENGTH],
    read: usize,
    output: Option<[u8; LENGTH]>,
    written: usize,
}
impl<S: SecureSession> Channel<S> {
    pub fn new(session: S, expected: PeerIdentity) -> Result<Self, (Error, S)> {
        let binding = match require_authenticated(&session) {
            Ok(binding) => binding,
            Err(e) => return Err((e.into(), session)),
        };
        if binding.peer.node != expected.node || binding.peer.store.identity != expected.store {
            return Err((
                crate::discovery::DiscoveryError::WrongBinding.into(),
                session,
            ));
        }
        Ok(Self {
            session: Some(session),
            binding,
            input: [0; LENGTH],
            read: 0,
            output: None,
            written: 0,
        })
    }
    pub fn queue(&mut self, frame: Frame) -> Result<(), Error> {
        if self.output.is_some() {
            return Err(Error::Protocol);
        }
        self.output = Some(frame.encode());
        self.written = 0;
        Ok(())
    }
    pub fn pending_output(&self) -> bool {
        self.output.is_some()
    }
    pub fn poll(
        &mut self,
        now: MonoTime,
        budget: SessionPollBudget,
    ) -> Result<Option<Frame>, Error> {
        budget.validate()?;
        if budget.io_calls == 0 {
            return Ok(None);
        }
        let session = self.session.as_mut().ok_or(SessionError::Closed)?;
        session.poll(now, budget)?;
        if require_authenticated(session)? != self.binding {
            return Err(Error::Protocol);
        }
        self.write(budget.write_bytes)?;
        if budget.read_bytes == 0 {
            return Ok(None);
        }
        let limit = (LENGTH - self.read).min(budget.read_bytes);
        match self
            .session
            .as_mut()
            .ok_or(SessionError::Closed)?
            .read_plaintext(&mut self.input[self.read..self.read + limit])
        {
            Ok(0) => return Err(SessionError::Closed.into()),
            Ok(n) if n <= limit => self.read += n,
            Ok(_) => return Err(Error::Protocol),
            Err(SessionError::WouldBlock) => (),
            Err(e) => return Err(e.into()),
        }
        if self.read < LENGTH {
            return Ok(None);
        }
        self.read = 0;
        Frame::decode(self.input).map(Some)
    }
    fn write(&mut self, limit: usize) -> Result<(), Error> {
        if limit == 0 {
            return Ok(());
        }
        let Some(output) = &self.output else {
            return Ok(());
        };
        let count = (LENGTH - self.written).min(limit);
        match self
            .session
            .as_mut()
            .ok_or(SessionError::Closed)?
            .write_plaintext(&output[self.written..self.written + count])
        {
            Ok(n) if n <= count => self.written += n,
            Ok(_) => return Err(Error::Protocol),
            Err(SessionError::WouldBlock) => (),
            Err(e) => return Err(e.into()),
        }
        if self.written == LENGTH {
            self.output = None;
        }
        Ok(())
    }
    pub fn close(&mut self) {
        if let Some(session) = &mut self.session {
            session.close();
        }
    }
    pub fn has_session(&self) -> bool {
        self.session.is_some()
    }
    pub fn take_session(&mut self) -> Option<S> {
        self.session.take()
    }
    pub fn into_session(self) -> S {
        self.session.expect("attached channel")
    }
}
