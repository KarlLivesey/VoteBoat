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
    codec::{Frame, HEADER, MAX_FRAME},
    RemoteManifestError as Error,
};
use crate::{routing::ManifestDiscoveryError, runtime::MonoTime, secure::*};
/// Two bounded frame buffers. Header length is checked before accepting a body.
pub(super) struct Channel<S> {
    session: S,
    pub binding: SessionBinding,
    input: Vec<u8>,
    read: usize,
    target: usize,
    output: Option<Vec<u8>>,
    written: usize,
}
impl<S: SecureSession> Channel<S> {
    pub fn new(session: S, expected: PeerIdentity) -> Result<Self, (Error, S)> {
        let binding = match require_authenticated(&session) {
            Ok(binding) => binding,
            Err(e) => return Err((e.into(), session)),
        };
        if binding.peer.node != expected.node || binding.peer.store.identity != expected.store {
            return Err((ManifestDiscoveryError::WrongIdentity.into(), session));
        }
        Ok(Self {
            session,
            binding,
            input: vec![0; MAX_FRAME],
            read: 0,
            target: HEADER,
            output: None,
            written: 0,
        })
    }
    pub fn queue(&mut self, frame: Frame) -> Result<(), Error> {
        if self.output.is_some() {
            return Err(Error::Protocol);
        }
        self.output = Some(frame.encode()?);
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
        self.session.poll(now, budget)?;
        if require_authenticated(&self.session)? != self.binding {
            return Err(Error::Protocol);
        }
        self.write(budget.write_bytes)?;
        if budget.read_bytes == 0 {
            return Ok(None);
        }
        let limit = (self.target - self.read).min(budget.read_bytes);
        match self
            .session
            .read_plaintext(&mut self.input[self.read..self.read + limit])
        {
            Ok(0) => return Err(SessionError::Closed.into()),
            Ok(n) if n <= limit => self.read += n,
            Ok(_) => return Err(Error::Protocol),
            Err(SessionError::WouldBlock) => (),
            Err(e) => return Err(e.into()),
        }
        if self.read < self.target {
            return Ok(None);
        }
        if self.target == HEADER {
            self.target = Frame::length(&self.input[..HEADER])?;
            if self.read < self.target {
                return Ok(None);
            }
        }
        let frame = Frame::decode(&self.input[..self.target])?;
        self.read = 0;
        self.target = HEADER;
        Ok(Some(frame))
    }
    fn write(&mut self, limit: usize) -> Result<(), Error> {
        if limit == 0 {
            return Ok(());
        }
        let Some(output) = &self.output else {
            return Ok(());
        };
        let count = (output.len() - self.written).min(limit);
        match self
            .session
            .write_plaintext(&output[self.written..self.written + count])
        {
            Ok(n) if n <= count => self.written += n,
            Ok(_) => return Err(Error::Protocol),
            Err(SessionError::WouldBlock) => (),
            Err(e) => return Err(e.into()),
        }
        if self.written == output.len() {
            self.output = None;
        }
        Ok(())
    }
    pub fn close(&mut self) {
        self.session.close();
    }
    pub fn into_session(self) -> S {
        self.session
    }
}
