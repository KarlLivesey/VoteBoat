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
//! Bounded peer frame driver over the public authenticated channel and codec.
use crate::{outbound::*, runtime::MonoTime, secure::*, transport::*, wire::*};
use std::mem::size_of;

struct Sending {
    batch: OutboundBatch,
    frame: Vec<u8>,
    written: usize,
}
pub struct NativePeerTransport<S: SecureSession, C: WireCodec> {
    session: Option<S>,
    codec: C,
    binding: SessionBinding,
    outbound: OutboundBinding,
    outbound_limits: OutboundLimits,
    limits: TransportLimits,
    state: TransportState,
    failure: Option<TransportError>,
    sending: Option<Sending>,
    completed: Option<TransportSend>,
    incoming: Vec<u8>,
    read: usize,
    frame_length: Option<usize>,
    received: Option<ReceivedBatch>,
    decoded_bytes: usize,
    prefer_read: bool,
    close_started: bool,
}
impl<S: SecureSession, C: WireCodec> NativePeerTransport<S, C> {
    pub fn new(
        session: S,
        codec: C,
        outbound_queue: &(impl OutboundQueue + ?Sized),
        limits: TransportLimits,
    ) -> Result<Self, TransportError> {
        let limits = limits.validate()?;
        let outbound = outbound_queue.binding();
        let outbound_limits = outbound_queue
            .limits()
            .validate()
            .map_err(TransportError::Outbound)?;
        let binding = require_authenticated(&session).map_err(TransportError::Session)?;
        if binding.local.node != outbound.node
            || binding.local.store != outbound.store
            || binding.local.node == binding.peer.node
        {
            return Err(TransportError::WrongBinding);
        }
        let wire = codec.limits().validate().map_err(TransportError::Wire)?;
        if codec.format_version() != binding.wire_version
            || codec.header_bytes() == 0
            || codec.header_bytes() > wire.max_frame_bytes
            || wire.max_frame_bytes > limits.send_frame_bytes
            || wire.max_frame_bytes > limits.receive_frame_bytes
            || wire.max_decoded_bytes > limits.decoded_bytes
        {
            return Err(TransportError::IncompatibleCodec);
        }
        Ok(Self {
            session: Some(session),
            codec,
            binding,
            outbound,
            outbound_limits,
            limits,
            state: TransportState::Open,
            failure: None,
            sending: None,
            completed: None,
            incoming: Vec::new(),
            read: 0,
            frame_length: None,
            received: None,
            decoded_bytes: 0,
            prefer_read: true,
            close_started: false,
        })
    }
    fn validate_session(&self) -> Result<(), TransportError> {
        let session = self.session.as_ref().ok_or(TransportError::Closed)?;
        if session.security() != SessionSecurity::Authenticated
            || session.binding() != Some(self.binding)
        {
            return Err(TransportError::WrongBinding);
        }
        Ok(())
    }
    fn clear_partial(&mut self) {
        self.incoming = Vec::new();
        self.read = 0;
        self.frame_length = None;
    }
    fn fail<T>(&mut self, error: TransportError) -> Result<T, TransportError> {
        // Drop the channel before releasing any completion: it may still own
        // ciphertext buffers and cannot continue external writes after failure.
        self.session.take();
        self.clear_partial();
        if let Some(send) = self.sending.take() {
            self.completed = Some(TransportSend {
                connection: self.binding,
                batch: send.batch,
                result: LocalSendResult::Failed,
            });
        }
        self.state = TransportState::Failed;
        self.failure = Some(error);
        Err(error)
    }
    fn finish_send(&mut self) -> bool {
        if self
            .sending
            .as_ref()
            .is_some_and(|s| s.written == s.frame.len())
            && self.session.as_ref().is_some_and(SecureSession::is_flushed)
        {
            let send = self.sending.take().unwrap();
            self.completed = Some(TransportSend {
                connection: self.binding,
                batch: send.batch,
                result: LocalSendResult::Sent,
            });
            return true;
        }
        false
    }
    fn read_step(&mut self, limit: usize) -> Result<usize, TransportError> {
        if self.incoming.is_empty() {
            self.incoming = vec![0; self.codec.header_bytes()];
        }
        let end = self.incoming.len().min(self.read + limit);
        let n = self
            .session
            .as_mut()
            .unwrap()
            .read_plaintext(&mut self.incoming[self.read..end])
            .map_err(TransportError::Session)?;
        if n > end - self.read {
            return Err(TransportError::ProviderViolation);
        }
        if n == 0 {
            if self.read > 0 {
                return Err(TransportError::Truncated);
            }
            if self.sending.is_some() {
                return Err(TransportError::Session(SessionError::Closed));
            }
            self.session.take();
            self.clear_partial();
            self.state = TransportState::Closed;
            return Ok(0);
        }
        self.read += n;
        if self.frame_length.is_none() && self.read == self.incoming.len() {
            let length = self
                .codec
                .frame_length(&self.incoming)
                .map_err(TransportError::Wire)?;
            if length < self.incoming.len()
                || length > self.limits.receive_frame_bytes
                || length > self.codec.limits().max_frame_bytes
            {
                return Err(TransportError::Wire(WireError::TooLarge));
            }
            self.incoming.reserve_exact(length - self.incoming.len());
            self.incoming.resize(length, 0);
            self.frame_length = Some(length);
        }
        if self.frame_length == Some(self.read) {
            let messages = self
                .codec
                .decode_batch(self.binding.incoming(), &self.incoming)
                .map_err(TransportError::Wire)?;
            if messages.is_empty()
                || messages.len() > self.codec.limits().max_messages
                || messages.iter().any(|m| !self.binding.incoming().matches(m))
            {
                return Err(TransportError::ProviderViolation);
            }
            let mut cost = messages
                .capacity()
                .checked_mul(size_of::<crate::raft::Message>())
                .ok_or(TransportError::ProviderViolation)?;
            for message in &messages {
                let (_, bytes) = message_cost(message, self.limits.decoded_bytes)
                    .map_err(|_| TransportError::ProviderViolation)?;
                cost = cost
                    .checked_add(bytes - size_of::<crate::raft::Message>())
                    .filter(|c| *c <= self.limits.decoded_bytes)
                    .ok_or(TransportError::ProviderViolation)?;
            }
            self.decoded_bytes = cost;
            self.received = Some(ReceivedBatch {
                connection: self.binding,
                messages,
            });
            self.clear_partial();
        }
        Ok(n)
    }
    fn write_step(&mut self, limit: usize) -> Result<usize, TransportError> {
        let send = self.sending.as_mut().unwrap();
        let end = send.frame.len().min(send.written + limit);
        let n = self
            .session
            .as_mut()
            .unwrap()
            .write_plaintext(&send.frame[send.written..end])
            .map_err(TransportError::Session)?;
        if n == 0 || n > end - send.written {
            return Err(TransportError::ProviderViolation);
        }
        send.written += n;
        Ok(n)
    }
    fn start_close(&mut self) {
        if self.state == TransportState::Draining && self.sending.is_none() && !self.close_started {
            self.clear_partial();
            self.session.as_mut().unwrap().close();
            self.close_started = true;
        }
    }
}
impl<S: SecureSession, C: WireCodec> PeerTransport for NativePeerTransport<S, C> {
    fn security(&self) -> SessionSecurity {
        SessionSecurity::Authenticated
    }
    fn binding(&self) -> SessionBinding {
        self.binding
    }
    fn state(&self) -> TransportState {
        self.state
    }
    fn limits(&self) -> TransportLimits {
        self.limits
    }
    fn usage(&self) -> TransportUsage {
        TransportUsage {
            send_frame_bytes: self.sending.as_ref().map_or(0, |s| s.frame.capacity()),
            receive_frame_bytes: self.incoming.capacity(),
            decoded_bytes: self.decoded_bytes,
            sending: self.sending.is_some(),
            completion: self.completed.is_some(),
        }
    }
    fn submit(&mut self, batch: OutboundBatch) -> Result<(), TransportRejected> {
        let result = (|| {
            if self.state != TransportState::Open {
                return Err(TransportError::Closed);
            }
            self.validate_session()?;
            if self.session.as_ref().unwrap().state() != SessionState::Ready {
                return Err(TransportError::Closed);
            }
            if self.sending.is_some() || self.completed.is_some() {
                return Err(TransportError::Overloaded);
            }
            if batch.ticket.binding != self.outbound
                || batch.ticket.peer != self.binding.peer.node
                || batch.ticket.sequence == 0
            {
                return Err(TransportError::WrongBinding);
            }
            batch_cost(
                self.outbound,
                &batch.messages,
                batch.messages.capacity(),
                self.outbound_limits,
            )
            .map_err(TransportError::Outbound)?;
            let frame = self
                .codec
                .encode_batch(self.binding.outgoing(), &batch.messages)
                .map_err(TransportError::Wire)?;
            if frame.is_empty()
                || frame.capacity() > self.limits.send_frame_bytes
                || frame.len() > self.codec.limits().max_frame_bytes
            {
                return Err(TransportError::ProviderViolation);
            }
            Ok(frame)
        })();
        match result {
            Ok(frame) => {
                self.sending = Some(Sending {
                    batch,
                    frame,
                    written: 0,
                });
                Ok(())
            }
            Err(reason) => Err(TransportRejected {
                reason,
                batch: Box::new(batch),
            }),
        }
    }
    fn poll(
        &mut self,
        now: MonoTime,
        budget: TransportPollBudget,
    ) -> Result<TransportProgress, TransportError> {
        budget.validate()?;
        if let Some(error) = self.failure {
            return Err(error);
        }
        if self.state == TransportState::Closed {
            return Ok(TransportProgress::default());
        }
        if let Err(e) = self.validate_session() {
            return self.fail(e);
        }
        self.start_close();
        let session = match self.session.as_mut().unwrap().poll(now, budget.session) {
            Ok(p) => p,
            Err(e) => return self.fail(TransportError::Session(e)),
        };
        if let Err(e) = self.validate_session() {
            return self.fail(e);
        }
        let mut progress = TransportProgress {
            session,
            sent: self.finish_send(),
            ..TransportProgress::default()
        };
        let mut blocked_read = false;
        let mut blocked_write = false;
        for _ in 0..budget.plaintext_calls {
            if self.close_started {
                break;
            }
            let read =
                !blocked_read && self.received.is_none() && progress.read_bytes < budget.read_bytes;
            let write = !blocked_write
                && self
                    .sending
                    .as_ref()
                    .is_some_and(|s| s.written < s.frame.len())
                && progress.written_bytes < budget.write_bytes;
            if !read && !write {
                break;
            }
            let reading = read && (!write || self.prefer_read);
            self.prefer_read = !reading;
            progress.plaintext_calls += 1;
            let result = if reading {
                self.read_step(budget.read_bytes - progress.read_bytes)
            } else {
                self.write_step(budget.write_bytes - progress.written_bytes)
            };
            match result {
                Ok(n) => {
                    if reading {
                        progress.read_bytes += n;
                        progress.received |= self.received.is_some();
                    } else {
                        progress.written_bytes += n;
                    }
                }
                Err(TransportError::Session(SessionError::WouldBlock)) => {
                    if reading {
                        blocked_read = true;
                    } else {
                        blocked_write = true;
                    }
                }
                Err(e) => return self.fail(e),
            }
            if self.state == TransportState::Closed {
                break;
            }
            progress.sent |= self.finish_send();
        }
        if self.state != TransportState::Closed {
            self.start_close();
            if self.close_started && self.session.as_ref().unwrap().state() == SessionState::Closed
            {
                self.session.take();
                self.state = TransportState::Closed;
            }
        }
        Ok(progress)
    }
    fn take_send(&mut self) -> Option<TransportSend> {
        self.completed.take()
    }
    fn take_received(&mut self) -> Option<ReceivedBatch> {
        let batch = self.received.take();
        if batch.is_some() {
            self.decoded_bytes = 0;
        }
        batch
    }
    fn close(&mut self) {
        if self.state == TransportState::Open {
            self.state = TransportState::Draining;
        }
    }
    fn abort(&mut self) {
        if matches!(self.state, TransportState::Open | TransportState::Draining) {
            let _: Result<(), _> = self.fail(TransportError::Aborted);
        }
    }
}
