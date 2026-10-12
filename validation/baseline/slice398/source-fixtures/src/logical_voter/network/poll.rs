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
use super::frame::LOGICAL_FRAME_HEADER;
use super::*;
impl<T: SecureSession, C: LogCodec + Clone, S: SnapshotCodec + Clone, W: WireCodec + Clone>
    LogicalPeerSession<T, C, S, W>
{
    pub fn poll(
        &mut self,
        now: MonoTime,
        budget: TransportPollBudget,
    ) -> Result<TransportProgress, LogicalNetworkError> {
        budget
            .validate()
            .map_err(|_| LogicalNetworkError::InvalidLimits)?;
        if self.last_time.is_some_and(|t| now < t) {
            return Err(LogicalNetworkError::TimeWentBack);
        }
        self.last_time = Some(now);
        if let Some(e) = self.failure {
            return Err(e);
        }
        if self.state == TransportState::Closed {
            return Ok(TransportProgress::default());
        }
        let result = self.progress(now, budget);
        if let Err(e) = result {
            self.fail(e);
        }
        result
    }
    fn progress(
        &mut self,
        now: MonoTime,
        budget: TransportPollBudget,
    ) -> Result<TransportProgress, LogicalNetworkError> {
        let mut p = TransportProgress {
            session: self
                .session
                .as_mut()
                .unwrap()
                .poll(now, budget.session)
                .map_err(LogicalNetworkError::Session)?,
            ..TransportProgress::default()
        };
        let session = self.session.as_ref().unwrap();
        if session.security() != SessionSecurity::Authenticated
            || session.binding() != Some(self.binding)
            || session.state() == SessionState::Handshaking
        {
            return Err(LogicalNetworkError::WrongBinding);
        }
        for _ in 0..budget.plaintext_calls {
            let before = (p.read_bytes, p.written_bytes);
            if self.prefer_read {
                self.read(&mut p, budget)?;
                self.write(&mut p, budget)?;
            } else {
                self.write(&mut p, budget)?;
                self.read(&mut p, budget)?;
            }
            self.prefer_read = !self.prefer_read;
            if before == (p.read_bytes, p.written_bytes)
                || p.plaintext_calls >= budget.plaintext_calls
            {
                break;
            }
        }
        let flushed = self.session.as_ref().unwrap().is_flushed();
        if self
            .sending
            .as_ref()
            .is_some_and(|s| s.written == s.frame.len())
            && flushed
        {
            let s = self.sending.take().unwrap();
            drop(s.frame);
            self.completion = Some(LogicalNetworkCompletion {
                batch: s.batch,
                result: LocalSendResult::Sent,
            });
            p.sent = true;
        }
        if self.state == TransportState::Draining && self.sending.is_none() {
            if !self.close_started {
                self.session.as_mut().unwrap().close();
                self.close_started = true;
                self.incoming = Vec::new();
            }
            if self.session.as_ref().unwrap().state() == SessionState::Closed {
                self.session = None;
                self.state = TransportState::Closed;
            }
        }
        Ok(p)
    }
    fn write(
        &mut self,
        p: &mut TransportProgress,
        b: TransportPollBudget,
    ) -> Result<(), LogicalNetworkError> {
        let Some(s) = self.sending.as_mut() else {
            return Ok(());
        };
        if s.written == s.frame.len()
            || p.written_bytes >= b.write_bytes
            || p.plaintext_calls >= b.plaintext_calls
        {
            return Ok(());
        }
        let n = (s.frame.len() - s.written).min(b.write_bytes - p.written_bytes);
        p.plaintext_calls += 1;
        match self
            .session
            .as_mut()
            .unwrap()
            .write_plaintext(&s.frame[s.written..s.written + n])
        {
            Ok(count) if count <= n => {
                s.written += count;
                p.written_bytes += count;
                Ok(())
            }
            Ok(_) => Err(LogicalNetworkError::ProviderViolation),
            Err(SessionError::WouldBlock) => Ok(()),
            Err(e) => Err(LogicalNetworkError::Session(e)),
        }
    }
    fn read(
        &mut self,
        p: &mut TransportProgress,
        b: TransportPollBudget,
    ) -> Result<(), LogicalNetworkError> {
        if self.received.is_some()
            || self.state != TransportState::Open
            || p.read_bytes >= b.read_bytes
            || p.plaintext_calls >= b.plaintext_calls
        {
            return Ok(());
        }
        let n = (self.target - self.incoming.len())
            .min(b.read_bytes - p.read_bytes)
            .min(8192);
        let mut bytes = [0u8; 8192];
        p.plaintext_calls += 1;
        match self
            .session
            .as_mut()
            .unwrap()
            .read_plaintext(&mut bytes[..n])
        {
            Ok(0) => {
                if !self.incoming.is_empty() {
                    return Err(LogicalNetworkError::Truncated);
                }
                if self.sending.is_some() {
                    return Err(LogicalNetworkError::Closed);
                }
                self.close();
                Ok(())
            }
            Ok(count) if count <= n => {
                self.incoming.extend_from_slice(&bytes[..count]);
                p.read_bytes += count;
                if self.incoming.len() == self.target {
                    if self.target == LOGICAL_FRAME_HEADER {
                        self.target = self.codec.frame_length(&self.incoming)?;
                        self.incoming
                            .reserve_exact(self.target - self.incoming.len());
                    }
                    if self.incoming.len() == self.target {
                        self.received = Some(self.codec.decode(
                            self.binding.peer,
                            &self.incoming,
                            self.authorization,
                        )?);
                        self.incoming = Vec::with_capacity(LOGICAL_FRAME_HEADER);
                        self.target = LOGICAL_FRAME_HEADER;
                        p.received = true;
                    }
                }
                Ok(())
            }
            Ok(_) => Err(LogicalNetworkError::ProviderViolation),
            Err(SessionError::WouldBlock) => Ok(()),
            Err(e) => Err(LogicalNetworkError::Session(e)),
        }
    }
}
