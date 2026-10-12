// SPDX-License-Identifier: RPL-1.5
// Copyright (c) 2026 Karl Livesey
//! Actual partial stream progress, terminal local output and channel closure.
use super::*;

impl<S: SecureSession> RelayHop<S> {
    pub fn poll(
        &mut self,
        now: MonoTime,
        budget: TransportPollBudget,
    ) -> Result<TransportProgress, RelayNetworkError> {
        budget
            .validate()
            .map_err(|_| RelayNetworkError::InvalidLimits)?;
        if self.last_time.is_some_and(|t| now < t) {
            return Err(RelayNetworkError::TimeWentBack);
        }
        self.last_time = Some(now);
        if matches!(self.state, TransportState::Closed | TransportState::Failed) {
            return Ok(TransportProgress::default());
        }
        let result = self.poll_inner(now, budget);
        if result.is_err() {
            self.fail();
        } else {
            self.next_deadline();
        }
        result
    }
    fn poll_inner(
        &mut self,
        now: MonoTime,
        budget: TransportPollBudget,
    ) -> Result<TransportProgress, RelayNetworkError> {
        self.validate_capture()?;
        let mut progress = TransportProgress::default();
        let session = self.session.as_mut().ok_or(RelayNetworkError::Closed)?;
        if self.close_started && session.state() == SessionState::Closed {
            self.state = TransportState::Closed;
            return Ok(progress);
        }
        let io = session
            .poll(now, budget.session)
            .map_err(RelayNetworkError::Session)?;
        self.validate_capture()?;
        progress.session = io;
        if budget.plaintext_calls != 0 {
            if self.prefer_read {
                self.read(&mut progress, budget.read_bytes)?;
            } else {
                self.write(&mut progress, budget.write_bytes)?;
            }
            if progress.plaintext_calls < budget.plaintext_calls {
                if self.prefer_read {
                    self.write(&mut progress, budget.write_bytes)?;
                } else {
                    self.read(&mut progress, budget.read_bytes)?;
                }
            }
            self.prefer_read = !self.prefer_read;
        }
        self.finish_close()?;
        Ok(progress)
    }
    fn write(
        &mut self,
        progress: &mut TransportProgress,
        maximum: usize,
    ) -> Result<(), RelayNetworkError> {
        let Some(sending) = &mut self.sending else {
            return Ok(());
        };
        let session = self.session.as_mut().ok_or(RelayNetworkError::Closed)?;
        let end = sending
            .written
            .saturating_add(maximum)
            .min(sending.bytes.len());
        if end > sending.written {
            progress.plaintext_calls += 1;
            match session.write_plaintext(&sending.bytes[sending.written..end]) {
                Ok(n) if n <= end - sending.written => {
                    sending.written += n;
                    progress.written_bytes += n;
                }
                Ok(_) => return Err(RelayNetworkError::ProviderViolation),
                Err(SessionError::WouldBlock) => (),
                Err(e) => return Err(RelayNetworkError::Session(e)),
            }
        }
        self.validate_capture()?;
        let sending = self.sending.as_ref().unwrap();
        let session = self.session.as_ref().ok_or(RelayNetworkError::Closed)?;
        if sending.written == sending.bytes.len() && session.is_flushed() {
            let s = self.sending.take().unwrap();
            progress.sent = true;
            self.completion = Some(RelayHopCompletion {
                ticket: s.ticket,
                frame: s.original,
                result: LocalSendResult::Sent,
            });
        }
        Ok(())
    }
    fn read(
        &mut self,
        progress: &mut TransportProgress,
        maximum: usize,
    ) -> Result<(), RelayNetworkError> {
        if self.received.is_some() || maximum == 0 || self.state != TransportState::Open {
            return Ok(());
        }
        let old = self.incoming.len();
        let count = (self.target - old).min(maximum);
        if count == 0 {
            return Ok(());
        }
        progress.plaintext_calls += 1;
        self.incoming.resize(old + count, 0);
        let result = self
            .session
            .as_mut()
            .ok_or(RelayNetworkError::Closed)?
            .read_plaintext(&mut self.incoming[old..]);
        self.validate_capture()?;
        let n = match result {
            Ok(0) => {
                self.incoming.truncate(old);
                return Err(RelayNetworkError::Truncated);
            }
            Ok(n) if n <= count => n,
            Ok(_) => return Err(RelayNetworkError::ProviderViolation),
            Err(SessionError::WouldBlock) => {
                self.incoming.truncate(old);
                return Ok(());
            }
            Err(e) => {
                self.incoming.truncate(old);
                return Err(RelayNetworkError::Session(e));
            }
        };
        self.incoming.truncate(old + n);
        progress.read_bytes += n;
        if self.incoming.len() != self.target {
            return Ok(());
        }
        if self.target == FRAME_HEADER {
            self.target = frame::frame_length(&self.incoming, self.limits.frame_bytes)?;
            self.incoming
                .reserve_exact(self.target - self.incoming.len());
            return Ok(());
        }
        let frame = frame::decode(&self.incoming, self.limits)?;
        self.validate_capture()?;
        self.validate_frame(&frame, true)?;
        let source = self.observed_source(&frame);
        self.receive_sequence = self
            .receive_sequence
            .checked_add(1)
            .ok_or(RelayNetworkError::Exhausted)?;
        self.received = Some(RelayReceived {
            frame,
            source,
            ticket: RelayReceiveTicket {
                connection: self.binding,
                sequence: self.receive_sequence,
            },
        });
        self.incoming = Vec::with_capacity(FRAME_HEADER);
        self.target = FRAME_HEADER;
        progress.received = true;
        Ok(())
    }
    fn finish_close(&mut self) -> Result<(), RelayNetworkError> {
        if self.state != TransportState::Draining
            || self.sending.is_some()
            || self.completion.is_some()
            || self.received.is_some()
        {
            return Ok(());
        }
        let s = self.session.as_mut().ok_or(RelayNetworkError::Closed)?;
        if !s.is_flushed() {
            return Ok(());
        }
        if !self.close_started {
            self.incoming = Vec::new();
            s.close();
            self.close_started = true;
        }
        if s.state() == SessionState::Closed {
            self.state = TransportState::Closed;
        }
        Ok(())
    }
}
