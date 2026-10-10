// SPDX-License-Identifier: RPL-1.5
// Copyright (c) 2026 Karl Livesey
//! Independent bounded logical-frame transport. Uses only public domain APIs;
//! frame sizing shares the selected codec, not the native transport algorithm.
//! No encrypted byte decoding, scheduling, crash durability or OS I/O claims.
use super::*;
struct Frame {
    messages: Vec<Message>,
    length: usize,
    written: usize,
    read: usize,
}
pub(super) struct Pipe {
    frame: Option<Frame>,
    pub(super) flushed: bool,
}
impl Default for Pipe {
    fn default() -> Self {
        Self {
            frame: None,
            flushed: true,
        }
    }
}
struct Sending {
    batch: OutboundBatch,
    length: usize,
    written: usize,
}
pub(super) struct Host {
    connection: SessionBinding,
    queue: OutboundBinding,
    input: Arc<Mutex<Pipe>>,
    output: Arc<Mutex<Pipe>>,
    state: TransportState,
    sending: Option<Sending>,
    done: Option<TransportSend>,
    received: Option<ReceivedBatch>,
    early_flush: bool,
}
pub(super) fn pair(
    a: OutboundBinding,
    b: OutboundBinding,
    early_flush: bool,
) -> (Host, Host, Arc<Mutex<Pipe>>) {
    let left = Arc::new(Mutex::new(Pipe::default()));
    let right = Arc::new(Mutex::new(Pipe::default()));
    let build = |connection, queue, input, output, early_flush| Host {
        connection,
        queue,
        input,
        output,
        state: TransportState::Open,
        sending: None,
        done: None,
        received: None,
        early_flush,
    };
    (
        build(binding(1, 2), a, left.clone(), right.clone(), early_flush),
        build(binding(2, 1), b, right.clone(), left, false),
        right,
    )
}
impl Host {
    fn finish(&mut self) -> bool {
        if self.sending.as_ref().is_some_and(|s| s.written == s.length)
            && (self.early_flush || self.output.lock().unwrap().flushed)
        {
            let batch = self.sending.take().unwrap().batch;
            self.done = Some(TransportSend {
                connection: self.connection,
                batch,
                result: LocalSendResult::Sent,
            });
            return true;
        }
        false
    }
    fn write(&mut self, budget: usize) -> usize {
        let Some(send) = self.sending.as_mut() else {
            return 0;
        };
        if send.written == send.length {
            return 0;
        }
        let count = (send.length - send.written).min(budget).min(7);
        send.written += count;
        self.output.lock().unwrap().frame.as_mut().unwrap().written += count;
        count
    }
    fn read(&mut self, budget: usize) -> (usize, bool) {
        let mut input = self.input.lock().unwrap();
        let Some(frame) = input.frame.as_mut() else {
            return (0, false);
        };
        let count = (frame.written - frame.read).min(budget).min(7);
        frame.read += count;
        if frame.read != frame.length {
            return (count, false);
        }
        self.received = Some(ReceivedBatch {
            connection: self.connection,
            messages: input.frame.take().unwrap().messages,
        });
        (count, true)
    }
}
impl PeerTransport for Host {
    fn security(&self) -> SessionSecurity {
        SessionSecurity::Authenticated
    }
    fn binding(&self) -> SessionBinding {
        self.connection
    }
    fn state(&self) -> TransportState {
        self.state
    }
    fn limits(&self) -> TransportLimits {
        TransportLimits::default()
    }
    fn usage(&self) -> TransportUsage {
        TransportUsage {
            send_frame_bytes: self.sending.as_ref().map_or(0, |s| s.length),
            receive_frame_bytes: if self.received.is_some() {
                0
            } else {
                self.input
                    .lock()
                    .unwrap()
                    .frame
                    .as_ref()
                    .map_or(0, |f| f.read)
            },
            decoded_bytes: self.received_info().map_or(0, |i| i.bytes),
            sending: self.sending.is_some(),
            completion: self.done.is_some(),
        }
    }
    fn submit(&mut self, batch: OutboundBatch) -> Result<(), TransportRejected> {
        let reject = |reason, batch| {
            Err(TransportRejected {
                reason,
                batch: Box::new(batch),
            })
        };
        if self.state != TransportState::Open {
            return reject(TransportError::Closed, batch);
        }
        if batch.ticket.binding != self.queue
            || batch.ticket.peer != self.connection.peer.node
            || batch.ticket.sequence == 0
        {
            return reject(TransportError::WrongBinding, batch);
        }
        if self.sending.is_some()
            || self.done.is_some()
            || self.output.lock().unwrap().frame.is_some()
        {
            return reject(TransportError::Overloaded, batch);
        }
        let length = match codec().encoded_length(self.connection.outgoing(), &batch.messages) {
            Ok(length) if length <= self.limits().send_frame_bytes => length,
            Ok(_) => return reject(TransportError::Overloaded, batch),
            Err(error) => return reject(TransportError::Wire(error), batch),
        };
        let incoming = ReceivedBatch {
            connection: SessionBinding {
                local: self.connection.peer,
                peer: self.connection.local,
                ..self.connection
            },
            messages: batch.messages.clone(),
        };
        if let Err(error) = incoming.info(self.limits().decoded_bytes) {
            return reject(error, batch);
        }
        self.output.lock().unwrap().frame = Some(Frame {
            messages: incoming.messages,
            length,
            written: 0,
            read: 0,
        });
        self.sending = Some(Sending {
            batch,
            length,
            written: 0,
        });
        Ok(())
    }
    fn poll(
        &mut self,
        _: MonoTime,
        budget: TransportPollBudget,
    ) -> Result<TransportProgress, TransportError> {
        budget.validate()?;
        if self.state == TransportState::Failed {
            return Err(TransportError::Aborted);
        }
        if self.state == TransportState::Closed {
            return Ok(TransportProgress::default());
        }
        let mut progress = TransportProgress {
            sent: self.finish(),
            ..Default::default()
        };
        if budget.plaintext_calls > 0 && budget.write_bytes > 0 && self.sending.is_some() {
            progress.plaintext_calls += 1;
            progress.written_bytes = self.write(budget.write_bytes);
        }
        if progress.plaintext_calls < budget.plaintext_calls
            && budget.read_bytes > 0
            && self.received.is_none()
        {
            progress.plaintext_calls += 1;
            (progress.read_bytes, progress.received) = self.read(budget.read_bytes);
        }
        progress.sent |= self.finish();
        if self.state == TransportState::Draining && self.sending.is_none() {
            self.state = TransportState::Closed;
        }
        Ok(progress)
    }
    fn take_send(&mut self) -> Option<TransportSend> {
        self.done.take()
    }
    fn received_info(&self) -> Option<ReceiveInfo> {
        self.received
            .as_ref()
            .map(|b| b.info(self.limits().decoded_bytes).unwrap())
    }
    fn take_received(&mut self) -> Option<ReceivedBatch> {
        self.received.take()
    }
    fn close(&mut self) {
        if self.state == TransportState::Open {
            self.state = TransportState::Draining
        }
    }
    fn abort(&mut self) {
        if self.state == TransportState::Open || self.state == TransportState::Draining {
            self.state = TransportState::Failed;
            if let Some(send) = self.sending.take() {
                self.done = Some(TransportSend {
                    connection: self.connection,
                    batch: send.batch,
                    result: LocalSendResult::Failed,
                });
            }
        }
    }
}
