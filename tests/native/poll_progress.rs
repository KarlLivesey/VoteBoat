// SPDX-License-Identifier: RPL-1.5
// Copyright (c) 2026 Karl Livesey
//! Independent buffered host session exposes plaintext-to-session follow-through.
use super::*;
struct Control {
    calls: Vec<SessionPollBudget>,
    first: SessionProgress,
    fail: usize,
    excessive: Option<(usize, usize)>,
}
struct BufferedSession {
    inner: HostSession,
    control: Arc<Mutex<Control>>,
    pending: usize,
}
impl SecureSession for BufferedSession {
    fn security(&self) -> SessionSecurity {
        self.inner.security()
    }
    fn state(&self) -> SessionState {
        self.inner.state()
    }
    fn binding(&self) -> Option<SessionBinding> {
        self.inner.binding()
    }
    fn limits(&self) -> SessionLimits {
        self.inner.limits()
    }
    fn poll(
        &mut self,
        _: MonoTime,
        budget: SessionPollBudget,
    ) -> Result<SessionProgress, SessionError> {
        let mut c = self.control.lock().unwrap();
        c.calls.push(budget);
        let phase = c.calls.len();
        if phase == c.fail {
            return Err(SessionError::Io(std::io::ErrorKind::BrokenPipe));
        }
        let mut progress = if phase == 1 && budget.io_calls > 0 {
            c.first
        } else {
            SessionProgress::default()
        };
        if self.pending > 0
            && progress.io_calls < budget.io_calls
            && self.pending <= budget.write_bytes
        {
            progress.io_calls += 1;
            progress.written_bytes += self.pending;
            self.pending = 0;
            self.inner.outgoing.lock().unwrap().flushed = true;
        }
        if let Some((target, dimension)) = c.excessive {
            if target == phase {
                match dimension {
                    0 => progress.io_calls = budget.io_calls + 1,
                    1 => progress.read_bytes = budget.read_bytes + 1,
                    _ => progress.written_bytes = budget.write_bytes + 1,
                }
            }
        }
        Ok(progress)
    }
    fn read_plaintext(&mut self, bytes: &mut [u8]) -> Result<usize, SessionError> {
        self.inner.read_plaintext(bytes)
    }
    fn write_plaintext(&mut self, bytes: &[u8]) -> Result<usize, SessionError> {
        let count = self.inner.write_plaintext(bytes)?;
        self.pending += count;
        self.inner.outgoing.lock().unwrap().flushed = false;
        Ok(count)
    }
    fn is_flushed(&self) -> bool {
        self.pending == 0 && self.inner.is_flushed()
    }
    fn close(&mut self) {
        self.inner.close()
    }
    fn revoke(&mut self) {
        self.inner.revoke()
    }
}
type Transport = NativePeerTransport<BufferedSession, NativeWireCodec>;
fn fixture() -> (Transport, NativeOutbound, Arc<Mutex<Control>>, HostSession) {
    let (inner, peer) = sessions(1024);
    let control = Arc::new(Mutex::new(Control {
        calls: Vec::new(),
        first: SessionProgress {
            io_calls: 1,
            read_bytes: 3,
            ..Default::default()
        },
        fail: 0,
        excessive: None,
    }));
    let queue = queue(1);
    let transport = NativePeerTransport::new(
        BufferedSession {
            inner,
            control: control.clone(),
            pending: 0,
        },
        codec(),
        &queue,
        TransportLimits::default(),
    )
    .unwrap();
    (transport, queue, control, peer)
}
fn budget() -> TransportPollBudget {
    TransportPollBudget {
        session: SessionPollBudget {
            io_calls: 3,
            read_bytes: 17,
            write_bytes: 19,
        },
        plaintext_calls: 2,
        read_bytes: 0,
        write_bytes: 7,
    }
}
#[test]
fn plaintext_follow_through_uses_only_remaining_independent_session_budget() {
    let (mut transport, mut queue, control, _peer) = fixture();
    transport
        .submit(batch(&mut queue, vec![message(1, 2, 1)]))
        .unwrap();
    let p = transport.poll(MonoTime(0), budget()).unwrap();
    assert_eq!(p.written_bytes, 7);
    assert_eq!(
        p.session,
        SessionProgress {
            io_calls: 2,
            read_bytes: 3,
            written_bytes: 7,
            became_ready: false
        }
    );
    let c = control.lock().unwrap();
    assert_eq!(c.calls.len(), 2);
    assert_eq!(c.calls[1].io_calls, 2);
    assert_eq!(c.calls[1].read_bytes, 14);
    assert_eq!(c.calls[1].write_bytes, 19);
    drop(c);
    assert!(transport.usage().sending);
    assert!(transport.take_send().is_none()); // partial original frame
    transport.abort();
    let done = transport.take_send().unwrap();
    queue.complete(done.batch, done.result).unwrap();
    assert_eq!(queue.usage(), OutboundUsage::default());
}
#[test]
fn exhausted_and_zero_session_budgets_do_not_add_follow_through_polls() {
    for calls in [0, 3] {
        let (mut transport, mut queue, control, _peer) = fixture();
        control.lock().unwrap().first.io_calls = calls;
        transport
            .submit(batch(&mut queue, vec![message(1, 2, 1)]))
            .unwrap();
        let mut limit = budget();
        limit.session.io_calls = calls;
        let p = transport.poll(MonoTime(0), limit).unwrap();
        assert_eq!(p.session.io_calls, calls);
        assert_eq!(control.lock().unwrap().calls.len(), 1);
        assert!(transport.take_send().is_none());
        transport.abort();
        let done = transport.take_send().unwrap();
        queue.complete(done.batch, done.result).unwrap();
    }
}
#[test]
fn follow_through_failure_preserves_validated_receive_and_original_failed_batch() {
    let (mut transport, mut queue, control, peer) = fixture();
    control.lock().unwrap().fail = 2;
    let received = vec![message(2, 1, 2)];
    let frame = codec()
        .encode_batch(binding(2, 1).outgoing(), &received)
        .unwrap();
    peer.outgoing.lock().unwrap().input.extend(frame);
    let expected = vec![message(1, 2, 1)];
    let batch = batch(&mut queue, expected.clone());
    let original = batch.ticket;
    transport.submit(batch).unwrap();
    assert_eq!(
        transport.poll(MonoTime(0), TransportPollBudget::default()),
        Err(TransportError::Session(SessionError::Io(
            std::io::ErrorKind::BrokenPipe
        )))
    );
    assert_eq!(control.lock().unwrap().calls.len(), 2);
    assert_eq!(transport.state(), TransportState::Failed);
    assert_eq!(transport.usage().send_frame_bytes, 0);
    assert_eq!(transport.usage().receive_frame_bytes, 0);
    assert_eq!(transport.take_received().unwrap().messages, received);
    assert_eq!(queue.usage().batches, 1);
    let done = transport.take_send().unwrap();
    assert_eq!(done.batch.ticket, original);
    assert_eq!(done.batch.messages, expected);
    assert_eq!(done.result, LocalSendResult::Failed);
    queue.complete(done.batch, done.result).unwrap();
    assert_eq!(queue.usage(), OutboundUsage::default());
}
#[test]
fn overreported_session_progress_on_either_poll_fails_before_reusing_budget() {
    for phase in [1, 2] {
        for dimension in 0..3 {
            let (mut transport, mut queue, control, _peer) = fixture();
            control.lock().unwrap().excessive = Some((phase, dimension));
            transport
                .submit(batch(&mut queue, vec![message(1, 2, 1)]))
                .unwrap();
            assert_eq!(
                transport.poll(MonoTime(0), budget()),
                Err(TransportError::ProviderViolation)
            );
            assert_eq!(control.lock().unwrap().calls.len(), phase);
            assert_eq!(transport.usage().send_frame_bytes, 0);
            let done = transport.take_send().unwrap();
            assert_eq!(done.result, LocalSendResult::Failed);
            queue.complete(done.batch, done.result).unwrap();
            assert_eq!(queue.usage(), OutboundUsage::default());
        }
    }
}
