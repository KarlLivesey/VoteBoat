// SPDX-License-Identifier: RPL-1.5
// Copyright (c) 2026 Karl Livesey
use super::*;

#[test]
fn drain_inspection_preserves_exact_frame_slots_and_boxed_session_override() {
    let (a, b) = sessions(7);
    let (input, output) = (a.incoming.clone(), a.outgoing.clone());
    output.lock().unwrap().flushed = false;
    let mut queue_a = queue(1);
    let queue_b = queue(2);
    let a: Box<dyn SecureSession> = Box::new(a);
    let mut a: Box<dyn PeerTransport> = Box::new(
        NativePeerTransport::new(a, codec(), &queue_a, TransportLimits::default()).unwrap(),
    );
    let mut b = NativePeerTransport::new(b, codec(), &queue_b, TransportLimits::default()).unwrap();
    let original = message(1, 2, 42);
    let send = batch(&mut queue_a, vec![original.clone()]);
    let ticket = send.ticket;
    a.submit(send).unwrap();
    for _ in 0..1000 {
        poll(&mut *a);
        poll(&mut b);
        if b.received_info().is_some() {
            break;
        }
    }
    let received = b.received_info().expect("original frame must be decoded");
    a.close();
    let io = (input.lock().unwrap().reads, output.lock().unwrap().writes);
    for _ in 0..3 {
        let status = a.drain_status();
        assert_eq!(status.binding, binding(1, 2));
        assert_eq!(status.state, TransportState::Draining);
        assert!(status.usage.sending);
        assert!(!status.usage.completion);
        assert_eq!(status.close_started, Some(false));
        let session = status.session.unwrap();
        assert_eq!(session.state, SessionState::Ready);
        assert!(!session.flushed);
        assert_eq!(session.next_deadline, Some(MonoTime(73)));
        assert_eq!(b.drain_status().received, Some(received));
        assert_eq!(queue_a.usage().batches, 1);
    }
    assert_eq!(
        (input.lock().unwrap().reads, output.lock().unwrap().writes),
        io
    );
    output.lock().unwrap().flushed = true;
    poll(&mut *a);
    let status = a.drain_status();
    assert!(!status.usage.sending);
    assert!(status.usage.completion);
    assert_eq!(queue_a.usage().batches, 1);
    let done = a.take_send().unwrap();
    assert_eq!(done.batch.ticket, ticket);
    assert_eq!(done.batch.messages, vec![original.clone()]);
    assert_eq!(done.result, LocalSendResult::Sent);
    queue_a.complete(done.batch, done.result).unwrap();
    assert_eq!(queue_a.usage().batches, 0);
    assert!(a.take_send().is_none());
    assert_eq!(b.take_received().unwrap().messages, vec![original]);
    assert!(b.take_received().is_none());
}
struct Validity(Arc<std::sync::atomic::AtomicBool>);
impl SessionValidity for Validity {
    fn validate(&self) -> Result<(), SessionError> {
        if self.0.load(std::sync::atomic::Ordering::Relaxed) {
            Ok(())
        } else {
            Err(SessionError::Revoked)
        }
    }
}
#[test]
fn guarded_inspection_forwards_timer_but_does_not_drive_or_revoke_the_channel() {
    let (session, _) = sessions(7);
    let output = session.outgoing.clone();
    let valid = Arc::new(std::sync::atomic::AtomicBool::new(true));
    let mut session = GuardedSession::new(session, Validity(valid.clone()))
        .unwrap_or_else(|_| panic!("valid host session"));
    assert_eq!(session.drain_status().next_deadline, Some(MonoTime(73)));
    valid.store(false, std::sync::atomic::Ordering::Relaxed);
    let status = session.drain_status();
    assert_eq!(status.state, SessionState::Failed);
    assert!(!status.flushed);
    assert_eq!(status.next_deadline, None);
    assert!(!output.lock().unwrap().broken);
    assert!(!output.lock().unwrap().clean);
    assert_eq!(output.lock().unwrap().writes, 0);
    assert_eq!(
        session.poll(MonoTime(0), SessionPollBudget::default()),
        Err(SessionError::Revoked)
    );
}
