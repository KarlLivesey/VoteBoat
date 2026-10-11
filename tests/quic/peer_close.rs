// SPDX-License-Identifier: RPL-1.5
// Copyright (c) 2026 Karl Livesey
use super::*;

#[test]
fn clean_peer_close_at_last_io_call_retains_response_and_protocol_timer() {
    let ((mut a, mut b), arrivals) =
        configured_pair_with(1, 1, SessionLimits::default(), Arrivals::capture);
    let start = arrivals.ready(&mut a, &mut b);
    a.close();
    let budget = SessionPollBudget {
        io_calls: 1,
        read_bytes: 1200,
        write_bytes: 1200,
    };
    let mut received_at = None;
    for time in start..start + 100 {
        let progress = a.poll(MonoTime(time), budget).unwrap();
        arrivals.emitted(0, progress.written_bytes);
        let progress = b.poll(MonoTime(time), budget).unwrap();
        arrivals.emitted(1, progress.written_bytes);
        if b.state() != SessionState::Ready {
            assert!(progress.read_bytes > 0);
            assert_eq!(progress.written_bytes, 0);
            received_at = Some(time);
            break;
        }
    }
    let received_at = received_at.expect("actual authenticated peer close must arrive");
    let status = b.drain_status();
    // EOF denies fresh writes; it does not remove the original protocol owner
    // before that owner can send its queued close response with a fresh budget.
    assert_eq!(b.read_plaintext(&mut [0; 1]), Ok(0));
    assert_eq!(
        b.write_plaintext(b"after peer close"),
        Err(SessionError::Closed)
    );
    let mut response = false;
    for time in received_at + 1..received_at + 100 {
        let progress = b.poll(MonoTime(time), budget).unwrap();
        assert!(progress.io_calls <= 1);
        assert!(progress.read_bytes <= 1200 && progress.written_bytes <= 1200);
        arrivals.emitted(1, progress.written_bytes);
        response |= progress.written_bytes > 0;
        let progress = a.poll(MonoTime(time), budget).unwrap();
        arrivals.emitted(0, progress.written_bytes);
        if response {
            break;
        }
    }
    assert!(response, "clean EOF must not strand Quinn's close response");
    assert_eq!(status.state, SessionState::Closing, "{status:?}");
    assert_eq!(status.protocol_drained, Some(false));
    assert!(status.next_deadline.is_some());
    assert_eq!(b.state(), SessionState::Closing);
    let deadline = b.drain_status().next_deadline.unwrap();
    b.poll(deadline, budget).unwrap();
    assert_eq!(b.state(), SessionState::Closed);
    assert_eq!(b.drain_status().protocol_drained, Some(true));
    assert_eq!(b.next_deadline(), None);
}
