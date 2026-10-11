// SPDX-License-Identifier: RPL-1.5
// Copyright (c) 2026 Karl Livesey
use super::*;

#[test]
fn resolved_send_close_retains_actual_long_rtt_protocol_deadline() {
    let ((mut a, mut b), arrivals) =
        configured_pair_with(1, 1, SessionLimits::default(), Arrivals::capture);
    let progress = a.poll(MonoTime(0), SessionPollBudget::default()).unwrap();
    assert!(progress.written_bytes > 0);
    arrivals.emitted(0, progress.written_bytes);
    // Hold the actual first datagram through caller time, without sleeps or
    // altered protocol timers. Physical readiness is still checked explicitly.
    let progress = b
        .poll(MonoTime(3500), SessionPollBudget::default())
        .unwrap();
    arrivals.emitted(1, progress.written_bytes);
    let mut start = None;
    for time in 3501..5000 {
        for (index, session) in [&mut a, &mut b].into_iter().enumerate() {
            let progress = session
                .poll(MonoTime(time), SessionPollBudget::default())
                .unwrap();
            arrivals.emitted(index, progress.written_bytes);
        }
        if a.state() == SessionState::Ready
            && b.state() == SessionState::Ready
            && a.is_flushed()
            && b.is_flushed()
        {
            start = Some(time);
            break;
        }
    }
    let start = start.expect("both authenticated hello sends must be flushed");
    assert_eq!(a.binding().unwrap().peer, local(2));
    assert_eq!(b.binding().unwrap().peer, local(1));
    drop(b);
    a.close();
    a.poll(MonoTime(start), SessionPollBudget::default())
        .unwrap();
    let deadline = a.next_deadline().unwrap();
    assert!(deadline.0 > start + 15000);
    a.poll(MonoTime(start + 15000), SessionPollBudget::default())
        .unwrap();
    assert_eq!(a.state(), SessionState::Closing);
    assert!(a.is_flushed());
    a.poll(deadline, SessionPollBudget::default()).unwrap();
    assert_eq!(a.state(), SessionState::Closed);
    assert!(a.is_flushed());
    assert_eq!(a.next_deadline(), None);
}
