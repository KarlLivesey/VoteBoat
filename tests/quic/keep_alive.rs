// SPDX-License-Identifier: RPL-1.5
// Copyright (c) 2026 Karl Livesey
use super::*;

const LOGICAL: u16 = voteboat::logical_voter::LOGICAL_NETWORK_VERSION;

#[test]
fn quiet_ordinary_authenticated_pair_retains_later_payload_and_close() {
    quiet(1);
}

#[test]
fn quiet_logical_authenticated_pair_retains_later_payload_and_close() {
    quiet(LOGICAL);
}

#[test]
fn stopped_ordinary_peer_still_reaches_original_bounded_failure() {
    stopped(1);
}

#[test]
fn stopped_logical_peer_still_reaches_original_bounded_failure() {
    stopped(LOGICAL);
}

fn quiet(version: u16) {
    let ((mut a, mut b), arrivals) = configured_pair_with(
        version,
        version,
        SessionLimits::default(),
        Arrivals::capture,
    );
    let start = arrivals.ready(&mut a, &mut b);
    let mut pair = [a, b];
    let late_bytes = quiet_interval(&mut pair, &arrivals, start, 7000);
    assert!(late_bytes.into_iter().all(|bytes| bytes > 0));
    eprintln!(
        "quiet original wire={version} start={start} duration=7000 late_ciphertext={late_bytes:?}"
    );
    let finish = exchange(&mut pair, &arrivals, start + 7000);
    close(&mut pair, &arrivals, finish);
}

// Real native UDP sessions, with the existing caller-time/arrival fixture.
// No plaintext is admitted; encrypted packet progress grants no Raft evidence.
fn quiet_interval(
    pair: &mut [NativeQuicSession; 2],
    arrivals: &Arrivals,
    start: u64,
    duration: u64,
) -> [usize; 2] {
    let binding = pair.each_ref().map(|s| require_authenticated(s).unwrap());
    let mut late_bytes = [0; 2];
    let budget = SessionPollBudget::default();
    for now in (start..start + duration).step_by(10) {
        for (index, session) in pair.iter_mut().enumerate() {
            let progress = session.poll(MonoTime(now), budget).unwrap_or_else(|error| {
                panic!(
                    "quiet original slot={index} now={now} elapsed={} error={error:?}",
                    now - start
                )
            });
            assert!(progress.io_calls <= budget.io_calls);
            assert!(progress.read_bytes <= budget.read_bytes);
            assert!(progress.written_bytes <= budget.write_bytes);
            arrivals.emitted(index, progress.written_bytes);
            assert_eq!(require_authenticated(session).unwrap(), binding[index]);
            assert_eq!(
                session.read_plaintext(&mut [0; 128]),
                Err(SessionError::WouldBlock)
            );
            if now >= start + 5000 {
                late_bytes[index] += progress.written_bytes;
            }
        }
    }
    late_bytes
}

fn exchange(pair: &mut [NativeQuicSession; 2], arrivals: &Arrivals, start: u64) -> u64 {
    let payloads: [&[u8]; 2] = [b"original quiet sender", b"original quiet receiver"];
    let mut written = [false; 2];
    let mut received = [Vec::new(), Vec::new()];
    for now in start..start + 1000 {
        for (index, session) in pair.iter_mut().enumerate() {
            if !written[index] {
                match session.write_plaintext(payloads[index]) {
                    Ok(count) => {
                        assert_eq!(count, payloads[index].len());
                        written[index] = true;
                    }
                    Err(SessionError::WouldBlock) => {}
                    Err(error) => panic!("original quiet payload write: {error:?}"),
                }
            }
            let progress = session
                .poll(MonoTime(now), SessionPollBudget::default())
                .unwrap();
            arrivals.emitted(index, progress.written_bytes);
            let mut buffer = [0; 128];
            match session.read_plaintext(&mut buffer) {
                Ok(count) => received[index].extend_from_slice(&buffer[..count]),
                Err(SessionError::WouldBlock) => {}
                Err(error) => panic!("original quiet payload read: {error:?}"),
            }
        }
        if received[0] == payloads[1]
            && received[1] == payloads[0]
            && written.into_iter().all(|sent| sent)
            && pair.iter().all(SecureSession::is_flushed)
        {
            return now;
        }
    }
    panic!("original quiet payload did not finish: {written:?}, {received:?}");
}

fn close(pair: &mut [NativeQuicSession; 2], arrivals: &Arrivals, start: u64) {
    for session in &mut *pair {
        session.close();
    }
    for now in (start..start + 10000).step_by(10) {
        for (index, session) in pair.iter_mut().enumerate() {
            let progress = session
                .poll(MonoTime(now), SessionPollBudget::default())
                .unwrap();
            arrivals.emitted(index, progress.written_bytes);
        }
        if pair.iter().all(|s| s.state() == SessionState::Closed) {
            for session in &mut *pair {
                let status = session.drain_status();
                assert!(status.flushed);
                assert_eq!(status.protocol_drained, Some(true));
                assert_eq!(status.next_deadline, None);
            }
            eprintln!("quiet original protocol close completed now={now}");
            return;
        }
    }
    panic!("original quiet pair must finish actual protocol close");
}

fn stopped(version: u16) {
    let ((mut a, mut b), arrivals) = configured_pair_with(
        version,
        version,
        SessionLimits::default(),
        Arrivals::capture,
    );
    let start = arrivals.ready(&mut a, &mut b);
    let mut pair = [a, b];
    quiet_interval(&mut pair, &arrivals, start, 1000);
    let [mut survivor, peer] = pair;
    let binding = require_authenticated(&survivor).unwrap();
    // The cloned real socket stays bound, but no peer owner processes or replies.
    drop(peer);
    let mut failed_at = None;
    for now in (start + 1000..start + 11000).step_by(10) {
        match survivor.poll(MonoTime(now), SessionPollBudget::default()) {
            Ok(_) => assert_eq!(require_authenticated(&survivor).unwrap(), binding),
            Err(error) => {
                assert_eq!(error, SessionError::Truncated);
                failed_at = Some(now);
                break;
            }
        }
    }
    let failed_at = failed_at.expect("stopped peer must still time out");
    eprintln!(
        "stopped original wire={version} start={start} failure={failed_at} elapsed={}",
        failed_at - start - 1000
    );
    assert!(failed_at <= start + 11000);
    assert_eq!(survivor.state(), SessionState::Failed);
    assert_eq!(
        require_authenticated(&survivor),
        Err(SessionError::NotReady)
    );
    assert_eq!(survivor.next_deadline(), None);
    assert_eq!(
        survivor.poll(MonoTime(failed_at), SessionPollBudget::default()),
        Err(SessionError::Truncated)
    );
    drop(arrivals);
}
