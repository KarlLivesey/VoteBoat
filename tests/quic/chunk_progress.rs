// SPDX-License-Identifier: RPL-1.5
// Copyright (c) 2026 Karl Livesey
use super::*;

#[test]
fn serialized_small_chunks_preserve_acknowledged_flush_within_election_interval() {
    history(1, 12, 150);
}

#[test]
fn three_group_chunk_burst_progresses_with_coarse_owner_polls() {
    // Three per-peer control chunks in one default heartbeat interval.
    // This selected 6ms cadence is not a measured macOS poll-interval claim.
    history(6, 3, 50);
}

fn history(cadence: usize, chunks: u8, horizon: u64) {
    let ((mut sender, mut receiver), arrivals) = configured_pair_with(
        1,
        1,
        SessionLimits {
            write_buffer_bytes: 256,
            ..SessionLimits::default()
        },
        Arrivals::capture,
    );
    let start = arrivals.ready(&mut sender, &mut receiver);
    let source = (0..chunks * 16).collect::<Vec<_>>();
    let mut written = 0;
    let mut received = Vec::new();
    let budget = SessionPollBudget::default();
    // A controlled finite schedule, not a wall-clock/network latency promise.
    // Keep the production one-chunk flush gate; do not admit work early.
    let mut elapsed = 0;
    let mut trace = Vec::with_capacity((horizon as usize).div_ceil(cadence));
    for now in (start..start + horizon).step_by(cadence) {
        elapsed = now - start;
        if sender.is_flushed() && written < source.len() {
            match sender.write_plaintext(&source[written..written + 16]) {
                Ok(count) => {
                    assert_eq!(count, 16);
                    written += count;
                    assert!(!sender.is_flushed());
                }
                Err(SessionError::WouldBlock) => {}
                Err(error) => panic!("write failed: {error:?}"),
            }
        }
        let mut polls = [SessionProgress::default(); 2];
        for (index, session) in [&mut sender, &mut receiver].into_iter().enumerate() {
            let progress = session.poll(MonoTime(now), budget).unwrap();
            assert!(progress.io_calls <= budget.io_calls);
            assert!(progress.read_bytes <= budget.read_bytes);
            assert!(progress.written_bytes <= budget.write_bytes);
            polls[index] = progress;
            arrivals.emitted(index, progress.written_bytes);
        }
        let mut bytes = [0; 16];
        match receiver.read_plaintext(&mut bytes) {
            Ok(count) => {
                assert!(count > 0);
                received.extend_from_slice(&bytes[..count]);
            }
            Err(SessionError::WouldBlock) => {}
            Err(error) => panic!("read failed: {error:?}"),
        }
        trace.push((
            elapsed,
            written,
            received.len(),
            sender.is_flushed(),
            sender.next_deadline().map(|t| t.0.saturating_sub(start)),
            receiver.next_deadline().map(|t| t.0.saturating_sub(start)),
            polls,
        ));
        if received == source && sender.is_flushed() {
            break;
        }
    }
    println!("cadence={cadence} horizon={horizon} elapsed={elapsed} admitted={written} received={} flushed={}", received.len(), sender.is_flushed());
    println!("tick/admitted/received/flushed/sender-deadline/receiver-deadline/polls: {trace:#?}");
    assert_eq!(written, source.len(), "serialized chunk admission stalled");
    assert_eq!(received, source);
    assert!(sender.is_flushed(), "original chunk still unacknowledged");
    assert_eq!(sender.state(), SessionState::Ready);
    assert_eq!(receiver.state(), SessionState::Ready);
}
