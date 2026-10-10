// SPDX-License-Identifier: RPL-1.5
// Copyright (c) 2026 Karl Livesey
use super::*;

#[test]
fn authenticated_reconnect_starts_a_fresh_disconnected_output_interval() {
    let mut f = Fixture::new();
    f.attach();
    f.driver
        .as_mut()
        .unwrap()
        .disconnect(node(2), MonoTime(1))
        .unwrap();
    f.poll(1).unwrap();
    f.poll(2).unwrap();
    assert_eq!(f.poll(2).unwrap().connections, 1);
    f.transports.borrow()[&node(2)].borrow_mut().blocked = true;
    let tickets = (0..4).map(|_| f.enqueue(2)).collect::<Vec<_>>();
    f.poll(2).unwrap();
    f.connect.borrow_mut().ready = false;
    f.driver
        .as_mut()
        .unwrap()
        .disconnect(node(2), MonoTime(600))
        .unwrap();
    let mut terminals = f.poll(600).unwrap().completions;
    assert_eq!(terminals.len(), 1);
    assert_eq!(f.outbound.peer_usage(node(2)).batches, 3);
    assert!(f.poll(1099).unwrap().completions.is_empty());
    terminals.extend(f.poll(1100).unwrap().completions);
    assert_eq!(terminals.len(), 4);
    for ticket in tickets {
        assert_eq!(
            terminals
                .iter()
                .filter(|c| c.ticket == ticket && c.result == LocalSendResult::Failed)
                .count(),
            1
        );
    }
    assert!(f.outbound.is_drained());
    // The host still owns a pending connector receipt after output resolves.
    // Permit its terminal delivery so close can drain that separate lifetime.
    f.connect.borrow_mut().ready = true;
    f.close(1100);
}

#[test]
fn failed_peer_releases_saturated_output_without_waiting_for_reconnect() {
    let mut f = Fixture::new();
    f.attach();
    let old = f.transports.borrow()[&node(2)].borrow().binding;
    f.transports.borrow()[&node(2)].borrow_mut().blocked = true;
    let tickets = (0..4).map(|_| f.enqueue(2)).collect::<Vec<_>>();
    f.poll(0).unwrap();
    assert_eq!(f.outbound.peer_usage(node(2)).batches, 4);
    f.connect.borrow_mut().ready = false;
    f.driver
        .as_mut()
        .unwrap()
        .disconnect(node(2), MonoTime(1))
        .unwrap();
    let first = f.poll(1).unwrap();
    assert_eq!(first.completions.len(), 1); // Transport-owned batch.
    assert_eq!(first.completions[0].result, LocalSendResult::Failed);
    assert!(tickets.contains(&first.completions[0].ticket));
    let healthy = f.enqueue(3);
    f.poll(2).unwrap();
    assert!(f
        .poll(2)
        .unwrap()
        .completions
        .iter()
        .any(|c| c.ticket == healthy));
    assert_eq!(f.outbound.peer_usage(node(2)).batches, 3);
    assert!(f.poll(500).unwrap().completions.is_empty());
    // Reconnect backoff cannot hide the output deadline.
    assert_eq!(
        f.driver.as_ref().unwrap().next_deadline(),
        Some(MonoTime(501))
    );
    let expired = f.poll(501).unwrap();
    let mut resolved = first.completions;
    resolved.extend(expired.completions);
    assert_eq!(resolved.len(), tickets.len());
    for ticket in tickets {
        assert_eq!(
            resolved
                .iter()
                .filter(|c| c.ticket == ticket && c.result == LocalSendResult::Failed)
                .count(),
            1
        );
    }
    assert_eq!(f.outbound.peer_usage(node(2)).batches, 0);
    assert_eq!(f.driver.as_ref().unwrap().usage().staged_batches, 0);
    assert!(!f.owner.is_failed());
    // New messages during continued failure also return exact local failure.
    let refused = f.enqueue(2);
    assert!(f
        .poll(501)
        .unwrap()
        .completions
        .iter()
        .any(|c| c.ticket == refused && c.result == LocalSendResult::Failed));
    f.connect.borrow_mut().ready = true;
    f.poll(502).unwrap();
    f.poll(502).unwrap();
    assert!(
        f.driver
            .as_ref()
            .unwrap()
            .roster()
            .binding(node(2))
            .unwrap()
            .generation
            > old.generation
    );
    let fresh = f.enqueue(2);
    f.poll(502).unwrap();
    assert!(f
        .poll(502)
        .unwrap()
        .completions
        .iter()
        .any(|c| c.ticket == fresh && c.result == LocalSendResult::Sent));
    assert!(f.outbound.is_drained());
    f.close(502);
}
