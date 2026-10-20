// SPDX-License-Identifier: RPL-1.5
// Copyright (c) 2026 Karl Livesey
//! Same-poll send flush (N1) of the direct peer driver, over transports that
//! accept, partly accept or refuse bytes on command.
use super::*;

impl Fixture {
    fn flush_with(
        &mut self,
        budget: PeerDriverBudget,
    ) -> Result<PeerDriverProgress, PeerDriverError> {
        let mut out = PeerDriverProgress::default();
        self.driver.as_mut().unwrap().flush(
            &mut self.owner,
            &mut self.outbound,
            MonoTime(0),
            budget,
            &mut out,
        )?;
        Ok(out)
    }
    fn flush(&mut self) -> PeerDriverProgress {
        self.flush_with(PeerDriverBudget::default()).unwrap()
    }
    fn control(&self, peer: u64) -> Rc<RefCell<TransportControl>> {
        self.transports.borrow()[&node(peer)].clone()
    }
    fn take_budgets(&self, peer: u64) -> Vec<TransportPollBudget> {
        std::mem::take(&mut self.control(peer).borrow_mut().budgets)
    }
    fn sent(&self, peer: u64) -> usize {
        self.control(peer).borrow().sent.len()
    }
}
fn done(p: &PeerDriverProgress) -> Vec<(SendTicket, LocalSendResult)> {
    p.completions.iter().map(|c| (c.ticket, c.result)).collect()
}

#[test]
fn flush_writes_and_completes_queued_output_in_one_call_without_reading() {
    let mut f = Fixture::new();
    f.attach();
    let (a, b) = (f.enqueue(2), f.enqueue(3));
    f.take_budgets(2);
    f.take_budgets(3);
    let p = f.flush();
    assert_eq!(p.sends, 2);
    let mut completed = done(&p);
    completed.sort_by_key(|(t, _)| t.sequence);
    assert_eq!(
        completed,
        [(a, LocalSendResult::Sent), (b, LocalSendResult::Sent)]
    );
    assert_eq!(f.outbound.usage(), OutboundUsage::default());
    assert_eq!(p.received, 0);
    let read_nothing = TransportPollBudget {
        session: SessionPollBudget {
            read_bytes: 0,
            ..Default::default()
        },
        read_bytes: 0,
        ..Default::default()
    };
    for peer in [2, 3] {
        // One write-only visit per peer: the caller's write budget, no reads.
        assert_eq!(
            format!("{:?}", f.take_budgets(peer)),
            format!("{:?}", [read_nothing])
        );
        assert_eq!(f.sent(peer), 1);
    }
    // Nothing queued: a further flush visits no transport.
    let p = f.flush();
    assert_eq!((p.sends, p.completions.len(), p.peers.len()), (0, 0, 0));
    assert!(f.take_budgets(2).is_empty() && f.take_budgets(3).is_empty());
    f.close(0);
}

#[test]
fn flush_keeps_a_partly_written_batch_and_the_rest_of_its_peer_queue_in_order() {
    let mut f = Fixture::new();
    f.attach();
    f.control(2).borrow_mut().short_writes = 2;
    let tickets = [f.enqueue(2), f.enqueue(2), f.enqueue(2)];
    let p = f.flush();
    assert_eq!(p.sends, 1, "one accepted batch per peer");
    assert!(p.completions.is_empty(), "a short write never completes");
    assert_eq!(f.outbound.usage().batches, 3);
    assert!(f.control(2).borrow().sent.is_empty());
    let mut completed = Vec::new();
    for _ in 0..8 {
        completed.extend(done(&f.poll(0).unwrap()));
        completed.extend(done(&f.flush()));
    }
    assert_eq!(
        completed,
        tickets.map(|t| (t, LocalSendResult::Sent)),
        "the same order, each once"
    );
    assert_eq!(f.sent(2), 3);
    assert!(f.outbound.is_drained());
    f.close(0);
}

#[test]
fn a_refused_write_is_retried_by_the_next_poll_without_duplication() {
    let mut f = Fixture::new();
    f.attach();
    f.control(2).borrow_mut().blocked = true;
    let a = f.enqueue(2);
    f.take_budgets(2);
    let p = f.flush();
    assert_eq!(p.sends, 1);
    assert!(p.completions.is_empty());
    assert_eq!(f.take_budgets(2).len(), 1, "the refused write was tried");
    assert!(f
        .driver
        .as_ref()
        .unwrap()
        .roster()
        .binding(node(2))
        .is_some());
    assert!(done(&f.poll(0).unwrap()).is_empty());
    assert!(done(&f.flush()).is_empty());
    assert_eq!(f.outbound.usage().batches, 1);
    f.control(2).borrow_mut().blocked = false;
    assert_eq!(done(&f.poll(0).unwrap()), [(a, LocalSendResult::Sent)]);
    for _ in 0..3 {
        assert!(done(&f.flush()).is_empty());
        assert!(done(&f.poll(0).unwrap()).is_empty());
    }
    assert_eq!(f.sent(2), 1);
    assert!(f.outbound.is_drained());
    f.close(0);
}

#[test]
fn a_transport_that_fails_between_submit_and_flush_completes_failed_once() {
    let mut f = Fixture::new();
    f.attach();
    f.control(2).borrow_mut().blocked = true;
    let a = f.enqueue(2);
    assert_eq!(f.poll(0).unwrap().sends, 1);
    f.control(2).borrow_mut().state = TransportState::Failed;
    let p = f.flush();
    assert_eq!(done(&p), [(a, LocalSendResult::Failed)]);
    assert!(p
        .peers
        .iter()
        .any(|e| matches!(e, PeerPoll::Disconnected { peer, .. } if *peer == node(2))));
    assert!(f
        .driver
        .as_ref()
        .unwrap()
        .roster()
        .binding(node(2))
        .is_none());
    assert_eq!(f.outbound.usage(), OutboundUsage::default());
    for _ in 0..3 {
        assert!(done(&f.flush()).is_empty());
        assert!(done(&f.poll(0).unwrap()).is_empty());
    }
    assert!(!f.owner.is_failed());
    assert_eq!(f.sent(2), 0);
}

#[test]
fn flush_obeys_the_send_and_visit_budgets_and_does_not_move_the_cursor() {
    let mut f = Fixture::new();
    f.attach();
    for peer in [2, 3] {
        f.control(peer).borrow_mut().blocked = true;
        f.enqueue(peer);
        f.take_budgets(peer);
    }
    let nothing = PeerDriverBudget {
        sends: 0,
        ..Default::default()
    };
    let p = f.flush_with(nothing).unwrap();
    assert_eq!((p.sends, p.peers.len()), (0, 0));
    assert!(f.take_budgets(2).is_empty() && f.take_budgets(3).is_empty());
    assert_eq!(f.outbound.usage().batches, 2);
    let one = PeerDriverBudget {
        peer_visits: 1,
        ..Default::default()
    };
    // Both batches are accepted; only one transport is visited, and a second
    // flush visits the same one again because flushing never moves the
    // driver's fair cursor.
    let p = f.flush_with(one).unwrap();
    assert_eq!((p.sends, p.peers.len()), (2, 1));
    let p = f.flush_with(one).unwrap();
    assert_eq!((p.sends, p.peers.len()), (0, 1));
    let visits = [f.take_budgets(2).len(), f.take_budgets(3).len()];
    assert!(visits == [2, 0] || visits == [0, 2], "{visits:?}");
    for peer in [2, 3] {
        f.control(peer).borrow_mut().blocked = false;
    }
    f.flush();
    assert!(f.outbound.is_drained());
    f.close(0);
}

#[test]
fn after_close_flush_fails_queued_output_and_completes_accepted_sends() {
    let mut f = Fixture::new();
    f.attach();
    f.control(2).borrow_mut().blocked = true;
    let a = f.enqueue(2);
    assert_eq!(f.poll(0).unwrap().sends, 1);
    let b = f.enqueue(3);
    f.driver.as_mut().unwrap().close();
    let p = f.flush();
    assert_eq!(p.sends, 0, "nothing new is submitted after close");
    assert_eq!(done(&p), [(b, LocalSendResult::Failed)]);
    assert_eq!(f.outbound.usage().batches, 1);
    f.control(2).borrow_mut().blocked = false;
    let p = f.flush();
    assert_eq!(done(&p), [(a, LocalSendResult::Sent)]);
    assert!(f.outbound.is_drained());
    for _ in 0..10 {
        f.poll(0).unwrap();
        if f.driver.as_ref().unwrap().is_drained() {
            return;
        }
    }
    panic!("not drained");
}

#[test]
fn a_flush_provider_violation_fences_the_owner_and_driver_like_poll() {
    let mut f = Fixture::new();
    f.attach();
    f.control(2).borrow_mut().wrong = true;
    f.enqueue(2);
    assert_eq!(
        f.flush_with(PeerDriverBudget::default()).unwrap_err(),
        PeerDriverError::Roster(PeerRosterError::ProviderViolation)
    );
    assert!(f.owner.is_failed());
    assert_eq!(f.poll(0).unwrap_err(), PeerDriverError::Fenced);
    // A fenced driver is left for poll to report; flush does nothing.
    assert!(f.flush_with(PeerDriverBudget::default()).is_ok());
    assert_eq!(f.outbound.usage().batches, 1, "credits held for recovery");
}
