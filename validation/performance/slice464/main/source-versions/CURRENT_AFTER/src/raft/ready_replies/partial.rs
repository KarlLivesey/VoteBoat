// SPDX-License-Identifier: RPL-1.5
// Copyright (c) 2026 Karl Livesey
//! Commit-prefix, append-base and remote catch-up cursors are distinct.
use super::*;

#[test]
fn partial_three_five_weighted_histories_retain_middle_and_match_serialized_receipts() {
    for kind in 0..3 {
        let (mut serial, replies) = partial(kind);
        let (mut joined, joined_replies) = partial(kind);
        let mut app = Counter::new(32).unwrap();
        app.apply_batch(serial.replay_committed()).unwrap();
        let mut joined_app = app.clone();
        let commit = serial.step(Event::Receive(replies[0].clone())).unwrap();
        let receipts = apply(&mut app, &durable(&mut serial, commit));
        serial.step(Event::Receive(replies[1].clone())).unwrap();
        let append = serial
            .propose_batch(vec![proposal(14), proposal(15)])
            .unwrap();
        assert!(apply(&mut app, &durable(&mut serial, append)).is_empty());
        let before = joined.state().clone();
        let combined = update(
            joined
                .propose_after_ready_replies(&joined_replies, vec![proposal(14), proposal(15)])
                .unwrap(),
        );
        assert_eq!(combined.commit_index, 3);
        assert_eq!(combined.suffix.as_ref().unwrap().from, 6);
        assert_eq!(joined.state(), &before);
        for reply in &joined_replies {
            assert_eq!(joined.progress[&reply.from], 3);
            assert_eq!(joined.next_index[&reply.from], 4);
        }
        assert!(joined.admitted_replication_reservation().is_none());
        let exact = ticket(&joined);
        assert!(joined.replicate_admitted(exact).unwrap().is_empty());
        assert_eq!(joined.state(), &before);
        let effects = joined
            .complete(&DurableLog {
                tickets: vec![exact],
            })
            .unwrap();
        assert_eq!(apply(&mut joined_app, &effects), receipts);
        assert_eq!(joined_app.applied_index(), 3);
        assert_eq!(joined.state().entries, serial.state().entries);
        assert_eq!(joined.state().commit_index, 3);
        assert_eq!(joined.state().last_index(), 7);
        let requests = sends(effects);
        for reply in &joined_replies {
            let request = requests.iter().find(|m| m.to == reply.from).unwrap();
            assert!(matches!(&request.rpc,
                Rpc::Append { previous_index: 3, entries, leader_commit: 3, .. }
                if entries.iter().map(|e| e.index).eq(4..=7)));
        }
        // Only an ACK for the new catch-up range may commit the middle and tail.
        assert!(joined
            .step(Event::Receive(joined_replies[0].clone()))
            .unwrap()
            .is_empty());
        assert_eq!(joined.state().commit_index, 3);
        let mut new_receipts = Vec::new();
        for request in &requests {
            let response = ack(&joined, request);
            let commit = joined.step(Event::Receive(response)).unwrap();
            if joined.pending.is_some() {
                new_receipts.extend(apply(&mut joined_app, &durable(&mut joined, commit)));
            }
            if joined.state().commit_index == 7 {
                break;
            }
        }
        assert_eq!(new_receipts.len(), 4);
        assert_eq!(joined.state().commit_index, 7);
    }
}

#[test]
fn later_reply_cannot_merge_another_commit_batch_and_already_committed_reply_cannot_join() {
    let (mut core, mut replies) = partial(0);
    // Exact peer3 request for the newer range would advance a second commit.
    core.retire_replication(replies[1].from);
    let request = sends(vec![core.append_for(replies[1].from).unwrap()]).remove(0);
    replies[1] = ack(&core, &request);
    gates::rejected(&mut core, &replies, vec![proposal(14)]);

    let (mut core, replies) = partial(0);
    let commit = core.step(Event::Receive(replies[0].clone())).unwrap();
    durable(&mut core, commit);
    assert_eq!(core.state().commit_index, 3);
    gates::rejected(&mut core, &replies[1..], vec![proposal(14)]);
}

#[test]
fn older_term_commit_frontier_refuses_even_with_current_term_tip() {
    let (mut core, mut replies) = partial(0);
    core.durable.hard_state.term += 1;
    for entry in &mut core.durable.entries[3..] {
        entry.term = core.durable.hard_state.term;
    }
    for reply in &mut replies {
        reply.term = core.durable.hard_state.term;
    }
    core.log_summary = LogSummary::from_validated(core.state());
    gates::rejected(&mut core, &replies, vec![proposal(14)]);
}

#[test]
fn successful_older_range_does_not_reduce_known_peer_progress() {
    let (mut core, replies) = partial(0);
    core.progress.insert(replies[0].from, 4);
    let planned = update(
        core.propose_after_ready_replies(&replies[..1], vec![proposal(14)])
            .unwrap(),
    );
    assert_eq!(planned.commit_index, 4);
    assert_eq!(core.progress[&replies[0].from], 4);
    assert_eq!(core.next_index[&replies[0].from], 5);
}
