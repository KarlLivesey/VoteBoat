// SPDX-License-Identifier: RPL-1.5
// Copyright (c) 2026 Karl Livesey
//! Held follower Appends on the actual core, with explicit host-asserted
//! durable completions and no clock: each step is one named event.
use super::*;
use crate::membership::ConfigurationChange;
use crate::raft::membership_tests::{
    accept, committed_fixture, configuration, core, durable, store,
};

fn node(id: u64) -> NodeId {
    NodeId::new(id).unwrap()
}
fn proposals(first: u128, count: u128) -> Vec<Proposal> {
    (first..first + count)
        .map(|id| Proposal {
            operation: OperationId::new(id).unwrap(),
            bytes: 1_i64.to_le_bytes().to_vec(),
        })
        .collect()
}
/// Complete every persistence dependency in order; return the other effects.
fn run(core: &mut Raft, effects: Vec<Effect>) -> Vec<Effect> {
    let mut queue = VecDeque::from(effects);
    let mut out = Vec::new();
    while let Some(effect) = queue.pop_front() {
        if matches!(effect, Effect::Persist(_)) {
            queue.extend(durable(core, vec![effect]));
        } else {
            out.push(effect);
        }
    }
    out
}
fn batch(core: &mut Raft, first: u128, count: u128) -> Vec<Effect> {
    let effects = core.propose_batch(proposals(first, count)).unwrap();
    run(core, effects)
}
fn step(core: &mut Raft, event: Event) -> Vec<Effect> {
    let effects = core.step(event).unwrap();
    run(core, effects)
}
fn sent(effects: &[Effect], peer: u64) -> Option<Message> {
    effects.iter().find_map(|effect| match effect {
        Effect::Send(m) if m.to == node(peer) && matches!(m.rpc, Rpc::Append { .. }) => {
            Some(m.clone())
        }
        _ => None,
    })
}
/// (previous index, indexes carried) of one Append.
fn range(m: &Message) -> (u64, Vec<u64>) {
    let Rpc::Append {
        previous_index,
        entries,
        ..
    } = &m.rpc
    else {
        panic!("expected append")
    };
    (*previous_index, entries.iter().map(|e| e.index).collect())
}
fn ack(core: &Raft, request: &Message) -> Event {
    let (previous, indexes) = range(request);
    Event::Receive(Message {
        group: request.group,
        configuration: request.configuration,
        from: request.to,
        sender: StoreBinding {
            identity: core.membership().replica_store(request.to).unwrap(),
            session: StoreSession::new(1).unwrap(),
        },
        to: request.from,
        term: request.term,
        context: request.context,
        rpc: Rpc::Appended {
            success: true,
            matching_index: indexes.last().copied().unwrap_or(previous),
        },
    })
}
fn reply(core: &mut Raft, request: &Message) -> Vec<Effect> {
    let event = ack(core, request);
    step(core, event)
}
fn held(hold: AppendHold) -> Raft {
    let mut leader = core();
    leader.set_append_hold(hold);
    leader
}
/// A leader whose followers have both acknowledged its whole log (a heartbeat,
/// 3 first, so 2 is the most recent ack): the prefix evidence a hold needs.
fn confirmed(hold: AppendHold) -> Raft {
    let mut leader = held(hold);
    let effects = step(&mut leader, Event::Heartbeat);
    for peer in [3, 2] {
        let request = sent(&effects, peer).unwrap();
        assert!(reply(&mut leader, &request).is_empty());
    }
    assert_eq!(leader.progress[&node(2)], leader.state().last_index());
    assert_eq!(leader.progress[&node(3)], leader.state().last_index());
    leader
}
fn nack(core: &Raft, request: &Message, matching_index: u64) -> Event {
    let Event::Receive(mut m) = ack(core, request) else {
        unreachable!()
    };
    m.rpc = Rpc::Appended {
        success: false,
        matching_index,
    };
    Event::Receive(m)
}
/// First batch (entries 2..=3): follower 2 gets it, follower 3 is held.
fn first_batch_holds_three(leader: &mut Raft) -> Message {
    let effects = batch(leader, 1, 2);
    let two = sent(&effects, 2).expect("the quorum follower is sent at once");
    assert_eq!(range(&two), (1, vec![2, 3]));
    assert!(sent(&effects, 3).is_none());
    assert_eq!(leader.held, BTreeMap::from([(node(3), 3)]));
    assert!(leader.has_held_appends());
    two
}

#[test]
fn quorum_follower_is_sent_at_once_and_the_held_one_gets_both_batches_next() {
    let mut leader = confirmed(AppendHold::QuorumFirst);
    let two = first_batch_holds_three(&mut leader);
    // The quorum ack commits. 2's commit notice is held as well: its next
    // data Append (or the heartbeat) carries the commit.
    let effects = reply(&mut leader, &two);
    assert_eq!(leader.state().commit_index, 3);
    assert!(effects.iter().all(|e| !matches!(e, Effect::Send(_))));
    assert_eq!(leader.held, BTreeMap::from([(node(2), 3), (node(3), 3)]));
    // The next durable leader batch releases 3 with one Append for both
    // batches; that Append covers the quorum, so 2 stays held, now owing 4..=5.
    let effects = batch(&mut leader, 3, 2);
    let three = sent(&effects, 3).unwrap();
    assert_eq!(range(&three), (1, vec![2, 3, 4, 5]));
    assert!(sent(&effects, 2).is_none());
    assert_eq!(leader.held, BTreeMap::from([(node(2), 5)]));
    reply(&mut leader, &three);
    assert_eq!(leader.state().commit_index, 5);
    // The third batch releases 2 with both of its batches and the commit.
    let effects = batch(&mut leader, 5, 2);
    let two = sent(&effects, 2).unwrap();
    assert_eq!(range(&two), (3, vec![4, 5, 6, 7]));
    let Rpc::Append { leader_commit, .. } = two.rpc else {
        unreachable!()
    };
    assert_eq!(leader_commit, 5);
    assert!(sent(&effects, 3).is_none());
}

#[test]
fn commit_covering_a_configuration_entry_is_never_held() {
    let mut leader = held(AppendHold::QuorumFirst);
    let change = ConfigurationChange::Learners(configuration(2, &[1, 2, 3], &[4]));
    let effects = accept(&mut leader, 100, change);
    let effects = run(&mut leader, effects);
    let two = sent(&effects, 2).unwrap();
    let effects = reply(&mut leader, &two);
    assert_eq!(leader.state().commit_index, 2);
    assert!(leader.progress_sends_enabled());
    assert!(leader.configuration_notice);
    let notice = sent(&effects, 2).expect("configuration commit notice at once");
    assert_eq!(range(&notice), (2, vec![]));
    assert!(leader.held.is_empty());
    // The heartbeat tells every peer; later commit notices may be held again.
    step(&mut leader, Event::Heartbeat);
    assert!(!leader.configuration_notice);
}

#[test]
fn heartbeat_and_release_event_each_end_a_hold() {
    let mut leader = confirmed(AppendHold::QuorumFirst);
    first_batch_holds_three(&mut leader);
    let effects = step(&mut leader, Event::ReleaseHeldAppends);
    assert_eq!(effects.len(), 1);
    assert_eq!(range(&sent(&effects, 3).unwrap()), (1, vec![2, 3]));
    assert!(!leader.has_held_appends());
    assert!(step(&mut leader, Event::ReleaseHeldAppends).is_empty());

    let mut leader = confirmed(AppendHold::QuorumFirst);
    let two = first_batch_holds_three(&mut leader);
    let effects = step(&mut leader, Event::Heartbeat);
    assert_eq!(range(&sent(&effects, 3).unwrap()), (1, vec![2, 3]));
    // The heartbeat retries 2's outstanding request with its original context.
    assert_eq!(sent(&effects, 2).unwrap().context, two.context);
    assert!(leader.held.is_empty());
}

#[test]
fn lagging_follower_is_never_held() {
    let mut leader = held(AppendHold::Off);
    let effects = batch(&mut leader, 1, 2);
    let two = sent(&effects, 2).unwrap();
    let three = sent(&effects, 3).unwrap();
    let effects = reply(&mut leader, &two);
    let notice = sent(&effects, 2).unwrap();
    reply(&mut leader, &notice);
    // Two more batches while 3 still owes its first acknowledgement.
    for first in [3, 5] {
        let effects = batch(&mut leader, first, 2);
        let two = sent(&effects, 2).unwrap();
        let effects = reply(&mut leader, &two);
        if let Some(notice) = sent(&effects, 2) {
            reply(&mut leader, &notice);
        }
    }
    assert_eq!(leader.state().commit_index, 7);
    leader.set_append_hold(AppendHold::QuorumFirst);
    // 3 holds 1..=3 but the latest batch starts at 6: it is behind, so its
    // catch-up Append goes at once even though 2 alone covers the quorum.
    let effects = reply(&mut leader, &three);
    assert_eq!(range(&sent(&effects, 3).unwrap()), (3, vec![4, 5, 6, 7]));
    assert!(leader.held.is_empty());
}

#[test]
fn step_down_clears_holds_and_a_release_then_emits_nothing() {
    let mut leader = confirmed(AppendHold::QuorumFirst);
    first_batch_holds_three(&mut leader);
    let sender = StoreBinding {
        identity: leader.membership().replica_store(node(2)).unwrap(),
        session: StoreSession::new(1).unwrap(),
    };
    let newer = Message {
        group: leader.state().bootstrap.group,
        configuration: leader.membership().id(),
        from: node(2),
        sender,
        to: node(1),
        term: 2,
        context: RequestContext {
            origin: sender,
            sequence: 1,
        },
        rpc: Rpc::Append {
            previous_index: 3,
            previous_term: 1,
            entries: EntryBatch::default(),
            leader_commit: 1,
        },
    };
    step(&mut leader, Event::Receive(newer));
    assert_eq!(leader.role(), Role::Follower);
    assert!(leader.held.is_empty());
    assert!(!leader.has_held_appends());
    assert!(step(&mut leader, Event::ReleaseHeldAppends).is_empty());
}

#[test]
fn transfer_start_releases_holds_through_its_broadcast() {
    let mut leader = confirmed(AppendHold::QuorumFirst);
    first_batch_holds_three(&mut leader);
    let transfer = Event::TransferLeadership(LeadershipTransferRequest {
        operation: OperationId::new(800).unwrap(),
        target: PeerIdentity {
            node: node(2),
            store: leader.membership().voter_store(node(2)).unwrap(),
        },
        configuration: leader.membership().id(),
    });
    let effects = step(&mut leader, transfer);
    assert_eq!(range(&sent(&effects, 3).unwrap()), (1, vec![2, 3]));
    assert!(leader.held.is_empty());
    // No hold while the transfer runs: a new batch goes to every ready voter.
    let three = sent(&effects, 3).unwrap();
    reply(&mut leader, &three);
    assert!(!leader.progress_sends_enabled());
    assert!(!leader.hold_append(node(3)));
}

#[test]
fn membership_change_clears_holds_and_keeps_todays_broadcast() {
    let change = || ConfigurationChange::Learners(configuration(2, &[1, 2, 3], &[4]));
    let mut off = confirmed(AppendHold::Off);
    let effects = batch(&mut off, 1, 2);
    let two = sent(&effects, 2).unwrap();
    reply(&mut off, &two);
    let effects = accept(&mut off, 100, change());
    let expected = run(&mut off, effects);

    let mut leader = confirmed(AppendHold::QuorumFirst);
    let two = first_batch_holds_three(&mut leader);
    reply(&mut leader, &two);
    let effects = accept(&mut leader, 100, change());
    let effects = run(&mut leader, effects);
    assert!(leader.held.is_empty());
    // Exactly today's sends: 3 was held here and in flight there, and the
    // configuration change replaces both with the same fresh request.
    let appends = |effects: &[Effect]| {
        effects
            .iter()
            .filter_map(|e| match e {
                Effect::Send(m) => Some((m.to, range(m))),
                _ => None,
            })
            .collect::<Vec<_>>()
    };
    assert_eq!(appends(&effects), appends(&expected));
    assert!(sent(&effects, 3).is_some());
    // The uncommitted configuration turns holds off until it commits.
    assert!(!leader.hold_append(node(3)));
    committed_fixture(&mut leader, 4);
    assert!(leader.progress_sends_enabled());
}

#[test]
fn compaction_releases_holds_and_a_snapshot_peer_is_never_held() {
    let mut leader = confirmed(AppendHold::QuorumFirst);
    let two = first_batch_holds_three(&mut leader);
    reply(&mut leader, &two);
    let reference = SnapshotRef {
        store: leader.binding.identity,
        group: leader.state().bootstrap.group,
        configuration: leader.membership().id(),
        generation: SnapshotGeneration::new(1).unwrap(),
        index: 3,
        term: 1,
        application_schema: 1,
        file_bytes: 128,
        checksum: 7,
    };
    let effects = leader.begin_compact(reference).unwrap();
    let effects = run(&mut leader, effects);
    assert!(leader.held.is_empty());
    // 3 is now behind the compacted base: snapshot catch-up, never held.
    assert!(effects.iter().any(|effect| matches!(effect,
        Effect::SnapshotRequired { to, .. } if *to == node(3))));
    leader.retire_replication(node(3));
    assert!(leader.next_index[&node(3)] <= leader.state().base_index());
    assert!(!leader.hold_append(node(3)));
}

#[test]
fn every_batch_commits_on_its_first_follower_ack_as_with_holds_off() {
    for hold in [AppendHold::Off, AppendHold::QuorumFirst] {
        let mut leader = confirmed(hold);
        let mut outstanding: Vec<Message> = Vec::new();
        for (round, first) in (1..=11).step_by(2).enumerate() {
            let tip = leader.state().last_index() + 2;
            let effects = batch(&mut leader, first, 2);
            outstanding.extend((2..=3).filter_map(|peer| sent(&effects, peer)));
            // The batch is sent at once to a follower that reaches the tip...
            let quorum = outstanding
                .iter()
                .position(|m| range(m).1.last() == Some(&tip))
                .unwrap_or_else(|| panic!("{hold:?} round {round}: no immediate Append"));
            let quorum = outstanding.remove(quorum);
            // ...and that one acknowledgement commits it, in the same step.
            let effects = reply(&mut leader, &quorum);
            assert_eq!(leader.state().commit_index, tip, "{hold:?} round {round}");
            outstanding.extend(sent(&effects, quorum.to.get()));
            // Settle every other outstanding request before the next batch.
            while let Some(request) = outstanding.pop() {
                let effects = reply(&mut leader, &request);
                outstanding.extend((2..=3).filter_map(|peer| sent(&effects, peer)));
            }
        }
        assert_eq!(leader.state().commit_index, 13);
    }
}

fn early_leader() -> Raft {
    confirmed(AppendHold::QuorumFirst)
}
/// Propose a batch and bind its exact ticket; the pre-barrier sends follow.
fn early_batch(leader: &mut Raft, first: u128, count: u128) -> (LogTicket, Vec<Effect>) {
    leader.propose_batch(proposals(first, count)).unwrap();
    let next = leader.pending.as_ref().unwrap().next.metadata();
    let ticket = LogTicket {
        binding: leader.binding,
        batch: leader.last_batch + 1,
        group: next.group,
        revision: next.revision,
        generation: next.generation,
        last_index: next.last_index,
        term: next.hard_state.term,
    };
    let early = leader.replicate_admitted(ticket).unwrap();
    (ticket, early)
}
fn finish(leader: &mut Raft, ticket: LogTicket) -> Vec<Effect> {
    let effects = leader
        .complete(&DurableLog {
            tickets: vec![ticket],
        })
        .unwrap();
    run(leader, effects)
}

#[test]
fn early_path_sends_only_to_the_quorum_follower_before_the_barrier() {
    let mut off = early_leader();
    off.set_append_hold(AppendHold::Off);
    let (_, early) = early_batch(&mut off, 1, 2);
    assert_eq!(early.len(), 2, "holds off: every caught-up follower");

    let mut leader = early_leader();
    let (ticket, early) = early_batch(&mut leader, 1, 2);
    assert_eq!(early.len(), 1);
    let two = sent(&early, 2).expect("most recent ack first");
    assert_eq!(range(&two), (1, vec![2, 3]));
    // Durable: 3 is ready and caught up, and 2's request covers the batch.
    let effects = finish(&mut leader, ticket);
    assert!(sent(&effects, 3).is_none());
    assert_eq!(leader.held, BTreeMap::from([(node(3), 3)]));
    reply(&mut leader, &two);
    assert_eq!(leader.state().commit_index, 3);
}

#[test]
fn a_held_follower_due_at_the_batch_counts_toward_the_early_quorum() {
    let mut leader = early_leader();
    let (ticket, early) = early_batch(&mut leader, 1, 2);
    let two = sent(&early, 2).unwrap();
    finish(&mut leader, ticket);
    reply(&mut leader, &two);
    // Everything is committed, 2's commit notice and 3's entries are held.
    assert_eq!(leader.state().commit_index, leader.state().last_index());
    assert_eq!(leader.held, BTreeMap::from([(node(2), 3), (node(3), 3)]));
    // 3 is released with this batch once it is durable, so nothing goes early.
    let (ticket, early) = early_batch(&mut leader, 3, 2);
    assert!(early.is_empty());
    let effects = finish(&mut leader, ticket);
    assert_eq!(range(&sent(&effects, 3).unwrap()), (1, vec![2, 3, 4, 5]));
    assert!(sent(&effects, 2).is_none());
    assert_eq!(leader.held, BTreeMap::from([(node(2), 5)]));
}

#[test]
fn sol_review_new_leader_unknown_prefix_is_catchup_not_holdable() {
    let mut leader = held(AppendHold::QuorumFirst);
    assert_eq!(leader.progress[&node(3)], 0);
    assert_eq!(leader.next_index[&node(3)], 2);
    let effects = leader.become_leader().unwrap();
    let effects = run(&mut leader, effects);
    assert!(sent(&effects, 2).is_some());
    assert!(
        sent(&effects, 3).is_some(),
        "probe unconfirmed prefixes eagerly"
    );
    assert!(!leader.has_held_appends());
}

/// A group with an empty log whose voter 1 won term 1 and has not yet
/// appended its no-op: `batch_floor` and every follower's progress are 0.
fn fresh_leader(hold: AppendHold) -> Raft {
    let initial = configuration(1, &[1, 2, 3], &[]);
    let bootstrap = Bootstrap {
        group: GroupIdentity {
            id: GroupId::new(1).unwrap(),
            incarnation: GroupIncarnation::new(1).unwrap(),
        },
        configuration: initial.id(),
        policy: initial.policy().clone(),
        voter_stores: initial.voter_stores().clone(),
    };
    let mut states = BTreeMap::new();
    apply_batch(
        &mut states,
        &[LogMutation::Create(bootstrap.clone())],
        LogLimits::default(),
    )
    .unwrap();
    let revision = states[&bootstrap.group].revision;
    let vote = LogUpdate {
        group: bootstrap.group,
        expected_revision: revision,
        hard_state: HardState {
            term: 1,
            voted_for: Some(node(1)),
        },
        commit_index: 0,
        suffix: None,
        snapshot: None,
        snapshot_membership: None,
    };
    apply_batch(
        &mut states,
        &[LogMutation::Update(vote)],
        LogLimits::default(),
    )
    .unwrap();
    let binding = StoreBinding {
        identity: store(1),
        session: StoreSession::new(1).unwrap(),
    };
    let mut leader = Raft::recover(
        node(1),
        binding,
        states.remove(&bootstrap.group).unwrap(),
        LogLimits::default(),
    )
    .unwrap();
    leader.set_append_hold(hold);
    assert_eq!(leader.state().last_index(), 0);
    leader
}

#[test]
fn fresh_group_no_op_reaches_every_follower_at_once() {
    // Round 3: in an empty log `batch_floor` is 0, and so is the progress of
    // a follower that has acknowledged nothing. That 0 is no evidence: the
    // no-op goes to both followers, as after any election, and so the first
    // batch's pre-barrier send is not displaced by a held no-op.
    let mut leader = fresh_leader(AppendHold::QuorumFirst);
    let effects = leader.become_leader().unwrap();
    let effects = run(&mut leader, effects);
    assert_eq!(leader.state().last_index(), 1);
    assert_eq!(range(&sent(&effects, 2).unwrap()), (0, vec![1]));
    assert_eq!(range(&sent(&effects, 3).unwrap()), (0, vec![1]));
    assert!(!leader.has_held_appends());
}

#[test]
fn unacknowledged_follower_gets_its_first_batch_and_commit_notice_at_once() {
    // No acknowledgement yet: the cursors are optimistic, so nothing is held,
    // neither the first data Append nor the commit notice after it.
    let mut leader = held(AppendHold::QuorumFirst);
    let effects = batch(&mut leader, 1, 2);
    let two = sent(&effects, 2).unwrap();
    assert_eq!(range(&sent(&effects, 3).unwrap()), (1, vec![2, 3]));
    assert!(leader.held.is_empty());
    reply(&mut leader, &two);
    assert_eq!(leader.state().commit_index, 3);
    // 2 acknowledged everything: its commit notice may now be held.
    assert_eq!(leader.held, BTreeMap::from([(node(2), 3)]));
}

#[test]
fn quorum_follower_rejection_releases_the_held_follower_at_once() {
    let mut leader = confirmed(AppendHold::QuorumFirst);
    let two = first_batch_holds_three(&mut leader);
    let event = nack(&leader, &two, 1);
    let effects = step(&mut leader, event);
    // 3 is caught up: it gets the batch in this step, and its ack commits it.
    let three = sent(&effects, 3).expect("the held follower is released");
    assert_eq!(range(&three), (1, vec![2, 3]));
    assert!(sent(&effects, 2).is_some(), "2 is retried as before");
    assert!(leader.held.is_empty());
    reply(&mut leader, &three);
    assert_eq!(leader.state().commit_index, 3);
}

#[test]
fn a_probe_that_may_be_rejected_does_not_cover_a_hold() {
    // A new leader over entries 1..=3: its no-op (4) probes both followers,
    // and the next batch (5..=6) waits behind both outstanding probes.
    let mut leader = held(AppendHold::QuorumFirst);
    batch(&mut leader, 1, 2);
    let effects = leader.become_leader().unwrap();
    let effects = run(&mut leader, effects);
    let two = sent(&effects, 2).unwrap();
    let three = sent(&effects, 3).unwrap();
    assert_eq!(range(&three), (3, vec![4]));
    assert!(batch(&mut leader, 3, 2).is_empty());
    // 2 rejects: its rewound Append reaches the tip but extends no
    // acknowledged prefix (2 acknowledged nothing), so it may be rejected again.
    let event = nack(&leader, &two, 3);
    let effects = step(&mut leader, event);
    assert_eq!(range(&sent(&effects, 2).unwrap()), (2, vec![3, 4, 5, 6]));
    assert_eq!(leader.progress[&node(2)], 0);
    // 3's ack leaves it one batch behind with its prefix confirmed. 2's probe
    // does not cover batch 5..=6 for it, so 3 gets that batch at once.
    let effects = reply(&mut leader, &three);
    assert_eq!(range(&sent(&effects, 3).unwrap()), (4, vec![5, 6]));
    assert!(leader.held.is_empty());
}

#[test]
fn an_optimistic_cursor_alone_never_justifies_a_hold() {
    // Before the new leader's no-op is durable, 3's cursor is past the tip
    // but 3 acknowledged nothing: its commit notice is not holdable.
    let mut leader = held(AppendHold::QuorumFirst);
    let effects = leader.become_leader().unwrap();
    assert_eq!(leader.next_index[&node(3)], leader.state().last_index() + 1);
    assert!(!leader.hold_append(node(3)));
    let effects = run(&mut leader, effects);
    // 2 acknowledges the no-op and then carries batch 2..=3 alone.
    reply(&mut leader, &sent(&effects, 2).unwrap());
    let effects = batch(&mut leader, 1, 2);
    reply(&mut leader, &sent(&effects, 2).unwrap());
    assert_eq!(leader.state().commit_index, 4);
    // Without 3's outstanding probe, and with a cursor inside the latest
    // batch, 3's next data Append is fully covered by 2, yet 3 acknowledged
    // nothing below the batch: it is not held. (The core only sets such a
    // cursor at a new leader's no-op, where nothing confirmed can cover it, so
    // this pins the rule directly.)
    leader.retire_replication(node(3));
    leader
        .next_index
        .insert(node(3), leader.state().last_index());
    assert_eq!(leader.progress[&node(3)], 0);
    assert!(!leader.hold_append(node(3)));
    leader
        .progress
        .insert(node(3), leader.state().last_index() - 1);
    assert!(leader.hold_append(node(3)), "acknowledged prefix: holdable");
}
