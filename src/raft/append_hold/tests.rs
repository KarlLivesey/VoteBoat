// SPDX-License-Identifier: RPL-1.5
// Copyright (c) 2026 Karl Livesey
//! Held follower Appends on the actual core, with explicit host-asserted
//! durable completions and no clock: each step is one named event.
use super::*;
use crate::membership::ConfigurationChange;
use crate::raft::membership_tests::{accept, committed_fixture, configuration, core, durable};

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
    let mut leader = held(AppendHold::QuorumFirst);
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
    let mut leader = held(AppendHold::QuorumFirst);
    first_batch_holds_three(&mut leader);
    let effects = step(&mut leader, Event::ReleaseHeldAppends);
    assert_eq!(effects.len(), 1);
    assert_eq!(range(&sent(&effects, 3).unwrap()), (1, vec![2, 3]));
    assert!(!leader.has_held_appends());
    assert!(step(&mut leader, Event::ReleaseHeldAppends).is_empty());

    let mut leader = held(AppendHold::QuorumFirst);
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
    let mut leader = held(AppendHold::QuorumFirst);
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
    let mut leader = held(AppendHold::QuorumFirst);
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
    let mut off = held(AppendHold::Off);
    let effects = batch(&mut off, 1, 2);
    let two = sent(&effects, 2).unwrap();
    reply(&mut off, &two);
    let effects = accept(&mut off, 100, change());
    let expected = run(&mut off, effects);

    let mut leader = held(AppendHold::QuorumFirst);
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
    let mut leader = held(AppendHold::QuorumFirst);
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
        let mut leader = held(hold);
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
    let mut leader = held(AppendHold::QuorumFirst);
    for id in 2..=3 {
        leader.progress.insert(node(id), 1);
    }
    leader
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
