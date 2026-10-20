// SPDX-License-Identifier: RPL-1.5
// Copyright (c) 2026 Karl Livesey
//! Per-peer slots against the map form over leader histories: membership
//! changes mid-flight, retirement, a lagging follower and conflict truncation.
//! Debug builds also compare every slot operation with the old map and every
//! frontier with the old `Vec` form; these tests count those comparisons.
use super::*;
use crate::membership::ConfigurationChange;
use crate::quorum::tests::{checks as frontier_checks, reference};
use membership_tests::{accept, cid, configuration, core, node, store};
use peer_slots::tests::{checks as slot_checks, Rng};
use volatile_tests::volatile_cluster;

fn peers_of(core: &Raft) -> BTreeSet<NodeId> {
    core.membership()
        .replicas()
        .map(|(node, _)| node)
        .filter(|node| *node != core.node)
        .collect()
}

/// The commit frontier recomputed with the old `Vec` implementation.
fn map_frontier(core: &Raft) -> u64 {
    let membership = core.membership();
    let old = reference(membership.stable().policy(), &core.progress);
    membership.joint().map_or(old, |joint| {
        old.min(reference(joint.next.policy(), &core.progress))
    })
}

#[derive(Default)]
struct Stats {
    decisions: usize,
    quiescent: usize,
}

/// After every core call: no slot for a peer outside the current membership,
/// the cached peer order is the fresh one, and the commit decisions equal the
/// map form's (a planned commit is exactly its frontier, and none is missed).
fn check(core: &Raft, stats: &mut Stats) {
    // Keys are bounded by the installed membership (a pending configuration
    // replaces the slots when it installs, as it replaced the maps).
    let peers: BTreeSet<NodeId> = core
        .membership
        .replicas()
        .map(|(node, _)| node)
        .filter(|node| *node != core.node)
        .collect();
    let tables: [Vec<NodeId>; 3] = [
        core.requests.keys().collect(),
        core.admitted_ranges.keys().collect(),
        core.empty_append_aliases.keys().collect(),
    ];
    for keys in tables {
        assert!(
            keys.iter().all(|key| peers.contains(key)),
            "slot kept for a departed peer: {keys:?} not within {peers:?}"
        );
        assert!(keys.len() <= peers.len());
    }
    assert!(core.requests.len() <= peers.len());
    if core.peer_order_for == Some(core.membership().id()) {
        assert_eq!(core.peer_order, core.peers());
    }
    if core.role != Role::Leader {
        return;
    }
    let frontier = map_frontier(core).min(core.durable.last_index());
    let term = core.durable.hard_state.term;
    match &core.pending {
        Some(pending) if pending.next.metadata().commit_index > core.durable.commit_index => {
            assert_eq!(
                pending.next.metadata().commit_index,
                frontier,
                "commit decision"
            );
            stats.decisions += 1;
        }
        Some(_) => {}
        None => {
            assert!(
                frontier <= core.durable.commit_index
                    || core.durable.term_at(frontier) != Some(term),
                "the map form would commit {frontier} (commit {})",
                core.durable.commit_index
            );
            stats.quiescent += 1;
        }
    }
}

fn complete(core: &mut Raft, update: &LogUpdate) -> Vec<Effect> {
    core.validate_persist_effect(update).unwrap();
    let next = core.pending.as_ref().unwrap().next.metadata();
    let ticket = LogTicket {
        binding: core.binding,
        batch: core.last_batch + 1,
        group: next.group,
        revision: next.revision,
        generation: next.generation,
        last_index: next.last_index,
        term: next.hard_state.term,
    };
    core.admitted(ticket).unwrap();
    core.complete(&DurableLog {
        tickets: vec![ticket],
    })
    .unwrap()
}

/// One real durable leader; followers are modelled by their matched length.
/// Messages are delivered out of order and the lagging peer loses half of
/// what it is sent, so the leader retries, backs off and reorders.
struct Leader {
    core: Raft,
    wire: Vec<Message>,
    held: Vec<Message>,
    matched: BTreeMap<NodeId, u64>,
    lagging: NodeId,
    rng: Rng,
    next_operation: u128,
    stats: Stats,
}

impl Leader {
    fn new(seed: u64) -> Self {
        let core = core();
        let mut leader = Self {
            core,
            wire: Vec::new(),
            held: Vec::new(),
            matched: BTreeMap::new(),
            lagging: node(3),
            rng: Rng(seed),
            next_operation: 1,
            stats: Stats::default(),
        };
        check(&leader.core, &mut leader.stats);
        leader
    }
    fn call(&mut self, call: impl FnOnce(&mut Raft) -> Vec<Effect>) {
        let effects = call(&mut self.core);
        check(&self.core, &mut self.stats);
        let mut queue: VecDeque<Effect> = effects.into();
        while let Some(effect) = queue.pop_front() {
            match effect {
                Effect::Persist(update) => {
                    let following = complete(&mut self.core, &update);
                    check(&self.core, &mut self.stats);
                    queue.extend(following);
                }
                Effect::Send(message) => self.wire.push(message),
                _ => {}
            }
        }
    }
    fn reply(&mut self, message: Message) {
        let Rpc::Append {
            previous_index,
            entries,
            ..
        } = &message.rpc
        else {
            return;
        };
        let have = self.matched.entry(message.to).or_insert(1);
        let rpc = if *previous_index <= *have {
            let matching = previous_index + entries.len() as u64;
            *have = (*have).max(matching);
            Rpc::Appended {
                success: true,
                matching_index: matching,
            }
        } else {
            Rpc::Appended {
                success: false,
                matching_index: *have,
            }
        };
        let reply = Message {
            group: message.group,
            configuration: message.configuration,
            from: message.to,
            sender: StoreBinding {
                identity: store(message.to.get()),
                session: StoreSession::new(1).unwrap(),
            },
            to: message.from,
            term: message.term,
            context: message.context,
            rpc,
        };
        // A reply scoped to an older configuration is refused before any
        // state change; the host drops it.
        let stale = reply.configuration != self.core.membership().id();
        self.call(|core| match core.step(Event::Receive(reply)) {
            Err(RaftError::WrongIdentity) if stale => Vec::new(),
            result => result.unwrap(),
        });
    }
    fn deliver(&mut self) -> bool {
        if self.wire.is_empty() {
            return false;
        }
        let at = self.rng.below(self.wire.len() as u64) as usize;
        let message = self.wire.swap_remove(at);
        if message.to == self.lagging && self.rng.below(2) == 0 {
            return true;
        }
        self.reply(message);
        true
    }
    fn heartbeat(&mut self) {
        self.call(|core| core.step(Event::Heartbeat).unwrap());
    }
    fn propose(&mut self) {
        let operation = OperationId::new(self.next_operation).unwrap();
        self.next_operation += 1;
        self.call(|core| {
            core.step(Event::Propose {
                operation,
                bytes: vec![7],
            })
            .unwrap()
        });
    }
    fn run(&mut self, steps: usize) {
        for _ in 0..steps {
            match self.rng.below(10) {
                0 => self.heartbeat(),
                1 | 2 => self.propose(),
                _ => {
                    if !self.deliver() {
                        self.heartbeat();
                    }
                }
            }
        }
    }
    fn until_committed(&mut self, index: u64) {
        for _ in 0..10_000 {
            if self.core.durable.commit_index >= index {
                return;
            }
            if !self.deliver() {
                self.heartbeat();
            }
        }
        panic!("index {index} never committed");
    }
    /// Keep the next message to `peer` undelivered (an outstanding request).
    fn hold_next_to(&mut self, peer: NodeId) {
        self.heartbeat();
        let at = self.wire.iter().position(|m| m.to == peer).unwrap();
        self.held.push(self.wire.swap_remove(at));
    }
}

#[test]
fn slot_and_map_forms_decide_the_same_commits_through_joint_change_and_retirement() {
    let (slots_before, frontiers_before) = (slot_checks(), frontier_checks());
    let mut leader = Leader::new(0x5851_f42d_4c95_7f2d);
    leader.run(200);
    // Learners join while requests to 2 and 3 are outstanding.
    assert!(!leader.core.requests.is_empty());
    leader.call(|core| {
        accept(
            core,
            1_000,
            ConfigurationChange::Learners(configuration(2, &[1, 2, 3], &[4, 5])),
        )
    });
    assert_eq!(peers_of(&leader.core).len(), 4);
    let learners = leader.core.durable.last_index();
    leader.run(200);
    leader.until_committed(learners);
    // A joint change mid-flight: 4 becomes a voter, the leader a learner.
    leader.run(50);
    assert!(!leader.core.requests.is_empty());
    leader.call(|core| {
        accept(
            core,
            1_001,
            ConfigurationChange::Joint {
                id: cid(3),
                next: configuration(4, &[2, 3, 4], &[1, 5]),
            },
        )
    });
    let joint = leader.core.durable.last_index();
    assert!(leader.core.membership().joint().is_some());
    leader.run(200);
    leader.until_committed(joint);
    leader.run(50);
    leader.call(|core| accept(core, 1_001, ConfigurationChange::Final { id: cid(4) }));
    let last = leader.core.durable.last_index();
    leader.until_committed(last);
    // Retired: no longer a voter, so it steps down and keeps no slot.
    assert_eq!(leader.core.role(), Role::Follower);
    assert!(leader.core.requests.keys().next().is_none());
    assert!(leader.core.progress.is_empty());
    assert!(
        leader.stats.decisions >= 30 && leader.stats.quiescent >= 500,
        "decisions {} quiescent {}",
        leader.stats.decisions,
        leader.stats.quiescent
    );
    eprintln!(
        "decisions {} quiescent {} slot checks {} frontier checks {}",
        leader.stats.decisions,
        leader.stats.quiescent,
        slot_checks() - slots_before,
        frontier_checks() - frontiers_before
    );
    if cfg!(debug_assertions) {
        // Every slot operation and frontier ran beside its map form.
        assert!(slot_checks() - slots_before >= 500);
        assert!(frontier_checks() - frontiers_before >= 200);
    }
}

#[test]
fn no_slot_is_retained_for_a_departed_peer() {
    let mut leader = Leader::new(0x2f69_3c4a_1b7e_9d53);
    leader.call(|core| {
        accept(
            core,
            1_000,
            ConfigurationChange::Learners(configuration(2, &[1, 2, 3], &[4, 5])),
        )
    });
    let learners = leader.core.durable.last_index();
    leader.until_committed(learners);
    leader.run(100);
    // An outstanding request to learner 5, then 5 leaves the membership.
    leader.hold_next_to(node(5));
    assert!(leader.core.requests.contains_key(&node(5)));
    leader.call(|core| {
        accept(
            core,
            1_001,
            ConfigurationChange::Learners(configuration(3, &[1, 2, 3], &[4])),
        )
    });
    assert!(!peers_of(&leader.core).contains(&node(5)));
    assert!(leader.core.requests.keys().all(|key| key != node(5)));
    // Its late reply changes nothing and recreates no slot.
    let late = leader.held.pop().unwrap();
    leader.reply(late);
    leader.wire.retain(|m| m.to != node(5));
    leader.run(300);
    leader.until_committed(leader.core.durable.last_index());
    for table in [
        leader.core.requests.keys().collect::<Vec<_>>(),
        leader.core.admitted_ranges.keys().collect(),
        leader.core.empty_append_aliases.keys().collect(),
    ] {
        assert!(table.len() <= 3 && !table.contains(&node(5)), "{table:?}");
    }
    assert!(!leader.core.progress.contains_key(&node(5)));
}

/// Deliver to quiescence on real volatile cores, checking every core after
/// every call. Sends from `silent` are lost.
fn settle(
    cluster: &mut [(Raft, VolatileLog)],
    slot: usize,
    effects: Vec<Effect>,
    silent: Option<usize>,
    stats: &mut Stats,
) {
    let mut inbox: Vec<VecDeque<Message>> = vec![VecDeque::new(); cluster.len()];
    let mut work = vec![(slot, effects)];
    loop {
        while let Some((slot, effects)) = work.pop() {
            let mut queue: VecDeque<Effect> = effects.into();
            while let Some(effect) = queue.pop_front() {
                match effect {
                    Effect::PersistVolatile(token) => {
                        let (core, log) = &mut cluster[slot];
                        let (_, following) =
                            persist_volatile(core, log, token).unwrap().into_parts();
                        check(core, stats);
                        queue.extend(following);
                    }
                    Effect::Send(message) if silent != Some(slot) => {
                        inbox[(message.to.get() - 1) as usize].push_back(message);
                    }
                    _ => {}
                }
            }
        }
        let Some(slot) = (0..cluster.len()).find(|slot| !inbox[*slot].is_empty()) else {
            return;
        };
        let message = inbox[slot].pop_front().unwrap();
        let effects = cluster[slot].0.step(Event::Receive(message)).unwrap();
        check(&cluster[slot].0, stats);
        work.push((slot, effects));
    }
}

fn propose(cluster: &mut [(Raft, VolatileLog)], slot: usize, operation: u128) -> Vec<Effect> {
    cluster[slot]
        .0
        .step(Event::Propose {
            operation: OperationId::new(operation).unwrap(),
            bytes: vec![operation as u8],
        })
        .unwrap()
}

#[test]
fn slots_follow_conflict_truncation_and_re_election_of_a_deposed_leader() {
    let mut cluster = volatile_cluster();
    let mut stats = Stats::default();
    for operation in 1..=3 {
        let effects = propose(&mut cluster, 0, operation);
        settle(&mut cluster, 0, effects, None, &mut stats);
    }
    // An entry only the old leader holds; its requests stay outstanding.
    let effects = propose(&mut cluster, 0, 4);
    settle(&mut cluster, 0, effects, Some(0), &mut stats);
    assert_eq!(cluster[0].0.requests.len(), 2);
    let orphan = cluster[0].0.state().last_index();
    // Node 2 is elected; its first append truncates the orphan on node 1,
    // whose slots were cleared when it stepped down.
    let effects = cluster[1].0.step(Event::Campaign).unwrap();
    settle(&mut cluster, 1, effects, None, &mut stats);
    assert_eq!(cluster[1].0.role(), Role::Leader);
    assert!(cluster[0].0.requests.keys().next().is_none());
    let effects = propose(&mut cluster, 1, 5);
    settle(&mut cluster, 1, effects, None, &mut stats);
    let term = cluster[1].0.state().hard_state.term;
    assert_eq!(cluster[0].0.state().term_at(orphan), Some(term));
    // The deposed leader is re-elected and replicates from fresh slots.
    let effects = cluster[0].0.step(Event::Campaign).unwrap();
    settle(&mut cluster, 0, effects, None, &mut stats);
    assert_eq!(cluster[0].0.role(), Role::Leader);
    for operation in 6..=8 {
        let effects = propose(&mut cluster, 0, operation);
        settle(&mut cluster, 0, effects, None, &mut stats);
    }
    let leader = &cluster[0].0;
    assert_eq!(leader.state().commit_index, leader.state().last_index());
    for (core, _) in &cluster[1..] {
        assert_eq!(core.state().entries, leader.state().entries);
        assert!(core.requests.keys().next().is_none());
    }
    assert!(stats.decisions >= 8, "decisions {}", stats.decisions);
}
