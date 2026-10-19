// SPDX-License-Identifier: RPL-1.5
// Copyright (c) 2026 Karl Livesey
// Unless explicitly acquired and licensed from Licensor under another license,
// the contents of this file are subject to the Reciprocal Public License
// ("RPL") Version 1.5, or subsequent versions as allowed by the RPL, and You may
// not copy or use this file in either source code or executable form, except
// in compliance with the terms and conditions of the RPL.
//
// All software distributed under the RPL is provided strictly on an "AS IS"
// basis, WITHOUT WARRANTY OF ANY KIND, EITHER EXPRESS OR IMPLIED, AND LICENSOR
// HEREBY DISCLAIMS ALL SUCH WARRANTIES, INCLUDING WITHOUT LIMITATION, ANY
// WARRANTIES OF MERCHANTABILITY, FITNESS FOR A PARTICULAR PURPOSE, QUIET
// ENJOYMENT, OR NON-INFRINGEMENT. See the RPL for specific language governing
// rights and limitations under the RPL.
//! ST2 round 3 (I1, I3): three replicas at the retained-entry capacity, in
//! exact rounds. Every persistence completes at once (fixture tokens, no disk);
//! messages are delivered in order unless a link is cut; a non-leader whose
//! election-reset sequence has not changed for `ELECTION_ROUNDS` plus its
//! node number heartbeat rounds campaigns, as the runtime's randomized
//! election timer would. The harness
//! checkpoints a replica with the ordinary threshold (half the data budget
//! applied beyond its base), as the default scan does.
use super::*;
use crate::{membership::*, quorum::*, snapshot::SnapshotRef};
use std::collections::VecDeque;

const ELECTION_ROUNDS: u64 = 3;
/// Bound on heartbeat rounds for a stuck group to commit a new write.
const PROGRESS_ROUNDS: u64 = 40;

fn node(n: u64) -> NodeId {
    NodeId::new(n).unwrap()
}
fn store(n: u64) -> StoreIdentity {
    StoreIdentity {
        id: StoreId::new(n as u128).unwrap(),
        incarnation: StoreIncarnation::new(1).unwrap(),
    }
}
fn bootstrap() -> Bootstrap {
    let configuration = Configuration::new(
        ConfigurationId::new(1).unwrap(),
        Policy::new(
            Tree::Majority((1..=3).map(|n| Tree::Voter(node(n))).collect()),
            Limits::default(),
        )
        .unwrap(),
        (1..=3).map(|n| (node(n), store(n))).collect(),
        BTreeMap::new(),
    )
    .unwrap();
    Bootstrap {
        group: GroupIdentity {
            id: GroupId::new(1).unwrap(),
            incarnation: GroupIncarnation::new(1).unwrap(),
        },
        configuration: configuration.id(),
        policy: configuration.policy().clone(),
        voter_stores: configuration.voter_stores().clone(),
    }
}
fn command(index: u64, term: u64) -> LogEntry {
    LogEntry {
        index,
        term,
        payload: EntryPayload::Command {
            operation: OperationId::new(u128::from(index)).unwrap(),
            bytes: vec![1].into(),
        },
    }
}
/// A recovered replica holding `entries` term-1 entries (the first a noop)
/// with nothing committed, in hard-state term 1.
fn recovered(n: u64, entries: u64) -> Raft {
    let limits = LogLimits::default();
    let bootstrap = bootstrap();
    let mut states = BTreeMap::new();
    apply_batch(
        &mut states,
        &[LogMutation::Create(bootstrap.clone())],
        limits,
    )
    .unwrap();
    if entries > 0 {
        let log = (1..=entries)
            .map(|index| {
                if index == 1 {
                    LogEntry {
                        index,
                        term: 1,
                        payload: EntryPayload::Noop,
                    }
                } else {
                    command(index, 1)
                }
            })
            .collect();
        let state = &states[&bootstrap.group];
        let update = LogUpdate {
            group: bootstrap.group,
            expected_revision: state.revision,
            hard_state: HardState {
                term: 1,
                voted_for: None,
            },
            commit_index: 0,
            suffix: Some(Suffix {
                from: 1,
                entries: log,
            }),
            snapshot: None,
            snapshot_membership: None,
        };
        apply_batch(&mut states, &[LogMutation::Update(update)], limits).unwrap();
    }
    let binding = StoreBinding {
        identity: store(n),
        session: StoreSession::new(1).unwrap(),
    };
    Raft::recover(
        node(n),
        binding,
        states.remove(&bootstrap.group).unwrap(),
        limits,
    )
    .unwrap()
}
/// Complete one staged transition as durable (fixture token, no disk).
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

#[derive(Default)]
struct Trace {
    snapshot_sends: u64,
    step_errors: Vec<(u64, RaftError)>,
}
struct Cluster {
    cores: BTreeMap<u64, Raft>,
    queue: VecDeque<Message>,
    cut: BTreeSet<(u64, u64)>,
    applied: BTreeMap<u64, u64>,
    committed_ops: BTreeMap<u64, BTreeSet<u128>>,
    quiet: BTreeMap<u64, (u64, u64)>,
    trace: Trace,
}
impl Cluster {
    fn new(cores: Vec<Raft>) -> Self {
        let cores: BTreeMap<_, _> = cores.into_iter().map(|c| (c.node.get(), c)).collect();
        let applied = cores.keys().map(|&n| (n, 0)).collect();
        Self {
            cores,
            queue: VecDeque::new(),
            cut: BTreeSet::new(),
            applied,
            committed_ops: BTreeMap::new(),
            quiet: BTreeMap::new(),
            trace: Trace::default(),
        }
    }
    fn isolate(&mut self, n: u64) {
        for other in self
            .cores
            .keys()
            .copied()
            .filter(|&o| o != n)
            .collect::<Vec<_>>()
        {
            self.cut.insert((n, other));
            self.cut.insert((other, n));
        }
    }
    fn step(&mut self, n: u64, event: Event) -> Result<(), RaftError> {
        let effects = self.cores.get_mut(&n).unwrap().step(event)?;
        self.effects(n, effects);
        Ok(())
    }
    fn effects(&mut self, n: u64, effects: Vec<Effect>) {
        let mut effects = VecDeque::from(effects);
        while let Some(effect) = effects.pop_front() {
            match effect {
                Effect::Send(m) => self.queue.push_back(m),
                Effect::Persist(update) => {
                    let core = self.cores.get_mut(&n).unwrap();
                    effects.extend(complete(core, &update));
                }
                Effect::Committed(entries) => {
                    for e in entries {
                        assert_eq!(e.index, self.applied[&n] + 1, "ordered apply on {n}");
                        self.applied.insert(n, e.index);
                        if let EntryPayload::Command { operation, .. } = e.payload {
                            self.committed_ops
                                .entry(n)
                                .or_default()
                                .insert(operation.get());
                        }
                    }
                }
                Effect::SnapshotRequired { .. } => self.trace.snapshot_sends += 1,
                Effect::ReadReady(_) | Effect::CheckpointRequired { .. } => {}
                other => panic!("unexpected effect {other:?}"),
            }
        }
    }
    fn pump(&mut self) {
        let mut count = 0;
        while let Some(m) = self.queue.pop_front() {
            count += 1;
            assert!(count < 1_000_000, "non-quiescent");
            let (from, to) = (m.from.get(), m.to.get());
            if self.cut.contains(&(from, to)) {
                continue;
            }
            if let Err(e) = self.step(to, Event::Receive(m)) {
                self.trace.step_errors.push((to, e));
            }
        }
    }
    fn leader(&self) -> Option<u64> {
        self.cores
            .iter()
            .filter(|(_, c)| c.role() == Role::Leader)
            .max_by_key(|(_, c)| c.state().hard_state.term)
            .map(|(&n, _)| n)
    }
    /// One heartbeat round: leaders heartbeat, quiet followers campaign,
    /// messages drain, then due checkpoints run.
    fn round(&mut self) {
        let ids: Vec<u64> = self.cores.keys().copied().collect();
        for &n in &ids {
            let core = &self.cores[&n];
            if core.role() == Role::Leader {
                let _ = self.step(n, Event::Heartbeat);
                continue;
            }
            let reset = core.election_reset_sequence();
            let quiet = self.quiet.entry(n).or_insert((reset, 0));
            if quiet.0 != reset {
                *quiet = (reset, 0);
            }
            quiet.1 += 1;
            // Staggered per node, as randomized election timeouts would be.
            if quiet.1 >= ELECTION_ROUNDS + n {
                *quiet = (reset, 0);
                if let Err(e) = self.step(n, Event::Campaign) {
                    self.trace.step_errors.push((n, e));
                }
            }
        }
        self.pump();
        for &n in &ids {
            self.checkpoint(n);
        }
        self.pump();
    }
    /// The default scan's ordinary rule: checkpoint once half the data budget
    /// is applied beyond the base, at the applied index.
    fn checkpoint(&mut self, n: u64) {
        let core = &self.cores[&n];
        let applied = self.applied[&n].min(core.state().commit_index);
        let base = core.state().base_index();
        if applied <= base
            || applied - base < core.limits.max_entries_per_group as u64 / 2
            || core.has_pending_dependency()
        {
            return;
        }
        let reference = SnapshotRef {
            store: core.binding.identity,
            group: core.state().bootstrap.group,
            generation: SnapshotGeneration::new(applied).unwrap(),
            configuration: core.membership().id(),
            index: applied,
            term: core.state().term_at(applied).unwrap(),
            application_schema: 1,
            file_bytes: 64,
            checksum: 0,
        };
        let effects = self
            .cores
            .get_mut(&n)
            .unwrap()
            .begin_compact(reference)
            .unwrap();
        self.effects(n, effects);
    }
    /// Propose `op` on the current leader each round until it commits on a
    /// majority, for at most `PROGRESS_ROUNDS` rounds; the rounds used.
    fn commit_new_write(&mut self, op: u128) -> Result<u64, String> {
        let mut last_error = None;
        for round in 1..=PROGRESS_ROUNDS {
            if let Some(leader) = self.leader() {
                let proposed = self.cores[&leader]
                    .state()
                    .entries
                    .iter()
                    .any(|e| matches!(&e.payload, EntryPayload::Command { operation, .. } if operation.get() == op));
                if !proposed {
                    if let Err(e) = self.step(
                        leader,
                        Event::Propose {
                            operation: OperationId::new(op).unwrap(),
                            bytes: vec![7],
                        },
                    ) {
                        last_error = Some(e);
                    }
                }
            }
            self.round();
            let committed = self
                .committed_ops
                .values()
                .filter(|ops| ops.contains(&op))
                .count();
            if committed >= 2 {
                return Ok(round);
            }
        }
        let summary: Vec<_> = self
            .cores
            .iter()
            .map(|(n, c)| {
                format!(
                    "node {n}: {:?} term {} last {} commit {} base {} noop_pending {}",
                    c.role(),
                    c.state().hard_state.term,
                    c.state().last_index(),
                    c.state().commit_index,
                    c.state().base_index(),
                    c.leader_noop_pending()
                )
            })
            .collect();
        Err(format!(
            "no commit of a new write after {PROGRESS_ROUNDS} heartbeat rounds; last proposal error {last_error:?}; {summary:?}"
        ))
    }
}

/// Codex P1-3: three replicas each hold 16,384 term-1 entries (the data
/// budget) with nothing committed; node 2 wins term 2. Its noop must go in
/// and commit the suffix, and a new write must commit.
#[test]
fn leader_elected_over_a_full_uncommitted_suffix_commits_new_writes() {
    let max = LogLimits::default().max_entries_per_group as u64;
    let mut cluster = Cluster::new((1..=3).map(|n| recovered(n, max)).collect());
    cluster.step(2, Event::Campaign).unwrap();
    cluster.pump();
    assert_eq!(cluster.leader(), Some(2));
    let rounds = cluster
        .commit_new_write(1 << 100)
        .unwrap_or_else(|e| panic!("{e}"));
    assert!(rounds <= PROGRESS_ROUNDS);
    assert_eq!(cluster.trace.snapshot_sends, 0);
    // Every old-term entry was committed, never discarded.
    for core in cluster.cores.values() {
        assert!(core.state().commit_index > max);
    }
}

/// Opus P1-1: the leader is cut off from both followers (which also cannot
/// reach each other), keeps admitting client writes to its data budget, the
/// partition heals, and the old leader (the most up-to-date log) wins a later
/// election. The group must commit a new write.
#[test]
fn partitioned_leader_that_filled_its_log_wins_again_and_the_group_progresses() {
    let max = LogLimits::default().max_entries_per_group;
    let mut cluster = Cluster::new((1..=3).map(|n| recovered(n, 0)).collect());
    cluster.step(1, Event::Campaign).unwrap();
    cluster.pump();
    assert_eq!(cluster.leader(), Some(1));
    cluster.round();
    assert_eq!(cluster.cores[&2].state().commit_index, 1);
    for n in 1..=3 {
        cluster.isolate(n);
    }
    // Client writes reach node 1 only, until its data budget refuses them.
    let mut op = 1u128 << 64;
    loop {
        let batch: Vec<_> = (0..64)
            .map(|k| Proposal {
                operation: OperationId::new(op + k).unwrap(),
                bytes: vec![1],
            })
            .collect();
        let core = cluster.cores.get_mut(&1).unwrap();
        match core.propose_batch(batch) {
            Ok(effects) => cluster.effects(1, effects),
            Err(_) => break,
        }
        op += 64;
    }
    // Then single writes, until the budget refuses exactly at the limit.
    while cluster
        .step(
            1,
            Event::Propose {
                operation: OperationId::new(op).unwrap(),
                bytes: vec![1],
            },
        )
        .is_ok()
    {
        op += 1;
    }
    assert_eq!(cluster.cores[&1].state().entries.len(), max);
    assert_eq!(cluster.cores[&1].state().commit_index, 1);
    // Heal node 1's link to node 2 only: node 2's campaign deposes node 1
    // (higher term) but cannot win. Then heal everything; node 1, with the
    // longest log, wins the next election.
    cluster.cut.remove(&(1, 2));
    cluster.cut.remove(&(2, 1));
    cluster.step(2, Event::Campaign).unwrap();
    cluster.pump();
    assert_eq!(cluster.leader(), None);
    cluster.cut.clear();
    cluster.step(1, Event::Campaign).unwrap();
    cluster.pump();
    assert_eq!(cluster.leader(), Some(1), "the old leader wins again");
    cluster.quiet.clear();
    let rounds = cluster
        .commit_new_write(1 << 101)
        .unwrap_or_else(|e| panic!("{e}"));
    assert!(rounds <= PROGRESS_ROUNDS);
}

/// Opus P2-1: a deposed leader whose divergent uncommitted suffix is what
/// keeps its node above the WAL high-water mark (modelled here: pressure
/// lasts while it holds any entry the leader does not). On an idle group it
/// must accept the append that truncates that suffix and then catch up,
/// without a snapshot.
#[test]
fn deposed_leader_under_pressure_truncates_its_suffix_and_catches_up_without_a_snapshot() {
    let mut cluster = Cluster::new((1..=3).map(|n| recovered(n, 0)).collect());
    cluster.step(1, Event::Campaign).unwrap();
    cluster.pump();
    cluster.round();
    cluster.isolate(1);
    for k in 0..100u128 {
        cluster
            .step(
                1,
                Event::Propose {
                    operation: OperationId::new((1 << 80) + k).unwrap(),
                    bytes: vec![1; 64],
                },
            )
            .unwrap();
    }
    assert_eq!(cluster.cores[&1].state().last_index(), 101);
    // Nodes 2 and 3 elect node 2 (term 2), which writes a few entries, then
    // node 3 (term 3). Node 3's first append to node 1 does not match, so it
    // probes back to index 1 and resends the whole range from the conflict:
    // the term-2 noop followed by data. The group then idles.
    cluster.cut.remove(&(2, 3));
    cluster.cut.remove(&(3, 2));
    cluster.step(2, Event::Campaign).unwrap();
    cluster.pump();
    assert_eq!(cluster.leader(), Some(2));
    for k in 0..5u128 {
        cluster
            .step(
                2,
                Event::Propose {
                    operation: OperationId::new((1 << 90) + k).unwrap(),
                    bytes: vec![2; 64],
                },
            )
            .unwrap();
    }
    cluster.round();
    assert_eq!(cluster.cores[&3].state().commit_index, 7);
    cluster.step(3, Event::Campaign).unwrap();
    cluster.pump();
    assert_eq!(cluster.leader(), Some(3));
    cluster.round();
    cluster.cut.clear();
    let divergent = |core: &Raft| core.state().term_at(2) == Some(1);
    let mut rounds = 0;
    loop {
        let pressure = divergent(&cluster.cores[&1]);
        cluster
            .cores
            .get_mut(&1)
            .unwrap()
            .set_storage_pressure(pressure);
        cluster.quiet.clear();
        let leader = &cluster.cores[&3];
        let (last, term) = (leader.state().last_index(), leader.state().last_term());
        let follower = &cluster.cores[&1];
        if follower.state().last_index() == last && follower.state().last_term() == term {
            break;
        }
        rounds += 1;
        assert!(
            rounds <= PROGRESS_ROUNDS,
            "deposed leader never caught up: last {} term {:?} vs leader {last}/{term}; divergent {}",
            follower.state().last_index(),
            follower.state().term_at(follower.state().last_index()),
            divergent(follower)
        );
        cluster.round();
    }
    assert_eq!(cluster.leader(), Some(3));
    assert_eq!(cluster.trace.snapshot_sends, 0);
}

/// I1 residual: a replica whose uncommitted log fills the whole retained
/// capacity (data budget and noop reserve; reachable only through 256 failed
/// leader terms) wins an election. No checkpoint can make room, so it steps
/// down with a typed reason and lets two election timeouts pass; a replica
/// with a shorter log wins, truncates the suffix and the group commits.
#[test]
fn leader_whose_uncommitted_log_fills_the_capacity_steps_down_and_another_wins() {
    let capacity = LogLimits::default().retained_entry_capacity() as u64;
    let mut cluster = Cluster::new(vec![
        recovered(1, capacity),
        recovered(2, 100),
        recovered(3, 100),
    ]);
    cluster.step(1, Event::Campaign).unwrap();
    cluster.pump();
    let core = &cluster.cores[&1];
    assert_eq!(core.role(), Role::Follower);
    assert_eq!(
        core.capacity_step_downs(),
        (
            1,
            Some(StepDown {
                term: 2,
                reason: StepDownReason::RetainedEntryCapacity
            })
        )
    );
    assert_eq!(cluster.leader(), None);
    let rounds = cluster
        .commit_new_write(1 << 102)
        .unwrap_or_else(|e| panic!("{e}"));
    assert!(rounds <= PROGRESS_ROUNDS);
    let leader = cluster.leader().unwrap();
    assert_ne!(leader, 1);
    // The full replica took the new leader's log; nothing committed was lost.
    let leader_last = cluster.cores[&leader].state().last_index();
    assert_eq!(cluster.cores[&1].state().last_index(), leader_last);
    assert_eq!(cluster.cores[&1].state().term_at(101), Some(3));
    assert_eq!(cluster.trace.snapshot_sends, 0);
}
