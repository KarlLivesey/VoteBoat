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
//! U2 probe: can admission let a capacity error reach an already-committed
//! entry? A committed entry replays identically after restart, so an apply
//! error there is a permanent crash loop.
//!
//! Three raw `Raft` cores on in-memory `HostLogStore`s, a deterministic message
//! queue (no clock, no sockets, no threads), one application per node.
//! Admission is the client router's: the leader validates a proposal against
//! its applied application and the commands in its own unapplied log suffix.
//! Every `Committed` batch is applied; restarts restore the application from
//! its last checkpoint and replay the committed suffix. The oracle: apply never
//! returns `Err`, replicas produce equal receipts for each index, and a restored
//! application equals one replayed from the start.
//!
//! Reusable for any application: supply a constructor, the commands that fill
//! it to one below its capacity, and a generator of novel commands.
use super::support::{append, bootstrap, group, HostLogStore};
use std::collections::{BTreeMap, BTreeSet, VecDeque};
use std::fmt::Debug;
use voteboat::{
    application::{ApplicationError, CheckpointStateMachine, ProposalAdmission},
    identity::{NodeId, OperationId},
    log::{EntryPayload, LogMutation, LogStore},
    raft::{persist_effect, Effect, Event, Message, Raft, RaftError, Role},
};

pub type Command = (OperationId, Vec<u8>);

/// What one probe run must supply.
pub struct ProbeSetup<A> {
    pub make: Box<dyn Fn() -> A>,
    /// Commands that bring the application to one below its capacity.
    pub fill: Vec<Command>,
    /// The `n`th novel command (`n` from 0), distinct from every other.
    pub novel: Box<dyn Fn(u64) -> Command>,
    pub checkpoint_bytes: usize,
}

/// An apply error on a committed entry: the defect the probe looks for.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct ApplyFailure {
    pub node: u64,
    pub first_index: u64,
    pub error: ApplicationError,
}

#[derive(Clone, Debug, Default, Eq, PartialEq)]
pub struct ProbeStats {
    pub proposals: u64,
    pub admitted: u64,
    pub refused: u64,
    pub applied_entries: u64,
    pub leader_terms: BTreeSet<u64>,
    pub restarts: u64,
    pub checkpoints: u64,
}

struct ProbeNode<A> {
    core: Raft,
    store: HostLogStore,
    app: A,
    checkpoint: Option<(u64, Vec<u8>)>,
}

pub struct Probe<A: ProposalAdmission + CheckpointStateMachine>
where
    A::Receipt: voteboat::application::ApplicationReceipt,
{
    setup: ProbeSetup<A>,
    nodes: BTreeMap<u64, ProbeNode<A>>,
    messages: VecDeque<Message>,
    blocked: BTreeSet<(u64, u64)>,
    receipts: BTreeMap<u64, A::Receipt>,
    novel: u64,
    pub failures: Vec<ApplyFailure>,
    pub stats: ProbeStats,
}

impl<A> Probe<A>
where
    A: ProposalAdmission + CheckpointStateMachine,
    A::Receipt: voteboat::application::ApplicationReceipt + Clone + Debug + PartialEq,
{
    pub fn new(setup: ProbeSetup<A>) -> Self {
        let mut nodes = BTreeMap::new();
        for id in 1..=3u64 {
            let mut store = HostLogStore::new(u128::from(id));
            append(&mut store, vec![LogMutation::Create(bootstrap(1, 3))]);
            let core = Raft::recover(
                NodeId::new(id).unwrap(),
                store.binding(),
                store.state(group(1)).unwrap(),
                store.limits(),
            )
            .unwrap();
            let app = (setup.make)();
            nodes.insert(
                id,
                ProbeNode {
                    core,
                    store,
                    app,
                    checkpoint: None,
                },
            );
        }
        Self {
            setup,
            nodes,
            messages: VecDeque::new(),
            blocked: BTreeSet::new(),
            receipts: BTreeMap::new(),
            novel: 0,
            failures: Vec::new(),
            stats: ProbeStats::default(),
        }
    }

    pub fn leader(&self) -> Option<u64> {
        self.nodes
            .iter()
            .filter(|(_, n)| n.core.role() == Role::Leader)
            .max_by_key(|(_, n)| n.core.state().hard_state.term)
            .map(|(id, _)| *id)
    }

    fn step(&mut self, id: u64, event: Event) {
        match self.nodes.get_mut(&id).unwrap().core.step(event) {
            Ok(effects) => self.effects(id, effects),
            // Late responses to a restarted store session, and events a role
            // refuses, are ordinary in a fault schedule.
            Err(RaftError::WrongIdentity | RaftError::NotLeader | RaftError::Busy) => (),
            Err(error) => panic!("node {id}: {error:?}"),
        }
    }

    fn effects(&mut self, id: u64, effects: Vec<Effect>) {
        let mut effects = VecDeque::from(effects);
        while let Some(effect) = effects.pop_front() {
            match effect {
                Effect::Send(message) => self.messages.push_back(message),
                Effect::Persist(update) => {
                    let node = self.nodes.get_mut(&id).unwrap();
                    effects
                        .extend(persist_effect(&mut node.core, &mut node.store, update).unwrap());
                }
                Effect::Committed(entries) => self.apply(id, &entries),
                Effect::ReadReady(_) | Effect::PersistVolatile(_) => (),
                other => panic!("probe history has no compaction or snapshots: {other:?}"),
            }
        }
        if let Some(node) = self.nodes.get(&id) {
            if node.core.role() == Role::Leader {
                self.stats
                    .leader_terms
                    .insert(node.core.state().hard_state.term);
            }
        }
    }

    fn apply(&mut self, id: u64, entries: &[voteboat::log::LogEntry]) {
        let node = self.nodes.get_mut(&id).unwrap();
        let applied = node.app.applied_index();
        let fresh: Vec<_> = entries
            .iter()
            .filter(|e| e.index > applied)
            .cloned()
            .collect();
        if fresh.is_empty() {
            return;
        }
        match node.app.apply_batch(&fresh) {
            Ok(receipts) => {
                self.stats.applied_entries += fresh.len() as u64;
                for receipt in receipts {
                    use voteboat::application::ApplicationReceipt;
                    let index = receipt.index();
                    match self.receipts.get(&index) {
                        Some(first) => assert_eq!(first, &receipt, "replicas disagree at {index}"),
                        None => {
                            self.receipts.insert(index, receipt);
                        }
                    }
                }
            }
            Err(error) => self.failures.push(ApplyFailure {
                node: id,
                first_index: fresh[0].index,
                error,
            }),
        }
    }

    /// Router admission at `id`, then the proposal. Returns whether admitted.
    pub fn propose(&mut self, id: u64, (operation, bytes): Command) -> bool {
        self.stats.proposals += 1;
        let node = &self.nodes[&id];
        if node.core.role() != Role::Leader {
            return false;
        }
        let applied = node.app.applied_index();
        let pending = node
            .core
            .state()
            .entries
            .iter()
            .filter(|e| e.index > applied)
            .filter_map(|e| match &e.payload {
                EntryPayload::Command { operation, bytes } => Some((*operation, bytes.as_slice())),
                _ => None,
            });
        if node
            .app
            .validate_proposal(operation, &bytes, pending)
            .is_err()
        {
            self.stats.refused += 1;
            return false;
        }
        self.stats.admitted += 1;
        self.step(id, Event::Propose { operation, bytes });
        true
    }

    pub fn next_novel(&mut self) -> Command {
        let command = (self.setup.novel)(self.novel);
        self.novel += 1;
        command
    }

    pub fn campaign(&mut self, id: u64) {
        self.step(id, Event::Campaign);
    }
    pub fn heartbeat(&mut self, id: u64) {
        self.step(id, Event::Heartbeat);
    }

    /// Deliver the oldest deliverable message; blocked ones are dropped.
    pub fn deliver_one(&mut self) -> bool {
        while let Some(message) = self.messages.pop_front() {
            if self
                .blocked
                .contains(&(message.from.get(), message.to.get()))
            {
                continue;
            }
            let to = message.to.get();
            self.step(to, Event::Receive(message));
            return true;
        }
        false
    }
    pub fn duplicate_oldest(&mut self) {
        if let Some(message) = self.messages.front().cloned() {
            self.messages.push_back(message);
        }
    }
    pub fn deliver_newest(&mut self) {
        if let Some(message) = self.messages.pop_back() {
            self.messages.push_front(message);
            self.deliver_one();
        }
    }
    pub fn pump(&mut self) {
        let mut budget = 100_000;
        while self.deliver_one() {
            budget -= 1;
            assert!(budget > 0, "non-quiescent simulation");
        }
    }
    /// Only nodes in `side` talk to each other; everyone else is cut off.
    pub fn partition(&mut self, side: &[u64]) {
        self.blocked.clear();
        for a in 1..=3 {
            for b in 1..=3 {
                if side.contains(&a) != side.contains(&b) {
                    self.blocked.insert((a, b));
                }
            }
        }
    }
    pub fn block(&mut self, from: u64, to: u64) {
        self.blocked.insert((from, to));
    }
    pub fn heal(&mut self) {
        self.blocked.clear();
    }

    pub fn checkpoint(&mut self, id: u64) {
        let limit = self.setup.checkpoint_bytes;
        let node = self.nodes.get_mut(&id).unwrap();
        let bytes = node.app.checkpoint(limit).unwrap();
        node.checkpoint = Some((node.app.applied_index(), bytes));
        self.stats.checkpoints += 1;
    }

    /// Process restart: recover the core from its store, restore the
    /// application from its checkpoint and replay the committed suffix, and
    /// check the result equals a replay from the start.
    pub fn restart(&mut self, id: u64) {
        self.stats.restarts += 1;
        let limit = self.setup.checkpoint_bytes;
        let node = self.nodes.remove(&id).unwrap();
        let ProbeNode {
            store, checkpoint, ..
        } = node;
        let core = Raft::recover(
            NodeId::new(id).unwrap(),
            store.binding(),
            store.state(group(1)).unwrap(),
            store.limits(),
        )
        .unwrap();
        let mut app = (self.setup.make)();
        if let Some((index, bytes)) = &checkpoint {
            app.restore_checkpoint(app.schema_version(), *index, bytes)
                .unwrap();
        }
        let committed = core.replay_committed().to_vec();
        let mut replayed = (self.setup.make)();
        let replay = if committed.is_empty() {
            Ok(Vec::new())
        } else {
            replayed.apply_batch(&committed)
        };
        self.nodes.insert(
            id,
            ProbeNode {
                core,
                store,
                app,
                checkpoint,
            },
        );
        self.apply(id, &committed);
        if let Err(error) = replay {
            self.failures.push(ApplyFailure {
                node: id,
                first_index: 1,
                error,
            });
            return;
        }
        let restored = self.nodes[&id].app.checkpoint(limit).unwrap();
        assert_eq!(
            restored,
            replayed.checkpoint(limit).unwrap(),
            "restore differs from replay"
        );
    }

    /// The fixed U2 history: fill to capacity − 1; the leader proposes two
    /// novel commands and replicates only the first, to one follower whose
    /// acknowledgement is lost, so neither commits; the leader is cut off; that
    /// follower wins an election and, before it has applied the inherited
    /// entry, admits its own novel command; the cluster heals and commits.
    pub fn partition_across_leader_change(&mut self) {
        self.campaign(1);
        self.pump();
        for command in self.setup.fill.clone() {
            assert!(self.propose(1, command), "fill must be admitted");
            self.pump();
        }
        let first = self.next_novel();
        let second = self.next_novel();
        self.block(1, 3);
        self.block(2, 1);
        self.propose(1, first);
        self.pump();
        self.partition(&[1]);
        self.propose(1, second);
        self.pump();
        self.partition(&[2, 3]);
        self.campaign(2);
        while self.leader() != Some(2) && self.deliver_one() {}
        let third = self.next_novel();
        self.propose(2, third);
        self.pump();
        self.heal();
        self.heartbeat(2);
        self.pump();
    }

    /// One seeded schedule after the fixed history.
    pub fn schedule(&mut self, seed: u64, steps: u64) {
        let mut random = seed.max(1);
        let mut next = move || {
            random ^= random << 13;
            random ^= random >> 7;
            random ^= random << 17;
            random
        };
        for _ in 0..steps {
            let r = next();
            let id = (r >> 8) % 3 + 1;
            match r % 12 {
                0 => self.campaign(id),
                1..=3 => {
                    let command = if r & 0x100_0000 == 0 && !self.receipts.is_empty() {
                        // A retry of an earlier command.
                        self.setup.fill[(r >> 32) as usize % self.setup.fill.len().max(1)].clone()
                    } else {
                        self.next_novel()
                    };
                    if let Some(leader) = self.leader() {
                        self.propose(if r & 0x200_0000 == 0 { leader } else { id }, command);
                    }
                }
                4 => self.partition(&[id]),
                5 => self.heal(),
                6 => {
                    self.deliver_one();
                }
                7 => self.deliver_newest(),
                8 => self.duplicate_oldest(),
                9 => self.heartbeat(id),
                10 => {
                    if r & 0x400_0000 == 0 {
                        self.checkpoint(id);
                    } else {
                        self.restart(id);
                    }
                }
                _ => self.pump(),
            }
        }
        self.heal();
        if let Some(leader) = self.leader() {
            self.heartbeat(leader);
        }
        self.pump();
    }
}
