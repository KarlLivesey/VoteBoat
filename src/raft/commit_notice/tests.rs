// SPDX-License-Identifier: RPL-1.5
// Copyright (c) 2026 Karl Livesey
use super::*;
use crate::application::{Counter, StateMachine};
use crate::raft::membership_tests::{core, node};
#[path = "histories.rs"]
mod histories;
#[cfg(feature = "native")]
#[path = "native.rs"]
mod native;
#[cfg(feature = "native")]
#[path = "provenance.rs"]
mod provenance;

#[derive(Default, Debug, Eq, PartialEq)]
struct Counts {
    messages: usize,
    events: usize,
    effects: usize,
    persists: usize,
}
struct Cluster {
    cores: Vec<Raft>,
    logs: Vec<VolatileLog>,
    apps: Vec<Counter>,
    queue: VecDeque<Message>,
    sent: Vec<Message>,
    counts: Counts,
    volatile: bool,
}
impl Cluster {
    fn new(enabled: bool, volatile: bool) -> Self {
        let state = core().state().clone();
        let mut cores = Vec::new();
        let mut logs = Vec::new();
        let mut apps = Vec::new();
        for n in 1..=3 {
            let binding = StoreBinding {
                identity: state.bootstrap.voter_stores[&node(n)],
                session: StoreSession::new(1).unwrap(),
            };
            let log = VolatileLog::new(binding, LogLimits::default()).unwrap();
            let mut raft = if volatile {
                Raft::recover_volatile(node(n), &log, state.clone()).unwrap()
            } else {
                Raft::recover(node(n), binding, state.clone(), LogLimits::default()).unwrap()
            };
            if enabled {
                raft.enable_commit_notices().unwrap();
            }
            let mut app = Counter::new(6000).unwrap();
            app.apply_entries(raft.replay_committed()).unwrap();
            cores.push(raft);
            logs.push(log);
            apps.push(app);
        }
        let mut c = Self {
            cores,
            logs,
            apps,
            queue: VecDeque::new(),
            sent: Vec::new(),
            counts: Counts::default(),
            volatile,
        };
        c.act(0, Event::Campaign);
        c.drain();
        assert_eq!(c.cores[0].role(), Role::Leader);
        c.reset();
        c
    }
    fn reset(&mut self) {
        self.counts = Counts::default();
        self.sent.clear();
    }
    fn act(&mut self, at: usize, event: Event) {
        self.counts.events += 1;
        let effects = self.cores[at].step(event).unwrap();
        self.effects(at, effects);
    }
    fn effects(&mut self, at: usize, effects: Vec<Effect>) {
        let mut effects = VecDeque::from(effects);
        while let Some(effect) = effects.pop_front() {
            self.counts.effects += 1;
            match effect {
                Effect::Send(m) => {
                    self.counts.messages += 1;
                    self.sent.push(m.clone());
                    self.queue.push_back(m);
                }
                Effect::PersistVolatile(token) => {
                    assert!(self.volatile);
                    self.counts.persists += 1;
                    let committed = persist_volatile_into(
                        &mut self.cores[at],
                        &self.logs[at],
                        token,
                        &mut effects,
                    )
                    .unwrap();
                    if let Some(entries) = committed {
                        self.apps[at].apply_entries(entries).unwrap();
                    }
                }
                Effect::Persist(update) => {
                    assert!(!self.volatile);
                    self.counts.persists += 1;
                    self.complete_durable(at, update, &mut effects);
                }
                _ => panic!("unexpected effect {effect:?}"),
            }
        }
    }
    fn complete_durable(&mut self, at: usize, update: LogUpdate, out: &mut VecDeque<Effect>) {
        let raft = &mut self.cores[at];
        raft.validate_persist_effect(&update).unwrap();
        let next = raft.pending.as_ref().unwrap().next.metadata();
        // Exact host-asserted durable seam, not a filesystem durability claim.
        let ticket = LogTicket {
            binding: raft.binding,
            batch: raft.last_batch + 1,
            group: next.group,
            revision: next.revision,
            generation: next.generation,
            last_index: next.last_index,
            term: next.hard_state.term,
        };
        raft.admitted(ticket).unwrap();
        let result = raft
            .complete_borrowed(&DurableLog {
                tickets: vec![ticket],
            })
            .unwrap();
        if let Some(entries) = result.committed() {
            self.apps[at].apply_entries(entries).unwrap();
        }
        out.extend(result.into_parts().1);
    }
    fn drain(&mut self) {
        let mut budget = 1000;
        while let Some(m) = self.queue.pop_front() {
            assert!(budget > 0);
            budget -= 1;
            self.deliver(m);
        }
    }
    fn deliver(&mut self, m: Message) {
        self.act(m.to.get() as usize - 1, Event::Receive(m));
    }
    fn propose(&mut self, operation: u128) {
        self.act(
            0,
            Event::Propose {
                operation: OperationId::new(operation).unwrap(),
                bytes: 1_i64.to_le_bytes().to_vec(),
            },
        );
    }
    fn assert_converged(&self, originals: i64) {
        let tip = self.cores[0].state().last_index();
        for (raft, app) in self.cores.iter().zip(&self.apps) {
            assert_eq!(raft.state().commit_index, tip);
            assert_eq!(app.applied_index(), tip);
            assert_eq!(app.read_applied(0).unwrap(), originals);
            assert!(!raft.has_pending_dependency());
            assert!(raft.requests.is_empty());
            assert!(raft.admitted_ranges.is_empty());
            assert!(raft.empty_append_aliases.is_empty());
            assert!(raft.commit_notice_emissions.len() <= 2);
        }
        assert!(self.queue.is_empty());
    }
    fn notices(&mut self) -> Vec<Message> {
        let mut held = Vec::new();
        let mut budget = 1000;
        while let Some(m) = self.queue.pop_front() {
            assert!(budget > 0);
            budget -= 1;
            if matches!(m.rpc, Rpc::CommitNotice { .. }) {
                held.push(m);
            } else {
                self.deliver(m);
            }
        }
        held
    }
}

#[test]
fn fixed_schedule_removes_only_two_messages_and_keeps_six_persistence_rounds() {
    for volatile in [true, false] {
        let mut off = Cluster::new(false, volatile);
        let mut on = Cluster::new(true, volatile);
        off.propose(1);
        off.drain();
        on.propose(1);
        on.drain();
        assert_eq!(
            off.counts,
            Counts {
                messages: 8,
                events: 9,
                effects: 14,
                persists: 6
            }
        );
        assert_eq!(
            on.counts,
            Counts {
                messages: 6,
                events: 7,
                effects: 12,
                persists: 6
            }
        );
        off.assert_converged(1);
        on.assert_converged(1);
        for i in 0..3 {
            assert_eq!(off.cores[i].state(), on.cores[i].state());
        }
        assert_eq!(
            on.sent
                .iter()
                .filter(|m| matches!(m.rpc, Rpc::CommitNotice { .. }))
                .count(),
            2
        );
    }
}

#[test]
fn static_checkpoint_after_5000_originals_keeps_the_notice_path_reachable() {
    let mut c = Cluster::new(true, true);
    for op in 1..=5000 {
        c.propose(op);
        c.drain();
    }
    c.assert_converged(5000);
    for at in 0..3 {
        let raft = &c.cores[at];
        let reference = SnapshotRef {
            store: raft.binding.identity,
            group: raft.state().bootstrap.group,
            configuration: raft.membership().id(),
            generation: SnapshotGeneration::new(1).unwrap(),
            index: raft.state().commit_index,
            term: raft.state().last_term(),
            application_schema: 1,
            file_bytes: 128,
            checksum: 7,
        };
        let context_effects = c.cores[at].step(Event::Checkpoint).unwrap();
        let [Effect::CheckpointRequired { context }] = context_effects.as_slice() else {
            panic!()
        };
        let effects = c.cores[at].checkpoint_stored(*context, reference).unwrap();
        let Effect::PersistVolatile(token) = effects[0] else {
            panic!()
        };
        let done = persist_volatile(&mut c.cores[at], &c.logs[at], token).unwrap();
        assert!(done.committed().is_none());
        assert!(matches!(done.following(), [Effect::CheckpointCompacted(_)]));
        c.cores[at].checkpoint_reconciled(reference).unwrap();
        assert!(c.cores[at].state().snapshot_membership.is_none());
        assert!(c.cores[at].durable.summary().is_plain());
        assert!(c.cores[at].commit_notice_emissions.is_empty());
    }
    let mut compacted = c
        .sent
        .iter()
        .rev()
        .find(|m| m.to == node(2) && matches!(m.rpc, Rpc::CommitNotice { .. }))
        .unwrap()
        .clone();
    compacted.rpc = Rpc::CommitNotice {
        index: 1,
        log_term: 1,
    };
    let before = c.cores[1].state().clone();
    let reset = c.cores[1].election_reset_sequence();
    c.deliver(compacted);
    assert_eq!(c.cores[1].state(), &before);
    assert!(c.cores[1].election_reset_sequence() > reset);
    c.act(0, Event::Heartbeat);
    c.drain();
    c.reset();
    c.propose(5001);
    c.drain();
    c.assert_converged(5001);
    assert_eq!(
        c.counts,
        Counts {
            messages: 6,
            events: 7,
            effects: 12,
            persists: 6
        }
    );
}
