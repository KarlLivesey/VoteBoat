// SPDX-License-Identifier: RPL-1.5
// Copyright (c) 2026 Karl Livesey
use super::*;

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub(super) enum Phase {
    Written,
    Synced,
}
struct Held {
    ticket: LogTicket,
    completion: Option<DurableLog>,
}
#[derive(Default)]
pub(super) struct Dependencies {
    pub(super) nodes: BTreeSet<u64>,
    held: BTreeMap<u64, Held>,
    previous: BTreeMap<u64, DurableLog>,
    pub(super) busy: usize,
    pub(super) stale_rejected: usize,
    pub(super) sync_failures: usize,
    pub(super) fenced_refused: usize,
}
impl Dependencies {
    pub(super) fn phases(&self) -> BTreeMap<u64, Phase> {
        self.held
            .iter()
            .map(|(id, held)| {
                (
                    *id,
                    if held.completion.is_some() {
                        Phase::Synced
                    } else {
                        Phase::Written
                    },
                )
            })
            .collect()
    }
}
impl Replay {
    pub(super) fn try_propose(&mut self, id: u64, operation: u128, bytes: Vec<u8>) {
        if self.cluster.replicas[&id].core.role() == Role::Leader {
            self.step_event(
                id,
                Event::Propose {
                    operation: OperationId::new(operation).unwrap(),
                    bytes,
                },
            );
        }
    }
    pub(super) fn step_event(&mut self, id: u64, event: Event) {
        let delivery = matches!(&event, Event::Receive(_));
        match self.cluster.replicas.get_mut(&id).unwrap().core.step(event) {
            Ok(effects) => self.effects(id, effects),
            Err(RaftError::Busy) => {
                assert!(self.delayed.held.contains_key(&id));
                self.delayed.busy += 1;
            }
            Err(RaftError::WrongIdentity) if delivery => (),
            Err(RaftError::Fenced) => {
                assert!(self.cluster.replicas[&id].core.is_fenced());
                self.delayed.fenced_refused += 1;
            }
            Err(error) => panic!("unexpected scheduled event error: {error:?}"),
        }
    }
    fn effects(&mut self, id: u64, effects: Vec<Effect>) {
        let mut effects = VecDeque::from(effects);
        while let Some(effect) = effects.pop_front() {
            if let Effect::Persist(update) = effect {
                let replica = self.cluster.replicas.get_mut(&id).unwrap();
                if self.delayed.nodes.contains(&id) {
                    let tickets = replica
                        .store
                        .append_batch(vec![LogMutation::Update(update)])
                        .unwrap();
                    assert_eq!(tickets.len(), 1);
                    replica.core.admitted(tickets[0]).unwrap();
                    assert!(self
                        .delayed
                        .held
                        .insert(
                            id,
                            Held {
                                ticket: tickets[0],
                                completion: None,
                            },
                        )
                        .is_none());
                } else {
                    effects.extend(
                        persist_effect(&mut replica.core, &mut replica.store, update).unwrap(),
                    );
                }
            } else {
                self.cluster.effects(id, vec![effect]);
            }
        }
        self.cluster.assert_safety();
    }
    pub(super) fn deliver(&mut self, message: Message) {
        if !self
            .cluster
            .blocked
            .contains(&(message.from.get(), message.to.get()))
        {
            self.step_event(message.to.get(), Event::Receive(message));
        }
    }
    pub(super) fn pump(&mut self) {
        let mut count = 0;
        while let Some(message) = self.cluster.messages.pop_front() {
            count += 1;
            assert!(count < 100000, "non-quiescent scheduled delivery");
            self.deliver(message);
        }
    }
    pub(super) fn sync(&mut self, id: u64) {
        if let Some(held) = self.delayed.held.get_mut(&id) {
            if held.completion.is_none() {
                held.completion = Some(
                    self.cluster
                        .replicas
                        .get_mut(&id)
                        .unwrap()
                        .store
                        .barrier(&[held.ticket])
                        .unwrap(),
                );
            }
        }
    }
    pub(super) fn complete(&mut self, id: u64) {
        if self
            .delayed
            .held
            .get(&id)
            .is_some_and(|held| held.completion.is_some())
        {
            let completion = self.delayed.held.remove(&id).unwrap().completion.unwrap();
            let effects = self
                .cluster
                .replicas
                .get_mut(&id)
                .unwrap()
                .core
                .complete(&completion)
                .unwrap();
            self.delayed.previous.insert(id, completion);
            self.effects(id, effects);
        }
    }
    pub(super) fn fail_sync(&mut self, id: u64) {
        if let Some(held) = self.delayed.held.get(&id) {
            if held.completion.is_none() {
                self.images[&id].device.0.borrow_mut().fault = Fault::Sync;
                let replica = self.cluster.replicas.get_mut(&id).unwrap();
                assert_eq!(
                    replica.store.barrier(&[held.ticket]),
                    Err(voteboat::contracts::StorageError::Uncertain(
                        "failed sync".to_owned()
                    ))
                );
                replica.core.storage_failed();
                assert!(replica.core.is_fenced());
                assert_eq!(
                    replica.store.state(group(1)),
                    Err(voteboat::contracts::StorageError::Fenced)
                );
                self.delayed.held.remove(&id);
                self.delayed.sync_failures += 1;
            }
        }
    }
    pub(super) fn discard_dependency(&mut self, id: u64) {
        if let Some(held) = self.delayed.held.remove(&id) {
            if let Some(completion) = held.completion {
                self.delayed.previous.insert(id, completion);
            }
        }
    }
    pub(super) fn stale(&mut self, id: u64) {
        if let Some(completion) = self.delayed.previous.get(&id) {
            let before = self.point();
            let expected = if self.cluster.replicas[&id].core.is_fenced() {
                RaftError::Fenced
            } else {
                RaftError::WrongCompletion
            };
            assert_eq!(
                self.cluster
                    .replicas
                    .get_mut(&id)
                    .unwrap()
                    .core
                    .complete(completion),
                Err(expected)
            );
            assert_eq!(self.point(), before);
            self.delayed.stale_rejected += 1;
        }
    }
}

#[test]
fn admitted_and_synced_votes_wait_for_actual_completion_delivery() {
    let mut run = Replay::new();
    for action in [Action::Delay(1), Action::Campaign(1)] {
        run.action(&action).unwrap();
    }
    assert_eq!(run.point().held[&1], Phase::Written);
    assert_eq!(run.point().logs[&1].hard_state.term, 0);
    assert_eq!(run.point().queued, 0);
    run.action(&Action::Complete(1)).unwrap();
    assert_eq!(run.point().held[&1], Phase::Written);
    run.action(&Action::Heartbeat(1)).unwrap();
    assert_eq!(run.point().busy, 1);
    run.action(&Action::Sync(1)).unwrap();
    assert_eq!(run.point().held[&1], Phase::Synced);
    assert_eq!(run.point().logs[&1].hard_state.term, 0);
    assert_eq!(run.point().queued, 0);
    run.action(&Action::Complete(1)).unwrap();
    assert!(run.point().held.is_empty());
    assert_eq!(run.point().logs[&1].hard_state.term, 1);
    assert_eq!(run.point().queued, 2);
}

#[test]
fn power_loss_distinguishes_written_and_synced_unobserved_votes() {
    for synced in [false, true] {
        let mut run = Replay::new();
        run.action(&Action::Delay(1)).unwrap();
        run.action(&Action::Campaign(1)).unwrap();
        if synced {
            run.action(&Action::Sync(1)).unwrap();
        }
        run.action(&Action::PowerLoss(1)).unwrap();
        assert!(run.point().held.is_empty());
        assert_eq!(run.point().logs[&1].hard_state.term, u64::from(synced));
        assert_eq!(run.point().queued, 0);
        run.action(&Action::Campaign(1)).unwrap();
        run.action(&Action::Stale(1)).unwrap();
        assert_eq!(run.point().stale_rejected, usize::from(synced));
        assert_eq!(run.point().held[&1], Phase::Written);
        run.action(&Action::Sync(1)).unwrap();
        run.action(&Action::Complete(1)).unwrap();
        assert_eq!(run.point().logs[&1].hard_state.term, u64::from(synced) + 1);
        assert_eq!(run.point().queued, 2);
        run.action(&Action::Stale(1)).unwrap();
        assert_eq!(run.point().stale_rejected, usize::from(synced) + 1);
    }
}

#[test]
fn independently_completed_follower_is_required_for_a_real_durable_quorum() {
    for follower in [2, 3] {
        let mut run = Replay::new();
        run.action(&Action::Campaign(1)).unwrap();
        run.action(&Action::Pump).unwrap();
        run.action(&Action::Delay(2)).unwrap();
        run.action(&Action::Delay(3)).unwrap();
        run.action(&Action::TryWrite(1, 8801, 8)).unwrap();
        run.action(&Action::Pump).unwrap();
        assert_eq!(run.point().held.len(), 2);
        assert!(run.cluster.commands(1).is_empty());
        run.action(&Action::Sync(follower)).unwrap();
        run.action(&Action::Pump).unwrap();
        assert!(run.cluster.commands(1).is_empty());
        run.action(&Action::Complete(follower)).unwrap();
        run.action(&Action::Pump).unwrap();
        assert_eq!(run.cluster.commands(1), vec![vec![8]]);
        assert!(run.cluster.commands(follower).is_empty());
        run.action(&Action::Sync(follower)).unwrap();
        run.action(&Action::Complete(follower)).unwrap();
        run.action(&Action::Pump).unwrap();
        assert_eq!(run.cluster.commands(follower), vec![vec![8]]);
        let other = 5 - follower;
        assert_eq!(run.point().held[&other], Phase::Written);
        run.action(&Action::PowerLoss(other)).unwrap();
        for id in [1, follower] {
            run.action(&Action::PowerLoss(id)).unwrap();
            assert_eq!(run.cluster.commands(id), vec![vec![8]]);
        }
    }
}

#[test]
fn delayed_completion_schedules_replay_with_bounded_pending_ownership() {
    for seed in 1..=16 {
        let base = Trace::generated(seed);
        let mut actions = vec![Action::Delay(1), Action::Delay(2), Action::Delay(3)];
        actions.extend(base.actions.into_iter().enumerate().map(|(step, old)| {
            let id = u64::try_from(step % 3 + 1).unwrap();
            match step % 7 {
                0 => Action::Sync(id),
                1 => Action::Complete(id),
                2 => Action::Stale(id),
                _ => old,
            }
        }));
        let trace = Trace { seed, actions };
        let decoded = Trace::decode(&trace.encode()).unwrap();
        let points = replay(&trace).unwrap();
        assert_eq!(points, replay(&decoded).unwrap(), "{}", trace.encode());
        assert!(points.iter().all(|point| point.held.len() <= 3));
        assert!(points.iter().any(|point| !point.held.is_empty()));
    }
}
