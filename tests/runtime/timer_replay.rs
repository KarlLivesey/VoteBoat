// SPDX-License-Identifier: RPL-1.5
// Copyright (c) 2026 Karl Livesey
use super::*;
use voteboat::native::{log_store::*, runtime::*};
#[path = "timer_replay/schedule.rs"]
mod schedule;

type NativeNode = AutoNode<NativeLogStore<ModelIo>, FairScheduler, DeadlineQueue, JitterEntropy>;
struct Held {
    node: usize,
    visit: VisitTicket,
    ticket: LogTicket,
    completion: Option<DurableLog>,
}
struct Replay {
    nodes: Vec<NativeNode>,
    images: Vec<ModelIo>,
    held: Option<Held>,
    now: MonoTime,
    partition: Option<usize>,
    delayed: Vec<Message>,
}
fn assembled(id: usize, log: NativeLogStore<ModelIo>, now: MonoTime, create: bool) -> NativeNode {
    AutoNode::new(
        id as u64 + 1,
        log,
        FairScheduler::new(3).unwrap(),
        |owner| DeadlineQueue::new(owner, 3).unwrap(),
        JitterEntropy::new(id as u64 + 1),
        now,
        create,
    )
}
impl Replay {
    fn new() -> Self {
        let images: Vec<_> = (0..3).map(|_| ModelIo::default()).collect();
        let nodes = images
            .iter()
            .enumerate()
            .map(|(id, io)| {
                assembled(
                    id,
                    NativeLogStore::create(
                        io.clone(),
                        identity(id as u128 + 1),
                        LogLimits::default(),
                    )
                    .unwrap(),
                    MonoTime(0),
                    true,
                )
            })
            .collect();
        Self {
            nodes,
            images,
            held: None,
            now: MonoTime(0),
            partition: None,
            delayed: Vec::new(),
        }
    }
    fn hold_election(&mut self, id: usize) {
        assert!(self.held.is_none());
        let token = self.nodes[id].runtime.deadline(group(1)).unwrap();
        assert_eq!(token.kind, TimerKind::Election);
        self.now = token.deadline;
        let n = &mut self.nodes[id];
        for _ in 0..16 {
            let visit = n.runtime.poll(self.now).unwrap().unwrap();
            let step = n.runtime.step_next(visit, self.now).unwrap().unwrap();
            let effects = step.result.unwrap();
            if visit.group != group(1) {
                n.apply_effects(visit, self.now, effects);
                n.runtime.finish(visit).unwrap();
                continue;
            }
            assert_eq!(step.timer, Some(token), "use an actual native expiration");
            let [Effect::Persist(update)] = effects.as_slice() else {
                panic!("campaign must first persist");
            };
            let tickets = n
                .log
                .append_batch(vec![LogMutation::Update(update.clone())])
                .unwrap();
            assert_eq!(tickets.len(), 1);
            n.runtime
                .with_core(visit, self.now, |core| {
                    core.admit_effect(update, tickets[0])
                })
                .unwrap()
                .unwrap();
            assert_eq!(
                n.runtime.finish(visit),
                Err(RuntimeError::DependencyPending)
            );
            assert!(n.messages.iter().all(|m| m.group != group(1)));
            self.held = Some(Held {
                node: id,
                visit,
                ticket: tickets[0],
                completion: None,
            });
            return;
        }
        panic!("the due group1 election was not stepped");
    }
    fn sync(&mut self) {
        let held = self.held.as_mut().unwrap();
        assert!(held.completion.is_none());
        held.completion = Some(self.nodes[held.node].log.barrier(&[held.ticket]).unwrap());
        assert!(self.nodes[held.node]
            .runtime
            .core(group(1))
            .unwrap()
            .has_pending_dependency());
        assert!(self.nodes[held.node]
            .messages
            .iter()
            .all(|m| m.group != group(1)));
    }
    fn release(&mut self) {
        let held = self.held.take().unwrap();
        let n = &mut self.nodes[held.node];
        let effects = n
            .runtime
            .with_core(held.visit, self.now, |core| {
                core.complete(&held.completion.unwrap())
            })
            .unwrap()
            .unwrap();
        assert!(effects
            .iter()
            .any(|e| matches!(e, Effect::Send(m) if matches!(m.rpc, Rpc::Vote { .. }))));
        n.apply_effects(held.visit, self.now, effects);
        n.runtime.finish(held.visit).unwrap();
    }
    fn advance(&mut self, ms: u64) {
        assert!(ms >= self.now.0);
        self.now = MonoTime(ms);
        pump_auto(&mut self.nodes, self.now, self.partition, &mut self.delayed);
    }
    fn crash(&mut self, id: usize, power_loss: bool) -> Option<Held> {
        let held = self.held.take();
        if let Some(h) = &held {
            assert_eq!(h.node, id);
        }
        drop(self.nodes.remove(id));
        if power_loss {
            self.images[id].0.borrow_mut().power_loss();
        }
        let log = NativeLogStore::recover(
            self.images[id].clone(),
            identity(id as u128 + 1),
            LogLimits::default(),
        )
        .unwrap();
        self.nodes.insert(id, assembled(id, log, self.now, false));
        held
    }
}
#[test]
fn timer_campaign_waits_for_real_wal_sync_and_original_completion() {
    let mut run = Replay::new();
    run.hold_election(0);
    assert_eq!(
        run.nodes[0]
            .runtime
            .core(group(1))
            .unwrap()
            .state()
            .hard_state
            .term,
        0
    );
    run.sync();
    assert_eq!(run.nodes[0].log.state(group(1)).unwrap().hard_state.term, 1);
    assert_eq!(
        run.nodes[0]
            .runtime
            .core(group(1))
            .unwrap()
            .state()
            .hard_state
            .term,
        0
    );
    run.release();
    assert_eq!(
        run.nodes[0]
            .runtime
            .core(group(1))
            .unwrap()
            .state()
            .hard_state
            .term,
        1
    );
    assert_eq!(run.nodes[0].runtime.active_visits(), 0);
}
#[test]
fn timed_owner_recovery_distinguishes_written_synced_and_both_crash_modes() {
    for synced in [false, true] {
        for power_loss in [false, true] {
            let mut run = Replay::new();
            run.hold_election(0);
            if synced {
                run.sync();
            }
            let held = run.crash(0, power_loss).unwrap();
            let n = &mut run.nodes[0];
            assert_ne!(n.runtime.owner(), held.visit.owner);
            let expected_term = u64::from(synced || !power_loss);
            assert_eq!(
                n.runtime.core(group(1)).unwrap().state().hard_state.term,
                expected_term
            );
            assert_eq!(
                n.runtime.with_core(held.visit, run.now, |_| ()),
                Err(RuntimeError::StaleTicket)
            );
            if let Some(completion) = held.completion {
                assert_eq!(
                    n.runtime
                        .with_core(held.visit, run.now, |core| core.complete(&completion)),
                    Err(RuntimeError::StaleTicket)
                );
            }
            assert_eq!(n.runtime.core(group(1)).unwrap().role(), Role::Follower);
            assert!(n.runtime.deadline(group(1)).unwrap().owner == n.runtime.owner());
        }
    }
}

#[test]
fn held_timer_campaign_does_not_stall_other_group_or_lose_cold_retries() {
    for power_loss in [false, true] {
        let mut run = Replay::new();
        run.hold_election(0);
        run.partition = Some(0);
        for ms in run.now.0..=120 {
            run.advance(ms);
        }
        let other = run
            .nodes
            .iter()
            .position(|n| n.runtime.core(group(2)).unwrap().role() == Role::Leader)
            .unwrap();
        run.nodes[other]
            .runtime
            .admit(group(2), proposal(9911))
            .unwrap();
        run.advance(121);
        for n in &run.nodes {
            assert_eq!(
                n.apps[&group(2)].read_applied(n.apps[&group(2)].applied_index()),
                Ok(7)
            );
        }
        let held = &run.nodes[0];
        assert!(held
            .runtime
            .core(group(1))
            .unwrap()
            .has_pending_dependency());
        assert_eq!(
            held.runtime.core(group(1)).unwrap().state().hard_state.term,
            0
        );
        assert_eq!(held.runtime.active_visits(), 1);
        assert!(held.runtime.usage().items <= held.runtime.limits().max_items);
        run.sync();
        run.release();
        run.partition = None;
        let old: Vec<_> = run.delayed.drain(..).take(8).collect();
        for message in old {
            for _ in 0..2 {
                run.nodes[message.to.get() as usize - 1]
                    .runtime
                    .admit(message.group, Event::Receive(message.clone()))
                    .unwrap();
            }
            run.advance(121);
        }
        for ms in 122..=200 {
            run.advance(ms);
        }
        let current = run
            .nodes
            .iter()
            .position(|n| n.runtime.core(group(1)).unwrap().role() == Role::Leader)
            .unwrap();
        run.nodes[current]
            .runtime
            .admit(group(1), proposal(9911))
            .unwrap();
        run.advance(201);
        // A follower the quorum did not need gets the write within the hold bound.
        run.advance(201 + timer_config().append_hold_ms);
        for n in &run.nodes {
            assert_eq!(
                n.apps[&group(1)].read_applied(n.apps[&group(1)].applied_index()),
                Ok(7)
            );
        }
        assert!(run.crash(current, power_loss).is_none());
        for ms in 202 + timer_config().append_hold_ms..=300 {
            run.advance(ms);
        }
        let current = run
            .nodes
            .iter()
            .position(|n| n.runtime.core(group(1)).unwrap().role() == Role::Leader)
            .unwrap();
        run.nodes[current]
            .runtime
            .admit(group(1), proposal(9911))
            .unwrap();
        run.advance(301);
        run.advance(301 + timer_config().append_hold_ms);
        for n in &run.nodes {
            assert_eq!(
                n.apps[&group(1)].read_applied(n.apps[&group(1)].applied_index()),
                Ok(7)
            );
            assert_eq!(
                n.apps[&group(2)].read_applied(n.apps[&group(2)].applied_index()),
                Ok(7)
            );
        }
    }
}
