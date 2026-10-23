// SPDX-License-Identifier: RPL-1.5
// Copyright (c) 2026 Karl Livesey
//! Public-only synchronous embedding: real elections, messages and Counter apply.
use std::{
    collections::VecDeque,
    sync::{
        atomic::{AtomicUsize, Ordering},
        Arc,
    },
};
use voteboat::{
    application::{Counter, CounterReceipt, ProposalAdmission, StateMachine},
    identity::*,
    log::*,
    quorum::{Limits, Policy, Tree, WeightedChild},
    raft::*,
};

fn node(n: u64) -> NodeId {
    NodeId::new(n).unwrap()
}
fn majority() -> Tree {
    Tree::Majority((1..=3).map(|n| Tree::Voter(node(n))).collect())
}
struct Replica {
    core: Raft,
    log: VolatileLog,
    application: Counter,
    receipts: Vec<CounterReceipt>,
    // The embedder owns this shared resource; dropping a group cannot stop it.
    host_applied_entries: Arc<AtomicUsize>,
}
#[derive(Debug, Default, Eq, PartialEq)]
struct Work {
    sends: usize,
    events: usize,
    external: usize,
    installs: usize,
    intervals: usize,
    local: [usize; 3],
}
struct Host {
    replicas: Vec<Replica>,
    messages: VecDeque<Message>,
    history: Vec<(usize, Effect)>,
    work: Work,
    selected: bool,
    fail_next_send: bool,
}
impl Host {
    fn new(group: u128, tree: Tree, selected: bool, resource: &Arc<AtomicUsize>) -> Self {
        let policy = Policy::new(tree, Limits::default()).unwrap();
        let bootstrap = Bootstrap {
            group: GroupIdentity {
                id: GroupId::new(group).unwrap(),
                incarnation: GroupIncarnation::new(1).unwrap(),
            },
            configuration: ConfigurationId::new(1).unwrap(),
            voter_stores: policy
                .voters()
                .iter()
                .map(|n| {
                    (
                        *n,
                        StoreIdentity {
                            id: StoreId::new(n.get() as u128).unwrap(),
                            incarnation: StoreIncarnation::new(1).unwrap(),
                        },
                    )
                })
                .collect(),
            policy,
        };
        let replicas = bootstrap
            .voter_stores
            .iter()
            .map(|(&node, &identity)| {
                let log = VolatileLog::new(
                    StoreBinding {
                        identity,
                        session: StoreSession::new(1).unwrap(),
                    },
                    LogLimits::default(),
                )
                .unwrap();
                let mut core =
                    Raft::recover_volatile(node, &log, log.bootstrap(&bootstrap).unwrap()).unwrap();
                core.configure_batch_limits(BatchLimits::for_ready_visit(1024).unwrap())
                    .unwrap();
                Replica {
                    core,
                    log,
                    application: Counter::new(20000).unwrap(),
                    receipts: Vec::new(),
                    host_applied_entries: resource.clone(),
                }
            })
            .collect();
        let mut host = Self {
            replicas,
            messages: VecDeque::new(),
            history: Vec::new(),
            work: Work::default(),
            selected,
            fail_next_send: false,
        };
        host.event(0, Event::Campaign);
        host.deliver();
        assert_eq!(host.replicas[0].core.role(), Role::Leader);
        host.reset();
        host
    }
    fn reset(&mut self) {
        self.work = Work::default();
        self.history.clear();
        for replica in &mut self.replicas {
            replica.receipts.clear();
        }
    }
    fn event(&mut self, slot: usize, event: Event) {
        self.work.events += 1;
        let mut out = VecDeque::new();
        let replica = &mut self.replicas[slot];
        if self.selected {
            let turn = replica
                .core
                .step_volatile_into(&replica.log, event, &mut out)
                .unwrap();
            Self::materialise(turn, slot, &mut self.work, &mut out);
        } else {
            replica.core.step_into(event, &mut out).unwrap();
        }
        self.effects(slot, out).unwrap();
    }
    fn propose(&mut self, first: u128, width: usize) {
        let bytes = 1i64.to_le_bytes();
        let replica = &self.replicas[0];
        for offset in 0..width {
            let operation = OperationId::new(first + offset as u128).unwrap();
            replica
                .application
                .validate_proposal_context(&replica.core, operation, &bytes)
                .unwrap();
            replica
                .application
                .validate_proposal(
                    operation,
                    &bytes,
                    (first..first + offset as u128)
                        .map(|id| (OperationId::new(id).unwrap(), bytes.as_slice())),
                )
                .unwrap();
        }
        if width == 1 {
            self.event(
                0,
                Event::Propose {
                    operation: OperationId::new(first).unwrap(),
                    bytes: 1i64.to_le_bytes().to_vec(),
                },
            );
        } else {
            self.work.events += 1;
            let replica = &mut self.replicas[0];
            let mut batch = replica.core.proposal_batch(width).unwrap();
            for offset in 0..width {
                batch
                    .push(
                        OperationId::new(first + offset as u128).unwrap(),
                        1i64.to_le_bytes().to_vec(),
                    )
                    .unwrap();
            }
            let mut out = VecDeque::new();
            if self.selected {
                let turn = replica
                    .core
                    .propose_entries_volatile_into(&replica.log, batch, &mut out)
                    .unwrap();
                Self::materialise(turn, 0, &mut self.work, &mut out);
            } else {
                replica.core.propose_entries_into(batch, &mut out).unwrap();
            }
            self.effects(0, out).unwrap();
        }
    }
    fn materialise(
        turn: VolatileTurn<'_>,
        slot: usize,
        work: &mut Work,
        out: &mut VecDeque<Effect>,
    ) {
        let units = turn.completed_local_units();
        assert!(units <= 2);
        work.installs += units;
        work.local[slot] += units;
        if let Some(entries) = turn.committed() {
            out.insert(turn.commit_position(), Effect::Committed(entries.to_vec()));
        }
    }
    fn effects(&mut self, slot: usize, mut out: VecDeque<Effect>) -> Result<(), &'static str> {
        while let Some(effect) = out.pop_front() {
            match effect {
                Effect::PersistVolatile(key) => {
                    self.work.external += 1;
                    self.work.installs += 1;
                    let position = out.len();
                    let replica = &mut self.replicas[slot];
                    if let Some(entries) =
                        persist_volatile_into(&mut replica.core, &replica.log, key, &mut out)
                            .unwrap()
                    {
                        out.insert(position, Effect::Committed(entries.to_vec()));
                    }
                }
                Effect::Send(message) => {
                    if std::mem::take(&mut self.fail_next_send) {
                        return Err("injected host data-send failure");
                    }
                    self.work.sends += 1;
                    self.history.push((slot, Effect::Send(message.clone())));
                    self.messages.push_back(message);
                }
                Effect::Committed(entries) => {
                    self.work.intervals += 1;
                    let replica = &mut self.replicas[slot];
                    let receipts = replica.application.apply_batch(&entries).unwrap();
                    replica
                        .host_applied_entries
                        .fetch_add(entries.len(), Ordering::Relaxed);
                    replica.receipts.extend(receipts);
                    self.history.push((slot, Effect::Committed(entries)));
                }
                other => panic!("unexpected public host effect {other:?}"),
            }
        }
        Ok(())
    }
    fn deliver(&mut self) {
        let mut visits = 0;
        while let Some(message) = self.messages.pop_front() {
            visits += 1;
            assert!(visits < 1000);
            self.event(message.to.get() as usize - 1, Event::Receive(message));
        }
    }
}

#[test]
fn selected_and_deferred_exact_histories_b1_b231_b515() {
    let resource = Arc::new(AtomicUsize::new(0));
    for width in [1, 231, 515] {
        let mut baseline = Host::new(1, majority(), false, &resource);
        let mut selected = Host::new(1, majority(), true, &resource);
        baseline.propose(100, width);
        selected.propose(100, width);
        baseline.deliver();
        selected.deliver();
        assert_eq!(baseline.history, selected.history);
        for (a, b) in baseline.replicas.iter().zip(&selected.replicas) {
            assert_eq!(a.core.state(), b.core.state());
            assert_eq!(a.core.role(), b.core.role());
            assert_eq!(a.receipts, b.receipts);
            assert_eq!(b.receipts.len(), width);
            assert_eq!(a.application.applied_index(), b.application.applied_index());
            assert_eq!(
                b.application.read_applied(b.core.state().commit_index),
                Ok(width as i64)
            );
            assert!(!b.core.has_pending_dependency());
        }
        assert_eq!(
            (
                baseline.work.sends,
                baseline.work.events,
                baseline.work.external,
                baseline.work.installs,
                baseline.work.intervals
            ),
            (8, 9, 6, 6, 3)
        );
        assert_eq!(
            (
                selected.work.sends,
                selected.work.events,
                selected.work.external,
                selected.work.installs,
                selected.work.intervals
            ),
            (8, 9, 0, 6, 3)
        );
        assert_eq!(selected.work.local, [2, 2, 2]);
    }
    assert_eq!(Arc::strong_count(&resource), 1);
}

#[test]
fn independent_groups_use_both_public_apis_without_an_ancestor_or_resource_owner() {
    let resource = Arc::new(AtomicUsize::new(0));
    let mut first = Host::new(41, majority(), true, &resource);
    let mut second = Host::new(42, majority(), true, &resource);
    first.propose(1, 1);
    first.deliver();
    second.propose(1, 231);
    second.deliver();
    drop(first);
    assert_eq!(Arc::strong_count(&resource), 4);
    let before = resource.load(Ordering::Relaxed);
    second.propose(500, 1);
    second.deliver();
    assert!(resource.load(Ordering::Relaxed) > before);
    assert_eq!(
        second.replicas[0]
            .application
            .read_applied(second.replicas[0].core.state().commit_index),
        Ok(232)
    );
    drop(second);
    assert_eq!(Arc::strong_count(&resource), 1);
}

#[test]
fn singleton_proposal_has_two_checked_installs_one_application_interval() {
    let mut host = Host::new(
        1,
        Tree::Voter(node(1)),
        true,
        &Arc::new(AtomicUsize::new(0)),
    );
    host.propose(1, 1);
    host.deliver();
    assert_eq!(
        (host.work.installs, host.work.external, host.work.intervals),
        (2, 0, 1)
    );
    assert_eq!(host.replicas[0].receipts.len(), 1);
}

#[test]
fn weighted_self_quorum_preserves_preceding_send_before_external_commit() {
    let tree = Tree::Weighted(
        (1..=3)
            .map(|n| WeightedChild {
                weight: if n == 1 { 3 } else { 1 },
                node: Tree::Voter(node(n)),
            })
            .collect(),
    );
    let mut host = Host::new(1, tree, true, &Arc::new(AtomicUsize::new(0)));
    let replica = &mut host.replicas[0];
    let before = replica.core.state().commit_index;
    let mut out = VecDeque::new();
    let turn = replica
        .core
        .step_volatile_into(
            &replica.log,
            Event::Propose {
                operation: OperationId::new(1).unwrap(),
                bytes: 1i64.to_le_bytes().to_vec(),
            },
            &mut out,
        )
        .unwrap();
    assert_eq!(turn.completed_local_units(), 1);
    assert!(turn.committed().is_none());
    assert!(matches!(out.front(), Some(Effect::Send(_))));
    assert!(matches!(out.back(), Some(Effect::PersistVolatile(_))));
    host.fail_next_send = true;
    assert_eq!(host.effects(0, out), Err("injected host data-send failure"));
    // The failing host send never consumes the later token or applies/results.
    let replica = &host.replicas[0];
    assert_eq!(replica.core.state().commit_index, before);
    assert_eq!(replica.application.applied_index(), before);
    assert!(replica.core.has_pending_dependency());
    assert!(replica.receipts.is_empty());
}

#[test]
fn physical_weighted_and_recursive_policies_need_exact_qualifying_evidence() {
    let weighted = Tree::Weighted(
        (1..=3)
            .map(|n| WeightedChild {
                weight: if n == 3 { 3 } else { 1 },
                node: Tree::Voter(node(n)),
            })
            .collect(),
    );
    let recursive = Tree::Majority(vec![
        Tree::Majority(vec![Tree::Voter(node(1)), Tree::Voter(node(2))]),
        Tree::Majority(vec![Tree::Voter(node(3))]),
    ]);
    for tree in [weighted, recursive] {
        let mut host = Host::new(1, tree, true, &Arc::new(AtomicUsize::new(0)));
        let before = host.replicas[0].core.state().commit_index;
        host.propose(1, 1);
        let withheld = host.messages.iter().position(|m| m.to == node(3)).unwrap();
        let data = host.messages.remove(withheld).unwrap();
        host.deliver();
        // Self plus node2 is a flat majority but fails both configured policies.
        assert_eq!(host.replicas[0].core.state().commit_index, before);
        assert!(host.replicas[0].receipts.is_empty());
        host.event(2, Event::Receive(data));
        let exact = host.messages.pop_front().unwrap();
        assert_eq!(exact.from, node(3));
        let mut wrong = exact.clone();
        wrong.context.sequence += 1;
        host.event(0, Event::Receive(wrong));
        assert_eq!(host.replicas[0].core.state().commit_index, before);
        host.event(0, Event::Receive(exact));
        host.deliver();
        assert_eq!(host.replicas[0].core.state().commit_index, before + 1);
        assert_eq!(host.replicas[0].receipts.len(), 1);
    }
}
