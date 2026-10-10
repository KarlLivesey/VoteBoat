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
#[path = "raft/leadership.rs"]
mod leadership;
mod support;
use std::collections::{BTreeMap, BTreeSet, VecDeque};
use support::*;
#[cfg(feature = "native")]
use voteboat::native::log_store::*;
use voteboat::{identity::*, log::*, quorum::*, raft::*};

struct Replica<S: LogStore> {
    core: Raft,
    store: S,
}
struct Cluster<S: LogStore> {
    replicas: BTreeMap<u64, Replica<S>>,
    messages: VecDeque<Message>,
    blocked: BTreeSet<(u64, u64)>,
    applied: BTreeMap<u64, Vec<LogEntry>>,
    reads: BTreeMap<u64, Vec<ReadBarrier>>,
}
impl<S: LogStore> Cluster<S> {
    fn new(bootstrap: Bootstrap, mut factory: impl FnMut(u64) -> S) -> Self {
        let mut replicas = BTreeMap::new();
        let mut applied = BTreeMap::new();
        for id in bootstrap.policy.voters() {
            let mut store = factory(id.get());
            append(&mut store, vec![LogMutation::Create(bootstrap.clone())]);
            let core = Raft::recover(
                *id,
                store.binding(),
                store.state(bootstrap.group).unwrap(),
                store.limits(),
            )
            .unwrap();
            replicas.insert(id.get(), Replica { core, store });
            applied.insert(id.get(), Vec::new());
        }
        Self {
            replicas,
            messages: VecDeque::new(),
            blocked: BTreeSet::new(),
            applied,
            reads: BTreeMap::new(),
        }
    }
    fn act(&mut self, id: u64, event: Event) {
        let effects = self
            .replicas
            .get_mut(&id)
            .unwrap()
            .core
            .step(event)
            .unwrap();
        self.effects(id, effects);
    }
    fn effects(&mut self, id: u64, effects: Vec<Effect>) {
        let mut effects = VecDeque::from(effects);
        while let Some(effect) = effects.pop_front() {
            match effect {
                Effect::SnapshotRequired { .. }
                | Effect::CheckpointRequired { .. }
                | Effect::CheckpointCompacted(_)
                | Effect::VerifyLearnerReadiness(_)
                | Effect::StageSnapshot(_)
                | Effect::SnapshotInstalled(_) => {
                    panic!("baseline history has no compacted groups")
                }
                Effect::ReadReady(barrier) => self.reads.entry(id).or_default().push(barrier),
                Effect::Send(message) => {
                    assert!(self.messages.len() < 10000);
                    self.messages.push_back(message);
                }
                Effect::Committed(entries) => {
                    for entry in entries {
                        let previous = self.applied[&id].len() as u64;
                        assert_eq!(entry.index, previous + 1);
                        self.applied.get_mut(&id).unwrap().push(entry);
                    }
                }
                Effect::Persist(update) => {
                    let replica = self.replicas.get_mut(&id).unwrap();
                    let more =
                        persist_effect(&mut replica.core, &mut replica.store, update).unwrap();
                    effects.extend(more);
                }
            }
        }
        self.assert_safety();
    }
    fn pump(&mut self) {
        let mut count = 0;
        while let Some(message) = self.messages.pop_front() {
            count += 1;
            assert!(count < 100000, "non-quiescent simulation");
            if !self
                .blocked
                .contains(&(message.from.get(), message.to.get()))
            {
                self.deliver(message);
            }
        }
    }
    fn deliver(&mut self, message: Message) {
        if self
            .blocked
            .contains(&(message.from.get(), message.to.get()))
        {
            return;
        }
        let id = message.to.get();
        match self
            .replicas
            .get_mut(&id)
            .unwrap()
            .core
            .step(Event::Receive(message))
        {
            Ok(effects) => self.effects(id, effects),
            // Reopened stores have a new request-origin session. Delayed
            // responses to the previous session are rejected before progress.
            Err(RaftError::WrongIdentity) => (),
            Err(e) => panic!("delivery failed: {e:?}"),
        }
    }
    fn partition(&mut self, allowed: &[u64]) {
        self.blocked.clear();
        for a in self.replicas.keys() {
            for b in self.replicas.keys() {
                if allowed.contains(a) != allowed.contains(b) {
                    self.blocked.insert((*a, *b));
                }
            }
        }
    }
    fn heal(&mut self) {
        self.blocked.clear();
    }
    fn commands(&self, id: u64) -> Vec<Vec<u8>> {
        self.applied[&id]
            .iter()
            .filter_map(|e| match &e.payload {
                EntryPayload::Command { bytes, .. } => Some(bytes.clone()),
                _ => None,
            })
            .collect()
    }
    fn propose(&mut self, id: u64, operation: u128, byte: u8) {
        self.act(
            id,
            Event::Propose {
                operation: OperationId::new(operation).unwrap(),
                bytes: vec![byte],
            },
        );
        self.pump();
    }
    fn assert_safety(&self) {
        for left in self.applied.values() {
            for right in self.applied.values() {
                for (a, b) in left.iter().zip(right) {
                    assert_eq!(a, b, "different committed entries at one index");
                }
            }
        }
        let mut leaders = BTreeMap::new();
        for (id, r) in &self.replicas {
            if r.core.role() == Role::Leader {
                assert!(
                    leaders
                        .insert(r.core.state().hard_state.term, *id)
                        .is_none(),
                    "two leaders in same term"
                );
            }
        }
    }
}
#[cfg(feature = "native")]
fn native(id: u64) -> NativeLogStore<ModelIo> {
    NativeLogStore::create(
        ModelIo::default(),
        identity(id as u128),
        LogLimits::default(),
    )
    .unwrap()
}

fn basic_history<S: LogStore>(mut cluster: Cluster<S>) {
    cluster.act(1, Event::Campaign);
    cluster.pump();
    assert_eq!(cluster.replicas[&1].core.role(), Role::Leader);
    cluster.propose(1, 10, 10);
    for id in 1..=3 {
        assert_eq!(cluster.commands(id), vec![vec![10]]);
    }
    cluster.partition(&[1]);
    cluster.propose(1, 20, 20);
    assert_eq!(cluster.commands(1), vec![vec![10]]);
    cluster.act(2, Event::Campaign);
    cluster.pump();
    assert_eq!(cluster.replicas[&2].core.role(), Role::Leader);
    cluster.propose(2, 30, 30);
    assert_eq!(cluster.commands(2), vec![vec![10], vec![30]]);
    cluster.heal();
    cluster.act(2, Event::Heartbeat);
    cluster.pump();
    for id in 1..=3 {
        assert_eq!(cluster.commands(id), vec![vec![10], vec![30]]);
    }
    assert_eq!(cluster.replicas[&1].core.role(), Role::Follower);
    assert!(cluster.replicas[&1].core.state().generation.get() > 1);
}
#[test]
#[cfg(feature = "native")]
fn three_node_commit_partition_election_and_conflict_repair_native_and_host() {
    basic_history(Cluster::new(bootstrap(1, 3), native));
    basic_history(Cluster::new(bootstrap(1, 3), |id| {
        HostLogStore::new(id as u128)
    }));
}
#[test]
#[cfg(feature = "native")]
fn election_ballot_and_commit_effects_wait_for_exact_persistence() {
    let mut store = native(1);
    append(&mut store, vec![LogMutation::Create(bootstrap(1, 3))]);
    let mut core = Raft::recover(
        node(1),
        store.binding(),
        store.state(group(1)).unwrap(),
        store.limits(),
    )
    .unwrap();
    let effects = core.step(Event::Campaign).unwrap();
    let [Effect::Persist(update)] = effects.as_slice() else {
        panic!("ballots escaped before persistence")
    };
    assert_eq!(core.state().hard_state.term, 0);
    assert_eq!(core.step(Event::Heartbeat), Err(RaftError::Busy));
    let tickets = store
        .append_batch(vec![LogMutation::Update(update.clone())])
        .unwrap();
    core.admitted(tickets[0]).unwrap();
    let mut wrong = tickets[0];
    wrong.generation = LogGeneration::new(99).unwrap();
    assert_eq!(
        core.complete(&DurableLog {
            tickets: vec![wrong]
        }),
        Err(RaftError::WrongCompletion)
    );
    wrong = tickets[0];
    wrong.binding.session = StoreSession::new(99).unwrap();
    assert_eq!(
        core.complete(&DurableLog {
            tickets: vec![wrong]
        }),
        Err(RaftError::WrongCompletion)
    );
    let effects = core.complete(&store.barrier(&tickets).unwrap()).unwrap();
    assert_eq!(effects.len(), 2);
    assert!(effects.iter().all(|e| matches!(
        e,
        Effect::Send(Message {
            rpc: Rpc::Vote { .. },
            ..
        })
    )));
    assert_eq!(core.state().hard_state.voted_for, Some(node(1)));
}
#[test]
#[cfg(feature = "native")]
fn no_quorum_means_no_committed_application_effects() {
    let mut cluster = Cluster::new(bootstrap(1, 3), native);
    cluster.partition(&[1]);
    cluster.act(1, Event::Campaign);
    cluster.pump();
    assert_eq!(cluster.replicas[&1].core.role(), Role::Candidate);
    assert!(cluster.applied[&1].is_empty());
    assert_eq!(
        cluster
            .replicas
            .get_mut(&1)
            .unwrap()
            .core
            .step(Event::Propose {
                operation: OperationId::new(1).unwrap(),
                bytes: vec![1]
            }),
        Err(RaftError::NotLeader)
    );
}
#[test]
#[cfg(feature = "native")]
fn duplicate_reordered_messages_cannot_commit_different_entries() {
    let mut cluster = Cluster::new(bootstrap(1, 3), native);
    cluster.act(1, Event::Campaign);
    let duplicate = cluster.messages.front().unwrap().clone();
    cluster.messages.push_front(duplicate);
    cluster.pump();
    for op in 1..=20 {
        cluster.act(
            1,
            Event::Propose {
                operation: OperationId::new(op).unwrap(),
                bytes: vec![op as u8],
            },
        );
        let saved = cluster.messages.front().unwrap().clone();
        cluster.messages.push_back(saved.clone());
        cluster.pump();
        // Delayed request and response contexts cannot certify a later index.
        cluster.messages.push_front(saved);
        cluster.pump();
    }
    for id in 1..=3 {
        assert_eq!(cluster.commands(id).len(), 20);
    }
}
#[test]
#[cfg(feature = "native")]
fn stale_incarnations_configuration_and_old_session_responses_are_rejected() {
    let mut cluster = Cluster::new(bootstrap(1, 3), native);
    cluster.act(1, Event::Campaign);
    let original = cluster.messages.front().unwrap().clone();
    for kind in 0..3 {
        let mut message = original.clone();
        match kind {
            0 => message.group.incarnation = GroupIncarnation::new(2).unwrap(),
            1 => message.sender.identity.incarnation = StoreIncarnation::new(2).unwrap(),
            _ => {
                message.configuration = ConfigurationId::new(2).unwrap();
                message.rpc = Rpc::ReadProbe;
            }
        };
        assert_eq!(
            cluster
                .replicas
                .get_mut(&2)
                .unwrap()
                .core
                .step(Event::Receive(message)),
            Err(RaftError::WrongIdentity)
        );
    }
    cluster.pump();
    let replica = cluster.replicas.get_mut(&1).unwrap();
    let term = replica.core.state().hard_state.term;
    let reply = Message {
        group: group(1),
        configuration: ConfigurationId::new(1).unwrap(),
        from: node(2),
        sender: cluster.replicas[&2].store.binding(),
        to: node(1),
        term,
        context: RequestContext {
            origin: StoreBinding {
                session: StoreSession::new(99).unwrap(),
                ..cluster.replicas[&1].store.binding()
            },
            sequence: 1,
        },
        rpc: Rpc::Appended {
            success: true,
            matching_index: 999,
        },
    };
    assert_eq!(
        cluster
            .replicas
            .get_mut(&1)
            .unwrap()
            .core
            .step(Event::Receive(reply)),
        Err(RaftError::WrongIdentity)
    );
}
#[test]
fn recursive_policy_is_used_for_both_election_and_commit() {
    let mut b = bootstrap(1, 9);
    b.policy = Policy::new(
        Tree::Majority(
            (0..3)
                .map(|site| {
                    Tree::Majority((1..=3).map(|n| Tree::Voter(node(site * 3 + n))).collect())
                })
                .collect(),
        ),
        Limits::default(),
    )
    .unwrap();
    let mut cluster = Cluster::new(b.clone(), |id| HostLogStore::new(id as u128));
    cluster.partition(&[1, 2, 4, 5]);
    cluster.act(1, Event::Campaign);
    cluster.pump();
    assert_eq!(cluster.replicas[&1].core.role(), Role::Leader);
    cluster.propose(1, 1, 7);
    assert_eq!(cluster.commands(1), vec![vec![7]]);
    // The leader's previous durable progress cannot certify a later command
    // when only a flat majority, rather than the recursive policy, is reachable.
    cluster.partition(&[1, 2, 3, 4, 7]);
    cluster.propose(1, 2, 8);
    assert_eq!(cluster.commands(1), vec![vec![7]]);
    cluster.heal();
    cluster.act(1, Event::Heartbeat);
    cluster.pump();
    assert_eq!(cluster.commands(1), vec![vec![7], vec![8]]);
    let mut cluster = Cluster::new(b, |id| HostLogStore::new(id as u128));
    cluster.partition(&[1, 2, 3, 4, 7]);
    cluster.act(1, Event::Campaign);
    cluster.pump();
    assert_eq!(cluster.replicas[&1].core.role(), Role::Candidate);
    assert!(cluster.applied[&1].is_empty());
}
#[test]
#[cfg(feature = "native")]
fn native_disk_histories_survive_recovery_on_every_replica() {
    let root = std::env::temp_dir().join(format!("voteboat-raft-{}", std::process::id()));
    std::fs::create_dir(&root).unwrap();
    let mut cluster = Cluster::new(bootstrap(1, 3), |id| {
        NativeLogStore::create(
            FileLogIo::create(root.join(id.to_string())).unwrap(),
            identity(id as u128),
            LogLimits::default(),
        )
        .unwrap()
    });
    cluster.act(1, Event::Campaign);
    cluster.pump();
    cluster.propose(1, 1, 42);
    let expected = cluster.replicas[&1].core.replay_committed().to_vec();
    drop(cluster);
    for id in 1..=3 {
        let store = NativeLogStore::recover(
            FileLogIo::open(root.join(id.to_string())).unwrap(),
            identity(id as u128),
            LogLimits::default(),
        )
        .unwrap();
        let raft = Raft::recover(
            node(id),
            store.binding(),
            store.state(group(1)).unwrap(),
            store.limits(),
        )
        .unwrap();
        assert_eq!(raft.replay_committed(), expected);
    }
    std::fs::remove_dir_all(root).unwrap();
}

#[test]
#[cfg(feature = "native")]
fn seeded_fault_schedules_preserve_committed_prefixes_and_acknowledged_recovery() {
    for seed in 1..=32u64 {
        let images: BTreeMap<_, _> = (1..=3).map(|id| (id, ModelIo::default())).collect();
        let mut cluster = Cluster::new(bootstrap(1, 3), |id| {
            NativeLogStore::create(
                images[&id].clone(),
                identity(id as u128),
                LogLimits::default(),
            )
            .unwrap()
        });
        let mut random = seed;
        for step in 0..256u64 {
            random ^= random << 13;
            random ^= random >> 7;
            random ^= random << 17;
            let id = (random >> 8) % 3 + 1;
            match random % 9 {
                0 => cluster.act(id, Event::Campaign),
                1 => {
                    if cluster.replicas[&id].core.role() == Role::Leader {
                        cluster.act(
                            id,
                            Event::Propose {
                                operation: OperationId::new((seed * 1000 + step + 1) as u128)
                                    .unwrap(),
                                bytes: vec![step as u8],
                            },
                        );
                    }
                }
                2 => cluster.partition(&[id]),
                3 => cluster.heal(),
                4 => {
                    if let Some(message) = cluster.messages.pop_back() {
                        cluster.deliver(message);
                    }
                }
                5 => {
                    if let Some(message) = cluster.messages.front().cloned() {
                        cluster.messages.push_back(message);
                    }
                }
                6 => {
                    let acknowledged = cluster.applied[&id].clone();
                    drop(cluster.replicas.remove(&id).unwrap());
                    images[&id].0.borrow_mut().power_loss();
                    let store = NativeLogStore::recover(
                        images[&id].clone(),
                        identity(id as u128),
                        LogLimits::default(),
                    )
                    .unwrap();
                    let core = Raft::recover(
                        node(id),
                        store.binding(),
                        store.state(group(1)).unwrap(),
                        store.limits(),
                    )
                    .unwrap();
                    assert!(
                        core.replay_committed().starts_with(&acknowledged),
                        "seed={seed} step={step}"
                    );
                    cluster.applied.insert(id, core.replay_committed().to_vec());
                    cluster.replicas.insert(id, Replica { core, store });
                }
                7 => cluster.act(id, Event::Heartbeat),
                _ => cluster.pump(),
            }
            cluster.assert_safety();
        }
        cluster.heal();
        cluster.pump();
        // Force an eventually stable majority using the freshest recovered log.
        let candidate = *cluster
            .replicas
            .iter()
            .max_by_key(|(_, r)| (r.core.state().last_term(), r.core.state().last_index()))
            .unwrap()
            .0;
        cluster.act(candidate, Event::Campaign);
        cluster.pump();
        // Other replicas may have larger terms after isolated campaigns. A
        // second election persists the learned term before soliciting ballots.
        if cluster.replicas[&candidate].core.role() != Role::Leader {
            cluster.act(candidate, Event::Campaign);
            cluster.pump();
        }
        assert_eq!(
            cluster.replicas[&candidate].core.role(),
            Role::Leader,
            "seed={seed}"
        );
        cluster.propose(candidate, (seed * 1000 + 999) as u128, 255);
        for id in 1..=3 {
            assert_eq!(
                cluster.commands(id).last(),
                Some(&vec![255]),
                "seed={seed} node={id}"
            );
        }
    }
}

#[test]
#[cfg(feature = "native")]
fn failed_election_persistence_fences_core_without_network_effects() {
    let io = ModelIo::default();
    let mut store = NativeLogStore::create(io.clone(), identity(1), LogLimits::default()).unwrap();
    append(&mut store, vec![LogMutation::Create(bootstrap(1, 3))]);
    let mut core = Raft::recover(
        node(1),
        store.binding(),
        store.state(group(1)).unwrap(),
        store.limits(),
    )
    .unwrap();
    let effects = core.step(Event::Campaign).unwrap();
    let [Effect::Persist(update)] = effects.as_slice() else {
        panic!("premature ballot")
    };
    io.0.borrow_mut().fault = Fault::Sync;
    assert!(persist_effect(&mut core, &mut store, update.clone()).is_err());
    assert_eq!(core.step(Event::Heartbeat), Err(RaftError::Fenced));
    assert!(core.replay_committed().is_empty());
}

#[test]
#[cfg(feature = "native")]
fn single_voter_commits_only_after_local_persistence() {
    let mut cluster = Cluster::new(bootstrap(1, 1), native);
    cluster.act(1, Event::Campaign);
    cluster.pump();
    assert_eq!(cluster.replicas[&1].core.role(), Role::Leader);
    cluster.propose(1, 1, 9);
    assert_eq!(cluster.commands(1), vec![vec![9]]);
}

#[test]
fn three_node_history_with_only_host_components() {
    basic_history(Cluster::new(bootstrap(1, 3), |id| {
        HostLogStore::new(id as u128)
    }));
}

#[test]
#[cfg(feature = "native")]
fn persistence_driver_rejects_a_different_body_with_identical_ticket_metadata() {
    let io = ModelIo::default();
    let mut cluster = Cluster::new(bootstrap(1, 1), |id| {
        NativeLogStore::create(io.clone(), identity(id as u128), LogLimits::default()).unwrap()
    });
    cluster.act(1, Event::Campaign);
    cluster.pump();
    let replica = cluster.replicas.get_mut(&1).unwrap();
    let effects = replica
        .core
        .step(Event::Propose {
            operation: OperationId::new(1).unwrap(),
            bytes: vec![1],
        })
        .unwrap();
    let effects: [Effect; 1] = effects.try_into().unwrap();
    let [Effect::Persist(mut update)] = effects else {
        panic!("expected persistence")
    };
    let EntryPayload::Command { bytes, .. } =
        &mut update.suffix.as_mut().unwrap().entries[0].payload
    else {
        unreachable!()
    };
    *bytes = vec![2];
    let before = io.0.borrow().log.clone();
    assert_eq!(
        persist_effect(&mut replica.core, &mut replica.store, update),
        Err(RaftError::WrongCompletion)
    );
    assert_eq!(io.0.borrow().log, before);
    assert_eq!(replica.core.step(Event::Heartbeat), Err(RaftError::Fenced));
}

fn read_request(id: u64) -> Event {
    Event::Read {
        request: ReadRequestId::new(id).unwrap(),
    }
}
fn read_history<S: LogStore>(mut cluster: Cluster<S>) {
    cluster.act(1, Event::Campaign);
    cluster.pump();
    cluster.propose(1, 1, 7);
    let required = cluster.applied[&1].len() as u64;
    cluster.act(1, read_request(1));
    assert!(
        cluster.reads.is_empty(),
        "local durability is not a read quorum"
    );
    cluster.pump();
    let barrier = cluster.reads[&1][0];
    assert_eq!(barrier.index(), required);
    assert_eq!(barrier.group(), group(1));
    assert_eq!(barrier.configuration(), ConfigurationId::new(1).unwrap());
    let leader = &mut cluster.replicas.get_mut(&1).unwrap().core;
    assert_eq!(
        leader.finish_read(&barrier, required - 1),
        Err(RaftError::NotApplied)
    );
    leader.finish_read(&barrier, required).unwrap();
    assert_eq!(
        leader.finish_read(&barrier, required),
        Err(RaftError::StaleRead)
    );

    cluster.partition(&[1]);
    cluster.act(1, read_request(2));
    cluster.pump();
    assert_eq!(cluster.reads[&1].len(), 1);
    cluster.act(2, Event::Campaign);
    cluster.pump();
    cluster.propose(2, 2, 9);
    cluster.act(1, Event::Heartbeat);
    cluster.pump();
    assert_eq!(
        cluster.reads[&1].len(),
        1,
        "isolated previous leader cannot serve a new read"
    );
    cluster.heal();
    cluster.act(2, Event::Heartbeat);
    cluster.pump();
    assert_eq!(cluster.replicas[&1].core.role(), Role::Follower);
    assert_eq!(
        cluster
            .replicas
            .get_mut(&1)
            .unwrap()
            .core
            .step(read_request(3)),
        Err(RaftError::NotLeader)
    );
    cluster.act(2, read_request(1));
    cluster.pump();
    let barrier = cluster.reads[&2][0];
    assert_eq!(barrier.index(), cluster.applied[&2].len() as u64);
    assert!(barrier.term() > cluster.reads[&1][0].term());
}

#[test]
fn read_authority_and_applied_prefix_history_with_host_storage() {
    read_history(Cluster::new(bootstrap(1, 3), |id| {
        HostLogStore::new(id as u128)
    }));
}
#[test]
#[cfg(feature = "native")]
fn read_authority_and_applied_prefix_history_with_native_storage() {
    read_history(Cluster::new(bootstrap(1, 3), native));
}

#[test]
fn read_requires_current_term_commit_and_ready_tokens_are_bounded_and_cancelable() {
    let mut cluster = Cluster::new(bootstrap(1, 3), |id| HostLogStore::new(id as u128));
    cluster.act(1, Event::Campaign);
    while cluster.replicas[&1].core.role() != Role::Leader {
        let m = cluster.messages.pop_front().unwrap();
        cluster.deliver(m);
    }
    assert_eq!(cluster.replicas[&1].core.state().commit_index, 0);
    assert_eq!(
        cluster
            .replicas
            .get_mut(&1)
            .unwrap()
            .core
            .step(read_request(1)),
        Err(RaftError::ReadNotReady)
    );
    cluster.pump();
    cluster.act(1, read_request(1));
    assert_eq!(
        cluster
            .replicas
            .get_mut(&1)
            .unwrap()
            .core
            .step(read_request(2)),
        Err(RaftError::ReadInFlight)
    );
    cluster.pump();
    let barrier = cluster.reads[&1][0];
    assert_eq!(
        cluster
            .replicas
            .get_mut(&1)
            .unwrap()
            .core
            .step(read_request(2)),
        Err(RaftError::ReadInFlight)
    );
    cluster.act(
        1,
        Event::CancelRead {
            request: ReadRequestId::new(1).unwrap(),
        },
    );
    assert_eq!(
        cluster
            .replicas
            .get_mut(&1)
            .unwrap()
            .core
            .finish_read(&barrier, 1),
        Err(RaftError::StaleRead)
    );
    assert_eq!(
        cluster
            .replicas
            .get_mut(&1)
            .unwrap()
            .core
            .step(read_request(1)),
        Err(RaftError::StaleRead)
    );
    cluster.act(1, read_request(2));
    cluster.act(
        1,
        Event::CancelRead {
            request: ReadRequestId::new(2).unwrap(),
        },
    );
    cluster.pump();
    assert_eq!(
        cluster.reads[&1].len(),
        1,
        "cancelled probes cannot re-create authority"
    );
    cluster.act(1, read_request(3));
    cluster.pump();
    let ready = cluster.reads[&1][1];
    cluster.act(1, Event::Campaign);
    assert_eq!(
        cluster
            .replicas
            .get_mut(&1)
            .unwrap()
            .core
            .finish_read(&ready, 1),
        Err(RaftError::NotLeader)
    );
}

#[test]
fn recursive_reads_require_fresh_distinct_voters_for_the_exact_context() {
    let mut b = bootstrap(1, 9);
    b.policy = Policy::new(
        Tree::Majority(
            (0..3)
                .map(|site| {
                    Tree::Majority((1..=3).map(|n| Tree::Voter(node(site * 3 + n))).collect())
                })
                .collect(),
        ),
        Limits::default(),
    )
    .unwrap();
    let mut cluster = Cluster::new(b, |id| HostLogStore::new(id as u128));
    cluster.act(1, Event::Campaign);
    cluster.pump();
    cluster.act(1, read_request(1));
    // Collect genuine responses before delivery; duplicated packets from one
    // voter must never stand in for distinct leaves in the recursive policy.
    let mut replies = Vec::new();
    while let Some(m) = cluster.messages.pop_front() {
        if matches!(m.rpc, Rpc::ReadAck) {
            replies.push(m);
        } else {
            cluster.deliver(m);
        }
    }
    let from2 = replies.iter().find(|m| m.from == node(2)).unwrap().clone();
    for _ in 0..20 {
        cluster.deliver(from2.clone());
    }
    assert!(cluster.reads.is_empty());
    // Fresh {1,2,4,5} is a quorum; {1,2,3,4,7} is merely a flat majority.
    for id in [3, 4, 7] {
        cluster.deliver(replies.iter().find(|m| m.from == node(id)).unwrap().clone());
    }
    assert!(cluster.reads.is_empty());
    cluster.deliver(replies.iter().find(|m| m.from == node(5)).unwrap().clone());
    let first = cluster.reads[&1][0];
    cluster
        .replicas
        .get_mut(&1)
        .unwrap()
        .core
        .finish_read(&first, first.index())
        .unwrap();
    cluster.act(1, read_request(2));
    cluster.messages.clear();
    for m in &replies {
        cluster.deliver(m.clone());
    }
    assert_eq!(
        cluster.reads[&1].len(),
        1,
        "old read quorum must not authorize a new invocation"
    );
    cluster.partition(&[1, 2, 3, 4, 7]);
    cluster.act(1, Event::Heartbeat);
    cluster.pump();
    assert_eq!(cluster.reads[&1].len(), 1);
    cluster.partition(&[1, 2, 4, 5]);
    cluster.act(1, Event::Heartbeat);
    cluster.pump();
    assert_eq!(
        cluster.reads[&1].len(),
        2,
        "heartbeat retries the outstanding read context"
    );
}

#[test]
fn read_probe_higher_term_waits_for_its_exact_durability_ticket() {
    let mut store = HostLogStore::new(2);
    append(&mut store, vec![LogMutation::Create(bootstrap(1, 3))]);
    let mut core = Raft::recover(
        node(2),
        store.binding(),
        store.state(group(1)).unwrap(),
        store.limits(),
    )
    .unwrap();
    let origin = HostLogStore::new(1).binding();
    let probe = Message {
        group: group(1),
        configuration: ConfigurationId::new(1).unwrap(),
        from: node(1),
        sender: origin,
        to: node(2),
        term: 7,
        context: RequestContext {
            origin,
            sequence: 1,
        },
        rpc: Rpc::ReadProbe,
    };
    let effects = core.step(Event::Receive(probe)).unwrap();
    let [Effect::Persist(update)] = effects.as_slice() else {
        panic!("read ack escaped before term durability");
    };
    assert_eq!(core.state().hard_state.term, 0);
    let tickets = store
        .append_batch(vec![LogMutation::Update(update.clone())])
        .unwrap();
    core.admitted(tickets[0]).unwrap();
    assert_eq!(
        core.complete(&DurableLog { tickets: vec![] }),
        Err(RaftError::WrongCompletion)
    );
    let completion = store.barrier(&tickets).unwrap();
    let effects = core.complete(&completion).unwrap();
    assert!(matches!(
        effects.as_slice(),
        [Effect::Send(Message {
            term: 7,
            rpc: Rpc::ReadAck,
            ..
        })]
    ));
}

#[test]
fn counter_read_waits_for_application_and_cannot_reuse_or_move_barrier() {
    use voteboat::application::*;
    struct HostApplication(Counter);
    impl StateMachine for HostApplication {
        type Receipt = CounterReceipt;
        fn applied_index(&self) -> u64 {
            self.0.applied_index()
        }
        fn apply_batch(
            &mut self,
            entries: &[LogEntry],
        ) -> Result<Vec<Self::Receipt>, ApplicationError> {
            self.0.apply_batch(entries)
        }
    }
    impl ReadableStateMachine for HostApplication {
        type Query = &'static str;
        type ReadResult = String;
        fn read_at(&self, required: u64, units: Self::Query) -> Result<String, ApplicationError> {
            Ok(format!("{} {units}", self.0.read_applied(required)?))
        }
    }
    let mut cluster = Cluster::new(bootstrap(1, 3), |id| HostLogStore::new(id as u128));
    cluster.act(1, Event::Campaign);
    cluster.pump();
    cluster.act(
        1,
        Event::Propose {
            operation: OperationId::new(1).unwrap(),
            bytes: 13i64.to_le_bytes().to_vec(),
        },
    );
    cluster.pump();
    cluster.act(1, read_request(1));
    cluster.pump();
    let barrier = cluster.reads[&1][0];
    let mut application = Counter::new(20).unwrap();
    assert_eq!(
        read_at_barrier(
            &mut cluster.replicas.get_mut(&1).unwrap().core,
            &barrier,
            &application,
            ()
        ),
        Err(ReadError::Consensus(RaftError::NotApplied))
    );
    application.apply_batch(&cluster.applied[&1]).unwrap();
    assert_eq!(
        read_at_barrier(
            &mut cluster.replicas.get_mut(&1).unwrap().core,
            &barrier,
            &application,
            ()
        ),
        Ok(13)
    );
    assert_eq!(
        read_at_barrier(
            &mut cluster.replicas.get_mut(&1).unwrap().core,
            &barrier,
            &application,
            ()
        ),
        Err(ReadError::Consensus(RaftError::StaleRead))
    );
    assert_eq!(
        read_at_barrier(
            &mut cluster.replicas.get_mut(&2).unwrap().core,
            &barrier,
            &application,
            ()
        ),
        Err(ReadError::Consensus(RaftError::NotLeader))
    );
    cluster.act(1, read_request(2));
    cluster.pump();
    let host_barrier = cluster.reads[&1][1];
    let host = HostApplication(application);
    assert_eq!(
        read_at_barrier(
            &mut cluster.replicas.get_mut(&1).unwrap().core,
            &host_barrier,
            &host,
            "units"
        ),
        Ok("13 units".to_string())
    );
}

#[test]
fn single_voter_read_and_restart_reject_prior_session_authority() {
    let mut cluster = Cluster::new(bootstrap(1, 1), |id| HostLogStore::new(id as u128));
    cluster.act(1, Event::Campaign);
    cluster.act(1, read_request(1));
    let barrier = cluster.reads[&1][0];
    let store = &cluster.replicas[&1].store;
    let mut reopened_binding = store.binding();
    reopened_binding.session = StoreSession::new(reopened_binding.session.get() + 1).unwrap();
    let mut core = Raft::recover(
        node(1),
        reopened_binding,
        store.state(group(1)).unwrap(),
        store.limits(),
    )
    .unwrap();
    assert_eq!(
        core.finish_read(&barrier, barrier.index()),
        Err(RaftError::NotLeader)
    );
    // A new core cannot recover a volatile ready read from the persisted log.
    assert_eq!(core.step(read_request(1)), Err(RaftError::NotLeader));
    cluster.replicas.get_mut(&1).unwrap().core.storage_failed();
    assert_eq!(
        cluster
            .replicas
            .get_mut(&1)
            .unwrap()
            .core
            .finish_read(&barrier, barrier.index()),
        Err(RaftError::Fenced)
    );
}

#[test]
fn delayed_read_acknowledgements_cannot_authorize_a_later_read_after_leader_change() {
    let mut cluster = Cluster::new(bootstrap(1, 3), |id| HostLogStore::new(id as u128));
    cluster.act(1, Event::Campaign);
    cluster.pump();
    cluster.propose(1, 1, 7);
    cluster.act(1, read_request(1));
    let mut replies = Vec::new();
    while let Some(m) = cluster.messages.pop_front() {
        if matches!(m.rpc, Rpc::ReadAck) {
            replies.push(m);
        } else {
            cluster.deliver(m);
        }
    }
    assert_eq!(replies.len(), 2);
    // The read began before this new leader/write. Its delayed old quorum may
    // finish that overlapping read at the old prefix, but cannot be reused by
    // another invocation after the new write's completion.
    cluster.partition(&[1]);
    cluster.act(2, Event::Campaign);
    cluster.pump();
    cluster.propose(2, 2, 9);
    assert_eq!(cluster.commands(2), vec![vec![7], vec![9]]);
    cluster.heal();
    cluster.deliver(replies[0].clone());
    let first = cluster.reads[&1][0];
    assert_eq!(first.index(), 2);
    cluster
        .replicas
        .get_mut(&1)
        .unwrap()
        .core
        .finish_read(&first, 2)
        .unwrap();
    cluster.act(1, read_request(2));
    for m in replies {
        cluster.deliver(m);
    }
    assert_eq!(cluster.reads[&1].len(), 1);
    cluster.pump();
    assert_eq!(cluster.reads[&1].len(), 1);
    assert_eq!(cluster.replicas[&1].core.role(), Role::Follower);
    cluster.act(2, read_request(1));
    cluster.pump();
    let fresh = cluster.reads[&2][0];
    assert_eq!(fresh.index(), 4);
    assert_eq!(cluster.commands(2), vec![vec![7], vec![9]]);
}

#[test]
fn read_ack_rejects_foreign_scope_old_term_and_previous_store_session() {
    let mut cluster = Cluster::new(bootstrap(1, 3), |id| HostLogStore::new(id as u128));
    cluster.act(1, Event::Campaign);
    cluster.pump();
    cluster.act(1, read_request(1));
    let mut reply = None;
    while let Some(m) = cluster.messages.pop_front() {
        if matches!(m.rpc, Rpc::ReadAck) {
            reply = Some(m);
        } else {
            cluster.deliver(m);
        }
    }
    let reply = reply.unwrap();
    let core = &mut cluster.replicas.get_mut(&1).unwrap().core;
    for mutate in 0..5 {
        let mut stale = reply.clone();
        match mutate {
            0 => stale.group.incarnation = GroupIncarnation::new(2).unwrap(),
            1 => stale.configuration = ConfigurationId::new(2).unwrap(),
            2 => stale.sender.identity.incarnation = StoreIncarnation::new(2).unwrap(),
            3 => stale.context.origin.session = StoreSession::new(99).unwrap(),
            4 => stale.group.id = GroupId::new(99).unwrap(),
            _ => unreachable!(),
        }
        assert_eq!(
            core.step(Event::Receive(stale)),
            Err(RaftError::WrongIdentity)
        );
    }
    let mut stale_term = reply.clone();
    stale_term.term += 1;
    // Establish the new term while preserving the old pending read response.
    let effects = core.step(Event::Receive(stale_term)).unwrap();
    let [Effect::Persist(update)] = effects.as_slice() else {
        panic!()
    };
    let replica = cluster.replicas.get_mut(&1).unwrap();
    persist_effect(&mut replica.core, &mut replica.store, update.clone()).unwrap();
    assert_eq!(
        replica.core.step(Event::Receive(reply)).unwrap(),
        Vec::new()
    );
    assert_eq!(replica.core.role(), Role::Follower);
    assert!(cluster.reads.is_empty());
}

#[test]
#[cfg(feature = "native")]
fn native_restart_and_failed_read_term_sync_never_release_old_authority() {
    let io = ModelIo::default();
    let mut cluster = Cluster::new(bootstrap(1, 1), |id| {
        NativeLogStore::create(io.clone(), identity(id as u128), LogLimits::default()).unwrap()
    });
    cluster.act(1, Event::Campaign);
    cluster.act(1, read_request(1));
    let old = cluster.reads[&1][0];
    let previous_session = cluster.replicas[&1].store.binding().session;
    drop(cluster);
    io.0.borrow_mut().power_loss();
    let store = NativeLogStore::recover(io.clone(), identity(1), LogLimits::default()).unwrap();
    assert!(store.binding().session > previous_session);
    let core = Raft::recover(
        node(1),
        store.binding(),
        store.state(group(1)).unwrap(),
        store.limits(),
    )
    .unwrap();
    let mut cluster = Cluster {
        replicas: BTreeMap::from([(1, Replica { core, store })]),
        applied: BTreeMap::from([(
            1,
            vec![LogEntry {
                index: 1,
                term: 1,
                payload: EntryPayload::Noop,
            }],
        )]),
        messages: VecDeque::new(),
        blocked: BTreeSet::new(),
        reads: BTreeMap::new(),
    };
    cluster.act(1, Event::Campaign);
    cluster.act(1, read_request(1));
    let fresh = cluster.reads[&1][0];
    let core = &mut cluster.replicas.get_mut(&1).unwrap().core;
    assert_eq!(
        core.finish_read(&old, fresh.index()),
        Err(RaftError::StaleRead)
    );
    core.finish_read(&fresh, fresh.index()).unwrap();

    // A follower's higher-term probe creates a real native hard-state write.
    // Neither a failed sync nor recovery of its interrupted tail can emit ack.
    let io = ModelIo::default();
    let mut store = NativeLogStore::create(io.clone(), identity(2), LogLimits::default()).unwrap();
    append(&mut store, vec![LogMutation::Create(bootstrap(1, 3))]);
    let mut core = Raft::recover(
        node(2),
        store.binding(),
        store.state(group(1)).unwrap(),
        store.limits(),
    )
    .unwrap();
    let origin = HostLogStore::new(1).binding();
    let effects = core
        .step(Event::Receive(Message {
            group: group(1),
            configuration: ConfigurationId::new(1).unwrap(),
            from: node(1),
            sender: origin,
            to: node(2),
            term: 1,
            context: RequestContext {
                origin,
                sequence: 3,
            },
            rpc: Rpc::ReadProbe,
        }))
        .unwrap();
    let [Effect::Persist(update)] = effects.as_slice() else {
        panic!()
    };
    io.0.borrow_mut().fault = Fault::Sync;
    assert!(persist_effect(&mut core, &mut store, update.clone()).is_err());
    assert_eq!(core.step(Event::Heartbeat), Err(RaftError::Fenced));
    drop(store);
    io.0.borrow_mut().power_loss();
    let recovered = NativeLogStore::recover(io, identity(2), LogLimits::default()).unwrap();
    assert_eq!(recovered.state(group(1)).unwrap().hard_state.term, 0);
}
