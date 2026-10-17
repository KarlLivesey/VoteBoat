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
//! Volatile in-process embedding against its durable twin, public API only.
mod support;
use std::collections::{BTreeMap, BTreeSet, VecDeque};
use support::{snapshot::HostSnapshots, *};
use voteboat::{application::*, identity::*, log::*, raft::*, snapshot::*};

enum Log {
    Durable(HostLogStore),
    Volatile(VolatileLog),
}
struct Replica {
    core: Raft,
    log: Log,
    snapshots: HostSnapshots,
    application: Counter,
}
/// Persistence effects of either seam compare equal; every other effect must
/// match exactly, including message contexts and committed entries.
#[derive(Debug, Eq, PartialEq)]
enum Seen {
    Persist,
    Other(Box<Effect>),
}
struct Cluster {
    replicas: BTreeMap<u64, Replica>,
    messages: VecDeque<Message>,
    blocked: BTreeSet<(u64, u64)>,
    trace: Vec<(u64, Seen)>,
    installs: usize,
}
fn binding(id: u64) -> StoreBinding {
    StoreBinding {
        identity: identity(id as u128),
        session: StoreSession::new(1).unwrap(),
    }
}
fn host_snapshots(id: u64) -> HostSnapshots {
    let mut s = HostSnapshots::new();
    s.identity.store = identity(id as u128);
    s.binding.identity = s.identity.store;
    s
}
impl Cluster {
    fn new(volatile: bool) -> Self {
        let bootstrap = bootstrap(1, 3);
        let mut replicas = BTreeMap::new();
        for id in 1..=3 {
            let (core, log) = if volatile {
                let log = VolatileLog::new(binding(id), LogLimits::default()).unwrap();
                let state = log.bootstrap(&bootstrap).unwrap();
                let core = Raft::recover_volatile(node(id), &log, state).unwrap();
                (core, Log::Volatile(log))
            } else {
                let mut log = HostLogStore::new(id as u128);
                append(&mut log, vec![LogMutation::Create(bootstrap.clone())]);
                let state = log.state(bootstrap.group).unwrap();
                let core = Raft::recover(node(id), log.binding(), state, log.limits()).unwrap();
                (core, Log::Durable(log))
            };
            assert_eq!(core.is_volatile(), volatile);
            replicas.insert(
                id,
                Replica {
                    core,
                    log,
                    snapshots: host_snapshots(id),
                    application: Counter::new(100).unwrap(),
                },
            );
        }
        Self {
            replicas,
            messages: VecDeque::new(),
            blocked: BTreeSet::new(),
            trace: Vec::new(),
            installs: 0,
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
    fn batch(&mut self, id: u64, operations: &[(u128, i64)]) {
        let proposals = operations
            .iter()
            .map(|&(operation, delta)| Proposal {
                operation: OperationId::new(operation).unwrap(),
                bytes: delta.to_le_bytes().to_vec(),
            })
            .collect();
        let core = &mut self.replicas.get_mut(&id).unwrap().core;
        let effects = core.propose_batch(proposals).unwrap();
        self.effects(id, effects);
        self.pump();
    }
    fn propose(&mut self, id: u64, operation: u128, delta: i64) {
        self.act(
            id,
            Event::Propose {
                operation: OperationId::new(operation).unwrap(),
                bytes: delta.to_le_bytes().to_vec(),
            },
        );
        self.pump();
    }
    fn effects(&mut self, id: u64, effects: Vec<Effect>) {
        let mut queue = VecDeque::from(effects);
        while let Some(effect) = queue.pop_front() {
            let seen = match &effect {
                Effect::Persist(_) | Effect::PersistVolatile(_) => Seen::Persist,
                other => Seen::Other(Box::new(other.clone())),
            };
            self.trace.push((id, seen));
            let r = self.replicas.get_mut(&id).unwrap();
            let more = match (effect, &mut r.log) {
                (Effect::Persist(update), Log::Durable(log)) => {
                    persist_effect(&mut r.core, log, update).unwrap()
                }
                (Effect::PersistVolatile(persist), Log::Volatile(log)) => {
                    persist_volatile(&mut r.core, log, persist)
                        .unwrap()
                        .into_owned()
                }
                (Effect::Persist(_) | Effect::PersistVolatile(_), _) => {
                    panic!("persistence effect of the other seam")
                }
                (Effect::Send(message), _) => {
                    assert!(self.messages.len() < 4096);
                    self.messages.push_back(message);
                    Vec::new()
                }
                (Effect::Committed(entries), _) => {
                    r.application.apply_batch(&entries).unwrap();
                    Vec::new()
                }
                (Effect::ReadReady(barrier), _) => {
                    read_at_barrier(&mut r.core, &barrier, &r.application, ()).unwrap();
                    Vec::new()
                }
                (
                    Effect::SnapshotRequired {
                        to,
                        context,
                        reference,
                    },
                    _,
                ) => supply_snapshot(&r.core, &mut r.snapshots, to, context, reference).unwrap(),
                (Effect::StageSnapshot(message), Log::Durable(log)) => stage_snapshot_effect(
                    &mut r.core,
                    log,
                    &mut r.snapshots,
                    &r.application,
                    message,
                )
                .unwrap(),
                (Effect::StageSnapshot(message), Log::Volatile(log)) => {
                    stage_volatile_snapshot_effect(
                        &mut r.core,
                        log,
                        &mut r.snapshots,
                        &r.application,
                        message,
                    )
                    .unwrap()
                }
                (Effect::SnapshotInstalled(reference), log) => {
                    self.installs += 1;
                    match log {
                        Log::Durable(log) => finish_snapshot_install(
                            &mut r.core,
                            log,
                            &mut r.snapshots,
                            &mut r.application,
                            reference,
                        ),
                        Log::Volatile(log) => finish_volatile_snapshot_install(
                            &mut r.core,
                            log,
                            &mut r.snapshots,
                            &mut r.application,
                            reference,
                        ),
                    }
                    .unwrap()
                }
                (
                    Effect::VerifyLearnerReadiness(_)
                    | Effect::CheckpointRequired { .. }
                    | Effect::CheckpointCompacted(_),
                    _,
                ) => panic!("synchronous static history has no asynchronous checkpoints"),
            };
            queue.extend(more);
        }
    }
    fn pump(&mut self) {
        let mut events = 0;
        while let Some(m) = self.messages.pop_front() {
            events += 1;
            assert!(events < 10000, "history did not converge");
            if !self.blocked.contains(&(m.from.get(), m.to.get())) {
                self.act(m.to.get(), Event::Receive(m));
            }
        }
    }
    fn isolate(&mut self, node: u64) {
        self.blocked.clear();
        for a in 1..=3 {
            for b in 1..=3 {
                if (a == node) != (b == node) {
                    self.blocked.insert((a, b));
                }
            }
        }
    }
    fn compact(&mut self, id: u64) {
        let r = self.replicas.get_mut(&id).unwrap();
        let receipt = checkpoint_application(&r.core, &r.application, &mut r.snapshots).unwrap();
        let effects = match &mut r.log {
            Log::Durable(log) => compact_replica(
                &mut r.core,
                log,
                &mut r.snapshots,
                &r.application,
                receipt.reference(),
            ),
            Log::Volatile(log) => compact_volatile_replica(
                &mut r.core,
                log,
                &mut r.snapshots,
                &r.application,
                receipt.reference(),
            ),
        }
        .unwrap();
        self.effects(id, effects);
    }
    fn assert_twin(&self, durable: &Self) {
        assert_eq!(self.trace, durable.trace);
        assert_eq!(self.installs, durable.installs);
        for (id, r) in &self.replicas {
            let twin = &durable.replicas[id];
            assert_eq!(r.core.state(), twin.core.state());
            assert_eq!(r.core.role(), twin.core.role());
            let applied = r.application.applied_index();
            assert_eq!(applied, twin.application.applied_index());
            assert_eq!(
                r.application.read_applied(applied),
                twin.application.read_applied(applied)
            );
            let Log::Durable(log) = &twin.log else {
                unreachable!()
            };
            assert_eq!(&log.state(group(1)).unwrap(), twin.core.state());
        }
    }
}

fn history(cluster: &mut Cluster) {
    cluster.act(1, Event::Campaign);
    cluster.pump();
    cluster.propose(1, 1, 5);
    cluster.batch(1, &[(2, 1), (3, -2), (4, 10)]);
    cluster.act(1, Event::Heartbeat);
    cluster.pump();
    // An isolated leader's uncommitted append is replaced after re-election,
    // exercising the non-append (suffix replacement) transition.
    cluster.isolate(1);
    cluster.act(
        1,
        Event::Propose {
            operation: OperationId::new(90).unwrap(),
            bytes: 99i64.to_le_bytes().to_vec(),
        },
    );
    cluster.pump();
    cluster.act(2, Event::Campaign);
    cluster.pump();
    cluster.propose(2, 5, 3);
    cluster.batch(2, &[(6, 1), (7, 1)]);
    cluster.blocked.clear();
    cluster.act(2, Event::Heartbeat);
    cluster.pump();
    cluster.propose(2, 8, -1);
    cluster.act(
        2,
        Event::Read {
            request: ReadRequestId::new(1).unwrap(),
        },
    );
    cluster.pump();
}

#[test]
fn volatile_and_durable_clusters_reach_identical_logs_and_effects() {
    let mut volatile = Cluster::new(true);
    let mut durable = Cluster::new(false);
    history(&mut volatile);
    history(&mut durable);
    volatile.assert_twin(&durable);
    for r in volatile.replicas.values() {
        assert_eq!(r.core.state().commit_index, r.core.state().last_index());
        assert_eq!(
            r.application.read_applied(r.core.state().commit_index),
            Ok(18)
        );
        assert!(r.core.state().entries.iter().all(|e| !matches!(
            &e.payload,
            EntryPayload::Command { operation, .. } if operation.get() == 90
        )));
    }
    assert_eq!(volatile.replicas[&2].core.role(), Role::Leader);
    assert!(volatile
        .trace
        .iter()
        .any(|(_, seen)| matches!(seen, Seen::Other(e) if matches!(**e, Effect::ReadReady(_)))));
}

fn snapshot_history(cluster: &mut Cluster) {
    cluster.act(1, Event::Campaign);
    cluster.pump();
    cluster.isolate(3);
    cluster.propose(1, 1, 7);
    cluster.compact(1);
    cluster.pump();
    assert_eq!(cluster.replicas[&1].core.state().base_index(), 2);
    cluster.propose(1, 2, 3);
    cluster.blocked.clear();
    cluster.act(1, Event::Heartbeat);
    cluster.pump();
    assert!(cluster.installs > 0);
    assert_eq!(cluster.replicas[&3].core.state().base_index(), 2);
    cluster.compact(3);
    cluster.pump();
    cluster.propose(1, 3, 2);
}

#[test]
fn volatile_compaction_and_snapshot_install_match_durable_helpers() {
    let mut volatile = Cluster::new(true);
    let mut durable = Cluster::new(false);
    snapshot_history(&mut volatile);
    snapshot_history(&mut durable);
    volatile.assert_twin(&durable);
    for (id, r) in &volatile.replicas {
        assert_eq!(r.application.read_applied(4), Ok(12));
        assert_eq!(r.core.state().snapshot.is_some(), *id != 2);
    }
}

#[test]
fn volatile_wrong_binding_or_stale_token_fences_and_recovers_completed_state() {
    let mut cluster = Cluster::new(true);
    cluster.act(1, Event::Campaign);
    cluster.pump();
    cluster.propose(1, 1, 4);
    let r = cluster.replicas.get_mut(&1).unwrap();
    let Log::Volatile(log) = r.log else {
        unreachable!()
    };
    let completed = r.core.state().clone();
    let effects = r
        .core
        .step(Event::Propose {
            operation: OperationId::new(2).unwrap(),
            bytes: 9i64.to_le_bytes().to_vec(),
        })
        .unwrap();
    let [Effect::PersistVolatile(persist)] = effects[..] else {
        panic!("one payload-free token: {effects:?}")
    };
    let mut other = binding(1);
    other.session = StoreSession::new(2).unwrap();
    let wrong = VolatileLog::new(other, LogLimits::default()).unwrap();
    assert!(matches!(
        persist_volatile(&mut r.core, &wrong, persist),
        Err(RaftError::WrongIdentity)
    ));
    assert!(r.core.is_fenced());
    assert!(!r.core.has_pending_dependency());
    assert_eq!(r.core.state(), &completed);
    assert!(matches!(
        persist_volatile(&mut r.core, &log, persist),
        Err(RaftError::Fenced)
    ));
    // The fenced core still exports its last completed state.
    r.core = Raft::recover_volatile(node(1), &log, r.core.state().clone()).unwrap();
    assert!(r.core.is_volatile());
    assert!(matches!(
        persist_volatile(&mut r.core, &log, persist),
        Err(RaftError::WrongCompletion)
    ));
    assert!(r.core.is_fenced());
    assert_eq!(r.core.state(), &completed);
    r.core = Raft::recover_volatile(node(1), &log, completed.clone()).unwrap();
    // A durable recovery entry point never yields a volatile core.
    let durable = Raft::recover(node(1), log.binding(), completed, log.limits()).unwrap();
    assert!(!durable.is_volatile());
    cluster.act(1, Event::Campaign);
    cluster.pump();
    assert_eq!(cluster.replicas[&1].core.role(), Role::Leader);
    cluster.propose(1, 3, 6);
    for r in cluster.replicas.values() {
        assert_eq!(r.core.state().commit_index, r.core.state().last_index());
        assert_eq!(
            r.application.read_applied(r.core.state().commit_index),
            Ok(10)
        );
        assert!(r.core.state().entries.iter().all(|e| !matches!(
            &e.payload,
            EntryPayload::Command { operation, .. } if operation.get() == 2
        )));
    }
}

fn committed_cluster(volatile: bool) -> Cluster {
    let mut cluster = Cluster::new(volatile);
    cluster.act(1, Event::Campaign);
    cluster.pump();
    cluster.propose(1, 1, 4);
    cluster
}
fn probe(term: u64) -> Message {
    Message {
        group: group(1),
        configuration: ConfigurationId::new(1).unwrap(),
        from: node(2),
        sender: binding(2),
        to: node(1),
        term,
        context: RequestContext {
            origin: binding(2),
            sequence: 1,
        },
        rpc: Rpc::ReadProbe,
    }
}
fn refused(core: &Raft, before: &GroupLog, snapshots: &HostSnapshots) {
    assert!(!core.is_fenced());
    assert!(!core.has_pending_dependency());
    assert_eq!(core.state(), before);
    assert!(snapshots.pins.is_empty());
}

#[test]
fn durable_helpers_refuse_volatile_cores_without_mutation_and_vice_versa() {
    durable_helpers_refuse_volatile_core();
    volatile_helpers_refuse_durable_core();
}

fn durable_helpers_refuse_volatile_core() {
    let mut volatile = committed_cluster(true);
    // A volatile core with a durable store of the same binding.
    let r = volatile.replicas.get_mut(&1).unwrap();
    let mut store = HostLogStore::new(1);
    append(&mut store, vec![LogMutation::Create(bootstrap(1, 3))]);
    let store_before = store.state(group(1)).unwrap();
    let message = probe(r.core.state().hard_state.term);
    let reference = checkpoint_application(&r.core, &r.application, &mut r.snapshots)
        .unwrap()
        .reference();
    let before = r.core.state().clone();
    assert_eq!(
        compact_replica(
            &mut r.core,
            &mut store,
            &mut r.snapshots,
            &r.application,
            reference
        ),
        Err(CheckpointError::InvalidBinding)
    );
    refused(&r.core, &before, &r.snapshots);
    assert_eq!(
        stage_snapshot_effect(
            &mut r.core,
            &mut store,
            &mut r.snapshots,
            &r.application,
            message
        ),
        Err(CheckpointError::InvalidBinding)
    );
    refused(&r.core, &before, &r.snapshots);
    assert_eq!(
        finish_snapshot_install(
            &mut r.core,
            &store,
            &mut r.snapshots,
            &mut r.application,
            reference
        ),
        Err(CheckpointError::InvalidBinding)
    );
    refused(&r.core, &before, &r.snapshots);
    // The update a durable twin would submit for the next proposal.
    let effects = r
        .core
        .step(Event::Propose {
            operation: OperationId::new(2).unwrap(),
            bytes: vec![1, 0, 0, 0, 0, 0, 0, 0],
        })
        .unwrap();
    assert!(matches!(effects[..], [Effect::PersistVolatile(_)]));
    let update = LogUpdate {
        group: group(1),
        expected_revision: before.revision,
        hard_state: before.hard_state,
        commit_index: before.commit_index,
        suffix: Some(Suffix {
            from: before.last_index() + 1,
            entries: vec![LogEntry {
                index: before.last_index() + 1,
                term: before.hard_state.term,
                payload: EntryPayload::Command {
                    operation: OperationId::new(2).unwrap(),
                    bytes: vec![1, 0, 0, 0, 0, 0, 0, 0].into(),
                },
            }],
        }),
        snapshot: None,
        snapshot_membership: None,
    };
    assert_eq!(
        r.core.validate_persist_effect(&update),
        Err(RaftError::WrongCompletion)
    );
    assert!(r.core.has_pending_dependency());
    assert!(matches!(
        persist_effect(&mut r.core, &mut store, update),
        Err(RaftError::WrongCompletion)
    ));
    assert!(r.core.is_fenced());
    assert_eq!(r.core.state(), &before);
    assert_eq!(store.state(group(1)).unwrap(), store_before);
    assert!(store.pending.is_empty());
}

fn volatile_helpers_refuse_durable_core() {
    let mut durable = committed_cluster(false);
    // A durable core with a volatile log of the same binding.
    let r = durable.replicas.get_mut(&1).unwrap();
    let message = probe(r.core.state().hard_state.term);
    let log = VolatileLog::new(binding(1), LogLimits::default()).unwrap();
    let reference = checkpoint_application(&r.core, &r.application, &mut r.snapshots)
        .unwrap()
        .reference();
    let before = r.core.state().clone();
    assert_eq!(
        compact_volatile_replica(
            &mut r.core,
            &log,
            &mut r.snapshots,
            &r.application,
            reference
        ),
        Err(CheckpointError::InvalidBinding)
    );
    refused(&r.core, &before, &r.snapshots);
    assert_eq!(
        stage_volatile_snapshot_effect(
            &mut r.core,
            &log,
            &mut r.snapshots,
            &r.application,
            message
        ),
        Err(CheckpointError::InvalidBinding)
    );
    refused(&r.core, &before, &r.snapshots);
    assert_eq!(
        finish_volatile_snapshot_install(
            &mut r.core,
            &log,
            &mut r.snapshots,
            &mut r.application,
            reference
        ),
        Err(CheckpointError::InvalidBinding)
    );
    refused(&r.core, &before, &r.snapshots);
    // The matching helper still compacts the untouched durable replica.
    let Log::Durable(store) = &mut r.log else {
        unreachable!()
    };
    let effects = compact_replica(
        &mut r.core,
        store,
        &mut r.snapshots,
        &r.application,
        reference,
    )
    .unwrap();
    durable.effects(1, effects);
    durable.pump();
    assert_eq!(durable.replicas[&1].core.state().base_index(), 2);
}
