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
use std::cell::Cell;
use std::collections::{BTreeMap, BTreeSet, VecDeque};
use support::{snapshot::HostSnapshots, *};
use voteboat::{
    application::*, contracts::StorageError, identity::*, log::*, raft::*, snapshot::*,
};

enum Log {
    Durable(HostLogStore),
    Volatile(VolatileLog),
}
struct Replica {
    core: Raft,
    log: Log,
    snapshots: HostSnapshots,
    application: Counter,
    /// The one reused effect queue of an `Api::Sink` replica.
    queue: VecDeque<Effect>,
}
/// Which public API family a cluster drives.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
enum Api {
    /// `step`, `propose_batch` and `persist_volatile(..).into_owned()`.
    Vec,
    /// The Vec forms, with batches staged by `propose_entries`.
    Entries,
    /// `step_into`, `propose_entries_into` and `persist_volatile_into`
    /// into one reused queue per replica. Every call finds sentinel effects
    /// already queued, which it must keep, unread, ahead of its own.
    Sink,
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
    api: Api,
    /// Committed intervals a sink completion returned and the host inserted
    /// behind earlier queued effects.
    inserted: usize,
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
/// Effects every sink call finds already queued.
fn sentinels() -> Vec<Effect> {
    vec![Effect::Send(probe(7)), Effect::Send(probe(8))]
}
/// The sentinels lead `queue`, unchanged; remove them.
fn take_sentinels(queue: &mut VecDeque<Effect>, from: usize) {
    let sentinels = sentinels();
    let end = from + sentinels.len();
    assert!(queue.range(from..end).eq(sentinels.iter()));
    queue.drain(from..end);
}
impl Cluster {
    fn sink(api: Api) -> Self {
        Self {
            api,
            ..Self::new(true)
        }
    }
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
                    queue: VecDeque::new(),
                },
            );
        }
        Self {
            replicas,
            messages: VecDeque::new(),
            blocked: BTreeSet::new(),
            trace: Vec::new(),
            installs: 0,
            api: Api::Vec,
            inserted: 0,
        }
    }
    fn act(&mut self, id: u64, event: Event) {
        if self.api == Api::Sink {
            let r = self.replicas.get_mut(&id).unwrap();
            assert!(r.queue.is_empty(), "an effect outlived its drain");
            r.queue.extend(sentinels());
            r.core.step_into(event, &mut r.queue).unwrap();
            take_sentinels(&mut r.queue, 0);
            return self.drain(id);
        }
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
        let r = self.replicas.get_mut(&id).unwrap();
        if self.api == Api::Vec {
            let effects = r.core.propose_batch(proposals).unwrap();
            self.effects(id, effects);
            return self.pump();
        }
        let mut batch = r.core.proposal_batch(proposals.len()).unwrap();
        for Proposal { operation, bytes } in proposals {
            batch.push(operation, bytes).unwrap();
        }
        if self.api == Api::Sink {
            assert!(r.queue.is_empty(), "an effect outlived its drain");
            r.queue.extend(sentinels());
            r.core.propose_entries_into(batch, &mut r.queue).unwrap();
            take_sentinels(&mut r.queue, 0);
            self.drain(id);
        } else {
            let effects = r.core.propose_entries(batch).unwrap();
            self.effects(id, effects);
        }
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
        self.run(id, &mut queue);
    }
    /// Handle a sink replica's queue, which must end empty and is kept.
    fn drain(&mut self, id: u64) {
        let mut queue = std::mem::take(&mut self.replicas.get_mut(&id).unwrap().queue);
        self.run(id, &mut queue);
        assert!(queue.is_empty());
        queue.clear();
        self.replicas.get_mut(&id).unwrap().queue = queue;
    }
    fn run(&mut self, id: u64, queue: &mut VecDeque<Effect>) {
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
                (Effect::PersistVolatile(persist), Log::Volatile(log)) if self.api == Api::Sink => {
                    // Earlier work stays first: the interval goes where this
                    // call's effects start, ahead of them.
                    let queued = queue.len();
                    queue.extend(sentinels());
                    let mark = queue.len();
                    let committed =
                        persist_volatile_into(&mut r.core, log, persist, queue).unwrap();
                    if let Some(entries) = committed {
                        queue.insert(mark, Effect::Committed(entries.to_vec()));
                        self.inserted += 1;
                    }
                    take_sentinels(queue, queued);
                    Vec::new()
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

impl Cluster {
    /// Same effects in the same order, same states, roles and applications.
    fn assert_same(&self, other: &Self) {
        assert_eq!(self.trace, other.trace);
        assert_eq!(self.installs, other.installs);
        for (id, r) in &self.replicas {
            let twin = &other.replicas[id];
            assert_eq!(r.core.state(), twin.core.state());
            assert_eq!(r.core.role(), twin.core.role());
            assert_eq!(r.core.is_fenced(), twin.core.is_fenced());
            let applied = r.application.applied_index();
            assert_eq!(applied, twin.application.applied_index());
            assert_eq!(
                r.application.read_applied(applied),
                twin.application.read_applied(applied)
            );
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

/// Counts every provider call, so a refusal can be shown to precede them all.
struct Counted {
    inner: HostLogStore,
    calls: Cell<usize>,
}
impl Counted {
    /// A durable mirror holding exactly `state`, as if it had persisted it.
    fn exact(state: &GroupLog) -> Self {
        let mut inner = HostLogStore::new(1);
        inner.accepted.insert(state.bootstrap.group, state.clone());
        inner.durable.insert(state.bootstrap.group, state.clone());
        Self {
            inner,
            calls: Cell::new(0),
        }
    }
    fn hit(&self) {
        self.calls.set(self.calls.get() + 1);
    }
}
impl LogStore for Counted {
    fn binding(&self) -> StoreBinding {
        self.hit();
        self.inner.binding()
    }
    fn limits(&self) -> LogLimits {
        self.hit();
        self.inner.limits()
    }
    fn state(&self, group: GroupIdentity) -> Result<GroupLog, StorageError> {
        self.hit();
        self.inner.state(group)
    }
    fn matches_state(
        &self,
        group: GroupIdentity,
        expected: &GroupLog,
    ) -> Result<bool, StorageError> {
        self.hit();
        self.inner.matches_state(group, expected)
    }
    fn matches_checkpoint_state(
        &mut self,
        expected: CheckpointLog<'_>,
    ) -> Result<bool, StorageError> {
        self.hit();
        self.inner.matches_checkpoint_state(expected)
    }
    fn append_batch(
        &mut self,
        mutations: Vec<LogMutation>,
    ) -> Result<Vec<LogTicket>, StorageError> {
        self.hit();
        self.inner.append_batch(mutations)
    }
    fn barrier(&mut self, dependencies: &[LogTicket]) -> Result<DurableLog, StorageError> {
        self.hit();
        self.inner.barrier(dependencies)
    }
    fn supports_reclaim(&self) -> bool {
        self.hit();
        self.inner.supports_reclaim()
    }
    fn reclaim(&mut self, max_bytes: usize) -> Result<LogReclaimed, StorageError> {
        self.hit();
        self.inner.reclaim(max_bytes)
    }
    fn fetch_range(
        &self,
        group: GroupIdentity,
        generation: LogGeneration,
        from: u64,
        max_entries: usize,
        max_bytes: usize,
    ) -> Result<Vec<LogEntry>, StorageError> {
        self.hit();
        self.inner
            .fetch_range(group, generation, from, max_entries, max_bytes)
    }
}

fn durable_helpers_refuse_volatile_core() {
    let mut volatile = committed_cluster(true);
    let r = volatile.replicas.get_mut(&1).unwrap();
    let message = probe(r.core.state().hard_state.term);
    let reference = checkpoint_application(&r.core, &r.application, &mut r.snapshots)
        .unwrap()
        .reference();
    let before = r.core.state().clone();
    // A durable store of the same binding whose mirror is exactly the core's
    // state, so only the mode guard can refuse, before any provider call.
    let mut store = Counted::exact(&before);
    assert_eq!(store.inner.binding(), r.core.storage_binding());
    assert!(store.inner.matches_state(group(1), &before).unwrap());
    let store_before = store.inner.state(group(1)).unwrap();
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
    assert_eq!(store.calls.get(), 0);
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
            }]
            .into(),
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
    assert_eq!(store.inner.state(group(1)).unwrap(), store_before);
    assert!(store.inner.pending.is_empty());
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

/// A single-voter volatile core whose only voter uses store `store`.
fn solo(g: u128, store: u128) -> (Raft, VolatileLog) {
    let mut bootstrap = bootstrap(g, 1);
    bootstrap.voter_stores.insert(node(1), identity(store));
    let log = VolatileLog::new(
        StoreBinding {
            identity: identity(store),
            session: StoreSession::new(1).unwrap(),
        },
        LogLimits::default(),
    )
    .unwrap();
    let state = log.bootstrap(&bootstrap).unwrap();
    (Raft::recover_volatile(node(1), &log, state).unwrap(), log)
}
fn only_token(effects: &[Effect]) -> VolatilePersist {
    let [Effect::PersistVolatile(persist)] = effects else {
        panic!("one payload-free token: {effects:?}")
    };
    *persist
}
/// Complete every volatile token in order; return the other effects.
fn settle(core: &mut Raft, log: &VolatileLog, effects: Vec<Effect>) -> Vec<Effect> {
    let mut queue = VecDeque::from(effects);
    let mut other = Vec::new();
    while let Some(effect) = queue.pop_front() {
        match effect {
            Effect::PersistVolatile(persist) => {
                queue.extend(persist_volatile(core, log, persist).unwrap().into_owned())
            }
            effect => other.push(effect),
        }
    }
    other
}
/// A wrong token returns `WrongCompletion`, fences and installs nothing.
fn rejected(core: &mut Raft, log: &VolatileLog, persist: VolatilePersist) {
    let before = core.state().clone();
    assert!(matches!(
        persist_volatile(core, log, persist),
        Err(RaftError::WrongCompletion)
    ));
    assert!(core.is_fenced());
    assert!(!core.has_pending_dependency());
    assert_eq!(core.state(), &before);
}

#[test]
fn volatile_tokens_complete_only_their_own_core_incarnation() {
    // Two groups sharing one store binding, at identical pending counters.
    let (mut a, a_log) = solo(1, 1);
    let (mut b, b_log) = solo(2, 1);
    assert_eq!(a_log.binding(), b_log.binding());
    let a_token = only_token(&a.step(Event::Campaign).unwrap());
    let b_token = only_token(&b.step(Event::Campaign).unwrap());
    assert_eq!(
        (a_token.revision(), a_token.generation()),
        (b_token.revision(), b_token.generation())
    );
    assert_ne!(a_token, b_token);
    rejected(&mut b, &b_log, a_token);
    settle(&mut a, &a_log, vec![Effect::PersistVolatile(a_token)]);
    assert_eq!(a.role(), Role::Leader);

    // Different store bindings: refused through either core's log.
    let (mut c, c_log) = solo(3, 3);
    let (mut d, d_log) = solo(4, 4);
    let c_token = only_token(&c.step(Event::Campaign).unwrap());
    let d_token = only_token(&d.step(Event::Campaign).unwrap());
    assert!(matches!(
        persist_volatile(&mut d, &c_log, c_token),
        Err(RaftError::WrongIdentity)
    ));
    assert!(d.is_fenced());
    let (mut d, _) = solo(4, 4);
    let fresh = only_token(&d.step(Event::Campaign).unwrap());
    assert_ne!(fresh, d_token);
    assert_eq!(
        (fresh.revision(), fresh.generation()),
        (c_token.revision(), c_token.generation())
    );
    rejected(&mut d, &d_log, c_token);
    settle(&mut c, &c_log, vec![Effect::PersistVolatile(c_token)]);

    // An earlier incarnation's token, after recovery of the same saved state
    // has created a new pending transition with identical counters.
    let (mut e, e_log) = solo(5, 5);
    let saved = e.state().clone();
    let old = only_token(&e.step(Event::Campaign).unwrap());
    e.storage_failed();
    let mut e = Raft::recover_volatile(node(1), &e_log, saved.clone()).unwrap();
    let new = only_token(&e.step(Event::Campaign).unwrap());
    assert_eq!(
        (old.revision(), old.generation()),
        (new.revision(), new.generation())
    );
    assert_ne!(old, new);
    rejected(&mut e, &e_log, old);
    assert_eq!(e.state(), &saved);
    // The rejected recovery recovers again and its own tokens complete.
    let mut e = Raft::recover_volatile(node(1), &e_log, saved).unwrap();
    let effects = e.step(Event::Campaign).unwrap();
    settle(&mut e, &e_log, effects);
    assert_eq!(e.role(), Role::Leader);

    // A same-core duplicate, both after completion and while a later
    // transition of the same core is pending.
    let (mut f, f_log) = solo(6, 6);
    let first = only_token(&f.step(Event::Campaign).unwrap());
    let following = persist_volatile(&mut f, &f_log, first)
        .unwrap()
        .into_owned();
    let second = only_token(&following);
    assert_ne!(first, second);
    rejected(&mut f, &f_log, first);
    let (mut f, f_log) = solo(6, 6);
    let first = only_token(&f.step(Event::Campaign).unwrap());
    settle(&mut f, &f_log, vec![Effect::PersistVolatile(first)]);
    assert_eq!(f.role(), Role::Leader);
    assert!(!f.has_pending_dependency());
    rejected(&mut f, &f_log, first);
}

fn learners_proposal(state: &GroupLog) -> ConfigurationProposal {
    use voteboat::membership::*;
    ConfigurationProposal {
        record: ConfigurationRecord {
            operation: OperationId::new(77).unwrap(),
            expected: state.membership().unwrap().id(),
            change: ConfigurationChange::Learners(
                Configuration::new(
                    ConfigurationId::new(2).unwrap(),
                    state.bootstrap.policy.clone(),
                    state.bootstrap.voter_stores.clone(),
                    [(node(4), identity(4))].into(),
                )
                .unwrap(),
            ),
        },
        readiness: Vec::new(),
        requirements: ReadinessRequirements {
            application_schema: 1,
            command_bytes: 1024,
            snapshot_bytes: 1024,
        },
    }
}
fn has_configuration(state: &GroupLog) -> bool {
    state.snapshot_membership.is_some()
        || state
            .entries
            .iter()
            .any(|e| matches!(e.payload, EntryPayload::Configuration(_)))
}
/// Fence, recover the completed state and commit on top of it.
fn recovers_after_fence(cluster: &mut Cluster, id: u64) {
    let r = cluster.replicas.get_mut(&id).unwrap();
    let Log::Volatile(log) = r.log else {
        unreachable!()
    };
    r.core.storage_failed();
    let state = r.core.state().clone();
    assert!(!has_configuration(&state));
    r.core = Raft::recover_volatile(node(id), &log, state.clone()).unwrap();
    assert_eq!(r.core.state(), &state);
}

#[test]
fn volatile_leader_refuses_configuration_that_a_durable_leader_accepts() {
    let mut durable = committed_cluster(false);
    let r = durable.replicas.get_mut(&1).unwrap();
    let proposal = learners_proposal(r.core.state());
    let effects = r
        .core
        .step(Event::Configure(Box::new(proposal.clone())))
        .unwrap();
    assert!(matches!(&effects[..], [Effect::Persist(u)]
        if u.suffix.as_ref().is_some_and(|s| matches!(s.entries[0].payload, EntryPayload::Configuration(_)))));

    let mut volatile = committed_cluster(true);
    let r = volatile.replicas.get_mut(&1).unwrap();
    let before = r.core.state().clone();
    assert_eq!(
        r.core.step(Event::Configure(Box::new(proposal))),
        Err(RaftError::Storage(StorageError::Rejected(
            "volatile log has static membership"
        )))
    );
    assert!(!r.core.is_fenced());
    assert!(!r.core.has_pending_dependency());
    assert_eq!(r.core.state(), &before);
    assert_eq!(r.core.role(), Role::Leader);
    // Everything it did commit survives a fence and recovery, and it continues.
    volatile.propose(1, 2, 3);
    for id in 1..=3 {
        recovers_after_fence(&mut volatile, id);
    }
    volatile.act(2, Event::Campaign);
    volatile.pump();
    volatile.propose(2, 3, 5);
    for r in volatile.replicas.values() {
        assert_eq!(r.core.state().commit_index, r.core.state().last_index());
        assert_eq!(
            r.application.read_applied(r.core.state().commit_index),
            Ok(12)
        );
    }
}

/// Durable leader 1, configuration-replicating durable follower 3, and a
/// volatile follower 2 that also selected configuration replication.
fn mixed_cluster() -> Cluster {
    let mut cluster = Cluster::new(false);
    let r = cluster.replicas.get_mut(&3).unwrap();
    let Log::Durable(store) = &r.log else {
        unreachable!()
    };
    let state = store.state(group(1)).unwrap();
    r.core = Raft::recover(node(3), store.binding(), state, store.limits())
        .unwrap()
        .with_configuration_replication();
    let log = VolatileLog::new(binding(2), LogLimits::default()).unwrap();
    let state = log.bootstrap(&bootstrap(1, 3)).unwrap();
    let r = cluster.replicas.get_mut(&2).unwrap();
    r.core = Raft::recover_volatile(node(2), &log, state)
        .unwrap()
        .with_configuration_replication();
    r.log = Log::Volatile(log);
    cluster.act(1, Event::Campaign);
    cluster.pump();
    cluster.propose(1, 1, 4);
    cluster
}
/// Deliver queued messages; the volatile replica must refuse each one that
/// carries configuration state, without installing or fencing anything.
fn deliver_refusing(cluster: &mut Cluster) -> usize {
    let mut refused = 0;
    while let Some(m) = cluster.messages.pop_front() {
        let configuration = match &m.rpc {
            Rpc::Append { entries, .. } => entries
                .iter()
                .any(|e| matches!(e.payload, EntryPayload::Configuration(_))),
            Rpc::Snapshot { snapshot } => snapshot.metadata.membership.is_some(),
            _ => false,
        };
        match m.to.get() {
            2 if configuration => {
                let r = cluster.replicas.get_mut(&2).unwrap();
                let before = r.core.state().clone();
                assert_eq!(
                    r.core.step(Event::Receive(m)),
                    Err(RaftError::InvalidMessage)
                );
                assert!(!r.core.is_fenced());
                assert!(!r.core.has_pending_dependency());
                assert_eq!(r.core.state(), &before);
                refused += 1;
            }
            1..=3 => cluster.act(m.to.get(), Event::Receive(m)),
            _ => {}
        }
    }
    refused
}

#[test]
fn volatile_follower_refuses_replicated_configuration_and_recovers() {
    let mut cluster = mixed_cluster();
    let proposal = learners_proposal(cluster.replicas[&1].core.state());
    cluster.act(1, Event::Configure(Box::new(proposal)));
    assert!(deliver_refusing(&mut cluster) > 0);
    cluster.act(1, Event::Heartbeat);
    deliver_refusing(&mut cluster);
    // The durable majority committed the entry; the volatile follower holds none.
    for id in [1, 3] {
        let state = cluster.replicas[&id].core.state();
        assert!(has_configuration(state));
        assert_eq!(state.commit_index, state.last_index());
    }
    assert!(!has_configuration(cluster.replicas[&2].core.state()));
    // A compacted membership snapshot is refused in the same way.
    cluster.compact(1);
    cluster.act(1, Event::Heartbeat);
    let mut snapshots = 0;
    while !cluster.messages.is_empty() {
        snapshots += cluster
            .messages
            .iter()
            .filter(|m| m.to == node(2) && matches!(m.rpc, Rpc::Snapshot { .. }))
            .count();
        deliver_refusing(&mut cluster);
    }
    assert!(snapshots > 0);
    recovers_after_fence(&mut cluster, 2);
}

#[test]
fn sink_forms_match_vec_forms_over_the_twin_histories() {
    for script in [history as fn(&mut Cluster), snapshot_history] {
        let mut sink = Cluster::sink(Api::Sink);
        let mut entries = Cluster::sink(Api::Entries);
        let mut vec = Cluster::new(true);
        script(&mut sink);
        script(&mut entries);
        script(&mut vec);
        sink.assert_same(&entries);
        sink.assert_same(&vec);
        // Committed intervals were placed behind queued effects.
        assert!(sink.inserted > 0);
        assert!(sink.trace.len() > 50);
    }
}

fn committed_api(api: Api) -> Cluster {
    let mut cluster = Cluster::sink(api);
    cluster.act(1, Event::Campaign);
    cluster.pump();
    cluster.propose(1, 1, 4);
    cluster
}
fn command(operation: u128) -> Event {
    Event::Propose {
        operation: OperationId::new(operation).unwrap(),
        bytes: vec![1],
    }
}
fn one_batch(core: &Raft, operation: u128) -> ProposalBatch {
    let mut batch = core.proposal_batch(1).unwrap();
    batch
        .push(OperationId::new(operation).unwrap(), vec![2])
        .unwrap();
    batch
}

#[test]
fn refused_sink_calls_keep_earlier_effects_and_match_the_vec_forms() {
    let mut sink = committed_api(Api::Sink);
    let mut twin = committed_api(Api::Entries);
    sink.assert_same(&twin);
    let mut queue = VecDeque::from(sentinels());
    let same = |sink: &Cluster, twin: &Cluster, queue: &VecDeque<Effect>| {
        assert!(queue.iter().eq(sentinels().iter()));
        sink.assert_same(twin);
    };
    // A follower refuses proposals in either form.
    let (s, t) = (
        sink.replicas.get_mut(&2).unwrap(),
        twin.replicas.get_mut(&2).unwrap(),
    );
    assert_eq!(
        s.core.step_into(command(5), &mut queue),
        t.core.step(command(5)).map(drop)
    );
    assert_eq!(
        s.core.step_into(command(5), &mut queue),
        Err(RaftError::NotLeader)
    );
    let refused = s
        .core
        .propose_entries_into(one_batch(&s.core, 5), &mut queue);
    assert_eq!(refused, Err(RaftError::NotLeader));
    assert_eq!(
        t.core.propose_entries(one_batch(&t.core, 5)).map(drop),
        refused
    );
    same(&sink, &twin, &queue);
    // A leader with a pending transition is busy in either form.
    let (s, t) = (
        sink.replicas.get_mut(&1).unwrap(),
        twin.replicas.get_mut(&1).unwrap(),
    );
    let mut pending = VecDeque::new();
    s.core.step_into(command(6), &mut pending).unwrap();
    let vec_effects = t.core.step(command(6)).unwrap();
    let [Effect::PersistVolatile(token)] = pending.make_contiguous() else {
        panic!("one token: {pending:?}")
    };
    let token = *token;
    assert!(matches!(vec_effects[..], [Effect::PersistVolatile(_)]));
    assert_eq!(
        s.core.step_into(Event::Heartbeat, &mut queue),
        Err(RaftError::Busy)
    );
    assert_eq!(
        t.core.step(Event::Heartbeat).map(drop),
        Err(RaftError::Busy)
    );
    let refused = s
        .core
        .propose_entries_into(one_batch(&s.core, 7), &mut queue);
    assert_eq!(refused, Err(RaftError::Busy));
    assert_eq!(
        t.core.propose_entries(one_batch(&t.core, 7)).map(drop),
        refused
    );
    same(&sink, &twin, &queue);
    // A stale token fences in either form and leaves no partial effects.
    let Effect::PersistVolatile(twin_token) = vec_effects[0] else {
        unreachable!()
    };
    let (s, t) = (
        sink.replicas.get_mut(&1).unwrap(),
        twin.replicas.get_mut(&1).unwrap(),
    );
    let (Log::Volatile(s_log), Log::Volatile(t_log)) = (&s.log, &t.log) else {
        unreachable!()
    };
    assert!(persist_volatile_into(&mut s.core, s_log, token, &mut pending).is_ok());
    assert!(persist_volatile(&mut t.core, t_log, twin_token).is_ok());
    let committed = s.core.state().clone();
    assert!(matches!(
        persist_volatile_into(&mut s.core, s_log, token, &mut queue),
        Err(RaftError::WrongCompletion)
    ));
    assert!(matches!(
        persist_volatile(&mut t.core, t_log, twin_token),
        Err(RaftError::WrongCompletion)
    ));
    assert!(s.core.is_fenced());
    assert_eq!(s.core.state(), &committed);
    same(&sink, &twin, &queue);
    // A fenced core refuses every sink call without touching the queue.
    let (s, t) = (
        sink.replicas.get_mut(&1).unwrap(),
        twin.replicas.get_mut(&1).unwrap(),
    );
    assert_eq!(
        s.core.step_into(Event::Heartbeat, &mut queue),
        Err(RaftError::Fenced)
    );
    assert_eq!(
        t.core.step(Event::Heartbeat).map(drop),
        Err(RaftError::Fenced)
    );
    let refused = s
        .core
        .propose_entries_into(one_batch(&s.core, 8), &mut queue);
    assert_eq!(refused, Err(RaftError::Fenced));
    same(&sink, &twin, &queue);
}

#[test]
fn reused_sink_queues_keep_their_allocation_and_hold_nothing_between_calls() {
    let mut cluster = Cluster::sink(Api::Sink);
    for r in cluster.replicas.values_mut() {
        r.application = Counter::new(2048).unwrap();
    }
    cluster.act(1, Event::Campaign);
    cluster.pump();
    for operation in 1..66 {
        cluster.propose(1, operation, 1);
    }
    let allocation = |cluster: &Cluster| {
        cluster
            .replicas
            .values()
            .map(|r| {
                assert!(r.queue.is_empty());
                (r.queue.as_slices().0.as_ptr(), r.queue.capacity())
            })
            .collect::<Vec<_>>()
    };
    let warm = allocation(&cluster);
    assert!(warm.iter().all(|&(_, capacity)| capacity > 0));
    for operation in 66..1066 {
        cluster.propose(1, operation, 1);
        assert_eq!(allocation(&cluster), warm);
    }
    for r in cluster.replicas.values() {
        assert_eq!(r.core.state().commit_index, r.core.state().last_index());
        assert_eq!(r.core.state().commit_index, 1066);
        assert_eq!(r.application.read_applied(1066), Ok(1065));
    }
}
