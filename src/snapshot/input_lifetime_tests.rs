// SPDX-License-Identifier: RPL-1.5
// Copyright (c) 2026 Karl Livesey
//! A heap-free witness of the actual input owner's destruction, not of take().
//! Entire module, hooks and witness field are absent from non-test builds.
use super::*;
use crate::{application::*, log::*, raft::*};
use std::cell::RefCell;

#[path = "../../tests/support/mod.rs"]
mod support;
use support::{snapshot::HostSnapshots, *};

#[derive(Default)]
struct Observation {
    input_live: Option<bool>,
    input_bytes: usize,
    boundaries: u8,
}
thread_local! {
    static OBSERVATION: RefCell<Option<Observation>> = const { RefCell::new(None) };
}

/// Only the watched queue owns an armed witness. It is declared AFTER slots,
/// so its destructor observes destruction of that actual VecDeque allocation.
/// No counter, allocation, allocator hook or manual notification at drop(effects).
#[derive(Debug, Default, Eq, PartialEq)]
pub(crate) struct StorageWitness(bool);
impl StorageWitness {
    pub(crate) fn armed() -> Self {
        Self(true)
    }
}
impl Drop for StorageWitness {
    fn drop(&mut self) {
        if self.0 {
            OBSERVATION.with(|state| {
                state.borrow_mut().as_mut().unwrap().input_live = Some(false);
            });
        }
    }
}

pub(crate) fn watch_input(mut input: EffectQueue) -> EffectQueue {
    OBSERVATION.with(|state| {
        if let Some(observation) = state.borrow_mut().as_mut() {
            assert_eq!(observation.input_live, None, "one real snapshot input");
            assert_eq!(input.len(), 1);
            observation.input_live = Some(true);
            observation.input_bytes = input.capacity() * std::mem::size_of::<Effect>();
            input.watch_storage_destruction();
        }
    });
    input
}

/// Called at real provider/completion/output boundaries, never at take or drop.
pub(crate) fn at_boundary(boundary: &'static str) {
    OBSERVATION.with(|state| {
        if let Some(observation) = state.borrow_mut().as_mut() {
            if let Some(live) = observation.input_live {
                assert!(
                    !live,
                    "consumed snapshot input ring remains live at {boundary}: {} dead bytes overlap continuation",
                    observation.input_bytes
                );
                observation.boundaries |= match boundary {
                    "provider append" => 1,
                    "provider barrier" => 2,
                    "volatile completion" => 4,
                    "following queue" => 8,
                    _ => unreachable!("known observation boundary"),
                };
            }
        }
    });
}
struct Scope;
impl Scope {
    fn new() -> Self {
        OBSERVATION.with(|state| {
            assert!(state.borrow_mut().replace(Observation::default()).is_none());
        });
        Self
    }
    fn check(&self, volatile: bool) {
        OBSERVATION.with(|state| {
            let state = state.borrow();
            let observation = state.as_ref().unwrap();
            assert_eq!(observation.input_live, Some(false));
            assert_eq!(observation.input_bytes, 4 * 272);
            assert_eq!(
                observation.boundaries,
                if volatile { 4 | 8 } else { 1 | 2 | 8 }
            );
        });
    }
}
impl Drop for Scope {
    fn drop(&mut self) {
        OBSERVATION.with(|state| *state.borrow_mut() = None);
    }
}

struct CheckedLog(HostLogStore);
impl LogStore for CheckedLog {
    fn binding(&self) -> StoreBinding {
        self.0.binding()
    }
    fn limits(&self) -> LogLimits {
        self.0.limits()
    }
    fn state(&self, group: GroupIdentity) -> Result<GroupLog, StorageError> {
        self.0.state(group)
    }
    fn append_batch(
        &mut self,
        mutations: Vec<LogMutation>,
    ) -> Result<Vec<LogTicket>, StorageError> {
        at_boundary("provider append");
        self.0.append_batch(mutations)
    }
    fn barrier(&mut self, tickets: &[LogTicket]) -> Result<DurableLog, StorageError> {
        at_boundary("provider barrier");
        self.0.barrier(tickets)
    }
    fn fetch_range(
        &self,
        group: GroupIdentity,
        generation: LogGeneration,
        from: u64,
        count: usize,
        bytes: usize,
    ) -> Result<Vec<LogEntry>, StorageError> {
        self.0.fetch_range(group, generation, from, count, bytes)
    }
}

struct Replica {
    core: Raft,
    log: CheckedLog,
    volatile: Option<VolatileLog>,
    snapshots: HostSnapshots,
    application: Counter,
}
impl Replica {
    fn new(id: u64, committed: bool, volatile: bool) -> Self {
        let mut log = CheckedLog(HostLogStore::new(id as u128));
        append(&mut log, vec![LogMutation::Create(bootstrap(1, 3))]);
        let state = log.state(group(1)).unwrap();
        let entries = committed.then(|| Suffix {
            from: 1,
            entries: vec![LogEntry {
                index: 1,
                term: 1,
                payload: EntryPayload::Command {
                    operation: OperationId::new(1).unwrap(),
                    bytes: 7i64.to_le_bytes().to_vec().into(),
                },
            }]
            .into(),
        });
        append(
            &mut log,
            vec![update(&state, 1, u64::from(committed), entries)],
        );
        let state = log.state(group(1)).unwrap();
        let volatile = volatile.then(|| VolatileLog::new(log.binding(), log.limits()).unwrap());
        let core = if let Some(volatile) = &volatile {
            Raft::recover_volatile(node(id), volatile, state).unwrap()
        } else {
            Raft::recover(node(id), log.binding(), state, log.limits()).unwrap()
        };
        let mut application = Counter::new(100).unwrap();
        application.apply_entries(core.replay_committed()).unwrap();
        let mut snapshots = HostSnapshots::new();
        snapshots.identity.store = log.binding().identity;
        snapshots.binding.identity = log.binding().identity;
        Self {
            core,
            log,
            volatile,
            snapshots,
            application,
        }
    }
    fn persist(&mut self, effects: Vec<Effect>) -> Vec<Effect> {
        assert_eq!(effects.len(), 1);
        match effects.into_iter().next().unwrap() {
            Effect::Persist(update) => {
                persist_effect(&mut self.core, &mut self.log, update).unwrap()
            }
            Effect::PersistVolatile(token) => {
                persist_volatile(&mut self.core, self.volatile.as_ref().unwrap(), token)
                    .unwrap()
                    .into_owned()
            }
            other => panic!("persistence dependency: {other:?}"),
        }
    }
    fn elect(&mut self) {
        let effects = self.core.step(Event::Campaign).unwrap();
        let mut reply = self
            .persist(effects)
            .into_iter()
            .find_map(|effect| match effect {
                Effect::Send(message) if message.to == node(2) => Some(message),
                _ => None,
            })
            .unwrap();
        reply.from = node(2);
        reply.sender = HostLogStore::new(2).binding();
        reply.to = node(1);
        reply.rpc = Rpc::Voted { granted: true };
        let effects = self.core.step(Event::Receive(reply)).unwrap();
        self.persist(effects);
        assert_eq!(self.core.role(), Role::Leader);
    }
}

fn compaction(volatile: bool) {
    let mut replica = Replica::new(1, true, volatile);
    replica.elect();
    let reference =
        checkpoint_application(&replica.core, &replica.application, &mut replica.snapshots)
            .unwrap()
            .reference();
    let scope = Scope::new();
    let mut following = if let Some(log) = &replica.volatile {
        compact_volatile_replica_queue(
            &mut replica.core,
            log,
            &mut replica.snapshots,
            &replica.application,
            reference,
        )
    } else {
        compact_replica_queue(
            &mut replica.core,
            &mut replica.log,
            &mut replica.snapshots,
            &replica.application,
            reference,
        )
    }
    .unwrap();
    scope.check(volatile);
    assert!(
        following.capacity() > 0,
        "real leader continuation allocation"
    );
    assert!(matches!(following.front(), Some(EffectFront::Send(_))));
    assert_eq!(replica.core.state().snapshot, Some(reference));
    assert_eq!(replica.core.state().base_index(), 1);
    assert_eq!(replica.application.read_applied(1), Ok(7));
}

fn installation(volatile: bool) {
    let mut source = Replica::new(1, true, volatile);
    checkpoint_application(&source.core, &source.application, &mut source.snapshots).unwrap();
    let image = source.snapshots.load().unwrap().unwrap();
    let mut replica = Replica::new(2, false, volatile);
    let message = Message {
        group: group(1),
        configuration: ConfigurationId::new(1).unwrap(),
        from: node(1),
        sender: source.log.binding(),
        to: node(2),
        term: 1,
        context: RequestContext {
            origin: source.log.binding(),
            sequence: 1,
        },
        rpc: Rpc::Snapshot {
            snapshot: Box::new(image),
        },
    };
    let effects = replica.core.step(Event::Receive(message.clone())).unwrap();
    assert!(matches!(effects.as_slice(), [Effect::StageSnapshot(staged)] if staged == &message));
    let scope = Scope::new();
    let mut following = if let Some(log) = &replica.volatile {
        stage_volatile_snapshot_effect_queue(
            &mut replica.core,
            log,
            &mut replica.snapshots,
            &replica.application,
            message,
        )
    } else {
        stage_snapshot_effect_queue(
            &mut replica.core,
            &mut replica.log,
            &mut replica.snapshots,
            &replica.application,
            message,
        )
    }
    .unwrap();
    scope.check(volatile);
    assert_eq!(following.len(), 1);
    let Some(EffectFront::SnapshotInstalled(front)) = following.front() else {
        panic!("installation precedes acknowledgement")
    };
    let reference = front.take();
    assert_eq!(replica.core.state().snapshot, Some(reference));
    let mut ack = if let Some(log) = &replica.volatile {
        finish_volatile_snapshot_install_queue(
            &mut replica.core,
            log,
            &mut replica.snapshots,
            &mut replica.application,
            reference,
        )
    } else {
        finish_snapshot_install_queue(
            &mut replica.core,
            &replica.log,
            &mut replica.snapshots,
            &mut replica.application,
            reference,
        )
    }
    .unwrap();
    let Some(EffectFront::Send(front)) = ack.front() else {
        panic!("acknowledge after application restore")
    };
    assert_eq!(front.take().rpc, Rpc::SnapshotAck { index: 1 });
    assert_eq!(replica.application.read_applied(1), Ok(7));
}

#[test]
fn durable_compaction_releases_consumed_ring_before_continuation() {
    compaction(false);
}
#[test]
fn volatile_compaction_releases_consumed_ring_before_continuation() {
    compaction(true);
}
#[test]
fn durable_installation_releases_consumed_ring_before_continuation() {
    installation(false);
}
#[test]
fn volatile_installation_releases_consumed_ring_before_continuation() {
    installation(true);
}
