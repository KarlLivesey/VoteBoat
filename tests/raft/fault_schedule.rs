// SPDX-License-Identifier: RPL-1.5
// Copyright (c) 2026 Karl Livesey
use super::*;
use std::{cell::Cell, io, rc::Rc};
#[path = "fault_schedule/codec.rs"]
mod codec;
#[path = "fault_schedule/crash.rs"]
mod crash;
#[path = "fault_schedule/delayed.rs"]
mod delayed;
#[path = "fault_schedule/publication.rs"]
mod publication;
#[path = "fault_schedule/reads.rs"]
mod reads;
#[path = "fault_schedule/reduce.rs"]
mod reduce;

const MAX_ACTIONS: usize = 512;
const MAX_FAULT_BYTES: usize = 65536;
#[derive(Clone, Debug, Eq, PartialEq)]
enum Action {
    Campaign(u64),
    TryWrite(u64, u128, u8),
    TryCounter(u64, u128, i64),
    Isolate(u64),
    Heal,
    DeliverNewest,
    DuplicateOldest,
    PowerLoss(u64),
    Heartbeat(u64),
    Pump,
    BadSync(u64),
    Delay(u64),
    Sync(u64),
    Complete(u64),
    Stale(u64),
    ProcessCrash(u64),
    FailSync(u64),
    ShortWrite(u64, usize),
    FailPublish(u64, publication::PublishCut),
    Read(u64, u64),
    CancelRead(u64, u64),
    ServeCounter(u64, u64),
}
#[derive(Clone, Debug, Eq, PartialEq)]
struct Trace {
    seed: u64,
    actions: Vec<Action>,
}
impl Trace {
    fn generated(seed: u64) -> Self {
        assert_ne!(seed, 0);
        let mut random = seed;
        let actions = (0..256u64)
            .map(|step| {
                random ^= random << 13;
                random ^= random >> 7;
                random ^= random << 17;
                let id = (random >> 8) % 3 + 1;
                match random % 9 {
                    0 => Action::Campaign(id),
                    1 => Action::TryWrite(
                        id,
                        u128::from(seed) * 1000 + u128::from(step) + 1,
                        u8::try_from(step).unwrap(),
                    ),
                    2 => Action::Isolate(id),
                    3 => Action::Heal,
                    4 => Action::DeliverNewest,
                    5 => Action::DuplicateOldest,
                    6 => Action::PowerLoss(id),
                    7 => Action::Heartbeat(id),
                    _ => Action::Pump,
                }
            })
            .collect();
        Self { seed, actions }
    }
}

// Deliberately defective only after an explicit test fault. The ordinary native
// journal path delegates every operation, including sync, to the existing model.
#[derive(Clone, Default)]
struct TraceIo {
    device: ModelIo,
    bad_sync: Rc<Cell<bool>>,
}
impl JournalIo for TraceIo {
    fn read_manifest(&mut self) -> io::Result<Vec<u8>> {
        self.device.read_manifest()
    }
    fn read_log(&mut self, limit: usize) -> io::Result<Vec<u8>> {
        self.device.read_log(limit)
    }
    fn append(&mut self, bytes: &[u8]) -> io::Result<()> {
        self.device.append(bytes)
    }
    fn sync_log(&mut self) -> io::Result<()> {
        if self.bad_sync.get() {
            Ok(())
        } else {
            self.device.sync_log()
        }
    }
    fn truncate_log(&mut self, length: u64) -> io::Result<()> {
        self.device.truncate_log(length)
    }
    fn publish_manifest(&mut self, bytes: &[u8]) -> io::Result<()> {
        self.device.publish_manifest(bytes)
    }
    fn supports_replacement(&self) -> bool {
        self.device.supports_replacement()
    }
    fn replace_log(&mut self, bytes: &[u8], manifest: &[u8]) -> io::Result<()> {
        self.device.replace_log(bytes, manifest)
    }
}

#[derive(Clone, Debug, Eq, PartialEq)]
struct Point {
    logs: BTreeMap<u64, GroupLog>,
    roles: BTreeMap<u64, Role>,
    applied: BTreeMap<u64, Vec<LogEntry>>,
    queued: usize,
    held: BTreeMap<u64, delayed::Phase>,
    pending: BTreeSet<u64>,
    busy: usize,
    stale_rejected: usize,
    fenced: BTreeSet<u64>,
    sync_failures: usize,
    fenced_refused: usize,
    append_failures: usize,
    publish_failures: usize,
    reads: BTreeMap<u64, Vec<reads::Ready>>,
    read_floor: BTreeMap<u64, u64>,
    read_outcomes: Vec<reads::Attempt>,
}
#[derive(Clone, Debug, Eq, PartialEq)]
enum FailureStage {
    DurableLogMissing,
    RecoveredPrefixLost,
}
#[derive(Clone, Debug, Eq, PartialEq)]
struct LostPrefix {
    node: u64,
    operation: Option<OperationId>,
    stage: FailureStage,
}
struct Replay {
    images: BTreeMap<u64, TraceIo>,
    cluster: Cluster<NativeLogStore<TraceIo>>,
    delayed: delayed::Dependencies,
    read_outcomes: Vec<reads::Attempt>,
}
impl Replay {
    fn new() -> Self {
        let images: BTreeMap<_, _> = (1..=3).map(|id| (id, TraceIo::default())).collect();
        let cluster = Cluster::new(bootstrap(1, 3), |id| {
            NativeLogStore::create(
                images[&id].clone(),
                identity(u128::from(id)),
                LogLimits::default(),
            )
            .unwrap()
        });
        Self {
            images,
            cluster,
            delayed: delayed::Dependencies::default(),
            read_outcomes: Vec::new(),
        }
    }
    fn restart(&mut self, id: u64, mode: crash::Mode) -> Result<(), LostPrefix> {
        let acknowledged = self.cluster.applied[&id].clone();
        self.discard_dependency(id);
        drop(self.cluster.replicas.remove(&id).unwrap());
        match mode {
            crash::Mode::PowerLoss => self.images[&id].device.0.borrow_mut().power_loss(),
            crash::Mode::Process => self.images[&id].device.0.borrow_mut().fault = Fault::None,
        }
        self.images[&id].bad_sync.set(false);
        let store = match NativeLogStore::recover(
            self.images[&id].clone(),
            identity(u128::from(id)),
            LogLimits::default(),
        ) {
            Ok(store) => store,
            Err(voteboat::contracts::StorageError::Corrupt("durable log prefix missing")) => {
                return Err(LostPrefix {
                    node: id,
                    operation: acknowledged.iter().rev().find_map(|e| match e.payload {
                        EntryPayload::Command { operation, .. } => Some(operation),
                        _ => None,
                    }),
                    stage: FailureStage::DurableLogMissing,
                });
            }
            Err(error) => panic!("unexpected recovery failure: {error:?}"),
        };
        let core = Raft::recover(
            node(id),
            store.binding(),
            store.state(group(1)).unwrap(),
            store.limits(),
        )
        .unwrap();
        if !core.replay_committed().starts_with(&acknowledged) {
            let missing = acknowledged
                .iter()
                .zip(
                    core.replay_committed()
                        .iter()
                        .map(Some)
                        .chain(std::iter::repeat(None)),
                )
                .find(|(old, new)| Some(*old) != *new)
                .unwrap()
                .0;
            return Err(LostPrefix {
                node: id,
                stage: FailureStage::RecoveredPrefixLost,
                operation: match missing.payload {
                    EntryPayload::Command { operation, .. } => Some(operation),
                    _ => None,
                },
            });
        }
        self.cluster
            .applied
            .insert(id, core.replay_committed().to_vec());
        self.cluster.replicas.insert(id, Replica { core, store });
        Ok(())
    }
    fn action(&mut self, action: &Action) -> Result<(), LostPrefix> {
        match *action {
            Action::Campaign(id) => self.step_event(id, Event::Campaign),
            Action::TryWrite(id, operation, byte) => self.try_propose(id, operation, vec![byte]),
            Action::TryCounter(id, operation, delta) => {
                self.try_propose(id, operation, delta.to_le_bytes().to_vec());
            }
            Action::Isolate(id) => self.cluster.partition(&[id]),
            Action::Heal => self.cluster.heal(),
            Action::DeliverNewest => {
                if let Some(message) = self.cluster.messages.pop_back() {
                    self.deliver(message);
                }
            }
            Action::DuplicateOldest => {
                if let Some(message) = self.cluster.messages.front().cloned() {
                    self.cluster.messages.push_back(message);
                }
            }
            Action::PowerLoss(id) => self.restart(id, crash::Mode::PowerLoss)?,
            Action::ProcessCrash(id) => self.restart(id, crash::Mode::Process)?,
            Action::FailSync(id) => self.fail_sync(id),
            Action::ShortWrite(id, cut) => {
                assert!(cut <= MAX_FAULT_BYTES);
                self.images[&id].device.0.borrow_mut().fault = Fault::Append(cut);
            }
            Action::FailPublish(id, cut) => self.fail_publish(id, cut),
            Action::Read(id, request) => self.read_action(id, request, reads::Kind::Begin),
            Action::CancelRead(id, request) => self.read_action(id, request, reads::Kind::Cancel),
            Action::ServeCounter(id, request) => self.serve_counter(id, request),
            Action::Heartbeat(id) => self.step_event(id, Event::Heartbeat),
            Action::Pump => self.pump(),
            Action::BadSync(id) => self.images[&id].bad_sync.set(true),
            Action::Delay(id) => {
                self.delayed.nodes.insert(id);
            }
            Action::Sync(id) => self.sync(id),
            Action::Complete(id) => self.complete(id),
            Action::Stale(id) => self.stale(id),
        }
        self.cluster.assert_safety();
        Ok(())
    }
    fn point(&self) -> Point {
        Point {
            logs: self
                .cluster
                .replicas
                .iter()
                .map(|(id, r)| (*id, r.core.state().clone()))
                .collect(),
            roles: self
                .cluster
                .replicas
                .iter()
                .map(|(id, r)| (*id, r.core.role()))
                .collect(),
            applied: self.cluster.applied.clone(),
            queued: self.cluster.messages.len(),
            held: self.delayed.phases(),
            pending: self
                .cluster
                .replicas
                .iter()
                .filter(|(_, replica)| replica.core.has_pending_dependency())
                .map(|(id, _)| *id)
                .collect(),
            busy: self.delayed.busy,
            stale_rejected: self.delayed.stale_rejected,
            fenced: self
                .cluster
                .replicas
                .iter()
                .filter(|(_, replica)| replica.core.is_fenced())
                .map(|(id, _)| *id)
                .collect(),
            sync_failures: self.delayed.sync_failures,
            fenced_refused: self.delayed.fenced_refused,
            append_failures: self.delayed.append_failures,
            publish_failures: self.delayed.publish_failures,
            reads: self
                .cluster
                .reads
                .iter()
                .map(|(id, barriers)| (*id, barriers.iter().map(reads::Ready::from).collect()))
                .collect(),
            read_floor: self
                .cluster
                .replicas
                .iter()
                .map(|(id, replica)| (*id, replica.core.read_request_floor()))
                .collect(),
            read_outcomes: self.read_outcomes.clone(),
        }
    }
}
fn replay(trace: &Trace) -> Result<Vec<Point>, LostPrefix> {
    assert!(trace.actions.len() <= MAX_ACTIONS);
    let mut run = Replay::new();
    let mut points = vec![run.point()];
    for action in &trace.actions {
        run.action(action)?;
        points.push(run.point());
    }
    Ok(points)
}

#[test]
fn recorded_seeded_schedules_replay_the_same_actual_core_states() {
    for seed in 1..=32 {
        let trace = Trace::generated(seed);
        let saved = trace.encode();
        let decoded = Trace::decode(&saved).unwrap();
        assert_eq!(decoded, trace);
        assert_eq!(
            replay(&trace).unwrap(),
            replay(&decoded).unwrap(),
            "seed {seed}\n{saved}"
        );
    }
}
