// SPDX-License-Identifier: RPL-1.5
// Copyright (c) 2026 Karl Livesey
//! Public-only caller-driven Counter host. Volatile, not crash durability.
//! Each group owns three real replicas and its unique write domain. Groups
//! share only the caller's bounded message pump; no ancestor commit or registry
//! mutex occurs on writes. Application/owned receipts run on every replica.
use std::{
    collections::{BTreeMap, VecDeque},
    error::Error,
    mem::size_of,
};
use voteboat::{
    application::*, embedding::writes::*, identity::*, log::*, quorum::*, raft::*, snapshot::*,
};
#[path = "snapshots.rs"]
mod snapshots;
use snapshots::MemorySnapshots;
pub type Failure = Box<dyn Error>;
fn error(value: impl std::fmt::Debug) -> Failure {
    format!("{value:?}").into()
}
const ORIGINALS: usize = 4096;
const MESSAGES: usize = 16384;
const OPERATIONS: usize = 20000;

pub fn port_limits(originals: usize) -> Result<WritePortLimits, Failure> {
    if originals == 0 || originals > 16384 {
        return Err("invalid port count".into());
    }
    let mut limits = WritePortLimits {
        originals,
        command_bytes: 8,
        command_capacity_bytes: originals * 8,
        receipt_bytes: size_of::<CounterReceipt>(),
        waker_bytes: 1024,
        retained_bytes: 0,
    };
    limits.retained_bytes = limits.required_bytes::<CounterReceipt>().map_err(error)?;
    Ok(limits)
}
struct Replica {
    core: Raft,
    log: VolatileLog,
    snapshots: MemorySnapshots,
    application: Counter,
    historical: Counter,
    tail: VecDeque<LogEntry>,
    last_checkpoint: u64,
    checkpoints: usize,
    installed: usize,
    receipts: usize,
}
enum Delivery {
    Port(OriginalReply<CounterReceipt>),
    Owned,
}
struct Group {
    replicas: Vec<Replica>,
    domain: WriteDomain<CounterReceipt>,
    pending: BTreeMap<u64, Delivery>,
    owned: VecDeque<CounterReceipt>,
    read: Option<ReadBarrier>,
    request: u64,
    failed: bool,
    paused: [bool; 3],
}
/// Caller-owned shared resources, with no hidden tasks or background progress.
pub struct Fleet {
    groups: Vec<Option<Group>>,
    messages: VecDeque<Message>,
}
impl Default for Fleet {
    fn default() -> Self {
        Self::new()
    }
}
impl Fleet {
    pub fn new() -> Self {
        Self {
            groups: Vec::new(),
            messages: VecDeque::with_capacity(MESSAGES),
        }
    }
    pub fn add_group(
        &mut self,
        identity: GroupIdentity,
    ) -> Result<(usize, WriteFactory<CounterReceipt>), Failure> {
        let slot = if let Some(slot) = self.groups.iter().position(Option::is_none) {
            slot
        } else {
            if self.groups.len() >= 8 {
                return Err("example live group budget exhausted".into());
            }
            self.groups.push(None);
            self.groups.len() - 1
        };
        if self
            .groups
            .iter()
            .flatten()
            .any(|g| g.domain.binding().group == identity)
        {
            return Err("group already live".into());
        }
        let (mut group, factory) = Group::new(identity)?;
        group.act(0, Event::Campaign, &mut self.messages)?;
        self.groups[slot] = Some(group);
        self.pump()?;
        Ok((slot, factory))
    }
    pub fn close_group(&mut self, slot: usize) {
        self.groups[slot].take();
    }
    pub fn submit(
        &mut self,
        slot: usize,
        submission: WriteSubmission<CounterReceipt>,
    ) -> Result<(), Failure> {
        let group = self.groups[slot].as_mut().ok_or("closed group")?;
        let result = group.submit(submission, &mut self.messages);
        if result.is_err() {
            group.fail();
        }
        result
    }
    pub fn write_owned(
        &mut self,
        slot: usize,
        operation: OperationId,
        bytes: Vec<u8>,
    ) -> Result<(), Failure> {
        let group = self.groups[slot].as_mut().ok_or("closed group")?;
        if group.pending.len() + group.owned.len() >= ORIGINALS {
            return Err("example output budget".into());
        }
        group.validate(operation, &bytes, std::iter::empty())?;
        let index = group.replicas[0]
            .core
            .state()
            .last_index()
            .checked_add(1)
            .ok_or("index exhausted")?;
        group.pending.insert(index, Delivery::Owned);
        let result = group.act(0, Event::Propose { operation, bytes }, &mut self.messages);
        if result.is_err() {
            group.fail();
        }
        result
    }
    pub fn take_owned(&mut self, slot: usize) -> Option<CounterReceipt> {
        self.groups[slot].as_mut()?.owned.pop_front()
    }
    pub fn pause(&mut self, slot: usize, replica: usize, paused: bool) {
        self.groups[slot].as_mut().unwrap().paused[replica] = paused;
    }
    pub fn heartbeat(&mut self, slot: usize) -> Result<(), Failure> {
        self.groups[slot].as_mut().ok_or("closed group")?.act(
            0,
            Event::Heartbeat,
            &mut self.messages,
        )
    }
    pub fn pump(&mut self) -> Result<(), Failure> {
        for _ in 0..MESSAGES * 64 {
            let Some(message) = self.messages.pop_front() else {
                return Ok(());
            };
            let Some(group) = self
                .groups
                .iter_mut()
                .flatten()
                .find(|g| g.domain.binding().group == message.group)
            else {
                continue;
            };
            let replica = usize::try_from(message.to.get() - 1)?;
            if replica >= 3 {
                return Err("unknown replica".into());
            }
            // Deterministic partition: transport can lose messages, never invent receipts.
            if group.paused[replica] {
                continue;
            }
            let result = group
                .act(replica, Event::Receive(message), &mut self.messages)
                .and_then(|()| group.maintain(replica, &mut self.messages));
            if result.is_err() {
                group.fail();
            }
            result?;
        }
        Err("finite message progress budget exhausted".into())
    }
    pub fn read(&mut self, slot: usize) -> Result<i64, Failure> {
        let group = self.groups[slot].as_mut().ok_or("closed group")?;
        group.request = group
            .request
            .checked_add(1)
            .ok_or("read identity exhausted")?;
        group.act(
            0,
            Event::Read {
                request: ReadRequestId::new(group.request).unwrap(),
            },
            &mut self.messages,
        )?;
        self.pump()?;
        let group = self.groups[slot].as_mut().unwrap();
        let barrier = group.read.take().ok_or("read quorum not ready")?;
        let replica = &mut group.replicas[0];
        read_at_barrier(&mut replica.core, &barrier, &replica.application, ()).map_err(error)
    }
    pub fn evidence(&self, slot: usize) -> [[u64; 6]; 3] {
        let group = self.groups[slot].as_ref().unwrap();
        std::array::from_fn(|i| {
            let r = &group.replicas[i];
            [
                r.core.state().commit_index,
                r.application.applied_index(),
                r.core.state().base_index(),
                r.checkpoints as u64,
                r.installed as u64,
                r.receipts as u64,
            ]
        })
    }
}
impl Group {
    fn new(group: GroupIdentity) -> Result<(Self, WriteFactory<CounterReceipt>), Failure> {
        let stores: BTreeMap<_, _> = (1..=3)
            .map(|n| {
                (
                    NodeId::new(n).unwrap(),
                    StoreIdentity {
                        id: StoreId::new(u128::from(n)).unwrap(),
                        incarnation: StoreIncarnation::new(group.incarnation.get()).unwrap(),
                    },
                )
            })
            .collect();
        let bootstrap = Bootstrap {
            group,
            configuration: ConfigurationId::new(1).unwrap(),
            policy: Policy::new(
                Tree::Majority(stores.keys().copied().map(Tree::Voter).collect()),
                Limits::default(),
            )
            .map_err(error)?,
            voter_stores: stores.clone(),
        };
        let limits = LogLimits {
            max_command_bytes: 8,
            max_snapshot_bytes: 2 * 1024 * 1024,
            ..LogLimits::default()
        };
        let mut replicas = Vec::with_capacity(3);
        for (node, identity) in stores {
            let binding = StoreBinding {
                identity,
                session: StoreSession::new(1).unwrap(),
            };
            let log = VolatileLog::new(binding, limits).map_err(error)?;
            let state = log.bootstrap(&bootstrap).map_err(error)?;
            let core = Raft::recover_volatile(node, &log, state).map_err(error)?;
            replicas.push(Replica {
                core,
                log,
                snapshots: MemorySnapshots::new(binding, group),
                application: Counter::new(OPERATIONS).map_err(error)?,
                historical: Counter::new(OPERATIONS).map_err(error)?,
                tail: VecDeque::with_capacity(1064),
                last_checkpoint: 0,
                checkpoints: 0,
                installed: 0,
                receipts: 0,
            });
        }
        let binding = WriteBinding {
            group,
            node: NodeId::new(1).unwrap(),
            store: replicas[0].log.binding(),
        };
        let (domain, factory) = WriteDomain::new(
            binding,
            WriteDomainLimits {
                ports: 16,
                retained_bytes: 64 * 1024 * 1024,
            },
        )
        .map_err(error)?;
        Ok((
            Self {
                replicas,
                domain,
                pending: BTreeMap::new(),
                owned: VecDeque::with_capacity(ORIGINALS),
                read: None,
                request: 0,
                failed: false,
                paused: [false; 3],
            },
            factory,
        ))
    }
    fn fail(&mut self) {
        self.failed = true;
        self.domain.close(WriteUnknown::OwnerFailed);
        self.pending.clear();
    }
    fn validate<'a>(
        &'a self,
        operation: OperationId,
        bytes: &[u8],
        prefix: impl Iterator<Item = (OperationId, &'a [u8])>,
    ) -> Result<(), Failure> {
        let replica = &self.replicas[0];
        replica
            .application
            .validate_proposal_context(&replica.core, operation, bytes)
            .map_err(error)?;
        let pending = replica
            .core
            .state()
            .entries
            .iter()
            .filter(|e| e.index > replica.application.applied_index())
            .filter_map(|e| {
                if let EntryPayload::Command { operation, bytes } = &e.payload {
                    Some((*operation, bytes.as_slice()))
                } else {
                    None
                }
            });
        let bound = replica
            .application
            .validate_proposal(operation, bytes, pending.chain(prefix))
            .map_err(error)?;
        if bound > size_of::<CounterReceipt>() {
            return Err("receipt admission bound differs".into());
        }
        Ok(())
    }
    fn submit(
        &mut self,
        mut submission: WriteSubmission<CounterReceipt>,
        messages: &mut VecDeque<Message>,
    ) -> Result<(), Failure> {
        if self.failed || submission.binding() != self.domain.binding() {
            return Err("closed/wrong host binding".into());
        }
        self.domain
            .validate_submission(&submission)
            .map_err(error)?;
        let n = submission.len();
        if n == 0
            || n > self.replicas[0].core.batch_limits().max_proposal_entries
            || self.pending.len() + self.owned.len() + n > ORIGINALS
        {
            return Err("original admission budget".into());
        }
        let first = self.replicas[0]
            .core
            .state()
            .last_index()
            .checked_add(1)
            .ok_or("index exhausted")?;
        let mut batch = self.replicas[0].core.proposal_batch(n).map_err(error)?;
        for ordinal in 0..n {
            let (operation, bytes, reply) = submission
                .take_next()
                .map_err(error)?
                .ok_or("missing original")?;
            self.validate(operation, &bytes, batch.iter())?;
            let index = first.checked_add(ordinal as u64).ok_or("index exhausted")?;
            self.pending.insert(index, Delivery::Port(reply));
            batch.push(operation, bytes).map_err(error)?;
        }
        self.reserve(0, batch.retained_bytes())?;
        let effects = self.replicas[0]
            .core
            .propose_entries(batch)
            .map_err(error)?;
        self.effects(0, effects, messages)?;
        // The exact real proposal is now locally installed; the three-voter
        // quorum cannot have applied it before the caller pumps peers.
        for ordinal in 0..n {
            let index = first + ordinal as u64;
            let actual = self.replicas[0]
                .core
                .state()
                .entry_at(index)
                .ok_or("proposal not installed")?;
            let Some(Delivery::Port(reply)) = self.pending.get_mut(&index) else {
                return Err("missing proposal owner".into());
            };
            reply.bind_proposal(&self.domain, actual).map_err(error)?;
        }
        Ok(())
    }
    fn reserve(&self, slot: usize, input: usize) -> Result<(), Failure> {
        if self.replicas[slot]
            .core
            .effect_reservation(input)
            .is_none_or(|n| n > u32::MAX as usize)
        {
            return Err("host effect budget".into());
        }
        Ok(())
    }
    fn act(
        &mut self,
        slot: usize,
        event: Event,
        messages: &mut VecDeque<Message>,
    ) -> Result<(), Failure> {
        if self.failed {
            return Err("failed host requires recovery".into());
        }
        self.reserve(slot, size_of::<Event>())?;
        let effects = self.replicas[slot].core.step(event).map_err(error)?;
        self.effects(slot, effects, messages)
    }
    fn effects(
        &mut self,
        slot: usize,
        initial: Vec<Effect>,
        messages: &mut VecDeque<Message>,
    ) -> Result<(), Failure> {
        let mut effects = VecDeque::from(initial);
        while let Some(effect) = effects.pop_front() {
            match effect {
                Effect::Send(message) => {
                    if messages.len() == MESSAGES {
                        return Err("shared message budget".into());
                    }
                    messages.push_back(message);
                }
                Effect::PersistVolatile(persist) => {
                    let replica = &mut self.replicas[slot];
                    effects.extend(
                        persist_volatile(&mut replica.core, &replica.log, persist)
                            .map_err(error)?
                            .into_owned(),
                    );
                }
                Effect::Committed(entries) => self.apply(slot, &entries)?,
                Effect::ReadReady(barrier) => self.read = Some(barrier),
                Effect::SnapshotRequired {
                    to,
                    context,
                    reference,
                } => {
                    let r = &mut self.replicas[slot];
                    effects.extend(
                        supply_snapshot(&r.core, &mut r.snapshots, to, context, reference)
                            .map_err(error)?,
                    );
                }
                Effect::StageSnapshot(message) => {
                    let r = &mut self.replicas[slot];
                    effects.extend(
                        stage_volatile_snapshot_effect(
                            &mut r.core,
                            &r.log,
                            &mut r.snapshots,
                            &r.application,
                            message,
                        )
                        .map_err(error)?,
                    );
                }
                Effect::SnapshotInstalled(reference) => {
                    let r = &mut self.replicas[slot];
                    effects.extend(
                        finish_volatile_snapshot_install(
                            &mut r.core,
                            &r.log,
                            &mut r.snapshots,
                            &mut r.application,
                            reference,
                        )
                        .map_err(error)?,
                    );
                    r.historical = r.application.clone();
                    r.tail.clear();
                    r.last_checkpoint = r.application.applied_index();
                    r.installed += 1;
                }
                Effect::CheckpointCompacted(reference) => self.replicas[slot]
                    .snapshots
                    .reconcile_log(Some(reference))
                    .map_err(error)?,
                _ => return Err("unexpected effect in fixed-membership Counter host".into()),
            }
        }
        Ok(())
    }
    fn apply(&mut self, slot: usize, entries: &[LogEntry]) -> Result<(), Failure> {
        let r = &mut self.replicas[slot];
        let bound = r.application.receipt_bytes_bound(entries).map_err(error)?;
        let receipts = r.application.apply_batch(entries).map_err(error)?;
        let commands = entries
            .iter()
            .filter(|e| matches!(e.payload, EntryPayload::Command { .. }));
        if receipts.len() != commands.clone().count()
            || receipts.capacity() * size_of::<CounterReceipt>() > bound
        {
            return Err("whole-interval receipt cardinality/capacity".into());
        }
        for (receipt, entry) in receipts.iter().zip(commands.clone()) {
            if receipt.index != entry.index
                || !matches!(entry.payload,EntryPayload::Command { operation,.. } if operation==receipt.operation)
            {
                return Err("individual application receipt identity".into());
            }
        }
        r.receipts += receipts.len();
        for entry in entries {
            r.tail.push_back(entry.clone());
        }
        while r.tail.len() > 1000 {
            let entry = r.tail.pop_front().unwrap();
            r.historical
                .apply_batch(std::slice::from_ref(&entry))
                .map_err(error)?;
        }
        // No result escapes until the entire actual interval and every owned R
        // validated. All followers execute this same application path.
        if slot == 0 {
            for (receipt, entry) in receipts.into_iter().zip(commands) {
                let delivery = self
                    .pending
                    .remove(&entry.index)
                    .ok_or("missing original result")?;
                match delivery {
                    Delivery::Port(reply) => reply
                        .publish_applied(&self.domain, entry, receipt, 0)
                        .map_err(error)?,
                    Delivery::Owned => self.owned.push_back(receipt),
                }
            }
        }
        Ok(())
    }
    fn maintain(&mut self, slot: usize, messages: &mut VecDeque<Message>) -> Result<(), Failure> {
        let r = &mut self.replicas[slot];
        let applied = r.application.applied_index();
        if applied.saturating_sub(r.last_checkpoint) < 5000 {
            return Ok(());
        }
        let boundary = applied.saturating_sub(1000);
        if boundary.saturating_sub(r.core.state().base_index()) < 1024 {
            return Ok(());
        }
        if r.historical.applied_index() != boundary {
            return Err("historical checkpoint boundary differs".into());
        }
        let receipt =
            checkpoint_application(&r.core, &r.historical, &mut r.snapshots).map_err(error)?;
        let effects = compact_volatile_replica(
            &mut r.core,
            &r.log,
            &mut r.snapshots,
            &r.application,
            receipt.reference(),
        )
        .map_err(error)?;
        r.last_checkpoint = applied;
        r.checkpoints += 1;
        self.effects(slot, effects, messages)
    }
}
