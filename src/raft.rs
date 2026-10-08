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
//! Deterministic static-configuration Raft. Host drives elections and delivery.
//! Every persistence-dependent send/application notification waits for its exact
//! store/group/revision/generation ticket. No transport or clock is constructed.
use crate::{
    contracts::{HardState, StorageError},
    identity::*,
    log::*,
    snapshot::{Snapshot, SnapshotRef},
};
use std::collections::{BTreeMap, BTreeSet};

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub struct RequestContext {
    pub origin: StoreBinding,
    pub sequence: u64,
}
#[derive(Clone, Debug, Eq, PartialEq)]
pub enum Rpc {
    Vote {
        last_index: u64,
        last_term: u64,
    },
    Voted {
        granted: bool,
    },
    Append {
        previous_index: u64,
        previous_term: u64,
        entries: Vec<LogEntry>,
        leader_commit: u64,
    },
    Appended {
        success: bool,
        matching_index: u64,
    },
    /// A fresh authority check for one admitted read, independent of log acks.
    ReadProbe,
    ReadAck,
    Snapshot {
        snapshot: Box<Snapshot>,
    },
    SnapshotAck {
        index: u64,
    },
    Compacted {
        index: u64,
        term: u64,
    },
}
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct Message {
    pub group: GroupIdentity,
    pub configuration: ConfigurationId,
    pub from: NodeId,
    pub sender: StoreBinding,
    pub to: NodeId,
    pub term: u64,
    pub context: RequestContext,
    pub rpc: Rpc,
}
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum Role {
    Follower,
    Candidate,
    Leader,
}
#[derive(Clone, Debug, Eq, PartialEq)]
pub enum Event {
    Campaign,
    Heartbeat,
    /// Request a local application checkpoint; never establishes quorum evidence.
    Checkpoint,
    Receive(Message),
    Propose {
        operation: OperationId,
        bytes: Vec<u8>,
    },
    Read {
        request: ReadRequestId,
    },
    CancelRead {
        request: ReadRequestId,
    },
}

/// One-use read authority. Only the core constructs it. The host must consume
/// it through `finish_read` (or `application::read_at_barrier`) for the original
/// read invocation; it is never a cached lease or authority for a later read.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub struct ReadBarrier {
    group: GroupIdentity,
    configuration: ConfigurationId,
    term: u64,
    context: RequestContext,
    request: ReadRequestId,
    index: u64,
}
impl ReadBarrier {
    pub fn group(&self) -> GroupIdentity {
        self.group
    }
    pub fn configuration(&self) -> ConfigurationId {
        self.configuration
    }
    pub fn term(&self) -> u64 {
        self.term
    }
    pub fn request(&self) -> ReadRequestId {
        self.request
    }
    /// Contiguous committed prefix captured after a current-term commit.
    pub fn index(&self) -> u64 {
        self.index
    }
}
#[derive(Clone, Debug, Eq, PartialEq)]
pub enum Effect {
    Persist(LogUpdate),
    Send(Message),
    /// Ordered committed entries. Not a client success: application must apply
    /// and return a result before a caller can acknowledge its operation.
    Committed(Vec<LogEntry>),
    /// Quorum authority is established; application may still lag this index.
    ReadReady(ReadBarrier),
    SnapshotRequired {
        to: NodeId,
        context: RequestContext,
        reference: SnapshotRef,
    },
    StageSnapshot(Message),
    SnapshotInstalled(SnapshotRef),
    CheckpointRequired {
        context: RequestContext,
    },
    /// The exact WAL anchor is durable; reconcile snapshot retention next.
    CheckpointCompacted(SnapshotRef),
}
#[derive(Clone, Debug, Eq, PartialEq)]
pub enum RaftError {
    /// Owner-side deterministic application validation rejected this invocation
    /// before the core proposed it. This is not a replicated outcome.
    Admission(crate::application::ApplicationError),
    Busy,
    Fenced,
    NotLeader,
    WrongIdentity,
    InvalidMessage,
    WrongCompletion,
    Exhausted,
    InvalidRecovery,
    ReadNotReady,
    ReadInFlight,
    StaleRead,
    NotApplied,
    Storage(StorageError),
}
impl From<StorageError> for RaftError {
    fn from(e: StorageError) -> Self {
        Self::Storage(e)
    }
}

#[derive(Clone, Copy, Debug)]
enum After {
    Campaign,
    Reply,
    LeaderAppend,
    Commit,
    Compact,
    CheckpointCompact(SnapshotRef),
    SnapshotInstall(SnapshotRef),
}
struct Pending {
    update: LogUpdate,
    next: GroupLog,
    ticket: Option<LogTicket>,
    after: After,
    reply: Option<Message>,
}
#[derive(Clone, Copy)]
struct Replication {
    context: RequestContext,
    start: u64,
    end: u64,
    snapshot: Option<SnapshotRef>,
}
struct PendingRead {
    barrier: ReadBarrier,
    acknowledgements: BTreeSet<NodeId>,
}

pub struct Raft {
    node: NodeId,
    binding: StoreBinding,
    limits: LogLimits,
    durable: GroupLog,
    role: Role,
    votes: BTreeSet<NodeId>,
    vote_context: Option<RequestContext>,
    progress: BTreeMap<NodeId, u64>,
    next_index: BTreeMap<NodeId, u64>,
    requests: BTreeMap<NodeId, Replication>,
    request_sequence: u64,
    last_batch: u64,
    pending: Option<Pending>,
    read: Option<PendingRead>,
    ready_read: Option<ReadBarrier>,
    last_read_request: u64,
    fenced: bool,
    staged_snapshot: Option<Message>,
    application_install: Option<(SnapshotRef, Message)>,
    checkpoint_requested: Option<RequestContext>,
    checkpoint_reconcile: Option<SnapshotRef>,
    election_reset: u64,
}

impl Raft {
    pub fn recover(
        node: NodeId,
        binding: StoreBinding,
        state: GroupLog,
        limits: LogLimits,
    ) -> Result<Self, RaftError> {
        if state.snapshot.is_some() {
            return Err(RaftError::InvalidRecovery);
        }
        Self::recover_verified(node, binding, state, limits)
    }
    pub(crate) fn recover_verified(
        node: NodeId,
        binding: StoreBinding,
        state: GroupLog,
        limits: LogLimits,
    ) -> Result<Self, RaftError> {
        let limits = limits.validate()?;
        if state.bootstrap.voter_stores.get(&node) != Some(&binding.identity)
            || state
                .bootstrap
                .voter_stores
                .keys()
                .copied()
                .collect::<BTreeSet<_>>()
                != *state.bootstrap.policy.voters()
            || state.commit_index > state.last_index()
            || state.commit_index < state.base_index()
            || state.last_index() == u64::MAX
            || state.snapshot.is_some_and(|s| {
                s.store != binding.identity
                    || s.group != state.bootstrap.group
                    || s.configuration != state.bootstrap.configuration
                    || s.term == 0
                    || s.application_schema == 0
            })
            || state.last_term() > state.hard_state.term
            || !state.hard_state.follows(HardState::default())
            || state
                .hard_state
                .voted_for
                .is_some_and(|n| !state.bootstrap.policy.voters().contains(&n))
            || state.entries.len() > limits.max_entries_per_group
        {
            return Err(RaftError::InvalidRecovery);
        }
        let mut previous_term = state.base_term();
        for (i, e) in state.entries.iter().enumerate() {
            if state.base_index().checked_add(i as u64 + 1) != Some(e.index)
                || e.term == 0
                || e.term < previous_term
                || e.payload_bytes() > limits.max_command_bytes
            {
                return Err(RaftError::InvalidRecovery);
            }
            previous_term = e.term;
        }
        Ok(Self {
            node,
            binding,
            limits,
            durable: state,
            role: Role::Follower,
            votes: BTreeSet::new(),
            vote_context: None,
            progress: BTreeMap::new(),
            next_index: BTreeMap::new(),
            requests: BTreeMap::new(),
            request_sequence: 0,
            last_batch: 0,
            pending: None,
            read: None,
            ready_read: None,
            last_read_request: 0,
            fenced: false,
            staged_snapshot: None,
            application_install: None,
            checkpoint_requested: None,
            checkpoint_reconcile: None,
            election_reset: 0,
        })
    }
    pub fn role(&self) -> Role {
        self.role
    }
    /// Volatile local timer-reset sequence, not a term or quorum watermark.
    /// Changes on a campaign, valid leader contact, or a durable granted vote.
    pub fn election_reset_sequence(&self) -> u64 {
        self.election_reset
    }
    pub fn is_fenced(&self) -> bool {
        self.fenced
    }
    fn reset_election(&mut self) -> Result<(), RaftError> {
        self.election_reset = self
            .election_reset
            .checked_add(1)
            .ok_or(RaftError::Exhausted)?;
        Ok(())
    }
    pub fn state(&self) -> &GroupLog {
        &self.durable
    }
    pub fn storage_binding(&self) -> StoreBinding {
        self.binding
    }
    /// Conservative output reservation for one event and its dependent durable
    /// completions. The owner must bound input retention by `input_bytes`, drain
    /// each output batch before another completion, and run only one event in
    /// the visit. Core/log memory and host snapshot-worker buffers are separate.
    pub fn effect_reservation(&self, input_bytes: usize) -> Option<usize> {
        use std::mem::size_of;
        let mut log = self
            .durable
            .entries
            .len()
            .checked_mul(size_of::<LogEntry>())?;
        for entry in &self.durable.entries {
            if let EntryPayload::Command { bytes, .. } = &entry.payload {
                log = log.checked_add(bytes.capacity())?;
            }
        }
        // A heartbeat can produce append and read probes for every peer. Each
        // append contains at most 64 entries and max_batch_bytes payload/framing.
        // Other paths add bounded persist/commit/snapshot-reference metadata.
        let peers = self.durable.bootstrap.policy.voters().len();
        let message = self
            .limits
            .max_batch_bytes
            .checked_add(64 * size_of::<LogEntry>())?
            .checked_add(size_of::<Message>() + 4 * size_of::<Effect>())?;
        log.checked_add(input_bytes.checked_mul(4)?)?
            .checked_add(peers.checked_add(2)?.checked_mul(2)?.checked_mul(message)?)?
            .checked_add(65536)
    }
    /// A suspended runtime visit cannot be released while these dependencies
    /// are unresolved. Outstanding reads still accept their protocol messages.
    pub fn has_pending_dependency(&self) -> bool {
        self.pending.is_some()
            || self.staged_snapshot.is_some()
            || self.application_install.is_some()
            || self.checkpoint_requested.is_some()
            || self.checkpoint_reconcile.is_some()
    }
    /// Host replays these through its application checkpoint/dedup contract.
    pub fn replay_committed(&self) -> &[LogEntry] {
        &self.durable.entries[..(self.durable.commit_index - self.durable.base_index()) as usize]
    }
    pub fn storage_failed(&mut self) {
        self.fenced = true;
        self.pending = None;
        self.staged_snapshot = None;
        self.application_install = None;
        self.checkpoint_requested = None;
        self.checkpoint_reconcile = None;
        self.role = Role::Follower;
        self.clear_reads();
    }

    fn clear_reads(&mut self) {
        self.read = None;
        self.ready_read = None;
    }

    /// Consume authority for the original invocation only, on the serialized
    /// group owner immediately before reading immutable application state.
    /// Insufficient application progress leaves the barrier available to retry.
    pub fn finish_read(
        &mut self,
        barrier: &ReadBarrier,
        applied_index: u64,
    ) -> Result<(), RaftError> {
        if self.fenced {
            return Err(RaftError::Fenced);
        }
        if self.role != Role::Leader {
            return Err(RaftError::NotLeader);
        }
        if self.ready_read.as_ref() != Some(barrier)
            || barrier.group != self.durable.bootstrap.group
            || barrier.configuration != self.durable.bootstrap.configuration
            || barrier.term != self.durable.hard_state.term
            || barrier.context.origin != self.binding
            || barrier.index > self.durable.commit_index
        {
            return Err(RaftError::StaleRead);
        }
        if applied_index < barrier.index {
            return Err(RaftError::NotApplied);
        }
        self.ready_read = None;
        Ok(())
    }

    /// Highest accepted read request in this core lifetime. Allocation floor,
    /// not quorum, persistence, application or contiguous-prefix evidence.
    pub fn read_request_floor(&self) -> u64 {
        self.last_read_request
    }

    fn begin_read(&mut self, request: ReadRequestId) -> Result<Vec<Effect>, RaftError> {
        if self.role != Role::Leader {
            return Err(RaftError::NotLeader);
        }
        if self.read.is_some() || self.ready_read.is_some() {
            return Err(RaftError::ReadInFlight);
        }
        if request.get() <= self.last_read_request {
            return Err(RaftError::StaleRead);
        }
        if self.durable.term_at(self.durable.commit_index) != Some(self.durable.hard_state.term) {
            return Err(RaftError::ReadNotReady);
        }
        let context = self.context()?;
        let barrier = ReadBarrier {
            group: self.durable.bootstrap.group,
            configuration: self.durable.bootstrap.configuration,
            term: self.durable.hard_state.term,
            context,
            request,
            index: self.durable.commit_index,
        };
        self.last_read_request = request.get();
        self.read = Some(PendingRead {
            barrier,
            acknowledgements: BTreeSet::from([self.node]),
        });
        let ready = self.maybe_read_ready();
        if !ready.is_empty() {
            return Ok(ready);
        }
        Ok(self.read_probes())
    }

    fn read_probes(&self) -> Vec<Effect> {
        let Some(read) = &self.read else {
            return Vec::new();
        };
        self.peers()
            .into_iter()
            .map(|peer| Effect::Send(self.message(peer, read.barrier.context, Rpc::ReadProbe)))
            .collect()
    }

    fn maybe_read_ready(&mut self) -> Vec<Effect> {
        if self.read.as_ref().is_some_and(|r| {
            self.durable
                .bootstrap
                .policy
                .is_satisfied(&r.acknowledgements)
        }) {
            let barrier = self.read.take().unwrap().barrier;
            self.ready_read = Some(barrier);
            vec![Effect::ReadReady(barrier)]
        } else {
            Vec::new()
        }
    }

    pub fn step(&mut self, event: Event) -> Result<Vec<Effect>, RaftError> {
        if self.fenced {
            return Err(RaftError::Fenced);
        }
        if self.has_pending_dependency() {
            return Err(RaftError::Busy);
        }
        match event {
            Event::Checkpoint => {
                if self.durable.commit_index <= self.durable.base_index() {
                    return Err(RaftError::NotApplied);
                }
                let context = self.context()?;
                self.checkpoint_requested = Some(context);
                Ok(vec![Effect::CheckpointRequired { context }])
            }
            Event::Campaign => {
                self.reset_election()?;
                self.clear_reads();
                self.role = Role::Candidate;
                self.votes.clear();
                self.requests.clear();
                self.vote_context = None;
                let term = self
                    .durable
                    .hard_state
                    .term
                    .checked_add(1)
                    .ok_or(RaftError::Exhausted)?;
                self.persist(
                    HardState {
                        term,
                        voted_for: Some(self.node),
                    },
                    self.durable.commit_index,
                    None,
                    After::Campaign,
                    None,
                )
            }
            Event::Heartbeat => {
                if self.role == Role::Leader {
                    let mut effects = self.broadcast()?;
                    effects.extend(self.read_probes());
                    Ok(effects)
                } else {
                    Ok(Vec::new())
                }
            }
            Event::Propose { operation, bytes } => {
                if self.role != Role::Leader {
                    return Err(RaftError::NotLeader);
                }
                if bytes.len() > self.limits.max_command_bytes {
                    return Err(StorageError::Rejected("command byte budget").into());
                }
                let index = self
                    .durable
                    .last_index()
                    .checked_add(1)
                    .ok_or(RaftError::Exhausted)?;
                let entry = LogEntry {
                    index,
                    term: self.durable.hard_state.term,
                    payload: EntryPayload::Command { operation, bytes },
                };
                self.persist(
                    self.durable.hard_state,
                    self.durable.commit_index,
                    Some(Suffix {
                        from: index,
                        entries: vec![entry],
                    }),
                    After::LeaderAppend,
                    None,
                )
            }
            Event::Receive(message) => self.receive(message),
            Event::Read { request } => self.begin_read(request),
            Event::CancelRead { request } => {
                if self
                    .read
                    .as_ref()
                    .is_some_and(|r| r.barrier.request == request)
                    || self.ready_read.is_some_and(|r| r.request == request)
                {
                    self.clear_reads();
                    Ok(Vec::new())
                } else {
                    Err(RaftError::StaleRead)
                }
            }
        }
    }

    /// Async/batched workers must correlate the full submitted effect as well
    /// as its scoped ticket before delivering a later durable completion.
    pub fn validate_persist_effect(&self, update: &LogUpdate) -> Result<(), RaftError> {
        if self.fenced {
            return Err(RaftError::Fenced);
        }
        if self
            .pending
            .as_ref()
            .is_none_or(|p| &p.update != update || p.ticket.is_some())
        {
            return Err(RaftError::WrongCompletion);
        }
        Ok(())
    }
    pub fn admit_effect(&mut self, update: &LogUpdate, ticket: LogTicket) -> Result<(), RaftError> {
        if self.pending.as_ref().is_none_or(|p| &p.update != update) {
            return Err(RaftError::WrongCompletion);
        }
        self.admitted(ticket)
    }
    pub fn admitted(&mut self, ticket: LogTicket) -> Result<(), RaftError> {
        if self.fenced {
            return Err(RaftError::Fenced);
        }
        let p = self.pending.as_mut().ok_or(RaftError::WrongCompletion)?;
        if p.ticket.is_some()
            || ticket.binding != self.binding
            || ticket.batch <= self.last_batch
            || ticket.group != p.next.bootstrap.group
            || ticket.revision != p.next.revision
            || ticket.generation != p.next.generation
            || ticket.last_index != p.next.last_index()
            || ticket.term != p.next.hard_state.term
        {
            return Err(RaftError::WrongCompletion);
        }
        self.last_batch = ticket.batch;
        p.ticket = Some(ticket);
        Ok(())
    }
    pub fn complete(&mut self, completion: &DurableLog) -> Result<Vec<Effect>, RaftError> {
        if self.fenced {
            return Err(RaftError::Fenced);
        }
        let p = self.pending.as_ref().ok_or(RaftError::WrongCompletion)?;
        let ticket = p.ticket.ok_or(RaftError::WrongCompletion)?;
        if !completion.tickets.contains(&ticket) {
            return Err(RaftError::WrongCompletion);
        }
        let p = self.pending.take().unwrap();
        let previous_commit = self.durable.commit_index;
        self.durable = p.next;
        let mut effects = Vec::new();
        if self.durable.commit_index > previous_commit
            && !matches!(p.after, After::SnapshotInstall(_))
        {
            effects.push(Effect::Committed(
                self.durable.entries[(previous_commit - self.durable.base_index()) as usize
                    ..(self.durable.commit_index - self.durable.base_index()) as usize]
                    .to_vec(),
            ));
        }
        match p.after {
            After::Campaign => {
                self.votes.insert(self.node);
                if self.durable.bootstrap.policy.is_satisfied(&self.votes) {
                    effects.extend(self.become_leader()?);
                } else {
                    let context = self.context()?;
                    self.vote_context = Some(context);
                    for peer in self.peers() {
                        effects.push(Effect::Send(self.message(
                            peer,
                            context,
                            Rpc::Vote {
                                last_index: self.durable.last_index(),
                                last_term: self.durable.last_term(),
                            },
                        )));
                    }
                }
            }
            After::Reply => {
                if let Some(mut reply) = p.reply {
                    reply.term = self.durable.hard_state.term;
                    effects.push(Effect::Send(reply));
                }
            }
            After::LeaderAppend => {
                self.progress.insert(self.node, self.durable.last_index());
                effects.extend(self.broadcast()?);
                effects.extend(self.maybe_commit()?);
            }
            After::Commit => effects.extend(self.broadcast()?),
            After::Compact => {
                self.requests.clear();
                if self.role == Role::Leader {
                    effects.extend(self.broadcast()?);
                }
            }
            After::SnapshotInstall(reference) => {
                self.application_install =
                    Some((reference, p.reply.ok_or(RaftError::WrongCompletion)?));
                effects.push(Effect::SnapshotInstalled(reference));
            }
            After::CheckpointCompact(reference) => {
                self.requests.clear();
                self.checkpoint_reconcile = Some(reference);
                effects.push(Effect::CheckpointCompacted(reference));
            }
        }
        if effects.iter().any(|e| {
            matches!(
                e,
                Effect::Send(Message {
                    rpc: Rpc::Voted { granted: true },
                    ..
                })
            )
        }) {
            self.reset_election()?;
        }
        Ok(effects)
    }

    fn persist(
        &mut self,
        hard_state: HardState,
        commit_index: u64,
        suffix: Option<Suffix>,
        after: After,
        reply: Option<Message>,
    ) -> Result<Vec<Effect>, RaftError> {
        let update = LogUpdate {
            group: self.durable.bootstrap.group,
            expected_revision: self.durable.revision,
            hard_state,
            commit_index,
            suffix,
            snapshot: None,
        };
        self.persist_update(update, after, reply)
    }
    fn persist_update(
        &mut self,
        update: LogUpdate,
        after: After,
        reply: Option<Message>,
    ) -> Result<Vec<Effect>, RaftError> {
        let mut state = BTreeMap::from([(update.group, self.durable.clone())]);
        apply_batch(
            &mut state,
            &[LogMutation::Update(update.clone())],
            self.limits,
        )?;
        self.pending = Some(Pending {
            update: update.clone(),
            next: state.remove(&update.group).unwrap(),
            ticket: None,
            after,
            reply,
        });
        Ok(vec![Effect::Persist(update)])
    }
    fn peers(&self) -> Vec<NodeId> {
        self.durable
            .bootstrap
            .policy
            .voters()
            .iter()
            .filter(|n| **n != self.node)
            .copied()
            .collect()
    }
    fn context(&mut self) -> Result<RequestContext, RaftError> {
        self.request_sequence = self
            .request_sequence
            .checked_add(1)
            .ok_or(RaftError::Exhausted)?;
        Ok(RequestContext {
            origin: self.binding,
            sequence: self.request_sequence,
        })
    }
    fn message(&self, to: NodeId, context: RequestContext, rpc: Rpc) -> Message {
        Message {
            group: self.durable.bootstrap.group,
            configuration: self.durable.bootstrap.configuration,
            from: self.node,
            sender: self.binding,
            to,
            term: self.durable.hard_state.term,
            context,
            rpc,
        }
    }
    fn become_leader(&mut self) -> Result<Vec<Effect>, RaftError> {
        self.role = Role::Leader;
        self.progress.clear();
        self.next_index.clear();
        self.requests.clear();
        let next = self
            .durable
            .last_index()
            .checked_add(1)
            .ok_or(RaftError::Exhausted)?;
        for peer in self.peers() {
            self.next_index.insert(peer, next);
            self.progress.insert(peer, 0);
        }
        self.progress.insert(self.node, self.durable.last_index());
        let entry = LogEntry {
            index: next,
            term: self.durable.hard_state.term,
            payload: EntryPayload::Noop,
        };
        self.persist(
            self.durable.hard_state,
            self.durable.commit_index,
            Some(Suffix {
                from: next,
                entries: vec![entry],
            }),
            After::LeaderAppend,
            None,
        )
    }
    fn broadcast(&mut self) -> Result<Vec<Effect>, RaftError> {
        let mut effects = Vec::new();
        for peer in self.peers() {
            effects.push(self.append_for(peer)?);
        }
        Ok(effects)
    }
    fn append_for(&mut self, peer: NodeId) -> Result<Effect, RaftError> {
        // Keep an outstanding request's range/context across heartbeat retries.
        // Replacing it every tick can starve all acknowledgements when the
        // network round trip exceeds the heartbeat interval. Term changes and
        // compaction clear requests; responses retire them before new work.
        if let Some(sent) = self.requests.get(&peer).copied() {
            if let Some(reference) = sent.snapshot {
                return Ok(Effect::SnapshotRequired {
                    to: peer,
                    context: sent.context,
                    reference,
                });
            }
            let entries = if sent.start > sent.end {
                Vec::new()
            } else {
                let start = (sent.start - self.durable.base_index() - 1) as usize;
                let end = (sent.end - self.durable.base_index()) as usize;
                self.durable
                    .entries
                    .get(start..end)
                    .ok_or(RaftError::WrongCompletion)?
                    .to_vec()
            };
            return Ok(Effect::Send(
                self.message(
                    peer,
                    sent.context,
                    Rpc::Append {
                        previous_index: sent.start - 1,
                        previous_term: self
                            .durable
                            .term_at(sent.start - 1)
                            .ok_or(RaftError::WrongCompletion)?,
                        entries,
                        leader_commit: self.durable.commit_index,
                    },
                ),
            ));
        }
        let next = self.next_index[&peer]
            .min(self.durable.last_index() + 1)
            .max(1);
        if next <= self.durable.base_index() {
            let reference = self.durable.snapshot.ok_or(RaftError::InvalidRecovery)?;
            let context = self.context()?;
            self.requests.insert(
                peer,
                Replication {
                    context,
                    start: 0,
                    end: reference.index,
                    snapshot: Some(reference),
                },
            );
            return Ok(Effect::SnapshotRequired {
                to: peer,
                context,
                reference,
            });
        }
        let mut entries = Vec::new();
        let mut bytes = 0usize;
        // One bounded RPC can carry several entries; outer wire limits are
        // negotiated by the later codec/transport assembly.
        for entry in self
            .durable
            .entries
            .iter()
            .skip((next - self.durable.base_index() - 1) as usize)
            .take(64)
        {
            let size = 37 + entry.payload_bytes();
            if size > self.limits.max_batch_bytes.saturating_sub(bytes + 256) {
                break;
            }
            entries.push(entry.clone());
            bytes += size;
        }
        if entries.is_empty() && next <= self.durable.last_index() {
            return Err(StorageError::Rejected("entry exceeds replication batch budget").into());
        }
        let end = entries.last().map_or(next - 1, |e| e.index);
        let context = self.context()?;
        self.requests.insert(
            peer,
            Replication {
                context,
                start: next,
                end,
                snapshot: None,
            },
        );
        Ok(Effect::Send(self.message(
            peer,
            context,
            Rpc::Append {
                previous_index: next - 1,
                previous_term: self.durable.term_at(next - 1).unwrap(),
                entries,
                leader_commit: self.durable.commit_index,
            },
        )))
    }
    fn maybe_commit(&mut self) -> Result<Vec<Effect>, RaftError> {
        let frontier = self
            .durable
            .bootstrap
            .policy
            .frontier(&self.progress)
            .min(self.durable.last_index());
        if frontier > self.durable.commit_index
            && self.durable.term_at(frontier) == Some(self.durable.hard_state.term)
        {
            self.persist(self.durable.hard_state, frontier, None, After::Commit, None)
        } else {
            Ok(Vec::new())
        }
    }

    fn receive(&mut self, m: Message) -> Result<Vec<Effect>, RaftError> {
        let contact = m.term >= self.durable.hard_state.term
            && matches!(
                &m.rpc,
                Rpc::Append { .. } | Rpc::ReadProbe | Rpc::Snapshot { .. }
            );
        let effects = self.receive_inner(m)?;
        if contact
            || effects.iter().any(|e| {
                matches!(
                    e,
                    Effect::Send(Message {
                        rpc: Rpc::Voted { granted: true },
                        ..
                    })
                )
            })
        {
            self.reset_election()?;
        }
        Ok(effects)
    }
    fn receive_inner(&mut self, m: Message) -> Result<Vec<Effect>, RaftError> {
        if m.to != self.node
            || m.from == self.node
            || m.group != self.durable.bootstrap.group
            || m.configuration != self.durable.bootstrap.configuration
            || self.durable.bootstrap.voter_stores.get(&m.from) != Some(&m.sender.identity)
        {
            return Err(RaftError::WrongIdentity);
        }
        if m.term == 0 || m.context.sequence == 0 {
            return Err(RaftError::InvalidMessage);
        }
        let request = matches!(
            &m.rpc,
            Rpc::Vote { .. } | Rpc::Append { .. } | Rpc::ReadProbe | Rpc::Snapshot { .. }
        );
        if request && m.context.origin != m.sender {
            return Err(RaftError::WrongIdentity);
        }
        if !request && m.context.origin != self.binding {
            return Err(RaftError::WrongIdentity);
        }
        let old = self.durable.hard_state;
        let hard = if m.term > old.term {
            HardState {
                term: m.term,
                voted_for: None,
            }
        } else {
            old
        };
        if m.term > old.term {
            self.clear_reads();
            self.role = Role::Follower;
            self.vote_context = None;
            self.requests.clear();
        }
        match &m.rpc {
            Rpc::Vote {
                last_index,
                last_term,
            } => {
                if *last_term > m.term || ((*last_index == 0) != (*last_term == 0)) {
                    return Err(RaftError::InvalidMessage);
                }
                let granted = m.term == hard.term
                    && (hard.voted_for.is_none() || hard.voted_for == Some(m.from))
                    && (*last_term, *last_index)
                        >= (self.durable.last_term(), self.durable.last_index());
                let next = HardState {
                    voted_for: if granted {
                        Some(m.from)
                    } else {
                        hard.voted_for
                    },
                    ..hard
                };
                let mut reply = self.message(m.from, m.context, Rpc::Voted { granted });
                reply.term = next.term;
                if next != old {
                    self.persist(
                        next,
                        self.durable.commit_index,
                        None,
                        After::Reply,
                        Some(reply),
                    )
                } else {
                    Ok(vec![Effect::Send(reply)])
                }
            }
            Rpc::Voted { granted } => {
                if hard != old {
                    return self.persist(hard, self.durable.commit_index, None, After::Reply, None);
                }
                if self.role != Role::Candidate
                    || m.term != old.term
                    || self.vote_context != Some(m.context)
                {
                    return Ok(Vec::new());
                }
                if *granted {
                    self.votes.insert(m.from);
                }
                if self.durable.bootstrap.policy.is_satisfied(&self.votes) {
                    self.become_leader()
                } else {
                    Ok(Vec::new())
                }
            }
            Rpc::Append {
                previous_index,
                previous_term,
                entries,
                leader_commit,
            } => {
                if *previous_term > m.term
                    || ((*previous_index == 0) != (*previous_term == 0))
                    || entries.len() > 64
                    || entries.iter().map(LogEntry::payload_bytes).sum::<usize>()
                        > self.limits.max_batch_bytes
                {
                    return Err(RaftError::InvalidMessage);
                }
                if m.term < old.term {
                    return Ok(vec![Effect::Send(self.message(
                        m.from,
                        m.context,
                        Rpc::Appended {
                            success: false,
                            matching_index: self.durable.last_index(),
                        },
                    ))]);
                }
                self.role = Role::Follower;
                self.clear_reads();
                self.vote_context = None;
                self.requests.clear();
                let mut previous = *previous_term;
                for (i, e) in entries.iter().enumerate() {
                    if previous_index.checked_add(i as u64 + 1) != Some(e.index)
                        || e.term == 0
                        || e.term < previous
                        || e.term > m.term
                        || e.payload_bytes() > self.limits.max_command_bytes
                    {
                        return Err(RaftError::InvalidMessage);
                    }
                    previous = e.term;
                }
                if self.durable.term_at(*previous_index) != Some(*previous_term) {
                    let mut reply = self.message(
                        m.from,
                        m.context,
                        if *previous_index < self.durable.base_index() {
                            Rpc::Compacted {
                                index: self.durable.base_index(),
                                term: self.durable.base_term(),
                            }
                        } else {
                            Rpc::Appended {
                                success: false,
                                matching_index: self.durable.last_index(),
                            }
                        },
                    );
                    reply.term = hard.term;
                    return if hard != old {
                        self.persist(
                            hard,
                            self.durable.commit_index,
                            None,
                            After::Reply,
                            Some(reply),
                        )
                    } else {
                        Ok(vec![Effect::Send(reply)])
                    };
                }
                let mut suffix = None;
                for (offset, entry) in entries.iter().enumerate() {
                    match self.durable.entry_at(entry.index) {
                        Some(old_entry) if old_entry.term == entry.term => {
                            if old_entry != entry {
                                return Err(RaftError::InvalidMessage);
                            }
                        }
                        _ => {
                            suffix = Some(Suffix {
                                from: entry.index,
                                entries: entries[offset..].to_vec(),
                            });
                            break;
                        }
                    }
                }
                let matching = entries.last().map_or(*previous_index, |e| e.index);
                let commit = self
                    .durable
                    .commit_index
                    .max((*leader_commit).min(matching));
                let mut reply = self.message(
                    m.from,
                    m.context,
                    Rpc::Appended {
                        success: true,
                        matching_index: matching,
                    },
                );
                reply.term = hard.term;
                if hard != old || suffix.is_some() || commit != self.durable.commit_index {
                    self.persist(hard, commit, suffix, After::Reply, Some(reply))
                } else {
                    Ok(vec![Effect::Send(reply)])
                }
            }
            Rpc::Appended {
                success,
                matching_index,
            } => {
                if hard != old {
                    return self.persist(hard, self.durable.commit_index, None, After::Reply, None);
                }
                if self.role != Role::Leader || m.term != old.term {
                    return Ok(Vec::new());
                }
                let Some(sent) = self.requests.get(&m.from).copied() else {
                    return Ok(Vec::new());
                };
                if sent.context != m.context {
                    return Ok(Vec::new());
                }
                if sent.snapshot.is_some() {
                    return Ok(Vec::new());
                }
                self.requests.remove(&m.from);
                if *success {
                    if *matching_index != sent.end {
                        return Err(RaftError::InvalidMessage);
                    }
                    let prefix = self.progress[&m.from].max(*matching_index);
                    self.progress.insert(m.from, prefix);
                    self.next_index.insert(m.from, prefix + 1);
                    let mut effects = self.maybe_commit()?;
                    if self.pending.is_none() && prefix < self.durable.last_index() {
                        effects.push(self.append_for(m.from)?);
                    }
                    Ok(effects)
                } else {
                    let next = self.next_index[&m.from]
                        .saturating_sub(1)
                        .min(matching_index.saturating_add(1))
                        .max(1);
                    self.next_index.insert(m.from, next);
                    Ok(vec![self.append_for(m.from)?])
                }
            }
            Rpc::ReadProbe => {
                if m.term < old.term {
                    // The higher-term response makes the requester step down;
                    // its term cannot satisfy the request's read quorum.
                    return Ok(vec![Effect::Send(self.message(
                        m.from,
                        m.context,
                        Rpc::ReadAck,
                    ))]);
                }
                self.role = Role::Follower;
                self.clear_reads();
                self.vote_context = None;
                self.requests.clear();
                let mut reply = self.message(m.from, m.context, Rpc::ReadAck);
                reply.term = hard.term;
                if hard != old {
                    self.persist(
                        hard,
                        self.durable.commit_index,
                        None,
                        After::Reply,
                        Some(reply),
                    )
                } else {
                    Ok(vec![Effect::Send(reply)])
                }
            }
            Rpc::ReadAck => {
                if hard != old {
                    return self.persist(hard, self.durable.commit_index, None, After::Reply, None);
                }
                if self.role != Role::Leader || m.term != old.term {
                    return Ok(Vec::new());
                }
                if let Some(read) = &mut self.read {
                    if read.barrier.context == m.context {
                        read.acknowledgements.insert(m.from);
                    }
                }
                Ok(self.maybe_read_ready())
            }
            Rpc::Snapshot { snapshot } => {
                let meta = &snapshot.metadata;
                if snapshot.application.is_empty()
                    || snapshot.application.len() > self.limits.max_snapshot_bytes
                    || meta.bootstrap != self.durable.bootstrap
                    || meta.validate().is_err()
                    || meta.term > m.term
                {
                    return Err(RaftError::InvalidMessage);
                }
                if m.term < old.term {
                    return Ok(vec![Effect::Send(self.message(
                        m.from,
                        m.context,
                        Rpc::SnapshotAck { index: 0 },
                    ))]);
                }
                self.role = Role::Follower;
                self.clear_reads();
                self.vote_context = None;
                self.requests.clear();
                if meta.index <= self.durable.commit_index {
                    if self
                        .durable
                        .term_at(meta.index)
                        .is_some_and(|t| t != meta.term)
                    {
                        return Err(RaftError::InvalidMessage);
                    }
                    let mut reply =
                        self.message(m.from, m.context, Rpc::SnapshotAck { index: meta.index });
                    reply.term = hard.term;
                    return if hard != old {
                        self.persist(
                            hard,
                            self.durable.commit_index,
                            None,
                            After::Reply,
                            Some(reply),
                        )
                    } else {
                        Ok(vec![Effect::Send(reply)])
                    };
                }
                self.staged_snapshot = Some(m.clone());
                Ok(vec![Effect::StageSnapshot(m)])
            }
            Rpc::SnapshotAck { index } => {
                if hard != old {
                    return self.persist(hard, self.durable.commit_index, None, After::Reply, None);
                }
                if self.role != Role::Leader || m.term != old.term {
                    return Ok(Vec::new());
                }
                let Some(sent) = self.requests.get(&m.from).copied() else {
                    return Ok(Vec::new());
                };
                if sent.context != m.context || sent.snapshot.is_none() {
                    return Ok(Vec::new());
                }
                if *index != sent.end {
                    return Err(RaftError::InvalidMessage);
                }
                self.requests.remove(&m.from);
                let prefix = self.progress[&m.from].max(*index);
                self.progress.insert(m.from, prefix);
                self.next_index.insert(m.from, prefix + 1);
                let mut effects = self.maybe_commit()?;
                if self.pending.is_none() {
                    effects.push(self.append_for(m.from)?);
                }
                Ok(effects)
            }
            Rpc::Compacted { index, term } => {
                if hard != old {
                    return self.persist(hard, self.durable.commit_index, None, After::Reply, None);
                }
                if self.role != Role::Leader || m.term != old.term {
                    return Ok(Vec::new());
                }
                let Some(sent) = self.requests.get(&m.from) else {
                    return Ok(Vec::new());
                };
                if sent.context != m.context || sent.snapshot.is_some() {
                    return Ok(Vec::new());
                }
                if *index == 0
                    || *term == 0
                    || *index > self.durable.last_index()
                    || self.durable.term_at(*index).is_some_and(|t| t != *term)
                {
                    return Err(RaftError::InvalidMessage);
                }
                self.requests.remove(&m.from);
                if *index < self.durable.base_index() {
                    self.next_index.insert(m.from, self.durable.base_index());
                } else {
                    self.progress
                        .insert(m.from, self.progress[&m.from].max(*index));
                    self.next_index.insert(m.from, index + 1);
                }
                let mut effects = self.maybe_commit()?;
                if self.pending.is_none() {
                    effects.push(self.append_for(m.from)?);
                }
                Ok(effects)
            }
        }
    }

    pub(crate) fn begin_compact(
        &mut self,
        reference: SnapshotRef,
    ) -> Result<Vec<Effect>, RaftError> {
        if self.fenced {
            return Err(RaftError::Fenced);
        }
        if self.has_pending_dependency() {
            return Err(RaftError::Busy);
        }
        if reference.store != self.binding.identity
            || reference.index > self.durable.commit_index
            || self.durable.term_at(reference.index) != Some(reference.term)
        {
            return Err(RaftError::WrongIdentity);
        }
        self.persist_update(
            LogUpdate {
                group: self.durable.bootstrap.group,
                expected_revision: self.durable.revision,
                hard_state: self.durable.hard_state,
                commit_index: self.durable.commit_index,
                suffix: None,
                snapshot: Some(reference),
            },
            After::Compact,
            None,
        )
    }
    pub(crate) fn checkpoint_matches(&self, context: RequestContext) -> bool {
        !self.fenced && self.checkpoint_requested == Some(context)
    }
    pub(crate) fn checkpoint_stored(
        &mut self,
        context: RequestContext,
        reference: SnapshotRef,
    ) -> Result<Vec<Effect>, RaftError> {
        if !self.checkpoint_matches(context) {
            return Err(RaftError::WrongCompletion);
        }
        self.checkpoint_requested = None;
        let effects = self.begin_compact(reference)?;
        self.pending
            .as_mut()
            .ok_or(RaftError::WrongCompletion)?
            .after = After::CheckpointCompact(reference);
        Ok(effects)
    }
    pub(crate) fn checkpoint_reconciled(
        &mut self,
        reference: SnapshotRef,
    ) -> Result<Vec<Effect>, RaftError> {
        if self.fenced
            || self.checkpoint_reconcile != Some(reference)
            || self.durable.snapshot != Some(reference)
        {
            return Err(RaftError::WrongCompletion);
        }
        self.checkpoint_reconcile = None;
        if self.role == Role::Leader {
            self.broadcast()
        } else {
            Ok(Vec::new())
        }
    }
    pub(crate) fn snapshot_send(
        &self,
        to: NodeId,
        context: RequestContext,
        reference: SnapshotRef,
        snapshot: Snapshot,
    ) -> Result<Vec<Effect>, RaftError> {
        if self.fenced {
            return Err(RaftError::Fenced);
        }
        if self.role != Role::Leader {
            return Err(RaftError::NotLeader);
        }
        if self
            .requests
            .get(&to)
            .is_none_or(|r| r.context != context || r.snapshot != Some(reference))
            || !reference.matches(&snapshot)
            || snapshot.metadata.bootstrap != self.durable.bootstrap
            || snapshot.application.len() > self.limits.max_snapshot_bytes
        {
            return Err(RaftError::WrongCompletion);
        }
        Ok(vec![Effect::Send(self.message(
            to,
            context,
            Rpc::Snapshot {
                snapshot: Box::new(snapshot),
            },
        ))])
    }
    pub(crate) fn staged_matches(&self, message: &Message) -> bool {
        self.staged_snapshot.as_ref() == Some(message) && !self.fenced
    }
    pub(crate) fn snapshot_stored(
        &mut self,
        reference: SnapshotRef,
    ) -> Result<Vec<Effect>, RaftError> {
        let message = self
            .staged_snapshot
            .take()
            .ok_or(RaftError::WrongCompletion)?;
        let Rpc::Snapshot { snapshot } = &message.rpc else {
            return Err(RaftError::WrongCompletion);
        };
        if reference.store != self.binding.identity || !reference.matches(snapshot) {
            self.storage_failed();
            return Err(RaftError::WrongCompletion);
        }
        let hard = HardState {
            term: message.term,
            voted_for: if message.term == self.durable.hard_state.term {
                self.durable.hard_state.voted_for
            } else {
                None
            },
        };
        let reply = self.message(
            message.from,
            message.context,
            Rpc::SnapshotAck {
                index: reference.index,
            },
        );
        self.persist_update(
            LogUpdate {
                group: self.durable.bootstrap.group,
                expected_revision: self.durable.revision,
                hard_state: hard,
                commit_index: reference.index,
                suffix: None,
                snapshot: Some(reference),
            },
            After::SnapshotInstall(reference),
            Some(reply),
        )
    }
    pub(crate) fn snapshot_applied(
        &mut self,
        reference: SnapshotRef,
        index: u64,
    ) -> Result<Vec<Effect>, RaftError> {
        if self.fenced {
            return Err(RaftError::Fenced);
        }
        let (expected, mut reply) = self
            .application_install
            .clone()
            .ok_or(RaftError::WrongCompletion)?;
        if expected != reference || index < reference.index || index > self.durable.commit_index {
            return Err(RaftError::WrongCompletion);
        }
        self.application_install = None;
        reply.term = self.durable.hard_state.term;
        Ok(vec![Effect::Send(reply)])
    }
}

/// Runs the storage portion of effects through the same seam used by hosts.
/// Networking and application effects are returned to the caller, never hidden.
pub fn persist_effect<S: LogStore>(
    raft: &mut Raft,
    store: &mut S,
    update: LogUpdate,
) -> Result<Vec<Effect>, RaftError> {
    let result = (|| {
        if store.binding() != raft.binding {
            return Err(RaftError::WrongIdentity);
        }
        if raft.pending.as_ref().is_none_or(|p| p.update != update) {
            return Err(RaftError::WrongCompletion);
        }
        let tickets = store.append_batch(vec![LogMutation::Update(update)])?;
        if tickets.len() != 1 {
            return Err(RaftError::WrongCompletion);
        }
        // The exact update was checked before submission above.
        raft.admitted(tickets[0])?;
        let completion = store.barrier(&tickets)?;
        raft.complete(&completion)
    })();
    if result.is_err() {
        raft.storage_failed();
    }
    result
}
