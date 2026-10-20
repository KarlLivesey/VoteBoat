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
use crate::log::transition::{LogStamp, OwnedNextLog, SummarizedLog};
#[cfg(test)]
use crate::log::transition::{LogSummary, LogView};
use crate::{
    contracts::{HardState, StorageError},
    identity::*,
    log::*,
    membership::Membership,
    secure::PeerIdentity,
    snapshot::{Snapshot, SnapshotRef},
};
use std::collections::{BTreeMap, BTreeSet, VecDeque};
use std::ops::Range;
use std::sync::atomic::{AtomicU64, Ordering};

mod admitted_replication;
mod append_hold;
pub use append_hold::AppendHold;
mod batch_limits;
pub use batch_limits::BatchLimits;
mod completion;
use completion::PreparedCompletion;
pub use completion::{
    persist_effect_borrowed, persist_volatile, persist_volatile_into, CompletionEffects,
};
mod leadership;
use admitted_replication::{AdmittedRange, EmptyAppendAlias};
mod log_staging;
mod progress_broadcast;
mod proposals;
pub use proposals::{Proposal, ProposalBatch, MAX_PROPOSAL_BATCH_ENTRIES};
mod readiness;
mod receive;
mod send_window;
mod source;
pub use leadership::{LeadershipTransfer, LeadershipTransferRequest};
pub use readiness::*;
mod configuration;
pub use authority::ReplicationAuthorizationStatus;
pub use configuration::*;

/// A host-owned effect queue that cold paths append their owned effects to.
trait AppendEffects {
    fn append_result(&mut self, effects: Result<Vec<Effect>, RaftError>) -> Result<(), RaftError>;
}
impl AppendEffects for VecDeque<Effect> {
    fn append_result(&mut self, effects: Result<Vec<Effect>, RaftError>) -> Result<(), RaftError> {
        self.extend(effects?);
        Ok(())
    }
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub struct RequestContext {
    pub origin: StoreBinding,
    pub sequence: u64,
}
#[derive(Clone, Debug, Eq, PartialEq)]
pub enum Rpc {
    /// Wire8: request a normal election after exact durable catch-up.
    TimeoutNow {
        operation: OperationId,
        index: u64,
        log_term: u64,
    },
    /// Committed stable checkpoint from an authenticated old-view voter (wire7).
    CommittedLearnerRepairSnapshot {
        snapshot: Box<Snapshot>,
    },
    /// Historical committed checkpoint repair to an exact old-view learner.
    LearnerRepairSnapshot {
        snapshot: Box<Snapshot>,
    },
    /// Pre-election pure extension to an exact committed learner. No commit claim.
    LearnerRepair {
        joint: Box<LogEntry>,
        previous_index: u64,
        previous_term: u64,
        entries: Vec<LogEntry>,
    },
    /// Durable repair cursor evidence; never a ballot or replication quorum ack.
    LearnerRepaired {
        success: bool,
        matching_index: u64,
        matching_term: u64,
    },
    LearnerReadinessRequest(Box<LearnerReadinessRequest>),
    LearnerReadinessReply {
        request: Box<LearnerReadinessRequest>,
        ready: bool,
    },
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
        /// Shared immutable entries; the wire encoding is the visible sequence.
        entries: EntryBatch,
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
    /// Zero index refuses an older-term snapshot and conveys the durable term.
    /// A successful reply must match the exact requested snapshot boundary.
    SnapshotAck {
        index: u64,
    },
    Compacted {
        index: u64,
        term: u64,
    },
    /// Direct query to a voter trusted in the requester's configuration.
    AuthorityRequest {
        candidate: PeerIdentity,
        configuration: ConfigurationId,
    },
    /// Committed membership evidence, never a ballot or read acknowledgement.
    AuthorityReply {
        candidate: PeerIdentity,
        configuration: ConfigurationId,
        committed_index: u64,
        committed_term: u64,
        granted: bool,
    },
}
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct Message {
    pub group: GroupIdentity,
    /// Requester's accepted configuration. Replies echo the request's scope;
    /// this is not a claim about the responder's current membership.
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
#[derive(Clone, Copy, Eq, PartialEq)]
pub(crate) enum RecoveryMode {
    StaticVoter,
    BootstrapLearner,
    Member,
}
#[derive(Clone, Debug, Eq, PartialEq)]
pub enum Event {
    TransferLeadership(LeadershipTransferRequest),
    CancelLeadershipTransfer {
        context: RequestContext,
    },
    CheckLearnerReadiness {
        learner: PeerIdentity,
        session: StoreSession,
        requirements: ReadinessRequirements,
    },
    CancelLearnerReadiness,
    /// Host-authorized administrative input; public network configuration
    /// ingress remains independently gated. This is not an application command.
    Configure(Box<ConfigurationProposal>),
    /// Volatile host maintenance gate. Recovery enables campaigning again;
    /// restore durable maintenance intent before exposing a recovered replica.
    SetCampaigning {
        enabled: bool,
    },
    Campaign,
    Heartbeat,
    /// Send every follower Append the leader holds under
    /// [`AppendHold::QuorumFirst`]. Hosts deliver it when their hold bound
    /// expires. Idempotent: a non-leader or a leader without holds emits nothing.
    ReleaseHeldAppends,
    /// Request a local application checkpoint; never establishes quorum evidence.
    Checkpoint,
    /// Request replication-only authorization from one currently trusted voter.
    AuthorizeReplication {
        witness: PeerIdentity,
        candidate: PeerIdentity,
        configuration: ConfigurationId,
    },
    /// Discard both the pending query and any installed replication permit.
    CancelReplicationAuthorization,
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
    /// Read-only maintenance, suspended under the original owner visit.
    VerifyLearnerReadiness(Message),
    Persist(LogUpdate),
    /// Emitted only by a core recovered with [`Raft::recover_volatile`]: the
    /// core already owns the planned transition, so the token carries no data.
    /// Complete it with [`persist_volatile`]; it is never durability evidence.
    PersistVolatile(VolatilePersist),
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
/// Correlates one pending transition of a volatile core with its completion.
/// Only the core constructs it; it names the exact core incarnation and the
/// revision and generation the pending transition installs, and owns no log
/// data. Revision and generation are local to one log history, so the
/// incarnation keeps a token from another core, or from an earlier recovery of
/// the same history, from completing this one. Not a durability receipt.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub struct VolatilePersist {
    incarnation: u64,
    revision: LogRevision,
    generation: LogGeneration,
}
/// Process-unique volatile core incarnations, one per `recover_volatile` call.
/// Only equality is used, never ordering or a protocol decision, and no value
/// outlives the process: a volatile core has no cross-process restart. Zero is
/// never issued, so a durable core's zero matches no token.
static VOLATILE_INCARNATIONS: AtomicU64 = AtomicU64::new(1);
impl VolatilePersist {
    pub fn revision(&self) -> LogRevision {
        self.revision
    }
    pub fn generation(&self) -> LogGeneration {
        self.generation
    }
}
#[derive(Clone, Debug, Eq, PartialEq)]
pub enum RaftError {
    Configuration(Box<ConfigurationProposalError>),
    /// Owner-side deterministic application validation rejected this invocation
    /// before the core proposed it. This is not a replicated outcome.
    Admission(crate::application::ApplicationError),
    Busy,
    Fenced,
    NotLeader,
    /// A learner or removed replica cannot start an election.
    NotVoter,
    WrongIdentity,
    InvalidMessage,
    WrongCompletion,
    Exhausted,
    InvalidRecovery,
    ReadNotReady,
    ReadInFlight,
    StaleRead,
    StaleTransfer,
    NotApplied,
    Storage(StorageError),
}
impl From<StorageError> for RaftError {
    fn from(e: StorageError) -> Self {
        Self::Storage(e)
    }
}

#[derive(Clone, Debug)]
enum After {
    Campaign,
    Reply,
    LeaderAppend,
    Commit,
    Compact,
    // Snapshot references are boxed: every round moves `Pending`.
    CheckpointCompact(Box<SnapshotRef>),
    SnapshotInstall(Box<SnapshotRef>),
}
struct Pending {
    next: OwnedNextLog,
    // Filled for every full-replacement plan (`next.full()`): configuration
    // entries, a new ballot over a configured history, truncating suffixes and
    // snapshot installs. Ordinary appends and scalar commit rounds leave it
    // `None` and allocate nothing.
    membership: Option<Box<Membership>>,
    ticket: Option<LogTicket>,
    after: After,
    reply: Option<Message>,
    // The stamp of the view `next` was planned from; the install skips the
    // predecessor comparison only if it is still the log's current one.
    planned: Option<LogStamp>,
}
#[derive(Clone, Copy)]
struct Replication {
    context: RequestContext,
    configuration: ConfigurationId,
    start: u64,
    end: u64,
    snapshot: Option<SnapshotRef>,
    // Original notification frontier; retries do not acknowledge a newer one.
    commit_at_start: u64,
}
struct PendingRead {
    barrier: ReadBarrier,
    acknowledgements: BTreeSet<NodeId>,
}

pub struct Raft {
    node: NodeId,
    binding: StoreBinding,
    limits: LogLimits,
    batch_limits: BatchLimits,
    batch_limits_locked: bool,
    // The canonical log and its summary, changed only together at install.
    durable: SummarizedLog,
    checkpoint_lineage: CheckpointLineage,
    membership: Membership,
    role: Role,
    campaigning_enabled: bool,
    votes: BTreeSet<NodeId>,
    vote_context: Option<RequestContext>,
    granted_vote_reply: Option<send_window::GrantedVoteReply>,
    progress: BTreeMap<NodeId, u64>,
    next_index: BTreeMap<NodeId, u64>,
    requests: BTreeMap<NodeId, Replication>,
    append_hold: AppendHold,
    // Leader only, at most one entry per peer: held peer -> leader tip when held.
    held: BTreeMap<NodeId, u64>,
    // Voting peers, most recent successful acknowledgement first.
    ack_order: Vec<NodeId>,
    // Leader durable tip before the most recent leader batch became durable.
    batch_floor: u64,
    admitted_ranges: BTreeMap<NodeId, AdmittedRange>,
    empty_append_aliases: BTreeMap<NodeId, EmptyAppendAlias>,
    // Candidate-only transfer cursors; never quorum replication or ballots.
    repair_requests: BTreeMap<NodeId, Replication>,
    batched_joint_repair: bool,
    snapshot_joint_repair: bool,
    committed_snapshot_repair: bool,
    configuration_replication: bool,
    request_sequence: u64,
    last_batch: u64,
    pending: Option<Pending>,
    read: Option<PendingRead>,
    ready_read: Option<ReadBarrier>,
    last_read_request: u64,
    fenced: bool,
    staged_snapshot: Option<Message>,
    staged_snapshot_repair: bool,
    application_install: Option<(SnapshotRef, Message)>,
    checkpoint_requested: Option<RequestContext>,
    checkpoint_reconcile: Option<SnapshotRef>,
    election_reset: u64,
    authority_request: Option<authority::PendingAuthority>,
    replication_permit: Option<authority::ReplicationPermit>,
    learner_readiness: Option<LearnerReadinessRequest>,
    ready_learner: Option<ReadyLearner>,
    readiness_check: Option<Message>,
    transfer: Option<LeadershipTransfer>,
    leader_contact: Option<(StoreBinding, NodeId, u64)>,
    // The core's log is the only copy: completions use `PersistVolatile` tokens.
    volatile: bool,
    // Nonzero only for a volatile core: the `recover_volatile` call that made it.
    incarnation: u64,
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
    /// Explicit host-authorized enrollment/restart of a non-voting learner.
    /// Its exact store must appear in both committed and accepted membership.
    /// Only learner-assignment changes retaining the bootstrap electorate are
    /// currently supported. Network traffic cannot create or enroll a replica.
    /// Compacted replicas require snapshot::recover_learner_replica instead.
    pub fn recover_learner(
        node: NodeId,
        binding: StoreBinding,
        state: GroupLog,
        limits: LogLimits,
    ) -> Result<Self, RaftError> {
        if state.snapshot.is_some() {
            return Err(RaftError::InvalidRecovery);
        }
        Self::recover_learner_verified(node, binding, state, limits)
    }
    /// Explicit host-authorized restart/import of a dynamic group member.
    /// Both committed and accepted membership must assign this exact store.
    /// Accepted log/snapshot state reconstructs voter or learner role; this
    /// entry point is not network enrollment or proof of remote commitment.
    /// Compacted state requires snapshot::recover_member_replica to verify data.
    pub fn recover_member(
        node: NodeId,
        binding: StoreBinding,
        state: GroupLog,
        limits: LogLimits,
    ) -> Result<Self, RaftError> {
        if state.snapshot.is_some() {
            return Err(RaftError::InvalidRecovery);
        }
        Self::recover_member_verified(node, binding, state, limits)
    }
    pub(crate) fn recover_verified(
        node: NodeId,
        binding: StoreBinding,
        state: GroupLog,
        limits: LogLimits,
    ) -> Result<Self, RaftError> {
        Self::recover_checked(node, binding, state, limits, RecoveryMode::StaticVoter)
    }
    pub(crate) fn recover_learner_verified(
        node: NodeId,
        binding: StoreBinding,
        state: GroupLog,
        limits: LogLimits,
    ) -> Result<Self, RaftError> {
        Self::recover_checked(node, binding, state, limits, RecoveryMode::BootstrapLearner)
    }
    pub(crate) fn recover_member_verified(
        node: NodeId,
        binding: StoreBinding,
        state: GroupLog,
        limits: LogLimits,
    ) -> Result<Self, RaftError> {
        Self::recover_checked(node, binding, state, limits, RecoveryMode::Member)
    }
    fn recover_checked(
        node: NodeId,
        binding: StoreBinding,
        state: GroupLog,
        limits: LogLimits,
        mode: RecoveryMode,
    ) -> Result<Self, RaftError> {
        let limits = limits.validate()?;
        let membership = Self::validate_recovery(node, binding, &state, limits, mode)?;
        Ok(Self {
            node,
            binding,
            limits,
            batch_limits: BatchLimits::default(),
            batch_limits_locked: false,
            durable: SummarizedLog::new(state),
            checkpoint_lineage: CheckpointLineage::new(),
            membership,
            role: Role::Follower,
            campaigning_enabled: true,
            votes: BTreeSet::new(),
            vote_context: None,
            granted_vote_reply: None,
            progress: BTreeMap::new(),
            next_index: BTreeMap::new(),
            requests: BTreeMap::new(),
            append_hold: AppendHold::Off,
            held: BTreeMap::new(),
            ack_order: Vec::new(),
            batch_floor: 0,
            admitted_ranges: BTreeMap::new(),
            empty_append_aliases: BTreeMap::new(),
            repair_requests: BTreeMap::new(),
            batched_joint_repair: false,
            snapshot_joint_repair: false,
            committed_snapshot_repair: false,
            configuration_replication: false,
            request_sequence: 0,
            last_batch: 0,
            pending: None,
            read: None,
            ready_read: None,
            last_read_request: 0,
            fenced: false,
            staged_snapshot: None,
            staged_snapshot_repair: false,
            application_install: None,
            checkpoint_requested: None,
            checkpoint_reconcile: None,
            election_reset: 0,
            authority_request: None,
            replication_permit: None,
            learner_readiness: None,
            ready_learner: None,
            readiness_check: None,
            transfer: None,
            leader_contact: None,
            volatile: false,
            incarnation: 0,
        })
    }
    /// Static voter over a [`VolatileLog`]: the core's log becomes the only
    /// canonical copy, kept in process memory. Nothing is durable; every
    /// persistence dependency is a payload-free [`Effect::PersistVolatile`].
    /// Recovery validation is the same as for a durable static voter. A fenced
    /// volatile core still exports its last completed state through
    /// [`Self::state`], which can be recovered here in the same process. If that
    /// state is compacted, the host's application must already be at or past the
    /// snapshot anchor, or be restored with `snapshot::restore_application`.
    ///
    /// Membership is static: the core refuses every configuration transition
    /// (see [`Event::Configure`] and replicated configuration entries), so any
    /// state it completes is recoverable here. Each call is a new incarnation;
    /// tokens emitted by any earlier core never complete the returned one.
    pub fn recover_volatile(
        node: NodeId,
        log: &VolatileLog,
        state: GroupLog,
    ) -> Result<Self, RaftError> {
        let mut core = Self::recover_checked(
            node,
            log.binding(),
            state,
            log.limits(),
            RecoveryMode::StaticVoter,
        )?;
        core.incarnation = Self::next_volatile_incarnation()?;
        core.volatile = true;
        Ok(core)
    }
    fn next_volatile_incarnation() -> Result<u64, RaftError> {
        // Uniqueness needs only the atomic increment, not ordering with other memory.
        VOLATILE_INCARNATIONS
            .fetch_update(Ordering::Relaxed, Ordering::Relaxed, |n| n.checked_add(1))
            .map_err(|_| RaftError::Exhausted)
    }
    /// True for a core whose log is the only copy (see [`Self::recover_volatile`]).
    pub fn is_volatile(&self) -> bool {
        self.volatile
    }
    fn validate_recovery(
        node: NodeId,
        binding: StoreBinding,
        state: &GroupLog,
        limits: LogLimits,
        mode: RecoveryMode,
    ) -> Result<Membership, RaftError> {
        if (mode == RecoveryMode::StaticVoter
            && state.bootstrap.voter_stores.get(&node) != Some(&binding.identity))
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
                    || s.configuration
                        != state
                            .snapshot_membership
                            .as_ref()
                            .map_or(state.bootstrap.configuration, |m| m.id())
                    || s.term == 0
                    || s.application_schema == 0
            })
            || state.last_term() > state.hard_state.term
            || !state.hard_state.follows(HardState::default())
            || state.validate_ballot().is_err()
            || state.entries.len() > limits.max_entries_per_group
            || state
                .snapshot_membership
                .as_ref()
                .is_some_and(|m| m.retained_bytes() > limits.max_batch_bytes)
            || (mode == RecoveryMode::StaticVoter && state.snapshot_membership.is_some())
            || (mode == RecoveryMode::StaticVoter
                && state
                    .entries
                    .iter()
                    .any(|e| matches!(e.payload, EntryPayload::Configuration(_))))
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
        let membership = state.membership().map_err(|_| RaftError::InvalidRecovery)?;
        Self::validate_recovery_assignment(node, binding, state, mode, &membership)?;
        Ok(membership)
    }
    fn validate_recovery_assignment(
        node: NodeId,
        binding: StoreBinding,
        state: &GroupLog,
        mode: RecoveryMode,
        membership: &Membership,
    ) -> Result<(), RaftError> {
        if mode == RecoveryMode::Member {
            let committed = state
                .membership_at(state.commit_index)
                .map_err(|_| RaftError::InvalidRecovery)?;
            if membership.replica_store(node) != Some(binding.identity)
                || committed.replica_store(node) != Some(binding.identity)
            {
                return Err(RaftError::InvalidRecovery);
            }
        }
        if mode == RecoveryMode::BootstrapLearner {
            let committed = state
                .membership_at(state.commit_index)
                .map_err(|_| RaftError::InvalidRecovery)?;
            let assignment_only = |m: &Membership| {
                m.joint().is_none()
                    && m.stable().policy() == &state.bootstrap.policy
                    && m.stable().voter_stores() == &state.bootstrap.voter_stores
            };
            if !assignment_only(membership)
                || !assignment_only(&committed)
                || membership.stable().learners().get(&node) != Some(&binding.identity)
                || committed.stable().learners().get(&node) != Some(&binding.identity)
                || state.hard_state.voted_for.is_some()
                || state
                    .snapshot_membership
                    .as_ref()
                    .is_some_and(|m| !assignment_only(m))
                || state.entries.iter().any(|e| {
                    matches!(&e.payload,
                    EntryPayload::Configuration(record) if !matches!(record.change,
                        crate::membership::ConfigurationChange::Learners(_)))
                })
            {
                return Err(RaftError::InvalidRecovery);
            }
        }
        Ok(())
    }

    pub fn role(&self) -> Role {
        self.role
    }
    /// Local maintenance policy, not membership or evidence of remote quorum.
    pub fn campaigning_enabled(&self) -> bool {
        self.campaigning_enabled
    }
    pub fn local_node(&self) -> NodeId {
        self.node
    }
    /// Volatile local timer-reset sequence, not a term or quorum watermark.
    /// Changes on a campaign, valid leader contact, a durable granted vote,
    /// or transfer of an original logical Vote/granted Voted into its authorized queue.
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
    /// Immutable checkpoint comparison input from this exact canonical owner.
    /// Recovered or rewritten owners never share a prior comparison lineage.
    pub fn checkpoint_log(&self) -> CheckpointLog<'_> {
        CheckpointLog::new(&self.durable, &self.checkpoint_lineage)
    }
    /// Latest accepted-log predicate. Pending accepted state can precede its
    /// durable WAL completion; this value is never a durability receipt.
    /// The core processes no other events while that dependency is pending.
    pub fn membership(&self) -> &Membership {
        self.pending
            .as_ref()
            .and_then(|p| p.membership.as_deref())
            .unwrap_or(&self.membership)
    }
    pub fn storage_binding(&self) -> StoreBinding {
        self.binding
    }
    /// Conservative output reservation for one event and its dependent durable
    /// completions. The owner must bound input retention by `input_bytes`, drain
    /// each output batch before another completion, and run only one event in
    /// the visit. Core/log memory and host snapshot-worker buffers are separate.
    /// Covers the accepted head and every configuration reachable by truncating
    /// its uncommitted suffix. Use `event_effect_reservation` for incoming growth.
    pub fn effect_reservation(&self, input_bytes: usize) -> Option<usize> {
        self.reserve_effects(input_bytes, self.rollback_replicas())
    }
    /// Reserve before executing an event, including prospective membership in
    /// incoming append/snapshot data. This is a memory bound, never authority to
    /// accept that data; configuration ingress still has its protocol gates.
    pub fn event_effect_reservation(&self, event: &Event, input_bytes: usize) -> Option<usize> {
        use crate::membership::ConfigurationChange;
        let mut peers = self.rollback_replicas();
        if let Event::Configure(proposal) = event {
            if let crate::membership::ConfigurationChange::Learners(next)
            | crate::membership::ConfigurationChange::Joint { next, .. } =
                &proposal.record.change
            {
                peers = peers
                    .saturating_add(next.voter_stores().len() + next.learners().len())
                    .min(crate::quorum::Limits::default().max_voters);
            }
        }
        if let Event::Receive(message) = event {
            match &message.rpc {
                Rpc::Append { entries, .. } => {
                    // Any surviving prefix may be the append's starting view.
                    // Count bounds avoid cloning/replaying untrusted history.
                    let mut stable = peers;
                    let mut joint = None;
                    for entry in entries {
                        if let EntryPayload::Configuration(record) = &entry.payload {
                            match &record.change {
                                ConfigurationChange::Learners(next) => {
                                    stable = next.voter_stores().len() + next.learners().len();
                                    joint = None;
                                }
                                ConfigurationChange::Joint { next, .. } => {
                                    joint = Some(next.voter_stores().len() + next.learners().len());
                                }
                                ConfigurationChange::Final { .. } => {
                                    stable = joint.take().unwrap_or(stable);
                                }
                            }
                            // Validated membership limits the union to this cap.
                            // Invalid histories are rejected before fanout.
                            peers = peers.max(
                                (stable + joint.unwrap_or(0))
                                    .min(crate::quorum::Limits::default().max_voters),
                            );
                        }
                    }
                }
                Rpc::Snapshot { snapshot }
                | Rpc::LearnerRepairSnapshot { snapshot }
                | Rpc::CommittedLearnerRepairSnapshot { snapshot } => {
                    if let Some(base) = &snapshot.metadata.membership {
                        peers = peers.max(base.replicas().count());
                    } else {
                        peers = peers.max(snapshot.metadata.bootstrap.voter_stores.len());
                    }
                }
                _ => (),
            }
        }
        self.reserve_effects(input_bytes, peers)
    }
    fn rollback_replicas(&self) -> usize {
        if self.durable.summary().is_plain() {
            // A plain log has no configuration entry or snapshot membership:
            // its accepted membership is the bootstrap electorate. Only a
            // pending full plan can name other replicas.
            let voters = self.durable.bootstrap.voter_stores.len();
            debug_assert_eq!(self.membership.replicas().count(), voters);
            return match self.pending.as_ref().and_then(|p| p.membership.as_deref()) {
                Some(pending) => voters.max(pending.replicas().count()),
                None => voters,
            };
        }
        use crate::membership::ConfigurationChange;
        let base = self.durable.snapshot_membership.as_deref();
        let mut stable = base.map(Membership::stable);
        let mut joint = base.and_then(Membership::joint).map(|j| &j.next);
        let count = |stable: Option<&crate::membership::Configuration>,
                     joint: Option<&crate::membership::Configuration>| {
            let mut count = stable.map_or(self.durable.bootstrap.voter_stores.len(), |c| {
                c.voter_stores().len() + c.learners().len()
            });
            if let Some(next) = joint {
                count += next
                    .voter_stores()
                    .keys()
                    .chain(next.learners().keys())
                    .filter(|node| {
                        stable.map_or_else(
                            || !self.durable.bootstrap.voter_stores.contains_key(node),
                            |c| {
                                !c.voter_stores().contains_key(node)
                                    && !c.learners().contains_key(node)
                            },
                        )
                    })
                    .count();
            }
            count
        };
        let mut maximum = self.membership().replicas().count();
        for entry in &self.durable.entries {
            if entry.index > self.durable.commit_index {
                maximum = maximum.max(count(stable, joint));
            }
            if let EntryPayload::Configuration(record) = &entry.payload {
                match &record.change {
                    ConfigurationChange::Learners(next) => {
                        stable = Some(next);
                        joint = None;
                    }
                    ConfigurationChange::Joint { next, .. } => joint = Some(next),
                    ConfigurationChange::Final { .. } => {
                        stable = joint.take().or(stable);
                    }
                }
            }
        }
        maximum.max(count(stable, joint))
    }
    fn reserve_effects(&self, input_bytes: usize, peers: usize) -> Option<usize> {
        use std::mem::size_of;
        let log = self.retained_log_bytes()?;
        // A heartbeat can produce append and read probes for every peer. Each
        // append uses its configured entry count and max_batch_bytes framing.
        // Other paths add bounded persist/commit/snapshot-reference metadata.
        let message = self
            .limits
            .max_batch_bytes
            .checked_add(self.batch_limits.max_replication_entries.max(64) * size_of::<LogEntry>())?
            .checked_add(size_of::<Message>() + 4 * size_of::<Effect>())?;
        log.checked_add(input_bytes.checked_mul(4)?)?
            .checked_add(peers.checked_add(2)?.checked_mul(2)?.checked_mul(message)?)?
            .checked_add(65536)
    }
    /// A suspended runtime visit cannot be released while these dependencies
    /// are unresolved. Outstanding reads still accept their protocol messages.
    pub fn has_pending_dependency(&self) -> bool {
        self.readiness_check.is_some() || self.has_persistence_dependency()
    }
    fn has_persistence_dependency(&self) -> bool {
        self.pending.is_some()
            || self.staged_snapshot.is_some()
            || self.application_install.is_some()
            || self.checkpoint_requested.is_some()
            || self.checkpoint_reconcile.is_some()
    }
    /// Host replays these through its application checkpoint/dedup contract.
    pub fn replay_committed(&self) -> EntryRange<'_> {
        self.durable
            .entries
            .borrowed(0..(self.durable.commit_index - self.durable.base_index()) as usize)
            .expect("committed prefix is retained")
    }
    pub fn storage_failed(&mut self) {
        self.checkpoint_lineage = CheckpointLineage::new();
        self.clear_replication_requests();
        self.transfer = None;
        self.leader_contact = None;
        self.readiness_check = None;
        self.cancel_learner_readiness();
        self.clear_replication_authority();
        self.fenced = true;
        self.pending = None;
        self.staged_snapshot = None;
        self.staged_snapshot_repair = false;
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
            || barrier.configuration != self.membership().id()
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
        if self.role != Role::Leader || !self.local_voter() {
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
            configuration: self.membership().id(),
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
        self.voting_peers()
            .into_iter()
            .map(|peer| Effect::Send(self.message(peer, read.barrier.context, Rpc::ReadProbe)))
            .collect()
    }

    fn maybe_read_ready(&mut self) -> Vec<Effect> {
        if self.read.as_ref().is_some_and(|r| {
            r.barrier.configuration == self.membership().id()
                && self.membership().is_satisfied(&r.acknowledgements)
        }) {
            let barrier = self.read.take().unwrap().barrier;
            self.ready_read = Some(barrier);
            vec![Effect::ReadReady(barrier)]
        } else {
            Vec::new()
        }
    }

    /// Handle one event, returning its effects in a new `Vec`. This wraps
    /// [`Self::step_into`] over a fresh queue, so a non-empty result has at
    /// least the standard library's minimum non-empty capacity: four effects
    /// (4 × 272 B on 64-bit targets) for a one-effect result. A host that
    /// charges memory by capacity sees that; reuse one queue with `step_into`
    /// to allocate no effect storage per call.
    pub fn step(&mut self, event: Event) -> Result<Vec<Effect>, RaftError> {
        let mut effects = VecDeque::new();
        self.step_into(event, &mut effects)?;
        Ok(effects.into())
    }
    /// [`Self::step`] into a host-owned queue: this event's effects are
    /// appended, in the same order, after any effects `out` already holds.
    /// Earlier effects are never read or changed. On error `out` is truncated
    /// back to them, as the Vec form returns no effects. Reusing one queue
    /// across calls avoids allocating effect storage per call.
    pub fn step_into(&mut self, event: Event, out: &mut VecDeque<Effect>) -> Result<(), RaftError> {
        let mark = out.len();
        let result = self.step_effects(event, out);
        if result.is_err() {
            out.truncate(mark);
        }
        result
    }
    fn step_effects(&mut self, event: Event, out: &mut VecDeque<Effect>) -> Result<(), RaftError> {
        self.begin_step()?;
        let effects = match event {
            Event::TransferLeadership(request) => self.start_transfer(request),
            Event::CancelLeadershipTransfer { context } => self.cancel_transfer(context),
            Event::CheckLearnerReadiness {
                learner,
                session,
                requirements,
            } => self.request_learner_readiness(learner, session, requirements),
            Event::CancelLearnerReadiness => {
                self.cancel_learner_readiness();
                Ok(Vec::new())
            }
            Event::Configure(_) if self.transfer.is_some() => Err(RaftError::Busy),
            Event::Configure(proposal) => self.configure(*proposal),
            Event::SetCampaigning { enabled } => self.set_campaigning(enabled),
            Event::AuthorizeReplication {
                witness,
                candidate,
                configuration,
            } => self.authorize_replication(witness, candidate, configuration),
            Event::CancelReplicationAuthorization => {
                self.clear_replication_authority();
                Ok(Vec::new())
            }
            Event::Checkpoint => {
                if self.durable.commit_index <= self.durable.base_index() {
                    return Err(RaftError::NotApplied);
                }
                let context = self.context()?;
                self.checkpoint_requested = Some(context);
                Ok(vec![Effect::CheckpointRequired { context }])
            }
            Event::Campaign if self.transfer.is_some() => Err(RaftError::Busy),
            Event::Campaign => self.campaign(),
            Event::Heartbeat => {
                if let Some(transfer) = &mut self.transfer {
                    transfer.signal_sent = false;
                }
                if self.role == Role::Leader {
                    self.broadcast_into(out)?;
                    out.extend(self.read_probes());
                }
                Ok(Vec::new())
            }
            Event::ReleaseHeldAppends => self.release_held_into(out).map(|()| Vec::new()),
            Event::Propose { operation, bytes } => self
                .propose_into(operation, bytes, out)
                .map(|()| Vec::new()),
            Event::Receive(message) => self.receive_into(message, out).map(|()| Vec::new()),
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
        }?;
        out.extend(effects);
        self.finish_step_into(out)
    }
    fn set_campaigning(&mut self, enabled: bool) -> Result<Vec<Effect>, RaftError> {
        if self.campaigning_enabled != enabled {
            self.reset_election()?;
            self.campaigning_enabled = enabled;
            if !enabled && self.role == Role::Candidate {
                self.role = Role::Follower;
                self.votes.clear();
                self.vote_context = None;
                self.repair_requests.clear();
            }
        }
        Ok(Vec::new())
    }
    fn campaign(&mut self) -> Result<Vec<Effect>, RaftError> {
        if !self.campaigning_enabled {
            return Err(RaftError::Busy);
        }
        if !self.local_voter() {
            return Err(RaftError::NotVoter);
        }
        self.reset_election()?;
        self.transfer = None;
        self.leader_contact = None;
        self.clear_reads();
        self.role = Role::Candidate;
        self.votes.clear();
        self.clear_replication_requests();
        self.repair_requests.clear();
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
    fn request_learner_readiness(
        &mut self,
        learner: PeerIdentity,
        session: StoreSession,
        requirements: ReadinessRequirements,
    ) -> Result<Vec<Effect>, RaftError> {
        let request = self
            .begin_learner_readiness(learner, session, requirements)
            .map_err(|e| match e {
                ReadinessError::Consensus(e) => e,
                _ => RaftError::InvalidMessage,
            })?;
        Ok(vec![Effect::Send(Message {
            group: request.group,
            configuration: request.configuration,
            from: self.node,
            sender: self.binding,
            to: learner.node,
            term: request.term,
            context: request.context,
            rpc: Rpc::LearnerReadinessRequest(Box::new(request)),
        })])
    }

    /// Async/batched workers must correlate the full submitted effect as well
    /// as its scoped ticket before delivering a later durable completion.
    pub fn validate_persist_effect(&self, update: &LogUpdate) -> Result<(), RaftError> {
        if self.fenced {
            return Err(RaftError::Fenced);
        }
        if self.volatile
            || self
                .pending
                .as_ref()
                .is_none_or(|p| !p.next.matches_update(update) || p.ticket.is_some())
        {
            return Err(RaftError::WrongCompletion);
        }
        Ok(())
    }
    pub fn admit_effect(&mut self, update: &LogUpdate, ticket: LogTicket) -> Result<(), RaftError> {
        if self.volatile
            || self
                .pending
                .as_ref()
                .is_none_or(|p| !p.next.matches_update(update))
        {
            return Err(RaftError::WrongCompletion);
        }
        self.admitted(ticket)
    }
    pub fn admitted(&mut self, ticket: LogTicket) -> Result<(), RaftError> {
        if self.fenced {
            return Err(RaftError::Fenced);
        }
        if self.volatile {
            return Err(RaftError::WrongCompletion);
        }
        let p = self.pending.as_mut().ok_or(RaftError::WrongCompletion)?;
        let next = p.next.metadata();
        if p.ticket.is_some()
            || ticket.binding != self.binding
            || ticket.batch <= self.last_batch
            || ticket.group != next.group
            || ticket.revision != next.revision
            || ticket.generation != next.generation
            || ticket.last_index != next.last_index
            || ticket.term != next.hard_state.term
        {
            return Err(RaftError::WrongCompletion);
        }
        self.last_batch = ticket.batch;
        p.ticket = Some(ticket);
        Ok(())
    }
    pub fn complete(&mut self, completion: &DurableLog) -> Result<Vec<Effect>, RaftError> {
        self.complete_borrowed(completion)
            .map(CompletionEffects::into_owned)
    }
    fn finish_completion(
        &mut self,
        completion: &DurableLog,
    ) -> Result<PreparedCompletion, RaftError> {
        if self.fenced {
            return Err(RaftError::Fenced);
        }
        if self.volatile {
            return Err(RaftError::WrongCompletion);
        }
        let p = self.pending.as_ref().ok_or(RaftError::WrongCompletion)?;
        let ticket = p.ticket.ok_or(RaftError::WrongCompletion)?;
        if !completion.tickets.contains(&ticket) {
            return Err(RaftError::WrongCompletion);
        }
        let mut following = VecDeque::new();
        let committed = self.install_pending_into(&mut following)?;
        Ok(PreparedCompletion {
            committed,
            following: following.into(),
        })
    }
    /// The exact single pending transition of this volatile core incarnation.
    /// Its revision advances on every transition and at most one is in
    /// flight; the incarnation is unique to this `recover_volatile` call.
    /// Following effects are appended to `out`; the committed interval, which
    /// precedes them, is returned.
    fn finish_volatile_into(
        &mut self,
        persist: VolatilePersist,
        out: &mut VecDeque<Effect>,
    ) -> Result<Option<Range<usize>>, RaftError> {
        if self.fenced {
            return Err(RaftError::Fenced);
        }
        if !self.volatile {
            return Err(RaftError::WrongCompletion);
        }
        if self.pending.as_ref().is_none_or(|p| {
            let next = p.next.metadata();
            p.ticket.is_some()
                || persist.incarnation != self.incarnation
                || next.revision != persist.revision
                || next.generation != persist.generation
        }) {
            return Err(RaftError::WrongCompletion);
        }
        self.install_pending_into(out)
    }
    /// Install the correlated pending transition; callers check correlation.
    /// Following effects are appended to `out` after its existing effects;
    /// the committed interval is returned. On error the caller discards what
    /// this call appended.
    fn install_pending_into(
        &mut self,
        out: &mut VecDeque<Effect>,
    ) -> Result<Option<Range<usize>>, RaftError> {
        let Some(p) = self.pending.take() else {
            return Err(RaftError::WrongCompletion);
        };
        let previous_commit = self.durable.commit_index;
        let configuration_changed = p
            .membership
            .as_ref()
            .is_some_and(|membership| membership.id() != self.membership.id());
        let preserves_checkpoint_prefix = p.next.preserves_checkpoint_prefix();
        if let Err(error) = self.durable.install(p.next, p.planned) {
            self.storage_failed();
            return Err(error.into());
        }
        if !preserves_checkpoint_prefix {
            self.checkpoint_lineage = CheckpointLineage::new();
        }
        if let Some(membership) = p.membership {
            self.membership = *membership;
        }
        if configuration_changed {
            self.configuration_changed()?;
        }
        // A retiring leader must publish its durable final commitment before
        // discarding replication state. These bounded announcements carry no
        // new entries and cannot confer ongoing leadership on the old voter.
        let retirement = if self.role == Role::Leader
            && !self.local_voter()
            && previous_commit < self.membership.last_configuration_index()
            && self.membership.last_configuration_index() <= self.durable.commit_index
        {
            self.retirement_announcements()?
        } else {
            Vec::new()
        };
        if !self.local_voter()
            && self.membership.last_configuration_index() <= self.durable.commit_index
        {
            self.role = Role::Follower;
            self.clear_reads();
            self.votes.clear();
            self.vote_context = None;
            self.clear_replication_requests();
            self.repair_requests.clear();
            self.progress.clear();
            self.next_index.clear();
        }
        let committed = if self.durable.commit_index > previous_commit
            && !matches!(p.after, After::SnapshotInstall(_))
        {
            Some(
                (previous_commit - self.durable.base_index()) as usize
                    ..(self.durable.commit_index - self.durable.base_index()) as usize,
            )
        } else {
            None
        };
        let mark = out.len();
        out.extend(retirement);
        self.after_durable(p.after, p.reply, out)?;
        if self.remember_granted_vote(out.range(mark..)) {
            self.reset_election()?;
        }
        Ok(committed)
    }
    fn after_durable(
        &mut self,
        after: After,
        reply: Option<Message>,
        effects: &mut VecDeque<Effect>,
    ) -> Result<(), RaftError> {
        match after {
            After::Campaign => {
                // Vote requests can escape only now. Persistence latency must
                // not consume the candidate's response window or leave an old
                // queued election eligible to supersede these requests.
                self.reset_election()?;
                self.votes.insert(self.node);
                if self.membership().is_satisfied(&self.votes) {
                    effects.extend(self.become_leader()?);
                } else {
                    effects.extend(if self.batched_joint_repair {
                        self.batched_joint_repair_messages()?
                    } else {
                        self.joint_repair_messages()?
                    });
                    let context = self.context()?;
                    self.vote_context = Some(context);
                    for peer in self.voting_peers() {
                        effects.push_back(Effect::Send(self.message(
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
                if let Some(mut reply) = reply {
                    reply.term = self.durable.hard_state.term;
                    effects.push_back(Effect::Send(reply));
                }
            }
            After::LeaderAppend => {
                if let Some(previous) = self.progress.insert(self.node, self.durable.last_index()) {
                    self.batch_floor = previous;
                }
                self.broadcast_append_progress_into(effects)?;
                self.maybe_commit_into(effects)?;
            }
            After::Commit if self.role == Role::Leader => {
                self.broadcast_commit_progress_into(effects)?
            }
            After::Commit => (),
            After::Compact => {
                self.clear_replication_requests();
                self.repair_requests.clear();
                if self.role == Role::Leader {
                    self.broadcast_into(effects)?;
                }
            }
            After::SnapshotInstall(reference) => {
                let reference = *reference;
                self.application_install =
                    Some((reference, reply.ok_or(RaftError::WrongCompletion)?));
                effects.push_back(Effect::SnapshotInstalled(reference));
            }
            After::CheckpointCompact(reference) => {
                let reference = *reference;
                self.clear_replication_requests();
                self.repair_requests.clear();
                self.checkpoint_reconcile = Some(reference);
                effects.push_back(Effect::CheckpointCompacted(reference));
            }
        }
        Ok(())
    }

    fn persist(
        &mut self,
        hard_state: HardState,
        commit_index: u64,
        suffix: Option<Suffix>,
        after: After,
        reply: Option<Message>,
    ) -> Result<Vec<Effect>, RaftError> {
        let mut effects = VecDeque::new();
        self.persist_into(hard_state, commit_index, suffix, after, reply, &mut effects)?;
        Ok(effects.into())
    }
    fn persist_into(
        &mut self,
        hard_state: HardState,
        commit_index: u64,
        suffix: Option<Suffix>,
        after: After,
        reply: Option<Message>,
        out: &mut VecDeque<Effect>,
    ) -> Result<(), RaftError> {
        if suffix.is_none() && hard_state == self.durable.hard_state {
            return self.persist_commit_into(commit_index, after, reply, out);
        }
        let update = LogUpdate {
            snapshot_membership: None,
            group: self.durable.bootstrap.group,
            expected_revision: self.durable.revision,
            hard_state,
            commit_index,
            suffix,
            snapshot: None,
        };
        self.persist_update_into(update, after, reply, out)
    }
    /// A round that only advances the commit index (or only the revision) plans
    /// a scalar advance. A durable store still receives the same update.
    fn persist_commit_into(
        &mut self,
        commit_index: u64,
        after: After,
        reply: Option<Message>,
        out: &mut VecDeque<Effect>,
    ) -> Result<(), RaftError> {
        let view = self.durable.view();
        let next =
            OwnedNextLog::plan_commit(&view, self.durable.summary(), commit_index, self.limits)?;
        let planned = view.stamp();
        let update = if self.volatile {
            None
        } else {
            Some(LogUpdate {
                snapshot_membership: None,
                group: self.durable.bootstrap.group,
                expected_revision: self.durable.revision,
                hard_state: self.durable.hard_state,
                commit_index,
                suffix: None,
                snapshot: None,
            })
        };
        self.stage_pending_into(next, planned, update, after, reply, out)
    }
    fn persist_update(
        &mut self,
        update: LogUpdate,
        after: After,
        reply: Option<Message>,
    ) -> Result<Vec<Effect>, RaftError> {
        let mut effects = VecDeque::new();
        self.persist_update_into(update, after, reply, &mut effects)?;
        Ok(effects.into())
    }
    fn persist_update_into(
        &mut self,
        update: LogUpdate,
        after: After,
        reply: Option<Message>,
        out: &mut VecDeque<Effect>,
    ) -> Result<(), RaftError> {
        // A volatile core is the only owner of its log: move the update into
        // the plan instead of keeping a copy for a store to validate again.
        if self.volatile {
            // The plan moves the update's batch without reallocating it. A
            // narrow owned batch keeps any spare capacity (the plan charges
            // it, the installed log charges the visible entries); a wide
            // batch was frozen, releasing spare capacity beyond 4 KiB.
            let view = self.durable.view();
            let next =
                OwnedNextLog::plan_owned(&view, self.durable.summary(), update, self.limits)?;
            let planned = view.stamp();
            self.stage_pending_into(next, planned, None, after, reply, out)
        } else {
            let view = self.durable.view();
            let next =
                OwnedNextLog::plan_copy(&view, self.durable.summary(), &update, self.limits)?;
            let planned = view.stamp();
            self.stage_pending_into(next, planned, Some(update), after, reply, out)
        }
    }
    /// Make `next` the single pending transition. `planned` is the stamp of
    /// the view `next` was planned from (`LogView::stamp`). `update` is the
    /// durable store's exact input; without one the core is volatile and
    /// emits a token.
    fn stage_pending_into(
        &mut self,
        next: OwnedNextLog,
        planned: Option<LogStamp>,
        update: Option<LogUpdate>,
        after: After,
        reply: Option<Message>,
        out: &mut VecDeque<Effect>,
    ) -> Result<(), RaftError> {
        let membership = next
            .full()
            .map(|full| full.membership().map(Box::new))
            .transpose()
            .map_err(|_| RaftError::InvalidRecovery)?;
        let metadata = next.metadata();
        let effect = match update {
            Some(update) => Effect::Persist(update),
            None => Effect::PersistVolatile(VolatilePersist {
                incarnation: self.incarnation,
                revision: metadata.revision,
                generation: metadata.generation,
            }),
        };
        let accepted_membership = membership.as_deref().unwrap_or(&self.membership);
        if self
            .learner_readiness
            .as_ref()
            .or_else(|| self.ready_learner.as_ref().map(|r| r.request()))
            .is_some_and(|r| {
                r.term != metadata.hard_state.term
                    || r.configuration != accepted_membership.id()
                    || r.index < metadata.commit_index
            })
        {
            self.cancel_learner_readiness();
        }
        if membership
            .as_ref()
            .is_some_and(|m| m.id() != self.membership().id())
        {
            self.clear_reads();
        }
        self.pending = Some(Pending {
            next,
            membership,
            ticket: None,
            after,
            reply,
            planned,
        });
        out.push_back(effect);
        Ok(())
    }
    /// Exact accepted node/store voting assignment, never durability evidence.
    /// Learners and replaced/removed physical replicas return false.
    pub fn local_voter(&self) -> bool {
        self.membership().voter_store(self.node) == Some(self.binding.identity)
    }
    fn peers(&self) -> Vec<NodeId> {
        self.membership()
            .replicas()
            .map(|(node, _)| node)
            .filter(|node| *node != self.node)
            .collect()
    }
    fn voting_peers(&self) -> Vec<NodeId> {
        self.peers()
            .into_iter()
            .filter(|node| self.membership().is_voter(*node))
            .collect()
    }
    /// Changing the accepted configuration invalidates every volatile quorum
    /// collection. Durable matching evidence is conservatively recollected in
    /// fresh request contexts, including unchanged identities and same-voter
    /// policy changes. This is not a durability or catch-up assertion.
    fn configuration_changed(&mut self) -> Result<(), RaftError> {
        self.clear_replication_authority();
        self.clear_reads();
        self.votes.clear();
        self.vote_context = None;
        self.clear_replication_requests();
        self.repair_requests.clear();
        self.progress.clear();
        self.next_index.clear();
        if self.role == Role::Candidate {
            self.role = Role::Follower;
        }
        if self.role == Role::Leader {
            self.initialize_replication()?;
        }
        Ok(())
    }
    fn initialize_replication(&mut self) -> Result<(), RaftError> {
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
        self.held.clear();
        self.ack_order = self.voting_peers();
        self.batch_floor = self.durable.last_index();
        Ok(())
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
        self.scoped_message(self.membership().id(), to, context, rpc)
    }
    fn scoped_message(
        &self,
        configuration: ConfigurationId,
        to: NodeId,
        context: RequestContext,
        rpc: Rpc,
    ) -> Message {
        Message {
            group: self.durable.bootstrap.group,
            configuration,
            from: self.node,
            sender: self.binding,
            to,
            term: self.durable.hard_state.term,
            context,
            rpc,
        }
    }
    fn reply(&self, request: &Message, rpc: Rpc) -> Message {
        self.scoped_message(request.configuration, request.from, request.context, rpc)
    }
    fn become_leader(&mut self) -> Result<Vec<Effect>, RaftError> {
        self.role = Role::Leader;
        self.progress.clear();
        self.next_index.clear();
        self.clear_replication_requests();
        self.repair_requests.clear();
        let next = self
            .durable
            .last_index()
            .checked_add(1)
            .ok_or(RaftError::Exhausted)?;
        self.initialize_replication()?;
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
                entries: vec![entry].into(),
            }),
            After::LeaderAppend,
            None,
        )
    }
    fn broadcast(&mut self) -> Result<Vec<Effect>, RaftError> {
        let mut effects = VecDeque::new();
        self.broadcast_into(&mut effects)?;
        Ok(effects.into())
    }
    fn broadcast_into(&mut self, out: &mut VecDeque<Effect>) -> Result<(), RaftError> {
        for peer in self.peers() {
            out.push_back(self.append_for(peer)?);
        }
        Ok(())
    }
    fn append_for(&mut self, peer: NodeId) -> Result<Effect, RaftError> {
        // Keep an outstanding request's range/context across heartbeat retries.
        // Replacing it every tick can starve all acknowledgements when the
        // network round trip exceeds the heartbeat interval. Term changes and
        // compaction clear requests; responses retire them before new work.
        self.held.remove(&peer);
        if let Some(sent) = self.requests.get(&peer).copied() {
            return self.retry_replication(peer, sent);
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
                    configuration: self.membership().id(),
                    start: 0,
                    end: reference.index,
                    snapshot: Some(reference),
                    commit_at_start: self.durable.commit_index,
                },
            );
            return Ok(Effect::SnapshotRequired {
                to: peer,
                context,
                reference,
            });
        }
        let offset = (next - self.durable.base_index() - 1) as usize;
        // One bounded RPC can carry several entries; outer wire limits are
        // negotiated by the later codec/transport assembly. The selection is
        // the original per-entry rule, evaluated from cached facts, and the
        // selected range shares the canonical backings.
        let entries = self.durable.entries.replication_range(
            offset,
            self.batch_limits.max_replication_entries,
            self.limits.max_batch_bytes,
        );
        if entries.is_empty() && next <= self.durable.last_index() {
            return Err(StorageError::Rejected("entry exceeds replication batch budget").into());
        }
        let end = entries.last().map_or(next - 1, |e| e.index);
        let context = self.context()?;
        self.requests.insert(
            peer,
            Replication {
                context,
                configuration: self.membership().id(),
                start: next,
                end,
                snapshot: None,
                commit_at_start: self.durable.commit_index,
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
        let mut effects = VecDeque::new();
        self.maybe_commit_into(&mut effects)?;
        Ok(effects.into())
    }
    fn maybe_commit_into(&mut self, out: &mut VecDeque<Effect>) -> Result<(), RaftError> {
        let frontier = self
            .membership()
            .frontier(&self.progress)
            .min(self.durable.last_index());
        if frontier > self.durable.commit_index
            && self.durable.term_at(frontier) == Some(self.durable.hard_state.term)
        {
            self.persist_into(
                self.durable.hard_state,
                frontier,
                None,
                After::Commit,
                None,
                out,
            )
        } else {
            Ok(())
        }
    }

    /// The final record and its committed predecessor authorize only this
    /// exact commit-only notification from a former voter. Compacted-away
    /// history cannot be used to recreate retired sender authority.
    fn retirement_predecessor(&self) -> Option<(u64, u64, Membership)> {
        let index = self.membership().last_configuration_index();
        let entry = self.durable.entry_at(index)?;
        let EntryPayload::Configuration(record) = &entry.payload else {
            return None;
        };
        if !matches!(record.change, crate::membership::ConfigurationChange::Final { id } if id == self.membership().id())
        {
            return None;
        }
        let previous = self.durable.membership_at(index.checked_sub(1)?).ok()?;
        let joint = previous.joint()?;
        if joint.id != record.expected || joint.index > self.durable.commit_index {
            return None;
        }
        Some((index, entry.term, previous))
    }
    fn retirement_announcements(&mut self) -> Result<Vec<Effect>, RaftError> {
        let Some((index, term, previous)) = self.retirement_predecessor() else {
            return Err(RaftError::InvalidRecovery);
        };
        if term != self.durable.hard_state.term
            || previous.voter_store(self.node) != Some(self.binding.identity)
        {
            return Err(RaftError::InvalidRecovery);
        }
        let context = self.context()?;
        Ok(self
            .peers()
            .into_iter()
            .map(|peer| {
                Effect::Send(self.message(
                    peer,
                    context,
                    Rpc::Append {
                        previous_index: index,
                        previous_term: term,
                        entries: EntryBatch::default(),
                        leader_commit: index,
                    },
                ))
            })
            .collect())
    }
    fn retirement_commit(&self, m: &Message) -> Option<u64> {
        // Ordinary active voters use the normal replication path. A retiring
        // sender cannot append data, raise a term, or claim another boundary.
        if self.membership().is_voter(m.from)
            || m.to != self.node
            || m.from == self.node
            || m.group != self.durable.bootstrap.group
            || m.configuration != self.membership().id()
            || m.term == 0
            || m.term != self.durable.hard_state.term
            || m.context.sequence == 0
            || m.context.origin != m.sender
        {
            return None;
        }
        let Rpc::Append {
            previous_index,
            previous_term,
            entries,
            leader_commit,
        } = &m.rpc
        else {
            return None;
        };
        if !entries.is_empty()
            || *previous_index != self.membership().last_configuration_index()
            || leader_commit != previous_index
            || self.durable.term_at(*previous_index) != Some(*previous_term)
        {
            return None;
        }
        let (index, term, previous) = self.retirement_predecessor()?;
        (m.term == term && previous.voter_store(m.from) == Some(m.sender.identity)).then_some(index)
    }
    fn authenticated_repair_source(&self, m: &Message) -> bool {
        matches!(
            m.rpc,
            Rpc::LearnerRepair { .. }
                | Rpc::LearnerRepairSnapshot { .. }
                | Rpc::CommittedLearnerRepairSnapshot { .. }
        ) && m.to == self.node
            && m.from != self.node
            && m.group == self.durable.bootstrap.group
            && m.context.origin == m.sender
            && m.context.sequence > 0
            && m.term > 0
            && self.membership().voter_store(m.from) == Some(m.sender.identity)
    }
    fn receive_into(
        &mut self,
        mut m: Message,
        out: &mut VecDeque<Effect>,
    ) -> Result<(), RaftError> {
        // A volatile log has static membership. Learner repair always carries
        // configuration state; ordinary replication is refused below.
        if self.volatile
            && matches!(
                m.rpc,
                Rpc::LearnerRepair { .. }
                    | Rpc::LearnerRepairSnapshot { .. }
                    | Rpc::CommittedLearnerRepairSnapshot { .. }
            )
        {
            return Err(RaftError::InvalidMessage);
        }
        // Repair can remain queued while its learner completes promotion.
        // Authenticate before discarding obsolete terms or refusing a repair
        // whose historical view no longer owns this replica's voting state.
        if self.authenticated_repair_source(&m) {
            if m.term < self.durable.hard_state.term {
                return Ok(());
            }
            if self.local_voter() && m.configuration < self.membership().id() {
                return Err(RaftError::WrongIdentity);
            }
        }
        if matches!(
            m.rpc,
            Rpc::LearnerRepairSnapshot { .. } | Rpc::CommittedLearnerRepairSnapshot { .. }
        ) {
            return out.append_result(self.receive_repair_snapshot(m));
        }
        if matches!(
            m.rpc,
            Rpc::LearnerRepair { .. } | Rpc::LearnerRepaired { .. }
        ) {
            return out.append_result(self.receive_learner_repair(m));
        }
        // Static/default assemblies retain the strict learner-repair exception.
        // Explicit member assemblies use ordinary authorized replication and
        // the same validated journal, log matching and durability dependencies.
        // A volatile core refuses all configuration ingress, before any state
        // change: static voter recovery could not accept the result.
        if (self.volatile || !self.configuration_replication)
            && (matches!(&m.rpc, Rpc::Append { entries, .. } if entries.has_configuration())
                || matches!(&m.rpc, Rpc::Snapshot { snapshot } if snapshot.metadata.membership.is_some()))
        {
            if self.volatile || !self.permits_joint_repair(&m) {
                return Err(RaftError::InvalidMessage);
            }
            self.trim_joint_repair(&mut m);
        }
        let contact = (self.membership().is_voter(m.from) || self.permitted_replication(&m))
            && m.term >= self.durable.hard_state.term
            && matches!(
                &m.rpc,
                Rpc::Append { .. } | Rpc::ReadProbe | Rpc::Snapshot { .. }
            );
        let leader = (m.sender, m.from, m.term);
        let mark = out.len();
        self.receive_inner_into(m, out)?;
        if contact {
            self.leader_contact = Some(leader);
        }
        let granted = self.remember_granted_vote(out.range(mark..));
        if contact || granted {
            self.reset_election()?;
        }
        Ok(())
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
                snapshot_membership: self
                    .checkpoint_membership(reference.index)
                    .map_err(|_| RaftError::InvalidRecovery)?,
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
    /// Only the exclusive validated owner can replace plain membership replay.
    pub(crate) fn checkpoint_membership(
        &self,
        index: u64,
    ) -> Result<Option<Box<crate::membership::Membership>>, crate::membership::MembershipError>
    {
        if self.durable.summary().is_plain() {
            if index < self.durable.base_index() || index > self.durable.last_index() {
                return Err(crate::membership::MembershipError::InvalidHistory);
            }
            Ok(None)
        } else {
            self.durable.checkpoint_membership(index)
        }
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
            .after = After::CheckpointCompact(Box::new(reference));
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
        if self.role == Role::Candidate && self.snapshot_joint_repair {
            return self.repair_snapshot_send(to, context, reference, snapshot);
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
            || snapshot.metadata.membership != self.durable.snapshot_membership
            || snapshot.application.len() > self.limits.max_snapshot_bytes
        {
            return Err(RaftError::WrongCompletion);
        }
        Ok(vec![Effect::Send(self.scoped_message(
            self.requests[&to].configuration,
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
        let repair = std::mem::take(&mut self.staged_snapshot_repair);
        let reply = self.reply(
            &message,
            if repair {
                Rpc::LearnerRepaired {
                    success: true,
                    matching_index: reference.index,
                    matching_term: reference.term,
                }
            } else {
                Rpc::SnapshotAck {
                    index: reference.index,
                }
            },
        );
        self.persist_update(
            LogUpdate {
                snapshot_membership: snapshot.metadata.membership.clone(),
                group: self.durable.bootstrap.group,
                expected_revision: self.durable.revision,
                hard_state: hard,
                commit_index: reference.index,
                suffix: None,
                snapshot: Some(reference),
            },
            After::SnapshotInstall(Box::new(reference)),
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
    persist_effect_borrowed(raft, store, update).map(CompletionEffects::into_owned)
}

#[cfg(test)]
#[path = "raft/append_prefix_tests.rs"]
mod append_prefix_tests;
#[cfg(test)]
mod checkpoint_tests;
#[cfg(test)]
mod commit_advance_tests;
#[cfg(test)]
mod completion_tests;
#[cfg(test)]
#[path = "raft/membership_tests.rs"]
mod membership_tests;
#[cfg(test)]
mod summarized_log_tests;
#[cfg(test)]
mod volatile_tests;

#[path = "raft/authority.rs"]
mod authority;
mod batched_repair;
#[path = "raft/connections.rs"]
mod connections;
mod joint_repair;
#[cfg(feature = "native")]
pub(crate) mod request_window;
mod snapshot_repair;
