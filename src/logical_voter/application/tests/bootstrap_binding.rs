// SPDX-License-Identifier: RPL-1.5
// Copyright (c) 2026 Karl Livesey
//! DR1 round 8 (Opus, round 7, P2-1): a logical voter's parent bootstrap is
//! bound by its first claim (bootstrap voter) or its enrollment (candidate).
//! Two replicas whose parent bootstraps differ declare equal requirements;
//! each test runs Opus's pair through a real leader and learner (raw `Raft`
//! cores, real replication) and the real learner verifier: both
//! `BootstrapPending` before the binding entry, then a receipt the leader
//! accepts for the matching replica, and `Err(InvalidCommand)` followed by
//! `NotCaughtUp` for the other. At 7045de08 both got receipts and then
//! disagreed on the same committed entry.
use super::{enrollment::fixture, *};
use crate::{
    contracts::StorageError,
    log::{apply_batch as apply_log, LogStore, LogTicket},
    membership::{Configuration, ConfigurationChange, ConfigurationRecord},
    raft::{
        persist_effect, verify_learner_readiness, Effect, Event, Message, Raft, ReadinessError,
        ReadinessRequirements,
    },
    snapshot::{
        SealedSnapshot, SnapshotIdentity, SnapshotLimits, SnapshotReceipt, SnapshotRetention,
        SnapshotStore, SnapshotTicket,
    },
};
use std::collections::VecDeque;

fn log_limits() -> LogLimits {
    LogLimits {
        max_command_bytes: LogicalLimits::default().max_command_bytes,
        ..LogLimits::default()
    }
}
fn binding(id: u128) -> StoreBinding {
    StoreBinding {
        identity: store(id),
        session: StoreSession::new(1).unwrap(),
    }
}
/// A memory log for one group: every batch is applied and durable at once.
struct MemoryLog {
    binding: StoreBinding,
    states: BTreeMap<GroupIdentity, GroupLog>,
    batch: u64,
}
impl MemoryLog {
    fn new(id: u128, mutations: &[LogMutation]) -> Self {
        let mut states = BTreeMap::new();
        for m in mutations {
            apply_log(&mut states, std::slice::from_ref(m), log_limits()).unwrap();
        }
        Self {
            binding: binding(id),
            states,
            batch: 0,
        }
    }
}
impl LogStore for MemoryLog {
    fn binding(&self) -> StoreBinding {
        self.binding
    }
    fn limits(&self) -> LogLimits {
        log_limits()
    }
    fn state(&self, group: GroupIdentity) -> Result<GroupLog, StorageError> {
        self.states
            .get(&group)
            .cloned()
            .ok_or(StorageError::Rejected("missing group"))
    }
    fn append_batch(
        &mut self,
        mutations: Vec<LogMutation>,
    ) -> Result<Vec<LogTicket>, StorageError> {
        apply_log(&mut self.states, &mutations, log_limits())?;
        self.batch += 1;
        Ok(mutations
            .iter()
            .map(|m| {
                let LogMutation::Update(update) = m else {
                    panic!("existing group")
                };
                let state = &self.states[&update.group];
                LogTicket {
                    binding: self.binding,
                    batch: self.batch,
                    group: update.group,
                    revision: state.revision,
                    generation: state.generation,
                    last_index: state.last_index(),
                    term: state.hard_state.term,
                }
            })
            .collect())
    }
    fn barrier(&mut self, dependencies: &[LogTicket]) -> Result<DurableLog, StorageError> {
        Ok(DurableLog {
            tickets: dependencies.to_vec(),
        })
    }
    fn fetch_range(
        &self,
        _: GroupIdentity,
        _: LogGeneration,
        _: u64,
        _: usize,
        _: usize,
    ) -> Result<Vec<LogEntry>, StorageError> {
        Err(StorageError::Rejected("unused range"))
    }
}
/// The learner's snapshot store: none published, so readiness only uses its
/// identity and limits.
struct NoSnapshots(StoreBinding);
impl SnapshotStore for NoSnapshots {
    fn identity(&self) -> SnapshotIdentity {
        SnapshotIdentity {
            store: self.0.identity,
            group: group(2),
        }
    }
    fn binding(&self) -> StoreBinding {
        self.0
    }
    fn limits(&self) -> SnapshotLimits {
        SnapshotLimits {
            max_application_bytes: LogicalLimits::default().max_child_checkpoint_bytes,
            max_metadata_bytes: 64 * 1024,
            max_chunk_bytes: 4096,
        }
    }
    fn begin(&mut self, _: SnapshotMetadata, _: usize) -> Result<SnapshotTicket, StorageError> {
        Err(StorageError::Rejected("unused"))
    }
    fn write_chunk(&mut self, _: SnapshotTicket, _: usize, _: &[u8]) -> Result<(), StorageError> {
        Err(StorageError::Rejected("unused"))
    }
    fn seal(&mut self, _: SnapshotTicket) -> Result<SealedSnapshot, StorageError> {
        Err(StorageError::Rejected("unused"))
    }
    fn publish(&mut self, _: SealedSnapshot) -> Result<SnapshotReceipt, StorageError> {
        Err(StorageError::Rejected("unused"))
    }
    fn abort(&mut self, _: SnapshotTicket) -> Result<(), StorageError> {
        Err(StorageError::Rejected("unused"))
    }
    fn load(&mut self) -> Result<Option<Snapshot>, StorageError> {
        Ok(None)
    }
}
impl SnapshotRetention for NoSnapshots {
    fn latest_reference(&self) -> Result<Option<SnapshotRef>, StorageError> {
        Ok(None)
    }
    fn pin_for_log(&mut self, _: SnapshotRef) -> Result<(), StorageError> {
        Err(StorageError::Rejected("unused"))
    }
    fn load_pinned(&mut self, _: SnapshotRef) -> Result<Snapshot, StorageError> {
        Err(StorageError::Rejected("unused"))
    }
    fn reconcile_log(&mut self, _: Option<SnapshotRef>) -> Result<(), StorageError> {
        Ok(())
    }
}

const LEARNER: u64 = 4;
fn learner_peer() -> PeerIdentity {
    PeerIdentity {
        node: NodeId::new(LEARNER).unwrap(),
        store: store(LEARNER.into()),
    }
}
/// The logical voters' child group: voter 2 (store 2, the representative's
/// physical replica), then a committed learner assignment for node 4.
struct Child {
    leader: Raft,
    leader_log: MemoryLog,
    learner: Raft,
    log: MemoryLog,
    snapshots: NoSnapshots,
}
impl Child {
    fn new() -> Self {
        let child = bootstrap(group(2), 2, store(2));
        let configuration = Configuration::new(
            ConfigurationId::new(2).unwrap(),
            child.policy.clone(),
            child.voter_stores.clone(),
            BTreeMap::from([(learner_peer().node, learner_peer().store)]),
        )
        .unwrap();
        let assignment = LogEntry {
            index: 1,
            term: 1,
            payload: EntryPayload::Configuration(Box::new(ConfigurationRecord {
                operation: OperationId::new(900).unwrap(),
                expected: child.configuration,
                change: ConfigurationChange::Learners(configuration),
            })),
        };
        let mut created = BTreeMap::new();
        apply_log(
            &mut created,
            &[LogMutation::Create(child.clone())],
            log_limits(),
        )
        .unwrap();
        let initial = [
            LogMutation::Create(child),
            LogMutation::Update(LogUpdate {
                group: group(2),
                expected_revision: created[&group(2)].revision,
                hard_state: HardState {
                    term: 1,
                    voted_for: None,
                },
                commit_index: 1,
                suffix: Some(Suffix {
                    from: 1,
                    entries: vec![assignment].into(),
                }),
                snapshot: None,
                snapshot_membership: None,
            }),
        ];
        let (leader_log, log) = (
            MemoryLog::new(2, &initial),
            MemoryLog::new(LEARNER.into(), &initial),
        );
        let leader = Raft::recover_member(
            NodeId::new(2).unwrap(),
            leader_log.binding,
            leader_log.state(group(2)).unwrap(),
            log_limits(),
        )
        .unwrap();
        let learner = Raft::recover_learner(
            learner_peer().node,
            log.binding,
            log.state(group(2)).unwrap(),
            log_limits(),
        )
        .unwrap();
        let mut child = Self {
            leader,
            leader_log,
            learner,
            log,
            snapshots: NoSnapshots(binding(LEARNER.into())),
        };
        // The sole voter elects itself; its Noop (index 2) reaches the learner.
        child.step_leader(Event::Campaign);
        assert!(child.leader.role() == crate::raft::Role::Leader);
        child.settle(2);
        child
    }
    fn drive(core: &mut Raft, log: &mut MemoryLog, effects: Vec<Effect>) -> Vec<Message> {
        let mut queue = VecDeque::from(effects);
        let mut sent = Vec::new();
        while let Some(effect) = queue.pop_front() {
            match effect {
                Effect::Persist(update) => queue.extend(persist_effect(core, log, update).unwrap()),
                Effect::Send(message) => sent.push(message),
                _ => {}
            }
        }
        sent
    }
    fn step_leader(&mut self, event: Event) {
        let effects = self.leader.step(event).unwrap();
        let sent = Self::drive(&mut self.leader, &mut self.leader_log, effects);
        self.exchange(sent);
    }
    /// Real replication: deliver every message until none remain.
    fn exchange(&mut self, mut to_learner: Vec<Message>) {
        for _ in 0..16 {
            let mut to_leader = Vec::new();
            for m in std::mem::take(&mut to_learner) {
                if m.to != learner_peer().node {
                    continue;
                }
                let effects = self.learner.step(Event::Receive(m)).unwrap();
                to_leader.extend(Self::drive(&mut self.learner, &mut self.log, effects));
            }
            for m in to_leader {
                let effects = self.leader.step(Event::Receive(m)).unwrap();
                to_learner.extend(Self::drive(&mut self.leader, &mut self.leader_log, effects));
            }
            if to_learner.is_empty() {
                return;
            }
        }
        panic!("replication did not settle");
    }
    /// Until the learner knows `index` is committed (heartbeats carry the
    /// leader's commit index).
    fn settle(&mut self, index: u64) {
        for _ in 0..8 {
            if self.learner.state().commit_index >= index {
                assert_eq!(self.leader.state().commit_index, index);
                return;
            }
            self.step_leader(Event::Heartbeat);
        }
        panic!("learner never learned commit {index}");
    }
    fn commit(&mut self, operation: OperationId, bytes: Vec<u8>) -> u64 {
        let index = self.leader.state().last_index() + 1;
        self.step_leader(Event::Propose { operation, bytes });
        self.settle(index);
        index
    }
    fn verify(
        &mut self,
        app: &LogicalApplication,
        request: crate::raft::LearnerReadinessRequest,
    ) -> Result<crate::raft::LearnerReadinessReceipt, ReadinessError> {
        verify_learner_readiness(
            &self.learner,
            app,
            &self.log,
            &mut self.snapshots,
            request,
            self.leader.storage_binding(),
        )
    }
}

/// The round-7 sequence for replicas `bound` and `other` with equal
/// requirements, where `entry` is encoded by `bound` (its binding).
fn binding_gap(mut bound: LogicalApplication, mut other: LogicalApplication, entry: &Command) {
    let declared: ReadinessRequirements = bound.deployment_requirements().unwrap();
    assert_eq!(other.deployment_requirements().unwrap(), declared);
    assert_eq!(other.validate_deployment_requirements(declared), Ok(()));
    let mut child = Child::new();
    for app in [&mut bound, &mut other] {
        StateMachine::apply_entries(app, child.learner.replay_committed()).unwrap();
        assert_eq!(app.applied_index(), 2);
        assert!(!app.deployment_bound());
    }
    // 7045de08 gave both a receipt here, accepted by the leader.
    let session = child.log.binding.session;
    let request = child
        .leader
        .begin_learner_readiness(learner_peer(), session, declared)
        .unwrap();
    for app in [&bound, &other] {
        assert_eq!(
            child.verify(app, request),
            Err(ReadinessError::BootstrapPending)
        );
    }
    child.leader.cancel_learner_readiness();

    // The mismatching replica refuses the binding entry at admission.
    let bytes = bound
        .encode_command(entry, bound.limits.max_command_bytes)
        .unwrap();
    assert!(bound
        .validate_proposal(entry.operation, &bytes, std::iter::empty())
        .is_ok());
    assert_eq!(
        other.validate_proposal(entry.operation, &bytes, std::iter::empty()),
        Err(ApplicationError::InvalidCommand)
    );
    let index = child.commit(entry.operation, bytes);
    assert_eq!(index, 3);
    let replay = child.learner.replay_committed().contiguous();
    let committed = &replay[2..];
    assert_eq!(committed.len(), 1);
    let receipts = bound.apply_batch(committed).unwrap();
    assert_eq!(receipts[0].outcome, LogicalOutcome::Claimed);
    let before = other.checkpoint(usize::MAX).unwrap();
    assert_eq!(
        other.apply_batch(committed).map(|_| ()),
        Err(ApplicationError::InvalidCommand)
    );
    assert_eq!(other.checkpoint(usize::MAX).unwrap(), before);
    assert_eq!((bound.applied_index(), other.applied_index()), (3, 2));
    assert!(bound.deployment_bound());
    assert!(!other.deployment_bound());

    let request = child
        .leader
        .begin_learner_readiness(learner_peer(), session, declared)
        .unwrap();
    assert_eq!(request.index, 3);
    let receipt = child.verify(&bound, request).unwrap();
    let ready = child
        .leader
        .accept_learner_readiness(receipt, child.log.binding)
        .unwrap();
    assert_eq!(
        child
            .leader
            .check_learner_readiness(&ready, child.log.binding),
        Ok(())
    );
    // The replica that failed the entry never covers the prefix.
    assert_eq!(
        child.verify(&other, request),
        Err(ReadinessError::NotCaughtUp)
    );
    // Nor can it take the bound replica's state: restore compares the parent
    // bootstrap through the configuration.
    let checkpoint = bound.checkpoint(usize::MAX).unwrap();
    assert_eq!(
        other.restore_checkpoint(other.schema_version(), 3, &checkpoint),
        Err(ApplicationError::InvalidCheckpoint)
    );
    assert_eq!(other.checkpoint(usize::MAX).unwrap(), before);
}

fn first_claim() -> Command {
    command(
        1,
        1,
        CommandBody::Claim {
            expected_epoch: None,
        },
    )
}
fn voter_with_parent(parent: Bootstrap) -> LogicalApplication {
    LogicalApplication::new(
        LogicalStoreMapping::new(group(1), NodeId::new(1).unwrap(), store(1), group(2)).unwrap(),
        bootstrap(group(2), 2, store(2)),
        parent,
        LogicalLimits::default(),
        NativeLogCodec,
        NativeSnapshotCodec,
        NativeWireCodec::with_membership(WireLimits::default()).unwrap(),
    )
    .unwrap()
}

#[test]
fn rr7_bootstrap_voter_parent_bootstrap_receipt_gap() {
    // Opus, round 7: parent configuration 1 vs 7, otherwise identical. At
    // 7045de08 both applied `Claimed` with different parent images.
    let one = app();
    let seven = voter_with_parent(Bootstrap {
        configuration: ConfigurationId::new(7).unwrap(),
        ..one.parent_bootstrap.clone()
    });
    binding_gap(one, seven, &first_claim());
    // Symmetric: the claim binds whichever bootstrap encoded it.
    let one = app();
    let seven = voter_with_parent(Bootstrap {
        configuration: ConfigurationId::new(7).unwrap(),
        ..one.parent_bootstrap.clone()
    });
    binding_gap(seven, one, &first_claim());
}

#[test]
fn rr7_candidate_parent_bootstrap_receipt_gap() {
    // Opus, round 7: candidates whose parent bootstraps differ only in voter
    // 1's store. At 7045de08 the same committed `EnrollmentClaim` gave
    // `Claimed` on one and `Refused(SnapshotConflict)` on the other.
    let a = fixture::candidate();
    let mut parent = a.parent_bootstrap.clone();
    parent
        .voter_stores
        .insert(NodeId::new(1).unwrap(), store(5));
    let b = fixture::candidate_with_parent(LogicalLimits::default(), parent).unwrap();
    let entry = fixture::enrollment(fixture::import(&a), 11);
    binding_gap(a, b, &entry);
}

#[test]
fn only_a_first_claim_carries_the_binding_and_it_must_fit_the_command_bound() {
    let a = app();
    let first = a
        .encode_command(&first_claim(), a.limits.max_command_bytes)
        .unwrap();
    let later = a
        .encode_command(
            &command(
                2,
                2,
                CommandBody::Claim {
                    expected_epoch: RepresentativeEpoch::new(1),
                },
            ),
            a.limits.max_command_bytes,
        )
        .unwrap();
    // Same fixed fields; the first claim adds the length and exact bytes.
    let binding = a.parent_binding().to_vec();
    assert_eq!(first.len(), later.len() + 8 + binding.len());
    assert!(first.ends_with(&binding));
    // A truncated or extended binding is refused, as is a later claim that
    // carries one.
    for bytes in [
        first[..first.len() - 1].to_vec(),
        [first.as_slice(), &[0]].concat(),
        [
            later.as_slice(),
            &(binding.len() as u64).to_le_bytes(),
            &binding,
        ]
        .concat(),
    ] {
        assert_eq!(
            a.decode_command(&bytes),
            Err(ApplicationError::InvalidCommand)
        );
    }
    // A parent bootstrap whose first claim exceeds the command bound is
    // refused at construction, for every replica alike.
    let voters = 1..=48u64;
    let wide = Bootstrap {
        policy: Policy::new(
            Tree::Majority(
                voters
                    .clone()
                    .map(|n| Tree::Voter(NodeId::new(n).unwrap()))
                    .collect(),
            ),
            PolicyLimits::default(),
        )
        .unwrap(),
        voter_stores: voters
            .map(|n| (NodeId::new(n).unwrap(), store(n.into())))
            .collect(),
        ..a.parent_bootstrap.clone()
    };
    let construct = |max_command_bytes| {
        LogicalApplication::new(
            a.mapping,
            a.child_bootstrap.clone(),
            wide.clone(),
            LogicalLimits {
                max_command_bytes,
                max_capsule_bytes: 512,
                ..LogicalLimits::default()
            },
            NativeLogCodec,
            NativeSnapshotCodec,
            NativeWireCodec::with_membership(WireLimits::default()).unwrap(),
        )
    };
    let fits = construct(64 * 1024).unwrap();
    let needed = fits
        .encode_command(&first_claim(), usize::MAX)
        .unwrap()
        .len();
    assert!(needed > 512);
    assert!(construct(needed).is_ok());
    assert_eq!(
        construct(needed - 1).map(|_| ()),
        Err(ApplicationError::InvalidCommand)
    );
}
