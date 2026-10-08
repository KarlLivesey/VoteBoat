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
//! Fresh host-driven learner readiness over the selected storage/application seams.
use super::*;
use crate::{
    application::{ApplicationError, CheckpointStateMachine},
    snapshot::{SnapshotIdentity, SnapshotRetention},
};

/// Required capabilities for this group's promotion. Placement/failure-domain
/// authorization belongs to the host's validated administration plan.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub struct ReadinessRequirements {
    pub application_schema: u64,
    pub command_bytes: usize,
    pub snapshot_bytes: usize,
}
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub struct LearnerReadinessRequest {
    pub group: GroupIdentity,
    pub configuration: ConfigurationId,
    pub leader: NodeId,
    pub context: RequestContext,
    pub term: u64,
    pub learner: PeerIdentity,
    pub session: StoreSession,
    /// Required contiguous committed prefix, not a maximum received index.
    pub index: u64,
    pub index_term: u64,
    pub requirements: ReadinessRequirements,
}
/// Host-transportable assertion under the authenticated non-Byzantine model.
/// Native/host learners produce this through verify_learner_readiness. A host
/// wire adapter may reconstruct it only from an authenticated peer response;
/// constructible data is not a cryptographic remote durability certificate.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct LearnerReadinessReceipt {
    pub request: LearnerReadinessRequest,
    pub binding: StoreBinding,
}
impl LearnerReadinessReceipt {
    pub fn request(&self) -> &LearnerReadinessRequest {
        &self.request
    }
    pub fn binding(&self) -> StoreBinding {
        self.binding
    }
}
/// Core-checked readiness for the captured prefix and exact live peer session.
/// Not a ballot, quorum acknowledgement, activation or reusable serving lease.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct ReadyLearner {
    receipt: LearnerReadinessReceipt,
}
impl ReadyLearner {
    pub fn request(&self) -> &LearnerReadinessRequest {
        self.receipt.request()
    }
    pub fn binding(&self) -> StoreBinding {
        self.receipt.binding()
    }
}
#[derive(Clone, Debug, Eq, PartialEq)]
pub enum ReadinessError {
    Consensus(RaftError),
    Storage(StorageError),
    Application(ApplicationError),
    InvalidRequirements,
    WrongBinding,
    Stale,
    NotCaughtUp,
    Capability,
}
impl From<RaftError> for ReadinessError {
    fn from(e: RaftError) -> Self {
        Self::Consensus(e)
    }
}
impl From<StorageError> for ReadinessError {
    fn from(e: StorageError) -> Self {
        Self::Storage(e)
    }
}
impl From<ApplicationError> for ReadinessError {
    fn from(e: ApplicationError) -> Self {
        Self::Application(e)
    }
}
impl Raft {
    fn readiness_idle(&self) -> Result<(), ReadinessError> {
        if self.fenced {
            return Err(RaftError::Fenced.into());
        }
        if self.has_persistence_dependency() {
            return Err(RaftError::Busy.into());
        }
        if self.membership.joint().is_some()
            || self.membership.last_configuration_index() > self.durable.commit_index
        {
            return Err(ReadinessError::Stale);
        }
        Ok(())
    }
    /// One pending request per group; rejection leaves the previous request intact.
    /// The host must use an authenticated current peer session and serialize this
    /// call with the core. No network or provider work is performed here.
    pub fn begin_learner_readiness(
        &mut self,
        learner: PeerIdentity,
        session: StoreSession,
        requirements: ReadinessRequirements,
    ) -> Result<LearnerReadinessRequest, ReadinessError> {
        self.readiness_idle()?;
        if self.readiness_check.is_some() {
            return Err(RaftError::Busy.into());
        }
        if self.role != Role::Leader || !self.local_voter() {
            return Err(RaftError::NotLeader.into());
        }
        if self.learner_readiness.is_some() {
            return Err(RaftError::Busy.into());
        }
        if requirements.application_schema == 0
            || requirements.command_bytes == 0
            || requirements.snapshot_bytes == 0
        {
            return Err(ReadinessError::InvalidRequirements);
        }
        if self.membership.stable().learners().get(&learner.node) != Some(&learner.store) {
            return Err(ReadinessError::WrongBinding);
        }
        let index = self.durable.commit_index;
        let index_term = self
            .durable
            .term_at(index)
            .ok_or(ReadinessError::NotCaughtUp)?;
        if index == 0 || index_term != self.durable.hard_state.term {
            return Err(ReadinessError::NotCaughtUp);
        }
        let request = LearnerReadinessRequest {
            group: self.durable.bootstrap.group,
            configuration: self.membership.id(),
            leader: self.node,
            context: self.context()?,
            term: self.durable.hard_state.term,
            learner,
            session,
            index,
            index_term,
            requirements,
        };
        self.learner_readiness = Some(request);
        self.ready_learner = None;
        Ok(request)
    }
    pub fn cancel_learner_readiness(&mut self) {
        self.learner_readiness = None;
        self.ready_learner = None;
    }
    /// Volatile native-exchange result. Promotion must still recheck its scope
    /// and current authenticated peer binding at execution.
    pub fn ready_learner(&self) -> Option<&ReadyLearner> {
        self.ready_learner.as_ref()
    }
    pub(crate) fn readiness_check_matches(&self, message: &Message) -> bool {
        self.readiness_check.as_ref() == Some(message)
    }
    pub(crate) fn receive_readiness(&mut self, message: Message) -> Result<Vec<Effect>, RaftError> {
        let request = match &message.rpc {
            Rpc::LearnerReadinessRequest(request) | Rpc::LearnerReadinessReply { request, .. } => {
                **request
            }
            _ => return Err(RaftError::InvalidMessage),
        };
        if message.group != request.group
            || message.configuration != request.configuration
            || message.term != request.term
            || message.context != request.context
            || message.to != self.node
            || message.from == self.node
        {
            return Err(RaftError::WrongIdentity);
        }
        match message.rpc {
            Rpc::LearnerReadinessRequest(_) => {
                if message.from != request.leader || message.sender != request.context.origin {
                    return Err(RaftError::WrongIdentity);
                }
                // Readiness cannot advance a term, reset election timers or
                // authorize a peer from a different configuration.
                if self
                    .check_readiness_request(request, message.sender)
                    .is_err()
                {
                    return Ok(Vec::new());
                }
                if request.index > self.durable.commit_index
                    || self.durable.term_at(request.index) != Some(request.index_term)
                {
                    return Ok(vec![Effect::Send(self.reply(
                        &message,
                        Rpc::LearnerReadinessReply {
                            request: Box::new(request),
                            ready: false,
                        },
                    ))]);
                }
                self.readiness_check = Some(message.clone());
                Ok(vec![Effect::VerifyLearnerReadiness(message)])
            }
            Rpc::LearnerReadinessReply { ready, .. } => {
                if message.from != request.learner.node
                    || message.to != request.leader
                    || message.sender.identity != request.learner.store
                    || message.sender.session != request.session
                {
                    return Err(RaftError::WrongIdentity);
                }
                let receipt = LearnerReadinessReceipt {
                    request,
                    binding: message.sender,
                };
                if self.learner_readiness != Some(request)
                    || self.check_ready_scope(&receipt, message.sender).is_err()
                {
                    return Ok(Vec::new());
                }
                if ready {
                    self.ready_learner = Some(
                        self.accept_learner_readiness(receipt, message.sender)
                            .map_err(|_| RaftError::InvalidMessage)?,
                    );
                } else {
                    self.cancel_learner_readiness();
                }
                Ok(Vec::new())
            }
            _ => unreachable!(),
        }
    }
    fn check_readiness_request(
        &self,
        request: LearnerReadinessRequest,
        authenticated_leader: StoreBinding,
    ) -> Result<(), ReadinessError> {
        self.readiness_idle()?;
        let state = self.state();
        if authenticated_leader != request.context.origin
            || self.membership.voter_store(request.leader) != Some(authenticated_leader.identity)
            || request.learner.node != self.node
            || request.learner.store != self.binding.identity
            || request.session != self.binding.session
        {
            return Err(ReadinessError::WrongBinding);
        }
        if request.context.sequence == 0
            || request.index == 0
            || request.index_term == 0
            || request.group != state.bootstrap.group
            || request.configuration != self.membership.id()
            || request.term != state.hard_state.term
            || self.role != Role::Follower
            || self.membership.stable().learners().get(&self.node) != Some(&self.binding.identity)
        {
            return Err(ReadinessError::Stale);
        }
        Ok(())
    }

    /// Complete only the staged read-only verification. No new durability or
    /// application state is created; the worker checks the existing pinned anchor.
    pub(crate) fn finish_readiness_check<A: CheckpointStateMachine>(
        &mut self,
        message: &Message,
        application: &A,
        snapshot: Option<crate::snapshot::Snapshot>,
        limits: crate::snapshot::SnapshotLimits,
    ) -> Result<Vec<Effect>, ReadinessError> {
        if !self.readiness_check_matches(message) {
            return Err(RaftError::WrongCompletion.into());
        }
        let Rpc::LearnerReadinessRequest(request) = &message.rpc else {
            return Err(RaftError::WrongCompletion.into());
        };
        let request = **request;
        // The owner remained suspended until this exact completion.
        self.check_readiness_request(request, message.sender)?;
        let ready = match verify_readiness_evidence(
            self,
            application,
            request,
            limits,
            snapshot.as_ref(),
        ) {
            Ok(()) => true,
            Err(
                ReadinessError::Capability
                | ReadinessError::NotCaughtUp
                | ReadinessError::Application(_)
                | ReadinessError::InvalidRequirements,
            ) => false,
            Err(e) => return Err(e),
        };
        self.readiness_check = None;
        Ok(vec![Effect::Send(self.reply(
            message,
            Rpc::LearnerReadinessReply {
                request: Box::new(request),
                ready,
            },
        ))])
    }
    fn check_ready_scope(
        &self,
        receipt: &LearnerReadinessReceipt,
        authenticated: StoreBinding,
    ) -> Result<(), ReadinessError> {
        self.readiness_idle()?;
        if self.role != Role::Leader || !self.local_voter() {
            return Err(RaftError::NotLeader.into());
        }
        let r = &receipt.request;
        if receipt.binding != authenticated
            || authenticated.identity != r.learner.store
            || authenticated.session != r.session
        {
            return Err(ReadinessError::WrongBinding);
        }
        if r.group != self.durable.bootstrap.group
            || r.configuration != self.membership.id()
            || r.leader != self.node
            || r.context.origin != self.binding
            || r.term != self.durable.hard_state.term
            || self.membership.stable().learners().get(&r.learner.node) != Some(&r.learner.store)
            || r.index < self.durable.commit_index
            || self.durable.term_at(r.index) != Some(r.index_term)
        {
            return Err(ReadinessError::Stale);
        }
        Ok(())
    }
    /// A response is consumed only after exact request and current-session checks.
    /// Invalid responses leave the pending request available for a valid retry.
    pub fn accept_learner_readiness(
        &mut self,
        receipt: LearnerReadinessReceipt,
        authenticated: StoreBinding,
    ) -> Result<ReadyLearner, ReadinessError> {
        self.check_ready_scope(&receipt, authenticated)?;
        if self.learner_readiness.as_ref() != Some(&receipt.request) {
            return Err(ReadinessError::Stale);
        }
        self.learner_readiness = None;
        Ok(ReadyLearner { receipt })
    }
    /// Recheck immediately before promotion, including the current authenticated
    /// learner session. Commitment advancing beyond the captured prefix requires
    /// a new readiness round; caller-held tokens do not reserve membership.
    pub fn check_learner_readiness(
        &self,
        ready: &ReadyLearner,
        authenticated: StoreBinding,
    ) -> Result<(), ReadinessError> {
        self.check_ready_scope(&ready.receipt, authenticated)
    }
}

/// Run on the serialized learner owner. This can load a pinned snapshot and
/// validate a bounded checkpoint, so hosts schedule it away from latency-sensitive
/// consensus loops. Native and host providers use this same contract. The host
/// binds the application to this group and authenticates both request and reply.
/// Provider assertions are trusted, as with LogStore durability completions.
pub fn verify_learner_readiness<A: CheckpointStateMachine, L: LogStore, S: SnapshotRetention>(
    raft: &Raft,
    application: &A,
    log: &L,
    snapshots: &mut S,
    request: LearnerReadinessRequest,
    authenticated_leader: StoreBinding,
) -> Result<LearnerReadinessReceipt, ReadinessError> {
    raft.check_readiness_request(request, authenticated_leader)?;
    let state = raft.state();
    if log.binding() != raft.binding
        || snapshots.identity()
            != (SnapshotIdentity {
                store: raft.binding.identity,
                group: request.group,
            })
        || snapshots.binding().identity != raft.binding.identity
    {
        return Err(ReadinessError::WrongBinding);
    }
    check_readiness_progress(raft, application, request)?;
    log.limits().validate()?;
    if log.limits().max_command_bytes < request.requirements.command_bytes
        || log.limits().max_snapshot_bytes < request.requirements.snapshot_bytes
    {
        return Err(ReadinessError::Capability);
    }
    if log.state(request.group)? != *state {
        return Err(ReadinessError::NotCaughtUp);
    }
    let snapshot = state
        .snapshot
        .map(|r| snapshots.load_pinned(r))
        .transpose()?;
    verify_readiness_evidence(
        raft,
        application,
        request,
        snapshots.limits(),
        snapshot.as_ref(),
    )?;
    Ok(LearnerReadinessReceipt {
        request,
        binding: raft.binding,
    })
}

fn check_readiness_progress<A: CheckpointStateMachine>(
    raft: &Raft,
    application: &A,
    request: LearnerReadinessRequest,
) -> Result<(), ReadinessError> {
    let state = raft.state();
    if request.index > state.commit_index
        || state.term_at(request.index) != Some(request.index_term)
        || application.applied_index() < request.index
        || application.applied_index() > state.commit_index
    {
        return Err(ReadinessError::NotCaughtUp);
    }
    Ok(())
}

fn verify_readiness_evidence<A: CheckpointStateMachine>(
    raft: &Raft,
    application: &A,
    request: LearnerReadinessRequest,
    snapshot_limits: crate::snapshot::SnapshotLimits,
    snapshot: Option<&crate::snapshot::Snapshot>,
) -> Result<(), ReadinessError> {
    let state = raft.state();
    let required = request.requirements;
    let snapshot_limits = snapshot_limits.validate()?;
    check_readiness_progress(raft, application, request)?;
    if required.application_schema == 0
        || required.command_bytes == 0
        || required.snapshot_bytes == 0
    {
        return Err(ReadinessError::InvalidRequirements);
    }
    if application.schema_version() != required.application_schema
        || raft.limits.max_command_bytes < required.command_bytes
        || raft.limits.max_snapshot_bytes < required.snapshot_bytes
        || snapshot_limits.max_application_bytes < required.snapshot_bytes
    {
        return Err(ReadinessError::Capability);
    }
    if state.snapshot.is_some() != snapshot.is_some() {
        return Err(ReadinessError::Storage(StorageError::Rejected(
            "readiness anchor",
        )));
    }
    if let Some(reference) = state.snapshot {
        let snapshot = snapshot.ok_or(ReadinessError::NotCaughtUp)?;
        if !reference.matches(snapshot)
            || snapshot.metadata.bootstrap != state.bootstrap
            || snapshot.metadata.membership
                != state
                    .checkpoint_membership(reference.index)
                    .map_err(|_| ReadinessError::Stale)?
        {
            return Err(ReadinessError::Storage(StorageError::Rejected(
                "readiness snapshot",
            )));
        }
        let mut restored = application.clone();
        restored.restore_checkpoint(
            reference.application_schema,
            reference.index,
            &snapshot.application,
        )?;
        if restored.applied_index() != reference.index {
            return Err(ReadinessError::NotCaughtUp);
        }
    }
    // Exercise the selected application's checkpoint compatibility, not just a
    // caller-supplied version number. No new snapshot is published or pinned.
    let index = application.applied_index();
    let bytes = application.checkpoint(snapshot_limits.max_application_bytes)?;
    if bytes.is_empty() || bytes.capacity() > snapshot_limits.max_application_bytes {
        return Err(ReadinessError::Capability);
    }
    let mut restored = application.clone();
    restored.restore_checkpoint(required.application_schema, index, &bytes)?;
    if restored.applied_index() != index {
        return Err(ReadinessError::NotCaughtUp);
    }
    Ok(())
}
