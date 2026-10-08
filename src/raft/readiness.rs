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
        if self.has_pending_dependency() {
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
        Ok(request)
    }
    pub fn cancel_learner_readiness(&mut self) {
        self.learner_readiness = None;
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
    raft.readiness_idle()?;
    let state = raft.state();
    if authenticated_leader != request.context.origin
        || raft.membership.voter_store(request.leader) != Some(authenticated_leader.identity)
        || request.learner.node != raft.node
        || request.learner.store != raft.binding.identity
        || request.session != raft.binding.session
        || log.binding() != raft.binding
        || snapshots.identity()
            != (SnapshotIdentity {
                store: raft.binding.identity,
                group: request.group,
            })
        || snapshots.binding().identity != raft.binding.identity
    {
        return Err(ReadinessError::WrongBinding);
    }
    if request.context.sequence == 0
        || request.index == 0
        || request.index_term == 0
        || request.group != state.bootstrap.group
        || request.configuration != raft.membership.id()
        || request.term != state.hard_state.term
        || raft.role != Role::Follower
        || raft.membership.stable().learners().get(&raft.node) != Some(&raft.binding.identity)
    {
        return Err(ReadinessError::Stale);
    }
    if request.index > state.commit_index
        || state.term_at(request.index) != Some(request.index_term)
        || application.applied_index() < request.index
        || application.applied_index() > state.commit_index
    {
        return Err(ReadinessError::NotCaughtUp);
    }
    let required = request.requirements;
    let snapshot_limits = snapshots.limits().validate()?;
    log.limits().validate()?;
    if required.application_schema == 0
        || required.command_bytes == 0
        || required.snapshot_bytes == 0
    {
        return Err(ReadinessError::InvalidRequirements);
    }
    if application.schema_version() != required.application_schema
        || log.limits().max_command_bytes < required.command_bytes
        || log.limits().max_snapshot_bytes < required.snapshot_bytes
        || snapshot_limits.max_application_bytes < required.snapshot_bytes
    {
        return Err(ReadinessError::Capability);
    }
    // Core durable state must agree with the selected provider, rather than a
    // second log binding or an uncompleted provider suffix.
    if log.state(request.group)? != *state {
        return Err(ReadinessError::NotCaughtUp);
    }
    if let Some(reference) = state.snapshot {
        let snapshot = snapshots.load_pinned(reference)?;
        if !reference.matches(&snapshot)
            || snapshot.metadata.bootstrap != state.bootstrap
            || snapshot.metadata.membership
                != state
                    .checkpoint_membership(reference.index)
                    .map_err(|_| ReadinessError::Stale)?
        {
            return Err(ReadinessError::NotCaughtUp);
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
    if bytes.is_empty() || bytes.len() > snapshot_limits.max_application_bytes {
        return Err(ReadinessError::Capability);
    }
    let mut restored = application.clone();
    restored.restore_checkpoint(required.application_schema, index, &bytes)?;
    if restored.applied_index() != index {
        return Err(ReadinessError::NotCaughtUp);
    }
    Ok(LearnerReadinessReceipt {
        request,
        binding: raft.binding,
    })
}
