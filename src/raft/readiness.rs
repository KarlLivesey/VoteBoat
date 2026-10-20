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
///
/// Readiness compares `application_schema`, `identity` and `inner_profile`
/// exactly: together they name every layer's protocol family, schema and
/// decision-affecting configuration. `command_bytes` and `snapshot_bytes` are
/// storage reservations, accepted when the declaration is at least the actual
/// bound.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub struct ReadinessRequirements {
    pub application_schema: u64,
    /// This application's own protocol family and decision-affecting
    /// configuration (the outermost layer).
    pub identity: LayerIdentity,
    /// The applications inside this one, if it wraps any. Readiness compares it
    /// with the schema, so a wrapper whose own schema is a fixed protocol
    /// constant still refuses a replica whose inner application differs.
    pub inner_profile: InnerProfile,
    pub command_bytes: usize,
    pub snapshot_bytes: usize,
}

/// Protocol family of one application layer. Schema numbers are chosen per
/// family (in-tree families reuse 1, 2, ...), so readiness compares the family
/// with the schema: two layers match only if both are equal.
///
/// The registry of in-tree families is the list of constants below; values
/// `1..FIRST_EXTERNAL` are reserved for it. Third-party applications use
/// [`ApplicationFamily::external`], or `UNREGISTERED` to be identified by their
/// schema and configuration alone (the provider then keeps its schemas
/// distinct from every other family it may be composed with).
#[derive(Clone, Copy, Debug, Eq, Hash, Ord, PartialEq, PartialOrd)]
pub struct ApplicationFamily(u16);
impl ApplicationFamily {
    /// Identified by schema and configuration alone.
    pub const UNREGISTERED: Self = Self(0);
    /// `application::Counter`.
    pub const COUNTER: Self = Self(1);
    /// `maintenance::Maintenance`.
    pub const MAINTENANCE: Self = Self(2);
    /// `logical_voter::LogicalActivationApplication`.
    pub const LOGICAL_ACTIVATION: Self = Self(3);
    /// `routed::RoutedApplication` (all of its schemas).
    pub const ROUTED: Self = Self(4);
    /// `transfer_source::TransferSource`.
    pub const TRANSFER_SOURCE: Self = Self(5);
    /// `scoped_source::ScopedTransferSource`.
    pub const SCOPED_TRANSFER_SOURCE: Self = Self(6);
    /// `transfer_target::TransferTarget`.
    pub const TRANSFER_TARGET: Self = Self(7);
    /// `namespace_creation::CreatedNamespace`.
    pub const CREATED_NAMESPACE: Self = Self(8);
    /// `retirement::RetirementGuard`.
    pub const RETIREMENT_GUARD: Self = Self(9);
    /// `bucket_counter::BucketCounter`.
    pub const BUCKET_COUNTER: Self = Self(10);
    /// `directory::Directory`.
    pub const DIRECTORY: Self = Self(11);
    /// `metadata_transfer::MetadataAuthoritySource`.
    pub const METADATA_SOURCE: Self = Self(12);
    /// `metadata_transfer::MetadataAuthorityTarget`.
    pub const METADATA_TARGET: Self = Self(13);
    /// `metadata_transfer::MetadataServingTarget`.
    pub const METADATA_SERVING: Self = Self(14);
    /// `metadata_transfer::MetadataPublishingSource`.
    pub const METADATA_PUBLISHING: Self = Self(15);
    /// `logical_voter::LogicalVoterApplication`.
    pub const LOGICAL_VOTER: Self = Self(16);
    /// `transaction::TransferApplication`.
    pub const TRANSFER_TRANSACTION: Self = Self(17);
    /// `program_transaction::ProgramApplication`.
    pub const PROGRAM_TRANSACTION: Self = Self(18);
    /// First value available to third-party families.
    pub const FIRST_EXTERNAL: u16 = 0x8000;
    /// A third-party family; `None` for a value reserved for in-tree families.
    pub const fn external(value: u16) -> Option<Self> {
        if value < Self::FIRST_EXTERNAL {
            None
        } else {
            Some(Self(value))
        }
    }
    /// Decoded from a readiness frame; any value is representable.
    pub const fn from_wire(value: u16) -> Self {
        Self(value)
    }
    pub const fn get(self) -> u16 {
        self.0
    }
}

/// Configuration words compared per layer.
pub const LAYER_CONFIGURATION_WORDS: usize = 4;

/// One layer's protocol family and the configuration that changes how it
/// decides committed entries or encodes its checkpoint (limits, capacities,
/// enabled modes). Compared exactly; storage reservations are not part of it.
/// Each family documents its word layout where it declares requirements.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub struct LayerIdentity {
    pub family: ApplicationFamily,
    pub configuration: [u64; LAYER_CONFIGURATION_WORDS],
}
impl LayerIdentity {
    /// No registered family and no configuration words.
    pub const UNREGISTERED: Self = Self::new(ApplicationFamily::UNREGISTERED, [0; 4]);
    pub const fn new(
        family: ApplicationFamily,
        configuration: [u64; LAYER_CONFIGURATION_WORDS],
    ) -> Self {
        Self {
            family,
            configuration,
        }
    }
    /// A family whose configuration is fully named by its schema.
    pub const fn family(family: ApplicationFamily) -> Self {
        Self::new(family, [0; LAYER_CONFIGURATION_WORDS])
    }
    /// Two values below 2^32 in one configuration word, `high` first. Callers
    /// pass values their constructors already bound below 2^32.
    pub const fn pair(high: usize, low: usize) -> u64 {
        ((high as u64) << 32) | (low as u64 & 0xffff_ffff)
    }
}

/// One application below the outer one: its schema and identity.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub struct InnerLayer {
    pub schema: u64,
    pub identity: LayerIdentity,
}
impl InnerLayer {
    pub const fn new(schema: u64, identity: LayerIdentity) -> Self {
        Self { schema, identity }
    }
    /// A layer identified by its schema alone (`LayerIdentity::UNREGISTERED`).
    pub const fn unregistered(schema: u64) -> Self {
        Self::new(schema, LayerIdentity::UNREGISTERED)
    }
}

/// Most application layers an [`InnerProfile`] records.
pub const MAX_INNER_PROFILE_LAYERS: usize = 7;

/// Hash-free structural identity of the applications inside a wrapper: the
/// schema and identity (family and configuration) of each application below
/// the outer one, outermost first (a leaf application has none). Readiness
/// compares it layer for layer, so two stacks match only if every layer's
/// family, schema and configuration do. A generic stack deeper than
/// [`MAX_INNER_PROFILE_LAYERS`] gets an overflowed profile, which readiness
/// refuses (`UnsupportedSchema`) rather than truncating.
///
/// A wrapper must set it with [`InnerProfile::wrapping`] or
/// [`InnerProfile::of`]; a transparent wrapper forwards its inner
/// application's requirements unchanged. Declaring `LEAF` around an inner
/// application hides that application from readiness: a trusted-provider
/// obligation, not something readiness can detect.
///
/// The profile also holds the stack's [`DeploymentPlan`]: `wrapping` carries
/// the inner application's plan segments one layer deeper, so every wrapper
/// that uses it propagates them, and [`InnerProfile::planned`] adds the
/// owning application's own segment.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub struct InnerProfile {
    /// Layers in use, or `u8::MAX` once overflowed.
    len: u8,
    layers: [InnerLayer; MAX_INNER_PROFILE_LAYERS],
    plan: DeploymentPlan,
}
impl InnerProfile {
    const UNUSED: InnerLayer = InnerLayer::unregistered(0);
    /// No inner application.
    pub const LEAF: Self = Self {
        len: 0,
        layers: [Self::UNUSED; MAX_INNER_PROFILE_LAYERS],
        plan: DeploymentPlan::NONE,
    };
    const OVERFLOWED: Self = Self {
        len: u8::MAX,
        layers: [Self::UNUSED; MAX_INNER_PROFILE_LAYERS],
        plan: DeploymentPlan::NONE,
    };
    /// The profile of a wrapper whose inner application declares `inner`: the
    /// inner schema and identity, then the inner application's own profile,
    /// and the inner plan segments one layer deeper.
    pub fn wrapping(inner: ReadinessRequirements) -> Self {
        let Some(below) = inner.inner_profile.layers() else {
            return Self::OVERFLOWED;
        };
        let mut layers = [Self::UNUSED; MAX_INNER_PROFILE_LAYERS + 1];
        layers[0] = InnerLayer::new(inner.application_schema, inner.identity);
        layers[1..=below.len()].copy_from_slice(below);
        let Some(profile) = Self::from_layers(&layers[..=below.len()]) else {
            return Self::OVERFLOWED;
        };
        match inner.inner_profile.plan.deeper() {
            Some(plan) => Self { plan, ..profile },
            None => Self::OVERFLOWED,
        }
    }
    /// This profile with the owning application's own plan segment (depth 0)
    /// added; overflowed if it does not fit or one is already present. No
    /// segment for empty `own`.
    pub fn planned(self, own: &[u64]) -> Self {
        if self.layers().is_none() {
            return Self::OVERFLOWED;
        }
        match self.plan.with_own(own) {
            Some(plan) => Self { plan, ..self },
            None => Self::OVERFLOWED,
        }
    }
    /// This profile with a decoded plan; `None` unless the plan's depths name
    /// layers of this profile.
    pub fn with_plan(self, plan: DeploymentPlan) -> Option<Self> {
        let layers = self.layers()?.len();
        DeploymentPlan::from_words(plan.words(), layers)?;
        Some(Self { plan, ..self })
    }
    /// The stack's deployment plan (empty if overflowed).
    pub fn plan(&self) -> &DeploymentPlan {
        &self.plan
    }
    /// The profile of a wrapper around `inner`: from its declared
    /// requirements, or its checkpoint schema alone if it declares none.
    pub fn of<A: CheckpointStateMachine>(inner: &A) -> Self {
        match inner.deployment_requirements() {
            Some(declared) => Self::wrapping(declared),
            None => Self::from_layers(&[InnerLayer::unregistered(inner.schema_version())])
                .unwrap_or(Self::OVERFLOWED),
        }
    }
    /// An explicit profile; `None` above [`MAX_INNER_PROFILE_LAYERS`] layers or
    /// with a zero schema.
    pub fn from_layers(layers: &[InnerLayer]) -> Option<Self> {
        if layers.len() > MAX_INNER_PROFILE_LAYERS || layers.iter().any(|l| l.schema == 0) {
            return None;
        }
        let mut all = [Self::UNUSED; MAX_INNER_PROFILE_LAYERS];
        all[..layers.len()].copy_from_slice(layers);
        Some(Self {
            len: layers.len() as u8,
            layers: all,
            plan: DeploymentPlan::NONE,
        })
    }
    /// The inner layers, outermost first; `None` if overflowed.
    pub fn layers(&self) -> Option<&[InnerLayer]> {
        self.layers.get(..usize::from(self.len))
    }
    /// The inner schemas, outermost first; `None` if overflowed.
    pub fn schemas(&self) -> Option<Vec<u64>> {
        Some(self.layers()?.iter().map(|l| l.schema).collect())
    }
}

/// Most words a [`DeploymentPlan`] holds. The largest in-tree plan is a
/// logical voter enrollment candidate with an activation control profile of
/// 16 mappings: 1 + 173 words (`LogicalApplication`); a logical activation
/// parent with 16 mappings needs 1 + 115.
pub const MAX_DEPLOYMENT_PLAN_WORDS: usize = 192;

/// Exact, hash-free words for the construction inputs that decide committed
/// entries but are neither a limit nor a mode: the groups, voters and stores
/// an application was built for (logical activation's parent and store
/// mappings, a transaction's group and profile, a bucket counter's scope and
/// partition scheme). Readiness compares it exactly, as part of
/// [`InnerProfile`]. It does not cover unbounded inputs (initial images,
/// grant manifests, directory plans, anchors); see
/// docs/APPLICATION_ENVELOPES.md.
///
/// Segments, outermost first: a header word `depth << 32 | count`, then
/// `count` words in the layer's documented layout. Depth 0 is the application
/// whose requirements hold the profile; depth `d` the `d`-th inner layer.
/// Canonical: depths strictly increasing, `count ≥ 1`, unused words zero, so
/// two plans are equal exactly when their segments are.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub struct DeploymentPlan {
    len: u8,
    words: [u64; MAX_DEPLOYMENT_PLAN_WORDS],
}
impl DeploymentPlan {
    /// No segments.
    pub const NONE: Self = Self {
        len: 0,
        words: [0; MAX_DEPLOYMENT_PLAN_WORDS],
    };
    /// Headers and segment words, as carried on the wire.
    pub fn words(&self) -> &[u64] {
        &self.words[..usize::from(self.len)]
    }
    /// A canonical plan for a profile with `layers` inner layers; `None` for a
    /// malformed, non-canonical or oversized one.
    pub fn from_words(words: &[u64], layers: usize) -> Option<Self> {
        if words.len() > MAX_DEPLOYMENT_PLAN_WORDS {
            return None;
        }
        let mut at = 0;
        let mut previous: Option<u64> = None;
        while let Some(&header) = words.get(at) {
            let (depth, count) = (header >> 32, (header & 0xffff_ffff) as usize);
            if depth > layers as u64
                || previous.is_some_and(|p| depth <= p)
                || count == 0
                || count >= words.len() - at
            {
                return None;
            }
            previous = Some(depth);
            at += 1 + count;
        }
        let mut all = [0; MAX_DEPLOYMENT_PLAN_WORDS];
        all[..words.len()].copy_from_slice(words);
        Some(Self {
            len: words.len() as u8,
            words: all,
        })
    }
    /// Each segment's depth and words, outermost first.
    pub fn segments(&self) -> impl Iterator<Item = (usize, &[u64])> {
        let words = self.words();
        let mut at = 0;
        std::iter::from_fn(move || {
            let header = *words.get(at)?;
            let count = (header & 0xffff_ffff) as usize;
            let segment = ((header >> 32) as usize, &words[at + 1..at + 1 + count]);
            at += 1 + count;
            Some(segment)
        })
    }
    /// The words of the layer at `depth`, if it declared any.
    pub fn segment(&self, depth: usize) -> Option<&[u64]> {
        self.segments().find(|(d, _)| *d == depth).map(|(_, w)| w)
    }
    /// Three words naming a group exactly: id high, id low, incarnation.
    pub const fn group_words(group: crate::identity::GroupIdentity) -> [u64; 3] {
        let id = group.id.get();
        [(id >> 64) as u64, id as u64, group.incarnation.get()]
    }
    /// Three words naming a store exactly: id high, id low, incarnation.
    pub const fn store_words(store: crate::identity::StoreIdentity) -> [u64; 3] {
        let id = store.id.get();
        [(id >> 64) as u64, id as u64, store.incarnation.get()]
    }
    /// Every segment one layer deeper; `None` beyond the deepest layer.
    fn deeper(&self) -> Option<Self> {
        let mut out = *self;
        let mut at = 0;
        while at < usize::from(out.len) {
            let header = out.words[at];
            if header >> 32 >= MAX_INNER_PROFILE_LAYERS as u64 {
                return None;
            }
            out.words[at] = header + (1 << 32);
            at += 1 + (header & 0xffff_ffff) as usize;
        }
        Some(out)
    }
    /// A depth-0 segment of `own` in front; `None` if one exists or it does
    /// not fit. Unchanged for empty `own`.
    fn with_own(&self, own: &[u64]) -> Option<Self> {
        if own.is_empty() {
            return Some(*self);
        }
        let len = usize::from(self.len);
        let total = len.checked_add(1 + own.len())?;
        if total > MAX_DEPLOYMENT_PLAN_WORDS || self.segment(0).is_some() {
            return None;
        }
        let mut words = [0; MAX_DEPLOYMENT_PLAN_WORDS];
        words[0] = own.len() as u64;
        words[1..=own.len()].copy_from_slice(own);
        words[1 + own.len()..total].copy_from_slice(self.words());
        Some(Self {
            len: total as u8,
            words,
        })
    }
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
        self.learner_readiness = Some(Box::new(request));
        self.ready_learner = None;
        Ok(request)
    }
    pub fn cancel_learner_readiness(&mut self) {
        self.learner_readiness = None;
        self.ready_learner = None;
    }
    /// Inspect the original volatile exchange still owned by this core.
    /// This is pending-work inspection, never promotion readiness or authority.
    pub fn pending_learner_readiness(&self) -> Option<LearnerReadinessRequest> {
        self.learner_readiness.as_deref().copied()
    }
    /// Volatile native-exchange result. Promotion must still recheck its scope
    /// and current authenticated peer binding at execution.
    pub fn ready_learner(&self) -> Option<&ReadyLearner> {
        self.ready_learner.as_deref()
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
        self.readiness_message_source(&message)?;
        match message.rpc {
            Rpc::LearnerReadinessRequest(_) => {
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
                let receipt = LearnerReadinessReceipt {
                    request,
                    binding: message.sender,
                };
                if self.learner_readiness.as_deref() != Some(&request)
                    || self.check_ready_scope(&receipt, message.sender).is_err()
                {
                    return Ok(Vec::new());
                }
                if ready {
                    self.ready_learner = Some(Box::new(
                        self.accept_learner_readiness(receipt, message.sender)
                            .map_err(|_| RaftError::InvalidMessage)?,
                    ));
                } else {
                    self.cancel_learner_readiness();
                }
                Ok(Vec::new())
            }
            _ => unreachable!(),
        }
    }
    pub(super) fn readiness_message_source(&self, message: &Message) -> Result<(), RaftError> {
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
            }
            Rpc::LearnerReadinessReply { .. } => {
                if message.from != request.learner.node
                    || message.to != request.leader
                    || message.sender.identity != request.learner.store
                    || message.sender.session != request.session
                {
                    return Err(RaftError::WrongIdentity);
                }
            }
            _ => unreachable!(),
        }
        Ok(())
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
        if self.learner_readiness.as_deref() != Some(&receipt.request) {
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
    application
        .validate_deployment_requirements(required)
        .map_err(|_| ReadinessError::Capability)?;
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
