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
//! Staged imports, decision-bound activation and later exact-boundary source fences.
use crate::{
    application::*,
    identity::*,
    log::*,
    routed::{
        codec::{decode, Command},
        OwnershipFence, ParentGrantStatus, RoutedQuery,
    },
    routing::{codec::*, *},
    scope::*,
    transfer::*,
    transfer_publication::*,
    transfer_source::{SourceExportCommitment, SourceFreezeStatus},
};
use std::mem::size_of;
mod parent;
mod partial;
use crate::{routed::RetainedGrantStatus, scoped_source::ScopedExportStatus};
pub use parent::PARENT_TRANSFER_TARGET_SCHEMA;
pub use partial::{MAX_TARGET_PARTIAL_TRANSFERS, PARTIAL_TRANSFER_TARGET_SCHEMA};

pub const TRANSFER_TARGET_SCHEMA: u64 = 2;
pub const INSERTION_TRANSFER_TARGET_SCHEMA: u64 = 3;
pub const RECURSIVE_INSERTION_TRANSFER_TARGET_SCHEMA: u64 = 4;
pub const MAX_TARGET_ACTIVATION_BYTES: usize = 64 * 1024;
pub const MAX_INLINE_IMPORT_BYTES: usize = 8 * 1024 * 1024;
pub const MAX_TARGET_FREEZE_BYTES: usize = 52 + MAX_TRANSFER_INTENT_BYTES;
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct SourceImport {
    pub fence: OwnershipFence,
    /// Authenticated source observation context, verified by the trusted host.
    pub configuration: ConfigurationId,
    pub image: ScopeImage,
    /// Expected commitment from authenticated quorum-readable source status.
    pub digest: ContentDigest,
}
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct TargetImport {
    operation: OperationId,
    intent: TransferIntent,
    target: GroupIdentity,
    sources: Vec<SourceImport>,
}
impl TargetImport {
    #[allow(clippy::result_large_err)] // Return original owned input on rejection.
    pub fn new(
        operation: OperationId,
        intent: TransferIntent,
        target: GroupIdentity,
        sources: Vec<SourceImport>,
    ) -> Result<Self, (ApplicationError, TransferIntent, Vec<SourceImport>)> {
        let validate = || -> Result<(), ApplicationError> {
            if !intent.permits_operation(operation) {
                return Err(ApplicationError::InvalidCommand);
            }
            let targets = intent.targets();
            let scope = targets
                .iter()
                .find(|r| r.target == RouteTarget::Group(target))
                .ok_or(ApplicationError::InvalidCommand)?
                .scope;
            let expected = intent
                .sources()
                .into_iter()
                .filter_map(|r| {
                    let start = scope.start().max(r.scope.start());
                    let end = scope.end().min(r.scope.end());
                    (start < end).then_some((r.target, start, end))
                })
                .collect::<Vec<_>>();
            if sources.len() != expected.len()
                || sources.is_empty()
                || sources.capacity() > MAX_SCOPE_IMPORTS
            {
                return Err(ApplicationError::InvalidCommand);
            }
            let mut total = 0usize;
            for (source, (group, start, end)) in sources.iter().zip(expected) {
                let fence = source.fence;
                if group != RouteTarget::Group(fence.group)
                    || fence.responsibility != intent.before().input().responsibility
                    || fence.epoch != intent.before().input().epoch
                    || fence.operation != operation
                    || fence.index == 0
                    || fence.index == u64::MAX
                    || source.image.source_applied() != fence.index
                    || source.image.scope().start() != start
                    || source.image.scope().end() != end
                    || source.digest != ContentDigest::scope_image(&source.image)
                    || source.image.scheme() != intent.before().input().scheme
                {
                    return Err(ApplicationError::InvalidCommand);
                }
                total = total
                    .checked_add(source.image.payload_capacity())
                    .ok_or(ApplicationError::InvalidCommand)?;
            }
            if total > MAX_INLINE_IMPORT_BYTES {
                return Err(ApplicationError::InvalidCommand);
            }
            Ok(())
        };
        if let Err(e) = validate() {
            return Err((e, intent, sources));
        }
        Ok(Self {
            operation,
            intent,
            target,
            sources,
        })
    }
    pub fn operation(&self) -> OperationId {
        self.operation
    }
    pub fn intent(&self) -> &TransferIntent {
        &self.intent
    }
    pub fn target(&self) -> GroupIdentity {
        self.target
    }
    pub fn sources(&self) -> &[SourceImport] {
        &self.sources
    }
    pub fn encode(&self, max_bytes: usize) -> Result<Vec<u8>, ApplicationError> {
        let intent = self.intent.encode(MAX_TRANSFER_INTENT_BYTES)?;
        let len = 54
            + intent.len()
            + self
                .sources
                .iter()
                .map(|s| 88 + s.image.bytes().len())
                .sum::<usize>();
        if len > max_bytes || len > MAX_INLINE_IMPORT_BYTES {
            return Err(ApplicationError::InvalidCommand);
        }
        let mut bytes = Vec::with_capacity(len);
        bytes.extend(b"VBTIMP01");
        bytes.extend(self.operation.get().to_le_bytes());
        put_group(&mut bytes, self.target);
        bytes.extend((intent.len() as u32).to_le_bytes());
        bytes.extend(intent);
        bytes.extend((self.sources.len() as u16).to_le_bytes());
        for source in &self.sources {
            put_group(&mut bytes, source.fence.group);
            bytes.extend(source.fence.index.to_le_bytes());
            bytes.extend(source.configuration.get().to_le_bytes());
            bytes.extend(source.image.schema().to_le_bytes());
            put_range(&mut bytes, source.image.scope());
            bytes.extend(source.digest.0);
            bytes.extend((source.image.bytes().len() as u32).to_le_bytes());
            bytes.extend(source.image.bytes());
        }
        Ok(bytes)
    }
    pub fn decode(bytes: &[u8]) -> Result<Self, ApplicationError> {
        if bytes.len() > MAX_INLINE_IMPORT_BYTES {
            return Err(ApplicationError::InvalidCommand);
        }
        let mut r = Reader::new(bytes);
        if r.take(8)? != b"VBTIMP01" {
            return Err(ApplicationError::InvalidCommand);
        }
        let operation = r.operation()?;
        let target = r.group()?;
        let len = r.u32()? as usize;
        let intent = TransferIntent::decode(r.take(len)?)?;
        let count = r.u16()? as usize;
        if count == 0 || count > MAX_SCOPE_IMPORTS {
            return Err(ApplicationError::InvalidCommand);
        }
        let mut sources = Vec::with_capacity(count);
        for _ in 0..count {
            let group = r.group()?;
            let index = r.u64()?;
            let configuration =
                ConfigurationId::new(r.u64()?).ok_or(ApplicationError::InvalidCommand)?;
            let schema = r.u64()?;
            let scope = r.range()?;
            let digest = ContentDigest(
                r.take(32)?
                    .try_into()
                    .map_err(|_| ApplicationError::InvalidCommand)?,
            );
            let len = r.u32()? as usize;
            let image = ScopeImage::new(
                schema,
                intent.before().input().scheme,
                scope,
                index,
                r.take(len)?.to_vec(),
            )
            .map_err(|e| e.0)?;
            let fence = OwnershipFence {
                group,
                responsibility: intent.before().input().responsibility,
                epoch: intent.before().input().epoch,
                operation,
                index,
            };
            sources.push(SourceImport {
                fence,
                configuration,
                image,
                digest,
            });
        }
        if !r.done() {
            return Err(ApplicationError::InvalidCommand);
        }
        Self::new(operation, intent, target, sources).map_err(|e| e.0)
    }
}
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub struct TargetLimits {
    pub import_bytes: usize,
    pub application_checkpoint_bytes: usize,
}
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct ImportedSource {
    pub fence: OwnershipFence,
    pub configuration: ConfigurationId,
    pub scope: BucketRange,
    pub digest: ContentDigest,
}
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct ImportStatus {
    pub index: u64,
    /// SHA-256 of the complete canonical load command, including bootstrap binding.
    pub digest: ContentDigest,
    pub sources: Vec<ImportedSource>,
}
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct TargetActivation {
    /// Authenticated directory configuration, verified by the trusted host.
    pub metadata_configuration: ConfigurationId,
    pub decision: TransferPublicationStatus,
}
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub struct ActivationStatus {
    pub index: u64,
    pub digest: ContentDigest,
    pub metadata_configuration: ConfigurationId,
    pub publication_operation: OperationId,
    pub publication_index: u64,
}
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct TargetStatus {
    pub group: GroupIdentity,
    pub operation: OperationId,
    pub staged_index: Option<u64>,
    pub imported: Option<ImportStatus>,
    /// Original activation, retained even after a later source freeze. This is
    /// historical lineage, not current serving permission; query Freeze too.
    pub activated: Option<ActivationStatus>,
}
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum TargetOutcome<R = ()> {
    ScopeFenced(ScopedExportStatus),
    GrantAdopted(RetainedGrantStatus),
    ParentAdopted(ParentGrantStatus),
    Activated(ActivationStatus),
    Frozen(OwnershipFence),
    Applied(R),
    Rejected(RoutingError),
    Staged { index: u64 },
    Imported { index: u64, digest: ContentDigest },
    NotActive,
    OperationConflict,
}
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub struct TargetReceipt<R = ()> {
    pub index: u64,
    pub operation: OperationId,
    pub outcome: TargetOutcome<R>,
}
impl<R: ApplicationReceipt> ApplicationReceipt for TargetReceipt<R> {
    fn index(&self) -> u64 {
        self.index
    }
    fn operation(&self) -> OperationId {
        self.operation
    }
    fn nested_bytes(&self, limit: usize) -> Result<usize, ApplicationError> {
        match &self.outcome {
            TargetOutcome::Applied(r) => r.nested_bytes(limit),
            _ => Ok(0),
        }
    }
}
#[derive(Clone, Debug, Eq, PartialEq)]
pub enum TargetQuery<Q> {
    ScopedFreeze(OperationId),
    RetainedGrant(OperationId),
    ParentAdoption(OperationId),
    Status,
    Freeze,
    Data(RoutedQuery<Q>),
}
#[derive(Clone, Debug, Eq, PartialEq)]
#[allow(clippy::large_enum_variant)] // Inline status is charged by read_result_bound.
pub enum TargetRead<R> {
    ScopedFreeze(Option<ScopedExportStatus>),
    RetainedGrant(Option<RetainedGrantStatus>),
    ParentAdoption(Option<ParentGrantStatus>),
    Status(TargetStatus),
    Freeze(Option<SourceFreezeStatus>),
    NotActive,
    Data(R),
    Rejected(RoutingError),
}
#[derive(Clone)]
struct ImportRecord {
    index: u64,
    bytes: Vec<u8>,
    status: ImportStatus,
}
#[derive(Clone)]
struct ActivationRecord {
    bytes: Vec<u8>,
    status: ActivationStatus,
}
#[derive(Clone)]
struct FreezeRecord {
    bytes: Vec<u8>,
    intent: TransferIntent,
    export_bytes: usize,
    fence: OwnershipFence,
}
#[derive(Clone)]
pub struct TransferTarget<A, P> {
    group: GroupIdentity,
    operation: OperationId,
    intent: TransferIntent,
    inner: A,
    policy: P,
    limits: TargetLimits,
    binding: Vec<u8>,
    staged: Option<u64>,
    imported: Option<ImportRecord>,
    activated: Option<ActivationRecord>,
    frozen: Option<FreezeRecord>,
    parent_limit: usize,
    parents: Vec<parent::ParentRecord>,
    active_grant: ResponsibilityManifest,
    applied: u64,
    partial: Option<partial::PartialState>,
}
impl<A, P> TransferTarget<A, P>
where
    A: ScopeStateMachine + BoundedStateMachine,
    A::Receipt: ApplicationReceipt,
    P: PartitionPolicy + Clone,
{
    #[allow(clippy::result_large_err)] // Preserve original owned configuration/application/policy.
    pub fn new(
        group: GroupIdentity,
        operation: OperationId,
        intent: TransferIntent,
        inner: A,
        policy: P,
        limits: TargetLimits,
    ) -> Result<Self, (ApplicationError, TransferIntent, A, P)> {
        let construct = || -> Result<Vec<u8>, ApplicationError> {
            inner.validate_group(group)?;
            if !intent.permits_operation(operation) {
                return Err(ApplicationError::InvalidCommand);
            }
            let targets = intent.targets();
            let scope = targets
                .iter()
                .find(|r| r.target == RouteTarget::Group(group))
                .ok_or(ApplicationError::InvalidCommand)?
                .scope;
            if inner.applied_index() != 0
                || inner.scope() != scope
                || inner.scheme() != intent.after().input().scheme
                || policy.scheme() != inner.scheme()
                || limits.import_bytes == 0
                || limits.import_bytes > MAX_INLINE_IMPORT_BYTES
                || limits.application_checkpoint_bytes == 0
                || limits.application_checkpoint_bytes > MAX_SCOPE_IMAGE_BYTES
            {
                return Err(ApplicationError::InvalidCommand);
            }
            let initial = inner.checkpoint(limits.application_checkpoint_bytes)?;
            let intent_bytes = intent.encode(MAX_TRANSFER_INTENT_BYTES)?;
            let len = 80 + intent_bytes.len() + initial.len();
            if len > MAX_INLINE_IMPORT_BYTES {
                return Err(ApplicationError::InvalidCommand);
            }
            let mut bytes = Vec::with_capacity(len);
            bytes.extend(
                if intent.insertion_children().is_some() && intent.delegation().is_some() {
                    b"VBTSOWN3"
                } else if intent.insertion_children().is_some() {
                    b"VBTSOWN2"
                } else {
                    b"VBTSOWN1"
                },
            );
            put_group(&mut bytes, group);
            bytes.extend(operation.get().to_le_bytes());
            bytes.extend((limits.import_bytes as u64).to_le_bytes());
            bytes.extend((limits.application_checkpoint_bytes as u64).to_le_bytes());
            bytes.extend((intent_bytes.len() as u32).to_le_bytes());
            bytes.extend(intent_bytes);
            bytes.extend(inner.schema_version().to_le_bytes());
            bytes.extend((initial.len() as u32).to_le_bytes());
            bytes.extend(initial);
            Ok(bytes)
        };
        let binding = match construct() {
            Ok(b) => b,
            Err(e) => return Err((e, intent, inner, policy)),
        };
        Ok(Self {
            group,
            operation,
            active_grant: intent
                .target_manifest(group)
                .expect("checked target")
                .clone(),
            intent,
            inner,
            policy,
            limits,
            binding,
            staged: None,
            imported: None,
            activated: None,
            frozen: None,
            parent_limit: 0,
            parents: Vec::new(),
            applied: 0,
            partial: None,
        })
    }
    pub fn application(&self) -> &A {
        &self.inner
    }
    pub fn partition_policy(&self) -> &P {
        &self.policy
    }
    /// Encode a later source freeze bound to this target's original bootstrap.
    /// The trusted host verifies the committed next intent and target staging.
    /// The budget covers aggregate lifetime export capacity, not current bytes.
    pub fn freeze_command(
        &self,
        intent: &TransferIntent,
        export_bytes: usize,
        max_bytes: usize,
    ) -> Result<Vec<u8>, ApplicationError> {
        self.check_freeze(intent, export_bytes)?;
        let body = intent.encode(MAX_TRANSFER_INTENT_BYTES)?;
        let len = 52 + body.len();
        if len > max_bytes {
            return Err(ApplicationError::InvalidCommand);
        }
        let mut bytes = Vec::with_capacity(len);
        bytes.extend(b"VBTFRZ01");
        bytes.extend(ContentDigest::sha256(&self.binding).0);
        bytes.extend((export_bytes as u64).to_le_bytes());
        bytes.extend((body.len() as u32).to_le_bytes());
        bytes.extend(body);
        Ok(bytes)
    }
    fn freeze_request(&self, bytes: &[u8]) -> Result<(TransferIntent, usize), ApplicationError> {
        if bytes.len() > MAX_TARGET_FREEZE_BYTES {
            return Err(ApplicationError::InvalidCommand);
        }
        let mut r = Reader::new(bytes);
        if r.take(8)? != b"VBTFRZ01" || r.take(32)? != ContentDigest::sha256(&self.binding).0 {
            return Err(ApplicationError::InvalidCommand);
        }
        let budget = usize::try_from(r.u64()?).map_err(|_| ApplicationError::InvalidCommand)?;
        let len = r.u32()? as usize;
        let intent = TransferIntent::decode(r.take(len)?)?;
        if !r.done() {
            return Err(ApplicationError::InvalidCommand);
        }
        self.check_freeze(&intent, budget)?;
        Ok((intent, budget))
    }
    fn export_ranges(
        &self,
        intent: &TransferIntent,
    ) -> Result<Vec<(GroupIdentity, BucketRange)>, ApplicationError> {
        let scope = if self.partial.is_some() {
            let sources = intent
                .sources()
                .into_iter()
                .filter(|r| r.target == RouteTarget::Group(self.group))
                .collect::<Vec<_>>();
            if sources.len() != 1 {
                return Err(ApplicationError::InvalidCommand);
            }
            sources[0].scope
        } else {
            self.inner.scope()
        };
        let mut cursor = scope.start();
        let mut ranges = Vec::new();
        for target in intent.targets() {
            let start = scope.start().max(target.scope.start());
            let end = scope.end().min(target.scope.end());
            if start >= end {
                continue;
            }
            let RouteTarget::Group(group) = target.target else {
                return Err(ApplicationError::InvalidCommand);
            };
            if start != cursor {
                return Err(ApplicationError::InvalidCommand);
            }
            ranges.push((
                group,
                BucketRange::new(start, end).map_err(|_| ApplicationError::InvalidCommand)?,
            ));
            cursor = end;
        }
        if cursor != scope.end() {
            return Err(ApplicationError::InvalidCommand);
        }
        Ok(ranges)
    }
    fn check_freeze(&self, intent: &TransferIntent, budget: usize) -> Result<(), ApplicationError> {
        if self.activated.is_none()
            || self.partial_pending()
            || intent.before() != self.grant()
            || budget == 0
            || budget > MAX_SCOPE_IMAGE_BYTES
        {
            return Err(ApplicationError::InvalidCommand);
        }
        let mut total = 0usize;
        for (_, range) in self.export_ranges(intent)? {
            let bound = self.inner.export_scope_bound(range)?;
            if bound == 0 || bound > MAX_SCOPE_IMAGE_BYTES {
                return Err(ApplicationError::InvalidCommand);
            }
            total = total
                .checked_add(bound)
                .ok_or(ApplicationError::InvalidCommand)?;
        }
        if total > budget {
            return Err(ApplicationError::InvalidCommand);
        }
        Ok(())
    }
    pub fn fence(&self) -> Option<OwnershipFence> {
        self.frozen.as_ref().map(|r| r.fence)
    }
    /// Immutable exact-F data; its use by another group requires quorum status.
    pub fn export_target(
        &self,
        target: GroupIdentity,
        max_bytes: usize,
    ) -> Result<ScopeImage, ApplicationError> {
        let record = self.frozen.as_ref().ok_or(ApplicationError::NotApplied)?;
        let scope = self
            .export_ranges(&record.intent)?
            .into_iter()
            .find(|(g, _)| *g == target)
            .ok_or(ApplicationError::InvalidCommand)?
            .1;
        let bound = self.inner.export_scope_bound(scope)?;
        let image = self.inner.export_scope(scope, bound.min(max_bytes))?;
        if image.scope() != scope
            || image.scheme() != self.inner.scheme()
            || image.schema() != self.inner.schema_version()
            || image.source_applied() != record.fence.index
            || image.payload_capacity() > bound
            || image.payload_capacity() > max_bytes
        {
            return Err(ApplicationError::InvalidCheckpoint);
        }
        Ok(image)
    }
    pub fn freeze_status(&self) -> Result<Option<SourceFreezeStatus>, ApplicationError> {
        let Some(record) = &self.frozen else {
            return Ok(None);
        };
        let ranges = self.export_ranges(&record.intent)?;
        let mut exports = Vec::with_capacity(ranges.len());
        for (target, scope) in ranges {
            let image = self.export_target(target, record.export_bytes)?;
            exports.push(SourceExportCommitment {
                target,
                scope,
                digest: ContentDigest::scope_image(&image),
            });
        }
        Ok(Some(SourceFreezeStatus {
            fence: record.fence,
            intent: record.intent.clone(),
            exports,
        }))
    }
    pub fn bootstrap_command(&self, max_bytes: usize) -> Result<Vec<u8>, ApplicationError> {
        if self.binding.len() > max_bytes {
            return Err(ApplicationError::InvalidCommand);
        }
        Ok(self.binding.clone())
    }
    pub fn import_command(
        &self,
        import: &TargetImport,
        max_bytes: usize,
    ) -> Result<Vec<u8>, ApplicationError> {
        if import.operation() != self.operation
            || import.target() != self.group
            || import.intent() != &self.intent
        {
            return Err(ApplicationError::InvalidCommand);
        }
        let body = import.encode(self.limits.import_bytes)?;
        let len = 44 + body.len();
        if len > max_bytes {
            return Err(ApplicationError::InvalidCommand);
        }
        let mut bytes = Vec::with_capacity(len);
        bytes.extend(b"VBTLOAD1");
        bytes.extend(ContentDigest::sha256(&self.binding).0);
        bytes.extend((body.len() as u32).to_le_bytes());
        bytes.extend(body);
        Ok(bytes)
    }
    fn load(&self, bytes: &[u8]) -> Result<TargetImport, ApplicationError> {
        if bytes.len() > 44 + self.limits.import_bytes {
            return Err(ApplicationError::InvalidCommand);
        }
        let mut r = Reader::new(bytes);
        if r.take(8)? != b"VBTLOAD1" || r.take(32)? != ContentDigest::sha256(&self.binding).0 {
            return Err(ApplicationError::InvalidCommand);
        }
        let len = r.u32()? as usize;
        let import = TargetImport::decode(r.take(len)?)?;
        if !r.done()
            || import.target() != self.group
            || import.operation() != self.operation
            || import.intent() != &self.intent
            || import
                .sources()
                .iter()
                .any(|s| s.image.schema() != self.inner.schema_version())
        {
            return Err(ApplicationError::InvalidCommand);
        }
        Ok(import)
    }
    /// Encode a target-bound activation after verifying the committed local import.
    /// Foreign quorum provenance/configuration must be authenticated by the host.
    pub fn activation_command(
        &self,
        activation: &TargetActivation,
        max_bytes: usize,
    ) -> Result<Vec<u8>, ApplicationError> {
        self.check_activation(activation)?;
        let body = activation
            .decision
            .encode(MAX_TARGET_ACTIVATION_BYTES - 52)?;
        let len = 52 + body.len();
        if len > max_bytes || len > MAX_TARGET_ACTIVATION_BYTES {
            return Err(ApplicationError::InvalidCommand);
        }
        let mut bytes = Vec::with_capacity(len);
        bytes.extend(b"VBTACT01");
        bytes.extend(ContentDigest::sha256(&self.binding).0);
        bytes.extend(activation.metadata_configuration.get().to_le_bytes());
        bytes.extend((body.len() as u32).to_le_bytes());
        bytes.extend(body);
        Ok(bytes)
    }
    fn activation(&self, bytes: &[u8]) -> Result<TargetActivation, ApplicationError> {
        if bytes.len() > MAX_TARGET_ACTIVATION_BYTES {
            return Err(ApplicationError::InvalidCommand);
        }
        let mut r = Reader::new(bytes);
        if r.take(8)? != b"VBTACT01" || r.take(32)? != ContentDigest::sha256(&self.binding).0 {
            return Err(ApplicationError::InvalidCommand);
        }
        let metadata_configuration =
            ConfigurationId::new(r.u64()?).ok_or(ApplicationError::InvalidCommand)?;
        let len = r.u32()? as usize;
        let decision = TransferPublicationStatus::decode(r.take(len)?)?;
        if !r.done() {
            return Err(ApplicationError::InvalidCommand);
        }
        Ok(TargetActivation {
            metadata_configuration,
            decision,
        })
    }
    fn check_activation(&self, activation: &TargetActivation) -> Result<(), ApplicationError> {
        let publication = &activation.decision.publication;
        // Validate the public status's index and operation as well as the checked body.
        activation
            .decision
            .encode(MAX_TARGET_ACTIVATION_BYTES - 52)?;
        if publication.operation() != self.operation || publication.intent() != &self.intent {
            return Err(ApplicationError::InvalidCommand);
        }
        let evidence = publication
            .targets()
            .iter()
            .find(|t| t.group == self.group)
            .ok_or(ApplicationError::InvalidCommand)?;
        if Some(evidence.staged_index) != self.staged
            || self
                .imported
                .as_ref()
                .is_none_or(|r| r.status != evidence.imported)
        {
            return Err(ApplicationError::NotApplied);
        }
        Ok(())
    }
    fn activation_status(
        index: u64,
        bytes: &[u8],
        activation: &TargetActivation,
    ) -> ActivationStatus {
        ActivationStatus {
            index,
            digest: ContentDigest::sha256(bytes),
            metadata_configuration: activation.metadata_configuration,
            publication_operation: activation.decision.publication_operation,
            publication_index: activation.decision.index,
        }
    }
    fn request(&self, bytes: &[u8]) -> Result<Option<TargetImport>, ApplicationError> {
        if bytes.len() > self.readiness_requirements().command_bytes {
            return Err(ApplicationError::InvalidCommand);
        }
        if bytes == self.binding || self.partial.is_some() && partial::control(bytes) {
            Ok(None)
        } else if bytes.starts_with(b"VBTLOAD1") {
            self.load(bytes).map(Some)
        } else if bytes.starts_with(b"VBTACT01") {
            self.activation(bytes)?;
            Ok(None)
        } else if bytes.starts_with(b"VBTFRZ01") {
            self.freeze_request(bytes)?;
            Ok(None)
        } else if parent::is_parent(bytes) && self.parent_limit != 0 {
            crate::routed::parent_adoption::ParentAdoptionCommand::decode(bytes, true)?;
            Ok(None)
        } else {
            match decode(bytes, crate::routed::MAX_ROUTED_PAYLOAD_BYTES)? {
                Command::Data { key, payload, .. } if self.inner.command_key(payload)? == key => {
                    Ok(None)
                }
                _ => Err(ApplicationError::InvalidCommand),
            }
        }
    }
    fn status_for(index: u64, bytes: &[u8], import: &TargetImport) -> ImportStatus {
        ImportStatus {
            index,
            digest: ContentDigest::sha256(bytes),
            sources: import
                .sources()
                .iter()
                .map(|s| ImportedSource {
                    fence: s.fence,
                    configuration: s.configuration,
                    scope: s.image.scope(),
                    digest: s.digest,
                })
                .collect(),
        }
    }
    pub fn status(&self) -> TargetStatus {
        TargetStatus {
            group: self.group,
            operation: self.operation,
            staged_index: self.staged,
            imported: self.imported.as_ref().map(|r| r.status.clone()),
            activated: self.activated.as_ref().map(|r| r.status),
        }
    }
    pub fn readiness_requirements(&self) -> crate::raft::ReadinessRequirements {
        crate::raft::ReadinessRequirements {
            application_schema: self.schema_version(),
            command_bytes: self
                .binding
                .len()
                .max(44 + self.limits.import_bytes)
                .max(MAX_TARGET_ACTIVATION_BYTES)
                .max(if self.partial.is_some() {
                    crate::scoped_source::MAX_RETAINED_ADOPTION_BYTES
                        .max(crate::scoped_source::MAX_PARENT_SLOT_ADOPTION_BYTES)
                } else {
                    0
                })
                .max(if self.parent_limit == 0 {
                    0
                } else {
                    crate::routed::MAX_CROSS_PARENT_ADOPTION_BYTES
                }),
            snapshot_bytes: 112
                + MAX_TARGET_FREEZE_BYTES
                + MAX_TARGET_ACTIVATION_BYTES
                + self.binding.len()
                + 44
                + self.limits.import_bytes
                + self.limits.application_checkpoint_bytes
                + self.parent_reserve()
                + self.partial_reserve(),
        }
    }
}
impl<A, P> StateMachine for TransferTarget<A, P>
where
    A: ScopeStateMachine + BoundedStateMachine,
    A::Receipt: ApplicationReceipt,
    P: PartitionPolicy + Clone,
{
    type Receipt = TargetReceipt<A::Receipt>;
    fn validate_group(&self, group: GroupIdentity) -> Result<(), ApplicationError> {
        if group != self.group {
            return Err(ApplicationError::InvalidCommand);
        }
        self.inner.validate_group(group)
    }
    fn deployment_requirements(&self) -> Option<crate::raft::ReadinessRequirements> {
        Some(self.readiness_requirements())
    }
    fn applied_index(&self) -> u64 {
        self.applied
    }
    fn apply_batch(
        &mut self,
        entries: &[LogEntry],
    ) -> Result<Vec<Self::Receipt>, ApplicationError> {
        let mut next = self.clone();
        let mut receipts = Vec::with_capacity(
            entries
                .iter()
                .filter(|e| matches!(e.payload, EntryPayload::Command { .. }))
                .count(),
        );
        for entry in entries {
            if next.applied_index().checked_add(1) != Some(entry.index) {
                return Err(ApplicationError::IndexGap);
            }
            let mut provider_advanced = false;
            if let EntryPayload::Command { operation, bytes } = &entry.payload {
                let import = next.request(bytes)?;
                let outcome = if next.partial.is_some() && partial::control(bytes) {
                    let (outcome, advanced) = next.apply_partial(entry, None, false)?;
                    provider_advanced = advanced;
                    outcome
                } else if parent::is_parent(bytes) {
                    next.apply_parent(*operation, entry.index, bytes)?
                } else if next.parent_adoption(*operation).is_some()
                    || next.partial_operation(*operation)
                {
                    TargetOutcome::OperationConflict
                } else if let Some(record) = &next.frozen {
                    if *operation == record.fence.operation && *bytes == record.bytes {
                        TargetOutcome::Frozen(record.fence)
                    } else {
                        TargetOutcome::Rejected(RoutingError::Fenced)
                    }
                } else if bytes.starts_with(b"VBTFRZ01") {
                    let (intent, export_bytes) = next.freeze_request(bytes)?;
                    if !intent.permits_operation(*operation) {
                        return Err(ApplicationError::InvalidCommand);
                    }
                    if *operation == next.operation
                        || next.inner.contains_operation(*operation)
                        || next.partial_operation(*operation)
                    {
                        TargetOutcome::OperationConflict
                    } else {
                        if entry.index == u64::MAX {
                            return Err(ApplicationError::IndexGap);
                        }
                        let fence = OwnershipFence {
                            group: next.group,
                            responsibility: intent.before().input().responsibility,
                            epoch: intent.before().input().epoch,
                            operation: *operation,
                            index: entry.index,
                        };
                        let mut projected = entry.clone();
                        projected.payload = EntryPayload::Noop;
                        if !next.inner.apply_batch(&[projected])?.is_empty()
                            || next.inner.applied_index() != entry.index
                        {
                            return Err(ApplicationError::InvalidCommand);
                        }
                        next.inner
                            .checkpoint(next.limits.application_checkpoint_bytes)?;
                        next.frozen = Some(FreezeRecord {
                            bytes: bytes.clone(),
                            intent,
                            export_bytes,
                            fence,
                        });
                        next.freeze_status()?; // Validate actual exports before publishing the fence.
                        provider_advanced = true;
                        TargetOutcome::Frozen(fence)
                    }
                } else if bytes == &next.binding {
                    if *operation != next.operation {
                        TargetOutcome::OperationConflict
                    } else {
                        TargetOutcome::Staged {
                            index: *next.staged.get_or_insert(entry.index),
                        }
                    }
                } else if let Some(import) = import {
                    if *operation != next.operation {
                        TargetOutcome::OperationConflict
                    } else if let Some(record) = &next.imported {
                        if record.bytes == *bytes {
                            TargetOutcome::Imported {
                                index: record.index,
                                digest: record.status.digest,
                            }
                        } else {
                            TargetOutcome::OperationConflict
                        }
                    } else {
                        if next.staged.is_none() {
                            return Err(ApplicationError::NotApplied);
                        }
                        let images = import
                            .sources()
                            .iter()
                            .map(|s| s.image.clone())
                            .collect::<Vec<_>>();
                        next.inner.import_scopes(&images, entry.index)?;
                        if next.inner.applied_index() != entry.index
                            || next.inner.contains_operation(next.operation)
                        {
                            return Err(ApplicationError::InvalidCheckpoint);
                        }
                        // Validate recoverable provider envelope before accepting readiness.
                        next.inner
                            .checkpoint(next.limits.application_checkpoint_bytes)?;
                        let status = Self::status_for(entry.index, bytes, &import);
                        let outcome = TargetOutcome::Imported {
                            index: entry.index,
                            digest: status.digest,
                        };
                        next.imported = Some(ImportRecord {
                            index: entry.index,
                            bytes: bytes.clone(),
                            status,
                        });
                        provider_advanced = true;
                        outcome
                    }
                } else if bytes.starts_with(b"VBTACT01") {
                    if *operation != next.operation {
                        TargetOutcome::OperationConflict
                    } else if let Some(record) = &next.activated {
                        if record.bytes == *bytes {
                            TargetOutcome::Activated(record.status)
                        } else {
                            TargetOutcome::OperationConflict
                        }
                    } else {
                        let activation = next.activation(bytes)?;
                        next.check_activation(&activation)?;
                        let status = Self::activation_status(entry.index, bytes, &activation);
                        next.activated = Some(ActivationRecord {
                            bytes: bytes.clone(),
                            status,
                        });
                        TargetOutcome::Activated(status)
                    }
                } else if *operation == next.operation {
                    TargetOutcome::OperationConflict
                } else if next.activated.is_none() {
                    TargetOutcome::NotActive
                } else {
                    let Command::Data { hint, key, payload } =
                        decode(bytes, crate::routed::MAX_ROUTED_PAYLOAD_BYTES)?
                    else {
                        return Err(ApplicationError::InvalidCommand);
                    };
                    if let Err(error) = next.check_active_context(&hint, key) {
                        TargetOutcome::Rejected(error)
                    } else {
                        let projected = LogEntry {
                            index: entry.index,
                            term: entry.term,
                            payload: EntryPayload::Command {
                                operation: *operation,
                                bytes: payload.to_vec(),
                            },
                        };
                        let mut result = next.inner.apply_batch(&[projected])?.into_iter();
                        let receipt = result.next().ok_or(ApplicationError::InvalidCommand)?;
                        if result.next().is_some()
                            || receipt.index() != entry.index
                            || receipt.operation() != *operation
                            || next.inner.applied_index() != entry.index
                        {
                            return Err(ApplicationError::InvalidCommand);
                        }
                        next.inner
                            .checkpoint(next.limits.application_checkpoint_bytes)?;
                        provider_advanced = true;
                        TargetOutcome::Applied(receipt)
                    }
                };
                receipts.push(TargetReceipt {
                    index: entry.index,
                    operation: *operation,
                    outcome,
                });
            }
            if !provider_advanced && next.frozen.is_none() {
                let mut projected = entry.clone();
                if matches!(projected.payload, EntryPayload::Command { .. }) {
                    projected.payload = EntryPayload::Noop;
                }
                if !next.inner.apply_batch(&[projected])?.is_empty()
                    || next.inner.applied_index() != entry.index
                {
                    return Err(ApplicationError::InvalidCommand);
                }
            }
            next.applied = entry.index;
        }
        *self = next;
        Ok(receipts)
    }
}
impl<A, P> BoundedStateMachine for TransferTarget<A, P>
where
    A: ScopeStateMachine + BoundedStateMachine,
    A::Receipt: ApplicationReceipt,
    P: PartitionPolicy + Clone,
{
    fn receipt_bytes_bound(&self, entries: &[LogEntry]) -> Result<usize, ApplicationError> {
        // Advance the staged copy across imports/activation before asking the
        // selected provider for each active data receipt's bound.
        let mut next = self.clone();
        let mut bound = 0usize;
        for entry in entries {
            if let EntryPayload::Command { operation, bytes } = &entry.payload {
                next.request(bytes)?;
                let mut nested = 0usize;
                if next.activated.is_some()
                    && next.frozen.is_none()
                    && *operation != next.operation
                    && next.parent_adoption(*operation).is_none()
                    && bytes.starts_with(b"VBRCMD01")
                {
                    let Command::Data { hint, key, payload } =
                        decode(bytes, crate::routed::MAX_ROUTED_PAYLOAD_BYTES)?
                    else {
                        return Err(ApplicationError::InvalidCommand);
                    };
                    if next.check_active_context(&hint, key).is_ok() {
                        let projected = LogEntry {
                            index: entry.index,
                            term: entry.term,
                            payload: EntryPayload::Command {
                                operation: *operation,
                                bytes: payload.to_vec(),
                            },
                        };
                        nested = next
                            .inner
                            .receipt_bytes_bound(&[projected])?
                            .checked_sub(size_of::<A::Receipt>())
                            .ok_or(ApplicationError::ReceiptBudget)?;
                    }
                }
                bound = bound
                    .checked_add(size_of::<Self::Receipt>())
                    .and_then(|n| n.checked_add(nested))
                    .ok_or(ApplicationError::ReceiptBudget)?;
            }
            next.apply_batch(std::slice::from_ref(entry))?;
        }
        Ok(bound)
    }
}
impl<A, P> ProposalAdmission for TransferTarget<A, P>
where
    A: ScopeStateMachine + BoundedStateMachine + ProposalAdmission,
    A::Receipt: ApplicationReceipt,
    P: PartitionPolicy + Clone,
{
    fn validate_proposal<'a>(
        &self,
        operation: OperationId,
        bytes: &[u8],
        pending: impl Iterator<Item = (OperationId, &'a [u8])>,
    ) -> Result<usize, ApplicationError> {
        if bytes.len() > self.readiness_requirements().command_bytes {
            return Err(ApplicationError::InvalidCommand);
        }
        let mut next = self.clone();
        for (position, (operation, bytes)) in pending.enumerate() {
            if position >= crate::routed::MAX_ROUTED_PENDING
                || bytes.len() > self.readiness_requirements().command_bytes
            {
                return Err(ApplicationError::ReceiptBudget);
            }
            let index = next
                .applied_index()
                .checked_add(1)
                .ok_or(ApplicationError::IndexGap)?;
            next.apply_batch(&[LogEntry {
                index,
                term: 1,
                payload: EntryPayload::Command {
                    operation,
                    bytes: bytes.to_vec(),
                },
            }])?;
        }
        let mut bound = size_of::<Self::Receipt>();
        if bytes.starts_with(b"VBRCMD01") {
            let Command::Data { hint, key, payload } =
                decode(bytes, crate::routed::MAX_ROUTED_PAYLOAD_BYTES)?
            else {
                return Err(ApplicationError::InvalidCommand);
            };
            if next.activated.is_none()
                || next.frozen.is_some()
                || operation == next.operation
                || next.parent_adoption(operation).is_some()
                || next.partial_operation(operation)
            {
                return Err(ApplicationError::InvalidCommand);
            }
            next.check_active_context(&hint, key)
                .map_err(|_| ApplicationError::InvalidCommand)?;
            let inner = next
                .inner
                .validate_proposal(operation, payload, std::iter::empty())?;
            bound = bound
                .checked_add(
                    inner
                        .checked_sub(size_of::<A::Receipt>())
                        .ok_or(ApplicationError::ReceiptBudget)?,
                )
                .ok_or(ApplicationError::ReceiptBudget)?;
        }
        let index = next
            .applied_index()
            .checked_add(1)
            .ok_or(ApplicationError::IndexGap)?;
        let receipt = next
            .apply_batch(&[LogEntry {
                index,
                term: 1,
                payload: EntryPayload::Command {
                    operation,
                    bytes: bytes.to_vec(),
                },
            }])?
            .remove(0);
        if matches!(
            receipt.outcome,
            TargetOutcome::NotActive
                | TargetOutcome::OperationConflict
                | TargetOutcome::Rejected(_)
        ) {
            return Err(ApplicationError::InvalidCommand);
        }
        Ok(bound)
    }
}
impl<A, P> CheckpointStateMachine for TransferTarget<A, P>
where
    A: ScopeStateMachine + BoundedStateMachine,
    A::Receipt: ApplicationReceipt,
    P: PartitionPolicy + Clone,
{
    fn schema_version(&self) -> u64 {
        if self.partial.is_some() {
            PARTIAL_TRANSFER_TARGET_SCHEMA
        } else if self.parent_limit != 0 {
            PARENT_TRANSFER_TARGET_SCHEMA
        } else if self.intent.insertion_children().is_some() && self.intent.delegation().is_some() {
            RECURSIVE_INSERTION_TRANSFER_TARGET_SCHEMA
        } else if self.intent.insertion_children().is_some() {
            INSERTION_TRANSFER_TARGET_SCHEMA
        } else {
            TRANSFER_TARGET_SCHEMA
        }
    }
    fn checkpoint(&self, max_bytes: usize) -> Result<Vec<u8>, ApplicationError> {
        let inner = self
            .inner
            .checkpoint(self.limits.application_checkpoint_bytes)?;
        let import = self
            .imported
            .as_ref()
            .map_or(&[][..], |r| r.bytes.as_slice());
        let activation = self
            .activated
            .as_ref()
            .map_or(&[][..], |r| r.bytes.as_slice());
        let freeze = self.frozen.as_ref().map_or(&[][..], |r| r.bytes.as_slice());
        let parents = if self.partial.is_some() {
            self.partial_checkpoint()?
        } else {
            self.parent_checkpoint()?
        };
        let len = 100
            + self.binding.len()
            + import.len()
            + activation.len()
            + freeze.len()
            + inner.len()
            + parents.len();
        if len > max_bytes {
            return Err(ApplicationError::InvalidCheckpoint);
        }
        let mut bytes = Vec::with_capacity(len);
        bytes.extend(if self.partial.is_some() {
            b"VBTRGT07"
        } else if self.parent_limit != 0 {
            b"VBTRGT06"
        } else if self.intent.insertion_children().is_some() && self.intent.delegation().is_some() {
            b"VBTRGT05"
        } else if self.intent.insertion_children().is_some() {
            b"VBTRGT04"
        } else {
            b"VBTRGT03"
        });
        bytes.extend(self.applied_index().to_le_bytes());
        bytes.extend((self.binding.len() as u32).to_le_bytes());
        bytes.extend(&self.binding);
        bytes.extend(self.staged.unwrap_or(0).to_le_bytes());
        bytes.extend(self.imported.as_ref().map_or(0, |r| r.index).to_le_bytes());
        bytes.extend((import.len() as u32).to_le_bytes());
        bytes.extend(import);
        bytes.extend(
            self.activated
                .as_ref()
                .map_or(0, |r| r.status.index)
                .to_le_bytes(),
        );
        bytes.extend((activation.len() as u32).to_le_bytes());
        bytes.extend(activation);
        bytes.extend(self.inner.applied_index().to_le_bytes());
        bytes.extend(
            self.frozen
                .as_ref()
                .map_or(0, |r| r.fence.operation.get())
                .to_le_bytes(),
        );
        bytes.extend(
            self.frozen
                .as_ref()
                .map_or(0, |r| r.fence.index)
                .to_le_bytes(),
        );
        bytes.extend((freeze.len() as u32).to_le_bytes());
        bytes.extend(freeze);
        bytes.extend(self.inner.schema_version().to_le_bytes());
        bytes.extend((inner.len() as u32).to_le_bytes());
        bytes.extend(inner);
        bytes.extend(parents);
        Ok(bytes)
    }
    fn restore_checkpoint(
        &mut self,
        schema: u64,
        applied: u64,
        bytes: &[u8],
    ) -> Result<(), ApplicationError> {
        let insertion = self.intent.insertion_children().is_some();
        let recursive = insertion && self.intent.delegation().is_some();
        let parent = self.parent_limit != 0;
        let partial = self.partial.is_some();
        if ((insertion || parent || partial) && schema != self.schema_version())
            || (!insertion
                && !parent
                && !partial
                && schema != TRANSFER_TARGET_SCHEMA
                && schema != 1)
        {
            return Err(ApplicationError::UnsupportedSchema);
        }
        if bytes.len() > self.readiness_requirements().snapshot_bytes {
            return Err(ApplicationError::InvalidCheckpoint);
        }
        let mut r = Reader::new(bytes);
        let tag = r.take(8)?;
        let frozen_format = tag == b"VBTRGT03"
            || tag == b"VBTRGT04"
            || tag == b"VBTRGT05"
            || tag == b"VBTRGT06"
            || tag == b"VBTRGT07";
        if (partial && tag != b"VBTRGT07")
            || (!partial && tag == b"VBTRGT07")
            || (!partial && parent && tag != b"VBTRGT06")
            || (!partial && !parent && tag == b"VBTRGT06")
            || (!partial
                && !parent
                && ((recursive && tag != b"VBTRGT05")
                    || (!recursive && tag == b"VBTRGT05")
                    || (insertion && !recursive && tag != b"VBTRGT04")
                    || (!insertion && tag == b"VBTRGT04")))
            || (schema >= TRANSFER_TARGET_SCHEMA) != frozen_format
        {
            return Err(ApplicationError::UnsupportedSchema);
        }
        let activated_format = frozen_format || tag == b"VBTRGT02";
        if (!activated_format && tag != b"VBTRGT01") || r.u64()? != applied {
            return Err(ApplicationError::InvalidCheckpoint);
        }
        let len = r.u32()? as usize;
        if r.take(len)? != self.binding {
            return Err(ApplicationError::InvalidCheckpoint);
        }
        let staged = r.u64()?;
        let index = r.u64()?;
        let len = r.u32()? as usize;
        let import_bytes = r.take(len)?;
        if staged > applied
            || index > applied
            || (index == 0) != (len == 0)
            || index != 0 && (staged == 0 || index <= staged)
        {
            return Err(ApplicationError::InvalidCheckpoint);
        }
        let imported = if len == 0 {
            None
        } else {
            let import = self.load(import_bytes)?;
            Some(ImportRecord {
                index,
                bytes: import_bytes.to_vec(),
                status: Self::status_for(index, import_bytes, &import),
            })
        };
        let (activation_index, activation_bytes) = if activated_format {
            let index = r.u64()?;
            let len = r.u32()? as usize;
            (index, r.take(len)?)
        } else {
            (0, &[][..])
        };
        if activation_index > applied
            || (activation_index == 0) != activation_bytes.is_empty()
            || activation_index != 0 && (index == 0 || activation_index <= index)
        {
            return Err(ApplicationError::InvalidCheckpoint);
        }
        let (inner_applied, freeze_operation, freeze_index, freeze_bytes) = if frozen_format {
            let inner = r.u64()?;
            let operation = r.u128()?;
            let index = r.u64()?;
            let len = r.u32()? as usize;
            (inner, operation, index, r.take(len)?)
        } else {
            (applied, 0, 0, &[][..])
        };
        if inner_applied > applied
            || (freeze_index == 0) != freeze_bytes.is_empty()
            || (freeze_operation == 0) != freeze_bytes.is_empty()
            || freeze_bytes.is_empty() && inner_applied != applied
            || !freeze_bytes.is_empty()
                && (activation_index == 0
                    || freeze_index <= activation_index
                    || freeze_index != inner_applied
                    || freeze_index == u64::MAX)
        {
            return Err(ApplicationError::InvalidCheckpoint);
        }
        let inner_schema = r.u64()?;
        let len = r.u32()? as usize;
        if len > self.limits.application_checkpoint_bytes {
            return Err(ApplicationError::InvalidCheckpoint);
        }
        let mut next = self.clone();
        next.inner
            .restore_checkpoint(inner_schema, inner_applied, r.take(len)?)?;
        if next.inner.applied_index() != inner_applied
            || next.inner.contains_operation(next.operation)
        {
            return Err(ApplicationError::InvalidCheckpoint);
        }
        next.staged = (staged != 0).then_some(staged);
        next.imported = imported;
        next.activated = if activation_bytes.is_empty() {
            None
        } else {
            let activation = next.activation(activation_bytes)?;
            next.check_activation(&activation)?;
            Some(ActivationRecord {
                bytes: activation_bytes.to_vec(),
                status: Self::activation_status(activation_index, activation_bytes, &activation),
            })
        };
        if partial {
            next.restore_partial(&mut r, applied, freeze_index)?;
        } else {
            next.restore_parents(&mut r, applied, freeze_index)?;
        }
        if !r.done() {
            return Err(ApplicationError::InvalidCheckpoint);
        }
        next.frozen = if freeze_bytes.is_empty() {
            None
        } else {
            let (intent, export_bytes) = next.freeze_request(freeze_bytes)?;
            let operation =
                OperationId::new(freeze_operation).ok_or(ApplicationError::InvalidCheckpoint)?;
            if operation == next.operation
                || next.parent_adoption(operation).is_some()
                || next.partial_operation(operation)
                || next.inner.contains_operation(operation)
                || !intent.permits_operation(operation)
            {
                return Err(ApplicationError::InvalidCheckpoint);
            }
            let fence = OwnershipFence {
                group: next.group,
                responsibility: intent.before().input().responsibility,
                epoch: intent.before().input().epoch,
                operation,
                index: freeze_index,
            };
            Some(FreezeRecord {
                bytes: freeze_bytes.to_vec(),
                intent,
                export_bytes,
                fence,
            })
        };
        next.applied = applied;
        next.freeze_status()?;
        *self = next;
        Ok(())
    }
}
impl<A, P> ReadableStateMachine for TransferTarget<A, P>
where
    A: ScopeStateMachine + BoundedStateMachine + ReadableStateMachine,
    A::Receipt: ApplicationReceipt,
    P: PartitionPolicy + Clone,
{
    type Query = TargetQuery<A::Query>;
    type ReadResult = TargetRead<A::ReadResult>;
    fn read_at(
        &self,
        required: u64,
        query: Self::Query,
    ) -> Result<Self::ReadResult, ApplicationError> {
        if required > self.applied_index() {
            return Err(ApplicationError::NotApplied);
        }
        match query {
            TargetQuery::ScopedFreeze(op) => {
                if self.partial.is_none() {
                    return Err(ApplicationError::UnsupportedSchema);
                }
                Ok(TargetRead::ScopedFreeze(self.scoped_freeze(op)))
            }
            TargetQuery::RetainedGrant(op) => {
                if self.partial.is_none() {
                    return Err(ApplicationError::UnsupportedSchema);
                }
                Ok(TargetRead::RetainedGrant(self.retained_grant(op)))
            }
            TargetQuery::ParentAdoption(op) => {
                if self.parent_limit == 0 {
                    Err(ApplicationError::UnsupportedSchema)
                } else {
                    Ok(TargetRead::ParentAdoption(self.parent_adoption(op)))
                }
            }
            TargetQuery::Status => Ok(TargetRead::Status(self.status())),
            TargetQuery::Freeze => self.freeze_status().map(TargetRead::Freeze),
            TargetQuery::Data(query) => {
                if self.frozen.is_some() {
                    return Ok(TargetRead::Rejected(RoutingError::Fenced));
                }
                if self.activated.is_none() {
                    return Ok(TargetRead::NotActive);
                }
                if let Err(error) = self.check_active_context(&query.hint, &query.key) {
                    return Ok(TargetRead::Rejected(error));
                }
                self.inner
                    .read_at(required, query.query)
                    .map(TargetRead::Data)
            }
        }
    }
}
impl<A, P> BoundedReadableStateMachine for TransferTarget<A, P>
where
    A: ScopeStateMachine + BoundedStateMachine + BoundedReadableStateMachine,
    A::Receipt: ApplicationReceipt,
    P: PartitionPolicy + Clone,
{
    fn query_bytes(&self, query: &Self::Query, limit: usize) -> Result<usize, ApplicationError> {
        match query {
            TargetQuery::Status
            | TargetQuery::Freeze
            | TargetQuery::ParentAdoption(_)
            | TargetQuery::ScopedFreeze(_)
            | TargetQuery::RetainedGrant(_) => Ok(0),
            TargetQuery::Data(q) => {
                if q.key.capacity() > MAX_ROUTING_KEY_BYTES || q.key.capacity() > limit {
                    return Err(ApplicationError::ReceiptBudget);
                }
                q.key
                    .capacity()
                    .checked_add(self.inner.query_bytes(&q.query, limit - q.key.capacity())?)
                    .filter(|n| *n <= limit)
                    .ok_or(ApplicationError::ReceiptBudget)
            }
        }
    }
    fn read_result_bound(&self, query: &Self::Query) -> Result<usize, ApplicationError> {
        let nested = match query {
            TargetQuery::ParentAdoption(_)
            | TargetQuery::ScopedFreeze(_)
            | TargetQuery::RetainedGrant(_) => 0,
            TargetQuery::Freeze => self.frozen.as_ref().map_or(0, |r| {
                r.intent.retained_bytes() - size_of::<TransferIntent>()
                    + self
                        .export_ranges(&r.intent)
                        .expect("validated freeze")
                        .len()
                        * size_of::<SourceExportCommitment>()
            }),
            TargetQuery::Status => self.imported.as_ref().map_or(0, |r| {
                r.status.sources.capacity() * size_of::<ImportedSource>()
            }),
            TargetQuery::Data(q) => self
                .inner
                .read_result_bound(&q.query)?
                .checked_sub(size_of::<A::ReadResult>())
                .ok_or(ApplicationError::ReceiptBudget)?,
        };
        size_of::<Self::ReadResult>()
            .checked_add(nested)
            .ok_or(ApplicationError::ReceiptBudget)
    }
    fn read_result_bytes(
        &self,
        result: &Self::ReadResult,
        limit: usize,
    ) -> Result<usize, ApplicationError> {
        let bytes = match result {
            TargetRead::Freeze(status) => status.as_ref().map_or(0, |s| {
                s.intent.retained_bytes() - size_of::<TransferIntent>()
                    + s.exports.capacity() * size_of::<SourceExportCommitment>()
            }),
            TargetRead::Status(s) => s
                .imported
                .as_ref()
                .map_or(0, |i| i.sources.capacity() * size_of::<ImportedSource>()),
            TargetRead::NotActive
            | TargetRead::Rejected(_)
            | TargetRead::ParentAdoption(_)
            | TargetRead::ScopedFreeze(_)
            | TargetRead::RetainedGrant(_) => 0,
            TargetRead::Data(r) => self.inner.read_result_bytes(r, limit)?,
        };
        if bytes > limit {
            return Err(ApplicationError::ReceiptBudget);
        }
        Ok(bytes)
    }
}

impl<A, P> crate::retirement::sealed::Sealed for TransferTarget<A, P> {}
impl<A, P> crate::retirement::RetirableOwner for TransferTarget<A, P>
where
    A: ScopeStateMachine + BoundedStateMachine + ProposalAdmission + BoundedReadableStateMachine,
    A::Receipt: ApplicationReceipt,
    P: PartitionPolicy + Clone,
{
    fn retirement_requirements(&self) -> crate::raft::ReadinessRequirements {
        self.readiness_requirements()
    }
    fn retirement_source_status(&self) -> Result<Option<SourceFreezeStatus>, ApplicationError> {
        self.freeze_status()
    }
    fn retirement_export(
        &self,
        target: GroupIdentity,
        max_bytes: usize,
    ) -> Result<ScopeImage, ApplicationError> {
        self.export_target(target, max_bytes)
    }
    fn retirement_lineage(&self) -> Result<Vec<u8>, ApplicationError> {
        let record = self
            .activated
            .as_ref()
            .ok_or(ApplicationError::NotApplied)?;
        let mut grant = Vec::new();
        if self.parent_limit != 0 {
            put_manifest(&mut grant, self.grant());
        }
        let mut bytes = Vec::with_capacity(
            12 + record.bytes.len()
                + if self.parent_limit != 0 {
                    12 + grant.len()
                } else {
                    0
                },
        );
        bytes.extend(record.status.index.to_le_bytes());
        bytes.extend((record.bytes.len() as u32).to_le_bytes());
        bytes.extend(&record.bytes);
        if self.parent_limit != 0 {
            bytes.extend(b"VBTPARN1");
            bytes.extend((grant.len() as u32).to_le_bytes());
            bytes.extend(grant);
        }
        Ok(bytes)
    }
    fn validate_retirement_source(
        &self,
        status: &SourceFreezeStatus,
    ) -> Result<(), ApplicationError> {
        self.validate_group(status.fence.group)?;
        let original = self
            .intent
            .target_manifest(self.group)
            .expect("checked target");
        if status.intent.before() != original
            && (self.parent_limit == 0
                || !parent::same_owner_lineage(status.intent.before(), original))
        {
            return Err(ApplicationError::InvalidCheckpoint);
        }
        Ok(())
    }
    fn validate_retirement_lineage(
        &self,
        bytes: &[u8],
        fence_index: u64,
    ) -> Result<(), ApplicationError> {
        if bytes.len() > crate::retirement::MAX_RETIREMENT_LINEAGE_BYTES {
            return Err(ApplicationError::InvalidCheckpoint);
        }
        let mut r = Reader::new(bytes);
        let index = r.u64()?;
        let len = r.u32()? as usize;
        let activation = self.activation(r.take(len)?)?;
        if activation.decision.publication.operation() != self.operation
            || activation.decision.publication.intent() != &self.intent
        {
            return Err(ApplicationError::InvalidCheckpoint);
        }
        let evidence = activation
            .decision
            .publication
            .targets()
            .iter()
            .find(|t| t.group == self.group)
            .ok_or(ApplicationError::InvalidCheckpoint)?;
        if index <= evidence.imported.index || index >= fence_index {
            return Err(ApplicationError::InvalidCheckpoint);
        }
        if self.parent_limit != 0 {
            if r.take(8)? != b"VBTPARN1" {
                return Err(ApplicationError::InvalidCheckpoint);
            }
            let len = r.u32()? as usize;
            let grant = read_manifest(r.take(len)?)?;
            let original = self
                .intent
                .target_manifest(self.group)
                .expect("checked target");
            if !parent::same_owner_lineage(&grant, original) {
                return Err(ApplicationError::InvalidCheckpoint);
            }
        }
        if !r.done() {
            return Err(ApplicationError::InvalidCheckpoint);
        }
        Ok(())
    }
    fn validate_retirement_evidence(
        &self,
        status: &SourceFreezeStatus,
        lineage: &[u8],
    ) -> Result<(), ApplicationError> {
        self.validate_retirement_source(status)?;
        self.validate_retirement_lineage(lineage, status.fence.index)?;
        if self.parent_limit != 0 {
            let mut r = Reader::new(lineage);
            r.u64()?;
            let len = r.u32()? as usize;
            r.take(len)?;
            r.take(8)?;
            let len = r.u32()? as usize;
            let grant = read_manifest(r.take(len)?)?;
            if &grant != status.intent.before() {
                return Err(ApplicationError::InvalidCheckpoint);
            }
        }
        Ok(())
    }
}
