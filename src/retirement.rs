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
//! Durable source retirement after complete activation and explicit retention release.
//! Foreign observations and external retention promises belong to the trusted host.
use crate::{
    application::*, identity::*, log::*, raft::ReadinessRequirements, routing::codec::*,
    transfer::*, transfer_publication::*, transfer_source::*, transfer_target::*,
};
use std::mem::size_of;

pub const RETIREMENT_GUARD_SCHEMA: u64 = 1;
pub const MAX_RETIREMENT_PROOF_BYTES: usize = 128 * 1024;
pub const MAX_RETIREMENT_COMMAND_BYTES: usize = 44 + MAX_RETIREMENT_PROOF_BYTES;
pub const MAX_RETIREMENT_LINEAGE_BYTES: usize =
    24 + MAX_TARGET_ACTIVATION_BYTES + crate::routing::MAX_MANIFEST_BYTES;

/// Explicit host release of backup/application recovery promises for this exact cut.
/// This does not discover or override external retention pins.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub struct RetentionRelease {
    pub source: GroupIdentity,
    pub operation: OperationId,
    pub fence_index: u64,
    pub release: OperationId,
}
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub struct TargetActivationEvidence {
    pub group: GroupIdentity,
    pub operation: OperationId,
    pub configuration: ConfigurationId,
    pub staged_index: u64,
    pub import_digest: ContentDigest,
    pub activation: ActivationStatus,
}
impl TargetActivationEvidence {
    /// Requires an authenticated quorum observation, not an uncommitted receipt.
    #[allow(clippy::result_large_err)]
    pub fn from_status(
        configuration: ConfigurationId,
        status: TargetStatus,
    ) -> Result<Self, (ApplicationError, TargetStatus)> {
        let (Some(staged), Some(import), Some(activation)) = (
            status.staged_index,
            status.imported.as_ref(),
            status.activated,
        ) else {
            return Err((ApplicationError::NotApplied, status));
        };
        if staged == 0
            || import.index <= staged
            || activation.index <= import.index
            || activation.index == u64::MAX
        {
            return Err((ApplicationError::InvalidCommand, status));
        }
        Ok(Self {
            group: status.group,
            operation: status.operation,
            configuration,
            staged_index: staged,
            import_digest: import.digest,
            activation,
        })
    }
}
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct RetirementProof {
    pub metadata_configuration: ConfigurationId,
    pub decision: TransferPublicationStatus,
    pub targets: Vec<TargetActivationEvidence>,
    pub release: RetentionRelease,
}
impl RetirementProof {
    fn validate(&self) -> Result<(), ApplicationError> {
        self.decision.encode(MAX_TRANSFER_PUBLICATION_BYTES + 36)?;
        let publication = &self.decision.publication;
        let source = publication
            .sources()
            .iter()
            .find(|s| s.fence.group == self.release.source)
            .ok_or(ApplicationError::InvalidCommand)?;
        if self.release.operation != publication.operation()
            || self.release.operation != source.fence.operation
            || self.release.fence_index != source.fence.index
            || self.targets.capacity() > 256
            || self.targets.len() != publication.targets().len()
        {
            return Err(ApplicationError::InvalidCommand);
        }
        for (actual, ready) in self.targets.iter().zip(publication.targets()) {
            let a = actual.activation;
            if actual.group != ready.group
                || actual.operation != publication.operation()
                || actual.configuration != ready.configuration
                || actual.staged_index != ready.staged_index
                || actual.import_digest != ready.imported.digest
                || a.index <= ready.imported.index
                || a.index == u64::MAX
                || a.metadata_configuration != self.metadata_configuration
                || a.publication_operation != self.decision.publication_operation
                || a.publication_index != self.decision.index
            {
                return Err(ApplicationError::InvalidCommand);
            }
        }
        Ok(())
    }
    pub fn source_status(&self) -> Result<SourceFreezeStatus, ApplicationError> {
        self.validate()?;
        let source = self
            .decision
            .publication
            .sources()
            .iter()
            .find(|s| s.fence.group == self.release.source)
            .ok_or(ApplicationError::InvalidCommand)?;
        Ok(SourceFreezeStatus {
            fence: source.fence,
            intent: self.decision.publication.intent().clone(),
            exports: source.exports.clone(),
        })
    }
    pub fn encode(&self, max_bytes: usize) -> Result<Vec<u8>, ApplicationError> {
        self.validate()?;
        let decision = self.decision.encode(MAX_TRANSFER_PUBLICATION_BYTES + 36)?;
        let len = 86 + decision.len() + 160 * self.targets.len();
        if len > max_bytes || len > MAX_RETIREMENT_PROOF_BYTES {
            return Err(ApplicationError::InvalidCommand);
        }
        let mut out = Vec::with_capacity(len);
        out.extend(b"VBRETP01");
        out.extend(self.metadata_configuration.get().to_le_bytes());
        put_group(&mut out, self.release.source);
        out.extend(self.release.operation.get().to_le_bytes());
        out.extend(self.release.fence_index.to_le_bytes());
        out.extend(self.release.release.get().to_le_bytes());
        out.extend((decision.len() as u32).to_le_bytes());
        out.extend(decision);
        out.extend((self.targets.len() as u16).to_le_bytes());
        for t in &self.targets {
            put_group(&mut out, t.group);
            out.extend(t.operation.get().to_le_bytes());
            out.extend(t.configuration.get().to_le_bytes());
            out.extend(t.staged_index.to_le_bytes());
            out.extend(t.import_digest.0);
            let a = t.activation;
            out.extend(a.index.to_le_bytes());
            out.extend(a.digest.0);
            out.extend(a.metadata_configuration.get().to_le_bytes());
            out.extend(a.publication_operation.get().to_le_bytes());
            out.extend(a.publication_index.to_le_bytes());
        }
        Ok(out)
    }
    pub fn decode(bytes: &[u8]) -> Result<Self, ApplicationError> {
        if bytes.len() > MAX_RETIREMENT_PROOF_BYTES {
            return Err(ApplicationError::InvalidCommand);
        }
        let mut r = Reader::new(bytes);
        if r.take(8)? != b"VBRETP01" {
            return Err(ApplicationError::InvalidCommand);
        }
        let metadata_configuration = configuration(r.u64()?)?;
        let release = RetentionRelease {
            source: r.group()?,
            operation: r.operation()?,
            fence_index: r.u64()?,
            release: r.operation()?,
        };
        let len = r.u32()? as usize;
        let decision = TransferPublicationStatus::decode(r.take(len)?)?;
        let count = r.u16()? as usize;
        if count == 0 || count > 256 {
            return Err(ApplicationError::InvalidCommand);
        }
        let mut targets = Vec::with_capacity(count);
        for _ in 0..count {
            targets.push(TargetActivationEvidence {
                group: r.group()?,
                operation: r.operation()?,
                configuration: configuration(r.u64()?)?,
                staged_index: r.u64()?,
                import_digest: digest(r.take(32)?)?,
                activation: ActivationStatus {
                    index: r.u64()?,
                    digest: digest(r.take(32)?)?,
                    metadata_configuration: configuration(r.u64()?)?,
                    publication_operation: r.operation()?,
                    publication_index: r.u64()?,
                },
            });
        }
        if !r.done() {
            return Err(ApplicationError::InvalidCommand);
        }
        let proof = Self {
            metadata_configuration,
            decision,
            targets,
            release,
        };
        proof.validate()?;
        Ok(proof)
    }
}
fn configuration(value: u64) -> Result<ConfigurationId, ApplicationError> {
    ConfigurationId::new(value).ok_or(ApplicationError::InvalidCommand)
}
fn digest(bytes: &[u8]) -> Result<ContentDigest, ApplicationError> {
    Ok(ContentDigest(
        bytes
            .try_into()
            .map_err(|_| ApplicationError::InvalidCommand)?,
    ))
}

pub(crate) mod sealed {
    pub trait Sealed {}
}
/// Fixed core source guards only: providers remain replaceable through their
/// public application/scope contracts, but cannot fabricate the local fence gate.
pub trait RetirableOwner:
    sealed::Sealed + ProposalAdmission + BoundedReadableStateMachine + CheckpointStateMachine
where
    Self::Receipt: ApplicationReceipt,
{
    fn retirement_requirements(&self) -> ReadinessRequirements;
    fn retirement_source_status(&self) -> Result<Option<SourceFreezeStatus>, ApplicationError>;
    fn retirement_export(
        &self,
        target: GroupIdentity,
        max_bytes: usize,
    ) -> Result<crate::scope::ScopeImage, ApplicationError>;
    fn retirement_lineage(&self) -> Result<Vec<u8>, ApplicationError>;
    /// Lifetime bound selected before bootstrap, including ownership changes.
    /// Ordinary owners retain the original activation/parent-grant envelope.
    fn retirement_lineage_bound(&self) -> usize {
        MAX_RETIREMENT_LINEAGE_BYTES
    }
    fn validate_retirement_source(
        &self,
        status: &SourceFreezeStatus,
    ) -> Result<(), ApplicationError>;
    fn validate_retirement_lineage(
        &self,
        bytes: &[u8],
        fence_index: u64,
    ) -> Result<(), ApplicationError>;
    /// Validate the retained lineage against the exact frozen source as one
    /// evidence unit. Implementations with mutable owner grants bind both here.
    fn validate_retirement_evidence(
        &self,
        status: &SourceFreezeStatus,
        lineage: &[u8],
    ) -> Result<(), ApplicationError> {
        self.validate_retirement_source(status)?;
        self.validate_retirement_lineage(lineage, status.fence.index)
    }
}
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub struct RetirementStatus {
    pub source: GroupIdentity,
    pub operation: OperationId,
    pub fence_index: u64,
    pub index: u64,
    pub digest: ContentDigest,
    pub metadata_configuration: ConfigurationId,
    pub publication_operation: OperationId,
    pub publication_index: u64,
    pub retention_release: OperationId,
}
#[derive(Clone, Debug, Eq, PartialEq)]
pub enum RetirementOutcome<R> {
    Owner(R),
    Retired(RetirementStatus),
    Fenced,
}
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct RetirementReceipt<R> {
    pub index: u64,
    pub operation: OperationId,
    pub outcome: RetirementOutcome<R>,
}
impl<R: ApplicationReceipt> ApplicationReceipt for RetirementReceipt<R> {
    fn index(&self) -> u64 {
        self.index
    }
    fn operation(&self) -> OperationId {
        self.operation
    }
    fn nested_bytes(&self, limit: usize) -> Result<usize, ApplicationError> {
        match &self.outcome {
            RetirementOutcome::Owner(r) => r.nested_bytes(limit),
            _ => Ok(0),
        }
    }
}
#[derive(Clone, Debug, Eq, PartialEq)]
pub enum RetirementQuery<Q> {
    Owner(Q),
    Status,
    Freeze,
}
#[derive(Clone, Debug, Eq, PartialEq)]
#[allow(clippy::large_enum_variant)] // Inline layout and nested facts are charged.
pub enum RetirementRead<R> {
    Owner(R),
    Status(Option<RetirementStatus>),
    Freeze(Option<SourceFreezeStatus>),
    Retired,
}
#[derive(Clone)]
struct RetiredRecord {
    status: RetirementStatus,
    bytes: Vec<u8>,
    proof: RetirementProof,
    lineage: Vec<u8>,
}
/// Installed at construction, sharing the owner's existing authoritative log.
/// The immutable initial template contains configuration/initial state only.
#[derive(Clone)]
pub struct RetirementGuard<S> {
    initial: S,
    owner: Option<S>,
    anchor: ContentDigest,
    applied: u64,
    retired: Option<RetiredRecord>,
}
impl<S: RetirableOwner> RetirementGuard<S>
where
    S::Receipt: ApplicationReceipt,
{
    #[allow(clippy::result_large_err)]
    pub fn new(owner: S) -> Result<Self, (ApplicationError, S)> {
        if owner.applied_index() != 0 {
            return Err((ApplicationError::InvalidCommand, owner));
        }
        let checkpoint = match owner.checkpoint(owner.retirement_requirements().snapshot_bytes) {
            Ok(b) => b,
            Err(e) => return Err((e, owner)),
        };
        let anchor = ContentDigest::sha256(&checkpoint);
        Ok(Self {
            initial: owner.clone(),
            owner: Some(owner),
            anchor,
            applied: 0,
            retired: None,
        })
    }
    pub fn owner(&self) -> Option<&S> {
        self.owner.as_ref()
    }
    pub fn status(&self) -> Option<RetirementStatus> {
        self.retired.as_ref().map(|r| r.status)
    }
    /// Bounded original activation lineage, without provider/import payloads.
    pub fn retired_lineage(&self) -> Option<&[u8]> {
        self.retired.as_ref().map(|r| r.lineage.as_slice())
    }
    pub fn freeze_status(&self) -> Result<Option<SourceFreezeStatus>, ApplicationError> {
        match &self.retired {
            Some(r) => r.proof.source_status().map(Some),
            None => self
                .owner
                .as_ref()
                .ok_or(ApplicationError::NotApplied)?
                .retirement_source_status(),
        }
    }
    pub fn export_target(
        &self,
        target: GroupIdentity,
        max_bytes: usize,
    ) -> Result<crate::scope::ScopeImage, ApplicationError> {
        self.owner
            .as_ref()
            .ok_or(ApplicationError::NotApplied)?
            .retirement_export(target, max_bytes)
    }
    fn check_proof(&self, proof: &RetirementProof) -> Result<(), ApplicationError> {
        let status = self.freeze_status()?.ok_or(ApplicationError::NotApplied)?;
        if proof.source_status()? != status {
            return Err(ApplicationError::InvalidCommand);
        }
        Ok(())
    }
    pub fn retirement_command(
        &self,
        proof: &RetirementProof,
        max_bytes: usize,
    ) -> Result<Vec<u8>, ApplicationError> {
        self.check_proof(proof)?;
        let body = proof.encode(MAX_RETIREMENT_PROOF_BYTES)?;
        let len = 44 + body.len();
        if len > max_bytes {
            return Err(ApplicationError::InvalidCommand);
        }
        let mut out = Vec::with_capacity(len);
        out.extend(b"VBRETC01");
        out.extend(self.anchor.0);
        out.extend((body.len() as u32).to_le_bytes());
        out.extend(body);
        Ok(out)
    }
    fn request(&self, bytes: &[u8]) -> Result<RetirementProof, ApplicationError> {
        if bytes.len() > MAX_RETIREMENT_COMMAND_BYTES {
            return Err(ApplicationError::InvalidCommand);
        }
        let mut r = Reader::new(bytes);
        if r.take(8)? != b"VBRETC01" || r.take(32)? != self.anchor.0 {
            return Err(ApplicationError::InvalidCommand);
        }
        let len = r.u32()? as usize;
        let proof = RetirementProof::decode(r.take(len)?)?;
        if !r.done() {
            return Err(ApplicationError::InvalidCommand);
        }
        Ok(proof)
    }
    fn status_for(index: u64, bytes: &[u8], proof: &RetirementProof) -> RetirementStatus {
        RetirementStatus {
            source: proof.release.source,
            operation: proof.release.operation,
            fence_index: proof.release.fence_index,
            index,
            digest: ContentDigest::sha256(bytes),
            metadata_configuration: proof.metadata_configuration,
            publication_operation: proof.decision.publication_operation,
            publication_index: proof.decision.index,
            retention_release: proof.release.release,
        }
    }
    pub fn readiness_requirements(&self) -> ReadinessRequirements {
        let inner = self.initial.retirement_requirements();
        ReadinessRequirements {
            application_schema: RETIREMENT_GUARD_SCHEMA,
            command_bytes: inner.command_bytes.max(MAX_RETIREMENT_COMMAND_BYTES),
            snapshot_bytes: 80
                + inner.snapshot_bytes.max(
                    MAX_RETIREMENT_COMMAND_BYTES + self.initial.retirement_lineage_bound() + 24,
                ),
        }
    }
}
impl<S: RetirableOwner> StateMachine for RetirementGuard<S>
where
    S::Receipt: ApplicationReceipt,
{
    type Receipt = RetirementReceipt<S::Receipt>;
    fn validate_group(&self, group: GroupIdentity) -> Result<(), ApplicationError> {
        self.initial.validate_group(group)
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
                .filter(|entry| matches!(entry.payload, EntryPayload::Command { .. }))
                .count(),
        );
        for entry in entries {
            if next.applied.checked_add(1) != Some(entry.index) {
                return Err(ApplicationError::IndexGap);
            }
            if let EntryPayload::Command { operation, bytes } = &entry.payload {
                if let Some(record) = &next.retired {
                    let outcome = if *operation == record.status.operation && *bytes == record.bytes
                    {
                        RetirementOutcome::Retired(record.status)
                    } else {
                        RetirementOutcome::Fenced
                    };
                    receipts.push(RetirementReceipt {
                        index: entry.index,
                        operation: *operation,
                        outcome,
                    });
                } else if bytes.starts_with(b"VBRETC01") {
                    let proof = next.request(bytes)?;
                    next.check_proof(&proof)?;
                    if *operation != proof.release.operation
                        || entry.index <= proof.release.fence_index
                        || entry.index == u64::MAX
                    {
                        return Err(ApplicationError::InvalidCommand);
                    }
                    let lineage = next
                        .owner
                        .as_ref()
                        .ok_or(ApplicationError::NotApplied)?
                        .retirement_lineage()?;
                    if lineage.capacity() > next.initial.retirement_lineage_bound() {
                        return Err(ApplicationError::InvalidCommand);
                    }
                    next.initial
                        .validate_retirement_evidence(&proof.source_status()?, &lineage)?;
                    let status = Self::status_for(entry.index, bytes, &proof);
                    next.retired = Some(RetiredRecord {
                        status,
                        bytes: bytes.clone(),
                        proof,
                        lineage,
                    });
                    next.owner = None;
                    receipts.push(RetirementReceipt {
                        index: entry.index,
                        operation: *operation,
                        outcome: RetirementOutcome::Retired(status),
                    });
                } else {
                    let owner = next.owner.as_mut().ok_or(ApplicationError::NotApplied)?;
                    let mut result = owner.apply_batch(std::slice::from_ref(entry))?.into_iter();
                    let receipt = result.next().ok_or(ApplicationError::InvalidCommand)?;
                    if result.next().is_some()
                        || receipt.index() != entry.index
                        || receipt.operation() != *operation
                        || owner.applied_index() != entry.index
                    {
                        return Err(ApplicationError::InvalidCommand);
                    }
                    receipts.push(RetirementReceipt {
                        index: entry.index,
                        operation: *operation,
                        outcome: RetirementOutcome::Owner(receipt),
                    });
                }
            } else if let Some(owner) = &mut next.owner {
                if !owner.apply_batch(std::slice::from_ref(entry))?.is_empty()
                    || owner.applied_index() != entry.index
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
impl<S: RetirableOwner> BoundedStateMachine for RetirementGuard<S>
where
    S::Receipt: ApplicationReceipt,
{
    fn receipt_bytes_bound(&self, entries: &[LogEntry]) -> Result<usize, ApplicationError> {
        let mut next = self.clone();
        let mut bound = 0usize;
        for entry in entries {
            if let EntryPayload::Command { bytes, .. } = &entry.payload {
                let nested = if next.retired.is_some() || bytes.starts_with(b"VBRETC01") {
                    0
                } else {
                    next.owner
                        .as_ref()
                        .ok_or(ApplicationError::NotApplied)?
                        .receipt_bytes_bound(std::slice::from_ref(entry))?
                        .checked_sub(size_of::<S::Receipt>())
                        .ok_or(ApplicationError::ReceiptBudget)?
                };
                bound = bound
                    .checked_add(size_of::<Self::Receipt>())
                    .and_then(|b| b.checked_add(nested))
                    .ok_or(ApplicationError::ReceiptBudget)?;
            }
            next.apply_batch(std::slice::from_ref(entry))?;
        }
        Ok(bound)
    }
}
impl<S: RetirableOwner> ProposalAdmission for RetirementGuard<S>
where
    S::Receipt: ApplicationReceipt,
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
                .applied
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
        if let Some(r) = &next.retired {
            if operation != r.status.operation || bytes != r.bytes {
                return Err(ApplicationError::InvalidCommand);
            }
        } else if !bytes.starts_with(b"VBRETC01") {
            next.owner
                .as_ref()
                .ok_or(ApplicationError::NotApplied)?
                .validate_proposal(operation, bytes, std::iter::empty())?;
        }
        let index = next
            .applied
            .checked_add(1)
            .ok_or(ApplicationError::IndexGap)?;
        let entry = LogEntry {
            index,
            term: 1,
            payload: EntryPayload::Command {
                operation,
                bytes: bytes.to_vec(),
            },
        };
        next.receipt_bytes_bound(&[entry])
    }
}
impl<S: RetirableOwner> CheckpointStateMachine for RetirementGuard<S>
where
    S::Receipt: ApplicationReceipt,
{
    fn schema_version(&self) -> u64 {
        RETIREMENT_GUARD_SCHEMA
    }
    fn checkpoint(&self, max_bytes: usize) -> Result<Vec<u8>, ApplicationError> {
        let (inner_schema, body, lineage, index) = if let Some(r) = &self.retired {
            (0, r.bytes.clone(), r.lineage.as_slice(), r.status.index)
        } else {
            let owner = self
                .owner
                .as_ref()
                .ok_or(ApplicationError::InvalidCheckpoint)?;
            (
                owner.schema_version(),
                owner.checkpoint(self.initial.retirement_requirements().snapshot_bytes)?,
                &[][..],
                0,
            )
        };
        let len = 72 + body.len() + lineage.len();
        if len > max_bytes {
            return Err(ApplicationError::InvalidCheckpoint);
        }
        let mut out = Vec::with_capacity(len);
        out.extend(b"VBRET001");
        out.extend(self.applied.to_le_bytes());
        out.extend(self.anchor.0);
        out.extend(inner_schema.to_le_bytes());
        out.extend(index.to_le_bytes());
        out.extend((body.len() as u32).to_le_bytes());
        out.extend(body);
        out.extend((lineage.len() as u32).to_le_bytes());
        out.extend(lineage);
        Ok(out)
    }
    fn restore_checkpoint(
        &mut self,
        schema: u64,
        applied: u64,
        bytes: &[u8],
    ) -> Result<(), ApplicationError> {
        if schema != RETIREMENT_GUARD_SCHEMA {
            return Err(ApplicationError::UnsupportedSchema);
        }
        if bytes.len() > self.readiness_requirements().snapshot_bytes {
            return Err(ApplicationError::InvalidCheckpoint);
        }
        let mut r = Reader::new(bytes);
        if r.take(8)? != b"VBRET001" || r.u64()? != applied || r.take(32)? != self.anchor.0 {
            return Err(ApplicationError::InvalidCheckpoint);
        }
        let inner_schema = r.u64()?;
        let index = r.u64()?;
        let len = r.u32()? as usize;
        let body = r.take(len)?;
        let len = r.u32()? as usize;
        if len > self.initial.retirement_lineage_bound() {
            return Err(ApplicationError::InvalidCheckpoint);
        }
        let lineage = r.take(len)?;
        if !r.done() {
            return Err(ApplicationError::InvalidCheckpoint);
        }
        let mut next = self.clone();
        if index == 0 {
            if !lineage.is_empty() || inner_schema == 0 {
                return Err(ApplicationError::InvalidCheckpoint);
            }
            let mut owner = next.initial.clone();
            owner.restore_checkpoint(inner_schema, applied, body)?;
            if owner.applied_index() != applied {
                return Err(ApplicationError::InvalidCheckpoint);
            }
            next.owner = Some(owner);
            next.retired = None;
        } else {
            let proof = next.request(body)?;
            if inner_schema != 0
                || index > applied
                || index <= proof.release.fence_index
                || index == u64::MAX
            {
                return Err(ApplicationError::InvalidCheckpoint);
            }
            next.initial.validate_group(proof.release.source)?;
            next.initial
                .validate_retirement_evidence(&proof.source_status()?, lineage)?;
            let status = Self::status_for(index, body, &proof);
            next.retired = Some(RetiredRecord {
                status,
                bytes: body.to_vec(),
                proof,
                lineage: lineage.to_vec(),
            });
            next.owner = None;
        }
        next.applied = applied;
        *self = next;
        Ok(())
    }
}
impl<S: RetirableOwner> ReadableStateMachine for RetirementGuard<S>
where
    S::Receipt: ApplicationReceipt,
{
    type Query = RetirementQuery<S::Query>;
    type ReadResult = RetirementRead<S::ReadResult>;
    fn read_at(
        &self,
        required: u64,
        query: Self::Query,
    ) -> Result<Self::ReadResult, ApplicationError> {
        if required > self.applied {
            return Err(ApplicationError::NotApplied);
        }
        match query {
            RetirementQuery::Status => Ok(RetirementRead::Status(self.status())),
            RetirementQuery::Freeze => self.freeze_status().map(RetirementRead::Freeze),
            RetirementQuery::Owner(q) => match &self.owner {
                Some(o) => o.read_at(required, q).map(RetirementRead::Owner),
                None => Ok(RetirementRead::Retired),
            },
        }
    }
}
impl<S: RetirableOwner> BoundedReadableStateMachine for RetirementGuard<S>
where
    S::Receipt: ApplicationReceipt,
{
    fn query_bytes(&self, query: &Self::Query, limit: usize) -> Result<usize, ApplicationError> {
        match query {
            RetirementQuery::Owner(q) => self
                .owner
                .as_ref()
                .unwrap_or(&self.initial)
                .query_bytes(q, limit),
            _ => Ok(0),
        }
    }
    fn read_result_bound(&self, query: &Self::Query) -> Result<usize, ApplicationError> {
        let nested = match query {
            RetirementQuery::Owner(q) => {
                if let Some(o) = &self.owner {
                    o.read_result_bound(q)?
                        .checked_sub(size_of::<S::ReadResult>())
                        .ok_or(ApplicationError::ReceiptBudget)?
                } else {
                    0
                }
            }
            RetirementQuery::Freeze => self.freeze_status()?.as_ref().map_or(0, freeze_bytes),
            RetirementQuery::Status => 0,
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
            RetirementRead::Owner(r) => self
                .owner
                .as_ref()
                .unwrap_or(&self.initial)
                .read_result_bytes(r, limit)?,
            RetirementRead::Freeze(s) => s.as_ref().map_or(0, freeze_bytes),
            _ => 0,
        };
        if bytes > limit {
            return Err(ApplicationError::ReceiptBudget);
        }
        Ok(bytes)
    }
}
fn freeze_bytes(status: &SourceFreezeStatus) -> usize {
    status.intent.retained_bytes() - size_of::<TransferIntent>()
        + status.exports.capacity() * size_of::<SourceExportCommitment>()
}
