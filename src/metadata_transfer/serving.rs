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
//! Activation of a verified metadata import with a separately replayed writable base.
use super::publication::{put_publication, read_publication};
use super::*;

pub const METADATA_SERVING_SCHEMA: u64 = 2;
pub const REPEATED_METADATA_SERVING_SCHEMA: u64 = 4;
const ACTIVATION_BYTES: usize = 288;
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub struct MetadataActivationStatus {
    pub index: u64,
    pub publication: MetadataPublicationStatus,
}
pub const METADATA_ACTIVATION_STATUS_BYTES: usize = 256;
impl MetadataActivationStatus {
    /// Canonical observation encoding; callers authenticate the originating quorum.
    pub fn encode(&self) -> Result<Vec<u8>, ApplicationError> {
        self.validate_indices()?;
        let mut b = Vec::with_capacity(METADATA_ACTIVATION_STATUS_BYTES);
        b.extend(self.index.to_le_bytes());
        put_publication(&mut b, self.publication);
        Ok(b)
    }
    pub fn decode(bytes: &[u8]) -> Result<Self, ApplicationError> {
        let mut r = Reader::new(bytes);
        let result = Self {
            index: r.u64()?,
            publication: read_publication(&mut r)?,
        };
        if !r.done() {
            return Err(ApplicationError::InvalidCommand);
        }
        result.validate_indices()?;
        Ok(result)
    }
    fn validate_indices(&self) -> Result<(), ApplicationError> {
        let p = self.publication;
        let i = p.imported;
        if i.source.index == 0
            || i.index == 0
            || i.source.directory_schema == 0
            || i.source.image_bytes == 0
            || i.source.image_bytes > MAX_METADATA_IMAGE_BYTES
            || p.index <= i.source.index
            || self.index <= i.index
            || [i.source.index, i.index, p.index, self.index].contains(&u64::MAX)
        {
            return Err(ApplicationError::InvalidCommand);
        }
        Ok(())
    }
}
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub struct MetadataServingStatus {
    pub target: MetadataTargetStatus,
    pub activation: Option<MetadataActivationStatus>,
}
#[derive(Clone, Debug, Eq, PartialEq)]
#[allow(clippy::large_enum_variant)]
pub enum MetadataServingOutcome {
    Target(MetadataTargetOutcome),
    Activated(MetadataActivationStatus),
    Directory(DirectoryReceipt),
    Historical {
        source: GroupIdentity,
        index: u64,
        outcome: DirectoryOutcome,
    },
    Conflict,
    NotActive,
}
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct MetadataServingReceipt {
    pub index: u64,
    pub operation: OperationId,
    pub outcome: MetadataServingOutcome,
}
impl ApplicationReceipt for MetadataServingReceipt {
    fn index(&self) -> u64 {
        self.index
    }
    fn operation(&self) -> OperationId {
        self.operation
    }
    fn nested_bytes(&self, _: usize) -> Result<usize, ApplicationError> {
        Ok(0)
    }
}
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum MetadataServingQuery {
    Status,
    Creation(GroupIdentity),
    Directory(DirectoryQuery),
    Historical(DirectoryQuery),
}
#[derive(Clone, Debug, Eq, PartialEq)]
#[allow(clippy::large_enum_variant)]
pub enum MetadataServingRead {
    Status(MetadataServingStatus),
    Creation(Option<MetadataCreationRead>),
    Directory(DirectoryRead),
    Historical {
        source: GroupIdentity,
        through: u64,
        value: DirectoryRead,
    },
    NotActive,
}
/// Bounded creation observation. Authority identifies the original index domain.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct MetadataCreationRead {
    pub authority: GroupIdentity,
    pub operation: OperationId,
    pub index: u64,
    pub intent: Vec<u8>,
}
impl MetadataCreationRead {
    pub fn decode(&self) -> Result<GroupCreationStatus, ApplicationError> {
        Ok(GroupCreationStatus {
            operation: self.operation,
            index: self.index,
            intent: GroupCreationIntent::decode(&self.intent)?,
        })
    }
}
#[derive(Clone)]
pub(super) struct Active {
    pub(super) status: MetadataActivationStatus,
    command: Vec<u8>,
    pub(super) directory: Directory,
    pub(super) tail: BTreeMap<OperationId, LogEntry>,
}
/// Owns the existing target application and, after activation, a derived
/// writable directory. The historical image is never overwritten or relabelled.
#[derive(Clone)]
pub struct MetadataServingTarget {
    pub(super) target: MetadataAuthorityTarget,
    source_profile: ContentDigest,
    target_configuration: ConfigurationId,
    profile: ContentDigest,
    pub(super) active: Option<Active>,
    directory_bound: ReadinessRequirements,
}
impl MetadataServingTarget {
    pub fn new(
        source: MetadataPublishingSource,
        plan: MetadataMovePlan,
        operation: OperationId,
        source_configuration: ConfigurationId,
        target_configuration: ConfigurationId,
    ) -> Result<Self, ApplicationError> {
        if source.applied_index() != 0 {
            return Err(ApplicationError::InvalidCommand);
        }
        let source_profile = source.profile();
        let directory_bound = source.source().initial.directory().readiness_requirements();
        let target = MetadataAuthorityTarget::new(
            source.source().clone(),
            plan,
            operation,
            source_configuration,
        )?;
        let mut b = b"VBMTA002".to_vec();
        b.extend(source_profile.0);
        b.extend(target.bootstrap_command(40)?);
        b.extend(target_configuration.get().to_le_bytes());
        Ok(Self {
            target,
            source_profile,
            target_configuration,
            profile: ContentDigest::sha256(&b),
            active: None,
            directory_bound,
        })
    }
    pub fn status(&self) -> MetadataServingStatus {
        MetadataServingStatus {
            target: self.target.status(),
            activation: self.active.as_ref().map(|a| a.status),
        }
    }
    /// Historical inspection never grants current directory authority.
    pub fn historical_directory(&self) -> Option<&LifecycleDirectory> {
        self.target.historical_directory()
    }
    pub fn imported_image(&self) -> Option<&MetadataImage> {
        self.target.imported_image()
    }
    pub fn bootstrap_command(&self, maximum: usize) -> Result<Vec<u8>, ApplicationError> {
        if maximum < 40 {
            return Err(ApplicationError::InvalidCommand);
        }
        let mut b = b"VBMTAB02".to_vec();
        b.extend(self.profile.0);
        Ok(b)
    }
    pub fn import_command(
        &self,
        image: &MetadataImage,
        configuration: ConfigurationId,
        maximum: usize,
    ) -> Result<Vec<u8>, ApplicationError> {
        self.target.import_command(image, configuration, maximum)
    }
    fn check_publication(&self, p: MetadataPublicationStatus) -> Result<(), ApplicationError> {
        if self.target.status().imported != Some(p.imported)
            || p.source_profile != self.source_profile
            || p.target_configuration != self.target_configuration
            || p.index <= p.imported.source.index
        {
            return Err(ApplicationError::InvalidCommand);
        }
        Ok(())
    }
    /// The caller authenticates this publication against its original source
    /// quorum. The encoded value is a checked observation, not a certificate.
    pub fn activation_command(
        &self,
        publication: MetadataPublicationStatus,
        maximum: usize,
    ) -> Result<Vec<u8>, ApplicationError> {
        self.check_publication(publication)?;
        if maximum < ACTIVATION_BYTES {
            return Err(ApplicationError::InvalidCommand);
        }
        let mut b = b"VBMTAA02".to_vec();
        b.extend(self.profile.0);
        put_publication(&mut b, publication);
        Ok(b)
    }
    fn parse_activation(&self, bytes: &[u8], index: u64) -> Result<Active, ApplicationError> {
        let mut r = Reader::new(bytes);
        if r.take(8)? != b"VBMTAA02" || r.take(32)? != self.profile.0 {
            return Err(ApplicationError::InvalidCommand);
        }
        let publication = read_publication(&mut r)?;
        self.check_publication(publication)?;
        if !r.done() || index <= publication.imported.index {
            return Err(ApplicationError::InvalidCommand);
        }
        let image = self
            .target
            .imported_image()
            .ok_or(ApplicationError::NotApplied)?;
        let history = self.target.history().ok_or(ApplicationError::NotApplied)?;
        let directory = history.directory().metadata_base(image.plan(), index)?;
        Ok(Active {
            status: MetadataActivationStatus { index, publication },
            command: bytes.to_vec(),
            directory,
            tail: BTreeMap::new(),
        })
    }
    pub fn readiness_requirements(&self) -> ReadinessRequirements {
        let inner = self.target.readiness_requirements();
        ReadinessRequirements {
            application_schema: self.schema_version(),
            command_bytes: inner
                .command_bytes
                .max(ACTIVATION_BYTES)
                .max(self.directory_bound.command_bytes),
            snapshot_bytes: 60
                + inner.snapshot_bytes
                + ACTIVATION_BYTES
                + self.directory_bound.snapshot_bytes,
        }
    }
    fn inner_noop(&mut self, index: u64, term: u64) -> Result<(), ApplicationError> {
        self.target.apply_batch(&[LogEntry {
            index,
            term,
            payload: EntryPayload::Noop,
        }])?;
        Ok(())
    }
    fn step(
        &mut self,
        entry: &LogEntry,
    ) -> Result<Option<MetadataServingReceipt>, ApplicationError> {
        if self.applied_index().checked_add(1) != Some(entry.index) {
            return Err(ApplicationError::IndexGap);
        }
        let EntryPayload::Command { operation, bytes } = &entry.payload else {
            self.target.apply_batch(std::slice::from_ref(entry))?;
            if let Some(a) = &mut self.active {
                a.directory.metadata_advance(entry.index)?;
            }
            return Ok(None);
        };
        let outcome;
        if bytes.starts_with(b"VBMTAA02") {
            if *operation != self.target.status().operation {
                outcome = MetadataServingOutcome::Conflict;
            } else if let Some(a) = &self.active {
                outcome = if a.command == *bytes {
                    MetadataServingOutcome::Activated(a.status)
                } else {
                    MetadataServingOutcome::Conflict
                };
            } else {
                let active = self.parse_activation(bytes, entry.index)?;
                outcome = MetadataServingOutcome::Activated(active.status);
                self.active = Some(active);
            }
            self.inner_noop(entry.index, entry.term)?;
        } else if bytes.starts_with(b"VBMTAB02") || bytes.starts_with(b"VBMATI01") {
            let mut forwarded = entry.clone();
            if bytes.starts_with(b"VBMTAB02") {
                if *bytes != self.bootstrap_command(40)? {
                    return Err(ApplicationError::InvalidCommand);
                }
                forwarded.payload = EntryPayload::Command {
                    operation: *operation,
                    bytes: self.target.bootstrap_command(40)?,
                };
            }
            outcome = MetadataServingOutcome::Target(
                self.target.apply_batch(&[forwarded])?.remove(0).outcome,
            );
        } else if self.active.is_none() {
            outcome = MetadataServingOutcome::NotActive;
            self.inner_noop(entry.index, entry.term)?;
        } else {
            let inherited = self
                .target
                .history()
                .unwrap()
                .historical_outcome(*operation, bytes);
            if self.reserved_operation(*operation) {
                outcome = MetadataServingOutcome::Conflict;
            } else if let Some((source, index, result)) = inherited {
                outcome = MetadataServingOutcome::Historical {
                    source,
                    index,
                    outcome: result,
                };
            } else {
                let active = self.active.as_mut().unwrap();
                // Bootstrap is solely the selected wrapper stage. It cannot be
                // replayed under a new operation to consume an inherited grant.
                if bytes.starts_with(b"VBDINI")
                    || bytes.starts_with(b"VBMAS")
                    || bytes.starts_with(b"VBMAT")
                {
                    return Err(ApplicationError::InvalidCommand);
                }
                let known = active.directory.contains_operation(*operation);
                let receipt = active
                    .directory
                    .apply_batch(std::slice::from_ref(entry))?
                    .remove(0);
                if !known {
                    active.tail.insert(*operation, entry.clone());
                }
                outcome = MetadataServingOutcome::Directory(receipt);
            }
            self.inner_noop(entry.index, entry.term)?;
        }
        if let Some(a) = &mut self.active {
            a.directory.metadata_advance(entry.index)?;
        }
        Ok(Some(MetadataServingReceipt {
            index: entry.index,
            operation: *operation,
            outcome,
        }))
    }
    pub(super) fn reserved_operation(&self, op: OperationId) -> bool {
        op == self.target.operation
            || self
                .imported_image()
                .is_some_and(|i| i.rejected_operations().iter().any(|(id, _)| *id == op))
            || self.target.history().is_some_and(|h| match h {
                AuthorityHistory::Directory(_) => false,
                AuthorityHistory::Serving(s) => s.reserved_operation(op),
            })
    }
    pub(super) fn historical_outcome(
        &self,
        op: OperationId,
        bytes: &[u8],
    ) -> Option<(GroupIdentity, u64, DirectoryOutcome)> {
        self.target
            .history()
            .and_then(|h| h.historical_outcome(op, bytes))
            .or_else(|| {
                self.active
                    .as_ref()?
                    .directory
                    .historical_outcome(op, bytes)
                    .map(|(index, outcome)| (self.target.plan.target(), index, outcome))
            })
    }
    /// Live local command builders, never a replacement for a quorum read.
    pub fn active_directory(&self) -> Option<&Directory> {
        self.active.as_ref().map(|a| &a.directory)
    }
    pub(super) fn query_view(
        &self,
        q: DirectoryQuery,
        explicit_history: bool,
    ) -> Result<(LifecycleDirectory, GroupIdentity, u64), ApplicationError> {
        let history = self.target.history().ok_or(ApplicationError::NotApplied)?;
        let inherited = query_operation(q).is_some_and(|op| history.contains_operation(op));
        if explicit_history || inherited {
            return history.query_view(q);
        }
        let active = self.active.as_ref().ok_or(ApplicationError::NotApplied)?;
        Ok((
            LifecycleDirectory::new(active.directory.clone()),
            self.target.plan.target(),
            self.applied_index(),
        ))
    }
}
fn query_operation(q: DirectoryQuery) -> Option<OperationId> {
    Some(match q {
        DirectoryQuery::Manifest(_) => return None,
        DirectoryQuery::MetadataLocator(op)
        | DirectoryQuery::Reparent(op)
        | DirectoryQuery::ReparentGuard(op)
        | DirectoryQuery::ReparentDecision(op)
        | DirectoryQuery::ReparentPublication(op)
        | DirectoryQuery::ReparentCompletion(op)
        | DirectoryQuery::ReparentCancellation(op)
        | DirectoryQuery::RetiredChildSlot(op)
        | DirectoryQuery::DeletionIntent(op)
        | DirectoryQuery::Deletion(op)
        | DirectoryQuery::Transfer(op)
        | DirectoryQuery::Publication(op)
        | DirectoryQuery::DelegationReservation(op)
        | DirectoryQuery::DelegationPublication(op)
        | DirectoryQuery::DelegationDecline(op)
        | DirectoryQuery::DelegationCancellation(op) => op,
    })
}
impl StateMachine for MetadataServingTarget {
    type Receipt = MetadataServingReceipt;
    fn validate_group(&self, group: GroupIdentity) -> Result<(), ApplicationError> {
        self.target.validate_group(group)
    }
    fn applied_index(&self) -> u64 {
        self.target.applied_index()
    }
    fn deployment_requirements(&self) -> Option<ReadinessRequirements> {
        Some(self.readiness_requirements())
    }
    fn apply_batch(
        &mut self,
        entries: &[LogEntry],
    ) -> Result<Vec<Self::Receipt>, ApplicationError> {
        let mut next = self.clone();
        let mut receipts = Vec::new();
        for e in entries {
            if let Some(r) = next.step(e)? {
                receipts.push(r);
            }
        }
        *self = next;
        Ok(receipts)
    }
}
impl BoundedStateMachine for MetadataServingTarget {
    fn receipt_bytes_bound(&self, entries: &[LogEntry]) -> Result<usize, ApplicationError> {
        entries
            .iter()
            .filter(|e| matches!(e.payload, EntryPayload::Command { .. }))
            .count()
            .checked_mul(size_of::<Self::Receipt>())
            .ok_or(ApplicationError::ReceiptBudget)
    }
}
impl ProposalAdmission for MetadataServingTarget {
    fn validate_proposal<'a>(
        &self,
        operation: OperationId,
        bytes: &[u8],
        pending: impl Iterator<Item = (OperationId, &'a [u8])>,
    ) -> Result<usize, ApplicationError> {
        let mut next = self.clone();
        let mut count = 0;
        let mut admit = |op, b: &[u8]| -> Result<(), ApplicationError> {
            let i = count;
            count += 1;
            if i >= MAX_DIRECTORY_PENDING || b.len() > self.readiness_requirements().command_bytes {
                return Err(ApplicationError::ReceiptBudget);
            }
            if let Some(a) = &next.active {
                if !b.starts_with(b"VBMT") && !next.target.history().unwrap().contains_operation(op)
                {
                    a.directory.validate_proposal(op, b, std::iter::empty())?;
                }
            }
            let index = next
                .applied_index()
                .checked_add(1)
                .ok_or(ApplicationError::IndexGap)?;
            let r = next
                .step(&LogEntry {
                    index,
                    term: 1,
                    payload: EntryPayload::Command {
                        operation: op,
                        bytes: b.to_vec(),
                    },
                })?
                .unwrap();
            if matches!(
                r.outcome,
                MetadataServingOutcome::Conflict
                    | MetadataServingOutcome::NotActive
                    | MetadataServingOutcome::Historical {
                        outcome: DirectoryOutcome::OperationConflict,
                        ..
                    }
                    | MetadataServingOutcome::Target(
                        MetadataTargetOutcome::Conflict | MetadataTargetOutcome::NotActive
                    )
            ) {
                return Err(ApplicationError::InvalidCommand);
            }
            Ok(())
        };
        for (op, b) in pending {
            admit(op, b)?;
        }
        admit(operation, bytes)?;
        Ok(size_of::<Self::Receipt>())
    }
}
impl CheckpointStateMachine for MetadataServingTarget {
    fn schema_version(&self) -> u64 {
        if self.target.source.initial.depth() == 0 {
            METADATA_SERVING_SCHEMA
        } else {
            REPEATED_METADATA_SERVING_SCHEMA
        }
    }
    fn checkpoint(&self, maximum: usize) -> Result<Vec<u8>, ApplicationError> {
        let inner = self
            .target
            .checkpoint(self.target.readiness_requirements().snapshot_bytes)?;
        let command = self
            .active
            .as_ref()
            .map_or(&[][..], |a| a.command.as_slice());
        let len = 60
            + inner.len()
            + command.len()
            + self.active.as_ref().map_or(0, |a| {
                a.tail
                    .values()
                    .map(|e| {
                        let EntryPayload::Command { bytes, .. } = &e.payload else {
                            unreachable!()
                        };
                        28 + bytes.len()
                    })
                    .sum::<usize>()
            });
        if len > maximum.min(self.readiness_requirements().snapshot_bytes) {
            return Err(ApplicationError::InvalidCheckpoint);
        }
        let mut b = b"VBMTA002".to_vec();
        b.extend(self.profile.0);
        b.extend((inner.len() as u32).to_le_bytes());
        b.extend(inner);
        b.extend(
            self.active
                .as_ref()
                .map_or(0, |a| a.status.index)
                .to_le_bytes(),
        );
        b.extend((command.len() as u32).to_le_bytes());
        b.extend(command);
        b.extend((self.active.as_ref().map_or(0, |a| a.tail.len()) as u32).to_le_bytes());
        if let Some(a) = &self.active {
            let mut records: Vec<_> = a.tail.values().collect();
            records.sort_by_key(|e| e.index);
            for e in records {
                let EntryPayload::Command { operation, bytes } = &e.payload else {
                    unreachable!()
                };
                b.extend(e.index.to_le_bytes());
                b.extend(operation.get().to_le_bytes());
                b.extend((bytes.len() as u32).to_le_bytes());
                b.extend(bytes);
            }
        }
        Ok(b)
    }
    fn restore_checkpoint(
        &mut self,
        schema: u64,
        applied: u64,
        bytes: &[u8],
    ) -> Result<(), ApplicationError> {
        if schema != self.schema_version() {
            return Err(ApplicationError::UnsupportedSchema);
        }
        if bytes.len() > self.readiness_requirements().snapshot_bytes {
            return Err(ApplicationError::InvalidCheckpoint);
        }
        let mut r = Reader::new(bytes);
        if r.take(8)? != b"VBMTA002" || r.take(32)? != self.profile.0 {
            return Err(ApplicationError::InvalidCheckpoint);
        }
        let len = r.u32()? as usize;
        if len > self.target.readiness_requirements().snapshot_bytes {
            return Err(ApplicationError::InvalidCheckpoint);
        }
        let body = r.take(len)?;
        let index = r.u64()?;
        let len = r.u32()? as usize;
        if len > ACTIVATION_BYTES {
            return Err(ApplicationError::InvalidCheckpoint);
        }
        let command = r.take(len)?;
        let count = r.u32()? as usize;
        if index > applied
            || (index == 0 && (!command.is_empty() || count != 0))
            || count > 4 * MAX_DIRECTORY_OPERATIONS
        {
            return Err(ApplicationError::InvalidCheckpoint);
        }
        let mut next = self.clone();
        next.active = None;
        next.target
            .restore_checkpoint(next.target.schema_version(), applied, body)?;
        if index != 0 {
            let mut active = next.parse_activation(command, index)?;
            let history = next.target.history().unwrap();
            let mut previous = index;
            for _ in 0..count {
                let at = r.u64()?;
                let operation = r.operation()?;
                let len = r.u32()? as usize;
                if at <= previous
                    || at > applied
                    || len > self.directory_bound.command_bytes
                    || next.reserved_operation(operation)
                    || history.contains_operation(operation)
                    || active.tail.contains_key(&operation)
                {
                    return Err(ApplicationError::InvalidCheckpoint);
                }
                let bytes = r.take(len)?.to_vec();
                if bytes.starts_with(b"VBDINI")
                    || bytes.starts_with(b"VBMAS")
                    || bytes.starts_with(b"VBMAT")
                {
                    return Err(ApplicationError::InvalidCheckpoint);
                }
                active.directory.metadata_advance(at - 1)?;
                let e = LogEntry {
                    index: at,
                    term: 1,
                    payload: EntryPayload::Command { operation, bytes },
                };
                active.directory.apply_batch(std::slice::from_ref(&e))?;
                active.tail.insert(operation, e);
                previous = at;
            }
            active.directory.metadata_advance(applied)?;
            next.active = Some(active);
        }
        if !r.done() {
            return Err(ApplicationError::InvalidCheckpoint);
        }
        *self = next;
        Ok(())
    }
}
impl ReadableStateMachine for MetadataServingTarget {
    type Query = MetadataServingQuery;
    type ReadResult = MetadataServingRead;
    fn read_at(
        &self,
        required: u64,
        query: Self::Query,
    ) -> Result<Self::ReadResult, ApplicationError> {
        if required > self.applied_index() {
            return Err(ApplicationError::NotApplied);
        }
        let (q, explicit_history) = match query {
            MetadataServingQuery::Status => return Ok(MetadataServingRead::Status(self.status())),
            MetadataServingQuery::Creation(group) => {
                let Some(active) = &self.active else {
                    return Ok(MetadataServingRead::NotActive);
                };
                let status = active
                    .directory
                    .group_creation_at(active.directory.applied_index(), group)?;
                let status = status
                    .map(|s| {
                        let authority = self
                            .target
                            .history()
                            .unwrap()
                            .operation_authority(s.operation)
                            .unwrap_or(self.target.plan.target());
                        Ok(MetadataCreationRead {
                            authority,
                            operation: s.operation,
                            index: s.index,
                            intent: s.intent.encode(MAX_GROUP_CREATION_BYTES)?,
                        })
                    })
                    .transpose()?;
                return Ok(MetadataServingRead::Creation(status));
            }
            MetadataServingQuery::Directory(q) => {
                if self.active.is_none() {
                    return Ok(MetadataServingRead::NotActive);
                }
                (q, false)
            }
            MetadataServingQuery::Historical(q) => (q, true),
        };
        let (view, source, through) = self.query_view(q, explicit_history)?;
        let value = view.read_at(view.applied_index(), q)?;
        Ok(if source != self.target.plan.target() {
            MetadataServingRead::Historical {
                source,
                through,
                value,
            }
        } else {
            MetadataServingRead::Directory(value)
        })
    }
}
impl BoundedReadableStateMachine for MetadataServingTarget {
    fn query_bytes(&self, _: &Self::Query, _: usize) -> Result<usize, ApplicationError> {
        Ok(0)
    }
    fn read_result_bound(&self, q: &Self::Query) -> Result<usize, ApplicationError> {
        let (q, old) = match q {
            MetadataServingQuery::Status => return Ok(size_of::<Self::ReadResult>()),
            MetadataServingQuery::Creation(_) => {
                return Ok(size_of::<Self::ReadResult>() + MAX_GROUP_CREATION_BYTES)
            }
            MetadataServingQuery::Directory(q) if self.active.is_none() => {
                let _ = q;
                return Ok(size_of::<Self::ReadResult>());
            }
            MetadataServingQuery::Directory(q) => (*q, false),
            MetadataServingQuery::Historical(q) => (*q, true),
        };
        let (view, _, _) = self.query_view(q, old)?;
        Ok(size_of::<Self::ReadResult>() + view.read_result_bound(&q)?)
    }
    fn read_result_bytes(
        &self,
        result: &Self::ReadResult,
        maximum: usize,
    ) -> Result<usize, ApplicationError> {
        match result {
            MetadataServingRead::Creation(Some(s)) => {
                if s.intent.capacity() > maximum {
                    return Err(ApplicationError::ReceiptBudget);
                }
                Ok(s.intent.capacity())
            }
            MetadataServingRead::Directory(value)
            | MetadataServingRead::Historical { value, .. } => {
                // Nested sizing is independent of the directory's current state.
                let view = self
                    .historical_directory()
                    .ok_or(ApplicationError::NotApplied)?;
                view.read_result_bytes(value, maximum)
            }
            _ => Ok(0),
        }
    }
}
