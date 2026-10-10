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
//! Source publication after durable, non-serving target import.
use super::*;

pub const METADATA_PUBLISHING_SCHEMA: u64 = 2;
pub const REPEATED_METADATA_PUBLISHING_SCHEMA: u64 = 4;
const PUBLICATION_BYTES: usize = 256;

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub struct MetadataPublicationStatus {
    pub index: u64,
    pub source_profile: ContentDigest,
    pub imported: MetadataImportStatus,
    pub target_configuration: ConfigurationId,
}
#[derive(Clone, Debug, Eq, PartialEq)]
#[allow(clippy::large_enum_variant)]
pub enum MetadataPublishingOutcome {
    Source(MetadataSourceOutcome),
    Published(MetadataPublicationStatus),
    Conflict,
}
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct MetadataPublishingReceipt {
    pub index: u64,
    pub operation: OperationId,
    pub outcome: MetadataPublishingOutcome,
}
impl ApplicationReceipt for MetadataPublishingReceipt {
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
#[derive(Clone, Debug, Eq, PartialEq)]
pub enum MetadataPublishingQuery {
    Source(MetadataSourceQuery),
    Publication,
}
#[derive(Clone, Debug, Eq, PartialEq)]
#[allow(clippy::large_enum_variant)]
pub enum MetadataPublishingRead {
    Source(MetadataSourceRead),
    Publication(Option<MetadataPublicationStatus>),
}
/// Select before bootstrap. Publication never unfreezes the source directory.
#[derive(Clone)]
pub struct MetadataPublishingSource {
    inner: MetadataAuthoritySource,
    profile: ContentDigest,
    publication: Option<(MetadataPublicationStatus, Vec<u8>)>,
}
impl MetadataPublishingSource {
    pub fn new(source: MetadataAuthoritySource) -> Result<Self, ApplicationError> {
        if source.applied_index() != 0 {
            return Err(ApplicationError::InvalidCommand);
        }
        let mut bytes = b"VBMPS002".to_vec();
        bytes.extend(source.bootstrap_command(source.readiness_requirements().command_bytes)?);
        Ok(Self {
            inner: source,
            profile: ContentDigest::sha256(&bytes),
            publication: None,
        })
    }
    pub fn source(&self) -> &MetadataAuthoritySource {
        &self.inner
    }
    pub fn profile(&self) -> ContentDigest {
        self.profile
    }
    pub fn publication(&self) -> Option<MetadataPublicationStatus> {
        self.publication.as_ref().map(|p| p.0)
    }
    pub fn bootstrap_command(&self, maximum: usize) -> Result<Vec<u8>, ApplicationError> {
        if maximum < 40 {
            return Err(ApplicationError::InvalidCommand);
        }
        let mut b = b"VBMPSB02".to_vec();
        b.extend(self.profile.0);
        Ok(b)
    }
    /// The host must authenticate the imported status against its original
    /// destination quorum and source configuration before proposing publication.
    pub fn publication_command(
        &self,
        imported: MetadataImportStatus,
        target_configuration: ConfigurationId,
        maximum: usize,
    ) -> Result<Vec<u8>, ApplicationError> {
        self.check_import(imported)?;
        let mut b = b"VBMPSP02".to_vec();
        b.extend(self.profile.0);
        put_import(&mut b, imported);
        b.extend(target_configuration.get().to_le_bytes());
        if b.len() > maximum {
            return Err(ApplicationError::InvalidCommand);
        }
        Ok(b)
    }
    fn check_import(&self, imported: MetadataImportStatus) -> Result<(), ApplicationError> {
        if imported.index == 0
            || imported.index == u64::MAX
            || self.inner.status() != Some(imported.source)
        {
            return Err(ApplicationError::InvalidCommand);
        }
        Ok(())
    }
    fn parse(
        &self,
        bytes: &[u8],
        index: u64,
    ) -> Result<MetadataPublicationStatus, ApplicationError> {
        let mut r = Reader::new(bytes);
        if r.take(8)? != b"VBMPSP02" || r.take(32)? != self.profile.0 {
            return Err(ApplicationError::InvalidCommand);
        }
        let imported = read_import(&mut r)?;
        let target_configuration =
            ConfigurationId::new(r.u64()?).ok_or(ApplicationError::InvalidCommand)?;
        if !r.done() || index <= imported.source.index {
            return Err(ApplicationError::InvalidCommand);
        }
        self.check_import(imported)?;
        Ok(MetadataPublicationStatus {
            index,
            source_profile: self.profile,
            imported,
            target_configuration,
        })
    }
    pub fn readiness_requirements(&self) -> ReadinessRequirements {
        let mut r = self.inner.readiness_requirements();
        r.application_schema = self.schema_version();
        r.command_bytes = r.command_bytes.max(PUBLICATION_BYTES);
        r.snapshot_bytes += 56 + PUBLICATION_BYTES;
        r
    }
    fn step(
        &mut self,
        entry: &LogEntry,
    ) -> Result<Option<MetadataPublishingReceipt>, ApplicationError> {
        if self.applied_index().checked_add(1) != Some(entry.index) {
            return Err(ApplicationError::IndexGap);
        }
        let EntryPayload::Command { operation, bytes } = &entry.payload else {
            self.inner.apply_batch(std::slice::from_ref(entry))?;
            return Ok(None);
        };
        let outcome = if bytes.starts_with(b"VBMPSP02") {
            let proposed = self.parse(bytes, entry.index)?;
            let outcome = if *operation != proposed.imported.source.operation {
                MetadataPublishingOutcome::Conflict
            } else if let Some((status, command)) = &self.publication {
                if command == bytes {
                    MetadataPublishingOutcome::Published(*status)
                } else {
                    MetadataPublishingOutcome::Conflict
                }
            } else {
                self.publication = Some((proposed, bytes.clone()));
                MetadataPublishingOutcome::Published(proposed)
            };
            self.inner.apply_batch(&[LogEntry {
                index: entry.index,
                term: entry.term,
                payload: EntryPayload::Noop,
            }])?;
            outcome
        } else {
            if bytes.starts_with(b"VBMASB01") {
                return Err(ApplicationError::InvalidCommand);
            }
            let mut forwarded = entry.clone();
            if bytes.starts_with(b"VBMPSB02") {
                if *bytes != self.bootstrap_command(40)? {
                    return Err(ApplicationError::InvalidCommand);
                }
                forwarded.payload = EntryPayload::Command {
                    operation: *operation,
                    bytes: self
                        .inner
                        .bootstrap_command(self.inner.readiness_requirements().command_bytes)?,
                };
            }
            let receipt = self.inner.apply_batch(&[forwarded])?.remove(0);
            MetadataPublishingOutcome::Source(receipt.outcome)
        };
        Ok(Some(MetadataPublishingReceipt {
            index: entry.index,
            operation: *operation,
            outcome,
        }))
    }
}
impl StateMachine for MetadataPublishingSource {
    type Receipt = MetadataPublishingReceipt;
    fn validate_group(&self, group: GroupIdentity) -> Result<(), ApplicationError> {
        self.inner.validate_group(group)
    }
    fn applied_index(&self) -> u64 {
        self.inner.applied_index()
    }
    fn deployment_requirements(&self) -> Option<ReadinessRequirements> {
        Some(self.readiness_requirements())
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
        for e in entries {
            if let Some(r) = next.step(e)? {
                receipts.push(r);
            }
        }
        *self = next;
        Ok(receipts)
    }
}
impl BoundedStateMachine for MetadataPublishingSource {
    fn receipt_bytes_bound(&self, entries: &[LogEntry]) -> Result<usize, ApplicationError> {
        entries
            .iter()
            .filter(|e| matches!(e.payload, EntryPayload::Command { .. }))
            .count()
            .checked_mul(size_of::<Self::Receipt>())
            .ok_or(ApplicationError::ReceiptBudget)
    }
}
impl ProposalAdmission for MetadataPublishingSource {
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
            // Inner admission performs the existing directory checks before the
            // wrapper forwards ordinary commands at this same local index.
            if !b.starts_with(b"VBMPS") {
                next.inner.validate_proposal(op, b, std::iter::empty())?;
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
            if match &r.outcome {
                MetadataPublishingOutcome::Conflict => true,
                MetadataPublishingOutcome::Source(source) => source.rejects_admission(),
                _ => false,
            } {
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
impl CheckpointStateMachine for MetadataPublishingSource {
    fn schema_version(&self) -> u64 {
        if self.inner.initial.depth() == 0 {
            METADATA_PUBLISHING_SCHEMA
        } else {
            REPEATED_METADATA_PUBLISHING_SCHEMA
        }
    }
    fn checkpoint(&self, maximum: usize) -> Result<Vec<u8>, ApplicationError> {
        let inner = self
            .inner
            .checkpoint(self.inner.readiness_requirements().snapshot_bytes)?;
        let command = self
            .publication
            .as_ref()
            .map_or(&[][..], |p| p.1.as_slice());
        if 56 + inner.len() + command.len() > maximum {
            return Err(ApplicationError::InvalidCheckpoint);
        }
        let mut b = b"VBMPS002".to_vec();
        b.extend(self.profile.0);
        b.extend(self.publication().map_or(0, |p| p.index).to_le_bytes());
        b.extend((inner.len() as u32).to_le_bytes());
        b.extend(inner);
        b.extend((command.len() as u32).to_le_bytes());
        b.extend(command);
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
        if r.take(8)? != b"VBMPS002" || r.take(32)? != self.profile.0 {
            return Err(ApplicationError::InvalidCheckpoint);
        }
        let index = r.u64()?;
        let len = r.u32()? as usize;
        if len > self.inner.readiness_requirements().snapshot_bytes {
            return Err(ApplicationError::InvalidCheckpoint);
        }
        let body = r.take(len)?;
        let len = r.u32()? as usize;
        if len > PUBLICATION_BYTES {
            return Err(ApplicationError::InvalidCheckpoint);
        }
        let command = r.take(len)?;
        if !r.done() || index > applied || (index == 0 && !command.is_empty()) {
            return Err(ApplicationError::InvalidCheckpoint);
        }
        let mut next = self.clone();
        next.publication = None;
        next.inner
            .restore_checkpoint(next.inner.schema_version(), applied, body)?;
        if index != 0 {
            next.publication = Some((next.parse(command, index)?, command.to_vec()));
        }
        *self = next;
        Ok(())
    }
}
impl ReadableStateMachine for MetadataPublishingSource {
    type Query = MetadataPublishingQuery;
    type ReadResult = MetadataPublishingRead;
    fn read_at(
        &self,
        required: u64,
        query: Self::Query,
    ) -> Result<Self::ReadResult, ApplicationError> {
        if required > self.applied_index() {
            return Err(ApplicationError::NotApplied);
        }
        Ok(match query {
            MetadataPublishingQuery::Publication => {
                MetadataPublishingRead::Publication(self.publication())
            }
            MetadataPublishingQuery::Source(q) => {
                MetadataPublishingRead::Source(self.inner.read_at(required, q)?)
            }
        })
    }
}
impl BoundedReadableStateMachine for MetadataPublishingSource {
    fn query_bytes(&self, q: &Self::Query, maximum: usize) -> Result<usize, ApplicationError> {
        match q {
            MetadataPublishingQuery::Source(q) => self.inner.query_bytes(q, maximum),
            _ => Ok(0),
        }
    }
    fn read_result_bound(&self, q: &Self::Query) -> Result<usize, ApplicationError> {
        Ok(size_of::<Self::ReadResult>()
            + match q {
                MetadataPublishingQuery::Source(q) => self.inner.read_result_bound(q)?,
                _ => 0,
            })
    }
    fn read_result_bytes(
        &self,
        result: &Self::ReadResult,
        maximum: usize,
    ) -> Result<usize, ApplicationError> {
        match result {
            MetadataPublishingRead::Source(r) => self.inner.read_result_bytes(r, maximum),
            _ => Ok(0),
        }
    }
}
pub(super) fn put_import(b: &mut Vec<u8>, i: MetadataImportStatus) {
    b.extend(i.index.to_le_bytes());
    b.extend(i.source_configuration.get().to_le_bytes());
    super::target::put_status(b, i.source);
}
pub(super) fn read_import(r: &mut Reader<'_>) -> Result<MetadataImportStatus, ApplicationError> {
    Ok(MetadataImportStatus {
        index: r.u64()?,
        source_configuration: ConfigurationId::new(r.u64()?)
            .ok_or(ApplicationError::InvalidCommand)?,
        source: super::target::read_status(r)?,
    })
}
pub(super) fn put_publication(b: &mut Vec<u8>, p: MetadataPublicationStatus) {
    b.extend(p.index.to_le_bytes());
    b.extend(p.source_profile.0);
    put_import(b, p.imported);
    b.extend(p.target_configuration.get().to_le_bytes());
}
pub(super) fn read_publication(
    r: &mut Reader<'_>,
) -> Result<MetadataPublicationStatus, ApplicationError> {
    Ok(MetadataPublicationStatus {
        index: r.u64()?,
        source_profile: ContentDigest(r.take(32)?.try_into().unwrap()),
        imported: read_import(r)?,
        target_configuration: ConfigurationId::new(r.u64()?)
            .ok_or(ApplicationError::InvalidCommand)?,
    })
}
