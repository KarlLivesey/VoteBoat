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
//! Non-serving target staging and verification of original-domain metadata.
use super::*;

pub const METADATA_TARGET_SCHEMA: u64 = 1;
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub struct MetadataImportStatus {
    /// Index in the destination group's log, distinct from source.index.
    pub index: u64,
    pub source_configuration: ConfigurationId,
    pub source: MetadataSourceStatus,
}
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub struct MetadataTargetStatus {
    pub target: GroupIdentity,
    pub operation: OperationId,
    pub plan_digest: ContentDigest,
    pub staged_index: Option<u64>,
    pub imported: Option<MetadataImportStatus>,
}
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
#[allow(clippy::large_enum_variant)] // Fixed inline receipt size is charged before apply.
pub enum MetadataTargetOutcome {
    Staged { index: u64 },
    Imported(MetadataImportStatus),
    Conflict,
    NotActive,
}
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub struct MetadataTargetReceipt {
    pub index: u64,
    pub operation: OperationId,
    pub outcome: MetadataTargetOutcome,
}
impl ApplicationReceipt for MetadataTargetReceipt {
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
pub enum MetadataTargetQuery {
    Status,
}
#[derive(Clone)]
struct Imported {
    status: MetadataImportStatus,
    image: MetadataImage,
    history: LifecycleDirectory,
    command: Vec<u8>,
}
/// A new metadata group with an immutable source profile and transfer binding.
/// This profile cannot serve or mutate directory state. Historical inspection
/// retains the source authority/index domain, never the destination's identity.
#[derive(Clone)]
pub struct MetadataAuthorityTarget {
    source: MetadataAuthoritySource,
    plan: MetadataMovePlan,
    operation: OperationId,
    source_configuration: ConfigurationId,
    anchor: ContentDigest,
    applied: u64,
    staged: Option<u64>,
    imported: Option<Imported>,
}
impl MetadataAuthorityTarget {
    pub fn new(
        source: MetadataAuthoritySource,
        plan: MetadataMovePlan,
        operation: OperationId,
        source_configuration: ConfigurationId,
    ) -> Result<Self, ApplicationError> {
        if source.applied_index() != 0
            || source.bootstrap.is_some()
            || source.frozen.is_some()
            || source.initial.directory().plan().authority() != plan.source()
        {
            return Err(ApplicationError::InvalidCommand);
        }
        let mut binding =
            source.bootstrap_command(source.readiness_requirements().command_bytes)?;
        binding.extend(plan.encode(MAX_METADATA_PLAN_BYTES)?);
        binding.extend(operation.get().to_le_bytes());
        binding.extend(source_configuration.get().to_le_bytes());
        Ok(Self {
            source,
            plan,
            operation,
            source_configuration,
            anchor: ContentDigest::sha256(&binding),
            applied: 0,
            staged: None,
            imported: None,
        })
    }
    pub fn status(&self) -> MetadataTargetStatus {
        MetadataTargetStatus {
            target: self.plan.target(),
            operation: self.operation,
            plan_digest: ContentDigest::sha256(
                &self
                    .plan
                    .encode(MAX_METADATA_PLAN_BYTES)
                    .expect("checked plan"),
            ),
            staged_index: self.staged,
            imported: self.imported.as_ref().map(|i| i.status),
        }
    }
    pub fn bootstrap_command(&self, maximum: usize) -> Result<Vec<u8>, ApplicationError> {
        if maximum < 40 {
            return Err(ApplicationError::InvalidCommand);
        }
        let mut bytes = Vec::with_capacity(40);
        bytes.extend(b"VBMATB01");
        bytes.extend(self.anchor.0);
        Ok(bytes)
    }
    /// This is original source history for inspection, not an active directory
    /// under the target group. Use the source identity and original indices.
    pub fn historical_directory(&self) -> Option<&LifecycleDirectory> {
        self.imported.as_ref().map(|i| &i.history)
    }
    pub fn imported_image(&self) -> Option<&MetadataImage> {
        self.imported.as_ref().map(|i| &i.image)
    }
    pub fn readiness_requirements(&self) -> ReadinessRequirements {
        ReadinessRequirements {
            application_schema: METADATA_TARGET_SCHEMA,
            command_bytes: 244
                + MAX_METADATA_PLAN_BYTES
                + self.source.export_bytes
                + 24 * self.source.initial.directory().limits().operations,
            snapshot_bytes: 68
                + 244
                + MAX_METADATA_PLAN_BYTES
                + self.source.export_bytes
                + 24 * self.source.initial.directory().limits().operations,
        }
    }
    fn verify(
        &self,
        image: &MetadataImage,
        configuration: ConfigurationId,
    ) -> Result<LifecycleDirectory, ApplicationError> {
        let status = image.status;
        if configuration != self.source_configuration
            || image.plan != self.plan
            || status.source != self.plan.source()
            || status.target != self.plan.target()
            || status.operation != self.operation
            || status.index == 0
            || status.index == u64::MAX
            || status.directory_schema != self.source.initial.schema_version()
            || status.image_bytes != image.bytes.len()
            || image.bytes.capacity() > self.source.export_bytes
            || image.bytes.len() > self.source.export_bytes
            || status.image_digest != ContentDigest::sha256(&image.bytes)
            || status.plan_digest != self.status().plan_digest
            || image.rejected.len() > self.source.initial.directory().limits().operations
            || image.rejected.capacity() > self.source.initial.directory().limits().operations
        {
            return Err(ApplicationError::InvalidCommand);
        }
        let mut history = self.source.initial.clone();
        history.restore_checkpoint(status.directory_schema, status.index, &image.bytes)?;
        let directory = history.directory();
        if !directory.has_bootstrap()
            || directory.contains_operation(self.operation)
            || directory.contains_command_at(status.index)
            || directory.authority_move_view()? != self.plan.manifests
        {
            return Err(ApplicationError::InvalidCommand);
        }
        let mut bytes = Vec::with_capacity(4 + 24 * image.rejected.len());
        bytes.extend((image.rejected.len() as u32).to_le_bytes());
        let mut previous = None;
        let mut indices = std::collections::BTreeSet::new();
        for (op, index) in &image.rejected {
            if *index == 0
                || *index >= status.index
                || *op == self.operation
                || directory.contains_operation(*op)
                || directory.contains_command_at(*index)
                || previous.is_some_and(|p| p >= *op)
                || !indices.insert(*index)
            {
                return Err(ApplicationError::InvalidCommand);
            }
            previous = Some(*op);
            bytes.extend(op.get().to_le_bytes());
            bytes.extend(index.to_le_bytes());
        }
        if status.rejected_digest != ContentDigest::sha256(&bytes) {
            return Err(ApplicationError::InvalidCommand);
        }
        Ok(history)
    }
    /// Encode a verified image. Source configuration is a construction-bound
    /// authenticated host observation, not a certificate created by this codec.
    pub fn import_command(
        &self,
        image: &MetadataImage,
        source_configuration: ConfigurationId,
        maximum: usize,
    ) -> Result<Vec<u8>, ApplicationError> {
        self.verify(image, source_configuration)?;
        let plan = image.plan.encode(MAX_METADATA_PLAN_BYTES)?;
        let len = 244 + plan.len() + image.bytes.len() + 24 * image.rejected.len();
        if len > maximum.min(self.readiness_requirements().command_bytes) {
            return Err(ApplicationError::InvalidCommand);
        }
        let mut bytes = Vec::with_capacity(len);
        bytes.extend(b"VBMATI01");
        bytes.extend(self.anchor.0);
        bytes.extend(source_configuration.get().to_le_bytes());
        put_status(&mut bytes, image.status);
        bytes.extend((plan.len() as u32).to_le_bytes());
        bytes.extend(plan);
        bytes.extend((image.bytes.len() as u32).to_le_bytes());
        bytes.extend(&image.bytes);
        bytes.extend((image.rejected.len() as u32).to_le_bytes());
        for (op, index) in &image.rejected {
            bytes.extend(op.get().to_le_bytes());
            bytes.extend(index.to_le_bytes());
        }
        Ok(bytes)
    }
    fn request(
        &self,
        bytes: &[u8],
    ) -> Result<(ConfigurationId, MetadataImage, LifecycleDirectory), ApplicationError> {
        if bytes.len() > self.readiness_requirements().command_bytes {
            return Err(ApplicationError::InvalidCommand);
        }
        let mut r = Reader::new(bytes);
        if r.take(8)? != b"VBMATI01" || r.take(32)? != self.anchor.0 {
            return Err(ApplicationError::InvalidCommand);
        }
        let configuration =
            ConfigurationId::new(r.u64()?).ok_or(ApplicationError::InvalidCommand)?;
        let status = read_status(&mut r)?;
        let len = r.u32()? as usize;
        if len > MAX_METADATA_PLAN_BYTES {
            return Err(ApplicationError::InvalidCommand);
        }
        let plan = MetadataMovePlan::decode(r.take(len)?)?;
        let len = r.u32()? as usize;
        if len > self.source.export_bytes {
            return Err(ApplicationError::InvalidCommand);
        }
        let payload = r.take(len)?.to_vec();
        let count = r.u32()? as usize;
        if count > self.source.initial.directory().limits().operations {
            return Err(ApplicationError::InvalidCommand);
        }
        let mut rejected = Vec::with_capacity(count);
        for _ in 0..count {
            rejected.push((r.operation()?, r.u64()?));
        }
        if !r.done() {
            return Err(ApplicationError::InvalidCommand);
        }
        let image = MetadataImage {
            status,
            plan,
            bytes: payload,
            rejected,
        };
        let history = self.verify(&image, configuration)?;
        Ok((configuration, image, history))
    }
    fn step(
        &mut self,
        entry: &LogEntry,
    ) -> Result<Option<MetadataTargetReceipt>, ApplicationError> {
        if self.applied.checked_add(1) != Some(entry.index) {
            return Err(ApplicationError::IndexGap);
        }
        let EntryPayload::Command { operation, bytes } = &entry.payload else {
            self.applied = entry.index;
            return Ok(None);
        };
        let outcome = if *operation != self.operation {
            MetadataTargetOutcome::Conflict
        } else if bytes.starts_with(b"VBMATB01") {
            if *bytes != self.bootstrap_command(40)? {
                return Err(ApplicationError::InvalidCommand);
            }
            let index = *self.staged.get_or_insert(entry.index);
            MetadataTargetOutcome::Staged { index }
        } else if bytes.starts_with(b"VBMATI01") {
            if self.staged.is_none() {
                MetadataTargetOutcome::NotActive
            } else if let Some(i) = &self.imported {
                if i.command == *bytes {
                    MetadataTargetOutcome::Imported(i.status)
                } else {
                    MetadataTargetOutcome::Conflict
                }
            } else {
                let (source_configuration, image, history) = self.request(bytes)?;
                let status = MetadataImportStatus {
                    index: entry.index,
                    source_configuration,
                    source: image.status,
                };
                self.imported = Some(Imported {
                    status,
                    image,
                    history,
                    command: bytes.clone(),
                });
                MetadataTargetOutcome::Imported(status)
            }
        } else {
            MetadataTargetOutcome::NotActive
        };
        self.applied = entry.index;
        Ok(Some(MetadataTargetReceipt {
            index: entry.index,
            operation: *operation,
            outcome,
        }))
    }
}
fn put_status(bytes: &mut Vec<u8>, s: MetadataSourceStatus) {
    put_group(bytes, s.source);
    put_group(bytes, s.target);
    bytes.extend(s.operation.get().to_le_bytes());
    bytes.extend(s.index.to_le_bytes());
    bytes.extend(s.directory_schema.to_le_bytes());
    bytes.extend(s.plan_digest.0);
    bytes.extend(s.image_digest.0);
    bytes.extend(s.rejected_digest.0);
    bytes.extend((s.image_bytes as u64).to_le_bytes());
}
fn read_status(r: &mut Reader<'_>) -> Result<MetadataSourceStatus, ApplicationError> {
    Ok(MetadataSourceStatus {
        source: r.group()?,
        target: r.group()?,
        operation: r.operation()?,
        index: r.u64()?,
        directory_schema: r.u64()?,
        plan_digest: ContentDigest(r.take(32)?.try_into().unwrap()),
        image_digest: ContentDigest(r.take(32)?.try_into().unwrap()),
        rejected_digest: ContentDigest(r.take(32)?.try_into().unwrap()),
        image_bytes: usize::try_from(r.u64()?).map_err(|_| ApplicationError::InvalidCommand)?,
    })
}
impl StateMachine for MetadataAuthorityTarget {
    type Receipt = MetadataTargetReceipt;
    fn validate_group(&self, g: GroupIdentity) -> Result<(), ApplicationError> {
        if g == self.plan.target() {
            Ok(())
        } else {
            Err(ApplicationError::InvalidCommand)
        }
    }
    fn deployment_requirements(&self) -> Option<ReadinessRequirements> {
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
        let mut receipts = Vec::new();
        for entry in entries {
            if let Some(receipt) = next.step(entry)? {
                receipts.push(receipt);
            }
        }
        *self = next;
        Ok(receipts)
    }
}
impl BoundedStateMachine for MetadataAuthorityTarget {
    fn receipt_bytes_bound(&self, entries: &[LogEntry]) -> Result<usize, ApplicationError> {
        entries
            .iter()
            .filter(|e| matches!(e.payload, EntryPayload::Command { .. }))
            .count()
            .checked_mul(size_of::<MetadataTargetReceipt>())
            .ok_or(ApplicationError::ReceiptBudget)
    }
}
impl ProposalAdmission for MetadataAuthorityTarget {
    fn validate_proposal<'a>(
        &self,
        operation: OperationId,
        bytes: &[u8],
        pending: impl Iterator<Item = (OperationId, &'a [u8])>,
    ) -> Result<usize, ApplicationError> {
        let mut next = self.clone();
        let mut count = 0;
        let mut admit = |op, bytes: &[u8]| -> Result<(), ApplicationError> {
            if count >= MAX_DIRECTORY_PENDING
                || bytes.len() > self.readiness_requirements().command_bytes
            {
                return Err(ApplicationError::ReceiptBudget);
            }
            count += 1;
            let index = next
                .applied
                .checked_add(1)
                .ok_or(ApplicationError::IndexGap)?;
            let receipt = next
                .step(&LogEntry {
                    index,
                    term: 1,
                    payload: EntryPayload::Command {
                        operation: op,
                        bytes: bytes.to_vec(),
                    },
                })?
                .unwrap();
            if matches!(
                receipt.outcome,
                MetadataTargetOutcome::Conflict | MetadataTargetOutcome::NotActive
            ) {
                return Err(ApplicationError::InvalidCommand);
            }
            Ok(())
        };
        for (op, bytes) in pending {
            admit(op, bytes)?;
        }
        admit(operation, bytes)?;
        Ok(size_of::<MetadataTargetReceipt>())
    }
}
impl CheckpointStateMachine for MetadataAuthorityTarget {
    fn schema_version(&self) -> u64 {
        METADATA_TARGET_SCHEMA
    }
    fn checkpoint(&self, maximum: usize) -> Result<Vec<u8>, ApplicationError> {
        let command = self
            .imported
            .as_ref()
            .map(|i| i.command.as_slice())
            .unwrap_or(&[]);
        let len = 68 + command.len();
        if len > maximum {
            return Err(ApplicationError::InvalidCheckpoint);
        }
        let mut bytes = Vec::with_capacity(len);
        bytes.extend(b"VBMAT001");
        bytes.extend(self.anchor.0);
        bytes.extend(self.applied.to_le_bytes());
        bytes.extend(self.staged.unwrap_or(0).to_le_bytes());
        bytes.extend(
            self.imported
                .as_ref()
                .map_or(0, |i| i.status.index)
                .to_le_bytes(),
        );
        bytes.extend((command.len() as u32).to_le_bytes());
        bytes.extend(command);
        Ok(bytes)
    }
    fn restore_checkpoint(
        &mut self,
        schema: u64,
        applied: u64,
        bytes: &[u8],
    ) -> Result<(), ApplicationError> {
        if schema != METADATA_TARGET_SCHEMA {
            return Err(ApplicationError::UnsupportedSchema);
        }
        if bytes.len() > self.readiness_requirements().snapshot_bytes {
            return Err(ApplicationError::InvalidCheckpoint);
        }
        let mut r = Reader::new(bytes);
        if r.take(8)? != b"VBMAT001" || r.take(32)? != self.anchor.0 || r.u64()? != applied {
            return Err(ApplicationError::InvalidCheckpoint);
        }
        let staged = r.u64()?;
        let imported = r.u64()?;
        let len = r.u32()? as usize;
        if len > self.readiness_requirements().command_bytes {
            return Err(ApplicationError::InvalidCheckpoint);
        }
        let command = r.take(len)?;
        if !r.done()
            || staged > applied
            || imported > applied
            || (imported != 0 && (staged == 0 || imported <= staged))
            || (imported == 0 && !command.is_empty())
        {
            return Err(ApplicationError::InvalidCheckpoint);
        }
        let mut next = self.clone();
        next.applied = 0;
        next.staged = None;
        next.imported = None;
        if staged != 0 {
            next.applied = staged - 1;
            next.step(&LogEntry {
                index: staged,
                term: 1,
                payload: EntryPayload::Command {
                    operation: next.operation,
                    bytes: next.bootstrap_command(40)?,
                },
            })?;
        }
        if imported != 0 {
            next.applied = imported - 1;
            let receipt = next
                .step(&LogEntry {
                    index: imported,
                    term: 1,
                    payload: EntryPayload::Command {
                        operation: next.operation,
                        bytes: command.to_vec(),
                    },
                })?
                .unwrap();
            if !matches!(receipt.outcome, MetadataTargetOutcome::Imported(_)) {
                return Err(ApplicationError::InvalidCheckpoint);
            }
        }
        next.applied = applied;
        *self = next;
        Ok(())
    }
}
impl ReadableStateMachine for MetadataAuthorityTarget {
    type Query = MetadataTargetQuery;
    type ReadResult = MetadataTargetStatus;
    fn read_at(&self, required: u64, _: Self::Query) -> Result<Self::ReadResult, ApplicationError> {
        if required > self.applied {
            return Err(ApplicationError::NotApplied);
        }
        Ok(self.status())
    }
}
impl BoundedReadableStateMachine for MetadataAuthorityTarget {
    fn query_bytes(&self, _: &Self::Query, _: usize) -> Result<usize, ApplicationError> {
        Ok(0)
    }
    fn read_result_bound(&self, _: &Self::Query) -> Result<usize, ApplicationError> {
        Ok(size_of::<MetadataTargetStatus>())
    }
    fn read_result_bytes(&self, _: &Self::ReadResult, _: usize) -> Result<usize, ApplicationError> {
        Ok(0)
    }
}
