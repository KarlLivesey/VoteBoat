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
//! Durable source fencing and exact metadata exports for authority movement.
//!
//! Select before bootstrap. Exports preserve the original authority/index
//! domain; an image is not permission to activate another metadata group.
mod locators;
mod publication;
mod serving;
mod target;
use crate::{
    application::*,
    directory::*,
    identity::*,
    log::*,
    raft::ReadinessRequirements,
    routing::{codec::*, *},
    transfer::{ContentDigest, DirectoryQuery, DirectoryRead, LifecycleDirectory},
};
pub use locators::*;
pub use publication::*;
pub use serving::*;
use std::{collections::BTreeMap, mem::size_of};
pub use target::*;

pub const METADATA_SOURCE_SCHEMA: u64 = 1;
pub const MAX_METADATA_IMAGE_BYTES: usize = 64 * 1024 * 1024;
pub const MAX_METADATA_PLAN_BYTES: usize = 58 + MAX_DIRECTORY_MANIFESTS * (4 + MAX_MANIFEST_BYTES);

/// Entire settled local metadata view. Concrete data groups and ownership epochs
/// are preserved. Only authority references and route generations change.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct MetadataMovePlan {
    source: GroupIdentity,
    target: GroupIdentity,
    manifests: Vec<ResponsibilityManifest>,
}
impl MetadataMovePlan {
    pub fn new(
        source: GroupIdentity,
        target: GroupIdentity,
        mut manifests: Vec<ResponsibilityManifest>,
    ) -> Result<Self, ApplicationError> {
        if manifests.is_empty()
            || manifests.len() > MAX_DIRECTORY_MANIFESTS
            || manifests.capacity() > MAX_DIRECTORY_MANIFESTS
        {
            return Err(ApplicationError::InvalidCommand);
        }
        manifests.sort_by_key(|m| m.input().responsibility);
        let plan = Self {
            source,
            target,
            manifests,
        };
        plan.validate()?;
        Ok(plan)
    }
    fn validate(&self) -> Result<(), ApplicationError> {
        if self.source.id == self.target.id {
            return Err(ApplicationError::InvalidCommand);
        }
        let mut by_id = BTreeMap::new();
        for m in &self.manifests {
            let v = m.input();
            if v.authority != self.source
                || v.generation.get() == u64::MAX
                || by_id.insert(v.responsibility, m).is_some()
                || by_id
                    .keys()
                    .any(|id| id.id == v.responsibility.id && *id != v.responsibility)
            {
                return Err(ApplicationError::InvalidCommand);
            }
            if v.parent.is_some_and(|p| p.group.id == self.target.id) {
                return Err(ApplicationError::InvalidCommand);
            }
            let groups: Vec<_> = match &v.execution {
                ExecutionMode::Single(g) => vec![*g],
                ExecutionMode::Partitioned(routes) | ExecutionMode::Delegated(routes) => routes
                    .iter()
                    .filter_map(|r| match r.target {
                        RouteTarget::Group(g) => Some(g),
                        RouteTarget::Child(c) => Some(c.group),
                        RouteTarget::Vacant => None,
                    })
                    .collect(),
            };
            if groups.iter().any(|g| g.id == self.target.id) {
                return Err(ApplicationError::InvalidCommand);
            }
        }
        for m in &self.manifests {
            let v = m.input();
            if let Some(p) = v.parent.filter(|p| p.group == self.source) {
                let parent = by_id
                    .get(&p.responsibility)
                    .ok_or(ApplicationError::InvalidCommand)?;
                let ExecutionMode::Delegated(routes) = &parent.input().execution else {
                    return Err(ApplicationError::InvalidCommand);
                };
                if !routes.iter().any(|r| {
                    r.scope == v.scope
                        && r.target
                            == RouteTarget::Child(ChildAuthority {
                                responsibility: v.responsibility,
                                group: self.source,
                                epoch: v.epoch,
                            })
                }) {
                    return Err(ApplicationError::InvalidCommand);
                }
            }
            if let ExecutionMode::Delegated(routes) = &v.execution {
                for r in routes {
                    if let RouteTarget::Child(c) = r.target {
                        if c.group == self.source {
                            let child = by_id
                                .get(&c.responsibility)
                                .ok_or(ApplicationError::InvalidCommand)?;
                            if child.input().parent
                                != Some(ParentAuthority {
                                    responsibility: v.responsibility,
                                    group: self.source,
                                })
                                || child.input().scope != r.scope
                                || child.input().epoch != c.epoch
                            {
                                return Err(ApplicationError::InvalidCommand);
                            }
                        }
                    }
                }
            }
        }
        // Bound ancestry and reject cycles even for independently decoded plans.
        for m in &self.manifests {
            let mut id = m.input().responsibility;
            let mut seen = Vec::new();
            loop {
                if seen.len() == MAX_ROUTE_HOPS || seen.contains(&id) {
                    return Err(ApplicationError::InvalidCommand);
                }
                seen.push(id);
                match by_id[&id].input().parent.filter(|p| p.group == self.source) {
                    Some(p) => id = p.responsibility,
                    None => break,
                }
            }
        }
        Ok(())
    }
    pub fn source(&self) -> GroupIdentity {
        self.source
    }
    pub fn target(&self) -> GroupIdentity {
        self.target
    }
    pub fn manifests(&self) -> &[ResponsibilityManifest] {
        &self.manifests
    }
    pub fn updated_manifests(&self) -> Vec<ResponsibilityManifest> {
        self.manifests
            .iter()
            .map(|m| {
                let mut v = m.clone().into_input();
                v.authority = self.target;
                v.generation = RouteGeneration::new(v.generation.get() + 1).unwrap();
                if let Some(p) = &mut v.parent {
                    if p.group == self.source {
                        p.group = self.target;
                    }
                }
                if let ExecutionMode::Delegated(routes) = &mut v.execution {
                    for r in routes {
                        if let RouteTarget::Child(c) = &mut r.target {
                            if c.group == self.source {
                                c.group = self.target;
                            }
                        }
                    }
                }
                ResponsibilityManifest::new(v).expect("checked authority-only transformation")
            })
            .collect()
    }
    pub fn encode(&self, maximum: usize) -> Result<Vec<u8>, ApplicationError> {
        let len = 58
            + self
                .manifests
                .iter()
                .map(|m| 4 + manifest_len(m))
                .sum::<usize>();
        if len > maximum.min(MAX_METADATA_PLAN_BYTES) {
            return Err(ApplicationError::InvalidCommand);
        }
        let mut b = Vec::with_capacity(len);
        b.extend(b"VBMAPL01");
        put_group(&mut b, self.source);
        put_group(&mut b, self.target);
        b.extend((self.manifests.len() as u16).to_le_bytes());
        for m in &self.manifests {
            b.extend((manifest_len(m) as u32).to_le_bytes());
            put_manifest(&mut b, m);
        }
        Ok(b)
    }
    pub fn decode(bytes: &[u8]) -> Result<Self, ApplicationError> {
        if bytes.len() > MAX_METADATA_PLAN_BYTES {
            return Err(ApplicationError::InvalidCommand);
        }
        let mut r = Reader::new(bytes);
        if r.take(8)? != b"VBMAPL01" {
            return Err(ApplicationError::InvalidCommand);
        }
        let source = r.group()?;
        let target = r.group()?;
        let n = usize::from(r.u16()?);
        if n == 0 || n > MAX_DIRECTORY_MANIFESTS {
            return Err(ApplicationError::InvalidCommand);
        }
        let mut manifests = Vec::with_capacity(n);
        for _ in 0..n {
            let len = r.u32()? as usize;
            if len > MAX_MANIFEST_BYTES {
                return Err(ApplicationError::InvalidCommand);
            }
            let m = read_manifest(r.take(len)?)?;
            manifests.push(m);
        }
        if !r.done() {
            return Err(ApplicationError::InvalidCommand);
        }
        let plan = Self::new(source, target, manifests)?;
        if plan.encode(MAX_METADATA_PLAN_BYTES)? != bytes {
            return Err(ApplicationError::InvalidCommand);
        }
        Ok(plan)
    }
}
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub struct MetadataSourceStatus {
    pub source: GroupIdentity,
    pub target: GroupIdentity,
    pub operation: OperationId,
    pub index: u64,
    pub directory_schema: u64,
    pub plan_digest: ContentDigest,
    pub image_digest: ContentDigest,
    pub rejected_digest: ContentDigest,
    pub image_bytes: usize,
}
/// Immutable, bounded bytes in the original source's index domain. Host code
/// must obtain the matching source status through an authenticated quorum read.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct MetadataImage {
    status: MetadataSourceStatus,
    plan: MetadataMovePlan,
    bytes: Vec<u8>,
    rejected: Vec<(OperationId, u64)>,
}
impl MetadataImage {
    pub fn status(&self) -> MetadataSourceStatus {
        self.status
    }
    pub fn plan(&self) -> &MetadataMovePlan {
        &self.plan
    }
    /// Failed source-control IDs remain reserved in the original source domain.
    pub fn rejected_operations(&self) -> &[(OperationId, u64)] {
        &self.rejected
    }
    pub fn bytes(&self) -> &[u8] {
        &self.bytes
    }
}
#[derive(Clone, Debug, Eq, PartialEq)]
pub enum MetadataSourceOutcome {
    Directory(DirectoryReceipt),
    Frozen(MetadataSourceStatus),
    Conflict,
    Fenced,
}
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct MetadataSourceReceipt {
    pub index: u64,
    pub operation: OperationId,
    pub outcome: MetadataSourceOutcome,
}
impl ApplicationReceipt for MetadataSourceReceipt {
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
pub enum MetadataSourceQuery {
    Directory(DirectoryQuery),
    Status,
}
#[derive(Clone, Debug, Eq, PartialEq)]
#[allow(clippy::large_enum_variant)]
pub enum MetadataSourceRead {
    Directory(DirectoryRead),
    Status(Option<MetadataSourceStatus>),
    Fenced,
}
#[derive(Clone)]
struct Frozen {
    status: MetadataSourceStatus,
    plan: MetadataMovePlan,
    command: Vec<u8>,
}
/// Source-side authority guard using the directory's existing log and providers.
/// Selection is immutable and mandatory before bootstrap; no target is activated.
#[derive(Clone)]
pub struct MetadataAuthoritySource {
    initial: LifecycleDirectory,
    directory: LifecycleDirectory,
    export_bytes: usize,
    anchor: ContentDigest,
    applied: u64,
    bootstrap: Option<OperationId>,
    frozen: Option<Frozen>,
    rejected: BTreeMap<OperationId, u64>,
}
impl MetadataAuthoritySource {
    #[allow(clippy::result_large_err)]
    pub fn new(
        directory: LifecycleDirectory,
        export_bytes: usize,
    ) -> Result<Self, (ApplicationError, LifecycleDirectory)> {
        if directory.applied_index() != 0
            || export_bytes == 0
            || export_bytes > MAX_METADATA_IMAGE_BYTES
            || directory
                .directory()
                .readiness_requirements()
                .snapshot_bytes
                > export_bytes
        {
            return Err((ApplicationError::InvalidCommand, directory));
        }
        let checkpoint = match directory.checkpoint(export_bytes) {
            Ok(b) => b,
            Err(e) => return Err((e, directory)),
        };
        Ok(Self {
            anchor: ContentDigest::sha256(&checkpoint),
            initial: directory.clone(),
            directory,
            export_bytes,
            applied: 0,
            bootstrap: None,
            frozen: None,
            rejected: BTreeMap::new(),
        })
    }
    pub fn plan(&self, target: GroupIdentity) -> Result<MetadataMovePlan, ApplicationError> {
        if self.frozen.is_some() {
            return Err(ApplicationError::NotApplied);
        }
        MetadataMovePlan::new(
            self.directory.directory().plan().authority(),
            target,
            self.directory.directory().authority_move_view()?,
        )
    }
    pub fn status(&self) -> Option<MetadataSourceStatus> {
        self.frozen.as_ref().map(|f| f.status)
    }
    /// Live local diagnostics and command builders. The caller still needs a
    /// matching quorum observation before treating these facts as authoritative.
    /// After fencing, use the immutable export in its original source domain.
    pub fn directory(&self) -> Option<&LifecycleDirectory> {
        self.frozen.is_none().then_some(&self.directory)
    }
    pub fn bootstrap_command(&self, maximum: usize) -> Result<Vec<u8>, ApplicationError> {
        let body = self
            .initial
            .directory()
            .bootstrap_command(MAX_DIRECTORY_COMMAND_BYTES)?;
        let mut b = Vec::with_capacity(52 + body.len());
        b.extend(b"VBMASB01");
        b.extend(self.anchor.0);
        b.extend((self.export_bytes as u64).to_le_bytes());
        b.extend((body.len() as u32).to_le_bytes());
        b.extend(body);
        if b.len() > maximum {
            return Err(ApplicationError::InvalidCommand);
        }
        Ok(b)
    }
    pub fn freeze_command(
        &self,
        plan: &MetadataMovePlan,
        maximum: usize,
    ) -> Result<Vec<u8>, ApplicationError> {
        let body = plan.encode(MAX_METADATA_PLAN_BYTES)?;
        let mut b = Vec::with_capacity(44 + body.len());
        b.extend(b"VBMASF01");
        b.extend(self.anchor.0);
        b.extend((body.len() as u32).to_le_bytes());
        b.extend(body);
        if b.len() > maximum {
            return Err(ApplicationError::InvalidCommand);
        }
        Ok(b)
    }
    fn parse_freeze(&self, bytes: &[u8]) -> Result<MetadataMovePlan, ApplicationError> {
        if bytes.len() > 44 + MAX_METADATA_PLAN_BYTES {
            return Err(ApplicationError::InvalidCommand);
        }
        let mut r = Reader::new(bytes);
        if r.take(8)? != b"VBMASF01" || r.take(32)? != self.anchor.0 {
            return Err(ApplicationError::InvalidCommand);
        }
        let n = r.u32()? as usize;
        let plan = MetadataMovePlan::decode(r.take(n)?)?;
        if !r.done() {
            return Err(ApplicationError::InvalidCommand);
        }
        Ok(plan)
    }
    fn matches(&self, plan: &MetadataMovePlan) -> bool {
        self.directory.directory().plan().authority() == plan.source
            && self.directory.directory().authority_move_view().as_ref() == Ok(&plan.manifests)
    }
    fn status_for(
        &self,
        operation: OperationId,
        plan: &MetadataMovePlan,
    ) -> Result<MetadataSourceStatus, ApplicationError> {
        let image = self.directory.checkpoint(self.export_bytes)?;
        Ok(MetadataSourceStatus {
            source: plan.source,
            target: plan.target,
            operation,
            index: self.directory.applied_index(),
            directory_schema: self.directory.schema_version(),
            plan_digest: ContentDigest::sha256(&plan.encode(MAX_METADATA_PLAN_BYTES)?),
            image_digest: ContentDigest::sha256(&image),
            rejected_digest: self.rejected_digest(),
            image_bytes: image.len(),
        })
    }
    fn rejected_digest(&self) -> ContentDigest {
        let mut bytes = Vec::with_capacity(4 + 24 * self.rejected.len());
        bytes.extend((self.rejected.len() as u32).to_le_bytes());
        for (op, index) in &self.rejected {
            bytes.extend(op.get().to_le_bytes());
            bytes.extend(index.to_le_bytes());
        }
        ContentDigest::sha256(&bytes)
    }
    pub fn export(&self, maximum: usize) -> Result<MetadataImage, ApplicationError> {
        let frozen = self.frozen.as_ref().ok_or(ApplicationError::NotApplied)?;
        let bytes = self.directory.checkpoint(maximum.min(self.export_bytes))?;
        if bytes.len() != frozen.status.image_bytes
            || ContentDigest::sha256(&bytes) != frozen.status.image_digest
        {
            return Err(ApplicationError::InvalidCheckpoint);
        }
        Ok(MetadataImage {
            status: frozen.status,
            plan: frozen.plan.clone(),
            bytes,
            rejected: self
                .rejected
                .iter()
                .map(|(op, index)| (*op, *index))
                .collect(),
        })
    }
    pub fn readiness_requirements(&self) -> ReadinessRequirements {
        let inner = self.initial.directory().readiness_requirements();
        ReadinessRequirements {
            application_schema: METADATA_SOURCE_SCHEMA,
            command_bytes: inner
                .command_bytes
                .max(44 + MAX_METADATA_PLAN_BYTES)
                .max(52 + inner.command_bytes),
            snapshot_bytes: 180
                + self.export_bytes
                + 44
                + MAX_METADATA_PLAN_BYTES
                + 24 * self.initial.directory().limits().operations,
        }
    }
    fn step(
        &mut self,
        entry: &LogEntry,
    ) -> Result<Option<MetadataSourceReceipt>, ApplicationError> {
        if self.applied.checked_add(1) != Some(entry.index) {
            return Err(ApplicationError::IndexGap);
        }
        let EntryPayload::Command { operation, bytes } = &entry.payload else {
            if self.frozen.is_none() {
                self.directory.apply_batch(std::slice::from_ref(entry))?;
            }
            self.applied = entry.index;
            return Ok(None);
        };
        let outcome = if self.rejected.contains_key(operation) {
            if self.frozen.is_none() {
                self.directory.apply_batch(&[LogEntry {
                    index: entry.index,
                    term: entry.term,
                    payload: EntryPayload::Noop,
                }])?;
            }
            MetadataSourceOutcome::Conflict
        } else if let Some(f) = &self.frozen {
            if f.status.operation == *operation && f.command == *bytes {
                MetadataSourceOutcome::Frozen(f.status)
            } else {
                MetadataSourceOutcome::Fenced
            }
        } else if bytes.starts_with(b"VBMASF01") {
            let plan = self.parse_freeze(bytes)?;
            let available = self.bootstrap.is_some()
                && !self.directory.directory().contains_operation(*operation)
                && self.matches(&plan);
            self.directory.apply_batch(&[LogEntry {
                index: entry.index,
                term: entry.term,
                payload: EntryPayload::Noop,
            }])?;
            if available {
                let status = self.status_for(*operation, &plan)?;
                self.frozen = Some(Frozen {
                    status,
                    plan,
                    command: bytes.clone(),
                });
                MetadataSourceOutcome::Frozen(status)
            } else {
                if !self.directory.directory().contains_operation(*operation) {
                    if self.rejected.len() >= self.initial.directory().limits().operations {
                        return Err(ApplicationError::DedupCapacity);
                    }
                    self.rejected.insert(*operation, entry.index);
                }
                MetadataSourceOutcome::Conflict
            }
        } else {
            let raw = if bytes.starts_with(b"VBMASB01") {
                if *bytes != self.bootstrap_command(self.readiness_requirements().command_bytes)? {
                    return Err(ApplicationError::InvalidCommand);
                }
                self.initial
                    .directory()
                    .bootstrap_command(MAX_DIRECTORY_COMMAND_BYTES)?
            } else {
                if self.bootstrap.is_none() {
                    return Err(ApplicationError::NotApplied);
                }
                bytes.clone()
            };
            let receipt = self
                .directory
                .apply_batch(&[LogEntry {
                    index: entry.index,
                    term: entry.term,
                    payload: EntryPayload::Command {
                        operation: *operation,
                        bytes: raw,
                    },
                }])?
                .remove(0);
            if matches!(receipt.outcome, DirectoryOutcome::Initialized) {
                self.bootstrap = Some(*operation);
            }
            MetadataSourceOutcome::Directory(receipt)
        };
        self.applied = entry.index;
        Ok(Some(MetadataSourceReceipt {
            index: entry.index,
            operation: *operation,
            outcome,
        }))
    }
}
impl StateMachine for MetadataAuthoritySource {
    type Receipt = MetadataSourceReceipt;
    fn validate_group(&self, g: GroupIdentity) -> Result<(), ApplicationError> {
        self.initial.validate_group(g)
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
        for e in entries {
            if let Some(r) = next.step(e)? {
                receipts.push(r);
            }
        }
        *self = next;
        Ok(receipts)
    }
}
impl BoundedStateMachine for MetadataAuthoritySource {
    fn receipt_bytes_bound(&self, entries: &[LogEntry]) -> Result<usize, ApplicationError> {
        entries
            .iter()
            .filter(|e| matches!(e.payload, EntryPayload::Command { .. }))
            .count()
            .checked_mul(size_of::<MetadataSourceReceipt>())
            .ok_or(ApplicationError::ReceiptBudget)
    }
}
impl ProposalAdmission for MetadataAuthoritySource {
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
            if let Some(f) = &next.frozen {
                if f.status.operation != op || f.command != bytes {
                    return Err(ApplicationError::InvalidCommand);
                }
            }
            if !bytes.starts_with(b"VBMAS") {
                next.directory
                    .validate_proposal(op, bytes, std::iter::empty())?;
            }
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
                MetadataSourceOutcome::Conflict | MetadataSourceOutcome::Fenced
            ) {
                return Err(ApplicationError::InvalidCommand);
            }
            Ok(())
        };
        for (op, bytes) in pending {
            admit(op, bytes)?;
        }
        admit(operation, bytes)?;
        Ok(size_of::<MetadataSourceReceipt>())
    }
}
impl CheckpointStateMachine for MetadataAuthoritySource {
    fn schema_version(&self) -> u64 {
        METADATA_SOURCE_SCHEMA
    }
    fn checkpoint(&self, maximum: usize) -> Result<Vec<u8>, ApplicationError> {
        let body = self.directory.checkpoint(self.export_bytes)?;
        let command = self
            .frozen
            .as_ref()
            .map(|f| f.command.as_slice())
            .unwrap_or(&[]);
        let len = 180 + 24 * self.rejected.len() + body.len() + command.len();
        if len > maximum {
            return Err(ApplicationError::InvalidCheckpoint);
        }
        let mut b = Vec::with_capacity(len);
        b.extend(b"VBMAS001");
        b.extend(self.anchor.0);
        b.extend((self.export_bytes as u64).to_le_bytes());
        b.extend(self.applied.to_le_bytes());
        b.extend(self.bootstrap.map_or(0, OperationId::get).to_le_bytes());
        b.extend(self.directory.applied_index().to_le_bytes());
        b.extend(self.directory.schema_version().to_le_bytes());
        b.extend((body.len() as u32).to_le_bytes());
        b.extend(body);
        b.extend((self.rejected.len() as u32).to_le_bytes());
        for (op, index) in &self.rejected {
            b.extend(op.get().to_le_bytes());
            b.extend(index.to_le_bytes());
        }
        b.extend(
            self.frozen
                .as_ref()
                .map_or(0, |f| f.status.operation.get())
                .to_le_bytes(),
        );
        b.extend(
            self.frozen
                .as_ref()
                .map_or([0; 32], |f| f.status.image_digest.0),
        );
        b.extend(
            self.frozen
                .as_ref()
                .map_or([0; 32], |f| f.status.rejected_digest.0),
        );
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
        if schema != METADATA_SOURCE_SCHEMA {
            return Err(ApplicationError::UnsupportedSchema);
        }
        if bytes.len() > self.readiness_requirements().snapshot_bytes {
            return Err(ApplicationError::InvalidCheckpoint);
        }
        let mut r = Reader::new(bytes);
        if r.take(8)? != b"VBMAS001"
            || r.take(32)? != self.anchor.0
            || r.u64()? != self.export_bytes as u64
            || r.u64()? != applied
        {
            return Err(ApplicationError::InvalidCheckpoint);
        }
        let boot = r.u128()?;
        let index = r.u64()?;
        let inner_schema = r.u64()?;
        let n = r.u32()? as usize;
        if n > self.export_bytes {
            return Err(ApplicationError::InvalidCheckpoint);
        }
        let body = r.take(n)?;
        let count = r.u32()? as usize;
        if count > self.initial.directory().limits().operations {
            return Err(ApplicationError::InvalidCheckpoint);
        }
        let mut rejected = BTreeMap::new();
        let mut indices = std::collections::BTreeSet::new();
        let mut previous = None;
        for _ in 0..count {
            let op = r.operation()?;
            let at = r.u64()?;
            if at == 0 || at > index || !indices.insert(at) || previous.is_some_and(|p| p >= op) {
                return Err(ApplicationError::InvalidCheckpoint);
            }
            previous = Some(op);
            rejected.insert(op, at);
        }
        let op = r.u128()?;
        let digest: [u8; 32] = r.take(32)?.try_into().unwrap();
        let rejected_digest: [u8; 32] = r.take(32)?.try_into().unwrap();
        let n = r.u32()? as usize;
        if n > 44 + MAX_METADATA_PLAN_BYTES {
            return Err(ApplicationError::InvalidCheckpoint);
        }
        let command = r.take(n)?;
        if !r.done() || index > applied {
            return Err(ApplicationError::InvalidCheckpoint);
        }
        let mut next = self.clone();
        next.directory = next.initial.clone();
        next.directory
            .restore_checkpoint(inner_schema, index, body)?;
        next.bootstrap = OperationId::new(boot);
        if rejected
            .keys()
            .any(|op| next.directory.directory().contains_operation(*op))
            || rejected
                .values()
                .any(|index| next.directory.directory().contains_command_at(*index))
        {
            return Err(ApplicationError::InvalidCheckpoint);
        }
        next.rejected = rejected;
        next.applied = applied;
        next.frozen = None;
        if next
            .bootstrap
            .is_some_and(|op| !next.directory.directory().is_bootstrap_operation(op))
        {
            return Err(ApplicationError::InvalidCheckpoint);
        }
        if op == 0 {
            if !command.is_empty()
                || index != applied
                || digest != [0; 32]
                || rejected_digest != [0; 32]
            {
                return Err(ApplicationError::InvalidCheckpoint);
            }
        } else {
            let operation = OperationId::new(op).unwrap();
            let plan = next.parse_freeze(command)?;
            if next.bootstrap.is_none()
                || index == 0
                || next.directory.directory().contains_operation(operation)
                || next.directory.directory().contains_command_at(index)
                || next.rejected.contains_key(&operation)
                || !next.matches(&plan)
            {
                return Err(ApplicationError::InvalidCheckpoint);
            }
            let status = next.status_for(operation, &plan)?;
            if status.image_digest.0 != digest || status.rejected_digest.0 != rejected_digest {
                return Err(ApplicationError::InvalidCheckpoint);
            }
            next.frozen = Some(Frozen {
                status,
                plan,
                command: command.to_vec(),
            });
        }
        if next.bootstrap.is_some() != next.directory.directory().has_bootstrap() {
            return Err(ApplicationError::InvalidCheckpoint);
        }
        *self = next;
        Ok(())
    }
}
impl ReadableStateMachine for MetadataAuthoritySource {
    type Query = MetadataSourceQuery;
    type ReadResult = MetadataSourceRead;
    fn read_at(
        &self,
        required: u64,
        query: Self::Query,
    ) -> Result<Self::ReadResult, ApplicationError> {
        if required > self.applied {
            return Err(ApplicationError::NotApplied);
        }
        match query {
            MetadataSourceQuery::Status => Ok(MetadataSourceRead::Status(self.status())),
            MetadataSourceQuery::Directory(q) => {
                if self.frozen.is_some() {
                    Ok(MetadataSourceRead::Fenced)
                } else {
                    self.directory
                        .read_at(required, q)
                        .map(MetadataSourceRead::Directory)
                }
            }
        }
    }
}
impl BoundedReadableStateMachine for MetadataAuthoritySource {
    fn query_bytes(&self, q: &Self::Query, limit: usize) -> Result<usize, ApplicationError> {
        match q {
            MetadataSourceQuery::Status => Ok(0),
            MetadataSourceQuery::Directory(q) => self.directory.query_bytes(q, limit),
        }
    }
    fn read_result_bound(&self, q: &Self::Query) -> Result<usize, ApplicationError> {
        Ok(size_of::<Self::ReadResult>()
            + match q {
                MetadataSourceQuery::Directory(q) if self.frozen.is_none() => {
                    self.directory.read_result_bound(q)?
                }
                _ => 0,
            })
    }
    fn read_result_bytes(
        &self,
        r: &Self::ReadResult,
        limit: usize,
    ) -> Result<usize, ApplicationError> {
        match r {
            MetadataSourceRead::Directory(r) => self.directory.read_result_bytes(r, limit),
            _ => Ok(0),
        }
    }
}
