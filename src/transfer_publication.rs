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
//! Checked cross-group observations for a committed ownership publication.
//! Values establish structure/content binding, never foreign authentication.
use crate::{
    application::*,
    identity::*,
    routed::OwnershipFence,
    routing::{codec::*, *},
    transfer::*,
    transfer_source::*,
    transfer_target::*,
};
use std::mem::size_of;
pub const MAX_TRANSFER_PUBLICATION_BYTES: usize = 64 * 1024;
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct SourceFenceEvidence {
    pub fence: OwnershipFence,
    pub configuration: ConfigurationId,
    pub intent_digest: ContentDigest,
    pub exports: Vec<SourceExportCommitment>,
}
impl SourceFenceEvidence {
    #[allow(clippy::result_large_err)] // Preserve original observation on rejection.
    pub fn from_status(
        configuration: ConfigurationId,
        status: SourceFreezeStatus,
    ) -> Result<Self, (ApplicationError, SourceFreezeStatus)> {
        let bytes = match status.intent.encode(MAX_TRANSFER_INTENT_BYTES) {
            Ok(b) => b,
            Err(e) => return Err((e, status)),
        };
        if status.exports.capacity() > MAX_SCOPE_FACTS {
            return Err((ApplicationError::InvalidCommand, status));
        }
        Ok(Self {
            fence: status.fence,
            configuration,
            intent_digest: ContentDigest::sha256(&bytes),
            exports: status.exports,
        })
    }
}
const MAX_SCOPE_FACTS: usize = 256;
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct TargetReadyEvidence {
    pub group: GroupIdentity,
    pub operation: OperationId,
    pub configuration: ConfigurationId,
    pub staged_index: u64,
    pub imported: ImportStatus,
}
impl TargetReadyEvidence {
    #[allow(clippy::result_large_err)] // Return original owned status if incomplete.
    pub fn from_status(
        configuration: ConfigurationId,
        status: TargetStatus,
    ) -> Result<Self, (ApplicationError, TargetStatus)> {
        if status.staged_index.is_none() || status.imported.is_none() {
            return Err((ApplicationError::NotApplied, status));
        }
        Ok(Self {
            group: status.group,
            operation: status.operation,
            configuration,
            staged_index: status.staged_index.expect("checked"),
            imported: status.imported.expect("checked"),
        })
    }
}
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct TransferPublication {
    operation: OperationId,
    intent: TransferIntent,
    sources: Vec<SourceFenceEvidence>,
    targets: Vec<TargetReadyEvidence>,
}
impl TransferPublication {
    #[allow(clippy::result_large_err)] // Return the original owned evidence on rejection.
    pub fn new(
        operation: OperationId,
        intent: TransferIntent,
        sources: Vec<SourceFenceEvidence>,
        targets: Vec<TargetReadyEvidence>,
    ) -> Result<
        Self,
        (
            ApplicationError,
            TransferIntent,
            Vec<SourceFenceEvidence>,
            Vec<TargetReadyEvidence>,
        ),
    > {
        let validate = || -> Result<(), ApplicationError> {
            if !intent.permits_operation(operation) {
                return Err(ApplicationError::InvalidCommand);
            }
            let old = intent.sources();
            let new = intent.targets();
            if sources.len() != old.len()
                || targets.len() != new.len()
                || sources.capacity() > MAX_SCOPE_FACTS
                || targets.capacity() > MAX_SCOPE_FACTS
            {
                return Err(ApplicationError::InvalidCommand);
            }
            let digest = ContentDigest::sha256(&intent.encode(MAX_TRANSFER_INTENT_BYTES)?);
            for (source, route) in sources.iter().zip(&old) {
                let f = source.fence;
                if route.target != RouteTarget::Group(f.group)
                    || f.operation != operation
                    || f.responsibility != intent.before().input().responsibility
                    || f.epoch != intent.before().input().epoch
                    || f.index == 0
                    || f.index == u64::MAX
                    || source.intent_digest != digest
                    || source.exports.capacity() > MAX_SCOPE_FACTS
                {
                    return Err(ApplicationError::InvalidCommand);
                }
                let mut exports = source.exports.iter();
                for target in &new {
                    let Some(scope) = intersection(route.scope, target.scope) else {
                        continue;
                    };
                    let export = exports.next().ok_or(ApplicationError::InvalidCommand)?;
                    if target.target != RouteTarget::Group(export.target) || export.scope != scope {
                        return Err(ApplicationError::InvalidCommand);
                    }
                }
                if exports.next().is_some() {
                    return Err(ApplicationError::InvalidCommand);
                }
            }
            for (target, route) in targets.iter().zip(&new) {
                if intent.insertion_children().is_some_and(|children| {
                    !children.iter().any(|child| {
                        child.manifest.input().execution == ExecutionMode::Single(target.group)
                            && child.configuration == target.configuration
                    })
                }) || route.target != RouteTarget::Group(target.group)
                    || target.operation != operation
                    || target.staged_index == 0
                    || target.imported.index <= target.staged_index
                    || target.imported.index == u64::MAX
                    || target.imported.sources.capacity() > MAX_SCOPE_FACTS
                {
                    return Err(ApplicationError::InvalidCommand);
                }
                let mut imports = target.imported.sources.iter();
                for (source, old_route) in sources.iter().zip(&old) {
                    let Some(scope) = intersection(route.scope, old_route.scope) else {
                        continue;
                    };
                    let import = imports.next().ok_or(ApplicationError::InvalidCommand)?;
                    let export = source
                        .exports
                        .iter()
                        .find(|e| e.target == target.group)
                        .ok_or(ApplicationError::InvalidCommand)?;
                    if import.fence != source.fence
                        || import.configuration != source.configuration
                        || import.scope != scope
                        || import.digest != export.digest
                    {
                        return Err(ApplicationError::InvalidCommand);
                    }
                }
                if imports.next().is_some() {
                    return Err(ApplicationError::InvalidCommand);
                }
            }
            Ok(())
        };
        if let Err(e) = validate() {
            return Err((e, intent, sources, targets));
        }
        Ok(Self {
            operation,
            intent,
            sources,
            targets,
        })
    }
    pub fn operation(&self) -> OperationId {
        self.operation
    }
    pub fn intent(&self) -> &TransferIntent {
        &self.intent
    }
    pub fn sources(&self) -> &[SourceFenceEvidence] {
        &self.sources
    }
    pub fn targets(&self) -> &[TargetReadyEvidence] {
        &self.targets
    }
    pub fn retained_bytes(&self) -> usize {
        size_of::<Self>() + self.intent.retained_bytes() - size_of::<TransferIntent>()
            + self.sources.capacity() * size_of::<SourceFenceEvidence>()
            + self.targets.capacity() * size_of::<TargetReadyEvidence>()
            + self
                .sources
                .iter()
                .map(|s| s.exports.capacity() * size_of::<SourceExportCommitment>())
                .sum::<usize>()
            + self
                .targets
                .iter()
                .map(|t| t.imported.sources.capacity() * size_of::<ImportedSource>())
                .sum::<usize>()
    }
    pub fn encode(&self, max_bytes: usize) -> Result<Vec<u8>, ApplicationError> {
        let intent = self.intent.encode(MAX_TRANSFER_INTENT_BYTES)?;
        let len = 32
            + intent.len()
            + self
                .sources
                .iter()
                .map(|s| 74 + 60 * s.exports.len())
                .sum::<usize>()
            + self
                .targets
                .iter()
                .map(|t| 82 + 76 * t.imported.sources.len())
                .sum::<usize>();
        if len > max_bytes || len > MAX_TRANSFER_PUBLICATION_BYTES {
            return Err(ApplicationError::InvalidCommand);
        }
        let mut bytes = Vec::with_capacity(len);
        bytes.extend(b"VBTPUB01");
        bytes.extend(self.operation.get().to_le_bytes());
        bytes.extend((intent.len() as u32).to_le_bytes());
        bytes.extend(intent);
        bytes.extend((self.sources.len() as u16).to_le_bytes());
        for source in &self.sources {
            put_group(&mut bytes, source.fence.group);
            bytes.extend(source.fence.index.to_le_bytes());
            bytes.extend(source.configuration.get().to_le_bytes());
            bytes.extend(source.intent_digest.0);
            bytes.extend((source.exports.len() as u16).to_le_bytes());
            for export in &source.exports {
                put_group(&mut bytes, export.target);
                put_range(&mut bytes, export.scope);
                bytes.extend(export.digest.0);
            }
        }
        bytes.extend((self.targets.len() as u16).to_le_bytes());
        for target in &self.targets {
            put_group(&mut bytes, target.group);
            bytes.extend(target.configuration.get().to_le_bytes());
            bytes.extend(target.staged_index.to_le_bytes());
            bytes.extend(target.imported.index.to_le_bytes());
            bytes.extend(target.imported.digest.0);
            bytes.extend((target.imported.sources.len() as u16).to_le_bytes());
            for source in &target.imported.sources {
                put_group(&mut bytes, source.fence.group);
                bytes.extend(source.fence.index.to_le_bytes());
                bytes.extend(source.configuration.get().to_le_bytes());
                put_range(&mut bytes, source.scope);
                bytes.extend(source.digest.0);
            }
        }
        Ok(bytes)
    }
    pub fn decode(bytes: &[u8]) -> Result<Self, ApplicationError> {
        if bytes.len() > MAX_TRANSFER_PUBLICATION_BYTES {
            return Err(ApplicationError::InvalidCommand);
        }
        let mut r = Reader::new(bytes);
        if r.take(8)? != b"VBTPUB01" {
            return Err(ApplicationError::InvalidCommand);
        }
        let operation = r.operation()?;
        let len = r.u32()? as usize;
        let intent = TransferIntent::decode(r.take(len)?)?;
        let n = count(&mut r)?;
        let mut sources = Vec::with_capacity(n);
        for _ in 0..n {
            let fence = fence(&mut r, operation, &intent)?;
            let configuration = config(&mut r)?;
            let intent_digest = digest(&mut r)?;
            let n = count(&mut r)?;
            let mut exports = Vec::with_capacity(n);
            for _ in 0..n {
                exports.push(SourceExportCommitment {
                    target: r.group()?,
                    scope: r.range()?,
                    digest: digest(&mut r)?,
                });
            }
            sources.push(SourceFenceEvidence {
                fence,
                configuration,
                intent_digest,
                exports,
            });
        }
        let n = count(&mut r)?;
        let mut targets = Vec::with_capacity(n);
        for _ in 0..n {
            let group = r.group()?;
            let configuration = config(&mut r)?;
            let staged_index = r.u64()?;
            let index = r.u64()?;
            let content = digest(&mut r)?;
            let n = count(&mut r)?;
            let mut source_records = Vec::with_capacity(n);
            for _ in 0..n {
                source_records.push(ImportedSource {
                    fence: fence(&mut r, operation, &intent)?,
                    configuration: config(&mut r)?,
                    scope: r.range()?,
                    digest: digest(&mut r)?,
                });
            }
            targets.push(TargetReadyEvidence {
                group,
                operation,
                configuration,
                staged_index,
                imported: ImportStatus {
                    index,
                    digest: content,
                    sources: source_records,
                },
            });
        }
        if !r.done() {
            return Err(ApplicationError::InvalidCommand);
        }
        Self::new(operation, intent, sources, targets).map_err(|e| e.0)
    }
}
fn intersection(a: BucketRange, b: BucketRange) -> Option<BucketRange> {
    BucketRange::new(a.start().max(b.start()), a.end().min(b.end())).ok()
}
fn count(r: &mut Reader<'_>) -> Result<usize, ApplicationError> {
    let n = r.u16()? as usize;
    if n == 0 || n > MAX_SCOPE_FACTS {
        return Err(ApplicationError::InvalidCommand);
    }
    Ok(n)
}
fn digest(r: &mut Reader<'_>) -> Result<ContentDigest, ApplicationError> {
    Ok(ContentDigest(
        r.take(32)?
            .try_into()
            .map_err(|_| ApplicationError::InvalidCommand)?,
    ))
}
fn config(r: &mut Reader<'_>) -> Result<ConfigurationId, ApplicationError> {
    ConfigurationId::new(r.u64()?).ok_or(ApplicationError::InvalidCommand)
}
fn fence(
    r: &mut Reader<'_>,
    operation: OperationId,
    intent: &TransferIntent,
) -> Result<OwnershipFence, ApplicationError> {
    Ok(OwnershipFence {
        group: r.group()?,
        index: r.u64()?,
        operation,
        responsibility: intent.before().input().responsibility,
        epoch: intent.before().input().epoch,
    })
}
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct TransferPublicationStatus {
    pub publication_operation: OperationId,
    pub index: u64,
    pub publication: TransferPublication,
}
impl TransferPublicationStatus {
    pub fn encode(&self, max_bytes: usize) -> Result<Vec<u8>, ApplicationError> {
        if self.index == 0
            || self.index == u64::MAX
            || self.publication_operation == self.publication.operation()
        {
            return Err(ApplicationError::InvalidCommand);
        }
        let body = self.publication.encode(MAX_TRANSFER_PUBLICATION_BYTES)?;
        if 36 + body.len() > max_bytes {
            return Err(ApplicationError::InvalidCommand);
        }
        let mut bytes = Vec::with_capacity(36 + body.len());
        bytes.extend(b"VBTDEC01");
        bytes.extend(self.publication_operation.get().to_le_bytes());
        bytes.extend(self.index.to_le_bytes());
        bytes.extend((body.len() as u32).to_le_bytes());
        bytes.extend(body);
        Ok(bytes)
    }
    pub fn decode(bytes: &[u8]) -> Result<Self, ApplicationError> {
        if bytes.len() > 36 + MAX_TRANSFER_PUBLICATION_BYTES {
            return Err(ApplicationError::InvalidCommand);
        }
        let mut r = Reader::new(bytes);
        if r.take(8)? != b"VBTDEC01" {
            return Err(ApplicationError::InvalidCommand);
        }
        let publication_operation = r.operation()?;
        let index = r.u64()?;
        let len = r.u32()? as usize;
        let publication = TransferPublication::decode(r.take(len)?)?;
        if index == 0
            || index == u64::MAX
            || publication_operation == publication.operation()
            || !r.done()
        {
            return Err(ApplicationError::InvalidCommand);
        }
        Ok(Self {
            publication_operation,
            index,
            publication,
        })
    }
}
