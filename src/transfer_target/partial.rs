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
//! Immutable subrange exports from an originally imported and activated owner.
use super::*;
use crate::{
    routed::{parent_adoption::ParentAdoptionCommand, RetainedGrantStatus, ScopedOwnershipFence},
    scoped_source::{
        RetainedGrantAdoption, ScopedExportStatus, MAX_PARENT_SLOT_ADOPTION_BYTES,
        MAX_RETAINED_ADOPTION_BYTES,
    },
};
pub const PARTIAL_TRANSFER_TARGET_SCHEMA: u64 = 6;
pub const MAX_TARGET_PARTIAL_TRANSFERS: usize = 16;
#[derive(Clone, Copy)]
enum Outcome {
    Frozen(ScopedExportStatus),
    Grant(RetainedGrantStatus),
    Parent(ParentGrantStatus),
}
impl Outcome {
    fn receipt<R>(self) -> TargetOutcome<R> {
        match self {
            Self::Frozen(s) => TargetOutcome::ScopeFenced(s),
            Self::Grant(s) => TargetOutcome::GrantAdopted(s),
            Self::Parent(s) => TargetOutcome::ParentAdopted(s),
        }
    }
}
#[derive(Clone)]
struct Event {
    operation: OperationId,
    index: u64,
    bytes: Vec<u8>,
    image: Option<ScopeImage>,
    outcome: Outcome,
}
#[derive(Clone)]
pub(super) struct PartialState {
    maximum: usize,
    export_bytes: usize,
    events: Vec<Event>,
}
pub(super) fn control(bytes: &[u8]) -> bool {
    bytes.starts_with(b"VBTINT06")
        || bytes.starts_with(b"VBSADP01")
        || parent::is_parent(bytes)
        || bytes.starts_with(b"VBSLAD01")
        || bytes.starts_with(b"VBSXAD01")
}
fn digest(op: OperationId, index: u64, bytes: &[u8], image: Option<&ScopeImage>) -> ContentDigest {
    let mut b = b"VBTPART1".to_vec();
    b.extend(op.get().to_le_bytes());
    b.extend(index.to_le_bytes());
    b.extend(bytes);
    if let Some(i) = image {
        b.extend(ContentDigest::scope_image(i).0);
    }
    ContentDigest::sha256(&b)
}
impl<A, P> TransferTarget<A, P>
where
    A: ScopeStateMachine + BoundedStateMachine,
    A::Receipt: ApplicationReceipt,
    P: PartitionPolicy + Clone,
{
    /// Select before bootstrap, optionally after parent adoption. One unpublished
    /// partial transfer at a time; immutable exports have a finite lifetime budget.
    /// Retirement retains the completed ownership history without export payloads.
    #[allow(clippy::result_large_err)]
    pub fn with_partial_delegation(
        mut self,
        maximum: usize,
        export_bytes: usize,
    ) -> Result<Self, (ApplicationError, Self)> {
        if maximum == 0
            || maximum > MAX_TARGET_PARTIAL_TRANSFERS
            || export_bytes == 0
            || export_bytes > MAX_SCOPE_IMAGE_BYTES
            || self.partial.is_some()
            || self.applied != 0
            || self.staged.is_some()
            || self.binding.len() + 10 > MAX_INLINE_IMPORT_BYTES
        {
            return Err((ApplicationError::InvalidCommand, self));
        }
        self.partial = Some(PartialState {
            maximum,
            export_bytes,
            events: Vec::new(),
        });
        if self
            .readiness_requirements()
            .snapshot_bytes
            .checked_add(10)
            .is_none_or(|n| n > MAX_SCOPE_IMAGE_BYTES)
        {
            self.partial = None;
            return Err((ApplicationError::InvalidCommand, self));
        }
        self.binding[..8].copy_from_slice(if self.metadata_adoption {
            b"VBTSOWN7"
        } else {
            b"VBTSOWN5"
        });
        self.binding.extend((maximum as u16).to_le_bytes());
        self.binding.extend((export_bytes as u64).to_le_bytes());
        Ok(self)
    }
    pub(super) fn partial_reserve(&self) -> usize {
        self.partial.as_ref().map_or(0, |p| {
            2 + p.export_bytes
                + p.maximum * (172 + MAX_TRANSFER_INTENT_BYTES + MAX_RETAINED_ADOPTION_BYTES)
                + self.parent_limit
                    * (4 + self
                        .parent_command_bound()
                        .max(MAX_PARENT_SLOT_ADOPTION_BYTES)
                        - self.parent_command_bound())
        })
    }
    pub fn scoped_freeze(&self, op: OperationId) -> Option<ScopedExportStatus> {
        self.partial
            .as_ref()?
            .events
            .iter()
            .find_map(|e| match e.outcome {
                Outcome::Frozen(s) if e.operation == op => Some(s),
                _ => None,
            })
    }
    pub fn retained_grant(&self, op: OperationId) -> Option<RetainedGrantStatus> {
        self.partial
            .as_ref()?
            .events
            .iter()
            .find_map(|e| match e.outcome {
                Outcome::Grant(s) if e.operation == op => Some(s),
                _ => None,
            })
    }
    pub fn export_scoped(
        &self,
        op: OperationId,
        maximum: usize,
    ) -> Result<ScopeImage, ApplicationError> {
        let image = self
            .partial
            .as_ref()
            .and_then(|p| p.events.iter().find(|e| e.operation == op))
            .and_then(|e| e.image.as_ref())
            .ok_or(ApplicationError::NotApplied)?;
        if image.payload_capacity() > maximum {
            return Err(ApplicationError::ReceiptBudget);
        }
        Ok(image.clone())
    }
    pub(super) fn partial_operation(&self, op: OperationId) -> bool {
        self.partial.as_ref().is_some_and(|p| {
            p.events.iter().any(|e| {
                e.operation == op
                    || e.bytes.starts_with(b"VBTINT06")
                        && TransferIntent::decode(&e.bytes)
                            .expect("checked intent")
                            .insertion_children()
                            .is_some_and(|c| c.iter().any(|c| c.creation == op))
            })
        })
    }
    pub(super) fn check_active_context(
        &self,
        hint: &RouteHint,
        key: &[u8],
    ) -> Result<(), RoutingError> {
        check_owner(self.grant(), self.group, hint, key, &self.policy)?;
        if self.partial.as_ref().is_some_and(|p| {
            p.events.iter().any(|e| match e.outcome {
                Outcome::Frozen(s) => {
                    hint.bucket >= s.fence.scope.start() && hint.bucket < s.fence.scope.end()
                }
                _ => false,
            })
        }) {
            return Err(RoutingError::Fenced);
        }
        Ok(())
    }
    pub(super) fn partial_pending(&self) -> bool {
        self.partial.as_ref().is_some_and(|p| {
            p.events.iter().any(|e| {
                matches!(e.outcome, Outcome::Frozen(_))
                    && !p.events.iter().any(
                        |a| matches!(a.outcome, Outcome::Grant(s) if s.transfer == e.operation),
                    )
            })
        })
    }
    pub(super) fn apply_partial<R>(
        &mut self,
        entry: &LogEntry,
        restored: Option<ScopeImage>,
        replay: bool,
    ) -> Result<(TargetOutcome<R>, bool), ApplicationError> {
        let EntryPayload::Command {
            operation: op,
            bytes,
        } = &entry.payload
        else {
            return Err(ApplicationError::InvalidCommand);
        };
        let p = self
            .partial
            .as_ref()
            .ok_or(ApplicationError::UnsupportedSchema)?;
        if let Some(old) = p.events.iter().find(|e| e.operation == *op) {
            return if old.bytes == *bytes {
                Ok((old.outcome.receipt(), false))
            } else {
                Err(ApplicationError::InvalidCommand)
            };
        }
        let activated = self
            .activated
            .as_ref()
            .ok_or(ApplicationError::NotApplied)?
            .status
            .index;
        if entry.index <= activated
            || entry.index == u64::MAX
            || self.frozen.is_some()
            || *op == self.operation
            || self.inner.contains_operation(*op)
            || self.partial_operation(*op)
        {
            return Err(ApplicationError::InvalidCommand);
        }
        let mut advanced = false;
        let (outcome, image) = if bytes.starts_with(b"VBTINT06") {
            let intent = TransferIntent::decode(bytes)?;
            let sources = intent.sources();
            if intent.before() != self.grant()
                || !intent.is_retained_insertion()
                || !intent.permits_operation(*op)
                || sources.len() != 1
                || sources[0].target != RouteTarget::Group(self.group)
                || self.partial_pending()
                || intent.insertion_children().is_none_or(|c| {
                    c.iter().any(|c| {
                        c.creation == self.operation
                            || self.inner.contains_operation(c.creation)
                            || self.partial_operation(c.creation)
                    })
                })
            {
                return Err(ApplicationError::InvalidCommand);
            }
            if p.events.iter().filter(|e| e.image.is_some()).count() >= p.maximum {
                return Err(ApplicationError::ReceiptBudget);
            }
            let scope = sources[0].scope;
            if p.events.iter().any(|e| {
                e.image.as_ref().is_some_and(|i| {
                    i.scope().start() < scope.end() && scope.start() < i.scope().end()
                })
            }) {
                return Err(ApplicationError::InvalidCommand);
            }
            let bound = self.inner.export_scope_bound(scope)?;
            let used =
                p.events
                    .iter()
                    .filter_map(|e| e.image.as_ref())
                    .try_fold(0usize, |used, i| {
                        used.checked_add(self.inner.export_scope_bound(i.scope())?)
                            .ok_or(ApplicationError::ReceiptBudget)
                    })?;
            if bound == 0 || bound > p.export_bytes.saturating_sub(used) {
                return Err(ApplicationError::ReceiptBudget);
            }
            let image = if replay {
                restored.ok_or(ApplicationError::InvalidCheckpoint)?
            } else {
                let receipts = self.inner.apply_batch(&[LogEntry {
                    index: entry.index,
                    term: entry.term,
                    payload: EntryPayload::Noop,
                }])?;
                if !receipts.is_empty() || self.inner.applied_index() != entry.index {
                    return Err(ApplicationError::InvalidCommand);
                }
                self.inner
                    .checkpoint(self.limits.application_checkpoint_bytes)?;
                advanced = true;
                self.inner.export_scope(scope, bound)?
            };
            if image.schema() != self.inner.schema_version()
                || image.scheme() != self.inner.scheme()
                || image.scope() != scope
                || image.source_applied() != entry.index
                || image.payload_capacity() > bound
            {
                return Err(ApplicationError::InvalidCheckpoint);
            }
            let status = ScopedExportStatus {
                fence: ScopedOwnershipFence {
                    fence: OwnershipFence {
                        group: self.group,
                        responsibility: self.grant().input().responsibility,
                        epoch: self.grant().input().epoch,
                        operation: *op,
                        index: entry.index,
                    },
                    scope,
                },
                digest: ContentDigest::scope_image(&image),
                schema: image.schema(),
                payload_bytes: image.bytes().len(),
                intent_digest: Some(ContentDigest::sha256(bytes)),
            };
            (Outcome::Frozen(status), Some(image))
        } else if bytes.starts_with(b"VBSADP01") {
            if restored.is_some() {
                return Err(ApplicationError::InvalidCheckpoint);
            }
            let command = RetainedGrantAdoption::decode(bytes)?;
            let publication = &command.decision.publication;
            let intent = publication.intent();
            let e = p
                .events
                .iter()
                .find(|e| e.operation == publication.operation())
                .ok_or(ApplicationError::NotApplied)?;
            let Outcome::Frozen(status) = e.outcome else {
                return Err(ApplicationError::InvalidCommand);
            };
            let source = publication
                .sources()
                .iter()
                .find(|s| s.fence.group == self.group)
                .ok_or(ApplicationError::InvalidCommand)?;
            let expected =
                SourceFenceEvidence::from_scoped_status(source.configuration, status, intent)
                    .map_err(|e| e.0)?;
            if intent.before() != self.grant()
                || e.bytes != intent.encode(MAX_TRANSFER_INTENT_BYTES)?
                || status.fence.fence.index >= entry.index
                || *source != expected
                || p.events.iter().any(|e| matches!(e.outcome, Outcome::Grant(s) if s.transfer == publication.operation()))
            {
                return Err(ApplicationError::InvalidCommand);
            }
            let status = RetainedGrantStatus {
                operation: *op,
                index: entry.index,
                transfer: publication.operation(),
                epoch: intent.after().input().epoch,
                generation: intent.after().input().generation,
            };
            self.active_grant = intent.after().clone();
            (Outcome::Grant(status), None)
        } else {
            if restored.is_some() || self.partial_pending() {
                return Err(ApplicationError::InvalidCommand);
            }
            let command =
                ParentAdoptionCommand::decode_scoped_metadata(bytes, true, self.metadata_adoption)?;
            let TargetOutcome::ParentAdopted(status) =
                self.apply_parent_command::<()>(*op, entry.index, command)?
            else {
                return Err(ApplicationError::InvalidCommand);
            };
            (Outcome::Parent(status), None)
        };
        let p = self.partial.as_mut().unwrap();
        p.events
            .try_reserve_exact(1)
            .map_err(|_| ApplicationError::ReceiptBudget)?;
        p.events.push(Event {
            operation: *op,
            index: entry.index,
            bytes: bytes.clone(),
            image,
            outcome,
        });
        Ok((outcome.receipt(), advanced))
    }
    pub(super) fn partial_checkpoint(&self) -> Result<Vec<u8>, ApplicationError> {
        let p = self
            .partial
            .as_ref()
            .ok_or(ApplicationError::InvalidCheckpoint)?;
        let mut out = Vec::new();
        out.extend((p.events.len() as u16).to_le_bytes());
        for e in &p.events {
            out.extend(digest(e.operation, e.index, &e.bytes, e.image.as_ref()).0);
            out.extend(e.operation.get().to_le_bytes());
            out.extend(e.index.to_le_bytes());
            out.extend((e.bytes.len() as u32).to_le_bytes());
            out.extend(&e.bytes);
            if let Some(i) = &e.image {
                out.extend((44 + i.bytes().len() as u32).to_le_bytes());
                out.extend(i.schema().to_le_bytes());
                out.extend(i.scheme().id.get().to_le_bytes());
                out.extend(i.scheme().version.to_le_bytes());
                put_range(&mut out, i.scope());
                out.extend(i.source_applied().to_le_bytes());
                out.extend((i.bytes().len() as u32).to_le_bytes());
                out.extend(i.bytes());
            } else {
                out.extend(0u32.to_le_bytes());
            }
        }
        Ok(out)
    }
    pub(super) fn restore_partial(
        &mut self,
        r: &mut Reader<'_>,
        applied: u64,
        freeze: u64,
    ) -> Result<(), ApplicationError> {
        self.parents.clear();
        self.active_grant = self.intent.target_manifest(self.group).unwrap().clone();
        let p = self.partial.as_mut().unwrap();
        p.events.clear();
        let count = r.u16()? as usize;
        if count > 2 * p.maximum + self.parent_limit {
            return Err(ApplicationError::InvalidCheckpoint);
        }
        let old_frozen = self.frozen.take();
        let mut previous = 0;
        for _ in 0..count {
            let hash = r.take(32)?;
            let operation = r.operation()?;
            let index = r.u64()?;
            let n = r.u32()? as usize;
            let bytes = r.take(n)?;
            let n = r.u32()? as usize;
            let image = if n == 0 {
                None
            } else {
                let mut i = Reader::new(r.take(n)?);
                let schema = i.u64()?;
                let scheme = PartitionScheme {
                    id: RoutingSchemeId::new(i.u128()?)
                        .ok_or(ApplicationError::InvalidCheckpoint)?,
                    version: i.u32()?,
                };
                let scope = i.range()?;
                let index = i.u64()?;
                let n = i.u32()? as usize;
                let data = i.take(n)?.to_vec();
                if !i.done() {
                    return Err(ApplicationError::InvalidCheckpoint);
                }
                Some(ScopeImage::new(schema, scheme, scope, index, data).map_err(|e| e.0)?)
            };
            if index <= previous
                || index > applied
                || freeze != 0 && index >= freeze
                || !control(bytes)
                || hash != digest(operation, index, bytes, image.as_ref()).0
            {
                return Err(ApplicationError::InvalidCheckpoint);
            }
            let entry = LogEntry {
                index,
                term: 1,
                payload: EntryPayload::Command {
                    operation,
                    bytes: bytes.to_vec(),
                },
            };
            self.apply_partial::<()>(&entry, image, true)?;
            previous = index;
        }
        self.frozen = old_frozen;
        Ok(())
    }
}

impl<A, P> TransferTarget<A, P>
where
    A: ScopeStateMachine + BoundedStateMachine,
    A::Receipt: ApplicationReceipt,
    P: PartitionPolicy + Clone,
{
    pub(super) fn partial_retirement_bound(&self) -> Option<usize> {
        let p = self.partial.as_ref()?;
        Some(
            22 + MAX_TARGET_ACTIVATION_BYTES
                + p.maximum * (60 + MAX_RETAINED_ADOPTION_BYTES)
                + self.parent_limit
                    * (60
                        + self
                            .parent_command_bound()
                            .max(MAX_PARENT_SLOT_ADOPTION_BYTES)),
        )
    }

    pub(super) fn partial_retirement_lineage(&self) -> Result<Vec<u8>, ApplicationError> {
        let p = self
            .partial
            .as_ref()
            .ok_or(ApplicationError::UnsupportedSchema)?;
        let activation = self
            .activated
            .as_ref()
            .ok_or(ApplicationError::NotApplied)?;
        if self.partial_pending() {
            return Err(ApplicationError::NotApplied);
        }
        let changes: Vec<_> = p
            .events
            .iter()
            .filter(|e| !matches!(e.outcome, Outcome::Frozen(_)))
            .collect();
        let len =
            22 + activation.bytes.len() + changes.iter().map(|e| 60 + e.bytes.len()).sum::<usize>();
        if len > self.partial_retirement_bound().unwrap() {
            return Err(ApplicationError::ReceiptBudget);
        }
        let mut bytes = Vec::with_capacity(len);
        bytes.extend(activation.status.index.to_le_bytes());
        bytes.extend((activation.bytes.len() as u32).to_le_bytes());
        bytes.extend(&activation.bytes);
        bytes.extend(if self.metadata_adoption {
            b"VBTPRTL2"
        } else {
            b"VBTPRTL1"
        });
        bytes.extend((changes.len() as u16).to_le_bytes());
        for e in changes {
            bytes.extend(digest(e.operation, e.index, &e.bytes, None).0);
            bytes.extend(e.operation.get().to_le_bytes());
            bytes.extend(e.index.to_le_bytes());
            bytes.extend((e.bytes.len() as u32).to_le_bytes());
            bytes.extend(&e.bytes);
        }
        Ok(bytes)
    }

    /// Reconstruct only authority history. No application payload or provider
    /// operation is needed after retirement. Foreign observations retain the
    /// same authenticated-host obligation as their original ordered adoption.
    pub(super) fn partial_retirement_grant(
        &self,
        bytes: &[u8],
        fence_index: u64,
    ) -> Result<ResponsibilityManifest, ApplicationError> {
        let p = self
            .partial
            .as_ref()
            .ok_or(ApplicationError::UnsupportedSchema)?;
        if bytes.len() > self.partial_retirement_bound().unwrap() {
            return Err(ApplicationError::InvalidCheckpoint);
        }
        let mut r = Reader::new(bytes);
        let mut previous = r.u64()?;
        let len = r.u32()? as usize;
        let activation = self.activation(r.take(len)?)?;
        let publication = &activation.decision.publication;
        if publication.operation() != self.operation || publication.intent() != &self.intent {
            return Err(ApplicationError::InvalidCheckpoint);
        }
        let imported = publication
            .targets()
            .iter()
            .find(|t| t.group == self.group)
            .ok_or(ApplicationError::InvalidCheckpoint)?;
        if previous <= imported.imported.index
            || previous >= fence_index
            || r.take(8)?
                != if self.metadata_adoption {
                    b"VBTPRTL2"
                } else {
                    b"VBTPRTL1"
                }
        {
            return Err(ApplicationError::InvalidCheckpoint);
        }
        let count = r.u16()? as usize;
        if count > p.maximum + self.parent_limit {
            return Err(ApplicationError::InvalidCheckpoint);
        }
        let mut grant = self.intent.target_manifest(self.group).unwrap().clone();
        let mut used = std::collections::BTreeSet::from([self.operation]);
        let (mut transfers, mut parents) = (0, 0);
        for _ in 0..count {
            let hash = r.take(32)?;
            let op = r.operation()?;
            let index = r.u64()?;
            let len = r.u32()? as usize;
            if len
                > MAX_RETAINED_ADOPTION_BYTES.max(
                    self.parent_command_bound()
                        .max(MAX_PARENT_SLOT_ADOPTION_BYTES),
                )
                || index <= previous
                || index >= fence_index
                || !used.insert(op)
            {
                return Err(ApplicationError::InvalidCheckpoint);
            }
            let command = r.take(len)?;
            if hash != digest(op, index, command, None).0 {
                return Err(ApplicationError::InvalidCheckpoint);
            }
            if command.starts_with(b"VBSADP01") {
                transfers += 1;
                if transfers > p.maximum {
                    return Err(ApplicationError::InvalidCheckpoint);
                }
                let adoption = RetainedGrantAdoption::decode(command)?;
                let pubn = &adoption.decision.publication;
                let intent = pubn.intent();
                let [source] = pubn.sources() else {
                    return Err(ApplicationError::InvalidCheckpoint);
                };
                if intent.before() != &grant
                    || source.fence.group != self.group
                    || source.fence.index <= previous
                    || source.fence.index >= index
                    || !used.insert(source.fence.operation)
                {
                    return Err(ApplicationError::InvalidCheckpoint);
                }
                for child in intent
                    .insertion_children()
                    .ok_or(ApplicationError::InvalidCheckpoint)?
                {
                    if !used.insert(child.creation) {
                        return Err(ApplicationError::InvalidCheckpoint);
                    }
                }
                grant = intent.after().clone();
            } else {
                parents += 1;
                if parents > self.parent_limit {
                    return Err(ApplicationError::InvalidCheckpoint);
                }
                let change = ParentAdoptionCommand::decode_scoped_metadata(
                    command,
                    true,
                    self.metadata_adoption,
                )?;
                if change.before() != &grant || change.encode(len)? != command {
                    return Err(ApplicationError::InvalidCheckpoint);
                }
                grant = change.after();
            }
            previous = index;
        }
        if !r.done() {
            return Err(ApplicationError::InvalidCheckpoint);
        }
        Ok(grant)
    }
}
