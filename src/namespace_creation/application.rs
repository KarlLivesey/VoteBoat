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
//! Local committed readiness/activation guard over the existing routed application.
use super::*;
use crate::{
    log::*,
    raft::ReadinessRequirements,
    routed::codec::{decode, Command},
    routed::*,
    scope::ScopeStateMachine,
    transfer_source::TransferSource,
};
use std::mem::size_of;
pub const CREATED_NAMESPACE_SCHEMA: u64 = 1;
pub const CREATED_NAMESPACE_SOURCE_SCHEMA: u64 = 2;
mod owner;
pub use owner::NamespaceOwner;
#[derive(Clone, Debug, Eq, PartialEq)]
pub enum NamespaceOutcome<R> {
    Ready,
    Activated,
    NotActive,
    OperationConflict,
    Data(RoutedReceipt<R>),
}
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct NamespaceReceipt<R> {
    pub index: u64,
    pub operation: OperationId,
    pub outcome: NamespaceOutcome<R>,
}
impl<R: ApplicationReceipt> ApplicationReceipt for NamespaceReceipt<R> {
    fn index(&self) -> u64 {
        self.index
    }
    fn operation(&self) -> OperationId {
        self.operation
    }
    fn nested_bytes(&self, limit: usize) -> Result<usize, ApplicationError> {
        match &self.outcome {
            NamespaceOutcome::Data(r) => r.nested_bytes(limit),
            _ => Ok(0),
        }
    }
}
#[derive(Clone, Debug, Eq, PartialEq)]
pub enum NamespaceOwnerQuery<Q> {
    Status,
    Data(Q),
}
#[derive(Clone, Debug, Eq, PartialEq)]
pub enum NamespaceOwnerRead<R> {
    Status(NamespaceStatus),
    NotActive,
    Data(R),
}
pub type NamespaceQuery<Q> = NamespaceOwnerQuery<RoutedQuery<Q>>;
pub type NamespaceRead<R> = NamespaceOwnerRead<RoutedRead<R>>;
pub type SourceNamespaceQuery<Q> = NamespaceOwnerQuery<crate::transfer_source::SourceQuery<Q>>;
pub type SourceNamespaceRead<R> = NamespaceOwnerRead<crate::transfer_source::SourceRead<R>>;
pub type CreatedNamespaceSource<A, P> = CreatedNamespace<A, P, TransferSource<A, P>>;
#[derive(Clone)]
pub struct CreatedNamespace<A, P, O = RoutedApplication<A, P>> {
    plan: NamespacePlan,
    digest: ContentDigest,
    inner: O,
    providers: std::marker::PhantomData<fn() -> (A, P)>,
    binding: Vec<u8>,
    inner_bootstrap: Vec<u8>,
    ready: Option<u64>,
    activation: Option<(u64, NamespacePublicationStatus)>,
}
#[allow(clippy::large_enum_variant)] // Bounded cold control decoding; no extra allocation.
enum Request<'a> {
    Initialize,
    Activate(NamespacePublicationStatus),
    Data(&'a [u8]),
}
impl<A, P> CreatedNamespace<A, P>
where
    A: CheckpointStateMachine + BoundedStateMachine,
    A::Receipt: ApplicationReceipt,
    P: PartitionPolicy + Clone,
{
    #[allow(clippy::result_large_err)]
    pub fn new(
        plan: NamespacePlan,
        app: A,
        policy: P,
        limits: RoutedLimits,
    ) -> Result<Self, (ApplicationError, NamespacePlan, A, P)> {
        let encoded = match plan.encode(MAX_NAMESPACE_PLAN_BYTES) {
            Ok(v) => v,
            Err(e) => return Err((e, plan, app, policy)),
        };
        let initial = match app.checkpoint(
            limits
                .inner_checkpoint_bytes
                .min(MAX_ROUTED_CHECKPOINT_BYTES),
        ) {
            Ok(v) => v,
            Err(e) => return Err((e, plan, app, policy)),
        };
        if 16 + encoded.len() + 68 + manifest_len(&plan.manifest) + initial.len()
            > MAX_ROUTED_COMMAND_BYTES
        {
            return Err((ApplicationError::InvalidCommand, plan, app, policy));
        }
        let inner = match RoutedApplication::new(
            plan.creation.intent.bootstrap.group,
            plan.manifest.clone(),
            app,
            policy,
            limits,
        ) {
            Ok(v) => v,
            Err(r) => return Err((r.error, plan, r.application, r.policy)),
        };
        // The routed bootstrap embeds the checked empty application checkpoint.
        let inner_bootstrap = inner
            .bootstrap_command(MAX_ROUTED_COMMAND_BYTES)
            .expect("validated routed binding");
        let mut binding = Vec::with_capacity(16 + encoded.len() + inner_bootstrap.len());
        binding.extend(b"VBNINIT1");
        binding.extend((encoded.len() as u32).to_le_bytes());
        binding.extend(encoded);
        binding.extend((inner_bootstrap.len() as u32).to_le_bytes());
        binding.extend(&inner_bootstrap);
        Ok(Self {
            providers: std::marker::PhantomData,
            digest: plan.digest().expect("validated plan"),
            plan,
            inner,
            binding,
            inner_bootstrap,
            ready: None,
            activation: None,
        })
    }
}
impl<A, P> CreatedNamespaceSource<A, P>
where
    A: ScopeStateMachine + BoundedStateMachine,
    A::Receipt: ApplicationReceipt,
    P: PartitionPolicy + Clone,
{
    /// Select source capability before initialization. Refusal returns the
    /// original owned plan/source. No live upgrade of a fixed owner is supplied.
    #[allow(clippy::result_large_err)]
    pub fn from_source(
        plan: NamespacePlan,
        source: TransferSource<A, P>,
    ) -> Result<Self, (ApplicationError, NamespacePlan, TransferSource<A, P>)> {
        let prepare = || -> Result<(Vec<u8>, Vec<u8>), ApplicationError> {
            let encoded = plan.encode(MAX_NAMESPACE_PLAN_BYTES)?;
            if source.applied_index() != 0
                || source.routed().is_initialized()
                || source.routed().fence().is_some()
                || source.routed().grant() != &plan.manifest
                || source.routed().local() != plan.creation.intent.bootstrap.group
            {
                return Err(ApplicationError::InvalidCommand);
            }
            let inner_bootstrap = source.bootstrap_command(MAX_ROUTED_COMMAND_BYTES)?;
            let len = 16 + encoded.len() + inner_bootstrap.len();
            if len > MAX_ROUTED_COMMAND_BYTES {
                return Err(ApplicationError::InvalidCommand);
            }
            let mut binding = Vec::with_capacity(len);
            binding.extend(b"VBNINIT2");
            binding.extend((encoded.len() as u32).to_le_bytes());
            binding.extend(encoded);
            binding.extend((inner_bootstrap.len() as u32).to_le_bytes());
            binding.extend(&inner_bootstrap);
            Ok((binding, inner_bootstrap))
        };
        let (binding, inner_bootstrap) = match prepare() {
            Ok(b) => b,
            Err(e) => return Err((e, plan, source)),
        };
        Ok(Self {
            digest: plan.digest().expect("validated plan"),
            plan,
            inner: source,
            providers: std::marker::PhantomData,
            binding,
            inner_bootstrap,
            ready: None,
            activation: None,
        })
    }
}
impl<A, P, O> CreatedNamespace<A, P, O>
where
    A: CheckpointStateMachine + BoundedStateMachine,
    A::Receipt: ApplicationReceipt,
    P: PartitionPolicy + Clone,
    O: NamespaceOwner<A, P>,
{
    pub fn owner(&self) -> &O {
        &self.inner
    }
    pub fn plan(&self) -> &NamespacePlan {
        &self.plan
    }
    pub fn routed(&self) -> &RoutedApplication<A, P> {
        self.inner.routed_owner()
    }
    pub fn status(&self) -> NamespaceStatus {
        NamespaceStatus {
            plan: self.digest,
            ready_index: self.ready,
            activation_index: self.activation.as_ref().map(|a| a.0),
        }
    }
    pub fn initialization_command(&self, max: usize) -> Result<Vec<u8>, ApplicationError> {
        if self.binding.len() > max {
            return Err(ApplicationError::InvalidCommand);
        }
        Ok(self.binding.clone())
    }
    pub fn activation_command(
        &self,
        status: &NamespacePublicationStatus,
        max: usize,
    ) -> Result<Vec<u8>, ApplicationError> {
        self.check_activation(status)?;
        let p = status.publication.encode(MAX_NAMESPACE_PUBLICATION_BYTES)?;
        if 32 + p.len() > max {
            return Err(ApplicationError::InvalidCommand);
        }
        let mut b = Vec::with_capacity(32 + p.len());
        b.extend(b"VBNACTV1");
        b.extend(status.operation.get().to_le_bytes());
        b.extend(status.index.to_le_bytes());
        b.extend(p);
        Ok(b)
    }
    fn check_activation(&self, s: &NamespacePublicationStatus) -> Result<(), ApplicationError> {
        s.publication.matches(&self.plan)?;
        if s.index == 0
            || s.operation == self.plan.creation.operation
            || Some(s.publication.ready_index) != self.ready
            || self.activation.as_ref().is_some_and(|a| &a.1 != s)
        {
            return Err(ApplicationError::InvalidCommand);
        }
        Ok(())
    }
    fn request<'a>(&self, b: &'a [u8]) -> Result<Request<'a>, ApplicationError> {
        if b.len() > MAX_ROUTED_COMMAND_BYTES {
            return Err(ApplicationError::InvalidCommand);
        }
        if b.starts_with(b"VBNINIT1") || b.starts_with(b"VBNINIT2") {
            if b != self.binding {
                return Err(ApplicationError::InvalidCommand);
            }
            Ok(Request::Initialize)
        } else if b.starts_with(b"VBNACTV1") {
            let mut r = Reader::new(b);
            r.take(8)?;
            let operation = r.operation()?;
            let index = r.u64()?;
            let publication = NamespacePublication::decode(r.take(b.len() - 32)?)?;
            Ok(Request::Activate(NamespacePublicationStatus {
                operation,
                index,
                publication,
            }))
        } else {
            if !self.inner.owner_command(b)? {
                return Err(ApplicationError::InvalidCommand);
            }
            Ok(Request::Data(b))
        }
    }
    pub fn readiness_requirements(&self) -> ReadinessRequirements {
        let r = self.inner.owner_requirements();
        ReadinessRequirements {
            application_schema: O::SCHEMA,
            command_bytes: r
                .command_bytes
                .max(self.binding.len())
                .max(32 + MAX_NAMESPACE_PUBLICATION_BYTES),
            snapshot_bytes: r.snapshot_bytes
                + self.binding.len()
                + MAX_NAMESPACE_PUBLICATION_BYTES
                + 80,
        }
    }
    fn reserved(&self, op: OperationId) -> bool {
        op == self.plan.creation.operation
            || self
                .activation
                .as_ref()
                .is_some_and(|a| a.1.operation == op)
    }
}
impl<A, P, O> StateMachine for CreatedNamespace<A, P, O>
where
    A: CheckpointStateMachine + BoundedStateMachine,
    A::Receipt: ApplicationReceipt,
    P: PartitionPolicy + Clone,
    O: NamespaceOwner<A, P>,
{
    type Receipt = NamespaceReceipt<A::Receipt>;
    fn validate_group(&self, g: GroupIdentity) -> Result<(), ApplicationError> {
        self.inner.validate_group(g)
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
            let mut projected = e.clone();
            let mut result = None;
            if let EntryPayload::Command { operation, bytes } = &e.payload {
                projected.payload = EntryPayload::Noop;
                let outcome = match next.request(bytes)? {
                    Request::Initialize => {
                        if *operation != next.plan.creation.operation {
                            NamespaceOutcome::OperationConflict
                        } else {
                            // A frozen source retains the original readiness;
                            // replaying its owner bootstrap must not revive it.
                            if next.ready.is_none() || next.routed().fence().is_none() {
                                projected.payload = EntryPayload::Command {
                                    operation: *operation,
                                    bytes: next.inner_bootstrap.clone(),
                                };
                            }
                            if next.ready.is_none() {
                                next.ready = Some(e.index);
                            }
                            NamespaceOutcome::Ready
                        }
                    }
                    Request::Activate(status) => {
                        if *operation != status.operation || next.check_activation(&status).is_err()
                        {
                            NamespaceOutcome::OperationConflict
                        } else {
                            if next.activation.is_none() {
                                next.activation = Some((e.index, status));
                            }
                            NamespaceOutcome::Activated
                        }
                    }
                    Request::Data(_) => {
                        if next.activation.is_none() {
                            NamespaceOutcome::NotActive
                        } else if next.reserved(*operation) {
                            NamespaceOutcome::OperationConflict
                        } else {
                            projected = e.clone();
                            NamespaceOutcome::NotActive
                        } // replaced by inner receipt below
                    }
                };
                result = Some((*operation, outcome));
            }
            let mut inner = next.inner.apply_batch(std::slice::from_ref(&projected))?;
            if let Some((operation, mut outcome)) = result {
                if matches!(outcome, NamespaceOutcome::Ready)
                    && matches!(projected.payload, EntryPayload::Command { .. })
                {
                    if inner.len() != 1
                        || !matches!(inner.remove(0).outcome, RoutedOutcome::Bootstrapped)
                    {
                        return Err(ApplicationError::InvalidCommand);
                    }
                } else if matches!(projected.payload, EntryPayload::Command { .. }) {
                    if inner.len() != 1 {
                        return Err(ApplicationError::InvalidCommand);
                    }
                    outcome = NamespaceOutcome::Data(inner.remove(0));
                } else if !inner.is_empty() {
                    return Err(ApplicationError::InvalidCommand);
                }
                receipts.push(NamespaceReceipt {
                    index: e.index,
                    operation,
                    outcome,
                });
            } else if !inner.is_empty() {
                return Err(ApplicationError::InvalidCommand);
            }
        }
        *self = next;
        Ok(receipts)
    }
}
impl<A, P, O> BoundedStateMachine for CreatedNamespace<A, P, O>
where
    A: CheckpointStateMachine + BoundedStateMachine,
    A::Receipt: ApplicationReceipt,
    P: PartitionPolicy + Clone,
    O: NamespaceOwner<A, P>,
{
    fn receipt_bytes_bound(&self, entries: &[LogEntry]) -> Result<usize, ApplicationError> {
        let mut projected = Vec::with_capacity(entries.len());
        let mut commands = 0;
        let mut data = 0;
        for e in entries {
            let mut p = e.clone();
            if let EntryPayload::Command { operation, bytes } = &e.payload {
                commands += 1;
                p.payload = EntryPayload::Noop;
                if let Request::Data(b) = self.request(bytes)? {
                    if let Some(payload) = self.inner.application_payload(b)? {
                        data += 1;
                        p.payload = EntryPayload::Command {
                            operation: *operation,
                            bytes: payload.to_vec(),
                        };
                    }
                }
            }
            projected.push(p);
        }
        let inner = self
            .inner
            .routed_owner()
            .application()
            .receipt_bytes_bound(&projected)?;
        inner
            .checked_sub(data * size_of::<A::Receipt>())
            .and_then(|n| n.checked_add(commands * size_of::<Self::Receipt>()))
            .ok_or(ApplicationError::ReceiptBudget)
    }
}
impl<A, P, O> ProposalAdmission for CreatedNamespace<A, P, O>
where
    A: CheckpointStateMachine + ProposalAdmission,
    A::Receipt: ApplicationReceipt,
    P: PartitionPolicy + Clone,
    O: NamespaceOwner<A, P> + ProposalAdmission,
{
    fn validate_proposal<'a>(
        &self,
        op: OperationId,
        b: &[u8],
        pending: impl Iterator<Item = (OperationId, &'a [u8])>,
    ) -> Result<usize, ApplicationError> {
        let mut data = Vec::new();
        for (position, (id, bytes)) in pending.enumerate() {
            if position >= MAX_ROUTED_PENDING {
                return Err(ApplicationError::ReceiptBudget);
            }
            if let Request::Data(b) = self.request(bytes)? {
                data.push((id, b));
            }
        }
        match self.request(b)? {
            Request::Initialize => {
                if op != self.plan.creation.operation {
                    return Err(ApplicationError::InvalidCommand);
                }
                if self.routed().fence().is_none() {
                    self.inner
                        .validate_proposal(op, &self.inner_bootstrap, data.into_iter())?;
                }
                Ok(size_of::<Self::Receipt>())
            }
            Request::Activate(s) => {
                if op != s.operation {
                    return Err(ApplicationError::InvalidCommand);
                }
                self.check_activation(&s)?;
                Ok(size_of::<Self::Receipt>())
            }
            Request::Data(b) => {
                if self.activation.is_none() || self.reserved(op) {
                    return Err(ApplicationError::NotApplied);
                }
                let n = self.inner.validate_proposal(op, b, data.into_iter())?;
                n.checked_sub(size_of::<RoutedReceipt<A::Receipt>>())
                    .and_then(|n| n.checked_add(size_of::<Self::Receipt>()))
                    .ok_or(ApplicationError::ReceiptBudget)
            }
        }
    }
}
impl<A, P, O> ReadableStateMachine for CreatedNamespace<A, P, O>
where
    A: CheckpointStateMachine + BoundedStateMachine + ReadableStateMachine,
    A::Receipt: ApplicationReceipt,
    P: PartitionPolicy + Clone,
    O: NamespaceOwner<A, P> + ReadableStateMachine,
{
    type Query = NamespaceOwnerQuery<O::Query>;
    type ReadResult = NamespaceOwnerRead<O::ReadResult>;
    fn read_at(&self, index: u64, q: Self::Query) -> Result<Self::ReadResult, ApplicationError> {
        if index > self.applied_index() {
            return Err(ApplicationError::NotApplied);
        }
        match q {
            NamespaceOwnerQuery::Status => Ok(NamespaceOwnerRead::Status(self.status())),
            NamespaceOwnerQuery::Data(q) => {
                if self.activation.is_none() {
                    Ok(NamespaceOwnerRead::NotActive)
                } else {
                    self.inner.read_at(index, q).map(NamespaceOwnerRead::Data)
                }
            }
        }
    }
}
impl<A, P, O> BoundedReadableStateMachine for CreatedNamespace<A, P, O>
where
    A: CheckpointStateMachine + BoundedStateMachine + BoundedReadableStateMachine,
    A::Receipt: ApplicationReceipt,
    P: PartitionPolicy + Clone,
    O: NamespaceOwner<A, P> + BoundedReadableStateMachine,
{
    fn query_bytes(&self, q: &Self::Query, limit: usize) -> Result<usize, ApplicationError> {
        match q {
            NamespaceOwnerQuery::Status => Ok(0),
            NamespaceOwnerQuery::Data(q) => self.inner.query_bytes(q, limit),
        }
    }
    fn read_result_bound(&self, q: &Self::Query) -> Result<usize, ApplicationError> {
        let nested = match q {
            NamespaceOwnerQuery::Status => 0,
            NamespaceOwnerQuery::Data(q) => self
                .inner
                .read_result_bound(q)?
                .checked_sub(size_of::<O::ReadResult>())
                .ok_or(ApplicationError::ReceiptBudget)?,
        };
        size_of::<Self::ReadResult>()
            .checked_add(nested)
            .ok_or(ApplicationError::ReceiptBudget)
    }
    fn read_result_bytes(
        &self,
        r: &Self::ReadResult,
        limit: usize,
    ) -> Result<usize, ApplicationError> {
        match r {
            NamespaceOwnerRead::Data(r) => self.inner.read_result_bytes(r, limit),
            _ => Ok(0),
        }
    }
}
impl<A, P, O> CheckpointStateMachine for CreatedNamespace<A, P, O>
where
    A: CheckpointStateMachine + BoundedStateMachine,
    A::Receipt: ApplicationReceipt,
    P: PartitionPolicy + Clone,
    O: NamespaceOwner<A, P>,
{
    fn schema_version(&self) -> u64 {
        O::SCHEMA
    }
    fn checkpoint(&self, max: usize) -> Result<Vec<u8>, ApplicationError> {
        let inner = self
            .inner
            .checkpoint(self.inner.owner_requirements().snapshot_bytes)?;
        let activation = self
            .activation
            .as_ref()
            .map(|(_, s)| s.publication.encode(MAX_NAMESPACE_PUBLICATION_BYTES))
            .transpose()?;
        let len =
            41 + self.binding.len() + inner.len() + activation.as_ref().map_or(0, |p| 36 + p.len());
        if len > max {
            return Err(ApplicationError::InvalidCheckpoint);
        }
        let mut b = Vec::with_capacity(len);
        b.extend(O::CHECKPOINT_MAGIC);
        b.extend(self.applied_index().to_le_bytes());
        b.extend((self.binding.len() as u32).to_le_bytes());
        b.extend(&self.binding);
        b.extend(self.ready.unwrap_or(0).to_le_bytes());
        b.push(u8::from(self.activation.is_some()));
        if let Some((index, s)) = &self.activation {
            b.extend(index.to_le_bytes());
            b.extend(s.operation.get().to_le_bytes());
            b.extend(s.index.to_le_bytes());
            let p = activation.unwrap();
            b.extend((p.len() as u32).to_le_bytes());
            b.extend(p);
        }
        b.extend(self.inner.schema_version().to_le_bytes());
        b.extend((inner.len() as u32).to_le_bytes());
        b.extend(inner);
        Ok(b)
    }
    fn restore_checkpoint(
        &mut self,
        schema: u64,
        applied: u64,
        b: &[u8],
    ) -> Result<(), ApplicationError> {
        if schema != O::SCHEMA {
            return Err(ApplicationError::UnsupportedSchema);
        }
        if b.len() > self.readiness_requirements().snapshot_bytes {
            return Err(ApplicationError::InvalidCheckpoint);
        }
        let mut next = self.clone();
        let mut r = Reader::new(b);
        if r.take(8)? != O::CHECKPOINT_MAGIC || r.u64()? != applied {
            return Err(ApplicationError::InvalidCheckpoint);
        }
        let len = r.u32()? as usize;
        if r.take(len)? != self.binding {
            return Err(ApplicationError::InvalidCheckpoint);
        }
        next.ready = match r.u64()? {
            0 => None,
            i if i <= applied => Some(i),
            _ => return Err(ApplicationError::InvalidCheckpoint),
        };
        next.activation = if r.boolean()? {
            let index = r.u64()?;
            let operation = r.operation()?;
            let metadata_index = r.u64()?;
            let len = r.u32()? as usize;
            let publication = NamespacePublication::decode(r.take(len)?)?;
            let s = NamespacePublicationStatus {
                operation,
                index: metadata_index,
                publication,
            };
            next.activation = None;
            next.check_activation(&s)?;
            if index <= next.ready.unwrap() || index > applied {
                return Err(ApplicationError::InvalidCheckpoint);
            }
            Some((index, s))
        } else {
            None
        };
        let schema = r.u64()?;
        let len = r.u32()? as usize;
        next.inner
            .restore_checkpoint(schema, applied, r.take(len)?)?;
        if !r.done()
            || next.inner.routed_owner().initialization()
                != next.ready.map(|i| (next.plan.creation.operation, i))
            || next
                .activation
                .as_ref()
                .is_some_and(|a| next.inner.routed_owner().has_data_operation(a.1.operation))
            || next.activation.is_none()
                && next.inner.routed_owner().remaining_operations()
                    != next.inner.routed_owner().limits().operations
            || next.routed().fence().is_some_and(|f| {
                next.activation.as_ref().is_none_or(|a| f.index <= a.0)
                    || next.reserved(f.operation)
            })
        {
            return Err(ApplicationError::InvalidCheckpoint);
        }
        *self = next;
        Ok(())
    }
}
