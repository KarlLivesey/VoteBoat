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
//! Reserved parent route publication for a child's checked ownership transfer.
//! Foreign quorum observations/configurations require authenticated host provenance.
use crate::{
    application::*,
    identity::*,
    routing::{codec::*, *},
    transfer::*,
    transfer_publication::*,
};
use std::mem::size_of;
pub const MAX_DELEGATION_PLAN_BYTES: usize = 64 * 1024;
pub const MAX_DELEGATION_COMPLETION_BYTES: usize = MAX_TRANSFER_PUBLICATION_BYTES + 96;
/// Compact provenance bound into the child's intent. Parent authority is the
/// child's immutable ParentAuthority; the digest binds the exact reserved parent.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub struct DelegationBinding {
    pub parent_digest: ContentDigest,
    pub plan_digest: ContentDigest,
    pub parent_generation: RouteGeneration,
    pub operation: OperationId,
    pub index: u64,
    pub configuration: ConfigurationId,
    pub child_operation: OperationId,
}
impl DelegationBinding {
    pub(crate) fn write(&self, out: &mut Vec<u8>) {
        out.extend(self.parent_digest.0);
        out.extend(self.plan_digest.0);
        out.extend(self.parent_generation.get().to_le_bytes());
        out.extend(self.operation.get().to_le_bytes());
        out.extend(self.index.to_le_bytes());
        out.extend(self.configuration.get().to_le_bytes());
        out.extend(self.child_operation.get().to_le_bytes());
    }
    pub(crate) fn read(r: &mut Reader<'_>) -> Result<Self, ApplicationError> {
        Ok(Self {
            parent_digest: ContentDigest(
                r.take(32)?
                    .try_into()
                    .map_err(|_| ApplicationError::InvalidCommand)?,
            ),
            plan_digest: ContentDigest(
                r.take(32)?
                    .try_into()
                    .map_err(|_| ApplicationError::InvalidCommand)?,
            ),
            parent_generation: RouteGeneration::new(r.u64()?)
                .ok_or(ApplicationError::InvalidCommand)?,
            operation: r.operation()?,
            index: r.u64()?,
            configuration: ConfigurationId::new(r.u64()?)
                .ok_or(ApplicationError::InvalidCommand)?,
            child_operation: r.operation()?,
        })
    }
    pub(crate) fn digest_for(
        &self,
        before: &ResponsibilityManifest,
        after: &ResponsibilityManifest,
    ) -> ContentDigest {
        let mut bytes = Vec::with_capacity(88 + manifest_len(before) + manifest_len(after));
        bytes.extend(self.parent_digest.0);
        bytes.extend(self.parent_generation.get().to_le_bytes());
        bytes.extend(self.operation.get().to_le_bytes());
        bytes.extend(self.index.to_le_bytes());
        bytes.extend(self.configuration.get().to_le_bytes());
        bytes.extend(self.child_operation.get().to_le_bytes());
        put_manifest(&mut bytes, before);
        put_manifest(&mut bytes, after);
        ContentDigest::sha256(&bytes)
    }
}
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct DelegationPlan {
    parent: ResponsibilityManifest,
    before: ResponsibilityManifest,
    after: ResponsibilityManifest,
    child_operation: OperationId,
}
impl DelegationPlan {
    pub fn new(
        parent: ResponsibilityManifest,
        before: ResponsibilityManifest,
        after: ResponsibilityManifest,
        child_operation: OperationId,
    ) -> Result<Self, ApplicationError> {
        TransferIntent::validate_shape(&before, &after)
            .map_err(|_| ApplicationError::InvalidCommand)?;
        let p = parent.input();
        let b = before.input();
        let ExecutionMode::Delegated(routes) = &p.execution else {
            return Err(ApplicationError::InvalidCommand);
        };
        if p.state != ResponsibilityState::Active
            || p.generation.get() == u64::MAX
            || b.parent
                != Some(ParentAuthority {
                    responsibility: p.responsibility,
                    group: p.authority,
                })
            || p.scheme != b.scheme
            || !routes.iter().any(|r| {
                r.scope == b.scope
                    && r.target
                        == RouteTarget::Child(ChildAuthority {
                            responsibility: b.responsibility,
                            group: b.authority,
                            epoch: b.epoch,
                        })
            })
        {
            return Err(ApplicationError::InvalidCommand);
        }
        Ok(Self {
            parent,
            before,
            after,
            child_operation,
        })
    }
    pub fn parent(&self) -> &ResponsibilityManifest {
        &self.parent
    }
    pub fn before(&self) -> &ResponsibilityManifest {
        &self.before
    }
    pub fn after(&self) -> &ResponsibilityManifest {
        &self.after
    }
    pub fn child_operation(&self) -> OperationId {
        self.child_operation
    }
    fn parent_digest(&self) -> ContentDigest {
        let mut bytes = Vec::with_capacity(manifest_len(&self.parent));
        put_manifest(&mut bytes, &self.parent);
        ContentDigest::sha256(&bytes)
    }
    pub fn encode(&self, limit: usize) -> Result<Vec<u8>, ApplicationError> {
        let len = 36
            + manifest_len(&self.parent)
            + manifest_len(&self.before)
            + manifest_len(&self.after);
        if len > limit || len > MAX_DELEGATION_PLAN_BYTES {
            return Err(ApplicationError::InvalidCommand);
        }
        let mut out = Vec::with_capacity(len);
        out.extend(b"VBDPLAN1");
        out.extend(self.child_operation.get().to_le_bytes());
        for manifest in [&self.parent, &self.before, &self.after] {
            out.extend((manifest_len(manifest) as u32).to_le_bytes());
            put_manifest(&mut out, manifest);
        }
        Ok(out)
    }
    pub fn decode(bytes: &[u8]) -> Result<Self, ApplicationError> {
        if bytes.len() > MAX_DELEGATION_PLAN_BYTES {
            return Err(ApplicationError::InvalidCommand);
        }
        let mut r = Reader::new(bytes);
        if r.take(8)? != b"VBDPLAN1" {
            return Err(ApplicationError::InvalidCommand);
        }
        let operation = r.operation()?;
        let len = r.u32()? as usize;
        let parent = read_manifest(r.take(len)?)?;
        let len = r.u32()? as usize;
        let before = read_manifest(r.take(len)?)?;
        let len = r.u32()? as usize;
        let after = read_manifest(r.take(len)?)?;
        if !r.done() {
            return Err(ApplicationError::InvalidCommand);
        }
        Self::new(parent, before, after, operation)
    }
    pub fn retained_bytes(&self) -> usize {
        size_of::<Self>()
            + [&self.parent, &self.before, &self.after]
                .iter()
                .map(|m| m.retained_bytes() - size_of::<ResponsibilityManifest>())
                .sum::<usize>()
    }
    pub(crate) fn updated_parent(&self) -> Result<ResponsibilityManifest, ApplicationError> {
        let mut input = self.parent.clone().into_input();
        input.generation = RouteGeneration::new(
            input
                .generation
                .get()
                .checked_add(1)
                .ok_or(ApplicationError::InvalidCommand)?,
        )
        .ok_or(ApplicationError::InvalidCommand)?;
        let ExecutionMode::Delegated(routes) = &mut input.execution else {
            unreachable!("checked plan")
        };
        let child = self.after.input();
        let route = routes.iter_mut().find(|r| matches!(r.target, RouteTarget::Child(c) if c.responsibility == child.responsibility)).ok_or(ApplicationError::InvalidCommand)?;
        route.target = RouteTarget::Child(ChildAuthority {
            responsibility: child.responsibility,
            group: child.authority,
            epoch: child.epoch,
        });
        ResponsibilityManifest::new(input).map_err(|_| ApplicationError::InvalidCommand)
    }
}
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct DelegationReservationStatus {
    pub operation: OperationId,
    pub index: u64,
    pub plan: DelegationPlan,
}
impl DelegationReservationStatus {
    /// Caller authenticates the parent quorum observation and its configuration.
    pub fn child_intent(
        &self,
        configuration: ConfigurationId,
    ) -> Result<TransferIntent, ApplicationError> {
        let mut binding = DelegationBinding {
            parent_digest: self.plan.parent_digest(),
            parent_generation: self.plan.parent.input().generation,
            plan_digest: ContentDigest([0; 32]),
            operation: self.operation,
            index: self.index,
            configuration,
            child_operation: self.plan.child_operation,
        };
        binding.plan_digest = binding.digest_for(&self.plan.before, &self.plan.after);
        TransferIntent::delegated(self.plan.before.clone(), self.plan.after.clone(), binding)
    }
}
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct DelegationCompletion {
    pub reservation: OperationId,
    pub reservation_index: u64,
    pub parent_configuration: ConfigurationId,
    pub child_configuration: ConfigurationId,
    pub decision: TransferPublicationStatus,
}
impl DelegationCompletion {
    pub fn encode(&self, limit: usize) -> Result<Vec<u8>, ApplicationError> {
        if self.reservation_index == 0 || self.reservation_index == u64::MAX {
            return Err(ApplicationError::InvalidCommand);
        }
        let body = self.decision.encode(MAX_TRANSFER_PUBLICATION_BYTES + 36)?;
        let len = 52 + body.len();
        if len > limit || len > MAX_DELEGATION_COMPLETION_BYTES {
            return Err(ApplicationError::InvalidCommand);
        }
        let mut out = Vec::with_capacity(len);
        out.extend(b"VBDCOMP1");
        out.extend(self.reservation.get().to_le_bytes());
        out.extend(self.reservation_index.to_le_bytes());
        out.extend(self.parent_configuration.get().to_le_bytes());
        out.extend(self.child_configuration.get().to_le_bytes());
        out.extend((body.len() as u32).to_le_bytes());
        out.extend(body);
        Ok(out)
    }
    pub fn decode(bytes: &[u8]) -> Result<Self, ApplicationError> {
        if bytes.len() > MAX_DELEGATION_COMPLETION_BYTES {
            return Err(ApplicationError::InvalidCommand);
        }
        let mut r = Reader::new(bytes);
        if r.take(8)? != b"VBDCOMP1" {
            return Err(ApplicationError::InvalidCommand);
        }
        let reservation = r.operation()?;
        let reservation_index = r.u64()?;
        if reservation_index == 0 || reservation_index == u64::MAX {
            return Err(ApplicationError::InvalidCommand);
        }
        let parent_configuration =
            ConfigurationId::new(r.u64()?).ok_or(ApplicationError::InvalidCommand)?;
        let child_configuration =
            ConfigurationId::new(r.u64()?).ok_or(ApplicationError::InvalidCommand)?;
        let len = r.u32()? as usize;
        let decision = TransferPublicationStatus::decode(r.take(len)?)?;
        if !r.done() {
            return Err(ApplicationError::InvalidCommand);
        }
        Ok(Self {
            reservation,
            reservation_index,
            parent_configuration,
            child_configuration,
            decision,
        })
    }
    pub fn retained_bytes(&self) -> usize {
        size_of::<Self>() + self.decision.publication.retained_bytes()
            - size_of::<TransferPublication>()
    }
}
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct DelegationPublicationStatus {
    pub operation: OperationId,
    pub index: u64,
    pub completion: DelegationCompletion,
}

pub const MAX_DELEGATION_DECLINE_BYTES: usize = MAX_TRANSFER_INTENT_BYTES + 12;
pub const MAX_DELEGATION_DECLINE_STATUS_BYTES: usize = MAX_DELEGATION_DECLINE_BYTES + 36;
pub const MAX_DELEGATION_CANCEL_BYTES: usize = MAX_DELEGATION_DECLINE_STATUS_BYTES + 52;

/// Request to permanently refuse one parent-bound child operation before any
/// successful child intent exists. Encoding alone is not a committed refusal.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct DelegationDecline {
    intent: TransferIntent,
}
impl DelegationDecline {
    pub fn new(intent: TransferIntent) -> Result<Self, ApplicationError> {
        if intent.delegation().is_none() {
            return Err(ApplicationError::InvalidCommand);
        }
        Ok(Self { intent })
    }
    pub fn intent(&self) -> &TransferIntent {
        &self.intent
    }
    pub fn encode(&self, limit: usize) -> Result<Vec<u8>, ApplicationError> {
        let body = self.intent.encode(MAX_TRANSFER_INTENT_BYTES)?;
        let len = 12 + body.len();
        if len > limit || len > MAX_DELEGATION_DECLINE_BYTES {
            return Err(ApplicationError::InvalidCommand);
        }
        let mut out = Vec::with_capacity(len);
        out.extend(b"VBDDECL1");
        out.extend((body.len() as u32).to_le_bytes());
        out.extend(body);
        Ok(out)
    }
    pub fn decode(bytes: &[u8]) -> Result<Self, ApplicationError> {
        if bytes.len() > MAX_DELEGATION_DECLINE_BYTES {
            return Err(ApplicationError::InvalidCommand);
        }
        let mut r = Reader::new(bytes);
        if r.take(8)? != b"VBDDECL1" {
            return Err(ApplicationError::InvalidCommand);
        }
        let len = r.u32()? as usize;
        let intent = TransferIntent::decode(r.take(len)?)?;
        if !r.done() {
            return Err(ApplicationError::InvalidCommand);
        }
        Self::new(intent)
    }
    pub fn retained_bytes(&self) -> usize {
        size_of::<Self>() + self.intent.retained_bytes() - size_of::<TransferIntent>()
    }
}
/// Actual original committed/applied child refusal. Foreign provenance must be
/// authenticated by the host; an arbitrary constructed value proves nothing.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct DelegationDeclineStatus {
    pub operation: OperationId,
    pub index: u64,
    pub decline: DelegationDecline,
}
impl DelegationDeclineStatus {
    fn validate(&self) -> Result<(), ApplicationError> {
        if self.index == 0
            || self.index == u64::MAX
            || self.operation
                == self
                    .decline
                    .intent
                    .delegation()
                    .expect("validated decline")
                    .child_operation
        {
            return Err(ApplicationError::InvalidCommand);
        }
        Ok(())
    }
    pub fn encode(&self, limit: usize) -> Result<Vec<u8>, ApplicationError> {
        self.validate()?;
        let body = self.decline.encode(MAX_DELEGATION_DECLINE_BYTES)?;
        let len = 36 + body.len();
        if len > limit || len > MAX_DELEGATION_DECLINE_STATUS_BYTES {
            return Err(ApplicationError::InvalidCommand);
        }
        let mut out = Vec::with_capacity(len);
        out.extend(b"VBDREFS1");
        out.extend(self.operation.get().to_le_bytes());
        out.extend(self.index.to_le_bytes());
        out.extend((body.len() as u32).to_le_bytes());
        out.extend(body);
        Ok(out)
    }
    pub fn decode(bytes: &[u8]) -> Result<Self, ApplicationError> {
        if bytes.len() > MAX_DELEGATION_DECLINE_STATUS_BYTES {
            return Err(ApplicationError::InvalidCommand);
        }
        let mut r = Reader::new(bytes);
        if r.take(8)? != b"VBDREFS1" {
            return Err(ApplicationError::InvalidCommand);
        }
        let operation = r.operation()?;
        let index = r.u64()?;
        let len = r.u32()? as usize;
        let decline = DelegationDecline::decode(r.take(len)?)?;
        if !r.done() {
            return Err(ApplicationError::InvalidCommand);
        }
        let status = Self {
            operation,
            index,
            decline,
        };
        status.validate()?;
        Ok(status)
    }
    pub fn retained_bytes(&self) -> usize {
        size_of::<Self>() + self.decline.retained_bytes() - size_of::<DelegationDecline>()
    }
}
/// Parent release request. Only an exact retained reservation and an actual
/// permanent child refusal can permit the ordered cancellation transition.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct DelegationCancellation {
    pub reservation: OperationId,
    pub reservation_index: u64,
    pub parent_configuration: ConfigurationId,
    pub child_configuration: ConfigurationId,
    pub decline: DelegationDeclineStatus,
}
impl DelegationCancellation {
    pub fn encode(&self, limit: usize) -> Result<Vec<u8>, ApplicationError> {
        if self.reservation_index == 0 || self.reservation_index == u64::MAX {
            return Err(ApplicationError::InvalidCommand);
        }
        let body = self.decline.encode(MAX_DELEGATION_DECLINE_STATUS_BYTES)?;
        let len = 52 + body.len();
        if len > limit || len > MAX_DELEGATION_CANCEL_BYTES {
            return Err(ApplicationError::InvalidCommand);
        }
        let mut out = Vec::with_capacity(len);
        out.extend(b"VBDCANC1");
        out.extend(self.reservation.get().to_le_bytes());
        out.extend(self.reservation_index.to_le_bytes());
        out.extend(self.parent_configuration.get().to_le_bytes());
        out.extend(self.child_configuration.get().to_le_bytes());
        out.extend((body.len() as u32).to_le_bytes());
        out.extend(body);
        Ok(out)
    }
    pub fn decode(bytes: &[u8]) -> Result<Self, ApplicationError> {
        if bytes.len() > MAX_DELEGATION_CANCEL_BYTES {
            return Err(ApplicationError::InvalidCommand);
        }
        let mut r = Reader::new(bytes);
        if r.take(8)? != b"VBDCANC1" {
            return Err(ApplicationError::InvalidCommand);
        }
        let reservation = r.operation()?;
        let reservation_index = r.u64()?;
        if reservation_index == 0 || reservation_index == u64::MAX {
            return Err(ApplicationError::InvalidCommand);
        }
        let parent_configuration =
            ConfigurationId::new(r.u64()?).ok_or(ApplicationError::InvalidCommand)?;
        let child_configuration =
            ConfigurationId::new(r.u64()?).ok_or(ApplicationError::InvalidCommand)?;
        let len = r.u32()? as usize;
        let decline = DelegationDeclineStatus::decode(r.take(len)?)?;
        if !r.done() {
            return Err(ApplicationError::InvalidCommand);
        }
        Ok(Self {
            reservation,
            reservation_index,
            parent_configuration,
            child_configuration,
            decline,
        })
    }
    pub fn retained_bytes(&self) -> usize {
        size_of::<Self>() + self.decline.retained_bytes() - size_of::<DelegationDeclineStatus>()
    }
}
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct DelegationCancellationStatus {
    pub operation: OperationId,
    pub index: u64,
    pub cancellation: DelegationCancellation,
}
