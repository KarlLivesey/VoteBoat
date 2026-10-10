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
//! Foreign parent/child locator refresh after a committed metadata move.
use super::*;

/// Fits the directory's existing bounded control-command reserve. A larger
/// complete plan is refused; it is never projected or truncated to fit.
pub const MAX_METADATA_LOCATOR_BYTES: usize = MAX_DIRECTORY_CONTROL_BYTES;

pub(crate) fn validate_move_observation(
    plan: &MetadataMovePlan,
    activation: MetadataActivationStatus,
) -> Result<(), ApplicationError> {
    activation.encode()?;
    let source = activation.publication.imported.source;
    if source.source != plan.source()
        || source.target != plan.target()
        || source.plan_digest != ContentDigest::sha256(&plan.encode(MAX_METADATA_PLAN_BYTES)?)
    {
        return Err(ApplicationError::InvalidCommand);
    }
    Ok(())
}

/// Host-authenticated original move observations plus an exact foreign view.
/// Encoding is not a commitment certificate. This never moves data ownership.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct MetadataLocatorUpdate {
    before: ResponsibilityManifest,
    plan: MetadataMovePlan,
    activation: MetadataActivationStatus,
}
impl MetadataLocatorUpdate {
    pub fn new(
        before: ResponsibilityManifest,
        plan: MetadataMovePlan,
        activation: MetadataActivationStatus,
    ) -> Result<Self, ApplicationError> {
        let value = Self {
            before,
            plan,
            activation,
        };
        value.checked_after()?;
        // Charge the complete encoding at construction as well as decoding.
        value.encode(MAX_METADATA_LOCATOR_BYTES)?;
        Ok(value)
    }
    pub fn before(&self) -> &ResponsibilityManifest {
        &self.before
    }
    pub fn plan(&self) -> &MetadataMovePlan {
        &self.plan
    }
    pub fn activation(&self) -> MetadataActivationStatus {
        self.activation
    }
    pub fn after(&self) -> ResponsibilityManifest {
        self.checked_after()
            .expect("validated immutable locator update")
    }
    fn checked_after(&self) -> Result<ResponsibilityManifest, ApplicationError> {
        validate_move_observation(&self.plan, self.activation)?;
        let old = self.before.input();
        if old.state != ResponsibilityState::Active
            || old.authority.id == self.plan.source.id
            || old.authority.id == self.plan.target.id
            || self
                .plan
                .manifests
                .iter()
                .any(|m| m.input().responsibility.id == old.responsibility.id)
        {
            return Err(ApplicationError::InvalidCommand);
        }
        let mut next = old.clone();
        let mut changed = false;
        if let Some(parent) = &mut next.parent {
            if parent.group.id == self.plan.source.id {
                let moved = self
                    .plan
                    .manifests
                    .iter()
                    .find(|m| m.input().responsibility == parent.responsibility)
                    .ok_or(ApplicationError::InvalidCommand)?;
                if parent.group != self.plan.source || !delegates_foreign(moved, &self.before) {
                    return Err(ApplicationError::InvalidCommand);
                }
                parent.group = self.plan.target;
                changed = true;
            }
        }
        if let ExecutionMode::Delegated(routes) = &mut next.execution {
            for route in routes {
                if let RouteTarget::Child(child) = &mut route.target {
                    if child.group.id == self.plan.source.id {
                        let moved = self
                            .plan
                            .manifests
                            .iter()
                            .find(|m| m.input().responsibility == child.responsibility)
                            .ok_or(ApplicationError::InvalidCommand)?;
                        if child.group != self.plan.source
                            || moved.input().parent
                                != Some(ParentAuthority {
                                    responsibility: old.responsibility,
                                    group: old.authority,
                                })
                            || moved.input().scope != route.scope
                            || moved.input().epoch != child.epoch
                            || moved.input().application != old.application
                            || moved.input().scheme != old.scheme
                        {
                            return Err(ApplicationError::InvalidCommand);
                        }
                        child.group = self.plan.target;
                        changed = true;
                    }
                }
            }
        }
        if !changed {
            return Err(ApplicationError::InvalidCommand);
        }
        next.generation = RouteGeneration::new(
            old.generation
                .get()
                .checked_add(1)
                .ok_or(ApplicationError::InvalidCommand)?,
        )
        .ok_or(ApplicationError::InvalidCommand)?;
        ResponsibilityManifest::new(next).map_err(|_| ApplicationError::InvalidCommand)
    }
    pub fn encode(&self, maximum: usize) -> Result<Vec<u8>, ApplicationError> {
        self.checked_after()?;
        let plan = self.plan.encode(MAX_METADATA_PLAN_BYTES)?;
        let mut before = Vec::with_capacity(manifest_len(&self.before));
        put_manifest(&mut before, &self.before);
        let len = 16 + METADATA_ACTIVATION_STATUS_BYTES + before.len() + plan.len();
        if len > maximum.min(MAX_METADATA_LOCATOR_BYTES) {
            return Err(ApplicationError::InvalidCommand);
        }
        let mut bytes = Vec::with_capacity(len);
        bytes.extend(b"VBMLUP01");
        bytes.extend((before.len() as u32).to_le_bytes());
        bytes.extend(before);
        bytes.extend(self.activation.encode()?);
        bytes.extend((plan.len() as u32).to_le_bytes());
        bytes.extend(plan);
        Ok(bytes)
    }
    pub fn decode(bytes: &[u8]) -> Result<Self, ApplicationError> {
        if bytes.len() > MAX_METADATA_LOCATOR_BYTES {
            return Err(ApplicationError::InvalidCommand);
        }
        let mut r = Reader::new(bytes);
        if r.take(8)? != b"VBMLUP01" {
            return Err(ApplicationError::InvalidCommand);
        }
        let n = r.u32()? as usize;
        let before = read_manifest(r.take(n)?)?;
        let activation =
            MetadataActivationStatus::decode(r.take(METADATA_ACTIVATION_STATUS_BYTES)?)?;
        let n = r.u32()? as usize;
        let plan = MetadataMovePlan::decode(r.take(n)?)?;
        if !r.done() {
            return Err(ApplicationError::InvalidCommand);
        }
        Self::new(before, plan, activation)
    }
}
fn delegates_foreign(parent: &ResponsibilityManifest, child: &ResponsibilityManifest) -> bool {
    let (p, c) = (parent.input(), child.input());
    p.application == c.application
        && p.scheme == c.scheme
        && matches!(&p.execution,
        ExecutionMode::Delegated(routes) if routes.iter().any(|r| r.scope == c.scope && r.target == RouteTarget::Child(ChildAuthority {
            responsibility: c.responsibility, group: c.authority, epoch: c.epoch
        })))
}

/// A local applied diagnostic until obtained through the original authority's
/// authenticated quorum read. Indices retain their separate original domains.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub struct MetadataLocatorStatus {
    pub operation: OperationId,
    pub index: u64,
    pub authority: GroupIdentity,
    pub responsibility: ResponsibilityIdentity,
    pub generation: RouteGeneration,
    pub command_digest: ContentDigest,
    pub activation: MetadataActivationStatus,
}
