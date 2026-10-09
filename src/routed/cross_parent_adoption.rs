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
//! Full-owner adoption of a completed guarded cross-authority metadata move.
use super::*;
use crate::{reparent_commit::*, reparent_guard::*};
pub const CROSS_PARENT_ADOPTING_ROUTED_SCHEMA: u64 = 4;
pub const MAX_CROSS_PARENT_ADOPTION_BYTES: usize =
    120 + MAX_REPARENT_GUARD_BYTES + MAX_REPARENT_PUBLISH_BYTES + 112;
const _: () = assert!(MAX_CROSS_PARENT_ADOPTION_BYTES <= MAX_ROUTED_COMMAND_BYTES);
/// Original quorum observations, authenticated by the host. No encoding here
/// establishes that any foreign group actually committed these values.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct CrossOwnerParentAdoption {
    pub plan: CrossReparentPlan,
    pub decision: PublishReparent,
    pub child_configuration: ConfigurationId,
    pub child_publication: ReparentPublicationStatus,
    pub completion: ReleaseCommittedReparent,
}
impl CrossOwnerParentAdoption {
    fn validate(&self) -> Result<(), ApplicationError> {
        let d = &self.decision.decision;
        let c = self.completion.completion;
        let p = self.child_publication;
        let digest = d.digest()?;
        let plan_digest = self.plan.digest();
        let authorities = self.plan.authorities();
        if d.coordinator != self.plan.coordinator()
            || d.commit.guards.len() != authorities.len()
            || d.commit
                .guards
                .iter()
                .zip(authorities)
                .any(|(g, a)| g.authority != a || g.digest != plan_digest)
            || p.authority != self.before().input().authority
            || p.guard != d.commit.guard
            || p.decision_digest != digest
            || p.operation == p.guard
            || p.index == u64::MAX
            || !d
                .commit
                .guards
                .iter()
                .any(|g| g.authority == p.authority && p.index > g.index)
            || (p.authority == d.coordinator && (p.operation != d.operation || p.index != d.index))
            || c.coordinator != d.coordinator
            || c.guard != d.commit.guard
            || c.decision_digest != digest
            || c.index <= d.index
            || c.index == u64::MAX
            || c.operation == d.operation
            || c.operation == c.guard
        {
            return Err(ApplicationError::InvalidCommand);
        }
        Ok(())
    }
    pub fn before(&self) -> &ResponsibilityManifest {
        self.plan.child()
    }
    pub fn after(&self) -> ResponsibilityManifest {
        self.plan.updated_manifests().into_iter().nth(2).unwrap()
    }
    pub fn encode(&self, max: usize) -> Result<Vec<u8>, ApplicationError> {
        self.validate()?;
        let plan = self.plan.encode(MAX_REPARENT_GUARD_BYTES)?;
        let decision = self.decision.encode(MAX_REPARENT_PUBLISH_BYTES)?;
        let completion = self.completion.encode()?;
        let size = 120 + plan.len() + decision.len() + completion.len();
        if size > max.min(MAX_CROSS_PARENT_ADOPTION_BYTES) {
            return Err(ApplicationError::InvalidCommand);
        }
        let mut b = Vec::with_capacity(size);
        b.extend(b"VBXPAD01");
        b.extend((plan.len() as u32).to_le_bytes());
        b.extend(plan);
        b.extend((decision.len() as u32).to_le_bytes());
        b.extend(decision);
        b.extend(completion);
        b.extend(self.child_configuration.get().to_le_bytes());
        let p = self.child_publication;
        put_group(&mut b, p.authority);
        b.extend(p.operation.get().to_le_bytes());
        b.extend(p.index.to_le_bytes());
        b.extend(p.guard.get().to_le_bytes());
        b.extend(p.decision_digest.0);
        Ok(b)
    }
    pub fn decode(bytes: &[u8]) -> Result<Self, ApplicationError> {
        if bytes.len() > MAX_CROSS_PARENT_ADOPTION_BYTES {
            return Err(ApplicationError::InvalidCommand);
        }
        let mut r = Reader::new(bytes);
        if r.take(8)? != b"VBXPAD01" {
            return Err(ApplicationError::InvalidCommand);
        }
        let n = r.u32()? as usize;
        let plan = CrossReparentPlan::decode(r.take(n)?)?;
        let n = r.u32()? as usize;
        let decision = PublishReparent::decode(r.take(n)?)?;
        let completion = ReleaseCommittedReparent::decode(r.take(112)?)?;
        let child_configuration =
            ConfigurationId::new(r.u64()?).ok_or(ApplicationError::InvalidCommand)?;
        let child_publication = ReparentPublicationStatus {
            authority: r.group()?,
            operation: r.operation()?,
            index: r.u64()?,
            guard: r.operation()?,
            decision_digest: crate::transfer::ContentDigest(r.take(32)?.try_into().unwrap()),
        };
        if !r.done() {
            return Err(ApplicationError::InvalidCommand);
        }
        let a = Self {
            plan,
            decision,
            child_configuration,
            child_publication,
            completion,
        };
        a.validate()?;
        Ok(a)
    }
}
