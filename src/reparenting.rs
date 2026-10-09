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
//! Atomic live-child reparenting when all affected metadata is locally owned.
use crate::{
    application::*,
    identity::*,
    routing::{codec::*, *},
};
use std::mem::size_of;
pub const LOCAL_REPARENT_DIRECTORY_SCHEMA: u64 = 11;
pub const MAX_REPARENT_PLAN_BYTES: usize = 20 + 3 * MAX_MANIFEST_BYTES;
/// Exact committed views under one metadata authority. This is a checked shape,
/// not authorization; the directory checks current ancestry and lifecycle state.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct ReparentPlan {
    old_parent: ResponsibilityManifest,
    new_parent: ResponsibilityManifest,
    child: ResponsibilityManifest,
}
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct ReparentStatus {
    pub operation: OperationId,
    pub index: u64,
    pub plan: ReparentPlan,
}
impl ReparentPlan {
    pub fn new(
        old_parent: ResponsibilityManifest,
        new_parent: ResponsibilityManifest,
        child: ResponsibilityManifest,
    ) -> Result<Self, ApplicationError> {
        let plan = Self {
            old_parent,
            new_parent,
            child,
        };
        plan.validate()?;
        Ok(plan)
    }
    fn validate(&self) -> Result<(), ApplicationError> {
        let [old, new, child] =
            [&self.old_parent, &self.new_parent, &self.child].map(|m| m.input());
        if old.responsibility == new.responsibility
            || old.responsibility == child.responsibility
            || new.responsibility == child.responsibility
            || [old, new, child].iter().any(|m| {
                m.authority != old.authority
                    || m.state != ResponsibilityState::Active
                    || m.generation.get() == u64::MAX
                    || m.application != child.application
                    || m.scheme != child.scheme
            })
            || child.parent
                != Some(ParentAuthority {
                    responsibility: old.responsibility,
                    group: old.authority,
                })
        {
            return Err(ApplicationError::InvalidCommand);
        }
        let (ExecutionMode::Delegated(a), ExecutionMode::Delegated(b)) =
            (&old.execution, &new.execution)
        else {
            return Err(ApplicationError::InvalidCommand);
        };
        if !a
            .iter()
            .any(|r| r.scope == child.scope && r.target == self.child_target())
            || !b
                .iter()
                .any(|r| r.scope == child.scope && r.target == RouteTarget::Vacant)
        {
            return Err(ApplicationError::InvalidCommand);
        }
        Ok(())
    }
    fn child_target(&self) -> RouteTarget {
        let c = self.child.input();
        RouteTarget::Child(ChildAuthority {
            responsibility: c.responsibility,
            group: c.authority,
            epoch: c.epoch,
        })
    }
    pub fn old_parent(&self) -> &ResponsibilityManifest {
        &self.old_parent
    }
    pub fn new_parent(&self) -> &ResponsibilityManifest {
        &self.new_parent
    }
    pub fn child(&self) -> &ResponsibilityManifest {
        &self.child
    }
    /// Old parent, new parent, then child. Ownership and descendants are unchanged.
    pub fn updated_manifests(&self) -> [ResponsibilityManifest; 3] {
        let mut old = self.old_parent.clone().into_input();
        let mut new = self.new_parent.clone().into_input();
        let mut child = self.child.clone().into_input();
        for (m, target) in [
            (&mut old, RouteTarget::Vacant),
            (&mut new, self.child_target()),
        ] {
            let ExecutionMode::Delegated(routes) = &mut m.execution else {
                unreachable!("checked plan")
            };
            routes
                .iter_mut()
                .find(|r| r.scope == child.scope)
                .expect("checked scope")
                .target = target;
            m.generation = RouteGeneration::new(m.generation.get() + 1).unwrap();
        }
        child.parent = Some(ParentAuthority {
            responsibility: new.responsibility,
            group: new.authority,
        });
        child.generation = RouteGeneration::new(child.generation.get() + 1).unwrap();
        [old, new, child].map(|m| ResponsibilityManifest::new(m).expect("preserved checked shape"))
    }
    pub fn retained_bytes(&self) -> usize {
        size_of::<Self>()
            + [&self.old_parent, &self.new_parent, &self.child]
                .iter()
                .map(|m| m.retained_bytes() - size_of::<ResponsibilityManifest>())
                .sum::<usize>()
    }
    pub fn encode(&self, maximum: usize) -> Result<Vec<u8>, ApplicationError> {
        let manifests = [&self.old_parent, &self.new_parent, &self.child];
        let len = 20 + manifests.iter().map(|m| manifest_len(m)).sum::<usize>();
        if len > maximum.min(MAX_REPARENT_PLAN_BYTES) {
            return Err(ApplicationError::InvalidCommand);
        }
        let mut b = Vec::with_capacity(len);
        b.extend(b"VBRPAR01");
        for m in manifests {
            b.extend((manifest_len(m) as u32).to_le_bytes());
            put_manifest(&mut b, m);
        }
        Ok(b)
    }
    pub fn decode(bytes: &[u8]) -> Result<Self, ApplicationError> {
        if bytes.len() > MAX_REPARENT_PLAN_BYTES {
            return Err(ApplicationError::InvalidCommand);
        }
        let mut r = Reader::new(bytes);
        if r.take(8)? != b"VBRPAR01" {
            return Err(ApplicationError::InvalidCommand);
        }
        let mut read = || {
            let n = r.u32()? as usize;
            read_manifest(r.take(n)?)
        };
        let plan = Self::new(read()?, read()?, read()?)?;
        if !r.done() {
            return Err(ApplicationError::InvalidCommand);
        }
        Ok(plan)
    }
}
