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
//! Explicit parent-to-child ownership mapping; references require host provenance.
use super::*;
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct InsertionChild {
    pub manifest: ResponsibilityManifest,
    pub creation: OperationId,
    pub creation_index: u64,
    pub configuration: ConfigurationId,
}
impl InsertionChild {
    /// Compact reference to a committed Staging reservation, not a certificate.
    #[allow(clippy::result_large_err)]
    pub fn from_creation(
        manifest: ResponsibilityManifest,
        creation: &GroupCreationStatus,
    ) -> Result<Self, (ApplicationError, ResponsibilityManifest)> {
        let c = &creation.intent;
        let m = manifest.input();
        if creation.index == 0
            || creation.index == u64::MAX
            || c.mode != GroupCreationMode::Staging
            || m.responsibility != c.responsibility
            || m.authority != c.authority
            || m.parent
                != Some(ParentAuthority {
                    responsibility: c.parent,
                    group: c.authority,
                })
            || m.application != c.application
            || m.execution != ExecutionMode::Single(c.bootstrap.group)
            || m.placement.minimum_voting_domains > c.bootstrap.voter_stores.len()
        {
            return Err((ApplicationError::InvalidCommand, manifest));
        }
        Ok(Self {
            manifest,
            creation: creation.operation,
            creation_index: creation.index,
            configuration: c.bootstrap.configuration,
        })
    }
}
impl TransferIntent {
    /// Move a root's complete data scope to fresh same-authority child groups.
    /// Source fences retain the parent's identity; target service uses each exact
    /// child identity. Publication must atomically install parent and children.
    #[allow(clippy::result_large_err)]
    pub fn insert_children(
        before: ResponsibilityManifest,
        after: ResponsibilityManifest,
        children: Vec<InsertionChild>,
    ) -> Result<
        Self,
        (
            ApplicationError,
            ResponsibilityManifest,
            ResponsibilityManifest,
            Vec<InsertionChild>,
        ),
    > {
        let validate = || -> Result<(), ApplicationError> {
            let b = before.input();
            let a = after.input();
            let ExecutionMode::Delegated(routes) = &a.execution else {
                return Err(ApplicationError::InvalidCommand);
            };
            if b.parent.is_some()
                || a.parent != b.parent
                || a.responsibility != b.responsibility
                || a.authority != b.authority
                || a.application != b.application
                || a.scheme != b.scheme
                || a.scope != b.scope
                || a.placement != b.placement
                || b.state != ResponsibilityState::Active
                || a.state != ResponsibilityState::Active
                || b.epoch.get().checked_add(1) != Some(a.epoch.get())
                || b.generation.get().checked_add(1) != Some(a.generation.get())
                || children.is_empty()
                || children.len() != routes.len()
                || children.len() > MAX_MANIFEST_ROUTES
                || children.capacity() > MAX_MANIFEST_ROUTES
            {
                return Err(ApplicationError::InvalidCommand);
            }
            let sources = Self::groups(&before).map_err(|_| ApplicationError::InvalidCommand)?;
            let mut ids = BTreeSet::new();
            let mut groups = BTreeSet::new();
            let mut operations = BTreeSet::new();
            for (child, route) in children.iter().zip(routes) {
                let c = child.manifest.input();
                let ExecutionMode::Single(g) = c.execution else {
                    return Err(ApplicationError::InvalidCommand);
                };
                if c.parent
                    != Some(ParentAuthority {
                        responsibility: b.responsibility,
                        group: b.authority,
                    })
                    || c.authority != b.authority
                    || c.application != b.application
                    || c.scheme != b.scheme
                    || c.scope != route.scope
                    || c.epoch.get() != 1
                    || c.generation.get() != 1
                    || c.state != ResponsibilityState::Active
                    || c.responsibility.id == b.responsibility.id
                    || g.id == b.authority.id
                    || sources.iter().any(|s| match s.target {
                        RouteTarget::Group(s) => s.id == g.id,
                        _ => true,
                    })
                    || !ids.insert(c.responsibility.id)
                    || !groups.insert(g.id)
                    || !operations.insert(child.creation)
                    || child.creation_index == 0
                    || child.creation_index == u64::MAX
                    || route.target
                        != RouteTarget::Child(ChildAuthority {
                            responsibility: c.responsibility,
                            group: c.authority,
                            epoch: c.epoch,
                        })
                {
                    return Err(ApplicationError::InvalidCommand);
                }
            }
            Ok(())
        };
        if let Err(e) = validate() {
            return Err((e, before, after, children));
        }
        Ok(Self {
            before,
            after,
            delegation: None,
            insertion: Some(children),
        })
    }
    pub fn insertion_children(&self) -> Option<&[InsertionChild]> {
        self.insertion.as_deref()
    }
    /// Checked grant for a concrete target. For insertion this is the fresh child;
    /// for ordinary split/merge it remains the original after manifest.
    pub fn target_manifest(&self, group: GroupIdentity) -> Option<&ResponsibilityManifest> {
        if let Some(children) = &self.insertion {
            children
                .iter()
                .find(|c| c.manifest.input().execution == ExecutionMode::Single(group))
                .map(|c| &c.manifest)
        } else {
            match &self.after.input().execution {
                ExecutionMode::Single(g) if *g == group => Some(&self.after),
                ExecutionMode::Partitioned(routes)
                    if routes.iter().any(|r| r.target == RouteTarget::Group(group)) =>
                {
                    Some(&self.after)
                }
                _ => None,
            }
        }
    }
}
