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
    pub(crate) fn insertion_candidate(
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
        Self::insertion_with_foreign_parent(before, after, children, false)
    }
    #[allow(clippy::result_large_err)]
    pub(crate) fn insertion_with_foreign_parent(
        before: ResponsibilityManifest,
        after: ResponsibilityManifest,
        children: Vec<InsertionChild>,
        foreign_parent: bool,
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
            if b.parent.is_some_and(|p| p.group != b.authority) != foreign_parent
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
                    || b.parent
                        .is_some_and(|p| p.responsibility.id == c.responsibility.id)
                    || g.id == b.authority.id
                    || b.parent.is_some_and(|p| g.id == p.group.id)
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
            retained: false,
        })
    }
    /// Root insertion; delegated sources require a checked parent reservation.
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
        if before.input().parent.is_some() {
            return Err((ApplicationError::InvalidCommand, before, after, children));
        }
        Self::insertion_candidate(before, after, children)
    }
    pub(crate) fn delegated_insertion(
        before: ResponsibilityManifest,
        after: ResponsibilityManifest,
        children: Vec<InsertionChild>,
        binding: crate::delegation::DelegationBinding,
    ) -> Result<Self, ApplicationError> {
        let foreign = before
            .input()
            .parent
            .is_some_and(|p| p.group != before.input().authority);
        let mut intent = Self::insertion_with_foreign_parent(before, after, children, foreign)
            .map_err(|e| e.0)?;
        let children = intent.insertion_children().expect("checked insertion");
        if intent.before.input().parent.is_none()
            || binding.index == 0
            || binding.index == u64::MAX
            || binding.operation == binding.child_operation
            || children
                .iter()
                .any(|c| c.creation == binding.operation || c.creation == binding.child_operation)
            || binding.plan_digest
                != binding.digest_insertion_for(&intent.before, &intent.after, children)
        {
            return Err(ApplicationError::InvalidCommand);
        }
        intent.delegation = Some(binding);
        Ok(intent)
    }
    pub fn insertion_children(&self) -> Option<&[InsertionChild]> {
        self.insertion.as_deref()
    }
    pub(crate) fn cross_authority_insertion(&self) -> bool {
        self.insertion.is_some()
            && self
                .before
                .input()
                .parent
                .is_some_and(|p| p.group != self.before.input().authority)
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

// Shared canonical mapping used by intents, reservations and their content binding.
pub(crate) fn insertion_len(children: &[InsertionChild]) -> usize {
    2 + children
        .iter()
        .map(|c| 36 + manifest_len(&c.manifest))
        .sum::<usize>()
}
pub(crate) fn put_insertion(out: &mut Vec<u8>, children: &[InsertionChild]) {
    out.extend((children.len() as u16).to_le_bytes());
    for c in children {
        out.extend(c.creation.get().to_le_bytes());
        out.extend(c.creation_index.to_le_bytes());
        out.extend(c.configuration.get().to_le_bytes());
        out.extend((manifest_len(&c.manifest) as u32).to_le_bytes());
        put_manifest(out, &c.manifest);
    }
}
pub(crate) fn read_insertion(r: &mut Reader<'_>) -> Result<Vec<InsertionChild>, ApplicationError> {
    let count = usize::from(r.u16()?);
    if count == 0 || count > MAX_MANIFEST_ROUTES {
        return Err(ApplicationError::InvalidCommand);
    }
    let mut children = Vec::with_capacity(count);
    for _ in 0..count {
        let creation = r.operation()?;
        let creation_index = r.u64()?;
        let configuration =
            ConfigurationId::new(r.u64()?).ok_or(ApplicationError::InvalidCommand)?;
        let len = r.u32()? as usize;
        children.push(InsertionChild {
            creation,
            creation_index,
            configuration,
            manifest: read_manifest(r.take(len)?)?,
        });
    }
    Ok(children)
}

impl TransferIntent {
    /// Insert one fresh child while the same concrete source keeps its other ranges.
    /// This is checked shape only; original creation/fence/import/publication must follow.
    #[allow(clippy::result_large_err)]
    pub fn insert_retained_child(
        before: ResponsibilityManifest,
        after: ResponsibilityManifest,
        child: InsertionChild,
    ) -> Result<
        Self,
        (
            ApplicationError,
            ResponsibilityManifest,
            ResponsibilityManifest,
            InsertionChild,
        ),
    > {
        if before.input().parent.is_some() {
            return Err((ApplicationError::InvalidCommand, before, after, child));
        }
        Self::retained_candidate(before, after, child)
    }
    #[allow(clippy::result_large_err)]
    pub(crate) fn retained_candidate(
        before: ResponsibilityManifest,
        after: ResponsibilityManifest,
        child: InsertionChild,
    ) -> Result<
        Self,
        (
            ApplicationError,
            ResponsibilityManifest,
            ResponsibilityManifest,
            InsertionChild,
        ),
    > {
        let validate = || -> Result<(), ApplicationError> {
            let b = before.input();
            let a = after.input();
            let c = child.manifest.input();
            let ExecutionMode::Single(target) = c.execution else {
                return Err(ApplicationError::InvalidCommand);
            };
            let ExecutionMode::Delegated(routes) = &a.execution else {
                return Err(ApplicationError::InvalidCommand);
            };
            if b.responsibility != a.responsibility
                || b.parent != a.parent
                || b.authority != a.authority
                || b.application != a.application
                || b.scheme != a.scheme
                || b.scope != a.scope
                || b.placement != a.placement
                || b.state != ResponsibilityState::Active
                || a.state != ResponsibilityState::Active
                || b.epoch.get().checked_add(1) != Some(a.epoch.get())
                || b.generation.get().checked_add(1) != Some(a.generation.get())
                || c.parent
                    != Some(ParentAuthority {
                        responsibility: b.responsibility,
                        group: b.authority,
                    })
                || c.authority != b.authority
                || c.application != b.application
                || c.scheme != b.scheme
                || c.epoch.get() != 1
                || c.generation.get() != 1
                || c.state != ResponsibilityState::Active
                || c.scope.start() < b.scope.start()
                || c.scope.end() > b.scope.end()
                || c.responsibility.id == b.responsibility.id
                || b.parent.is_some_and(|p| {
                    p.responsibility.id == c.responsibility.id || p.group.id == target.id
                })
                || target.id == b.authority.id
                || child.creation_index == 0
                || child.creation_index == u64::MAX
            {
                return Err(ApplicationError::InvalidCommand);
            }
            let source = match &b.execution {
                ExecutionMode::Single(g) => *g,
                ExecutionMode::Delegated(v) => {
                    let r = v
                        .iter()
                        .find(|r| {
                            r.scope.start() <= c.scope.start() && r.scope.end() >= c.scope.end()
                        })
                        .ok_or(ApplicationError::InvalidCommand)?;
                    let RouteTarget::Group(g) = r.target else {
                        return Err(ApplicationError::InvalidCommand);
                    };
                    if v.iter().any(|r| {
                        matches!(r.target, RouteTarget::Group(other) if other != g)
                            || matches!(r.target, RouteTarget::Child(old) if old.responsibility.id == c.responsibility.id)
                    }) {
                        return Err(ApplicationError::InvalidCommand);
                    }
                    g
                }
                _ => return Err(ApplicationError::InvalidCommand),
            };
            if source.id == target.id
                || source.id == b.authority.id
                || b.parent.is_some_and(|p| p.group.id == source.id)
            {
                return Err(ApplicationError::InvalidCommand);
            }
            let old_target = |bucket| match &b.execution {
                ExecutionMode::Single(g) => RouteTarget::Group(*g),
                ExecutionMode::Delegated(v) => {
                    v.iter()
                        .find(|r| r.scope.contains(bucket))
                        .expect("covered before")
                        .target
                }
                _ => unreachable!(),
            };
            let fresh = RouteTarget::Child(ChildAuthority {
                responsibility: c.responsibility,
                group: c.authority,
                epoch: c.epoch,
            });
            if !routes
                .iter()
                .any(|r| r.scope == c.scope && r.target == fresh)
            {
                return Err(ApplicationError::InvalidCommand);
            }
            if let ExecutionMode::Delegated(old) = &b.execution {
                if old
                    .iter()
                    .filter(|r| matches!(r.target, RouteTarget::Child(_)))
                    .any(|r| !routes.contains(r))
                {
                    return Err(ApplicationError::InvalidCommand);
                }
            }
            let mut retained = false;
            for bucket in b.scope.start()..b.scope.end() {
                let old = old_target(bucket);
                let expected = if c.scope.contains(bucket) {
                    if old != RouteTarget::Group(source) {
                        return Err(ApplicationError::InvalidCommand);
                    }
                    fresh
                } else {
                    retained |= old == RouteTarget::Group(source);
                    old
                };
                if routes
                    .iter()
                    .find(|r| r.scope.contains(bucket))
                    .expect("covered after")
                    .target
                    != expected
                {
                    return Err(ApplicationError::InvalidCommand);
                }
            }
            if !retained {
                return Err(ApplicationError::InvalidCommand);
            }
            Ok(())
        };
        if let Err(e) = validate() {
            return Err((e, before, after, child));
        }
        Ok(Self {
            before,
            after,
            delegation: None,
            insertion: Some(vec![child]),
            retained: true,
        })
    }
    pub(crate) fn retained_source(&self) -> Option<RouteEntry> {
        if !self.retained {
            return None;
        }
        let scope = self.insertion.as_ref()?.first()?.manifest.input().scope;
        let group = match &self.before.input().execution {
            ExecutionMode::Single(g) => *g,
            ExecutionMode::Delegated(v) => {
                let r = v
                    .iter()
                    .find(|r| r.scope.start() <= scope.start() && r.scope.end() >= scope.end())?;
                let RouteTarget::Group(g) = r.target else {
                    return None;
                };
                g
            }
            _ => return None,
        };
        Some(RouteEntry {
            scope,
            target: RouteTarget::Group(group),
        })
    }
    pub(crate) fn delegated_retained(
        before: ResponsibilityManifest,
        after: ResponsibilityManifest,
        child: InsertionChild,
        binding: crate::delegation::DelegationBinding,
    ) -> Result<Self, ApplicationError> {
        let mut intent = Self::retained_candidate(before, after, child).map_err(|e| e.0)?;
        let children = intent.insertion_children().expect("one retained child");
        if intent.before.input().parent.is_none()
            || binding.index == 0
            || binding.index == u64::MAX
            || binding.operation == binding.child_operation
            || children[0].creation == binding.operation
            || children[0].creation == binding.child_operation
            || binding.plan_digest
                != binding.digest_retained_for(&intent.before, &intent.after, children)
        {
            return Err(ApplicationError::InvalidCommand);
        }
        intent.delegation = Some(binding);
        Ok(intent)
    }
}
