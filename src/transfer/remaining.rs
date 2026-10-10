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
//! Complete remaining concrete ownership moves preserve existing delegation.
use super::*;
impl TransferIntent {
    /// Move a root's remaining concrete range to fresh groups. Nested owners use
    /// `DelegationPlan::move_remaining` and its committed parent reservation.
    pub fn move_remaining(
        before: ResponsibilityManifest,
        after: ResponsibilityManifest,
    ) -> Result<Self, ApplicationError> {
        if before.input().parent.is_some() {
            return Err(ApplicationError::InvalidCommand);
        }
        Self::remaining_candidate(before, after, None)
    }
    pub fn is_remaining_transfer(&self) -> bool {
        self.insertion.is_none()
            && matches!(self.before.input().execution, ExecutionMode::Delegated(_))
    }
    pub(crate) fn concrete_routes(m: &ResponsibilityManifest) -> Vec<RouteEntry> {
        let ExecutionMode::Delegated(routes) = &m.input().execution else {
            return Vec::new();
        };
        routes
            .iter()
            .filter(|r| matches!(r.target, RouteTarget::Group(_)))
            .copied()
            .collect()
    }
    pub(crate) fn remaining_candidate(
        before: ResponsibilityManifest,
        after: ResponsibilityManifest,
        binding: Option<crate::delegation::DelegationBinding>,
    ) -> Result<Self, ApplicationError> {
        let b = before.input();
        let a = after.input();
        let (ExecutionMode::Delegated(old), ExecutionMode::Delegated(new)) =
            (&b.execution, &a.execution)
        else {
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
        {
            return Err(ApplicationError::InvalidCommand);
        }
        let sources = Self::concrete_routes(&before);
        let targets = Self::concrete_routes(&after);
        if sources.len() != 1
            || targets.is_empty()
            || !old
                .iter()
                .any(|r| matches!(r.target, RouteTarget::Child(_)))
            || old
                .iter()
                .filter(|r| !matches!(r.target, RouteTarget::Group(_)))
                .ne(new
                    .iter()
                    .filter(|r| !matches!(r.target, RouteTarget::Group(_))))
        {
            return Err(ApplicationError::InvalidCommand);
        }
        let source = sources[0];
        let RouteTarget::Group(source_group) = source.target else {
            unreachable!()
        };
        if source_group.id == b.authority.id
            || b.parent.is_some_and(|p| p.group.id == source_group.id)
        {
            return Err(ApplicationError::InvalidCommand);
        }
        let mut cursor = source.scope.start();
        let mut ids = BTreeSet::new();
        for target in targets {
            let RouteTarget::Group(g) = target.target else {
                unreachable!()
            };
            if target.scope.start() != cursor
                || target.scope.end() > source.scope.end()
                || g.id == source_group.id
                || g.id == b.authority.id
                || b.parent.is_some_and(|p| p.group.id == g.id)
                || old
                    .iter()
                    .any(|r| matches!(r.target, RouteTarget::Child(c) if c.group.id == g.id))
                || !ids.insert(g.id)
            {
                return Err(ApplicationError::InvalidCommand);
            }
            cursor = target.scope.end();
        }
        if cursor != source.scope.end() {
            return Err(ApplicationError::InvalidCommand);
        }
        if let Some(v) = binding {
            if b.parent.is_none()
                || v.index == 0
                || v.index == u64::MAX
                || v.operation == v.child_operation
                || v.plan_digest != v.digest_for(&before, &after)
            {
                return Err(ApplicationError::InvalidCommand);
            }
        }
        Ok(Self {
            before,
            after,
            delegation: binding,
            insertion: None,
            retained: false,
        })
    }
}
