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
//! Construction-bound history, retaining each authority's original index domain.
use super::*;

/// Maximum successive metadata moves retained by a selected profile. Images
/// remain subject to the independent 64 MiB bound; this is not a pruning policy.
pub const MAX_METADATA_MOVES: usize = 8;

#[derive(Clone)]
#[allow(clippy::large_enum_variant)]
pub(super) enum AuthorityHistory {
    Directory(LifecycleDirectory),
    Serving(Box<MetadataServingTarget>),
}
impl AuthorityHistory {
    pub(super) fn depth(&self) -> usize {
        match self {
            Self::Directory(_) => 0,
            Self::Serving(s) => 1 + s.target.source.initial.depth(),
        }
    }
    pub(super) fn authority(&self) -> GroupIdentity {
        match self {
            Self::Directory(d) => d.directory().plan().authority(),
            Self::Serving(s) => s.target.plan.target(),
        }
    }
    pub(super) fn contains_authority(&self, group: GroupIdentity) -> bool {
        self.authority().id == group.id
            || match self {
                Self::Directory(_) => false,
                Self::Serving(s) => s.target.source.initial.contains_authority(group),
            }
    }
    pub(super) fn original(&self) -> &LifecycleDirectory {
        match self {
            Self::Directory(d) => d,
            Self::Serving(s) => s
                .target
                .history()
                .unwrap_or(&s.target.source.initial)
                .original(),
        }
    }
    /// Construction limits are inherited; only a live serving base is used for
    /// mutable state. Callers must check `active_directory` before planning.
    pub(super) fn directory(&self) -> &Directory {
        self.active_directory()
            .unwrap_or_else(|| self.original().directory())
    }
    pub(super) fn active_directory(&self) -> Option<&Directory> {
        match self {
            Self::Directory(d) => Some(d.directory()),
            Self::Serving(s) => s.active.as_ref().map(|a| &a.directory),
        }
    }
    pub(super) fn has_bootstrap(&self) -> bool {
        match self {
            Self::Directory(d) => d.directory().has_bootstrap(),
            Self::Serving(s) => s.target.staged.is_some(),
        }
    }
    pub(super) fn is_bootstrap_operation(&self, op: OperationId) -> bool {
        match self {
            Self::Directory(d) => d.directory().is_bootstrap_operation(op),
            Self::Serving(s) => s.target.staged.is_some() && s.target.operation == op,
        }
    }
    pub(super) fn contains_operation(&self, op: OperationId) -> bool {
        match self {
            Self::Directory(d) => d.directory().contains_operation(op),
            Self::Serving(s) => {
                s.reserved_operation(op)
                    || s.target.history().is_some_and(|h| h.contains_operation(op))
                    || s.active
                        .as_ref()
                        .is_some_and(|a| a.directory.contains_operation(op))
            }
        }
    }
    /// Only this authority's commands occupy this authority's index domain.
    pub(super) fn contains_command_at(&self, at: u64) -> bool {
        match self {
            Self::Directory(d) => d.directory().contains_command_at(at),
            Self::Serving(s) => {
                s.target.staged == Some(at)
                    || s.target
                        .imported
                        .as_ref()
                        .is_some_and(|i| i.status.index == at)
                    || s.active.as_ref().is_some_and(|a| {
                        a.status.index == at || a.tail.values().any(|e| e.index == at)
                    })
            }
        }
    }
    pub(super) fn bootstrap_command(&self, maximum: usize) -> Result<Vec<u8>, ApplicationError> {
        match self {
            Self::Directory(d) => d.directory().bootstrap_command(maximum),
            Self::Serving(s) => s.bootstrap_command(maximum),
        }
    }
    pub(super) fn applied_index(&self) -> u64 {
        match self {
            Self::Directory(d) => d.applied_index(),
            Self::Serving(s) => s.applied_index(),
        }
    }
    pub(super) fn schema_version(&self) -> u64 {
        match self {
            Self::Directory(d) => d.schema_version(),
            Self::Serving(s) => s.schema_version(),
        }
    }
    pub(super) fn readiness_requirements(&self) -> ReadinessRequirements {
        match self {
            Self::Directory(d) => d.directory().readiness_requirements(),
            Self::Serving(s) => s.readiness_requirements(),
        }
    }
    pub(super) fn validate_group(&self, g: GroupIdentity) -> Result<(), ApplicationError> {
        match self {
            Self::Directory(d) => d.validate_group(g),
            Self::Serving(s) => s.validate_group(g),
        }
    }
    pub(super) fn apply_batch(
        &mut self,
        entries: &[LogEntry],
    ) -> Result<Vec<MetadataSourceOutcome>, ApplicationError> {
        match self {
            Self::Directory(d) => Ok(d
                .apply_batch(entries)?
                .into_iter()
                .map(MetadataSourceOutcome::Directory)
                .collect()),
            Self::Serving(s) => Ok(s
                .apply_batch(entries)?
                .into_iter()
                .map(|r| MetadataSourceOutcome::Serving(r.outcome))
                .collect()),
        }
    }
    pub(super) fn validate_proposal<'a>(
        &self,
        op: OperationId,
        bytes: &[u8],
        pending: impl Iterator<Item = (OperationId, &'a [u8])>,
    ) -> Result<usize, ApplicationError> {
        match self {
            Self::Directory(d) => d.validate_proposal(op, bytes, pending),
            Self::Serving(s) => s.validate_proposal(op, bytes, pending),
        }
    }
    pub(super) fn checkpoint(&self, maximum: usize) -> Result<Vec<u8>, ApplicationError> {
        match self {
            Self::Directory(d) => d.checkpoint(maximum),
            Self::Serving(s) => s.checkpoint(maximum),
        }
    }
    pub(super) fn restore_checkpoint(
        &mut self,
        schema: u64,
        at: u64,
        bytes: &[u8],
    ) -> Result<(), ApplicationError> {
        match self {
            Self::Directory(d) => d.restore_checkpoint(schema, at, bytes),
            Self::Serving(s) => s.restore_checkpoint(schema, at, bytes),
        }
    }
    pub(super) fn read_at(
        &self,
        at: u64,
        q: DirectoryQuery,
    ) -> Result<MetadataSourceRead, ApplicationError> {
        match self {
            Self::Directory(d) => d.read_at(at, q).map(MetadataSourceRead::Directory),
            Self::Serving(s) => s
                .read_at(at, MetadataServingQuery::Directory(q))
                .map(MetadataSourceRead::Serving),
        }
    }
    pub(super) fn read_result_bound(&self, q: &DirectoryQuery) -> Result<usize, ApplicationError> {
        match self {
            Self::Directory(d) => d.read_result_bound(q),
            Self::Serving(s) => s.read_result_bound(&MetadataServingQuery::Directory(*q)),
        }
    }
    pub(super) fn read_result_bytes(
        &self,
        r: &MetadataSourceRead,
        limit: usize,
    ) -> Result<usize, ApplicationError> {
        match (self, r) {
            (Self::Directory(d), MetadataSourceRead::Directory(r)) => d.read_result_bytes(r, limit),
            (Self::Serving(s), MetadataSourceRead::Serving(r)) => s.read_result_bytes(r, limit),
            _ => Ok(0),
        }
    }
    pub(super) fn historical_outcome(
        &self,
        op: OperationId,
        bytes: &[u8],
    ) -> Option<(GroupIdentity, u64, DirectoryOutcome)> {
        match self {
            Self::Directory(d) => d
                .directory()
                .historical_outcome(op, bytes)
                .map(|(i, o)| (self.authority(), i, o)),
            Self::Serving(s) => s.historical_outcome(op, bytes),
        }
    }
    pub(super) fn query_view(
        &self,
        q: DirectoryQuery,
    ) -> Result<(LifecycleDirectory, GroupIdentity, u64), ApplicationError> {
        match self {
            Self::Directory(d) => Ok((d.clone(), self.authority(), d.applied_index())),
            Self::Serving(s) => s.query_view(q, false),
        }
    }
    pub(super) fn operation_authority(&self, op: OperationId) -> Option<GroupIdentity> {
        match self {
            Self::Directory(d) => d
                .directory()
                .contains_operation(op)
                .then(|| self.authority()),
            Self::Serving(s) => s
                .target
                .history()
                .and_then(|h| h.operation_authority(op))
                .or_else(|| {
                    s.active
                        .as_ref()
                        .filter(|a| a.directory.contains_operation(op))
                        .map(|_| self.authority())
                }),
        }
    }
}
