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
//! Consensus log transitions and the host-replaceable storage contract.
use super::*;

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub(crate) struct LogMetadata {
    pub(crate) group: GroupIdentity,
    pub(crate) configuration: ConfigurationId,
    pub(crate) revision: LogRevision,
    pub(crate) generation: LogGeneration,
    pub(crate) hard_state: HardState,
    pub(crate) ballot_origin: Option<BallotOrigin>,
    pub(crate) commit_index: u64,
    pub(crate) last_index: u64,
    pub(crate) last_term: u64,
    pub(crate) entries_len: usize,
    pub(crate) base_index: u64,
    pub(crate) base_term: u64,
    pub(crate) snapshot: Option<SnapshotRef>,
}
impl LogMetadata {
    pub(crate) fn from_log(state: &GroupLog) -> Self {
        Self {
            group: state.bootstrap.group,
            configuration: state.bootstrap.configuration,
            revision: state.revision,
            generation: state.generation,
            hard_state: state.hard_state,
            ballot_origin: state.ballot_origin,
            commit_index: state.commit_index,
            last_index: state.last_index(),
            last_term: state.last_term(),
            entries_len: state.entries.len(),
            base_index: state.base_index(),
            base_term: state.base_term(),
            snapshot: state.snapshot,
        }
    }
    pub(crate) fn install_header(self, state: &mut GroupLog) {
        state.revision = self.revision;
        state.generation = self.generation;
        state.hard_state = self.hard_state;
        state.ballot_origin = self.ballot_origin;
        state.commit_index = self.commit_index;
        state.snapshot = self.snapshot;
    }
}

#[derive(Clone, Copy)]
pub(crate) struct LogView<'a> {
    base: &'a GroupLog,
    tail: &'a [LogEntry],
    metadata: LogMetadata,
    removed: usize,
}
impl<'a> LogView<'a> {
    pub(crate) fn new(base: &'a GroupLog) -> Self {
        Self {
            base,
            tail: &[],
            metadata: LogMetadata::from_log(base),
            removed: 0,
        }
    }
    /// Prefix/tail were admitted by this owner; metadata describes their exact
    /// accepted version. This is not an unchecked public storage admission API.
    #[cfg(test)]
    pub(crate) fn overlay(base: &'a GroupLog, tail: &'a [LogEntry], metadata: LogMetadata) -> Self {
        Self::trimmed_overlay(base, tail, metadata, 0)
    }
    #[cfg(any(feature = "native", test))]
    pub(crate) fn trimmed_overlay(
        base: &'a GroupLog,
        tail: &'a [LogEntry],
        metadata: LogMetadata,
        removed: usize,
    ) -> Self {
        Self {
            base,
            tail,
            metadata,
            removed,
        }
    }
    pub(crate) fn metadata(&self) -> &LogMetadata {
        &self.metadata
    }
    pub(crate) fn bootstrap(&self) -> &'a Bootstrap {
        &self.base.bootstrap
    }
    pub(crate) fn term_at(&self, index: u64) -> Option<u64> {
        if index == self.metadata.base_index {
            return Some(self.metadata.base_term);
        }
        if index < self.metadata.base_index {
            return None;
        }
        if index > self.base.last_index() {
            return self
                .tail
                .get(usize::try_from(index.checked_sub(self.tail.first()?.index)?).ok()?)
                .map(|entry| entry.term);
        }
        self.base.term_at(index)
    }
    pub(crate) fn materialize(&self) -> GroupLog {
        let mut current = self.base.clone();
        current.entries.drain(..self.removed);
        current.entries.extend(self.tail.iter().cloned());
        self.metadata.install_header(&mut current);
        current
    }
    pub(crate) fn entries(&self) -> impl Iterator<Item = &'a LogEntry> + use<'a> {
        self.base.entries[self.removed..].iter().chain(self.tail)
    }
}
