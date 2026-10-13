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
//! Private planned transitions; neither cached summaries nor plans are receipts.
use super::*;
use std::mem::size_of;

mod view;
pub(crate) use view::{LogMetadata, LogView};

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub(crate) struct LogSummary {
    metadata: LogMetadata,
    plain: bool,
    retained: Option<usize>,
}
impl LogSummary {
    /// Caller has validated this canonical state and exclusively owns its mutations.
    pub(crate) fn from_validated(state: &GroupLog) -> Self {
        let retained = state
            .entries
            .len()
            .checked_mul(size_of::<LogEntry>())
            .and_then(|bytes| {
                state.entries.iter().try_fold(bytes, |sum, entry| {
                    sum.checked_add(entry.retained_payload_bytes())
                })
            })
            .and_then(|bytes| {
                bytes.checked_add(
                    state
                        .snapshot_membership
                        .as_ref()
                        .map_or(0, |base| base.retained_bytes()),
                )
            });
        Self {
            metadata: LogMetadata::from_log(state),
            plain: state.snapshot_membership.is_none()
                && !state
                    .entries
                    .iter()
                    .any(|entry| matches!(entry.payload, EntryPayload::Configuration(_))),
            retained,
        }
    }
    pub(crate) fn matches(&self, state: &GroupLog) -> bool {
        self.matches_metadata(&LogMetadata::from_log(state))
    }
    pub(crate) fn matches_metadata(&self, metadata: &LogMetadata) -> bool {
        self.metadata == *metadata
    }
    pub(crate) fn is_plain(&self) -> bool {
        self.plain
    }
    pub(crate) fn retained_bytes(&self) -> Option<usize> {
        self.retained
    }
}

pub(crate) struct PlainDelta {
    pub(crate) predecessor: LogMetadata,
    pub(crate) append: Option<Suffix>,
    pub(crate) summary: LogSummary,
}
pub(crate) enum NextLog {
    Delta(PlainDelta),
    Full {
        predecessor: LogMetadata,
        next: Box<GroupLog>,
        update: LogUpdate,
    },
}
/// Preflighted for one exact exclusive predecessor. Keep preparation and commit
/// under the same owner, with no intervening mutation of that predecessor.
pub(crate) struct PreparedLog(NextLog);
impl NextLog {
    pub(crate) fn plan(
        view: LogView<'_>,
        summary: &LogSummary,
        update: &LogUpdate,
        limits: LogLimits,
    ) -> Result<Self, StorageError> {
        let limits = limits.validate()?;
        if view.metadata().group != update.group {
            return Err(StorageError::Rejected("group not bootstrapped"));
        }
        if !summary.matches_metadata(&view.metadata()) {
            return Err(StorageError::Rejected("stale log summary"));
        }
        let plain = summary.is_plain()
            && update.snapshot.is_none()
            && update.snapshot_membership.is_none()
            && update.suffix.as_ref().is_none_or(|suffix| {
                view.metadata().last_index.checked_add(1) == Some(suffix.from)
                    && !suffix
                        .entries
                        .iter()
                        .any(|entry| matches!(entry.payload, EntryPayload::Configuration(_)))
            });
        if plain {
            return plan_plain(view, summary, update, limits).map(Self::Delta);
        }
        let mut next = view.materialize();
        apply_update(&mut next, update, limits)?;
        Ok(Self::Full {
            predecessor: view.metadata(),
            next: Box::new(next),
            update: update.clone(),
        })
    }
    pub(crate) fn metadata(&self) -> LogMetadata {
        match self {
            Self::Delta(delta) => delta.metadata(),
            Self::Full { next, .. } => LogMetadata::from_log(next),
        }
    }
    pub(crate) fn full(&self) -> Option<&GroupLog> {
        match self {
            Self::Delta(_) => None,
            Self::Full { next, .. } => Some(next),
        }
    }
    pub(crate) fn term_at(
        &self,
        current: &GroupLog,
        _update: &LogUpdate,
        index: u64,
    ) -> Option<u64> {
        match self {
            Self::Full { next, .. } => next.term_at(index),
            Self::Delta(delta) => {
                if let Some(suffix) = &delta.append {
                    if index >= suffix.from {
                        return suffix
                            .entries
                            .get(usize::try_from(index - suffix.from).ok()?)
                            .map(|entry| entry.term);
                    }
                }
                current.term_at(index)
            }
        }
    }
    pub(crate) fn install(
        self,
        current: &mut GroupLog,
        update: LogUpdate,
    ) -> Result<LogSummary, StorageError> {
        let prepared = self.prepare_install(current, &update)?;
        Ok(prepared.install(current))
    }
    pub(crate) fn prepare_install(
        self,
        current: &GroupLog,
        update: &LogUpdate,
    ) -> Result<PreparedLog, StorageError> {
        match &self {
            Self::Full {
                predecessor,
                update: original,
                ..
            } => {
                if LogMetadata::from_log(current) != *predecessor || update != original {
                    return Err(StorageError::Rejected(
                        "transition plan predecessor or update mismatch",
                    ));
                }
            }
            Self::Delta(delta) => {
                delta.check_install(LogMetadata::from_log(current), update)?;
            }
        }
        Ok(PreparedLog(self))
    }
}
impl PreparedLog {
    pub(crate) fn install(self, current: &mut GroupLog) -> LogSummary {
        match self.0 {
            NextLog::Full { next, .. } => {
                *current = *next;
                LogSummary::from_validated(current)
            }
            NextLog::Delta(delta) => {
                let metadata = delta.metadata();
                if let Some(suffix) = delta.append {
                    current.entries.extend(suffix.entries);
                }
                metadata.install_header(current);
                delta.summary
            }
        }
    }
}
impl PlainDelta {
    pub(crate) fn metadata(&self) -> LogMetadata {
        self.summary.metadata
    }
    pub(crate) fn check_install(
        &self,
        current: LogMetadata,
        update: &LogUpdate,
    ) -> Result<(), StorageError> {
        if current != self.predecessor
            || update.group != current.group
            || update.expected_revision != current.revision
            || update.hard_state != self.metadata().hard_state
            || update.commit_index != self.metadata().commit_index
            || update.snapshot.is_some()
            || update.snapshot_membership.is_some()
            || update.suffix != self.append
        {
            return Err(StorageError::Rejected(
                "transition plan predecessor or update mismatch",
            ));
        }
        Ok(())
    }
}
fn plan_plain(
    view: LogView<'_>,
    summary: &LogSummary,
    update: &LogUpdate,
    limits: LogLimits,
) -> Result<PlainDelta, StorageError> {
    let current = view.metadata();
    if update.expected_revision != current.revision
        || !update.hard_state.follows(current.hard_state)
    {
        return Err(StorageError::Rejected("stale revision or invalid ballot"));
    }
    validate_ballot_fields(view.bootstrap(), current.hard_state, current.ballot_origin)?;
    let ballot_origin = ballot_origin_for(
        current.hard_state,
        current.ballot_origin,
        update.hard_state,
        |candidate| {
            Ok(BallotOrigin {
                configuration: view.bootstrap().configuration,
                candidate_store: *view
                    .bootstrap()
                    .voter_stores
                    .get(&candidate)
                    .ok_or(StorageError::Rejected("candidate is not an accepted voter"))?,
            })
        },
    )?;
    let mut next = current;
    if let Some(suffix) = &update.suffix {
        if suffix.entries.len()
            > limits
                .max_entries_per_group
                .saturating_sub(current.entries_len)
        {
            return Err(StorageError::Rejected("group entry limit"));
        }
        validate_suffix_entries(
            suffix,
            view.term_at(current.last_index)
                .ok_or(StorageError::Rejected("invalid plain log predecessor"))?,
            update.hard_state.term,
            limits,
        )?;
        if let Some(last) = suffix.entries.last() {
            next.last_index = last.index;
            next.last_term = last.term;
        }
        next.entries_len += suffix.entries.len();
    }
    if update.commit_index < current.commit_index
        || update.commit_index > next.last_index
        || next.last_term > update.hard_state.term
    {
        return Err(StorageError::Rejected(
            "commit regression/gap or hard term behind log",
        ));
    }
    next.revision = current
        .revision
        .get()
        .checked_add(1)
        .and_then(LogRevision::new)
        .ok_or(StorageError::Rejected("revision exhausted"))?;
    next.hard_state = update.hard_state;
    next.ballot_origin = ballot_origin;
    next.commit_index = update.commit_index;
    // Clone only incoming entries: preserve the old validator's payload capacity
    // normalization without copying any retained prefix.
    let append = update.suffix.clone();
    let extra = append.as_ref().map_or(Some(0), |suffix| {
        suffix
            .entries
            .len()
            .checked_mul(size_of::<LogEntry>())
            .and_then(|bytes| {
                suffix.entries.iter().try_fold(bytes, |sum, entry| {
                    sum.checked_add(entry.retained_payload_bytes())
                })
            })
    });
    let retained = summary.retained.and_then(|bytes| bytes.checked_add(extra?));
    Ok(PlainDelta {
        predecessor: current,
        append,
        summary: LogSummary {
            metadata: next,
            plain: true,
            retained,
        },
    })
}

#[cfg(test)]
mod tests;
