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
//! Intrinsic facts of one immutable backing, measured once when it is frozen.
//!
//! Facts describe only the content itself: consecutive indices, positive and
//! nondecreasing terms, and payload/configuration metrics. They carry no
//! receiver state, so every receiver still checks its own predecessor, terms,
//! limits and configuration scope against them.
use super::super::{EntryPayload, LogEntry};
use crate::{identity::ConfigurationId, membership::ConfigurationChange};
use std::{mem::size_of, ops::Range};

/// Configuration a record activates; receivers bound it by their message scope.
pub(crate) fn activated_configuration(entry: &LogEntry) -> Option<ConfigurationId> {
    match &entry.payload {
        EntryPayload::Configuration(record) => Some(match &record.change {
            ConfigurationChange::Learners(next) => next.id(),
            ConfigurationChange::Joint { id, .. } | ConfigurationChange::Final { id } => *id,
        }),
        EntryPayload::Command { .. } | EntryPayload::Noop => None,
    }
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
struct Metrics {
    payload: usize,
    non_noop: bool,
    configuration: bool,
}
impl Metrics {
    const EMPTY: Self = Self {
        payload: 0,
        non_noop: false,
        configuration: false,
    };
    fn of(entry: &LogEntry) -> Self {
        Self {
            payload: entry.payload_bytes(),
            non_noop: !matches!(entry.payload, EntryPayload::Noop),
            configuration: matches!(entry.payload, EntryPayload::Configuration(_)),
        }
    }
}

/// Exact totals of a visible range.
#[derive(Clone, Copy, Debug, Default)]
pub(crate) struct Totals {
    pub(crate) payload: usize,
    pub(crate) non_noops: usize,
    pub(crate) configurations: usize,
}
impl Totals {
    pub(crate) fn join(self, next: Self) -> Option<Self> {
        Some(Self {
            payload: self.payload.checked_add(next.payload)?,
            non_noops: self.non_noops.checked_add(next.non_noops)?,
            configurations: self.configurations.checked_add(next.configurations)?,
        })
    }
}

/// Cumulative metrics before an entry; range totals are exact differences.
#[derive(Clone, Copy, Debug, Default)]
struct Prefix {
    payload: usize,
    non_noops: usize,
    configurations: usize,
}

#[derive(Debug)]
enum MetricsIndex {
    /// Every entry has these metrics: every singleton and equal-size batch.
    Uniform(Metrics),
    /// `len + 1` cumulative prefixes, charged with the backing.
    Mixed(Box<[Prefix]>),
    /// A total overflowed; receivers use the visible-entry validators.
    Unavailable,
}

/// One immutable backing and its facts. Nothing here is mutable after
/// construction and no caller can supply or replace a fact.
#[derive(Debug)]
pub(super) struct BatchData {
    /// Never resized after construction; spare capacity is kept (and
    /// charged) rather than reallocated away.
    pub(super) entries: Vec<LogEntry>,
    /// Consecutive indices from the first, every term positive and
    /// nondecreasing. Without it the old full validators run.
    sealed: bool,
    index: MetricsIndex,
    /// Backing-wide maxima: an upper bound for any visible range. A range is
    /// accepted from the bound; otherwise its visible entries are checked.
    max_payload: usize,
    max_configuration: Option<ConfigurationId>,
    /// Entry storage (allocated capacity), every retained payload and the
    /// metrics index.
    charge: Option<usize>,
}

/// Exact facts of a visible range, or of several joined ranges.
#[derive(Clone, Copy, Debug)]
pub(crate) struct Summary {
    pub(crate) count: usize,
    /// First and last `(index, term)`; `None` exactly when `count == 0`.
    pub(crate) bounds: Option<((u64, u64), (u64, u64))>,
    pub(crate) payload: usize,
    pub(crate) non_noops: usize,
    pub(crate) configurations: usize,
    /// Upper bounds of the visible maxima, never used to reject.
    pub(crate) max_payload: usize,
    pub(crate) max_configuration: Option<ConfigurationId>,
}
impl Summary {
    pub(crate) const EMPTY: Self = Self {
        count: 0,
        bounds: None,
        payload: 0,
        non_noops: 0,
        configurations: 0,
        max_payload: 0,
        max_configuration: None,
    };
    /// Facts of `self` followed by `next`; `None` unless the join continues
    /// the index sequence with a nondecreasing term and every sum fits.
    pub(crate) fn join(self, next: Self) -> Option<Self> {
        let bounds = match (self.bounds, next.bounds) {
            (None, bounds) | (bounds, None) => bounds,
            (Some((first, last)), Some((next_first, next_last))) => {
                if last.0.checked_add(1) != Some(next_first.0) || last.1 > next_first.1 {
                    return None;
                }
                Some((first, next_last))
            }
        };
        Some(Self {
            count: self.count.checked_add(next.count)?,
            bounds,
            payload: self.payload.checked_add(next.payload)?,
            non_noops: self.non_noops.checked_add(next.non_noops)?,
            configurations: self.configurations.checked_add(next.configurations)?,
            max_payload: self.max_payload.max(next.max_payload),
            max_configuration: self.max_configuration.max(next.max_configuration),
        })
    }
}

impl BatchData {
    /// Inspect every entry once. Invalid content is still frozen, unsealed.
    /// A mixed backing takes a second pass to build its prefix index.
    pub(super) fn new(entries: Vec<LogEntry>) -> Self {
        let first = entries.first().map(Metrics::of);
        let mut sealed = true;
        let mut uniform = true;
        let mut total = Some(Prefix::default());
        let mut retained = entries.capacity().checked_mul(size_of::<LogEntry>());
        let mut max_payload = 0;
        let mut max_configuration = None;
        let mut previous: Option<&LogEntry> = None;
        for entry in &entries {
            let metrics = Metrics::of(entry);
            sealed &= entry.term != 0
                && previous.is_none_or(|previous| {
                    previous.index.checked_add(1) == Some(entry.index)
                        && previous.term <= entry.term
                });
            previous = Some(entry);
            uniform &= first == Some(metrics);
            total = total.and_then(|sum| sum.add(metrics));
            retained = retained.and_then(|sum| sum.checked_add(entry.retained_payload_bytes()));
            max_payload = max_payload.max(metrics.payload);
            max_configuration = max_configuration.max(activated_configuration(entry));
        }
        let index = match (total, first) {
            (None, _) => MetricsIndex::Unavailable,
            (Some(_), None) => MetricsIndex::Uniform(Metrics::EMPTY),
            (Some(_), Some(first)) if uniform => MetricsIndex::Uniform(first),
            (Some(_), Some(_)) => {
                prefixes(&entries).map_or(MetricsIndex::Unavailable, MetricsIndex::Mixed)
            }
        };
        let charge = match &index {
            MetricsIndex::Mixed(prefixes) => prefixes
                .len()
                .checked_mul(size_of::<Prefix>())
                .and_then(|bytes| retained?.checked_add(bytes)),
            MetricsIndex::Uniform(_) | MetricsIndex::Unavailable => retained,
        };
        Self {
            entries,
            sealed,
            index,
            max_payload,
            max_configuration,
            charge,
        }
    }
    pub(super) fn charge(&self) -> Option<usize> {
        self.charge
    }
    /// Exact payload total of `range` from the index, sealed or not; `None`
    /// when the index is unavailable. The caller checked the bounds.
    pub(super) fn payload(&self, range: Range<usize>) -> Option<usize> {
        self.totals(range).map(|totals| totals.payload)
    }
    /// Payload of every entry when all are alike, sealed or not.
    pub(super) fn uniform_payload(&self) -> Option<usize> {
        match &self.index {
            MetricsIndex::Uniform(metrics) => Some(metrics.payload),
            MetricsIndex::Mixed(_) | MetricsIndex::Unavailable => None,
        }
    }
    /// Exact totals of `range` from the index, sealed or not.
    pub(super) fn totals(&self, range: Range<usize>) -> Option<Totals> {
        let count = range.end - range.start;
        match &self.index {
            MetricsIndex::Uniform(metrics) => Some(Totals {
                payload: metrics.payload.checked_mul(count)?,
                non_noops: if metrics.non_noop { count } else { 0 },
                configurations: if metrics.configuration { count } else { 0 },
            }),
            MetricsIndex::Mixed(prefixes) => {
                let (start, end) = (prefixes[range.start], prefixes[range.end]);
                Some(Totals {
                    payload: end.payload - start.payload,
                    non_noops: end.non_noops - start.non_noops,
                    configurations: end.configurations - start.configurations,
                })
            }
            MetricsIndex::Unavailable => None,
        }
    }
    /// Facts of `range`; `None` for unsealed content or an unavailable index.
    pub(super) fn summary(&self, range: Range<usize>) -> Option<Summary> {
        if !self.sealed || range.start > range.end || range.end > self.entries.len() {
            return None;
        }
        let count = range.end - range.start;
        if count == 0 {
            return Some(Summary::EMPTY);
        }
        let Totals {
            payload,
            non_noops,
            configurations,
        } = self.totals(range.clone())?;
        let first = &self.entries[range.start];
        let last = &self.entries[range.end - 1];
        Some(Summary {
            count,
            bounds: Some(((first.index, first.term), (last.index, last.term))),
            payload,
            non_noops,
            configurations,
            max_payload: self.max_payload,
            max_configuration: if configurations == 0 {
                None
            } else {
                self.max_configuration
            },
        })
    }
}
/// Facts of a small owned piece, read directly from its entries.
pub(super) fn summarize(entries: &[LogEntry]) -> Option<Summary> {
    let (first, last) = match (entries.first(), entries.last()) {
        (Some(first), Some(last)) => (first, last),
        _ => return Some(Summary::EMPTY),
    };
    let mut previous: Option<&LogEntry> = None;
    let mut totals = Totals::default();
    let (mut max_payload, mut max_configuration) = (0, None);
    for entry in entries {
        if entry.term == 0
            || previous.is_some_and(|previous| {
                previous.index.checked_add(1) != Some(entry.index) || previous.term > entry.term
            })
        {
            return None;
        }
        previous = Some(entry);
        let metrics = Metrics::of(entry);
        totals = totals.join(Totals {
            payload: metrics.payload,
            non_noops: usize::from(metrics.non_noop),
            configurations: usize::from(metrics.configuration),
        })?;
        max_payload = max_payload.max(metrics.payload);
        max_configuration = max_configuration.max(activated_configuration(entry));
    }
    Some(Summary {
        count: entries.len(),
        bounds: Some(((first.index, first.term), (last.index, last.term))),
        payload: totals.payload,
        non_noops: totals.non_noops,
        configurations: totals.configurations,
        max_payload,
        max_configuration,
    })
}
/// Totals of a small owned piece, sealed or not.
pub(super) fn totals_of(entries: &[LogEntry]) -> Option<Totals> {
    entries.iter().try_fold(Totals::default(), |sum, entry| {
        let metrics = Metrics::of(entry);
        sum.join(Totals {
            payload: metrics.payload,
            non_noops: usize::from(metrics.non_noop),
            configurations: usize::from(metrics.configuration),
        })
    })
}
/// `slots` entry records plus every retained payload.
#[inline]
pub(super) fn charge_of(entries: &[LogEntry], slots: usize) -> Option<usize> {
    entries
        .iter()
        .try_fold(slots.checked_mul(size_of::<LogEntry>())?, |sum, entry| {
            sum.checked_add(entry.retained_payload_bytes())
        })
}
fn prefixes(entries: &[LogEntry]) -> Option<Box<[Prefix]>> {
    let mut prefixes = Vec::with_capacity(entries.len().checked_add(1)?);
    let mut sum = Prefix::default();
    prefixes.push(sum);
    for entry in entries {
        sum = sum.add(Metrics::of(entry))?;
        prefixes.push(sum);
    }
    Some(prefixes.into_boxed_slice())
}
impl Prefix {
    fn add(self, metrics: Metrics) -> Option<Self> {
        Some(Self {
            payload: self.payload.checked_add(metrics.payload)?,
            non_noops: self.non_noops + usize::from(metrics.non_noop),
            configurations: self.configurations + usize::from(metrics.configuration),
        })
    }
}
