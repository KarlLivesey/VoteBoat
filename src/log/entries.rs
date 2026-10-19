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
//! Shared immutable entry batches and the chunked canonical log.
//!
//! A batch of at least [`SHARE_MIN`] entries is frozen once into an immutable
//! backing whose intrinsic facts are measured in the same step. Appends,
//! pending plans and canonical logs share exact ranges of backings; nothing
//! can mutate a backing afterwards. Narrower batches stay owned vectors that
//! are copied and scanned as before. An edit is an explicit copy (`to_vec`),
//! which is re-inspected when converted back.
use super::{EntryPayload, LogEntry};
use crate::identity::ConfigurationId;
use std::{borrow::Cow, fmt, ops::Range, sync::Arc};

mod canonical;
mod facts;
mod iter;
mod range;
#[cfg(test)]
mod tests;
pub use canonical::EntryLog;
pub(crate) use facts::{activated_configuration, IndexNeed};
use facts::{BatchData, Summary, Totals};
use iter::Iter;
pub use iter::Iter as EntryIter;
pub use range::{EntryRange, Slices as EntrySlices};

/// Batches narrower than this stay owned vectors: below it the fixed cost
/// of a shared backing (allocation, reference counts, descriptors) exceeds
/// what sharing and cached facts save per entry.
pub(crate) const SHARE_MIN: usize = 16;

/// Most a leader's Append may keep alive beyond plain copies of its visible
/// entries (see [`EntryBatch::for_message`]). It covers a whole backing's
/// kept spare capacity (at most 4 KiB) and the index of a mixed backing at
/// the receive ceiling (1,025 × 24 bytes), so whole frozen proposals are
/// shared whatever their content.
pub(crate) const MESSAGE_HIDDEN_MAX: usize = 32 * 1024;

/// One ordered piece of entries.
#[derive(Clone)]
enum Span {
    /// Exact visible range of one shared immutable backing. Backings hold at
    /// most `u32::MAX` entries, so the range fits beside the pointer and a
    /// piece is as small as a vector.
    Shared {
        data: Arc<BatchData>,
        start: u32,
        end: u32,
    },
    /// A small batch held directly. It is copied when shared, and its facts
    /// are read from its entries when needed, exactly as before sharing.
    Owned(Vec<LogEntry>),
}
impl Span {
    #[inline]
    fn new(entries: Vec<LogEntry>) -> Option<Self> {
        if entries.is_empty() {
            return None;
        }
        if entries.len() < SHARE_MIN {
            return Some(Self::Owned(entries));
        }
        // A (practically impossible) wider batch stays owned and is scanned.
        Some(match u32::try_from(entries.len()) {
            Ok(end) => Self::freeze(entries, end),
            Err(_) => Self::Owned(entries),
        })
    }
    /// Freeze a wide batch of `end` entries into a shared backing, measuring
    /// its facts.
    #[inline(never)]
    fn freeze(entries: Vec<LogEntry>, end: u32) -> Self {
        let data = Arc::new(BatchData::new(entries));
        Self::Shared {
            data,
            start: 0,
            end,
        }
    }
    /// What this piece keeps alive beyond a plain copy of its visible
    /// entries (an upper bound); `None` when unknown.
    fn hidden(&self) -> Option<usize> {
        match self {
            Self::Shared { data, start, end } => data.hidden(*start as usize..*end as usize),
            Self::Owned(entries) => {
                (entries.capacity() - entries.len()).checked_mul(std::mem::size_of::<LogEntry>())
            }
        }
    }
    /// The visible entries as a plain owned copy, held as a vector is.
    fn copied(&self) -> Self {
        Self::Owned(self.entries().to_vec())
    }
    #[inline]
    fn entries(&self) -> &[LogEntry] {
        match self {
            Self::Shared { data, start, end } => &data.entries[*start as usize..*end as usize],
            Self::Owned(entries) => entries,
        }
    }
    #[inline]
    fn len(&self) -> usize {
        match self {
            Self::Shared { start, end, .. } => (end - start) as usize,
            Self::Owned(entries) => entries.len(),
        }
    }
    /// Subrange relative to this piece; the caller checked the bounds, so
    /// a shared range stays within its backing (and `u32`). An owned piece
    /// is copied.
    fn part(&self, range: Range<usize>) -> Self {
        match self {
            Self::Shared { data, start, .. } => Self::Shared {
                data: Arc::clone(data),
                start: start + range.start as u32,
                end: start + range.end as u32,
            },
            Self::Owned(entries) => Self::Owned(entries[range].to_vec()),
        }
    }
    /// Same backing, and `next` starts where this ends.
    fn adjoins(&self, next: &Self) -> bool {
        match (self, next) {
            (
                Self::Shared { data, end, .. },
                Self::Shared {
                    data: next_data,
                    start: next_start,
                    ..
                },
            ) => Arc::ptr_eq(data, next_data) && end == next_start,
            _ => false,
        }
    }
    /// Extend a shared range that `next` adjoins.
    fn extend_to(&mut self, next: &Self) {
        if let (Self::Shared { end, .. }, Self::Shared { end: next_end, .. }) = (self, next) {
            *end = *next_end;
        }
    }
    /// Same backing and range: an ownership shortcut, never consensus evidence.
    fn same(&self, other: &Self) -> bool {
        match (self, other) {
            (
                Self::Shared { data, start, end },
                Self::Shared {
                    data: other_data,
                    start: other_start,
                    end: other_end,
                },
            ) => Arc::ptr_eq(data, other_data) && start == other_start && end == other_end,
            _ => false,
        }
    }
    fn summary(&self) -> Option<Summary> {
        match self {
            Self::Shared { data, start, end } => data.summary(*start as usize..*end as usize),
            Self::Owned(entries) => facts::summarize(entries),
        }
    }
    fn totals(&self) -> Option<Totals> {
        match self {
            Self::Shared { data, start, end } => data.totals(*start as usize..*end as usize),
            Self::Owned(entries) => facts::totals_of(entries),
        }
    }
    /// What this piece keeps alive: a shared piece its whole backing, an
    /// owned piece its allocated entries and payloads.
    fn charge(&self) -> Option<usize> {
        match self {
            Self::Shared { data, .. } => data.charge(),
            Self::Owned(entries) => facts::charge_of(entries, entries.capacity()),
        }
    }
    /// Canonical-log charge: shared pieces as above, owned pieces logically
    /// (their visible entries and payloads), as the flat log was charged.
    fn log_charge(&self) -> Option<usize> {
        match self {
            Self::Shared { data, .. } => data.charge(),
            Self::Owned(entries) => facts::charge_of(entries, entries.len()),
        }
    }
    /// Payload of every entry when a shared backing's entries are all alike.
    fn uniform_payload(&self) -> Option<usize> {
        match self {
            Self::Shared { data, .. } => data.uniform_payload(),
            Self::Owned(_) => None,
        }
    }
    /// Exact payload total of `range` (relative) from a shared index.
    fn indexed_payload(&self, range: Range<usize>) -> Option<usize> {
        match self {
            Self::Shared { data, start, .. } => {
                let start = *start as usize;
                data.payload(start + range.start..start + range.end)
            }
            Self::Owned(_) => None,
        }
    }
}

fn totals(spans: &[Span]) -> Option<Totals> {
    match spans {
        [] => Some(Totals::default()),
        [only] => only.totals(),
        _ => spans
            .iter()
            .try_fold(Totals::default(), |sum, span| sum.join(span.totals()?)),
    }
}
/// Exact, from a shared piece's index, or a scan of the visible entries.
fn has_configuration(spans: &[Span]) -> bool {
    let scan = |span: &Span| {
        span.entries()
            .iter()
            .any(|entry| matches!(entry.payload, EntryPayload::Configuration(_)))
    };
    spans.iter().any(|span| match span {
        Span::Owned(_) => scan(span),
        Span::Shared { .. } => span
            .totals()
            .map_or_else(|| scan(span), |totals| totals.configurations != 0),
    })
}

impl fmt::Debug for Span {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::Shared { start, end, .. } => formatter
                .debug_struct("Shared")
                .field("start", start)
                .field("end", end)
                .finish_non_exhaustive(),
            Self::Owned(entries) => formatter
                .debug_tuple("Owned")
                .field(&entries.len())
                .finish(),
        }
    }
}

/// Ordered pieces under construction; one piece needs no heap allocation.
/// Adjacent ranges of one backing merge.
#[derive(Default)]
struct SpanBuilder {
    spans: Spans,
    many: Vec<Span>,
}
impl SpanBuilder {
    fn push(&mut self, span: Span) {
        match &mut self.spans {
            Spans::Empty if self.many.is_empty() => self.spans = Spans::One(span),
            Spans::One(last) if last.adjoins(&span) => last.extend_to(&span),
            Spans::One(_) => {
                if let Spans::One(first) = std::mem::take(&mut self.spans) {
                    self.many.push(first);
                }
                self.many.push(span);
            }
            _ => match self.many.last_mut() {
                Some(last) if last.adjoins(&span) => last.extend_to(&span),
                _ => self.many.push(span),
            },
        }
    }
    fn finish(self) -> EntryBatch {
        let spans = if self.many.is_empty() {
            self.spans
        } else {
            Spans::Many(self.many.into_boxed_slice())
        };
        EntryBatch { spans }
    }
}

#[derive(Clone, Default)]
enum Spans {
    #[default]
    Empty,
    One(Span),
    Many(Box<[Span]>),
}
impl Spans {
    fn as_slice(&self) -> &[Span] {
        match self {
            Self::Empty => &[],
            Self::One(span) => std::slice::from_ref(span),
            Self::Many(spans) => spans,
        }
    }
}

/// An immutable ordered entry sequence sharing its backings.
///
/// Cloning copies only range descriptors. Indexing, iteration and comparison
/// are logical: two batches are equal when their visible entries are equal,
/// however they are partitioned. There is no mutable access; edit a copy from
/// [`Self::to_vec`] and convert it back, which inspects it again.
///
/// The batch is as small as a vector: its length is read from its pieces.
#[derive(Clone, Default)]
pub struct EntryBatch {
    spans: Spans,
}
impl EntryBatch {
    fn single(span: Span) -> Self {
        Self {
            spans: Spans::One(span),
        }
    }
    #[inline]
    pub fn len(&self) -> usize {
        match &self.spans {
            Spans::Empty => 0,
            Spans::One(span) => span.len(),
            Spans::Many(spans) => spans.iter().map(Span::len).sum(),
        }
    }
    #[inline]
    pub fn is_empty(&self) -> bool {
        self.len() == 0
    }
    #[inline]
    pub fn first(&self) -> Option<&LogEntry> {
        match &self.spans {
            Spans::One(Span::Owned(entries)) => entries.first(),
            spans => spans.as_slice().first()?.entries().first(),
        }
    }
    #[inline]
    pub fn last(&self) -> Option<&LogEntry> {
        match &self.spans {
            Spans::One(Span::Owned(entries)) => entries.last(),
            Spans::Empty => None,
            _ => self.last_shared(),
        }
    }
    fn last_shared(&self) -> Option<&LogEntry> {
        self.spans.as_slice().last()?.entries().last()
    }
    #[inline]
    pub fn get(&self, offset: usize) -> Option<&LogEntry> {
        match &self.spans {
            Spans::One(Span::Owned(entries)) => entries.get(offset),
            _ => self.get_shared(offset),
        }
    }
    fn get_shared(&self, offset: usize) -> Option<&LogEntry> {
        if let Spans::One(span) = &self.spans {
            return span.entries().get(offset);
        }
        let mut offset = offset;
        for span in self.spans.as_slice() {
            if offset < span.len() {
                return span.entries().get(offset);
            }
            offset -= span.len();
        }
        None
    }
    pub fn iter(&self) -> Iter<'_> {
        match &self.spans {
            Spans::One(span) => Iter::from_parts(span.entries(), &[], &[], span.len()),
            spans => Iter::new(spans.as_slice(), 0, 0, self.len()),
        }
    }
    /// Contiguous pieces in order; their concatenation is this batch.
    pub fn slices(&self) -> impl DoubleEndedIterator<Item = &[LogEntry]> + ExactSizeIterator {
        self.spans.as_slice().iter().map(Span::entries)
    }
    /// Shared subrange; `None` for a reversed or out-of-bounds range.
    pub fn slice(&self, range: Range<usize>) -> Option<Self> {
        if range.start > range.end || range.end > self.len() {
            return None;
        }
        // An empty range holds no piece, so it keeps no backing alive.
        if range.is_empty() {
            return Some(Self::default());
        }
        let mut spans = SpanBuilder::default();
        let mut base = 0;
        for span in self.spans.as_slice() {
            let (start, end) = (base, base + span.len());
            base = end;
            if end <= range.start || start >= range.end {
                continue;
            }
            let from = range.start.max(start) - start;
            let to = range.end.min(end) - start;
            spans.push(span.part(from..to));
        }
        Some(spans.finish())
    }
    /// Borrowed when this batch has a single piece, otherwise one copy.
    pub fn contiguous(&self) -> Cow<'_, [LogEntry]> {
        match &self.spans {
            Spans::Empty => Cow::Borrowed(&[]),
            Spans::One(span) => Cow::Borrowed(span.entries()),
            Spans::Many(_) => Cow::Owned(self.to_vec()),
        }
    }
    pub fn to_vec(&self) -> Vec<LogEntry> {
        let mut entries = Vec::with_capacity(self.len());
        for slice in self.slices() {
            entries.extend_from_slice(slice);
        }
        entries
    }
    /// Replace this batch with an edited copy. The shared backings, and every
    /// other holder of them, are unchanged; the copy is inspected again.
    pub fn edit<R>(&mut self, edit: impl FnOnce(&mut Vec<LogEntry>) -> R) -> R {
        let mut entries = self.to_vec();
        let result = edit(&mut entries);
        *self = entries.into();
        result
    }
    /// Conservative retained charge of every backing this batch keeps alive,
    /// including entries outside its visible range; inline value excluded.
    pub fn retained_bytes(&self) -> Option<usize> {
        self.spans
            .as_slice()
            .iter()
            .try_fold(0usize, |sum, span| sum.checked_add(span.charge()?))
    }
    /// Charge as a canonical log holds these pieces on their own: shared
    /// pieces their whole backing, owned pieces their visible entries.
    pub(crate) fn log_charge(&self) -> Option<usize> {
        self.spans
            .as_slice()
            .iter()
            .try_fold(0usize, |sum, span| sum.checked_add(span.log_charge()?))
    }
    /// This batch as a leader's outbound Append keeps it.
    ///
    /// A shared piece pins its whole backing, which can be far wider than the
    /// message (a recovered log is one backing). Shared pieces are kept while
    /// what they keep alive beyond plain copies of their visible entries
    /// totals at most [`MESSAGE_HIDDEN_MAX`] and at most the selection budget
    /// the visible entries leave unused (`max_batch_bytes - 256 - (37 +
    /// payload)` per entry); any other piece is copied, as a plain vector
    /// would hold it. So a message's retained charge exceeds that of a plain
    /// vector of its visible entries by at most `MESSAGE_HIDDEN_MAX`, and it
    /// stays within the per-message bound the core reserves for its effects
    /// (`max_batch_bytes` plus one record per entry: a retained payload
    /// exceeds its length by at most 23 bytes, so records, payloads and the
    /// hidden bytes together stay within `max_batch_bytes - 256` plus `size_of
    /// LogEntry - 14` per entry). The visible entries, and so the protocol,
    /// are unchanged.
    pub(crate) fn for_message(self, max_batch_bytes: usize) -> Self {
        let spans = self.spans.as_slice();
        // An exact owned copy (every narrow selection) keeps nothing hidden.
        if let [Span::Owned(entries)] = spans {
            if entries.capacity() == entries.len() {
                return self;
            }
        }
        let visible = self
            .len()
            .checked_mul(37)
            .and_then(|bytes| bytes.checked_add(self.payload_bytes()));
        let allowance = visible
            .and_then(|visible| max_batch_bytes.checked_sub(256)?.checked_sub(visible))
            .unwrap_or(0)
            .min(MESSAGE_HIDDEN_MAX);
        // Pieces are kept in order while the running total stays within the
        // allowance; a piece that would exceed it is copied instead.
        let keeps = |span: &Span, hidden: &mut usize| match span
            .hidden()
            .and_then(|bytes| hidden.checked_add(bytes))
        {
            Some(total) if total <= allowance => {
                *hidden = total;
                true
            }
            _ => false,
        };
        let mut hidden = 0;
        if spans.iter().all(|span| keeps(span, &mut hidden)) {
            return self;
        }
        let mut hidden = 0;
        let mut pieces = SpanBuilder::default();
        for span in spans {
            if keeps(span, &mut hidden) {
                pieces.push(span.clone());
            } else {
                pieces.push(span.copied());
            }
        }
        pieces.finish()
    }
    /// Freeze decoded entries like `From<Vec<LogEntry>>`, admitting the
    /// bytes of any index with `admit_index` before it is allocated.
    pub(crate) fn decoded<E>(
        entries: Vec<LogEntry>,
        admit_index: impl FnOnce(usize) -> Result<(), E>,
    ) -> Result<Self, E> {
        if entries.len() < SHARE_MIN || u32::try_from(entries.len()).is_err() {
            return Ok(entries.into());
        }
        let end = entries.len() as u32;
        let data = Arc::new(BatchData::charged(entries, admit_index)?);
        Ok(Self::single(Span::Shared {
            data,
            start: 0,
            end,
        }))
    }
    /// Exact intrinsic facts of the visible entries; `None` when any piece is
    /// unsealed or a join does not continue the sequence.
    pub(crate) fn summary(&self) -> Option<Summary> {
        match &self.spans {
            Spans::Empty => Some(Summary::EMPTY),
            // A small owned batch is validated by the original loops, which
            // do less than measuring its facts first.
            Spans::One(Span::Owned(_)) => None,
            Spans::One(span) => span.summary(),
            Spans::Many(spans) => spans
                .iter()
                .try_fold(Summary::EMPTY, |sum, span| sum.join(span.summary()?)),
        }
    }
    /// Exact visible totals; `None` when a piece's index is unavailable.
    pub(crate) fn totals(&self) -> Option<Totals> {
        totals(self.spans.as_slice())
    }
    #[inline]
    pub(crate) fn has_configuration(&self) -> bool {
        match &self.spans {
            Spans::One(Span::Owned(entries)) => entries
                .iter()
                .any(|entry| matches!(entry.payload, EntryPayload::Configuration(_))),
            Spans::Empty => false,
            spans => has_configuration(spans.as_slice()),
        }
    }
    /// [`Self::retained_bytes`] and [`Self::has_data`] together, in one walk
    /// of a small owned batch; `None` when the charge overflows.
    #[inline]
    pub(crate) fn retained_bytes_and_data(&self) -> Option<(usize, bool)> {
        if let Spans::One(Span::Owned(entries)) = &self.spans {
            let mut data = false;
            let mut charge = entries
                .capacity()
                .checked_mul(std::mem::size_of::<LogEntry>())?;
            for entry in entries {
                charge = charge.checked_add(entry.retained_payload_bytes())?;
                data |= !matches!(entry.payload, EntryPayload::Noop);
            }
            return Some((charge, data));
        }
        if let Spans::Empty = &self.spans {
            return Some((0, false));
        }
        Some((self.retained_bytes()?, self.has_data()))
    }
    /// Any visible command or configuration.
    pub(crate) fn has_data(&self) -> bool {
        match &self.spans {
            Spans::Empty => return false,
            Spans::One(Span::Owned(entries)) => {
                return entries
                    .iter()
                    .any(|entry| !matches!(entry.payload, EntryPayload::Noop))
            }
            _ => {}
        }
        self.totals().map_or_else(
            || {
                self.iter()
                    .any(|entry| !matches!(entry.payload, EntryPayload::Noop))
            },
            |totals| totals.non_noops != 0,
        )
    }
    /// Visible payload total, summed as the old unchecked loop did when the
    /// index is unavailable.
    #[inline]
    pub(crate) fn payload_bytes(&self) -> usize {
        match &self.spans {
            Spans::One(Span::Owned(entries)) => entries.iter().map(LogEntry::payload_bytes).sum(),
            Spans::Empty => 0,
            _ => self.payload_bytes_shared(),
        }
    }
    fn payload_bytes_shared(&self) -> usize {
        self.totals().map_or_else(
            || self.iter().map(LogEntry::payload_bytes).sum(),
            |totals| totals.payload,
        )
    }
    /// The planner's suffix rule: indices from `from`, positive terms, each at
    /// least the previous (starting at the predecessor) and at most
    /// `hard_term`, payloads within `max_command_bytes`.
    #[inline]
    pub(crate) fn valid_suffix(
        &self,
        from: u64,
        predecessor_term: u64,
        hard_term: u64,
        max_command_bytes: usize,
    ) -> bool {
        // A small owned batch is validated by the original loop; an empty one
        // passes it.
        if let Spans::One(Span::Owned(entries)) = &self.spans {
            return valid_sequence(
                std::iter::once(entries.as_slice()),
                from,
                predecessor_term,
                hard_term,
                max_command_bytes,
            );
        }
        if let Spans::Empty = &self.spans {
            return true;
        }
        self.valid_suffix_shared(from, predecessor_term, hard_term, max_command_bytes)
    }
    fn valid_suffix_shared(
        &self,
        from: u64,
        predecessor_term: u64,
        hard_term: u64,
        max_command_bytes: usize,
    ) -> bool {
        let Some(summary) = self.summary() else {
            return valid_sequence(
                self.slices(),
                from,
                predecessor_term,
                hard_term,
                max_command_bytes,
            );
        };
        let Some((first, last)) = summary.bounds else {
            return true;
        };
        first.0 == from
            && first.1 >= predecessor_term
            && last.1 <= hard_term
            && self.payloads_within(summary, max_command_bytes)
    }
    /// The follower's append rule: the suffix rule after `previous_index`,
    /// bounded by the message term, and no configuration newer than the
    /// message's configuration scope.
    #[inline]
    pub(crate) fn valid_append(
        &self,
        previous_index: u64,
        previous_term: u64,
        message_term: u64,
        configuration: ConfigurationId,
        max_command_bytes: usize,
    ) -> bool {
        // A small owned batch is validated by the original loop; an empty one
        // passes it.
        if let Spans::One(Span::Owned(entries)) = &self.spans {
            return valid_append_sequence(
                std::iter::once(entries.as_slice()),
                previous_index,
                previous_term,
                message_term,
                configuration,
                max_command_bytes,
            );
        }
        if let Spans::Empty = &self.spans {
            return true;
        }
        self.valid_append_shared(
            previous_index,
            previous_term,
            message_term,
            configuration,
            max_command_bytes,
        )
    }
    fn valid_append_shared(
        &self,
        previous_index: u64,
        previous_term: u64,
        message_term: u64,
        configuration: ConfigurationId,
        max_command_bytes: usize,
    ) -> bool {
        let configurations_within = || {
            self.iter()
                .all(|entry| activated_configuration(entry).is_none_or(|id| id <= configuration))
        };
        let Some(summary) = self.summary() else {
            return valid_append_sequence(
                self.slices(),
                previous_index,
                previous_term,
                message_term,
                configuration,
                max_command_bytes,
            );
        };
        let Some((first, last)) = summary.bounds else {
            return true;
        };
        previous_index.checked_add(1) == Some(first.0)
            && first.1 >= previous_term
            && last.1 <= message_term
            && self.payloads_within(summary, max_command_bytes)
            && (summary.max_configuration <= Some(configuration) || configurations_within())
    }
    fn payloads_within(&self, summary: Summary, max_command_bytes: usize) -> bool {
        summary.max_payload <= max_command_bytes
            || self
                .iter()
                .all(|entry| entry.payload_bytes() <= max_command_bytes)
    }
    /// Entry slots allocated by the pieces: a shared piece's whole backing,
    /// an owned piece's capacity.
    #[cfg(test)]
    pub(crate) fn allocated_entries(&self) -> usize {
        self.spans
            .as_slice()
            .iter()
            .map(|span| match span {
                Span::Shared { data, .. } => data.entries.capacity(),
                Span::Owned(entries) => entries.capacity(),
            })
            .sum()
    }
    #[cfg(test)]
    pub(crate) fn backing_count(&self) -> usize {
        self.spans.as_slice().len()
    }
    #[cfg(test)]
    pub(crate) fn first_backing_ptr(&self) -> Option<*const LogEntry> {
        self.spans.as_slice().first().map(|span| match span {
            Span::Shared { data, .. } => data.entries.as_ptr(),
            Span::Owned(entries) => entries.as_ptr(),
        })
    }
    #[cfg(test)]
    pub(crate) fn is_shared(&self) -> bool {
        self.spans
            .as_slice()
            .iter()
            .all(|span| matches!(span, Span::Shared { .. }))
    }
}
/// The original follower loop over the visible pieces, for content without facts.
#[inline]
fn valid_append_sequence<'a>(
    slices: impl IntoIterator<Item = &'a [LogEntry]>,
    previous_index: u64,
    mut previous_term: u64,
    message_term: u64,
    configuration: ConfigurationId,
    max_command_bytes: usize,
) -> bool {
    let mut expected = previous_index.checked_add(1);
    for slice in slices {
        for entry in slice {
            if activated_configuration(entry).is_some_and(|id| id > configuration)
                || expected != Some(entry.index)
                || entry.term == 0
                || entry.term < previous_term
                || entry.term > message_term
                || entry.payload_bytes() > max_command_bytes
            {
                return false;
            }
            previous_term = entry.term;
            expected = entry.index.checked_add(1);
        }
    }
    true
}
/// The original per-entry loop over the visible pieces, for content without facts.
#[inline]
fn valid_sequence<'a>(
    slices: impl IntoIterator<Item = &'a [LogEntry]>,
    from: u64,
    mut previous_term: u64,
    hard_term: u64,
    max_command_bytes: usize,
) -> bool {
    let mut expected = Some(from);
    for slice in slices {
        for entry in slice {
            if expected != Some(entry.index)
                || entry.term == 0
                || entry.term < previous_term
                || entry.term > hard_term
                || entry.payload_bytes() > max_command_bytes
            {
                return false;
            }
            previous_term = entry.term;
            expected = entry.index.checked_add(1);
        }
    }
    true
}
impl From<Vec<LogEntry>> for EntryBatch {
    /// Freeze and inspect once; invalid content is kept and validated in full
    /// wherever it is used.
    #[inline]
    fn from(entries: Vec<LogEntry>) -> Self {
        Self {
            spans: Span::new(entries).map_or(Spans::Empty, Spans::One),
        }
    }
}
impl From<&[LogEntry]> for EntryBatch {
    fn from(entries: &[LogEntry]) -> Self {
        entries.to_vec().into()
    }
}
impl<const N: usize> From<[LogEntry; N]> for EntryBatch {
    fn from(entries: [LogEntry; N]) -> Self {
        Vec::from(entries).into()
    }
}
impl FromIterator<LogEntry> for EntryBatch {
    fn from_iter<I: IntoIterator<Item = LogEntry>>(entries: I) -> Self {
        entries.into_iter().collect::<Vec<_>>().into()
    }
}
impl IntoIterator for EntryBatch {
    type Item = LogEntry;
    type IntoIter = std::vec::IntoIter<LogEntry>;
    /// Owned entries are copies; the shared backings are unchanged.
    fn into_iter(self) -> Self::IntoIter {
        self.to_vec().into_iter()
    }
}
impl<'a> IntoIterator for &'a EntryBatch {
    type Item = &'a LogEntry;
    type IntoIter = Iter<'a>;
    fn into_iter(self) -> Self::IntoIter {
        self.iter()
    }
}
impl std::ops::Index<usize> for EntryBatch {
    type Output = LogEntry;
    fn index(&self, offset: usize) -> &LogEntry {
        self.get(offset).expect("entry batch offset out of bounds")
    }
}
impl fmt::Debug for EntryBatch {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter.debug_list().entries(self.iter()).finish()
    }
}
impl PartialEq for EntryBatch {
    fn eq(&self, other: &Self) -> bool {
        let (left, right) = (self.spans.as_slice(), other.spans.as_slice());
        self.len() == other.len()
            && ((left.len() == right.len() && left.iter().zip(right).all(|(a, b)| a.same(b)))
                || self.iter().eq(other.iter()))
    }
}
impl Eq for EntryBatch {}
impl PartialEq<[LogEntry]> for EntryBatch {
    fn eq(&self, other: &[LogEntry]) -> bool {
        self.len() == other.len() && self.iter().eq(other)
    }
}
impl PartialEq<Vec<LogEntry>> for EntryBatch {
    fn eq(&self, other: &Vec<LogEntry>) -> bool {
        *self == **other
    }
}
impl PartialEq<&[LogEntry]> for EntryBatch {
    fn eq(&self, other: &&[LogEntry]) -> bool {
        *self == **other
    }
}
impl<const N: usize> PartialEq<[LogEntry; N]> for EntryBatch {
    fn eq(&self, other: &[LogEntry; N]) -> bool {
        *self == other[..]
    }
}
impl PartialEq<EntryBatch> for Vec<LogEntry> {
    fn eq(&self, other: &EntryBatch) -> bool {
        *other == **self
    }
}
impl PartialEq<EntryBatch> for [LogEntry] {
    fn eq(&self, other: &EntryBatch) -> bool {
        *other == *self
    }
}
