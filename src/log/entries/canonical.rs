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
//! The canonical log as ordered pieces of shared immutable backings.
use super::{
    facts, has_configuration, EntryBatch, EntryRange, Iter, LogEntry, Span, SpanBuilder, Spans,
};
use std::{borrow::Cow, fmt, ops::Range};

/// A group's retained entries, in order, as exact ranges of immutable
/// backings. Appending, truncating and trimming change only range
/// descriptors; entries are never moved or copied. Ordinal positions count
/// from the first retained entry and never trust an entry's own index.
#[derive(Clone, Default)]
pub struct EntryLog {
    spans: Vec<Span>,
    /// `ends[i]`: entries in `spans[..=i]`.
    ends: Vec<usize>,
    /// Index and term of the last entry, read on almost every step; only
    /// meaningful when the log is nonempty.
    tail: (u64, u64),
    /// `ends.last()`, kept inline for the same reason.
    len: usize,
}
impl EntryLog {
    pub fn len(&self) -> usize {
        self.len
    }
    pub fn is_empty(&self) -> bool {
        self.ends.is_empty()
    }
    pub fn first(&self) -> Option<&LogEntry> {
        self.spans.first()?.entries().first()
    }
    /// Index and term of the last entry, without visiting it.
    pub fn last_index_term(&self) -> Option<(u64, u64)> {
        (!self.ends.is_empty()).then_some(self.tail)
    }
    fn refresh_tail(&mut self) {
        self.len = self.ends.last().copied().unwrap_or(0);
        if let Some(last) = self.last() {
            self.tail = (last.index, last.term);
        }
    }
    pub fn last(&self) -> Option<&LogEntry> {
        self.spans.last()?.entries().last()
    }
    /// Piece holding `ordinal` and the ordinal of that piece's first entry.
    fn locate(&self, ordinal: usize) -> Option<(usize, usize)> {
        if ordinal >= self.len() {
            return None;
        }
        // Lookups are overwhelmingly near the tail (the previous entry, the
        // newly committed interval): probe the last few pieces before a
        // binary search over all of them.
        let mut position = self.spans.len() - 1;
        let last_start = self.len - self.spans[position].len();
        if ordinal >= last_start {
            return Some((position, last_start));
        }
        for _ in 0..4 {
            let start = self.start_of(position);
            if ordinal >= start {
                return Some((position, start));
            }
            position -= 1;
        }
        let position = self.ends[..=position].partition_point(|&end| end <= ordinal);
        Some((position, self.start_of(position)))
    }
    fn start_of(&self, position: usize) -> usize {
        position
            .checked_sub(1)
            .map_or(0, |previous| self.ends[previous])
    }
    pub fn get(&self, ordinal: usize) -> Option<&LogEntry> {
        // Most lookups fall in the last piece; past the end its `get` is `None`.
        let last = self.spans.last()?;
        if let Some(offset) = ordinal.checked_sub(self.len - last.len()) {
            return last.entries().get(offset);
        }
        let (position, start) = self.locate(ordinal)?;
        self.spans[position].entries().get(ordinal - start)
    }
    pub fn iter(&self) -> Iter<'_> {
        Iter::new(&self.spans, 0, 0, self.len())
    }
    /// The retained entries in `range`; `None` if reversed or out of bounds.
    pub fn range(&self, range: Range<usize>) -> Option<Iter<'_>> {
        self.borrowed(range).map(|range| range.iter())
    }
    /// Borrowed interval; `None` if reversed or out of bounds.
    pub fn borrowed(&self, range: Range<usize>) -> Option<EntryRange<'_>> {
        if range.start > range.end || range.end > self.len() {
            return None;
        }
        if range.is_empty() {
            return Some(EntryRange::EMPTY);
        }
        // Newly committed intervals usually lie in the last piece.
        if let Some(last) = self.spans.last() {
            if let Some(head) = range.start.checked_sub(self.len - last.len()) {
                let len = range.end - range.start;
                return Some(EntryRange::single(&last.entries()[head..head + len]));
            }
        }
        let (first, first_start) = self.locate(range.start)?;
        // The interval usually ends in the same or a following nearby piece.
        let last = if range.end <= self.ends[first] {
            first
        } else {
            first + self.ends[first..].partition_point(|&end| end < range.end)
        };
        let tail = self.ends[last] - range.end;
        Some(EntryRange::new(
            &self.spans[first..=last],
            range.start - first_start,
            tail,
            range.end - range.start,
        ))
    }
    /// Shared batch of `range`, copying only descriptors.
    pub fn shared_range(&self, range: Range<usize>) -> Option<EntryBatch> {
        if range.start > range.end || range.end > self.len() {
            return None;
        }
        if range.is_empty() {
            return Some(EntryBatch::default());
        }
        let (first, first_start) = self.locate(range.start)?;
        if range.end <= self.ends[first] {
            // Within one piece: one descriptor, no builder.
            let span = &self.spans[first];
            return Some(EntryBatch::single(
                span.part(range.start - first_start..range.end - first_start),
            ));
        }
        let mut spans = SpanBuilder::default();
        let mut ordinal = first_start;
        for span in &self.spans[first..] {
            let (start, end) = (ordinal, ordinal + span.len());
            ordinal = end;
            if start >= range.end {
                break;
            }
            let from = range.start.max(start) - start;
            let to = range.end.min(end) - start;
            spans.push(span.part(from..to));
        }
        Some(spans.finish())
    }
    /// The whole retained log as one shared batch.
    pub fn to_batch(&self) -> EntryBatch {
        let mut spans = SpanBuilder::default();
        for span in &self.spans {
            spans.push(span.clone());
        }
        spans.finish()
    }
    pub fn contiguous(&self, range: Range<usize>) -> Option<Cow<'_, [LogEntry]>> {
        self.borrowed(range).map(|range| range.contiguous())
    }
    pub fn to_vec(&self) -> Vec<LogEntry> {
        let mut entries = Vec::with_capacity(self.len());
        for span in &self.spans {
            entries.extend_from_slice(span.entries());
        }
        entries
    }
    /// Any retained configuration record.
    pub(crate) fn has_configuration(&self) -> bool {
        has_configuration(&self.spans)
    }
    /// Conservative retained charge: every backing a piece keeps alive, in
    /// full, including entries outside the retained range.
    pub fn retained_bytes(&self) -> Option<usize> {
        self.spans
            .iter()
            .try_fold(0usize, |sum, span| sum.checked_add(span.log_charge()?))
    }
    /// Charge this log would have after `append(batch)`: an adjoining first
    /// piece extends the last one instead of adding its backing again; owned
    /// entries add their logical charge wherever they are stored.
    pub(crate) fn appended_charge(&self, retained: usize, batch: &EntryBatch) -> Option<usize> {
        // An owned piece never adjoins: its logical charge is added.
        if let Spans::One(Span::Owned(entries)) = &batch.spans {
            return retained.checked_add(facts::charge_of(entries, entries.len())?);
        }
        let mut pieces = batch.spans.as_slice();
        if let (Some(last), Some(first)) = (self.spans.last(), pieces.first()) {
            if last.adjoins(first) {
                pieces = &pieces[1..];
            }
        }
        pieces
            .iter()
            .try_fold(retained, |sum, span| sum.checked_add(span.log_charge()?))
    }
    /// Charge after `trim_prefix(count)`: whole removed pieces release their
    /// backings; a partly trimmed shared piece keeps its full charge and a
    /// partly trimmed owned piece keeps its remaining entries.
    pub(crate) fn trimmed_charge(&self, count: usize) -> Option<usize> {
        if count >= self.len {
            return Some(0);
        }
        let keep = self.ends.partition_point(|&end| end <= count);
        let skip = count - self.start_of(keep);
        let first = match &self.spans[keep] {
            Span::Owned(entries) => {
                let left = &entries[skip..];
                facts::charge_of(left, left.len())?
            }
            shared @ Span::Shared { .. } => shared.log_charge()?,
        };
        self.spans[keep + 1..]
            .iter()
            .try_fold(first, |sum, span| sum.checked_add(span.log_charge()?))
    }
    /// Append a batch's pieces after the last entry, sharing their backings.
    pub fn append(&mut self, batch: EntryBatch) {
        match batch.spans {
            Spans::Empty => {}
            Spans::One(span) => self.append_span(span),
            Spans::Many(spans) => {
                for span in spans.into_vec() {
                    self.append_span(span);
                }
            }
        }
    }
    /// Move another log's pieces after this log's last entry.
    pub fn append_log(&mut self, other: EntryLog) {
        for span in other.spans {
            self.append_span(span);
        }
    }
    /// Exact charge of `self[skip..]` followed by `tail`, after trimming
    /// `count` entries from that sequence, without building it: pieces are
    /// charged as `append_log` would hold them.
    pub(crate) fn joined_trimmed_charge(
        &self,
        skip: usize,
        tail: &EntryLog,
        count: usize,
    ) -> Option<usize> {
        let visible = self.len.saturating_sub(skip);
        if count >= visible {
            return tail.trimmed_charge(count - visible);
        }
        let head = self.trimmed_charge(skip + count)?;
        let joined = match (self.spans.last(), tail.spans.first()) {
            (Some(last), Some(first)) if last.adjoins(first) => {
                tail.retained_bytes()?.checked_sub(first.log_charge()?)?
            }
            _ => tail.retained_bytes()?,
        };
        head.checked_add(joined)
    }
    fn append_span(&mut self, span: Span) {
        let ordinal = self.len() + span.len();
        // The appended entries end the log; an empty piece leaves the tail.
        let tail = span.entries().last().map(|last| (last.index, last.term));
        match (self.spans.last_mut(), span) {
            // Small batches gather into one owned piece, like the flat log.
            // Owned pieces never adjoin, so this precedes the shared check.
            (Some(Span::Owned(last)), Span::Owned(mut entries)) => {
                last.append(&mut entries);
                if let Some(end) = self.ends.last_mut() {
                    *end = ordinal;
                }
            }
            (Some(last), span) if last.adjoins(&span) => {
                last.extend_to(&span);
                if let Some(end) = self.ends.last_mut() {
                    *end = ordinal;
                }
            }
            (_, span) => {
                self.spans.push(span);
                self.ends.push(ordinal);
            }
        }
        self.len = ordinal;
        if let Some(tail) = tail {
            self.tail = tail;
        }
    }
    /// Append one entry as its own backing.
    pub fn push(&mut self, entry: LogEntry) {
        self.append(vec![entry].into());
    }
    /// Remove the last entry, returning a copy of it.
    pub fn pop(&mut self) -> Option<LogEntry> {
        let last = self.last()?.clone();
        self.truncate(self.len() - 1);
        Some(last)
    }
    pub fn extend_from_slice(&mut self, entries: &[LogEntry]) {
        if !entries.is_empty() {
            self.append(entries.to_vec().into());
        }
    }
    /// Replace this log with an edited copy. The shared backings, and every
    /// other holder of them, are unchanged; the copy is inspected again.
    pub fn edit<R>(&mut self, edit: impl FnOnce(&mut Vec<LogEntry>) -> R) -> R {
        let mut entries = self.to_vec();
        let result = edit(&mut entries);
        *self = entries.into();
        result
    }
    /// Keep the first `len` entries; longer logs are unchanged.
    pub fn truncate(&mut self, len: usize) {
        if len == 0 {
            self.clear();
            return;
        }
        let Some((position, start)) = self.locate(len - 1) else {
            return;
        };
        self.spans.truncate(position + 1);
        self.ends.truncate(position + 1);
        match &mut self.spans[position] {
            Span::Shared {
                start: piece_start,
                end,
                ..
            } => *end = *piece_start + (len - start),
            Span::Owned(entries) => entries.truncate(len - start),
        }
        self.ends[position] = len;
        self.refresh_tail();
    }
    /// Remove the first `count` entries (all of them if fewer remain).
    pub fn trim_prefix(&mut self, count: usize) {
        if count == 0 {
            return;
        }
        if count >= self.len() {
            self.clear();
            return;
        }
        let keep = self.ends.partition_point(|&end| end <= count);
        let first_start = self.start_of(keep);
        self.spans.drain(..keep);
        self.ends.drain(..keep);
        match &mut self.spans[0] {
            Span::Shared { start, .. } => *start += count - first_start,
            Span::Owned(entries) => {
                entries.drain(..count - first_start);
            }
        }
        for end in &mut self.ends {
            *end -= count;
        }
        self.len -= count;
    }
    pub fn clear(&mut self) {
        self.spans.clear();
        self.ends.clear();
        self.len = 0;
    }
    /// Largest prefix from `offset` that the replication selector admits,
    /// shared: at most `max_entries`, each costing `37 + payload` with a
    /// 256-byte allowance, stopping before the first entry that would exceed
    /// `max_batch_bytes`, exactly as the original per-entry loop.
    pub(crate) fn replication_range(
        &self,
        offset: usize,
        max_entries: usize,
        max_batch_bytes: usize,
    ) -> EntryBatch {
        let Some((position, start)) = self.locate(offset) else {
            return EntryBatch::default();
        };
        // The first piece decides alone when the selection stops inside it,
        // reaches `max_entries` or there is no next piece: one descriptor, no
        // builder.
        let first = &self.spans[position];
        let skip = offset - start;
        let limit = (first.len() - skip).min(max_entries);
        let (taken, bytes) = select(first, skip, limit, 0, max_batch_bytes);
        if taken < limit || taken == max_entries || position + 1 == self.spans.len() {
            if taken == 0 {
                return EntryBatch::default();
            }
            return EntryBatch::single(first.part(skip..skip + taken));
        }
        let mut count = taken;
        let mut bytes = bytes;
        let mut spans = SpanBuilder::default();
        spans.push(first.part(skip..skip + taken));
        // Later pieces are selected from their first entry.
        for span in &self.spans[position + 1..] {
            if count == max_entries {
                break;
            }
            let limit = span.len().min(max_entries - count);
            let (taken, used) = select(span, 0, limit, bytes, max_batch_bytes);
            if taken != 0 {
                spans.push(span.part(0..taken));
            }
            count += taken;
            bytes = used;
            if taken < limit {
                break;
            }
        }
        spans.finish()
    }
}

/// Entries of the piece from `from` (at most `limit`) admitted after
/// `bytes`, and the running total after them.
fn select(span: &Span, from: usize, limit: usize, bytes: usize, max: usize) -> (usize, usize) {
    if let Span::Owned(entries) = span {
        // No index for a small owned piece: the original loop.
        return select_scan(&entries[from..from + limit], bytes, max);
    }
    // Every admitted entry adds `37 + payload`; it is admitted exactly when
    // the new running total plus the 256-byte allowance is within `max`.
    let total = |taken: usize| -> Option<usize> {
        let payload = span.indexed_payload(from..from + taken)?;
        bytes
            .checked_add(taken.checked_mul(37)?)?
            .checked_add(payload)
    };
    let fits = |taken: usize| {
        total(taken)
            .and_then(|total| total.checked_add(256))
            .is_some_and(|total| total <= max)
    };
    if let Some(payload) = span.uniform_payload() {
        // Equal sizes: the admitted count is the floor of the remaining budget.
        let room = max
            .checked_sub(256)
            .and_then(|room| room.checked_sub(bytes));
        let taken = match (room, payload.checked_add(37)) {
            (Some(room), Some(size)) => (room / size).min(limit),
            _ => 0,
        };
        return (taken, total(taken).unwrap_or(bytes));
    }
    if span.indexed_payload(from..from).is_some() {
        // The running total only grows, so the admitted count is the largest
        // prefix that fits; uniform and prefix-sum totals are exact.
        let (mut low, mut high) = (0, limit);
        while low < high {
            let middle = low + (high - low).div_ceil(2);
            if fits(middle) {
                low = middle;
            } else {
                high = middle - 1;
            }
        }
        return (low, total(low).unwrap_or(bytes));
    }
    select_scan(&span.entries()[from..from + limit], bytes, max)
}
/// The original per-entry selection loop over `entries`.
fn select_scan(entries: &[LogEntry], bytes: usize, max: usize) -> (usize, usize) {
    let mut used = bytes;
    for (taken, entry) in entries.iter().enumerate() {
        let size = 37 + entry.payload_bytes();
        if size > max.saturating_sub(used + 256) {
            return (taken, used);
        }
        used += size;
    }
    (entries.len(), used)
}
impl From<Vec<LogEntry>> for EntryLog {
    /// One raw piece; recovery validates the history independently.
    fn from(entries: Vec<LogEntry>) -> Self {
        let mut log = Self::default();
        log.append(entries.into());
        log
    }
}
impl From<EntryBatch> for EntryLog {
    fn from(batch: EntryBatch) -> Self {
        let mut log = Self::default();
        log.append(batch);
        log
    }
}
impl Extend<LogEntry> for EntryLog {
    /// The new entries form one new backing after the existing ones.
    fn extend<I: IntoIterator<Item = LogEntry>>(&mut self, entries: I) {
        let entries: Vec<_> = entries.into_iter().collect();
        if !entries.is_empty() {
            self.append(entries.into());
        }
    }
}
impl FromIterator<LogEntry> for EntryLog {
    fn from_iter<I: IntoIterator<Item = LogEntry>>(entries: I) -> Self {
        entries.into_iter().collect::<Vec<_>>().into()
    }
}
impl IntoIterator for EntryLog {
    type Item = LogEntry;
    type IntoIter = std::vec::IntoIter<LogEntry>;
    /// Owned entries are copies; the shared backings are unchanged.
    fn into_iter(self) -> Self::IntoIter {
        self.to_vec().into_iter()
    }
}
impl<'a> IntoIterator for &'a EntryLog {
    type Item = &'a LogEntry;
    type IntoIter = Iter<'a>;
    fn into_iter(self) -> Self::IntoIter {
        self.iter()
    }
}
impl std::ops::Index<usize> for EntryLog {
    type Output = LogEntry;
    fn index(&self, ordinal: usize) -> &LogEntry {
        self.get(ordinal).expect("entry log position out of bounds")
    }
}
impl fmt::Debug for EntryLog {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter.debug_list().entries(self.iter()).finish()
    }
}
impl PartialEq for EntryLog {
    fn eq(&self, other: &Self) -> bool {
        self.len() == other.len()
            && ((self.spans.len() == other.spans.len()
                && self.spans.iter().zip(&other.spans).all(|(a, b)| a.same(b)))
                || self.iter().eq(other.iter()))
    }
}
impl Eq for EntryLog {}
impl PartialEq<[LogEntry]> for EntryLog {
    fn eq(&self, other: &[LogEntry]) -> bool {
        self.len() == other.len() && self.iter().eq(other)
    }
}
impl PartialEq<Vec<LogEntry>> for EntryLog {
    fn eq(&self, other: &Vec<LogEntry>) -> bool {
        *self == **other
    }
}
impl PartialEq<&[LogEntry]> for EntryLog {
    fn eq(&self, other: &&[LogEntry]) -> bool {
        *self == **other
    }
}
impl<const N: usize> PartialEq<[LogEntry; N]> for EntryLog {
    fn eq(&self, other: &[LogEntry; N]) -> bool {
        *self == other[..]
    }
}
impl PartialEq<EntryLog> for Vec<LogEntry> {
    fn eq(&self, other: &EntryLog) -> bool {
        *other == **self
    }
}
