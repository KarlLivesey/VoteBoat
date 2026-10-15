// SPDX-License-Identifier: RPL-1.5
// Copyright (c) 2026 Karl Livesey
//! Owned positional history over immutable, conservatively charged batches.
use super::{EntryBatch, LogEntry};
use std::{
    alloc::Layout,
    borrow::Cow,
    fmt,
    ops::{Bound, Index, RangeBounds},
};

#[derive(Clone)]
struct Chunk {
    batch: EntryBatch,
    start: usize,
    end: usize,
    through: usize,
}

/// Ordered entries with independently owned descriptors and immutable backing.
/// A partial boundary retains and charges its entire original batch.
pub struct EntryHistory {
    chunks: Vec<Chunk>,
    batch_bytes: Option<usize>,
}
impl Default for EntryHistory {
    fn default() -> Self {
        Self {
            chunks: Vec::new(),
            batch_bytes: Some(0),
        }
    }
}
impl Clone for EntryHistory {
    fn clone(&self) -> Self {
        let mut chunks = Vec::with_capacity(self.chunks.capacity());
        chunks.extend(self.chunks.iter().cloned());
        Self {
            chunks,
            batch_bytes: self.batch_bytes,
        }
    }
}
impl EntryHistory {
    pub fn len(&self) -> usize {
        self.chunks.last().map_or(0, |chunk| chunk.through)
    }
    pub fn is_empty(&self) -> bool {
        self.chunks.is_empty()
    }
    pub fn get(&self, index: usize) -> Option<&LogEntry> {
        let chunk = self
            .chunks
            .get(self.chunks.partition_point(|chunk| chunk.through <= index))?;
        chunk
            .batch
            .get(chunk.start + index - (chunk.through - (chunk.end - chunk.start)))
    }
    pub fn first(&self) -> Option<&LogEntry> {
        self.get(0)
    }
    pub fn last(&self) -> Option<&LogEntry> {
        self.len().checked_sub(1).and_then(|index| self.get(index))
    }
    pub fn iter(&self) -> EntryIter<'_> {
        self.view(..).iter()
    }
    pub fn append_batch(&mut self, batch: EntryBatch) {
        if batch.is_empty() {
            return;
        }
        let through = self
            .len()
            .checked_add(batch.len())
            .expect("entry history length overflow");
        self.ensure_additional(1);
        self.batch_bytes = self
            .batch_bytes
            .and_then(|sum| sum.checked_add(batch.allocation_bytes()?));
        self.chunks.push(Chunk {
            start: 0,
            end: batch.len(),
            batch,
            through,
        });
    }
    pub fn append_history(&mut self, other: Self) {
        let offset = self.len();
        offset
            .checked_add(other.len())
            .expect("entry history length overflow");
        self.ensure_additional(other.chunks.len());
        self.batch_bytes = self
            .batch_bytes
            .and_then(|sum| sum.checked_add(other.batch_bytes?));
        self.chunks
            .extend(other.chunks.into_iter().map(|mut chunk| {
                chunk.through += offset;
                chunk
            }));
    }
    pub fn push(&mut self, entry: LogEntry) {
        self.append_batch(vec![entry].into());
    }
    pub fn extend_from_slice(&mut self, entries: &[LogEntry]) {
        self.append_batch(entries.to_vec().into());
    }
    pub fn truncate(&mut self, len: usize) {
        if len >= self.len() {
            return;
        }
        if len == 0 {
            self.clear();
            return;
        }
        let index = self.chunks.partition_point(|chunk| chunk.through < len);
        let removed = self.chunks[index + 1..]
            .iter()
            .try_fold(0usize, |sum, chunk| {
                sum.checked_add(chunk.batch.allocation_bytes()?)
            });
        self.chunks.truncate(index + 1);
        let last = &mut self.chunks[index];
        last.end -= last.through - len;
        last.through = len;
        self.subtract_removed(removed);
    }
    pub fn discard_prefix(&mut self, count: usize) {
        let count = count.min(self.len());
        if count == 0 {
            return;
        }
        let remove = self.chunks.partition_point(|chunk| chunk.through <= count);
        let removed = self.chunks[..remove].iter().try_fold(0usize, |sum, chunk| {
            sum.checked_add(chunk.batch.allocation_bytes()?)
        });
        self.chunks.drain(..remove);
        if let Some(first) = self.chunks.first_mut() {
            let before = first.through - (first.end - first.start);
            first.start += count - before;
        }
        for chunk in &mut self.chunks {
            chunk.through -= count;
        }
        self.subtract_removed(removed);
    }
    pub fn clear(&mut self) {
        self.chunks.clear();
        self.batch_bytes = Some(0);
    }
    /// Replace one logical value using fresh backing. Existing history clones
    /// and ranges keep their original immutable values.
    pub fn replace(&mut self, index: usize, entry: LogEntry) -> Option<LogEntry> {
        let old = self.get(index)?.clone();
        let chunk_index = self.chunks.partition_point(|chunk| chunk.through <= index);
        let chunk = &self.chunks[chunk_index];
        let old_charge = chunk.batch.allocation_bytes();
        let mut entries = chunk.batch[chunk.start..chunk.end].to_vec();
        let offset = index - (chunk.through - entries.len());
        entries[offset] = entry;
        let replacement = &mut self.chunks[chunk_index];
        replacement.start = 0;
        replacement.end = entries.len();
        replacement.batch = entries.into();
        self.batch_bytes = self.batch_bytes.and_then(|sum| {
            sum.checked_sub(old_charge?)?
                .checked_add(replacement.batch.allocation_bytes()?)
        });
        if self.batch_bytes.is_none() {
            self.recount_batches();
        }
        Some(old)
    }
    pub fn to_vec(&self) -> Vec<LogEntry> {
        self.iter().cloned().collect()
    }
    pub fn into_vec(mut self) -> Vec<LogEntry> {
        if self.chunks.len() == 1 {
            let chunk = self.chunks.pop().expect("one chunk");
            if chunk.start == 0 && chunk.end == chunk.batch.len() {
                return chunk.batch.into_vec();
            }
            return chunk.batch[chunk.start..chunk.end].to_vec();
        }
        self.to_vec()
    }
    /// Aggregate full backing entry slots, counting each retained span.
    pub fn capacity(&self) -> usize {
        self.chunks.iter().fold(0usize, |sum, chunk| {
            sum.saturating_add(chunk.batch.capacity())
        })
    }
    /// Full batch allocations per span plus descriptor capacity. This charges
    /// hidden boundary entries and never discounts sharing between owners.
    pub fn allocation_bytes(&self) -> Option<usize> {
        Self::charge(self.chunks.capacity(), self.batch_bytes)
    }
    #[cfg(test)]
    pub(crate) fn chunk_count(&self) -> usize {
        self.chunks.len()
    }
    #[cfg(test)]
    pub(crate) fn projected_append_allocation_bytes(&self, batch: &EntryBatch) -> Option<usize> {
        if batch.is_empty() {
            return self.allocation_bytes();
        }
        let capacity = self.descriptor_capacity(1)?;
        Self::charge(
            capacity,
            self.batch_bytes?.checked_add(batch.allocation_bytes()?),
        )
    }
    #[cfg(test)]
    pub(crate) fn projected_concat_allocation_bytes(&self, other: &Self) -> Option<usize> {
        Self::charge(
            self.descriptor_capacity(other.chunk_count())?,
            self.batch_bytes?.checked_add(other.batch_bytes?),
        )
    }
    #[cfg(test)]
    pub(crate) fn allocation_after_discard_prefix(&self, count: usize) -> Option<usize> {
        let remove = self.prefix_chunks(count);
        self.charge_retained(remove..self.chunks.len())
    }
    #[cfg(test)]
    pub(crate) fn allocation_after_truncate(&self, len: usize) -> Option<usize> {
        let keep = if len == 0 {
            0
        } else if len >= self.len() {
            self.chunks.len()
        } else {
            self.chunks.partition_point(|chunk| chunk.through < len) + 1
        };
        self.charge_retained(0..keep)
    }
    /// Conservative allocation after prefix cuts, optional trimmed tail
    /// concatenation, and append. Descriptor capacity covers either cutting
    /// before concatenation or materializing the overlay before a later cut.
    /// No entry or descriptor is copied by this projection.
    pub(crate) fn projected_view_allocation_bytes(
        &self,
        base_removed: usize,
        tail: Option<(&Self, usize)>,
        append: Option<&EntryBatch>,
    ) -> Option<usize> {
        let removed = self.prefix_chunks(base_removed);
        let mut count = self.chunks.len() - removed;
        let mut batches = self.retained_batches(removed..self.chunks.len())?;
        let mut capacity = self.chunks.capacity();
        if let Some((tail, removed)) = tail {
            let materialized =
                Self::capacity_for(capacity, self.chunks.len().checked_add(tail.chunks.len())?)?;
            let removed = tail.prefix_chunks(removed);
            count = count.checked_add(tail.chunks.len() - removed)?;
            capacity = Self::capacity_for(capacity, count)?.max(materialized);
            batches = batches.checked_add(tail.retained_batches(removed..tail.chunks.len())?)?;
        }
        if let Some(batch) = append.filter(|batch| !batch.is_empty()) {
            count = count.checked_add(1)?;
            capacity = Self::capacity_for(capacity, count)?;
            batches = batches.checked_add(batch.allocation_bytes()?)?;
        }
        Self::charge(capacity, Some(batches))
    }
    fn charge(capacity: usize, batches: Option<usize>) -> Option<usize> {
        Layout::array::<Chunk>(capacity)
            .ok()?
            .size()
            .checked_add(batches?)
    }
    fn prefix_chunks(&self, count: usize) -> usize {
        if count == 0 {
            0
        } else {
            self.chunks
                .partition_point(|chunk| chunk.through <= count.min(self.len()))
        }
    }
    fn descriptor_capacity(&self, additional: usize) -> Option<usize> {
        let required = self.chunks.len().checked_add(additional)?;
        Self::capacity_for(self.chunks.capacity(), required)
    }
    fn capacity_for(current: usize, required: usize) -> Option<usize> {
        if required <= current {
            Some(current)
        } else {
            required.max(4).checked_next_power_of_two()
        }
    }
    fn ensure_additional(&mut self, additional: usize) {
        let capacity = self
            .descriptor_capacity(additional)
            .expect("entry descriptor capacity overflow");
        if capacity > self.chunks.capacity() {
            self.chunks.reserve_exact(capacity - self.chunks.len());
        }
    }
    #[cfg(test)]
    fn charge_retained(&self, kept: std::ops::Range<usize>) -> Option<usize> {
        Self::charge(self.chunks.capacity(), self.retained_batches(kept))
    }
    fn retained_batches(&self, kept: std::ops::Range<usize>) -> Option<usize> {
        if kept.start == 0 && kept.end == self.chunks.len() {
            return self.batch_bytes;
        }
        if kept.is_empty() {
            return Some(0);
        }
        let removed = self.chunks[..kept.start]
            .iter()
            .chain(&self.chunks[kept.end..])
            .try_fold(0usize, |sum, chunk| {
                sum.checked_add(chunk.batch.allocation_bytes()?)
            });
        match (self.batch_bytes, removed) {
            (Some(total), Some(removed)) => total.checked_sub(removed),
            _ => self.chunks[kept].iter().try_fold(0usize, |sum, chunk| {
                sum.checked_add(chunk.batch.allocation_bytes()?)
            }),
        }
    }
    fn subtract_removed(&mut self, removed: Option<usize>) {
        self.batch_bytes = self.batch_bytes.and_then(|sum| sum.checked_sub(removed?));
        if self.batch_bytes.is_none() {
            self.recount_batches();
        }
    }
    fn recount_batches(&mut self) {
        self.batch_bytes = self.chunks.iter().try_fold(0usize, |sum, chunk| {
            sum.checked_add(chunk.batch.allocation_bytes()?)
        });
    }
    pub fn range<R: RangeBounds<usize>>(&self, bounds: R) -> Option<EntryRange<'_>> {
        let start = match bounds.start_bound() {
            Bound::Included(&index) => index,
            Bound::Excluded(&index) => index.checked_add(1)?,
            Bound::Unbounded => 0,
        };
        let end = match bounds.end_bound() {
            Bound::Included(&index) => index.checked_add(1)?,
            Bound::Excluded(&index) => index,
            Bound::Unbounded => self.len(),
        };
        (start <= end && end <= self.len()).then_some(EntryRange {
            history: self,
            start,
            end,
        })
    }
    pub fn view<R: RangeBounds<usize>>(&self, bounds: R) -> EntryRange<'_> {
        self.range(bounds)
            .expect("entry history range out of bounds")
    }
}
impl From<EntryBatch> for EntryHistory {
    fn from(batch: EntryBatch) -> Self {
        let mut history = Self::default();
        history.append_batch(batch);
        history
    }
}
impl From<Vec<LogEntry>> for EntryHistory {
    fn from(entries: Vec<LogEntry>) -> Self {
        Self::from(EntryBatch::from(entries))
    }
}
impl FromIterator<LogEntry> for EntryHistory {
    fn from_iter<T: IntoIterator<Item = LogEntry>>(iter: T) -> Self {
        Self::from(iter.into_iter().collect::<Vec<_>>())
    }
}
impl Extend<LogEntry> for EntryHistory {
    fn extend<T: IntoIterator<Item = LogEntry>>(&mut self, iter: T) {
        self.append_batch(iter.into_iter().collect());
    }
}
impl Index<usize> for EntryHistory {
    type Output = LogEntry;
    fn index(&self, index: usize) -> &Self::Output {
        self.get(index).expect("entry history index out of bounds")
    }
}
impl fmt::Debug for EntryHistory {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_list().entries(self).finish()
    }
}
impl PartialEq for EntryHistory {
    fn eq(&self, other: &Self) -> bool {
        self.len() == other.len() && self.iter().eq(other.iter())
    }
}
impl Eq for EntryHistory {}
impl PartialEq<Vec<LogEntry>> for EntryHistory {
    fn eq(&self, other: &Vec<LogEntry>) -> bool {
        self.len() == other.len() && self.iter().eq(other.iter())
    }
}
impl PartialEq<[LogEntry]> for EntryHistory {
    fn eq(&self, other: &[LogEntry]) -> bool {
        self.len() == other.len() && self.iter().eq(other.iter())
    }
}
impl PartialEq<&[LogEntry]> for EntryHistory {
    fn eq(&self, other: &&[LogEntry]) -> bool {
        self.len() == other.len() && self.iter().eq(other.iter())
    }
}
impl<const N: usize> PartialEq<[LogEntry; N]> for EntryHistory {
    fn eq(&self, other: &[LogEntry; N]) -> bool {
        self.len() == N && self.iter().eq(other.iter())
    }
}
impl<'a> IntoIterator for &'a EntryHistory {
    type Item = &'a LogEntry;
    type IntoIter = EntryIter<'a>;
    fn into_iter(self) -> Self::IntoIter {
        self.iter()
    }
}
impl IntoIterator for EntryHistory {
    type Item = LogEntry;
    type IntoIter = std::vec::IntoIter<LogEntry>;
    fn into_iter(self) -> Self::IntoIter {
        self.into_vec().into_iter()
    }
}

/// Borrowed logical range; a contiguous range needs no coalescing allocation.
#[derive(Clone, Copy)]
pub struct EntryRange<'a> {
    history: &'a EntryHistory,
    start: usize,
    end: usize,
}
impl fmt::Debug for EntryRange<'_> {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_list().entries(*self).finish()
    }
}
impl<'a> EntryRange<'a> {
    pub fn len(&self) -> usize {
        self.end - self.start
    }
    pub fn is_empty(&self) -> bool {
        self.start == self.end
    }
    pub fn iter(&self) -> EntryIter<'a> {
        EntryIter {
            history: self.history,
            front: self.start,
            back: self.end,
            front_chunk: self
                .history
                .chunks
                .partition_point(|chunk| chunk.through <= self.start),
            back_chunk: self
                .history
                .chunks
                .partition_point(|chunk| chunk.through < self.end),
        }
    }
    pub fn as_contiguous(&self) -> Option<&'a [LogEntry]> {
        if self.is_empty() {
            return Some(&[]);
        }
        let chunk = &self.history.chunks[self
            .history
            .chunks
            .partition_point(|chunk| chunk.through <= self.start)];
        if self.end > chunk.through {
            return None;
        }
        let base = chunk.through - (chunk.end - chunk.start);
        Some(&chunk.batch[chunk.start + self.start - base..chunk.start + self.end - base])
    }
    pub fn to_vec(&self) -> Vec<LogEntry> {
        self.iter().cloned().collect()
    }
    pub fn to_cow(&self) -> Cow<'a, [LogEntry]> {
        self.as_contiguous()
            .map_or_else(|| Cow::Owned(self.to_vec()), Cow::Borrowed)
    }
}
impl<'a> IntoIterator for EntryRange<'a> {
    type Item = &'a LogEntry;
    type IntoIter = EntryIter<'a>;
    fn into_iter(self) -> Self::IntoIter {
        self.iter()
    }
}
impl<'a> IntoIterator for &EntryRange<'a> {
    type Item = &'a LogEntry;
    type IntoIter = EntryIter<'a>;
    fn into_iter(self) -> Self::IntoIter {
        self.iter()
    }
}

/// Exact-size traversal over chunk boundaries in either direction.
#[derive(Clone)]
pub struct EntryIter<'a> {
    history: &'a EntryHistory,
    front: usize,
    back: usize,
    front_chunk: usize,
    back_chunk: usize,
}
impl<'a> Iterator for EntryIter<'a> {
    type Item = &'a LogEntry;
    fn next(&mut self) -> Option<Self::Item> {
        if self.front == self.back {
            return None;
        }
        let chunk = &self.history.chunks[self.front_chunk];
        let base = chunk.through - (chunk.end - chunk.start);
        let entry = &chunk.batch[chunk.start + self.front - base];
        self.front += 1;
        if self.front == chunk.through {
            self.front_chunk += 1;
        }
        Some(entry)
    }
    fn size_hint(&self) -> (usize, Option<usize>) {
        let len = self.back - self.front;
        (len, Some(len))
    }
}
impl DoubleEndedIterator for EntryIter<'_> {
    fn next_back(&mut self) -> Option<Self::Item> {
        if self.front == self.back {
            return None;
        }
        self.back -= 1;
        let mut chunk = &self.history.chunks[self.back_chunk];
        let mut base = chunk.through - (chunk.end - chunk.start);
        if self.back < base {
            self.back_chunk -= 1;
            chunk = &self.history.chunks[self.back_chunk];
            base = chunk.through - (chunk.end - chunk.start);
        }
        Some(&chunk.batch[chunk.start + self.back - base])
    }
}
impl ExactSizeIterator for EntryIter<'_> {}
impl std::iter::FusedIterator for EntryIter<'_> {}

#[cfg(test)]
mod tests;
