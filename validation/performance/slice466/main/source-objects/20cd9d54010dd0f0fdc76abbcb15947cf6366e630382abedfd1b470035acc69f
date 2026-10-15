// SPDX-License-Identifier: RPL-1.5
// Copyright (c) 2026 Karl Livesey
//! Immutable entries with full, per-owner backing accounting.
use super::{EntryPayload, LogEntry};
use std::{
    alloc::Layout,
    fmt,
    ops::Deref,
    sync::{atomic::AtomicUsize, Arc},
};

struct Backing {
    entries: Vec<LogEntry>,
    payload_bytes: Option<usize>,
    plain: bool,
}

/// Immutable entry storage. Clones share contents, never validation authority.
///
/// Capture preserves raw input capacity, including an empty reserved vector.
/// A capacity-zero empty value retains no allocation.
/// Explicit mutable copies cannot alter other owners or queued effects.
#[derive(Clone, Default)]
pub struct EntryBatch(Option<Arc<Backing>>);

impl EntryBatch {
    pub fn as_slice(&self) -> &[LogEntry] {
        self.0.as_ref().map_or(&[], |backing| &backing.entries)
    }
    pub fn as_ptr(&self) -> *const LogEntry {
        self.as_slice().as_ptr()
    }
    pub fn capacity(&self) -> usize {
        self.0
            .as_ref()
            .map_or(0, |backing| backing.entries.capacity())
    }
    pub fn to_vec(&self) -> Vec<LogEntry> {
        self.as_slice().to_vec()
    }
    /// Recover unique storage, or materialize an independent mutable vector.
    pub fn into_vec(self) -> Vec<LogEntry> {
        self.0.map_or_else(Vec::new, |backing| {
            Arc::try_unwrap(backing)
                .map_or_else(|shared| shared.entries.clone(), |owned| owned.entries)
        })
    }
    /// Complete backing charge, excluding this inline handle. Shared owners
    /// each charge the complete allocation, including spare entry capacity.
    pub fn allocation_bytes(&self) -> Option<usize> {
        let Some(backing) = &self.0 else {
            return Some(0);
        };
        Self::allocation_bytes_for_shape(backing.entries.capacity(), backing.payload_bytes?)
    }
    /// Preflight an already-validated raw shape without capturing or allocating.
    pub(crate) fn allocation_bytes_for_shape(
        capacity: usize,
        payload_bytes: usize,
    ) -> Option<usize> {
        if capacity == 0 {
            return Some(0);
        }
        let header = Layout::new::<[AtomicUsize; 2]>().align_to(2).ok()?;
        let arc = header
            .extend(Layout::new::<Backing>())
            .ok()?
            .0
            .pad_to_align();
        arc.size()
            .checked_add(Layout::array::<LogEntry>(capacity).ok()?.size())?
            .checked_add(payload_bytes)
    }
    pub(crate) fn is_plain(&self) -> bool {
        self.0.as_ref().is_none_or(|backing| backing.plain)
    }
    /// Preserve the former independent Vec clone for raw spare capacity and
    /// configuration payloads. Only already-tight plain content can be shared.
    pub(crate) fn normalized_clone(&self) -> Self {
        if self.capacity() == self.len() && self.is_plain() {
            self.clone()
        } else {
            self.to_vec().into()
        }
    }
}
impl From<Vec<LogEntry>> for EntryBatch {
    fn from(entries: Vec<LogEntry>) -> Self {
        if entries.capacity() == 0 {
            return Self::default();
        }
        let payload_bytes = entries.iter().try_fold(0usize, |total, entry| {
            total.checked_add(entry.retained_payload_bytes())
        });
        let plain = entries
            .iter()
            .all(|entry| !matches!(entry.payload, EntryPayload::Configuration(_)));
        Self(Some(Arc::new(Backing {
            entries,
            payload_bytes,
            plain,
        })))
    }
}
impl FromIterator<LogEntry> for EntryBatch {
    fn from_iter<T: IntoIterator<Item = LogEntry>>(iter: T) -> Self {
        Self::from(iter.into_iter().collect::<Vec<_>>())
    }
}
impl Deref for EntryBatch {
    type Target = [LogEntry];
    fn deref(&self) -> &Self::Target {
        self.as_slice()
    }
}
impl AsRef<[LogEntry]> for EntryBatch {
    fn as_ref(&self) -> &[LogEntry] {
        self.as_slice()
    }
}
impl fmt::Debug for EntryBatch {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        self.as_slice().fmt(f)
    }
}
impl PartialEq for EntryBatch {
    fn eq(&self, other: &Self) -> bool {
        match (&self.0, &other.0) {
            (Some(left), Some(right)) if Arc::ptr_eq(left, right) => true,
            _ => self.as_slice() == other.as_slice(),
        }
    }
}
impl Eq for EntryBatch {}
impl PartialEq<Vec<LogEntry>> for EntryBatch {
    fn eq(&self, other: &Vec<LogEntry>) -> bool {
        self.as_slice() == other.as_slice()
    }
}
impl PartialEq<[LogEntry]> for EntryBatch {
    fn eq(&self, other: &[LogEntry]) -> bool {
        self.as_slice() == other
    }
}
impl PartialEq<&[LogEntry]> for EntryBatch {
    fn eq(&self, other: &&[LogEntry]) -> bool {
        self.as_slice() == *other
    }
}
impl<const N: usize> PartialEq<[LogEntry; N]> for EntryBatch {
    fn eq(&self, other: &[LogEntry; N]) -> bool {
        self.as_slice() == other
    }
}
impl<'a> IntoIterator for &'a EntryBatch {
    type Item = &'a LogEntry;
    type IntoIter = std::slice::Iter<'a, LogEntry>;
    fn into_iter(self) -> Self::IntoIter {
        self.iter()
    }
}
impl IntoIterator for EntryBatch {
    type Item = LogEntry;
    type IntoIter = std::vec::IntoIter<LogEntry>;
    fn into_iter(self) -> Self::IntoIter {
        self.into_vec().into_iter()
    }
}

#[cfg(test)]
mod tests;
