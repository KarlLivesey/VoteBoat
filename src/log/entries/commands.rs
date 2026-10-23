// SPDX-License-Identifier: RPL-1.5
// Copyright (c) 2026 Karl Livesey
//! Command-origin ownership, without receiving-core authority or caller facts.
use super::{facts::BatchData, EntryBatch, EntryPayload, LogEntry, Span, SHARE_MIN};
use crate::{identity::OperationId, log::CommandBytes};
use std::sync::Arc;

/// Only `push` creates entries. No arbitrary entry/mutable slice constructor is
/// exposed: decoded, edited and configuration entries cannot enter this path.
#[derive(Debug)]
pub(crate) struct CommandEntries {
    entries: Vec<LogEntry>,
}
impl CommandEntries {
    pub(crate) fn with_capacity(capacity: usize) -> Self {
        Self {
            entries: Vec::with_capacity(capacity),
        }
    }
    pub(crate) fn len(&self) -> usize {
        self.entries.len()
    }
    pub(crate) fn is_empty(&self) -> bool {
        self.entries.is_empty()
    }
    pub(crate) fn capacity(&self) -> usize {
        self.entries.capacity()
    }
    pub(crate) fn reserve_exact(&mut self, additional: usize) {
        self.entries.reserve_exact(additional);
    }
    pub(crate) fn shrink_to(&mut self, capacity: usize) {
        self.entries.shrink_to(capacity);
    }
    pub(crate) fn push(&mut self, operation: OperationId, bytes: CommandBytes) {
        self.entries.push(LogEntry {
            index: 0,
            term: 0,
            payload: EntryPayload::Command { operation, bytes },
        });
    }
    pub(crate) fn iter(&self) -> impl Iterator<Item = (OperationId, &[u8])> + '_ {
        self.entries.iter().map(|entry| match &entry.payload {
            EntryPayload::Command { operation, bytes } => (*operation, bytes.as_slice()),
            _ => unreachable!("command-origin storage only constructs commands"),
        })
    }
    #[cfg(test)]
    pub(crate) fn allocation(&self) -> (*const LogEntry, usize) {
        (self.entries.as_ptr(), self.entries.capacity())
    }
    pub(super) fn into_vec(self) -> Vec<LogEntry> {
        self.entries
    }
    /// The caller already admitted the batch under receiving-core authority.
    /// Recheck arithmetic/positive term before assigning anything; neither a
    /// partially bound batch nor a caller-supplied fact can escape.
    pub(crate) fn bind(self, first: u64, term: u64) -> Option<EntryBatch> {
        let len = self.len();
        if len == 0 || term == 0 {
            return None;
        }
        first.checked_add(u64::try_from(len - 1).ok()?)?;
        if len >= SHARE_MIN {
            if let Ok(end) = u32::try_from(len) {
                return Some(EntryBatch::single(Span::Shared {
                    data: Arc::new(BatchData::commands(self, first, term)),
                    start: 0,
                    end,
                }));
            }
        }
        let mut entries = self.entries;
        for (offset, entry) in entries.iter_mut().enumerate() {
            entry.index = first + offset as u64;
            entry.term = term;
        }
        if len == 1 {
            Some(EntryBatch::from_entry(entries.pop()?))
        } else {
            Some(EntryBatch::single(Span::Owned(entries)))
        }
    }
}
