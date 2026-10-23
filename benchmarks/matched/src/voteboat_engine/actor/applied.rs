// SPDX-License-Identifier: RPL-1.5
// Copyright (c) 2026 Karl Livesey
use super::*;
/// Carry actual immutable command context through either original FIFO arm;
/// no copied entry vector is created for borrowed delivery.
#[derive(Clone, Copy)]
pub(super) enum Applied<'a> {
    Borrowed(EntryRange<'a>),
    Owned(&'a [LogEntry]),
}
impl<'a> Applied<'a> {
    pub fn apply(self, application: &mut Noop) -> Result<Vec<OperationId>, ApplicationError> {
        match self {
            Self::Borrowed(entries) => application.apply_entries(entries),
            Self::Owned(entries) => application.apply_batch(entries),
        }
    }
    pub fn iter(self) -> impl Iterator<Item = &'a LogEntry> {
        let len = match self {
            Self::Borrowed(entries) => entries.len(),
            Self::Owned(entries) => entries.len(),
        };
        (0..len).filter_map(move |i| match self {
            Self::Borrowed(entries) => entries.get(i),
            Self::Owned(entries) => entries.get(i),
        })
    }
    pub fn commands(self) -> impl Iterator<Item = &'a LogEntry> {
        self.iter()
            .filter(|entry| matches!(entry.payload, EntryPayload::Command { .. }))
    }
}
