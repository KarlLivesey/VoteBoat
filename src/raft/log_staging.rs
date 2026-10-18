// SPDX-License-Identifier: RPL-1.5
// Copyright (c) 2026 Karl Livesey
//! Cached conservative allocation accounting for an exact canonical revision.
use super::*;

impl Raft {
    pub(super) fn retained_log_bytes(&self) -> Option<usize> {
        if self.log_summary.matches(&self.durable) {
            return self.log_summary.retained_bytes();
        }
        // A stale summary is never an admission authority. Preserve the
        // conservative inspection for diagnostics, charged as a fresh summary
        // would be; transition planning rejects mismatched summary versions
        // independently.
        let mut bytes = self.durable.entries.retained_bytes()?;
        if let Some(base) = &self.durable.snapshot_membership {
            bytes = bytes.checked_add(base.retained_bytes())?;
        }
        Some(bytes)
    }
}
