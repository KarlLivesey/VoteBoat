// SPDX-License-Identifier: RPL-1.5
// Copyright (c) 2026 Karl Livesey
//! Cached conservative allocation accounting for an exact canonical revision.
use super::*;

impl Raft {
    /// The summary always describes the canonical log (`SummarizedLog`
    /// replaces it at every change), so its charge is exact.
    pub(super) fn retained_log_bytes(&self) -> Option<usize> {
        self.durable.summary().retained_bytes()
    }
}
