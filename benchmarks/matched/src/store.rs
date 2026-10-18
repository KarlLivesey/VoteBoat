// SPDX-License-Identifier: RPL-1.5
// Copyright (c) 2026 Karl Livesey
//! Volatile in-process log: each core's log is the only canonical copy.
use crate::attribution::Recorder;
use voteboat::{contracts::StorageError, identity::*, log::*};

/// Process memory only, not durable. The core plans, owns and installs every transition; completion is
/// a payload-free token, so there is no provider mirror to plan, validate,
/// install or cross-check. Store-side attribution phases therefore record no
/// calls, and the report keeps their keys.
pub(super) struct VolatileStore<const TRACE: bool = false> {
    pub attribution: Recorder<TRACE>,
    pub log: VolatileLog,
}
impl<const TRACE: bool> VolatileStore<TRACE> {
    pub fn new(binding: StoreBinding, limits: LogLimits) -> Result<Self, StorageError> {
        Ok(Self {
            attribution: Recorder::default(),
            log: VolatileLog::new(binding, limits)?,
        })
    }
}

#[cfg(test)]
mod tests;
