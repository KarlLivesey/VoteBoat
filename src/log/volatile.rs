// SPDX-License-Identifier: RPL-1.5
// Copyright (c) 2026 Karl Livesey
//! Process-memory log for in-process embeddings. NOT DURABLE STORAGE.
use super::*;

/// Process-memory log: the core's log is the only canonical copy.
///
/// This is not durable storage and not a [`LogStore`]. It keeps no copy of the
/// log, writes nothing, and process exit loses everything. A core recovered with
/// [`crate::raft::Raft::recover_volatile`] emits payload-free
/// [`crate::raft::Effect::PersistVolatile`] tokens, completed with
/// [`crate::raft::persist_volatile`]. Never use it for state a client has been
/// told is durably committed beyond the life of this process: a "committed"
/// result here means committed by a quorum of in-process replicas only.
///
/// It owns only the store binding and validated limits that the core checks
/// against every completion, so a token cannot be completed through another
/// replica's log.
#[derive(Clone, Copy, Debug)]
pub struct VolatileLog {
    binding: StoreBinding,
    limits: LogLimits,
}

impl VolatileLog {
    pub fn new(binding: StoreBinding, limits: LogLimits) -> Result<Self, StorageError> {
        Ok(Self {
            binding,
            limits: limits.validate()?,
        })
    }
    pub fn binding(&self) -> StoreBinding {
        self.binding
    }
    pub fn limits(&self) -> LogLimits {
        self.limits
    }
    /// Validated initial state for a new group, exactly as a store would create
    /// it. Nothing is retained here; pass the result to `Raft::recover_volatile`.
    pub fn bootstrap(&self, bootstrap: &Bootstrap) -> Result<GroupLog, StorageError> {
        stage_create(bootstrap)
    }
}
