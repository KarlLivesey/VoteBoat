// SPDX-License-Identifier: RPL-1.5
// Copyright (c) 2026 Karl Livesey
//! Startup envelopes for owned original proposals and ordinary replication.
use super::*;

/// Count ceilings supplement storage, wire, scheduler and owned-byte budgets.
/// They do not change quorum or durability semantics. A host must ensure its
/// selected wire/provider supports the configured outbound replication range.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub struct BatchLimits {
    pub max_proposal_entries: usize,
    pub max_replication_entries: usize,
}
impl BatchLimits {
    /// Opt-in original-entry ceiling; owned-byte and retained-history limits
    /// remain independent. Existing default proposal batches contain at most64.
    pub const MAX_PROPOSAL_ENTRIES: usize = 16384;
    /// Fixed bounded ordinary receive ceiling, independent of outbound batching.
    /// A default64-entry sender can therefore receive a larger peer's chunks.
    pub const MAX_REPLICATION_ENTRIES: usize = 1024;

    pub fn validate(self) -> Result<Self, RaftError> {
        if self.max_proposal_entries == 0
            || self.max_proposal_entries > Self::MAX_PROPOSAL_ENTRIES
            || self.max_replication_entries == 0
            || self.max_replication_entries > Self::MAX_REPLICATION_ENTRIES
        {
            return Err(StorageError::Rejected("invalid core batch limits").into());
        }
        Ok(self)
    }
}
impl Default for BatchLimits {
    fn default() -> Self {
        Self {
            max_proposal_entries: MAX_PROPOSAL_BATCH_ENTRIES,
            max_replication_entries: 64,
        }
    }
}
impl Raft {
    /// Configure a recovered Core before its first event. Every first event,
    /// including a refused one, locks the setting for this incarnation. Hosts
    /// reapply it after verified recovery; it is not replicated safety state.
    /// Incoming ordinary appends retain the independent1024-entry ceiling and
    /// existing byte/identity/term/configuration checks. Repair RPCs retain64.
    pub fn configure_batch_limits(&mut self, limits: BatchLimits) -> Result<(), RaftError> {
        if self.fenced {
            return Err(RaftError::Fenced);
        }
        if self.batch_limits_locked {
            return Err(RaftError::Busy);
        }
        self.batch_limits = limits.validate()?;
        Ok(())
    }
    pub fn batch_limits(&self) -> BatchLimits {
        self.batch_limits
    }
}
