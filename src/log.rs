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
//! Consensus log transitions and the host-replaceable storage contract.
use crate::{
    contracts::{HardState, StorageError},
    identity::*,
    quorum::{Limits as PolicyLimits, Policy},
};
use std::collections::BTreeMap;

#[derive(Clone, Debug, Eq, PartialEq)]
pub enum EntryPayload {
    Noop,
    Command {
        operation: OperationId,
        bytes: Vec<u8>,
    },
}
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct LogEntry {
    pub index: u64,
    pub term: u64,
    pub payload: EntryPayload,
}
impl LogEntry {
    pub fn payload_bytes(&self) -> usize {
        match &self.payload {
            EntryPayload::Noop => 0,
            EntryPayload::Command { bytes, .. } => bytes.len(),
        }
    }
}

/// Explicitly authorized local bootstrap. Neither network traffic nor an empty
/// read can create a group. Static policy content is recoverable with its ID.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct Bootstrap {
    pub group: GroupIdentity,
    pub configuration: ConfigurationId,
    pub policy: Policy,
    /// Durable voter-to-store binding; disk loss needs explicit reconfiguration.
    pub voter_stores: BTreeMap<NodeId, StoreIdentity>,
}
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct GroupLog {
    pub bootstrap: Bootstrap,
    pub revision: LogRevision,
    pub generation: LogGeneration,
    pub hard_state: HardState,
    /// Contiguous committed prefix, never a largest completed index.
    pub commit_index: u64,
    pub entries: Vec<LogEntry>,
}
impl GroupLog {
    pub fn last_index(&self) -> u64 {
        self.entries.last().map_or(0, |e| e.index)
    }
    pub fn last_term(&self) -> u64 {
        self.entries.last().map_or(0, |e| e.term)
    }
    pub fn term_at(&self, index: u64) -> Option<u64> {
        if index == 0 {
            Some(0)
        } else {
            self.entries
                .get(usize::try_from(index - 1).ok()?)
                .map(|e| e.term)
        }
    }
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub struct Suffix {
    /// First index removed/replaced; last_index+1 is a pure append.
    pub from: u64,
    pub entries: Vec<LogEntry>,
}
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct LogUpdate {
    pub group: GroupIdentity,
    pub expected_revision: LogRevision,
    pub hard_state: HardState,
    pub commit_index: u64,
    pub suffix: Option<Suffix>,
}
#[derive(Clone, Debug, Eq, PartialEq)]
pub enum LogMutation {
    Create(Bootstrap),
    Update(LogUpdate),
}

/// Exact atomic group transition. Batch is an admission identity, not a prefix.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub struct LogTicket {
    pub binding: StoreBinding,
    pub batch: u64,
    pub group: GroupIdentity,
    pub revision: LogRevision,
    pub generation: LogGeneration,
    pub last_index: u64,
    pub term: u64,
}
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct DurableLog {
    pub tickets: Vec<LogTicket>,
}

#[derive(Clone, Copy, Debug)]
pub struct LogLimits {
    pub max_groups: usize,
    pub max_entries_per_group: usize,
    pub max_command_bytes: usize,
    pub max_batch_bytes: usize,
    pub max_batch_units: usize,
    pub max_pending_units: usize,
    pub max_wal_bytes: usize,
    /// Space applications cannot consume; term/vote/commit records can use it.
    pub control_reserve_bytes: usize,
}
impl Default for LogLimits {
    fn default() -> Self {
        Self {
            max_groups: 4096,
            max_entries_per_group: 16384,
            max_command_bytes: 64 * 1024,
            max_batch_bytes: 1024 * 1024,
            max_batch_units: 256,
            max_pending_units: 1024,
            max_wal_bytes: 64 * 1024 * 1024,
            control_reserve_bytes: 1024 * 1024,
        }
    }
}
impl LogLimits {
    pub fn validate(self) -> Result<Self, StorageError> {
        if self.max_groups == 0
            || self.max_entries_per_group == 0
            || self.max_entries_per_group > u32::MAX as usize
            || self.max_command_bytes == 0
            || self.max_batch_bytes < 256
            || self.max_command_bytes > self.max_batch_bytes
            || self.max_batch_bytes > u32::MAX as usize
            || self.max_batch_units == 0
            || self.max_batch_units > 4096
            || self.max_pending_units < self.max_batch_units
            || self.max_wal_bytes < self.max_batch_bytes
            || self.max_wal_bytes > u32::MAX as usize
            || self.control_reserve_bytes >= self.max_wal_bytes
        {
            return Err(StorageError::Rejected("invalid log limits"));
        }
        Ok(self)
    }
}

/// Public logical storage seam. Append/replace and dependent hard state form
/// one atomic per-group transition. A batch can share the barrier among groups.
/// No cross-group application transaction is promised. After admission, wait
/// cancellation cannot roll back writes. Errors with uncertain persistence
/// fence the store until recovery. Calls are synchronous progress steps.
/// Snapshot installation/retention enter this contract when implemented; this
/// version intentionally exposes no unsafe prefix-deletion operation.
pub trait LogStore {
    fn binding(&self) -> StoreBinding;
    fn limits(&self) -> LogLimits;
    /// Verified durable state only; missing is an error, never empty success.
    fn state(&self, group: GroupIdentity) -> Result<GroupLog, StorageError>;
    fn append_batch(&mut self, mutations: Vec<LogMutation>)
        -> Result<Vec<LogTicket>, StorageError>;
    fn barrier(&mut self, dependencies: &[LogTicket]) -> Result<DurableLog, StorageError>;
    /// Bounded durable matching range, guarded against suffix-generation reuse.
    /// A first entry exceeding max_bytes is refused rather than exceeding budget.
    fn fetch_range(
        &self,
        group: GroupIdentity,
        generation: LogGeneration,
        from: u64,
        max_entries: usize,
        max_bytes: usize,
    ) -> Result<Vec<LogEntry>, StorageError>;
}

/// Shared transition validation for native storage and host conformance tests.
/// Transactional: all units validate before the caller's map changes.
pub fn apply_batch(
    state: &mut BTreeMap<GroupIdentity, GroupLog>,
    mutations: &[LogMutation],
    limits: LogLimits,
) -> Result<(), StorageError> {
    let limits = limits.validate()?;
    if mutations.is_empty() || mutations.len() > limits.max_batch_units {
        return Err(StorageError::Rejected("batch unit limit"));
    }
    let mut next = state.clone();
    let mut seen = std::collections::BTreeSet::new();
    for mutation in mutations {
        let group = match mutation {
            LogMutation::Create(b) => b.group,
            LogMutation::Update(u) => u.group,
        };
        if !seen.insert(group) {
            return Err(StorageError::Rejected(
                "one atomic unit per group per batch",
            ));
        }
        match mutation {
            LogMutation::Create(b) => {
                if next.len() >= limits.max_groups || next.keys().any(|g| g.id == b.group.id) {
                    return Err(StorageError::Rejected(
                        "existing group identity or group limit",
                    ));
                }
                Policy::new(b.policy.tree().clone(), PolicyLimits::default()).map_err(|_| {
                    StorageError::Rejected("policy exceeds persisted format limits")
                })?;
                if b.voter_stores
                    .keys()
                    .copied()
                    .collect::<std::collections::BTreeSet<_>>()
                    != *b.policy.voters()
                {
                    return Err(StorageError::Rejected(
                        "voter stores must match exact policy membership",
                    ));
                }
                next.insert(
                    group,
                    GroupLog {
                        bootstrap: b.clone(),
                        revision: LogRevision::new(1).unwrap(),
                        generation: LogGeneration::new(1).unwrap(),
                        hard_state: HardState::default(),
                        commit_index: 0,
                        entries: Vec::new(),
                    },
                );
            }
            LogMutation::Update(update) => {
                let current = next
                    .get_mut(&group)
                    .ok_or(StorageError::Rejected("group not bootstrapped"))?;
                if update.expected_revision != current.revision
                    || !update.hard_state.follows(current.hard_state)
                    || update
                        .hard_state
                        .voted_for
                        .is_some_and(|n| !current.bootstrap.policy.voters().contains(&n))
                {
                    return Err(StorageError::Rejected("stale revision or invalid ballot"));
                }
                if let Some(suffix) = &update.suffix {
                    if suffix.from == 0
                        || suffix.from
                            > current
                                .last_index()
                                .checked_add(1)
                                .ok_or(StorageError::Rejected("index exhausted"))?
                        || suffix.from <= current.commit_index
                    {
                        return Err(StorageError::Rejected(
                            "replacement crosses committed prefix or log gap",
                        ));
                    }
                    let start = usize::try_from(suffix.from - 1)
                        .map_err(|_| StorageError::Rejected("index overflow"))?;
                    if suffix.entries.len() > limits.max_entries_per_group.saturating_sub(start) {
                        return Err(StorageError::Rejected("group entry limit"));
                    }
                    let mut previous_term = current.term_at(suffix.from - 1).unwrap();
                    for (offset, entry) in suffix.entries.iter().enumerate() {
                        if entry.index != suffix.from + offset as u64
                            || entry.term == 0
                            || entry.term < previous_term
                            || entry.term > update.hard_state.term
                            || entry.payload_bytes() > limits.max_command_bytes
                        {
                            return Err(StorageError::Rejected("entry index/term/payload invalid"));
                        }
                        previous_term = entry.term;
                    }
                    if suffix.from <= current.last_index() {
                        current.generation = current
                            .generation
                            .get()
                            .checked_add(1)
                            .and_then(LogGeneration::new)
                            .ok_or(StorageError::Rejected("generation exhausted"))?;
                    }
                    current.entries.truncate(start);
                    current.entries.extend(suffix.entries.iter().cloned());
                }
                if update.commit_index < current.commit_index
                    || update.commit_index > current.last_index()
                    || current.last_term() > update.hard_state.term
                {
                    return Err(StorageError::Rejected(
                        "commit regression/gap or hard term behind log",
                    ));
                }
                current.hard_state = update.hard_state;
                current.commit_index = update.commit_index;
                current.revision = current
                    .revision
                    .get()
                    .checked_add(1)
                    .and_then(LogRevision::new)
                    .ok_or(StorageError::Rejected("revision exhausted"))?;
            }
        }
    }
    *state = next;
    Ok(())
}
