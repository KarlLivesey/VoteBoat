// SPDX-License-Identifier: RPL-1.5
// Copyright (c) 2026 Karl Livesey
//! Original commands staged once, as the log entries the core will persist.
use super::*;

const ENTRY: usize = size_of::<LogEntry>();
const HEADER: usize = size_of::<Vec<LogEntry>>();

/// Bounded host staging for original commands, built directly as log entries.
///
/// Create one with `Raft::proposal_batch`, add originals in order with `push`
/// and submit it with `Raft::propose_entries`. Each original's bytes become its
/// shared command allocation when pushed, so the core neither stages a second
/// proposal representation nor rescans the input. Index and term are assigned
/// only by `propose_entries`, from the receiving core's own state.
///
/// The limits copied at creation bound this allocation; they are not admission
/// authority. `propose_entries` rechecks the recorded counts, command lengths
/// and staging peak against the receiving core's current limits.
#[derive(Debug)]
pub struct ProposalBatch {
    entries: Vec<LogEntry>,
    max_entries: usize,
    max_command_bytes: usize,
    max_batch_bytes: usize,
    shared: usize,
    largest: usize,
    peak: usize,
}

impl ProposalBatch {
    /// Append one original command.
    ///
    /// Before copying the bytes into their shared allocation this checks the
    /// original count, command length and, once the batch holds another
    /// entry, the staging peak: the entry allocation (both buffers while it
    /// grows), the shared bytes already staged, the incoming Vec's capacity
    /// and the new shared allocation must together fit `max_batch_bytes`. The
    /// first entry keeps the single-proposal ceiling (command length only).
    /// A refused push drops `bytes` and leaves the earlier entries usable.
    pub fn push(&mut self, operation: OperationId, bytes: Vec<u8>) -> Result<(), RaftError> {
        let len = self.entries.len();
        let length = bytes.len();
        if len >= self.max_entries {
            return Err(StorageError::Rejected("proposal count budget").into());
        }
        if length > self.max_command_bytes {
            return Err(StorageError::Rejected("command byte budget").into());
        }
        let shared = CommandBytes::retained_bytes_for_len(length).ok_or_else(exhausted)?;
        // Everything alive at once while this original converts, besides the
        // entry allocation: the Vec header, staged shared bytes and the input.
        let held = self
            .shared
            .checked_add(HEADER)
            .and_then(|n| n.checked_add(bytes.capacity()))
            .ok_or_else(exhausted)?;
        let capacity = self.entries.capacity();
        let (grown, peak) = if len < capacity {
            let peak = entry_bytes(capacity)
                .and_then(|n| n.checked_add(held))
                .and_then(|n| n.checked_add(shared))
                .ok_or_else(exhausted)?;
            (None, peak)
        } else {
            let next = self.next_capacity(held, shared)?;
            let growing = entry_bytes(capacity + next).and_then(|n| n.checked_add(held));
            let converting = entry_bytes(next)
                .and_then(|n| n.checked_add(held))
                .and_then(|n| n.checked_add(shared));
            let peak = growing
                .zip(converting)
                .map(|(a, b)| a.max(b))
                .ok_or_else(exhausted)?;
            (Some(next), peak)
        };
        let peak = peak.max(self.peak);
        if len > 0 && peak > self.max_batch_bytes {
            return Err(StorageError::Rejected("proposal shared conversion peak budget").into());
        }
        if let Some(next) = grown {
            self.entries.reserve_exact(next - len);
            if self.entries.capacity() != next {
                // The peak was checked for exactly `next`. `reserve_exact` may
                // allocate more; restore the old size so the batch stays usable.
                self.entries.shrink_to(capacity);
                return Err(StorageError::Rejected("proposal allocation count budget").into());
            }
        }
        self.entries.push(LogEntry {
            index: 0,
            term: 0,
            payload: EntryPayload::Command {
                operation,
                bytes: bytes.into(),
            },
        });
        self.shared += shared;
        self.largest = self.largest.max(length);
        self.peak = peak;
        Ok(())
    }

    /// Number of staged originals.
    pub fn len(&self) -> usize {
        self.entries.len()
    }

    /// Whether no original has been staged; an empty batch is refused.
    pub fn is_empty(&self) -> bool {
        self.entries.is_empty()
    }

    /// Staged originals in order, read-only.
    pub fn iter(&self) -> impl Iterator<Item = (OperationId, &[u8])> + '_ {
        self.entries
            .iter()
            .filter_map(|entry| match &entry.payload {
                EntryPayload::Command { operation, bytes } => Some((*operation, bytes.as_slice())),
                _ => None,
            })
    }

    /// Owned input charge to pass to `Raft::effect_reservation`: the entry Vec
    /// header, its full allocated capacity and every shared command allocation.
    pub fn retained_bytes(&self) -> usize {
        HEADER + self.entries.capacity() * ENTRY + self.shared
    }

    /// Entry allocation and its capacity, for allocation-survival tests.
    #[cfg(test)]
    pub(crate) fn allocation(&self) -> (*const LogEntry, usize) {
        (self.entries.as_ptr(), self.entries.capacity())
    }

    /// Capped doubling, clamped so a multi-entry batch's growth (old and new
    /// buffers together) and the following conversion fit the byte budget.
    fn next_capacity(&self, held: usize, shared: usize) -> Result<usize, RaftError> {
        let len = self.entries.len();
        let capacity = self.entries.capacity();
        let doubled = capacity.saturating_mul(2).max(1).min(self.max_entries);
        if len == 0 {
            return Ok(doubled);
        }
        let fixed = entry_bytes(capacity)
            .map(|n| n.max(shared))
            .and_then(|n| n.checked_add(held))
            .ok_or_else(exhausted)?;
        let fits = self.max_batch_bytes.saturating_sub(fixed) / ENTRY;
        let next = doubled.min(fits);
        if next <= len {
            return Err(StorageError::Rejected("proposal shared conversion peak budget").into());
        }
        Ok(next)
    }
}

/// Built only when a check fails, so a successful push creates and drops no
/// error value.
#[cold]
fn exhausted() -> RaftError {
    RaftError::Exhausted
}

fn entry_bytes(count: usize) -> Option<usize> {
    count.checked_mul(ENTRY)
}

impl Raft {
    /// Start a bounded batch of original commands for `propose_entries`.
    /// `capacity` entries are allocated up front; `push` grows the allocation
    /// within the same bounds. It must not exceed `max_proposal_entries`.
    pub fn proposal_batch(&self, capacity: usize) -> Result<ProposalBatch, RaftError> {
        let max_entries = self.batch_limits.max_proposal_entries;
        if capacity > max_entries {
            return Err(StorageError::Rejected("proposal allocation count budget").into());
        }
        let entries = Vec::with_capacity(capacity);
        if entries.capacity() > max_entries {
            return Err(StorageError::Rejected("proposal allocation count budget").into());
        }
        Ok(ProposalBatch {
            entries,
            max_entries,
            max_command_bytes: self.limits.max_command_bytes,
            max_batch_bytes: self.limits.max_batch_bytes,
            shared: 0,
            largest: 0,
            peak: 0,
        })
    }

    /// Admit a staged batch of original commands in order under one exact
    /// persistence ticket, like `propose_batch`, without converting them again.
    ///
    /// The batch is checked against this core's own limits, whichever core
    /// created it: count and allocated capacity within `max_proposal_entries`
    /// and the largest command within `max_command_bytes`; the retained-entry
    /// data budget (`RaftError::StoragePressure` at it, counted as for
    /// `propose_batch`); for two or more entries also the recorded staging
    /// peak within `max_batch_bytes`. A single entry keeps the
    /// `Event::Propose` ceiling. Each entry then receives this core's next index and current
    /// term in place, and the same allocation becomes the persisted suffix.
    /// Reserve effects with `effect_reservation(batch.retained_bytes())` and
    /// separately reserve individual application receipts, as for
    /// `propose_batch`. A refused batch leaves no pending log state.
    /// Success returns a persistence request, not commitment or application.
    pub fn propose_entries(&mut self, batch: ProposalBatch) -> Result<Vec<Effect>, RaftError> {
        let mut effects = VecDeque::new();
        self.propose_entries_into(batch, &mut effects)?;
        Ok(effects.into())
    }

    /// [`Self::propose_entries`] into a host-owned queue: the effects are
    /// appended, in the same order, after any effects `out` already holds,
    /// which are never read or changed. A refused batch leaves `out` exactly
    /// as it was and no pending log state.
    pub fn propose_entries_into(
        &mut self,
        batch: ProposalBatch,
        out: &mut VecDeque<Effect>,
    ) -> Result<(), RaftError> {
        let mark = out.len();
        let result = self.stage_entry_batch(batch, out);
        if result.is_err() {
            out.truncate(mark);
        }
        result
    }

    fn stage_entry_batch(
        &mut self,
        batch: ProposalBatch,
        out: &mut VecDeque<Effect>,
    ) -> Result<(), RaftError> {
        self.begin_step()?;
        self.check_proposal_role()?;
        let first = self.validate_entry_batch(&batch)?;
        let term = self.durable.hard_state.term;
        let mut entries = batch.entries;
        // `validate_entry_batch` checked `first + (len - 1)`, so no offset
        // here overflows, and no index past the last entry is computed.
        for (offset, entry) in entries.iter_mut().enumerate() {
            entry.index = first + offset as u64;
            entry.term = term;
        }
        // No bound computed here: a volatile core stages no WAL bytes, and a
        // durable one bounds the update once at staging (ST2).
        self.persist_proposal_entries_into(first, entries, None, out)?;
        self.finish_step_into(out)
    }

    fn validate_entry_batch(&mut self, batch: &ProposalBatch) -> Result<u64, RaftError> {
        let count = batch.entries.len();
        let limit = self.batch_limits.max_proposal_entries;
        if count == 0 || count > limit {
            return Err(StorageError::Rejected("proposal count budget").into());
        }
        if batch.entries.capacity() > limit {
            return Err(StorageError::Rejected("proposal allocation count budget").into());
        }
        // ST2: every batch, like `propose` and `propose_batch`, stops at the
        // data budget; the noop reserve above it is the leader's (I1).
        self.check_data_budget(count)?;
        let first = self
            .durable
            .last_index()
            .checked_add(1)
            .ok_or(RaftError::Exhausted)?;
        first
            .checked_add(count as u64 - 1)
            .ok_or(RaftError::Exhausted)?;
        if batch.largest > self.limits.max_command_bytes {
            return Err(StorageError::Rejected("command byte budget").into());
        }
        if count > 1 && batch.peak > self.limits.max_batch_bytes {
            return Err(StorageError::Rejected("proposal shared conversion peak budget").into());
        }
        Ok(first)
    }
}
