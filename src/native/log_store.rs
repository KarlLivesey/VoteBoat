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
//! Native recoverable log: atomic group transitions, shared barriers, no threads.
use super::vote_store::crc32c;
use crate::log::CheckpointComparison;
use crate::{contracts::StorageError, identity::*, log::*};
use std::{collections::BTreeMap, io};

mod accepted;
mod checkpoint;
mod file;
pub mod striped;
mod timing;
mod witness;
pub use file::FileLogIo;
pub use timing::{
    JournalCallTiming, JournalTimingSnapshot, JournalTimings, ManifestPublicationTiming,
};
pub use witness::WitnessLogIo;

use crate::log_format::{Decoder, HEADER, TRAILER};
pub use crate::log_format::{LogCodec, NativeLogCodec};

/// Exclusive native journal binding. Complete writes, bounded reads and durable
/// atomic manifest publication are mandatory. Replacement is optional and must
/// recover either the entire old journal/manifest or the entire new pair. Sync
/// replacement data and metadata and their selection before deleting old data.
pub trait JournalIo {
    fn read_manifest(&mut self) -> io::Result<Vec<u8>>;
    fn read_log(&mut self, limit: usize) -> io::Result<Vec<u8>>;
    fn append(&mut self, bytes: &[u8]) -> io::Result<()>;
    fn sync_log(&mut self) -> io::Result<()>;
    fn truncate_log(&mut self, length: u64) -> io::Result<()>;
    fn publish_manifest(&mut self, bytes: &[u8]) -> io::Result<()>;
    /// Synchronize accepted WAL bytes and their acknowledged recovery boundary.
    /// Success covers both dependencies; failure must not release durability.
    /// Providers may combine their physical work while preserving that order.
    fn publish_barrier(&mut self, manifest: &[u8]) -> io::Result<()> {
        self.sync_log()?;
        self.publish_manifest(manifest)
    }
    fn supports_replacement(&self) -> bool {
        false
    }
    fn replace_log(&mut self, _bytes: &[u8], _manifest: &[u8]) -> io::Result<()> {
        Err(io::Error::new(
            io::ErrorKind::Unsupported,
            "journal replacement unsupported",
        ))
    }
}

pub struct NativeLogStore<I: JournalIo, C: LogCodec = NativeLogCodec> {
    io: I,
    codec: C,
    binding: StoreBinding,
    limits: LogLimits,
    durable: BTreeMap<GroupIdentity, GroupLog>,
    checkpoint_comparisons: BTreeMap<GroupIdentity, CheckpointComparison>,
    accepted: accepted::Accepted,
    pending: Vec<LogTicket>,
    length: usize,
    sequence: u64,
    fenced: bool,
}

impl<I: JournalIo> NativeLogStore<I> {
    pub fn create(io: I, identity: StoreIdentity, limits: LogLimits) -> Result<Self, StorageError> {
        Self::create_with_codec(io, identity, limits, NativeLogCodec)
    }
    pub fn recover(
        io: I,
        identity: StoreIdentity,
        limits: LogLimits,
    ) -> Result<Self, StorageError> {
        Self::recover_with_codec(io, identity, limits, NativeLogCodec)
    }
}
impl<I: JournalIo, C: LogCodec> NativeLogStore<I, C> {
    #[cfg(feature = "tls")]
    pub(crate) fn matches_startup_groups(
        &self,
        groups: &[GroupIdentity],
    ) -> Result<bool, StorageError> {
        if self.fenced {
            return Err(StorageError::Fenced);
        }
        if !self.pending.is_empty() {
            return Err(StorageError::Rejected("startup requires quiescent WAL"));
        }
        Ok(self.durable.keys().copied().eq(groups.iter().copied()))
    }
    pub(crate) fn creation_state_native(
        &self,
        group: GroupIdentity,
    ) -> Result<Option<GroupLog>, StorageError> {
        if self.fenced {
            return Err(StorageError::Fenced);
        }
        if !self.pending.is_empty() {
            return Err(StorageError::Rejected("creation requires quiescent WAL"));
        }
        Ok(self.durable.get(&group).cloned())
    }

    pub fn create_with_codec(
        mut io: I,
        identity: StoreIdentity,
        limits: LogLimits,
        codec: C,
    ) -> Result<Self, StorageError> {
        let limits = limits.validate()?;
        if codec.format_version() != 2 {
            return Err(StorageError::Rejected("incompatible log codec"));
        }
        match io.read_manifest() {
            Err(e) if e.kind() == io::ErrorKind::NotFound => (),
            Ok(_) => return Err(StorageError::Rejected("store already initialized")),
            Err(e) => return Err(uncertain(e)),
        }
        if !io
            .read_log(limits.max_wal_bytes)
            .map_err(uncertain)?
            .is_empty()
        {
            return Err(StorageError::Rejected("new store requires empty WAL"));
        }
        let binding = StoreBinding {
            identity,
            session: StoreSession::new(1).unwrap(),
        };
        io.publish_barrier(&manifest(binding, 0))
            .map_err(uncertain)?;
        Ok(Self {
            io,
            codec,
            binding,
            limits,
            durable: BTreeMap::new(),
            checkpoint_comparisons: BTreeMap::new(),
            accepted: accepted::Accepted::default(),
            pending: Vec::new(),
            length: 0,
            sequence: 0,
            fenced: false,
        })
    }

    pub fn recover_with_codec(
        mut io: I,
        identity: StoreIdentity,
        limits: LogLimits,
        codec: C,
    ) -> Result<Self, StorageError> {
        let limits = limits.validate()?;
        if codec.format_version() != 2 {
            return Err(StorageError::Rejected("incompatible log codec"));
        }
        let (old, boundary) = read_manifest(&io.read_manifest().map_err(uncertain)?)?;
        if old.identity != identity {
            return Err(StorageError::WrongIdentity);
        }
        let bytes = io.read_log(limits.max_wal_bytes).map_err(uncertain)?;
        if boundary > bytes.len() as u64 {
            return Err(StorageError::Corrupt("durable log prefix missing"));
        }
        let mut offset = 0usize;
        let mut sequence = 0u64;
        let mut state = BTreeMap::new();
        if codec.is_checkpoint(&bytes[..bytes.len().min(HEADER)]) {
            (sequence, state, offset) = codec.decode_checkpoint(&bytes, limits)?;
            if offset < HEADER + TRAILER || offset > bytes.len() || offset > boundary as usize {
                return Err(StorageError::Corrupt("checkpoint outside durable prefix"));
            }
            checkpoint::validate_state(sequence, &state, limits)?;
            if state
                .values()
                .any(|s| s.snapshot.is_some_and(|r| r.store != identity))
            {
                return Err(StorageError::Corrupt("foreign checkpoint snapshot"));
            }
        }
        while offset < bytes.len() {
            let remaining = &bytes[offset..];
            if remaining.len() < HEADER {
                if offset < boundary as usize {
                    return Err(StorageError::Corrupt("durable header torn"));
                }
                break;
            }
            let size = codec.frame_length(remaining, limits)?;
            if size < HEADER + TRAILER || size > limits.max_batch_bytes {
                return Err(StorageError::Corrupt("frame length outside budget"));
            }
            if remaining.len() < size {
                if offset < boundary as usize {
                    return Err(StorageError::Corrupt("durable batch torn"));
                }
                break;
            }
            let (seq, mutations) = codec.decode_batch(&remaining[..size], limits)?;
            if mutations.iter().any(|m| matches!(m, LogMutation::Update(u) if u.snapshot.is_some_and(|r| r.store != identity))) {
                return Err(StorageError::Corrupt("foreign snapshot store identity"));
            }
            if sequence.checked_add(1) != Some(seq) {
                return Err(StorageError::Corrupt("batch sequence discontinuity"));
            }
            apply_batch(&mut state, &mutations, limits)
                .map_err(|_| StorageError::Corrupt("invalid log transition"))?;
            if (offset as u64) < boundary && (offset + size) as u64 > boundary {
                return Err(StorageError::Corrupt("durable marker inside frame"));
            }
            offset += size;
            sequence = seq;
        }
        let session = old
            .session
            .get()
            .checked_add(1)
            .and_then(StoreSession::new)
            .ok_or(StorageError::Corrupt("session exhausted"))?;
        let binding = StoreBinding { identity, session };
        io.truncate_log(offset as u64).map_err(uncertain)?;
        io.publish_barrier(&manifest(binding, offset as u64))
            .map_err(uncertain)?;
        let accepted = accepted::Accepted::recovered(&state);
        Ok(Self {
            io,
            codec,
            binding,
            limits,
            durable: state,
            checkpoint_comparisons: BTreeMap::new(),
            accepted,
            pending: Vec::new(),
            length: offset,
            sequence,
            fenced: false,
        })
    }

    fn failed(&mut self, error: io::Error) -> StorageError {
        self.fenced = true;
        self.checkpoint_comparisons.clear();
        uncertain(error)
    }
}

impl<I: JournalIo, C: LogCodec> LogStore for NativeLogStore<I, C> {
    fn binding(&self) -> StoreBinding {
        self.binding
    }
    fn limits(&self) -> LogLimits {
        self.limits
    }
    fn state(&self, group: GroupIdentity) -> Result<GroupLog, StorageError> {
        if self.fenced {
            return Err(StorageError::Fenced);
        }
        self.durable
            .get(&group)
            .cloned()
            .ok_or(StorageError::Rejected("group not durably bootstrapped"))
    }
    fn matches_state(
        &self,
        group: GroupIdentity,
        expected: &GroupLog,
    ) -> Result<bool, StorageError> {
        if self.fenced {
            return Err(StorageError::Fenced);
        }
        let state = self
            .durable
            .get(&group)
            .ok_or(StorageError::Rejected("group not durably bootstrapped"))?;
        Ok(state == expected)
    }
    fn matches_checkpoint_state(
        &mut self,
        expected: CheckpointLog<'_>,
    ) -> Result<bool, StorageError> {
        if self.fenced {
            self.checkpoint_comparisons.clear();
            return Err(StorageError::Fenced);
        }
        let group = expected.group();
        let state = self
            .durable
            .get(&group)
            .ok_or(StorageError::Rejected("group not durably bootstrapped"))?;
        Ok(self
            .checkpoint_comparisons
            .entry(group)
            .or_default()
            .compare(state, &expected))
    }
    fn append_batch(
        &mut self,
        mutations: Vec<LogMutation>,
    ) -> Result<Vec<LogTicket>, StorageError> {
        if self.fenced {
            return Err(StorageError::Fenced);
        }
        if mutations.iter().any(|m|matches!(m,LogMutation::Update(u) if u.snapshot.is_some_and(|r|r.store!=self.binding.identity))) {
            return Err(StorageError::WrongIdentity);
        }
        if mutations.len()
            > self
                .limits
                .max_pending_units
                .saturating_sub(self.pending.len())
        {
            return Err(StorageError::Rejected("outstanding transition budget"));
        }
        let sequence = self
            .sequence
            .checked_add(1)
            .ok_or(StorageError::Rejected("batch sequence exhausted"))?;
        let bytes = self.codec.encode_batch(sequence, &mutations, self.limits)?;
        if bytes.len() < HEADER + TRAILER || bytes.len() > self.limits.max_batch_bytes {
            return Err(StorageError::Rejected(
                "codec encoded length outside budget",
            ));
        }
        let groups: Vec<_> = mutations.iter().map(accepted::mutation_group).collect();
        let control = mutations.iter().all(|m| match m {
            LogMutation::Create(_) => true,
            LogMutation::Update(u) => u.suffix.as_ref().is_none_or(|s| {
                s.entries
                    .iter()
                    .all(|e| matches!(e.payload, EntryPayload::Noop))
            }),
        });
        let next = self
            .accepted
            .plan_batch(&self.durable, mutations, self.limits)?;
        let ceiling = if control {
            self.limits.max_wal_bytes
        } else {
            self.limits.max_wal_bytes - self.limits.control_reserve_bytes
        };
        if bytes.len() > ceiling.saturating_sub(self.length) {
            return Err(StorageError::Rejected("WAL capacity/control reserve"));
        }
        let tickets: Vec<_> = groups
            .iter()
            .map(|&g| {
                let state = next[&g].metadata();
                LogTicket {
                    binding: self.binding,
                    batch: sequence,
                    group: g,
                    revision: state.revision,
                    generation: state.generation,
                    last_index: state.last_index,
                    term: state.hard_state.term,
                }
            })
            .collect();
        if let Err(e) = self.io.append(&bytes) {
            return Err(self.failed(e));
        }
        // A suffix fence invalidates outstanding evidence for the old suffix.
        self.pending.retain(|t| {
            next.get(&t.group)
                .is_none_or(|plan| plan.metadata().generation == t.generation)
        });
        if let Err(error) = self.accepted.install(&self.durable, next) {
            self.fenced = true;
            self.checkpoint_comparisons.clear();
            return Err(StorageError::Uncertain(format!(
                "accepted install invariant: {error:?}"
            )));
        }
        self.pending.extend(tickets.iter().copied());
        self.sequence = sequence;
        self.length += bytes.len();
        Ok(tickets)
    }
    fn barrier(&mut self, dependencies: &[LogTicket]) -> Result<DurableLog, StorageError> {
        if self.fenced {
            return Err(StorageError::Fenced);
        }
        if dependencies.is_empty() || dependencies.len() > self.limits.max_pending_units {
            return Err(StorageError::Rejected("barrier dependency budget"));
        }
        if dependencies
            .iter()
            .any(|t| t.binding != self.binding || !self.pending.contains(t))
        {
            return Err(StorageError::StaleTicket);
        }
        self.accepted.check_promotion(&self.durable)?;
        if let Err(e) = self
            .io
            .publish_barrier(&manifest(self.binding, self.length as u64))
        {
            return Err(self.failed(e));
        }
        if let Err(error) = self
            .accepted
            .promote(&mut self.durable, &mut self.checkpoint_comparisons)
        {
            self.fenced = true;
            self.checkpoint_comparisons.clear();
            return Err(StorageError::Uncertain(format!(
                "accepted promotion invariant: {error:?}"
            )));
        }
        self.pending.retain(|t| !dependencies.contains(t));
        Ok(DurableLog {
            tickets: dependencies.to_vec(),
        })
    }
    fn supports_reclaim(&self) -> bool {
        self.io.supports_replacement() && self.codec.supports_checkpoint()
    }
    fn reclaim(&mut self, max_bytes: usize) -> Result<LogReclaimed, StorageError> {
        if self.fenced {
            return Err(StorageError::Fenced);
        }
        if !self.pending.is_empty() || !self.accepted.is_clean() {
            return Err(StorageError::Rejected(
                "reclamation requires drained transitions",
            ));
        }
        if !self.io.supports_replacement() || !self.codec.supports_checkpoint() {
            return Err(StorageError::Rejected("physical reclamation unsupported"));
        }
        if max_bytes < HEADER + TRAILER || max_bytes > self.limits.max_wal_bytes {
            return Err(StorageError::Rejected("reclamation byte budget"));
        }
        let bytes =
            self.codec
                .encode_checkpoint(self.sequence, &self.durable, self.limits, max_bytes)?;
        if bytes.len() > max_bytes || bytes.len() < HEADER + TRAILER {
            return Err(StorageError::Rejected("checkpoint codec byte budget"));
        }
        if !self.codec.is_checkpoint(&bytes[..HEADER]) {
            return Err(StorageError::Rejected("checkpoint codec prefix mismatch"));
        }
        let (sequence, recovered, length) = self.codec.decode_checkpoint(&bytes, self.limits)?;
        if sequence != self.sequence || recovered != self.durable || length != bytes.len() {
            return Err(StorageError::Rejected(
                "checkpoint codec changed live state",
            ));
        }
        let before_bytes = self.length;
        if bytes.len() >= before_bytes {
            return Ok(LogReclaimed {
                before_bytes,
                after_bytes: before_bytes,
            });
        }
        if let Err(e) = self
            .io
            .replace_log(&bytes, &manifest(self.binding, bytes.len() as u64))
        {
            return Err(self.failed(e));
        }
        self.length = bytes.len();
        Ok(LogReclaimed {
            before_bytes,
            after_bytes: self.length,
        })
    }
    fn fetch_range(
        &self,
        group: GroupIdentity,
        generation: LogGeneration,
        from: u64,
        max_entries: usize,
        max_bytes: usize,
    ) -> Result<Vec<LogEntry>, StorageError> {
        if self.fenced {
            return Err(StorageError::Fenced);
        }
        let state = self
            .durable
            .get(&group)
            .ok_or(StorageError::Rejected("unknown group"))?;
        if state.generation != generation {
            return Err(StorageError::StaleTicket);
        }
        if from == 0
            || from > state.last_index() + 1
            || max_entries == 0
            || max_bytes == 0
            || max_entries > self.limits.max_entries_per_group
            || max_bytes > self.limits.max_batch_bytes
        {
            return Err(StorageError::Rejected("range budget or boundary"));
        }
        if from <= state.base_index() {
            return Err(StorageError::Compacted {
                first_index: state.base_index() + 1,
            });
        }
        let mut bytes = 0usize;
        let mut result = Vec::new();
        for entry in state
            .entries
            .iter()
            .skip((from - state.base_index() - 1) as usize)
            .take(max_entries)
        {
            let size = 37 + entry.payload_bytes();
            if size > max_bytes.saturating_sub(bytes) {
                if result.is_empty() {
                    return Err(StorageError::Rejected(
                        "first entry exceeds range byte budget",
                    ));
                }
                break;
            }
            bytes += size;
            result.push(entry.clone());
        }
        Ok(result)
    }
}

fn uncertain(e: io::Error) -> StorageError {
    StorageError::Uncertain(e.to_string())
}
fn manifest(binding: StoreBinding, boundary: u64) -> Vec<u8> {
    let mut b = b"VBLSTR02".to_vec();
    b.extend(binding.identity.id.get().to_le_bytes());
    b.extend(binding.identity.incarnation.get().to_le_bytes());
    b.extend(binding.session.get().to_le_bytes());
    b.extend(boundary.to_le_bytes());
    b.extend(crc32c(&b).to_le_bytes());
    b
}
fn read_manifest(b: &[u8]) -> Result<(StoreBinding, u64), StorageError> {
    if b.len() != 52
        || &b[..8] != b"VBLSTR02"
        || crc32c(&b[..48]) != u32::from_le_bytes(b[48..].try_into().unwrap())
    {
        return Err(StorageError::Corrupt("manifest format/checksum"));
    }
    let mut d = Decoder::new(&b[8..48]);
    let id = StoreId::new(d.u128()?).ok_or(StorageError::WrongIdentity)?;
    let incarnation = StoreIncarnation::new(d.u64()?).ok_or(StorageError::WrongIdentity)?;
    let session = StoreSession::new(d.u64()?).ok_or(StorageError::WrongIdentity)?;
    Ok((
        StoreBinding {
            identity: StoreIdentity { id, incarnation },
            session,
        },
        d.u64()?,
    ))
}
