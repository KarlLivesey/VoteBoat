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
//! Bounded, single-segment term/vote WAL. No replication log or snapshots yet.
use crate::{contracts::*, identity::*};
use std::{
    collections::BTreeMap,
    fs::{self, File, OpenOptions},
    io::{self, Read, Seek, SeekFrom, Write},
    path::{Path, PathBuf},
};

const HEADER: usize = 32;
const RECORD: usize = 48;
const TRAILER: usize = 16;
const MANIFEST: usize = 52;
const MAGIC: &[u8; 8] = b"VBVOTE01";
const END: &[u8; 8] = b"VBEND001";

/// Lower-level native storage seam. Implementations own an exclusive binding.
/// `append` must finish every byte or fail. `publish_manifest` must write and
/// sync a new file, atomically replace the old name, then sync its directory.
/// On any failure callers assume uncertain progress and fence the store.
/// Bounded reads must reject oversize input before allocating it.
pub trait VoteIo {
    fn read_manifest(&mut self) -> io::Result<Vec<u8>>;
    fn read_log(&mut self, limit: usize) -> io::Result<Vec<u8>>;
    fn append(&mut self, bytes: &[u8]) -> io::Result<()>;
    fn sync_log(&mut self) -> io::Result<()>;
    fn truncate_log(&mut self, length: u64) -> io::Result<()>;
    fn publish_manifest(&mut self, bytes: &[u8]) -> io::Result<()>;
}

/// Native files retain an exclusive OS lock for their entire lifetime.
pub struct FileVoteIo {
    directory: PathBuf,
    log: File,
    _lock: File,
}

impl FileVoteIo {
    /// Explicit new-store creation. Never turns an existing/missing old store
    /// into a fresh voter. An interrupted creation requires operator recovery.
    pub fn create(directory: impl AsRef<Path>) -> io::Result<Self> {
        Self::create_named(directory, "votes.wal")
    }

    pub(super) fn create_named(directory: impl AsRef<Path>, name: &str) -> io::Result<Self> {
        let directory = directory.as_ref().to_owned();
        match fs::create_dir(&directory) {
            Ok(()) => (),
            Err(e) if e.kind() == io::ErrorKind::AlreadyExists => (),
            Err(e) => return Err(e),
        }
        let lock = Self::lock(&directory)?;
        if directory.join("MANIFEST").exists()
            || directory.join("votes.wal").exists()
            || directory.join("log.wal").exists()
            || directory.join("CURRENT").exists()
            || directory.join("log.0.wal").exists()
            || directory.join("log.1.wal").exists()
            || directory.join("MANIFEST.0").exists()
            || directory.join("MANIFEST.1").exists()
        {
            return Err(io::Error::new(
                io::ErrorKind::AlreadyExists,
                "store already exists",
            ));
        }
        let log = OpenOptions::new()
            .read(true)
            .write(true)
            .create_new(true)
            .open(directory.join(name))?;
        // Persist the directory name in its parent, as well as the WAL name.
        File::open(&directory)?.sync_all()?;
        if let Some(parent) = directory.parent().filter(|p| !p.as_os_str().is_empty()) {
            File::open(parent)?.sync_all()?;
        }
        Ok(Self {
            directory,
            log,
            _lock: lock,
        })
    }

    pub fn open(directory: impl AsRef<Path>) -> io::Result<Self> {
        Self::open_named(directory, "votes.wal")
    }

    pub(super) fn open_named(directory: impl AsRef<Path>, name: &str) -> io::Result<Self> {
        let directory = directory.as_ref().to_owned();
        let lock = Self::lock(&directory)?;
        // Both names must exist; opening an old voter never creates its WAL.
        File::open(directory.join("MANIFEST"))?;
        let log = OpenOptions::new()
            .read(true)
            .write(true)
            .open(directory.join(name))?;
        Ok(Self {
            directory,
            log,
            _lock: lock,
        })
    }

    fn lock(directory: &Path) -> io::Result<File> {
        let file = OpenOptions::new()
            .read(true)
            .write(true)
            .create(true)
            .truncate(false)
            .open(directory.join("LOCK"))?;
        file.try_lock().map_err(io::Error::other)?;
        Ok(file)
    }
}

impl VoteIo for FileVoteIo {
    fn read_manifest(&mut self) -> io::Result<Vec<u8>> {
        let mut bytes = Vec::new();
        File::open(self.directory.join("MANIFEST"))?
            .take((MANIFEST + 1) as u64)
            .read_to_end(&mut bytes)?;
        Ok(bytes)
    }
    fn read_log(&mut self, limit: usize) -> io::Result<Vec<u8>> {
        if self.log.metadata()?.len() > limit as u64 {
            return Err(io::Error::new(
                io::ErrorKind::InvalidData,
                "WAL exceeds capacity",
            ));
        }
        self.log.seek(SeekFrom::Start(0))?;
        let mut bytes = Vec::new();
        (&mut self.log)
            .take(limit as u64 + 1)
            .read_to_end(&mut bytes)?;
        if bytes.len() > limit {
            return Err(io::Error::other("WAL grew during recovery"));
        }
        Ok(bytes)
    }
    fn append(&mut self, bytes: &[u8]) -> io::Result<()> {
        self.log.seek(SeekFrom::End(0))?;
        self.log.write_all(bytes)
    }
    fn sync_log(&mut self) -> io::Result<()> {
        self.log.sync_all()
    }
    fn truncate_log(&mut self, length: u64) -> io::Result<()> {
        self.log.set_len(length)
    }
    fn publish_manifest(&mut self, bytes: &[u8]) -> io::Result<()> {
        let staging = self.directory.join("MANIFEST.tmp");
        let mut file = OpenOptions::new()
            .write(true)
            .create(true)
            .truncate(true)
            .open(&staging)?;
        file.write_all(bytes)?;
        file.sync_all()?;
        fs::rename(staging, self.directory.join("MANIFEST"))?;
        File::open(&self.directory)?.sync_all()
    }
}

#[derive(Clone, Copy, Debug)]
pub struct VoteStoreLimits {
    pub max_batch_records: usize,
    pub max_pending_batches: usize,
    pub max_groups: usize,
    pub max_wal_bytes: usize,
}
impl Default for VoteStoreLimits {
    fn default() -> Self {
        Self {
            max_batch_records: 256,
            max_pending_batches: 64,
            max_groups: 4096,
            max_wal_bytes: 16 * 1024 * 1024,
        }
    }
}
impl VoteStoreLimits {
    fn validate(self) -> Result<Self, StorageError> {
        if self.max_batch_records == 0
            || self.max_batch_records > 4096
            || self.max_pending_batches == 0
            || self.max_groups == 0
            || self.max_wal_bytes < HEADER + RECORD + TRAILER
            || self.max_wal_bytes > u32::MAX as usize
        {
            return Err(StorageError::Rejected("invalid storage limits"));
        }
        Ok(self)
    }
}

/// Physical framing seam. Providers advertising format 1 must preserve the
/// fixed frame/header layout and its checksums, not merely the Rust API.
/// A future different format requires a manifest version/migration change.
pub trait VoteLogCodec {
    fn format_version(&self) -> u32;
    fn encode(&self, sequence: u64, records: &[VoteRecord]) -> Vec<u8>;
    fn frame_length(&self, bytes: &[u8], limits: VoteStoreLimits) -> Result<usize, StorageError>;
    fn decode(
        &self,
        bytes: &[u8],
        limits: VoteStoreLimits,
    ) -> Result<(u64, Vec<VoteRecord>), StorageError>;
}
#[derive(Clone, Copy, Debug, Default)]
pub struct NativeVoteCodec;
impl VoteLogCodec for NativeVoteCodec {
    fn format_version(&self) -> u32 {
        1
    }
    fn encode(&self, sequence: u64, records: &[VoteRecord]) -> Vec<u8> {
        encode_frame(sequence, records)
    }
    fn frame_length(&self, bytes: &[u8], limits: VoteStoreLimits) -> Result<usize, StorageError> {
        checked_header(bytes, limits)
    }
    fn decode(
        &self,
        bytes: &[u8],
        limits: VoteStoreLimits,
    ) -> Result<(u64, Vec<VoteRecord>), StorageError> {
        decode_frame(bytes, limits)
    }
}

pub struct NativeVoteStore<I: VoteIo, C: VoteLogCodec = NativeVoteCodec> {
    io: I,
    codec: C,
    binding: StoreBinding,
    limits: VoteStoreLimits,
    recovered: BTreeMap<GroupIdentity, VoteRecord>,
    accepted: BTreeMap<GroupIdentity, VoteRecord>,
    sequence: u64,
    /// Contiguous byte prefix from complete frames, never a maximum index.
    length: usize,
    pending: Vec<WriteTicket>,
    fenced: bool,
}

impl<I: VoteIo> NativeVoteStore<I> {
    pub fn create(
        io: I,
        identity: StoreIdentity,
        limits: VoteStoreLimits,
    ) -> Result<Self, StorageError> {
        Self::create_with_codec(io, identity, limits, NativeVoteCodec)
    }
    pub fn recover(
        io: I,
        identity: StoreIdentity,
        limits: VoteStoreLimits,
    ) -> Result<Self, StorageError> {
        Self::recover_with_codec(io, identity, limits, NativeVoteCodec)
    }
}

impl<I: VoteIo, C: VoteLogCodec> NativeVoteStore<I, C> {
    pub fn create_with_codec(
        mut io: I,
        identity: StoreIdentity,
        limits: VoteStoreLimits,
        codec: C,
    ) -> Result<Self, StorageError> {
        let limits = limits.validate()?;
        if codec.format_version() != 1 {
            return Err(StorageError::Rejected("incompatible vote codec"));
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
            return Err(StorageError::Rejected("creation needs an empty WAL"));
        }
        // FileVoteIo::create excludes any old manifest. Host I/O providers must
        // likewise reserve a new store before calling this constructor.
        let binding = StoreBinding {
            identity,
            session: StoreSession::new(1).unwrap(),
        };
        io.sync_log().map_err(uncertain)?;
        io.publish_manifest(&encode_manifest(binding, 0))
            .map_err(uncertain)?;
        Ok(Self {
            io,
            codec,
            binding,
            limits,
            recovered: BTreeMap::new(),
            accepted: BTreeMap::new(),
            sequence: 0,
            length: 0,
            pending: Vec::new(),
            fenced: false,
        })
    }

    pub fn recover_with_codec(
        mut io: I,
        identity: StoreIdentity,
        limits: VoteStoreLimits,
        codec: C,
    ) -> Result<Self, StorageError> {
        let limits = limits.validate()?;
        if codec.format_version() != 1 {
            return Err(StorageError::Rejected("incompatible vote codec"));
        }
        let manifest = io.read_manifest().map_err(uncertain)?;
        let (old_binding, durable_bytes) = decode_manifest(&manifest)?;
        if old_binding.identity != identity {
            return Err(StorageError::WrongIdentity);
        }
        let bytes = io.read_log(limits.max_wal_bytes).map_err(uncertain)?;
        if durable_bytes > bytes.len() as u64 {
            return Err(StorageError::Corrupt("acknowledged WAL prefix missing"));
        }
        let mut offset = 0;
        let mut sequence = 0;
        let mut state = BTreeMap::new();
        while offset < bytes.len() {
            let remaining = &bytes[offset..];
            if remaining.len() < HEADER {
                if offset < durable_bytes as usize {
                    return Err(StorageError::Corrupt("durable header torn"));
                }
                break;
            }
            let frame_len = codec.frame_length(remaining, limits)?;
            if frame_len < HEADER + TRAILER
                || frame_len > HEADER + RECORD * limits.max_batch_records + TRAILER
            {
                return Err(StorageError::Corrupt("codec frame length outside budget"));
            }
            if remaining.len() < frame_len {
                if offset < durable_bytes as usize {
                    return Err(StorageError::Corrupt("durable batch torn"));
                }
                break;
            }
            let (seq, records) = codec.decode(&remaining[..frame_len], limits)?;
            if records.is_empty() || records.len() > limits.max_batch_records {
                return Err(StorageError::Corrupt("codec record count outside budget"));
            }
            if seq != sequence + 1 {
                return Err(StorageError::Corrupt("batch sequence discontinuity"));
            }
            apply_records(&mut state, &records, limits)
                .map_err(|_| StorageError::Corrupt("invalid recovered ballot transition"))?;
            offset += frame_len;
            // A durable marker is always an exact complete-frame boundary.
            if offset as u64 > durable_bytes && ((offset - frame_len) as u64) < durable_bytes {
                return Err(StorageError::Corrupt("durable marker inside batch"));
            }
            sequence = seq;
        }
        let session = old_binding
            .session
            .get()
            .checked_add(1)
            .and_then(StoreSession::new)
            .ok_or(StorageError::Corrupt("session exhausted"))?;
        let binding = StoreBinding { identity, session };
        io.truncate_log(offset as u64).map_err(uncertain)?;
        io.sync_log().map_err(uncertain)?;
        // Recovered complete but unacknowledged batches are now made durable.
        // Publish session before issuing any new tickets: old completions fail.
        io.publish_manifest(&encode_manifest(binding, offset as u64))
            .map_err(uncertain)?;
        Ok(Self {
            io,
            codec,
            binding,
            limits,
            recovered: state.clone(),
            accepted: state,
            sequence,
            length: offset,
            pending: Vec::new(),
            fenced: false,
        })
    }

    fn failure(&mut self, error: io::Error) -> StorageError {
        self.fenced = true;
        uncertain(error)
    }
}

impl<I: VoteIo, C: VoteLogCodec> VoteStore for NativeVoteStore<I, C> {
    fn binding(&self) -> StoreBinding {
        self.binding
    }
    fn recovered(&self) -> &BTreeMap<GroupIdentity, VoteRecord> {
        &self.recovered
    }
    fn append_votes(&mut self, records: Vec<VoteRecord>) -> Result<WriteTicket, StorageError> {
        if self.fenced {
            return Err(StorageError::Fenced);
        }
        if records.is_empty() || records.len() > self.limits.max_batch_records {
            return Err(StorageError::Rejected("batch record limit"));
        }
        if self.pending.len() >= self.limits.max_pending_batches {
            return Err(StorageError::Rejected(
                "pending batch limit; drive barriers",
            ));
        }
        let sequence = self
            .sequence
            .checked_add(1)
            .ok_or(StorageError::Rejected("sequence exhausted"))?;
        let bytes = self.codec.encode(sequence, &records);
        if bytes.len() != HEADER + RECORD * records.len() + TRAILER {
            return Err(StorageError::Rejected(
                "codec encoded length outside format contract",
            ));
        }
        if bytes.len() > self.limits.max_wal_bytes.saturating_sub(self.length) {
            return Err(StorageError::Rejected(
                "vote WAL capacity; no automatic reclamation",
            ));
        }
        let mut next = self.accepted.clone();
        apply_records(&mut next, &records, self.limits)?;
        if let Err(error) = self.io.append(&bytes) {
            return Err(self.failure(error));
        }
        self.accepted = next;
        self.sequence = sequence;
        self.length += bytes.len();
        let ticket = WriteTicket {
            binding: self.binding,
            sequence,
        };
        self.pending.push(ticket);
        Ok(ticket)
    }
    fn barrier(&mut self, tickets: &[WriteTicket]) -> Result<DurableVotes, StorageError> {
        if self.fenced {
            return Err(StorageError::Fenced);
        }
        if tickets.is_empty() || tickets.len() > self.limits.max_pending_batches {
            return Err(StorageError::Rejected("barrier ticket limit"));
        }
        // Completed tickets are not retained indefinitely: duplicate callbacks
        // are harmless at the core; a second barrier is explicitly rejected.
        if tickets
            .iter()
            .any(|t| t.binding != self.binding || !self.pending.contains(t))
        {
            return Err(StorageError::StaleTicket);
        }
        if let Err(error) = self.io.sync_log() {
            return Err(self.failure(error));
        }
        if let Err(error) = self
            .io
            .publish_manifest(&encode_manifest(self.binding, self.length as u64))
        {
            return Err(self.failure(error));
        }
        self.pending.retain(|t| !tickets.contains(t));
        Ok(DurableVotes {
            tickets: tickets.to_vec(),
        })
    }
}

fn uncertain(e: io::Error) -> StorageError {
    StorageError::Uncertain(e.to_string())
}

fn apply_records(
    state: &mut BTreeMap<GroupIdentity, VoteRecord>,
    records: &[VoteRecord],
    limits: VoteStoreLimits,
) -> Result<(), StorageError> {
    for record in records {
        if let Some(previous) = state.get(&record.group) {
            if previous.configuration != record.configuration
                || !record.hard_state.follows(previous.hard_state)
            {
                return Err(StorageError::Rejected(
                    "ballot regression or unimplemented reconfiguration",
                ));
            }
        } else {
            if state.len() >= limits.max_groups {
                return Err(StorageError::Rejected("group limit"));
            }
            if !record.hard_state.follows(HardState::default()) {
                return Err(StorageError::Rejected("invalid initial ballot"));
            }
        }
        state.insert(record.group, *record);
    }
    Ok(())
}

/// Standard reflected CRC32C (Castagnoli), for accidental corruption only.
pub(crate) fn crc32c(bytes: &[u8]) -> u32 {
    crc32c_extend(0, bytes)
}
pub(crate) fn crc32c_extend(previous: u32, bytes: &[u8]) -> u32 {
    let mut crc = !previous;
    for byte in bytes {
        crc ^= *byte as u32;
        for _ in 0..8 {
            crc = (crc >> 1) ^ (0x82f63b78 & 0u32.wrapping_sub(crc & 1));
        }
    }
    !crc
}
fn u64_at(bytes: &[u8], offset: usize) -> u64 {
    u64::from_le_bytes(bytes[offset..offset + 8].try_into().unwrap())
}
fn u32_at(bytes: &[u8], offset: usize) -> u32 {
    u32::from_le_bytes(bytes[offset..offset + 4].try_into().unwrap())
}
fn u128_at(bytes: &[u8], offset: usize) -> u128 {
    u128::from_le_bytes(bytes[offset..offset + 16].try_into().unwrap())
}
fn encode_manifest(binding: StoreBinding, durable_bytes: u64) -> Vec<u8> {
    let mut bytes = b"VBSTORE1".to_vec();
    bytes.extend(binding.identity.id.get().to_le_bytes());
    bytes.extend(binding.identity.incarnation.get().to_le_bytes());
    bytes.extend(binding.session.get().to_le_bytes());
    bytes.extend(durable_bytes.to_le_bytes());
    bytes.extend(crc32c(&bytes).to_le_bytes());
    bytes
}
fn decode_manifest(bytes: &[u8]) -> Result<(StoreBinding, u64), StorageError> {
    if bytes.len() != MANIFEST
        || &bytes[..8] != b"VBSTORE1"
        || crc32c(&bytes[..48]) != u32_at(bytes, 48)
    {
        return Err(StorageError::Corrupt("invalid manifest"));
    }
    let identity = StoreIdentity {
        id: StoreId::new(u128_at(bytes, 8)).ok_or(StorageError::WrongIdentity)?,
        incarnation: StoreIncarnation::new(u64_at(bytes, 24)).ok_or(StorageError::WrongIdentity)?,
    };
    let session = StoreSession::new(u64_at(bytes, 32)).ok_or(StorageError::WrongIdentity)?;
    Ok((StoreBinding { identity, session }, u64_at(bytes, 40)))
}
fn encode_frame(sequence: u64, records: &[VoteRecord]) -> Vec<u8> {
    let mut bytes = MAGIC.to_vec();
    bytes.extend(sequence.to_le_bytes());
    bytes.extend((records.len() as u32).to_le_bytes());
    bytes.extend((records.len() as u32 * RECORD as u32).to_le_bytes());
    bytes.extend(crc32c(&bytes).to_le_bytes());
    bytes.extend(0u32.to_le_bytes());
    for record in records {
        bytes.extend(record.group.id.get().to_le_bytes());
        bytes.extend(record.group.incarnation.get().to_le_bytes());
        bytes.extend(record.configuration.get().to_le_bytes());
        bytes.extend(record.hard_state.term.to_le_bytes());
        bytes.extend(
            record
                .hard_state
                .voted_for
                .map_or(0, NodeId::get)
                .to_le_bytes(),
        );
    }
    let checksum = crc32c(&bytes);
    bytes.extend(END);
    bytes.extend(checksum.to_le_bytes());
    bytes.extend(0u32.to_le_bytes());
    bytes
}
fn checked_header(bytes: &[u8], limits: VoteStoreLimits) -> Result<usize, StorageError> {
    if bytes.len() < HEADER
        || &bytes[..8] != MAGIC
        || u32_at(bytes, 28) != 0
        || crc32c(&bytes[..24]) != u32_at(bytes, 24)
    {
        return Err(StorageError::Corrupt(
            "invalid batch header/version/checksum",
        ));
    }
    let count = u32_at(bytes, 16) as usize;
    let length = u32_at(bytes, 20) as usize;
    if count == 0 || count > limits.max_batch_records || length != count * RECORD {
        return Err(StorageError::Corrupt("invalid batch length"));
    }
    Ok(HEADER + length + TRAILER)
}
fn decode_frame(
    bytes: &[u8],
    limits: VoteStoreLimits,
) -> Result<(u64, Vec<VoteRecord>), StorageError> {
    if checked_header(bytes, limits)? != bytes.len() {
        return Err(StorageError::Corrupt("incomplete frame"));
    }
    let end = bytes.len() - TRAILER;
    if &bytes[end..end + 8] != END
        || u32_at(bytes, end + 12) != 0
        || crc32c(&bytes[..end]) != u32_at(bytes, end + 8)
    {
        return Err(StorageError::Corrupt("invalid batch trailer/checksum"));
    }
    let mut records = Vec::new();
    for chunk in bytes[HEADER..end].as_chunks::<RECORD>().0 {
        let group = GroupIdentity {
            id: GroupId::new(u128_at(chunk, 0)).ok_or(StorageError::Corrupt("zero group"))?,
            incarnation: GroupIncarnation::new(u64_at(chunk, 16))
                .ok_or(StorageError::Corrupt("zero incarnation"))?,
        };
        let configuration = ConfigurationId::new(u64_at(chunk, 24))
            .ok_or(StorageError::Corrupt("zero configuration"))?;
        records.push(VoteRecord {
            group,
            configuration,
            hard_state: HardState {
                term: u64_at(chunk, 32),
                voted_for: NodeId::new(u64_at(chunk, 40)),
            },
        });
    }
    Ok((u64_at(bytes, 8), records))
}

#[cfg(test)]
mod tests {
    use super::*;
    #[test]
    fn crc_castagnoli_check_vector() {
        assert_eq!(crc32c(b"123456789"), 0xe3069283);
        assert_eq!(crc32c_extend(crc32c(b"1234"), b"56789"), 0xe3069283);
    }
}
