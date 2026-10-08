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
//! Native crash-atomic local checkpoints. Two alternating data slots per bound
//! group keep publication atomic and disk use bounded. No log deletion is enabled.
use super::{
    log_store::{LogCodec, NativeLogCodec},
    vote_store::{crc32c, crc32c_extend},
};
use crate::{
    contracts::StorageError,
    identity::*,
    log::{LogLimits, LogMutation},
    snapshot::*,
};
use std::{
    collections::BTreeMap,
    fs::{self, File, OpenOptions},
    io::{self, Read, Write},
    path::{Path, PathBuf},
};

/// Exclusive per-replica native snapshot I/O. begin_slot truncates only the
/// inactive slot. append must finish all bytes or fail. sync_slot covers data
/// and its directory entry before any root may reference it;
/// publish_manifest must sync a temporary file, replace the root atomically,
/// then sync its directory. Reads refuse oversize data before allocating.
pub trait SnapshotIo {
    fn read_manifest(&mut self) -> io::Result<Vec<u8>>;
    fn read_slot(&mut self, slot: u8, limit: usize) -> io::Result<Vec<u8>>;
    fn begin_slot(&mut self, slot: u8, prefix: &[u8]) -> io::Result<()>;
    fn append_slot(&mut self, slot: u8, bytes: &[u8]) -> io::Result<()>;
    fn sync_slot(&mut self, slot: u8) -> io::Result<()>;
    fn publish_manifest(&mut self, bytes: &[u8]) -> io::Result<()>;
}

pub struct FileSnapshotIo {
    directory: PathBuf,
    _lock: File,
}
impl FileSnapshotIo {
    fn lock(directory: &Path) -> io::Result<File> {
        let lock = OpenOptions::new()
            .read(true)
            .write(true)
            .create(true)
            .truncate(false)
            .open(directory.join("LOCK"))?;
        lock.try_lock().map_err(io::Error::other)?;
        Ok(lock)
    }
    pub fn create(directory: impl AsRef<Path>) -> io::Result<Self> {
        let directory = directory.as_ref().to_owned();
        match fs::create_dir(&directory) {
            Ok(()) => (),
            Err(e) if e.kind() == io::ErrorKind::AlreadyExists => (),
            Err(e) => return Err(e),
        }
        let lock = Self::lock(&directory)?;
        if ["MANIFEST", "snapshot-0", "snapshot-1"]
            .iter()
            .any(|name| directory.join(name).exists())
        {
            return Err(io::Error::new(
                io::ErrorKind::AlreadyExists,
                "snapshot store already exists",
            ));
        }
        File::open(&directory)?.sync_all()?;
        if let Some(parent) = directory.parent().filter(|p| !p.as_os_str().is_empty()) {
            File::open(parent)?.sync_all()?;
        }
        Ok(Self {
            directory,
            _lock: lock,
        })
    }
    pub fn open(directory: impl AsRef<Path>) -> io::Result<Self> {
        let directory = directory.as_ref().to_owned();
        let lock = Self::lock(&directory)?;
        File::open(directory.join("MANIFEST"))?;
        Ok(Self {
            directory,
            _lock: lock,
        })
    }
    fn slot_path(&self, slot: u8) -> io::Result<PathBuf> {
        match slot {
            0 | 1 => Ok(self.directory.join(format!("snapshot-{slot}"))),
            _ => Err(io::Error::other("invalid snapshot slot")),
        }
    }
    fn read_bounded(path: &Path, limit: usize) -> io::Result<Vec<u8>> {
        let file = File::open(path)?;
        if file.metadata()?.len() > limit as u64 {
            return Err(io::Error::other("oversize snapshot file"));
        }
        let mut bytes = Vec::new();
        file.take((limit as u64).saturating_add(1))
            .read_to_end(&mut bytes)?;
        if bytes.len() > limit {
            return Err(io::Error::other("snapshot grew past budget"));
        }
        Ok(bytes)
    }
}
impl SnapshotIo for FileSnapshotIo {
    fn read_manifest(&mut self) -> io::Result<Vec<u8>> {
        Self::read_bounded(&self.directory.join("MANIFEST"), 133)
    }
    fn read_slot(&mut self, slot: u8, limit: usize) -> io::Result<Vec<u8>> {
        Self::read_bounded(&self.slot_path(slot)?, limit)
    }
    fn begin_slot(&mut self, slot: u8, prefix: &[u8]) -> io::Result<()> {
        OpenOptions::new()
            .write(true)
            .create(true)
            .truncate(true)
            .open(self.slot_path(slot)?)?
            .write_all(prefix)
    }
    fn append_slot(&mut self, slot: u8, bytes: &[u8]) -> io::Result<()> {
        OpenOptions::new()
            .append(true)
            .open(self.slot_path(slot)?)?
            .write_all(bytes)
    }
    fn sync_slot(&mut self, slot: u8) -> io::Result<()> {
        OpenOptions::new()
            .write(true)
            .open(self.slot_path(slot)?)?
            .sync_all()?;
        File::open(&self.directory)?.sync_all()
    }
    fn publish_manifest(&mut self, bytes: &[u8]) -> io::Result<()> {
        let temp = self.directory.join("MANIFEST.next");
        let mut file = OpenOptions::new()
            .write(true)
            .create(true)
            .truncate(true)
            .open(&temp)?;
        file.write_all(bytes)?;
        file.sync_all()?;
        fs::rename(temp, self.directory.join("MANIFEST"))?;
        File::open(&self.directory)?.sync_all()
    }
}

/// Native snapshot encoding is independently selectable from logical storage.
/// format_version must be 1 for this provider. Prefix and finish must preserve
/// metadata and enforce length/format/checksum bounds before decode allocation.
pub trait SnapshotCodec {
    fn format_version(&self) -> u32;
    fn prefix(
        &self,
        metadata: &SnapshotMetadata,
        application_bytes: usize,
        limits: SnapshotLimits,
    ) -> Result<Vec<u8>, StorageError>;
    fn finish(&self, body: &[u8], limits: SnapshotLimits) -> Result<[u8; 4], StorageError>;
    fn decode(&self, file: &[u8], limits: SnapshotLimits) -> Result<Snapshot, StorageError>;
}
#[derive(Clone, Copy, Default)]
pub struct NativeSnapshotCodec;
const HEADER: usize = 44;
fn codec_log_limits(limits: SnapshotLimits) -> LogLimits {
    LogLimits {
        max_batch_bytes: limits.max_metadata_bytes,
        max_command_bytes: 1,
        ..LogLimits::default()
    }
}
impl NativeSnapshotCodec {
    fn parse(&self, body: &[u8], limits: SnapshotLimits) -> Result<Snapshot, StorageError> {
        limits.validate()?;
        if body.len() < HEADER || &body[..8] != b"VBSNAP01" {
            return Err(StorageError::Corrupt("snapshot header/version"));
        }
        let schema = u64::from_le_bytes(body[8..16].try_into().unwrap());
        let index = u64::from_le_bytes(body[16..24].try_into().unwrap());
        let term = u64::from_le_bytes(body[24..32].try_into().unwrap());
        let metadata_bytes = u32::from_le_bytes(body[32..36].try_into().unwrap()) as usize;
        let application_bytes = u64::from_le_bytes(body[36..44].try_into().unwrap());
        if metadata_bytes > limits.max_metadata_bytes
            || application_bytes == 0
            || application_bytes > limits.max_application_bytes as u64
            || body.len() != HEADER + metadata_bytes + application_bytes as usize
        {
            return Err(StorageError::Corrupt("snapshot length budgets"));
        }
        let (sequence, mutations) = NativeLogCodec.decode_batch(
            &body[HEADER..HEADER + metadata_bytes],
            codec_log_limits(limits),
        )?;
        let [LogMutation::Create(bootstrap)] = mutations.as_slice() else {
            return Err(StorageError::Corrupt("snapshot bootstrap record"));
        };
        if sequence != 1 {
            return Err(StorageError::Corrupt("snapshot bootstrap sequence"));
        }
        let metadata = SnapshotMetadata {
            bootstrap: bootstrap.clone(),
            index,
            term,
            application_schema: schema,
        };
        metadata
            .validate()
            .map_err(|_| StorageError::Corrupt("snapshot metadata invalid"))?;
        Ok(Snapshot {
            metadata,
            application: body[HEADER + metadata_bytes..].to_vec(),
        })
    }
}
impl SnapshotCodec for NativeSnapshotCodec {
    fn format_version(&self) -> u32 {
        1
    }
    fn prefix(
        &self,
        metadata: &SnapshotMetadata,
        application_bytes: usize,
        limits: SnapshotLimits,
    ) -> Result<Vec<u8>, StorageError> {
        limits.validate()?;
        metadata.validate()?;
        if application_bytes == 0 || application_bytes > limits.max_application_bytes {
            return Err(StorageError::Rejected("snapshot application budget"));
        }
        let encoded = NativeLogCodec.encode_batch(
            1,
            &[LogMutation::Create(metadata.bootstrap.clone())],
            codec_log_limits(limits),
        )?;
        let mut b = b"VBSNAP01".to_vec();
        b.extend(metadata.application_schema.to_le_bytes());
        b.extend(metadata.index.to_le_bytes());
        b.extend(metadata.term.to_le_bytes());
        b.extend((encoded.len() as u32).to_le_bytes());
        b.extend((application_bytes as u64).to_le_bytes());
        b.extend(encoded);
        Ok(b)
    }
    fn finish(&self, body: &[u8], limits: SnapshotLimits) -> Result<[u8; 4], StorageError> {
        self.parse(body, limits)?;
        Ok(crc32c(body).to_le_bytes())
    }
    fn decode(&self, file: &[u8], limits: SnapshotLimits) -> Result<Snapshot, StorageError> {
        limits.validate()?;
        if file.len() < HEADER + 4 || file.len() > limits.max_file_bytes() {
            return Err(StorageError::Corrupt("snapshot file length"));
        }
        let end = file.len() - 4;
        if crc32c(&file[..end]) != u32::from_le_bytes(file[end..].try_into().unwrap()) {
            return Err(StorageError::Corrupt("snapshot checksum"));
        }
        self.parse(&file[..end], limits)
    }
}

#[derive(Clone)]
struct Root {
    slot: u8,
    generation: SnapshotGeneration,
    length: u64,
    checksum: u32,
    metadata: SnapshotMetadata,
}
struct Stage {
    ticket: SnapshotTicket,
    slot: u8,
    metadata: SnapshotMetadata,
    expected: usize,
    written: usize,
    prefix_bytes: usize,
    expected_checksum: u32,
    sealed: Option<SealedSnapshot>,
}
pub struct NativeSnapshotStore<I: SnapshotIo, C: SnapshotCodec = NativeSnapshotCodec> {
    io: I,
    codec: C,
    identity: SnapshotIdentity,
    binding: StoreBinding,
    limits: SnapshotLimits,
    root: Option<Root>,
    pins: BTreeMap<SnapshotGeneration, Root>,
    stage: Option<Stage>,
    next_generation: u64,
    fenced: bool,
}
impl<I: SnapshotIo> NativeSnapshotStore<I> {
    pub fn create(
        io: I,
        identity: SnapshotIdentity,
        limits: SnapshotLimits,
    ) -> Result<Self, StorageError> {
        Self::create_with_codec(io, identity, limits, NativeSnapshotCodec)
    }
    pub fn recover(
        io: I,
        identity: SnapshotIdentity,
        limits: SnapshotLimits,
    ) -> Result<Self, StorageError> {
        Self::recover_with_codec(io, identity, limits, NativeSnapshotCodec)
    }
}
impl<I: SnapshotIo, C: SnapshotCodec> NativeSnapshotStore<I, C> {
    pub fn create_with_codec(
        mut io: I,
        identity: SnapshotIdentity,
        limits: SnapshotLimits,
        codec: C,
    ) -> Result<Self, StorageError> {
        let limits = limits.validate()?;
        if codec.format_version() != 1 {
            return Err(StorageError::Rejected("snapshot codec format incompatible"));
        }
        match io.read_manifest() {
            Err(e) if e.kind() == io::ErrorKind::NotFound => (),
            Ok(_) => return Err(StorageError::Rejected("snapshot store already exists")),
            Err(e) => return Err(uncertain(e)),
        }
        for slot in 0..=1 {
            match io.read_slot(slot, limits.max_file_bytes()) {
                Err(e) if e.kind() == io::ErrorKind::NotFound => (),
                Ok(_) => {
                    return Err(StorageError::Rejected(
                        "orphaned snapshot data requires recovery",
                    ))
                }
                Err(e) => return Err(uncertain(e)),
            }
        }
        let binding = StoreBinding {
            identity: identity.store,
            session: StoreSession::new(1).unwrap(),
        };
        io.publish_manifest(&manifest(identity, binding, None, &BTreeMap::new()))
            .map_err(uncertain)?;
        Ok(Self {
            io,
            codec,
            identity,
            binding,
            limits,
            root: None,
            pins: BTreeMap::new(),
            stage: None,
            next_generation: 0,
            fenced: false,
        })
    }
    pub fn recover_with_codec(
        mut io: I,
        identity: SnapshotIdentity,
        limits: SnapshotLimits,
        codec: C,
    ) -> Result<Self, StorageError> {
        let limits = limits.validate()?;
        if codec.format_version() != 1 {
            return Err(StorageError::Rejected("snapshot codec format incompatible"));
        }
        let bytes = io.read_manifest().map_err(uncertain)?;
        let (old, descriptor, pin_descriptors) = read_manifest(&bytes, identity, limits)?;
        let root = descriptor
            .map(|d| recover_root(&mut io, &codec, identity, limits, d))
            .transpose()?;
        let mut pins = BTreeMap::new();
        for d in pin_descriptors {
            let pin = recover_root(&mut io, &codec, identity, limits, d)?;
            if pins.insert(pin.generation, pin).is_some() {
                return Err(StorageError::Corrupt("duplicate snapshot pin"));
            }
        }
        if let Some(r) = &root {
            for p in pins.values() {
                if p.slot == r.slot && p.descriptor() != r.descriptor() {
                    return Err(StorageError::Corrupt("snapshot slot aliases"));
                }
            }
        }
        if pins
            .values()
            .map(|p| p.slot)
            .collect::<std::collections::BTreeSet<_>>()
            .len()
            != pins.len()
        {
            return Err(StorageError::Corrupt("pin slots alias"));
        }
        let session = old
            .session
            .get()
            .checked_add(1)
            .and_then(StoreSession::new)
            .ok_or(StorageError::Rejected("snapshot session exhausted"))?;
        let binding = StoreBinding { session, ..old };
        io.publish_manifest(&manifest(identity, binding, root.as_ref(), &pins))
            .map_err(uncertain)?;
        let next_generation = root
            .as_ref()
            .map_or(0, |r| r.generation.get())
            .max(pins.keys().map(|g| g.get()).max().unwrap_or(0));
        Ok(Self {
            io,
            codec,
            identity,
            binding,
            limits,
            root,
            pins,
            stage: None,
            next_generation,
            fenced: false,
        })
    }
    fn live(&self) -> Result<(), StorageError> {
        if self.fenced {
            Err(StorageError::Fenced)
        } else {
            Ok(())
        }
    }
    fn failed(&mut self, e: io::Error) -> StorageError {
        self.fenced = true;
        uncertain(e)
    }
    fn active_stage(&self, ticket: SnapshotTicket) -> Result<&Stage, StorageError> {
        self.live()?;
        self.stage
            .as_ref()
            .filter(|s| s.ticket == ticket)
            .ok_or(StorageError::StaleTicket)
    }
}
impl<I: SnapshotIo, C: SnapshotCodec> SnapshotStore for NativeSnapshotStore<I, C> {
    fn identity(&self) -> SnapshotIdentity {
        self.identity
    }
    fn binding(&self) -> StoreBinding {
        self.binding
    }
    fn limits(&self) -> SnapshotLimits {
        self.limits
    }
    fn begin(
        &mut self,
        metadata: SnapshotMetadata,
        application_bytes: usize,
    ) -> Result<SnapshotTicket, StorageError> {
        self.live()?;
        if self.stage.is_some() {
            return Err(StorageError::Rejected("snapshot stage already admitted"));
        }
        metadata.validate()?;
        if metadata.bootstrap.group != self.identity.group {
            return Err(StorageError::WrongIdentity);
        }
        if self.root.as_ref().is_some_and(|r| {
            metadata.index <= r.metadata.index
                || metadata.term < r.metadata.term
                || metadata.bootstrap != r.metadata.bootstrap
                || metadata.application_schema != r.metadata.application_schema
        }) {
            return Err(StorageError::Rejected(
                "snapshot regression or configuration/schema migration",
            ));
        }
        let prefix = self
            .codec
            .prefix(&metadata, application_bytes, self.limits)?;
        if prefix.len() > self.limits.max_metadata_bytes + HEADER
            || application_bytes == 0
            || application_bytes > self.limits.max_application_bytes
        {
            return Err(StorageError::Rejected("snapshot prefix/application budget"));
        }
        let next = self
            .next_generation
            .checked_add(1)
            .ok_or(StorageError::Rejected("snapshot generation exhausted"))?;
        let ticket = SnapshotTicket {
            binding: self.binding,
            group: self.identity.group,
            generation: SnapshotGeneration::new(next).unwrap(),
        };
        let slot = self.root.as_ref().map_or(0, |r| 1 - r.slot);
        if self.pins.values().any(|p| p.slot == slot) {
            return Err(StorageError::Rejected("inactive snapshot slot is pinned"));
        }
        if let Err(e) = self.io.begin_slot(slot, &prefix) {
            return Err(self.failed(e));
        }
        self.next_generation = next;
        self.stage = Some(Stage {
            ticket,
            slot,
            metadata,
            expected: application_bytes,
            written: 0,
            prefix_bytes: prefix.len(),
            expected_checksum: crc32c(&prefix),
            sealed: None,
        });
        Ok(ticket)
    }
    fn write_chunk(
        &mut self,
        ticket: SnapshotTicket,
        offset: usize,
        bytes: &[u8],
    ) -> Result<(), StorageError> {
        let stage = self.active_stage(ticket)?;
        if stage.sealed.is_some()
            || offset != stage.written
            || bytes.is_empty()
            || bytes.len() > self.limits.max_chunk_bytes
            || bytes.len() > stage.expected.saturating_sub(stage.written)
        {
            return Err(StorageError::Rejected("snapshot chunk offset/phase/budget"));
        }
        let slot = stage.slot;
        if let Err(e) = self.io.append_slot(slot, bytes) {
            return Err(self.failed(e));
        }
        let stage = self.stage.as_mut().unwrap();
        stage.written += bytes.len();
        stage.expected_checksum = crc32c_extend(stage.expected_checksum, bytes);
        Ok(())
    }
    fn seal(&mut self, ticket: SnapshotTicket) -> Result<SealedSnapshot, StorageError> {
        let stage = self.active_stage(ticket)?;
        if stage.sealed.is_some() || stage.written != stage.expected {
            return Err(StorageError::Rejected(
                "incomplete or already sealed snapshot",
            ));
        }
        let slot = stage.slot;
        let expected_length = stage.prefix_bytes + stage.expected;
        let expected_checksum = stage.expected_checksum;
        let metadata = stage.metadata.clone();
        let mut body = match self.io.read_slot(slot, self.limits.max_file_bytes()) {
            Ok(b) => b,
            Err(e) => return Err(self.failed(e)),
        };
        if body.len() != expected_length || crc32c(&body) != expected_checksum {
            self.fenced = true;
            return Err(StorageError::Corrupt("staged snapshot length/checksum"));
        }
        let footer = match self.codec.finish(&body, self.limits) {
            Ok(v) => v,
            Err(e) => {
                self.fenced = true;
                return Err(e);
            }
        };
        if u32::from_le_bytes(footer) != expected_checksum {
            self.fenced = true;
            return Err(StorageError::Corrupt("snapshot codec checksum changed"));
        }
        body.extend(footer);
        let snapshot = match self.codec.decode(&body, self.limits) {
            Ok(v) => v,
            Err(e) => {
                self.fenced = true;
                return Err(e);
            }
        };
        if snapshot.metadata != metadata
            || snapshot.application.len() != self.stage.as_ref().unwrap().expected
        {
            self.fenced = true;
            return Err(StorageError::Corrupt("snapshot codec changed metadata"));
        }
        if let Err(e) = self
            .io
            .append_slot(slot, &footer)
            .and_then(|()| self.io.sync_slot(slot))
        {
            return Err(self.failed(e));
        }
        let sealed = SealedSnapshot {
            ticket,
            file_bytes: body.len() as u64,
            checksum: u32::from_le_bytes(footer),
        };
        self.stage.as_mut().unwrap().sealed = Some(sealed);
        Ok(sealed)
    }
    fn publish(&mut self, sealed: SealedSnapshot) -> Result<SnapshotReceipt, StorageError> {
        let stage = self.active_stage(sealed.ticket)?;
        if stage.sealed != Some(sealed) {
            return Err(StorageError::StaleTicket);
        }
        let root = Root {
            slot: stage.slot,
            generation: sealed.ticket.generation,
            length: sealed.file_bytes,
            checksum: sealed.checksum,
            metadata: stage.metadata.clone(),
        };
        if let Err(e) = self.io.publish_manifest(&manifest(
            self.identity,
            self.binding,
            Some(&root),
            &self.pins,
        )) {
            return Err(self.failed(e));
        }
        let receipt = SnapshotReceipt {
            sealed,
            metadata: root.metadata.clone(),
        };
        self.root = Some(root);
        self.stage = None;
        Ok(receipt)
    }
    fn abort(&mut self, ticket: SnapshotTicket) -> Result<(), StorageError> {
        self.active_stage(ticket)?;
        self.stage = None;
        Ok(())
    }
    fn load(&mut self) -> Result<Option<Snapshot>, StorageError> {
        self.live()?;
        let Some(root) = self.root.clone() else {
            return Ok(None);
        };
        let bytes = match self.io.read_slot(root.slot, self.limits.max_file_bytes()) {
            Ok(b) => b,
            Err(e) => return Err(self.failed(e)),
        };
        if bytes.len() as u64 != root.length
            || bytes.len() < 4
            || u32::from_le_bytes(bytes[bytes.len() - 4..].try_into().unwrap()) != root.checksum
        {
            self.fenced = true;
            return Err(StorageError::Corrupt(
                "published snapshot root/data mismatch",
            ));
        }
        let snapshot = match self.codec.decode(&bytes, self.limits) {
            Ok(s) => s,
            Err(e) => {
                self.fenced = true;
                return Err(e);
            }
        };
        if snapshot.metadata != root.metadata {
            self.fenced = true;
            return Err(StorageError::Corrupt(
                "published snapshot metadata mismatch",
            ));
        }
        Ok(Some(snapshot))
    }
}
fn uncertain(e: io::Error) -> StorageError {
    StorageError::Uncertain(e.to_string())
}
impl Root {
    fn descriptor(&self) -> RootDescriptor {
        (self.slot, self.generation, self.length, self.checksum)
    }
    fn reference(&self, identity: SnapshotIdentity) -> SnapshotRef {
        SnapshotRef {
            store: identity.store,
            group: identity.group,
            generation: self.generation,
            configuration: self.metadata.bootstrap.configuration,
            index: self.metadata.index,
            term: self.metadata.term,
            application_schema: self.metadata.application_schema,
            file_bytes: self.length,
            checksum: self.checksum,
        }
    }
}
fn recover_root<I: SnapshotIo, C: SnapshotCodec>(
    io: &mut I,
    codec: &C,
    identity: SnapshotIdentity,
    limits: SnapshotLimits,
    d: RootDescriptor,
) -> Result<Root, StorageError> {
    let (slot, generation, length, checksum) = d;
    let bytes = io
        .read_slot(slot, limits.max_file_bytes())
        .map_err(uncertain)?;
    if bytes.len() as u64 != length
        || bytes.len() < 4
        || u32::from_le_bytes(bytes[bytes.len() - 4..].try_into().unwrap()) != checksum
    {
        return Err(StorageError::Corrupt("snapshot root/data mismatch"));
    }
    let snapshot = codec.decode(&bytes, limits)?;
    if snapshot.metadata.bootstrap.group != identity.group {
        return Err(StorageError::WrongIdentity);
    }
    Ok(Root {
        slot,
        generation,
        length,
        checksum,
        metadata: snapshot.metadata,
    })
}
impl<I: SnapshotIo, C: SnapshotCodec> SnapshotRetention for NativeSnapshotStore<I, C> {
    fn latest_reference(&self) -> Result<Option<SnapshotRef>, StorageError> {
        self.live()?;
        Ok(self.root.as_ref().map(|r| r.reference(self.identity)))
    }
    fn pin_for_log(&mut self, reference: SnapshotRef) -> Result<(), StorageError> {
        self.live()?;
        if self
            .pins
            .get(&reference.generation)
            .is_some_and(|p| p.reference(self.identity) == reference)
        {
            self.load_pinned(reference)?;
            return Ok(());
        }
        let root = self
            .root
            .as_ref()
            .filter(|r| r.reference(self.identity) == reference)
            .cloned()
            .ok_or(StorageError::StaleTicket)?;
        if self.pins.len() >= 2 {
            return Err(StorageError::Rejected("snapshot pin budget"));
        }
        // Validate the published bytes before promising a durable log anchor.
        self.load()?;
        let mut pins = self.pins.clone();
        pins.insert(root.generation, root);
        if let Err(e) = self.io.publish_manifest(&manifest(
            self.identity,
            self.binding,
            self.root.as_ref(),
            &pins,
        )) {
            return Err(self.failed(e));
        }
        self.pins = pins;
        Ok(())
    }
    fn load_pinned(&mut self, reference: SnapshotRef) -> Result<Snapshot, StorageError> {
        self.live()?;
        let root = self
            .pins
            .get(&reference.generation)
            .filter(|p| p.reference(self.identity) == reference)
            .cloned()
            .ok_or(StorageError::StaleTicket)?;
        let bytes = match self.io.read_slot(root.slot, self.limits.max_file_bytes()) {
            Ok(v) => v,
            Err(e) => return Err(self.failed(e)),
        };
        if bytes.len() as u64 != root.length
            || bytes.len() < 4
            || u32::from_le_bytes(bytes[bytes.len() - 4..].try_into().unwrap()) != root.checksum
        {
            self.fenced = true;
            return Err(StorageError::Corrupt("pinned snapshot length/checksum"));
        }
        let snapshot = match self.codec.decode(&bytes, self.limits) {
            Ok(s) => s,
            Err(e) => {
                self.fenced = true;
                return Err(e);
            }
        };
        if snapshot.metadata != root.metadata {
            self.fenced = true;
            return Err(StorageError::Corrupt("pinned metadata mismatch"));
        }
        Ok(snapshot)
    }
    fn reconcile_log(&mut self, reference: Option<SnapshotRef>) -> Result<(), StorageError> {
        self.live()?;
        if self.stage.is_some() {
            return Err(StorageError::Rejected("snapshot stage pending"));
        }
        let root = reference
            .map(|r| {
                self.pins
                    .get(&r.generation)
                    .filter(|p| p.reference(self.identity) == r)
                    .cloned()
                    .ok_or(StorageError::StaleTicket)
            })
            .transpose()?;
        let pins = root
            .as_ref()
            .map(|r| BTreeMap::from([(r.generation, r.clone())]))
            .unwrap_or_default();
        if let Err(e) =
            self.io
                .publish_manifest(&manifest(self.identity, self.binding, root.as_ref(), &pins))
        {
            return Err(self.failed(e));
        }
        self.root = root;
        self.pins = pins;
        Ok(())
    }
}
fn manifest(
    identity: SnapshotIdentity,
    binding: StoreBinding,
    root: Option<&Root>,
    pins: &BTreeMap<SnapshotGeneration, Root>,
) -> Vec<u8> {
    let mut b = b"VBSSTR02".to_vec();
    b.extend(binding.identity.id.get().to_le_bytes());
    b.extend(binding.identity.incarnation.get().to_le_bytes());
    b.extend(binding.session.get().to_le_bytes());
    b.extend(identity.group.id.get().to_le_bytes());
    b.extend(identity.group.incarnation.get().to_le_bytes());
    match root {
        None => b.push(0),
        Some(r) => {
            b.push(1);
            put_descriptor(&mut b, r.descriptor());
        }
    }
    b.push(pins.len() as u8);
    for p in pins.values() {
        put_descriptor(&mut b, p.descriptor());
    }
    b.extend(crc32c(&b).to_le_bytes());
    b
}
type RootDescriptor = (u8, SnapshotGeneration, u64, u32);
fn put_descriptor(b: &mut Vec<u8>, d: RootDescriptor) {
    b.push(d.0);
    b.extend(d.1.get().to_le_bytes());
    b.extend(d.2.to_le_bytes());
    b.extend(d.3.to_le_bytes());
}
fn read_descriptor(
    b: &[u8],
    cursor: &mut usize,
    limits: SnapshotLimits,
) -> Result<RootDescriptor, StorageError> {
    if b.len().saturating_sub(*cursor) < 21 {
        return Err(StorageError::Corrupt("truncated snapshot descriptor"));
    }
    let p = &b[*cursor..*cursor + 21];
    *cursor += 21;
    let generation = SnapshotGeneration::new(u64::from_le_bytes(p[1..9].try_into().unwrap()))
        .ok_or(StorageError::Corrupt("zero snapshot generation"))?;
    let length = u64::from_le_bytes(p[9..17].try_into().unwrap());
    let checksum = u32::from_le_bytes(p[17..21].try_into().unwrap());
    if p[0] > 1 || length < 48 || length > limits.max_file_bytes() as u64 {
        return Err(StorageError::Corrupt("snapshot descriptor limits"));
    }
    Ok((p[0], generation, length, checksum))
}
type ManifestState = (StoreBinding, Option<RootDescriptor>, Vec<RootDescriptor>);
fn read_manifest(
    b: &[u8],
    identity: SnapshotIdentity,
    limits: SnapshotLimits,
) -> Result<ManifestState, StorageError> {
    if b.len() < 69
        || b.len() > 133
        || (&b[..8] != b"VBSSTR01" && &b[..8] != b"VBSSTR02")
        || crc32c(&b[..b.len() - 4]) != u32::from_le_bytes(b[b.len() - 4..].try_into().unwrap())
    {
        return Err(StorageError::Corrupt("snapshot manifest format/checksum"));
    }
    let id = StoreId::new(u128::from_le_bytes(b[8..24].try_into().unwrap()))
        .ok_or(StorageError::WrongIdentity)?;
    let incarnation = StoreIncarnation::new(u64::from_le_bytes(b[24..32].try_into().unwrap()))
        .ok_or(StorageError::WrongIdentity)?;
    let session = StoreSession::new(u64::from_le_bytes(b[32..40].try_into().unwrap()))
        .ok_or(StorageError::WrongIdentity)?;
    let group_id = GroupId::new(u128::from_le_bytes(b[40..56].try_into().unwrap()))
        .ok_or(StorageError::WrongIdentity)?;
    let group_incarnation =
        GroupIncarnation::new(u64::from_le_bytes(b[56..64].try_into().unwrap()))
            .ok_or(StorageError::WrongIdentity)?;
    let binding = StoreBinding {
        identity: StoreIdentity { id, incarnation },
        session,
    };
    if binding.identity != identity.store
        || (GroupIdentity {
            id: group_id,
            incarnation: group_incarnation,
        }) != identity.group
    {
        return Err(StorageError::WrongIdentity);
    }
    let payload = &b[..b.len() - 4];
    let mut cursor = 65;
    let root = match b[64] {
        0 => None,
        1 => Some(read_descriptor(payload, &mut cursor, limits)?),
        _ => return Err(StorageError::Corrupt("snapshot root flags")),
    };
    let mut pins = Vec::new();
    if &b[..8] == b"VBSSTR02" {
        let count = *payload
            .get(cursor)
            .ok_or(StorageError::Corrupt("snapshot pin count"))?;
        cursor += 1;
        if count > 2 || (root.is_none() && count > 0) {
            return Err(StorageError::Corrupt("snapshot pin limit"));
        }
        for _ in 0..count {
            pins.push(read_descriptor(payload, &mut cursor, limits)?);
        }
    }
    if cursor != payload.len() {
        return Err(StorageError::Corrupt("snapshot manifest trailing bytes"));
    }
    Ok((binding, root, pins))
}
