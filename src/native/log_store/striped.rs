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
//! Explicit native WAL striping; one authoritative log store, mirrored controls.
use super::{JournalIo, NativeLogStore};
use crate::{contracts::StorageError, identity::StoreIdentity, log::LogLimits};
use std::io;
mod catalog;
mod file;
mod journal;
mod record;
use catalog::Catalog;
pub use file::FileStripedIo;
pub use record::{StripedControlImage, StripedGroupControl};

/// Native I/O seam. Each fixed lane has two payload slots selected by generation
/// parity. Reusing a slot requires every catalog to have selected the other slot.
/// Catalog publication is atomic and synchronizes file and directory metadata.
/// Complete writes and bounded reads are mandatory. A lost lane returns NotFound.
pub trait StripedIo {
    fn lane_count(&self) -> usize;
    fn read_catalog(&mut self, lane: usize, limit: usize) -> io::Result<Vec<u8>>;
    fn read_payload(&mut self, lane: usize, generation: u64, limit: usize) -> io::Result<Vec<u8>>;
    fn append_payload(&mut self, lane: usize, generation: u64, bytes: &[u8]) -> io::Result<()>;
    fn sync_payload(&mut self, lane: usize, generation: u64) -> io::Result<()>;
    fn truncate_payload(&mut self, lane: usize, generation: u64, length: u64) -> io::Result<()>;
    fn publish_catalog(&mut self, lane: usize, bytes: &[u8]) -> io::Result<()>;
}
#[derive(Clone, Copy, Debug)]
pub struct StripedLimits {
    pub chunk_bytes: usize,
    pub max_catalog_bytes: usize,
    pub control_reserve_bytes: usize,
    pub max_records: usize,
}
impl Default for StripedLimits {
    fn default() -> Self {
        Self {
            chunk_bytes: 64 * 1024,
            max_catalog_bytes: 128 * 1024 * 1024,
            control_reserve_bytes: 2 * 1024 * 1024,
            max_records: 262_144,
        }
    }
}
impl StripedLimits {
    fn validate(self, limits: LogLimits) -> Result<Self, StorageError> {
        if self.chunk_bytes < 256
            || self.chunk_bytes > limits.max_wal_bytes
            || self.max_catalog_bytes < 256
            || self.max_catalog_bytes > 256 * 1024 * 1024
            || self.control_reserve_bytes >= self.max_catalog_bytes
            || self.max_records == 0
            || self.max_records > 1_048_576
        {
            return Err(StorageError::Rejected("invalid striped limits"));
        }
        Ok(self)
    }
}

/// Uses the native log format only. Chunks carry original exact framed bytes;
/// mirrored command-stripped controls cannot be used as a recovered Raft log.
pub struct StripedJournal<I: StripedIo> {
    io: I,
    identity: StoreIdentity,
    limits: LogLimits,
    striped: StripedLimits,
    catalog: Catalog,
    lengths: Vec<usize>,
    next_lane: usize,
    synchronized: bool,
}
pub type StripedLogStore<I> = NativeLogStore<StripedJournal<I>>;

impl<I: StripedIo> NativeLogStore<StripedJournal<I>> {
    pub fn create_striped(
        io: I,
        identity: StoreIdentity,
        limits: LogLimits,
        striped: StripedLimits,
    ) -> Result<Self, StorageError> {
        Self::create(
            StripedJournal::create(io, identity, limits, striped)?,
            identity,
            limits,
        )
    }
    pub fn recover_striped(
        io: I,
        identity: StoreIdentity,
        limits: LogLimits,
        striped: StripedLimits,
    ) -> Result<Self, StorageError> {
        Self::recover(
            StripedJournal::recover(io, identity, limits, striped)?,
            identity,
            limits,
        )
    }
}

fn storage(error: io::Error) -> StorageError {
    StorageError::Uncertain(format!("striped journal: {error}"))
}
fn invalid(message: &'static str) -> io::Error {
    io::Error::new(io::ErrorKind::InvalidData, message)
}
fn encode_error(error: StorageError) -> io::Error {
    io::Error::new(io::ErrorKind::InvalidData, format!("{error:?}"))
}
