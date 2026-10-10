// SPDX-License-Identifier: RPL-1.5
// Copyright (c) 2026 Karl Livesey
//! Bounded metadata history. Complete corruption is never an older-slot fallback.
use std::{
    fs::File,
    io::{self, Read},
    path::Path,
};
pub(super) const MAGIC: &[u8; 8] = b"VBMJR001";
pub(super) const RECORD: usize = 52;
pub(super) const RECORDS: usize = 4096;
const MAX_BYTES: usize = 8 + RECORD * RECORDS + RECORD - 1;

#[derive(Clone)]
pub(super) struct State {
    pub length: u64,
    pub latest: [u8; RECORD],
}
fn invalid(message: impl Into<String>) -> io::Error {
    io::Error::new(io::ErrorKind::InvalidData, message.into())
}
fn validate(bytes: &[u8]) -> io::Result<()> {
    crate::native::log_store::read_manifest(bytes)
        .map(|_| ())
        .map_err(|error| invalid(format!("manifest journal record: {error:?}")))
}
impl State {
    pub fn single(bytes: &[u8]) -> io::Result<Self> {
        validate(bytes)?;
        Ok(Self {
            length: (8 + RECORD) as u64,
            latest: bytes.try_into().unwrap(),
        })
    }
    pub fn next(&self, bytes: &[u8]) -> io::Result<Self> {
        let next = Self::single(bytes)?;
        let (old, boundary) = crate::native::log_store::read_manifest(&self.latest).unwrap();
        let (new, end) = crate::native::log_store::read_manifest(bytes).unwrap();
        if old.identity != new.identity || old.session.get() > new.session.get() || boundary > end {
            return Err(invalid(
                "manifest journal identity/session/boundary regression",
            ));
        }
        Ok(Self {
            length: self.length + RECORD as u64,
            ..next
        })
    }
    pub fn read(path: &Path) -> io::Result<Self> {
        let file = File::open(path)?;
        if file.metadata()?.len() > MAX_BYTES as u64 {
            return Err(invalid("manifest journal capacity"));
        }
        let mut bytes = Vec::new();
        file.take(MAX_BYTES as u64 + 1).read_to_end(&mut bytes)?;
        Self::decode(&bytes)
    }
    pub fn decode(bytes: &[u8]) -> io::Result<Self> {
        if bytes.len() < 8 + RECORD || bytes.len() > MAX_BYTES || &bytes[..8] != MAGIC {
            return Err(invalid("manifest journal format/capacity"));
        }
        let (records, _incomplete_tail) = bytes[8..].as_chunks::<RECORD>();
        let mut records = records.iter();
        let mut state = Self::single(records.next().unwrap())?;
        for record in records {
            state = state.next(record)?;
        }
        // Only the incomplete final record is omitted. Every complete record,
        // including a complete unsynchronized tail, must validate.
        Ok(state)
    }
    pub fn compacted(&self) -> Vec<u8> {
        let mut bytes = MAGIC.to_vec();
        bytes.extend(self.latest);
        bytes
    }
    pub fn full(&self) -> bool {
        (self.length as usize - 8) / RECORD >= RECORDS
    }
}
