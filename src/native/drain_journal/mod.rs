// SPDX-License-Identifier: RPL-1.5
// Copyright (c) 2026 Karl Livesey
use crate::{drain::*, secure::PeerIdentity};
mod file;
mod format;
pub use file::FileDrainRecord;
pub const MAX_DRAIN_RECORD_BYTES: usize = 80 + 32 * crate::runtime::MAX_LOCAL_DRAIN_GROUPS + 32;

/// Domain-specific atomic record I/O. A replacement recovers the old or whole
/// new record; partial final records are corruption. No independent writer may
/// access this record. The owner retains directory exclusivity until I/O ends.
pub trait DrainRecordIo {
    fn read(&mut self) -> Result<Option<Vec<u8>>, DrainJournalError>;
    fn replace(&mut self, bytes: &[u8]) -> Result<(), DrainJournalError>;
}
pub struct NativeDrainJournal<I = FileDrainRecord> {
    io: I,
    owner: PeerIdentity,
    latest: Option<DrainRecord>,
    initialized: bool,
    fenced: bool,
}
impl<I: DrainRecordIo> NativeDrainJournal<I> {
    /// Initial selection may have no record. After initialization use recover,
    /// which distinguishes a lost required record from an unused journal.
    pub fn new(mut io: I, owner: PeerIdentity) -> Result<Self, (DrainJournalError, I)> {
        let bytes = match io.read() {
            Ok(bytes) => bytes,
            Err(e) => return Err((e, io)),
        };
        let latest = match bytes.as_deref().map(format::envelope).transpose() {
            Ok(Some((found, _))) if found != owner => {
                return Err((DrainJournalError::WrongOwner, io));
            }
            Ok(record) => record.and_then(|(_, record)| record),
            Err(e) => return Err((e, io)),
        };
        Ok(Self {
            io,
            owner,
            latest,
            initialized: bytes.is_some(),
            fenced: false,
        })
    }
    pub fn recover(io: I, owner: PeerIdentity) -> Result<Self, (DrainJournalError, I)> {
        let journal = Self::new(io, owner)?;
        if !journal.initialized {
            return Err((DrainJournalError::MissingRecord, journal.io));
        }
        Ok(journal)
    }
    /// Durably establish an empty, owner-bound journal before exposing a new
    /// service. Existing records are preserved. Call outside the owner poll.
    pub fn initialize(io: I, owner: PeerIdentity) -> Result<Self, (DrainJournalError, I)> {
        let mut journal = Self::new(io, owner)?;
        if !journal.initialized {
            if let Err(e) = journal.io.replace(&format::empty(owner)) {
                return Err((e, journal.io));
            }
            journal.initialized = true;
        }
        Ok(journal)
    }
    pub fn into_io(self) -> I {
        self.io
    }
}
impl<I: DrainRecordIo> DrainJournal for NativeDrainJournal<I> {
    fn owner(&self) -> PeerIdentity {
        self.owner
    }
    fn latest(&self) -> Result<Option<DrainRecord>, DrainJournalError> {
        if self.fenced {
            Err(DrainJournalError::Fenced)
        } else {
            Ok(self.latest.clone())
        }
    }
    fn publish(&mut self, record: DrainRecord) -> Result<(), DrainJournalError> {
        if self.fenced {
            return Err(DrainJournalError::Fenced);
        }
        record.validate()?;
        if record.owner != self.owner {
            return Err(DrainJournalError::WrongOwner);
        }
        match &self.latest {
            Some(old) if old == &record => return Ok(()),
            Some(old) if !record.follows(old) => return Err(DrainJournalError::Conflict),
            None if record.phase != DrainPhase::Active => return Err(DrainJournalError::Conflict),
            _ => (),
        }
        if let Err(e) = self.io.replace(&format::encode(&record)) {
            self.fenced = matches!(
                e,
                DrainJournalError::Fenced
                    | DrainJournalError::Io {
                        uncertain: true,
                        ..
                    }
            );
            return Err(e);
        }
        self.latest = Some(record);
        self.initialized = true;
        Ok(())
    }
}
