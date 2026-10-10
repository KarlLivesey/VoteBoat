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
//! Fixed-size checksummed credential journal over explicit atomic record I/O.
mod file;
mod format;
use crate::{credential_reload::*, secure::PeerIdentity};
pub use file::FileCredentialRecord;
/// Atomic replacement must recover either the old or new complete record.
/// Filesystem I/O belongs on a host-owned preparation worker, not Node polling.
/// An error with `uncertain: false` guarantees that the old record is unchanged.
/// The caller supplies exclusive writer ownership for the whole journal lifetime.
pub trait CredentialRecordIo {
    fn read(&mut self) -> Result<Option<[u8; 128]>, CredentialJournalError>;
    fn replace(&mut self, bytes: &[u8; 128]) -> Result<(), CredentialJournalError>;
}
pub struct NativeCredentialJournal<I = FileCredentialRecord> {
    io: I,
    owner: PeerIdentity,
    latest: Option<CredentialReloadRecord>,
    fenced: bool,
}
impl<I: CredentialRecordIo> NativeCredentialJournal<I> {
    pub fn new(mut io: I, owner: PeerIdentity) -> Result<Self, (CredentialJournalError, I)> {
        let latest = match io.read().and_then(|b| b.map(format::decode).transpose()) {
            Ok(latest) => latest,
            Err(e) => return Err((e, io)),
        };
        if latest.is_some_and(|r| r.owner != owner) {
            return Err((CredentialJournalError::WrongOwner, io));
        }
        Ok(Self {
            io,
            owner,
            latest,
            fenced: false,
        })
    }
    pub fn into_io(self) -> I {
        self.io
    }
}
impl<I: CredentialRecordIo> CredentialJournal for NativeCredentialJournal<I> {
    fn owner(&self) -> PeerIdentity {
        self.owner
    }
    fn latest(&self) -> Result<Option<CredentialReloadRecord>, CredentialJournalError> {
        if self.fenced {
            Err(CredentialJournalError::Fenced)
        } else {
            Ok(self.latest)
        }
    }
    fn publish(&mut self, r: CredentialReloadRecord) -> Result<(), CredentialJournalError> {
        if self.fenced {
            return Err(CredentialJournalError::Fenced);
        }
        format::validate(r)?;
        if r.owner != self.owner {
            return Err(CredentialJournalError::WrongOwner);
        }
        if let Some(old) = self.latest {
            if old == r {
                return Ok(());
            }
            if r.request.sequence <= old.request.sequence {
                return Err(CredentialJournalError::SequenceConflict);
            }
            if r.request.expected < old.request.replacement {
                return Err(CredentialJournalError::WrongGeneration);
            }
        }
        if let Err(e) = self.io.replace(&format::encode(r)) {
            self.fenced = matches!(
                e,
                CredentialJournalError::Io {
                    uncertain: true,
                    ..
                } | CredentialJournalError::Fenced
            );
            return Err(e);
        }
        self.latest = Some(r);
        Ok(())
    }
}
