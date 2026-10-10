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
//! Durable local credential preparation records; never membership authority.
use crate::{authorization::CredentialGeneration, secure::PeerIdentity};
pub const CREDENTIAL_JOURNAL_CONTRACT_VERSION: u16 = 1;
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub struct CredentialReloadRequest {
    pub sequence: u64,
    pub expected: CredentialGeneration,
    pub replacement: CredentialGeneration,
}
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub struct CredentialReloadRecord {
    pub owner: PeerIdentity,
    pub request: CredentialReloadRequest,
    pub digest: [u8; 32],
}
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum CredentialJournalError {
    InvalidRecord,
    WrongOwner,
    SequenceConflict,
    WrongGeneration,
    Fenced,
    Io {
        kind: std::io::ErrorKind,
        uncertain: bool,
    },
}
/// Synchronous bounded journal, driven outside the consensus poll thread.
/// Successful publish is durable. An uncertain error fences writes until reopen.
/// Records are fixed-size Copy values; rejection preserves caller ownership.
/// Exactly one writer owns each journal. Exact latest-record retries are
/// idempotent; superseded sequences and changed bodies at a reused sequence fail.
/// The owner must validate current material before preparing a replacement.
/// This records local preparation, not cluster-wide activation or audit history.
pub trait CredentialJournal {
    fn owner(&self) -> PeerIdentity;
    fn latest(&self) -> Result<Option<CredentialReloadRecord>, CredentialJournalError>;
    fn publish(&mut self, record: CredentialReloadRecord) -> Result<(), CredentialJournalError>;
}
