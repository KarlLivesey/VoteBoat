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
//! First storage slice: durable term/vote transitions, not a full log store.
use crate::identity::*;
use std::collections::BTreeMap;

#[derive(Clone, Copy, Debug, Default, Eq, PartialEq)]
pub struct HardState {
    pub term: u64,
    pub voted_for: Option<NodeId>,
}

impl HardState {
    /// Storage independently rejects a second ballot or erased ballot in a term.
    pub fn follows(self, previous: Self) -> bool {
        if self.term < previous.term || (self.term == 0 && self.voted_for.is_some()) {
            return false;
        }
        self.term > previous.term
            || previous.voted_for.is_none()
            || self.voted_for == previous.voted_for
    }
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub struct VoteRecord {
    pub group: GroupIdentity,
    pub configuration: ConfigurationId,
    pub hard_state: HardState,
}

/// Sequence is an admission identity, NEVER a durable-prefix watermark.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub struct WriteTicket {
    pub binding: StoreBinding,
    pub sequence: u64,
}

/// Constructible by host providers; the core verifies exact pending provenance.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct DurableVotes {
    pub tickets: Vec<WriteTicket>,
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub enum StorageError {
    Rejected(&'static str),
    /// Some bytes may have persisted; stop this binding and resolve by recovery.
    Uncertain(String),
    Corrupt(&'static str),
    WrongIdentity,
    StaleTicket,
    Fenced,
}
impl std::fmt::Display for StorageError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "{self:?}")
    }
}
impl std::error::Error for StorageError {}

/// One host-owned authoritative binding. Calls are synchronous progress steps;
/// run blocking native calls off the future consensus scheduler's owner loop.
/// Dropping a wait does not undo admitted writes. Dropping the store sends no
/// completions; reopening establishes a new session. No hidden worker threads.
/// This deliberately narrow contract grows into LogStore in the next slice.
pub trait VoteStore {
    fn binding(&self) -> StoreBinding;
    /// Verified state at startup, including complete unacknowledged records.
    fn recovered(&self) -> &BTreeMap<GroupIdentity, VoteRecord>;
    /// Rejection transfers no work. Uncertain errors fence this binding.
    fn append_votes(&mut self, records: Vec<VoteRecord>) -> Result<WriteTicket, StorageError>;
    /// Explicit known tickets only. May physically flush additional admitted
    /// writes, but certifies only requested tickets; never future admissions.
    fn barrier(&mut self, tickets: &[WriteTicket]) -> Result<DurableVotes, StorageError>;
}
