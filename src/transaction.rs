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
//! Bounded retained account transfers in existing replicated application logs.
//!
//! Internal serialized phase commands are trusted host-driver assertions derived
//! from original Node Applied completions or fresh quorum-backed status. Their
//! bytes are not cryptographic certificates or a public foreign-proof intake.
//! Ownership is fixed; ordinary safe voter membership changes remain supported.
mod types;
pub use types::*;
mod status;
pub use status::*;
mod admission;
mod codec;
mod command;
mod driver;
mod state;
#[cfg(test)]
mod tests;
mod traits;
pub use driver::*;

use crate::{application::ApplicationError, identity::GroupIdentity};
use command::Command;
use state::{Account, Record};
use std::collections::BTreeMap;

pub const TRANSFER_SCHEMA: u64 = 0x5642_5452_0000_0001;
pub const TRANSFER_COMMAND_BYTES: usize = 444;

/// A single concrete application hosts coordinator or participant roles in the
/// existing Node map. No retained transaction or original phase ID is evicted.
/// The driver must provision each participant for the coordinator's whole
/// lifetime capacity and preflight fixed account membership before Start.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct TransferApplication {
    group: GroupIdentity,
    profile: TransferProfile,
    limits: TransferLimits,
    role: TransferRole,
    applied: u64,
    accounts: BTreeMap<AccountId, Account>,
    records: BTreeMap<TransactionId, Record>,
}
impl TransferApplication {
    pub fn coordinator(
        group: GroupIdentity,
        profile: TransferProfile,
        limits: TransferLimits,
    ) -> Result<Self, ApplicationError> {
        if group != profile.coordinator() {
            return Err(ApplicationError::InvalidCommand);
        }
        Ok(Self::empty(
            group,
            profile,
            limits,
            TransferRole::Coordinator,
        ))
    }
    pub fn participant(
        group: GroupIdentity,
        profile: TransferProfile,
        accounts: &[(AccountId, i64)],
        limits: TransferLimits,
    ) -> Result<Self, ApplicationError> {
        let slot = profile
            .participant(group)
            .ok_or(ApplicationError::InvalidCommand)?;
        if accounts.is_empty() || accounts.len() > limits.accounts() {
            return Err(ApplicationError::DedupCapacity);
        }
        let mut app = Self::empty(group, profile, limits, TransferRole::Participant(slot));
        for &(id, value) in accounts {
            if value < 0
                || app
                    .accounts
                    .insert(
                        id,
                        Account {
                            opening: value,
                            balance: value,
                            lock: None,
                        },
                    )
                    .is_some()
            {
                return Err(ApplicationError::InvalidCommand);
            }
        }
        Ok(app)
    }
    fn empty(
        group: GroupIdentity,
        profile: TransferProfile,
        limits: TransferLimits,
        role: TransferRole,
    ) -> Self {
        Self {
            group,
            profile,
            limits,
            role,
            applied: 0,
            accounts: BTreeMap::new(),
            records: BTreeMap::new(),
        }
    }
    pub fn group(&self) -> GroupIdentity {
        self.group
    }
    pub fn profile(&self) -> TransferProfile {
        self.profile
    }
    pub fn limits(&self) -> TransferLimits {
        self.limits
    }
    pub fn role(&self) -> TransferRole {
        self.role
    }
    pub fn remaining_transactions(&self) -> usize {
        self.limits.transactions() - self.records.len()
    }
    /// The only public command encoder is business Start. Protocol encoders are
    /// crate-private and require the trusted driver's actual phase provenance.
    pub fn start(spec: TransferSpec) -> Result<Vec<u8>, ApplicationError> {
        codec::encode(Command::start(spec))
    }
    /// Local applied diagnostic; distributed decisions require Node quorum reads.
    pub fn status(&self, id: TransactionId) -> Option<TransferStatus> {
        self.records.get(&id).map(|r| r.status(self.group))
    }
    pub fn contains_account(&self, id: AccountId) -> bool {
        self.accounts.contains_key(&id)
    }
    pub fn account_ids(&self) -> impl Iterator<Item = AccountId> + '_ {
        self.accounts.keys().copied()
    }
    pub fn account(&self, id: AccountId) -> AccountRead {
        match self.accounts.get(&id) {
            None => AccountRead::Unknown,
            Some(account) => match account.lock {
                Some(tx) => AccountRead::Locked(tx),
                None => AccountRead::Balance(account.balance),
            },
        }
    }
}
