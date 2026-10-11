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
use crate::{
    application::ApplicationError,
    identity::{GroupIdentity, OperationId},
};
use std::num::{NonZeroU128, NonZeroU64};

#[derive(Clone, Copy, Debug, Eq, Ord, PartialEq, PartialOrd, Hash)]
pub struct TransactionId(NonZeroU128);
impl TransactionId {
    pub const fn new(value: u128) -> Option<Self> {
        match NonZeroU128::new(value) {
            Some(v) => Some(Self(v)),
            None => None,
        }
    }
    pub const fn get(self) -> u128 {
        self.0.get()
    }
}
#[derive(Clone, Copy, Debug, Eq, Ord, PartialEq, PartialOrd, Hash)]
pub struct AccountId(NonZeroU64);
impl AccountId {
    pub const fn new(value: u64) -> Option<Self> {
        match NonZeroU64::new(value) {
            Some(v) => Some(Self(v)),
            None => None,
        }
    }
    pub const fn get(self) -> u64 {
        self.0.get()
    }
}
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub struct AccountRef {
    group: GroupIdentity,
    account: AccountId,
}
impl AccountRef {
    pub const fn new(group: GroupIdentity, account: AccountId) -> Self {
        Self { group, account }
    }
    pub const fn group(self) -> GroupIdentity {
        self.group
    }
    pub const fn account(self) -> AccountId {
        self.account
    }
}
/// Participants are supplied in canonical GroupIdentity order, independent of
/// debit direction. Each transfer selects one account in each participant group.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub struct TransferProfile {
    coordinator: GroupIdentity,
    participants: [GroupIdentity; 2],
}
impl TransferProfile {
    pub fn new(
        coordinator: GroupIdentity,
        participants: [GroupIdentity; 2],
    ) -> Result<Self, ApplicationError> {
        if participants[0] >= participants[1] || participants.contains(&coordinator) {
            return Err(ApplicationError::InvalidCommand);
        }
        Ok(Self {
            coordinator,
            participants,
        })
    }
    pub const fn coordinator(self) -> GroupIdentity {
        self.coordinator
    }
    pub const fn participants(self) -> [GroupIdentity; 2] {
        self.participants
    }
    pub(crate) fn participant(self, group: GroupIdentity) -> Option<usize> {
        self.participants.iter().position(|g| *g == group)
    }
}
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub struct TransferLimits {
    transactions: usize,
    accounts: usize,
}
impl TransferLimits {
    pub fn new(transactions: usize, accounts: usize) -> Result<Self, ApplicationError> {
        if transactions == 0 || transactions > 4096 || accounts == 0 || accounts > 4096 {
            return Err(ApplicationError::DedupCapacity);
        }
        Ok(Self {
            transactions,
            accounts,
        })
    }
    pub const fn transactions(self) -> usize {
        self.transactions
    }
    pub const fn accounts(self) -> usize {
        self.accounts
    }
}
/// IDs are distinct within each destination group. Identical IDs in different
/// groups are allowed; cross-transaction aliases in one group are rejected.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub struct PhaseIds {
    coordinator: [OperationId; 4],
    participants: [[OperationId; 3]; 2],
}
impl PhaseIds {
    pub fn new(
        coordinator: [OperationId; 4],
        participants: [[OperationId; 3]; 2],
    ) -> Result<Self, ApplicationError> {
        if !distinct(&coordinator) || participants.iter().any(|p| !distinct(p)) {
            return Err(ApplicationError::OperationConflict);
        }
        Ok(Self {
            coordinator,
            participants,
        })
    }
    pub const fn coordinator(self) -> [OperationId; 4] {
        self.coordinator
    }
    pub const fn participants(self) -> [[OperationId; 3]; 2] {
        self.participants
    }
}
fn distinct(ids: &[OperationId]) -> bool {
    ids.iter().enumerate().all(|(i, id)| !ids[..i].contains(id))
}
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub struct TransferSpec {
    id: TransactionId,
    profile: TransferProfile,
    debit: AccountRef,
    credit: AccountRef,
    amount: i64,
    phases: PhaseIds,
}
impl TransferSpec {
    pub fn new(
        id: TransactionId,
        profile: TransferProfile,
        debit: AccountRef,
        credit: AccountRef,
        amount: i64,
        phases: PhaseIds,
    ) -> Result<Self, ApplicationError> {
        if amount <= 0
            || debit.group == credit.group
            || profile.participant(debit.group).is_none()
            || profile.participant(credit.group).is_none()
        {
            return Err(ApplicationError::InvalidCommand);
        }
        Ok(Self {
            id,
            profile,
            debit,
            credit,
            amount,
            phases,
        })
    }
    pub const fn id(self) -> TransactionId {
        self.id
    }
    pub const fn profile(self) -> TransferProfile {
        self.profile
    }
    pub const fn debit(self) -> AccountRef {
        self.debit
    }
    pub const fn credit(self) -> AccountRef {
        self.credit
    }
    pub const fn amount(self) -> i64 {
        self.amount
    }
    pub const fn phases(self) -> PhaseIds {
        self.phases
    }
    pub(crate) fn account(self, group: GroupIdentity) -> Option<AccountRef> {
        if self.debit.group == group {
            Some(self.debit)
        } else if self.credit.group == group {
            Some(self.credit)
        } else {
            None
        }
    }
}
