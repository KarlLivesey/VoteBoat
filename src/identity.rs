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
//! Distinct, checked identities. Restart generations are persisted by storage.
use std::num::{NonZeroU128, NonZeroU64};

macro_rules! id64 {
    ($($name:ident),+ $(,)?) => {$ (
        #[derive(Clone, Copy, Debug, Eq, Ord, PartialEq, PartialOrd, Hash)]
        pub struct $name(NonZeroU64);
        impl $name {
            pub const fn new(value: u64) -> Option<Self> {
                match NonZeroU64::new(value) { Some(v) => Some(Self(v)), None => None }
            }
            pub const fn get(self) -> u64 { self.0.get() }
        }
    )+};
}
macro_rules! id128 {
    ($($name:ident),+ $(,)?) => {$ (
        #[derive(Clone, Copy, Debug, Eq, Ord, PartialEq, PartialOrd, Hash)]
        pub struct $name(NonZeroU128);
        impl $name {
            pub const fn new(value: u128) -> Option<Self> {
                match NonZeroU128::new(value) { Some(v) => Some(Self(v)), None => None }
            }
            pub const fn get(self) -> u128 { self.0.get() }
        }
    )+};
}
id64!(
    NodeId,
    GroupIncarnation,
    StoreIncarnation,
    StoreSession,
    ConfigurationId,
    OwnershipEpoch,
    ExecutionLaneId,
    WalLaneId,
    WalLaneGeneration,
    LogGeneration,
    LogRevision,
    ReadRequestId,
    SnapshotGeneration,
    RuntimeGeneration,
    StorageWorkerGeneration,
    SnapshotWorkerGeneration,
    OutboundGeneration,
    SecureSessionGeneration,
    IngressGeneration,
    ApplicationRouterGeneration,
    ClientRouterGeneration,
    ReadRouterGeneration,
    ReadInvocationGeneration
);
id128!(GroupId, StoreId, ResponsibilityId, OperationId);

#[derive(Clone, Copy, Debug, Eq, Ord, PartialEq, PartialOrd, Hash)]
pub struct GroupIdentity {
    pub id: GroupId,
    pub incarnation: GroupIncarnation,
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub struct StoreIdentity {
    pub id: StoreId,
    pub incarnation: StoreIncarnation,
}

/// A fresh session is persisted before a reopened store admits work.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub struct StoreBinding {
    pub identity: StoreIdentity,
    pub session: StoreSession,
}
