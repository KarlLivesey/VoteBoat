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
//! Private storage-accounting controls. Physical metadata below mints no logical
//! owner, committed lease, read barrier or protocol authorization.
use super::*;
use crate::logical_voter::{
    LogicalActivationApplication, LogicalActivationLimits, LogicalStoreMapping,
};
mod capacity;
mod classification;
mod construction;
fn group(id: u128) -> GroupIdentity {
    GroupIdentity {
        id: GroupId::new(id).unwrap(),
        incarnation: GroupIncarnation::new(1).unwrap(),
    }
}
fn store() -> StoreIdentity {
    StoreIdentity {
        id: StoreId::new(1).unwrap(),
        incarnation: StoreIncarnation::new(1).unwrap(),
    }
}
fn wrapper() -> LogicalActivationApplication<Counter> {
    LogicalActivationApplication::new(
        group(1),
        vec![
            LogicalStoreMapping::new(group(1), NodeId::new(1).unwrap(), store(), group(2)).unwrap(),
        ],
        LogicalActivationLimits::default(),
        Counter::new(16).unwrap(),
    )
    .unwrap()
}
fn binding(generation: u64) -> ApplicationRouterBinding {
    ApplicationRouterBinding::new(
        RuntimeAuthorization::physical(RuntimeOwner {
            store: StoreBinding {
                identity: store(),
                session: StoreSession::new(1).unwrap(),
            },
            lane: ExecutionLaneId::new(1).unwrap(),
            generation: RuntimeGeneration::new(1).unwrap(),
        }),
        ApplicationRouterGeneration::new(generation).unwrap(),
    )
}
fn limits() -> ApplicationRouterLimits {
    ApplicationRouterLimits {
        batches: 8,
        receipts: 12,
        bytes: 32768,
        control_batches: 2,
        control_bytes: 8192,
        batch_receipts: 3,
        batch_bytes: 2048,
    }
}
fn selected() -> ApplicationRouter<CounterReceipt> {
    ApplicationRouter::new(binding(1), limits())
        .unwrap()
        .select_activation(wrapper().control_profile(), 3)
        .unwrap()
}
