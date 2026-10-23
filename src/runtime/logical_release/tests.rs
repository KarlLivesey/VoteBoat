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
//! Private credit controls use physical metadata; no original logical authority,
//! committed lease, child observation or read barrier is minted by this fixture.
use super::*;
use crate::application::{Counter, CounterReceipt};
mod capacity;
mod classification;
mod construction_capacity;
mod prepared_capsules;
fn group(id: u128) -> GroupIdentity {
    GroupIdentity {
        id: GroupId::new(id).unwrap(),
        incarnation: GroupIncarnation::new(1).unwrap(),
    }
}
fn owner() -> RuntimeOwner {
    RuntimeOwner {
        store: StoreBinding {
            identity: StoreIdentity {
                id: StoreId::new(1).unwrap(),
                incarnation: StoreIncarnation::new(1).unwrap(),
            },
            session: StoreSession::new(1).unwrap(),
        },
        lane: ExecutionLaneId::new(1).unwrap(),
        generation: RuntimeGeneration::new(1).unwrap(),
    }
}
fn wrapper() -> LogicalActivationApplication<Counter> {
    LogicalActivationApplication::new(
        group(1),
        vec![LogicalStoreMapping::new(
            group(1),
            NodeId::new(1).unwrap(),
            owner().store.identity,
            group(2),
        )
        .unwrap()],
        LogicalActivationLimits::default(),
        Counter::new(16).unwrap(),
    )
    .unwrap()
}
fn accounting() -> LogicalReleaseCoordinator<CounterReceipt, i64> {
    LogicalReleaseCoordinator {
        authorization: RuntimeAuthorization::physical(owner()),
        control_profile: None,
        limits: LogicalReleaseLimits {
            pending: 5,
            bytes: 256 * 1024,
            capsule_bytes: 1024,
            control_pending: 2,
            control_bytes: 128 * 1024,
        },
        pending: BTreeMap::new(),
        requests: VecDeque::new(),
        sequence: 0,
        usage: LogicalReleaseUsage::default(),
        closed: false,
        failed_domain: false,
    }
}
fn selected() -> LogicalReleaseCoordinator<CounterReceipt, i64> {
    accounting()
        .select_activation(wrapper().control_profile(), 3)
        .unwrap()
}
