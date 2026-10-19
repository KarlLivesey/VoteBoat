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
//! Capacity controls only: physical bindings below never mint logical authority.
use super::*;
use crate::logical_voter::{
    LogicalActivationApplication, LogicalActivationLimits, LogicalStoreMapping,
};
mod capacity;
mod classification;
mod construction;
mod lifecycle;
fn group(id: u128) -> GroupIdentity {
    GroupIdentity {
        id: GroupId::new(id).unwrap(),
        incarnation: GroupIncarnation::new(1).unwrap(),
    }
}
fn binding(generation: u64) -> ClientRouterBinding {
    let owner = RuntimeOwner {
        store: StoreBinding {
            identity: StoreIdentity {
                id: StoreId::new(1).unwrap(),
                incarnation: StoreIncarnation::new(1).unwrap(),
            },
            session: StoreSession::new(1).unwrap(),
        },
        lane: ExecutionLaneId::new(1).unwrap(),
        generation: RuntimeGeneration::new(1).unwrap(),
    };
    ClientRouterBinding::new(
        RuntimeAuthorization::physical(owner),
        ClientRouterGeneration::new(generation).unwrap(),
    )
}
fn profile() -> LogicalActivationControlProfile {
    LogicalActivationApplication::new(
        group(1),
        vec![LogicalStoreMapping::new(
            group(1),
            NodeId::new(1).unwrap(),
            binding(1).owner.store.identity,
            group(2),
        )
        .unwrap()],
        LogicalActivationLimits::default(),
        Counter::new(16).unwrap(),
    )
    .unwrap()
    .control_profile()
}
fn limits() -> ClientRouterLimits {
    let mut l = ClientRouterLimits {
        requests: 4,
        group_requests: 4,
        ..ClientRouterLimits::default()
    };
    let worst = ClientRouter::<CounterReceipt>::window_bytes(l).unwrap();
    l.control_bytes = 2 * worst;
    l.bytes = 3 * worst;
    l
}
fn selected() -> ClientRouter<CounterReceipt> {
    // This is the private accounting selection, not public logical construction.
    ClientRouter::new(binding(1), limits())
        .unwrap()
        .select_activation(profile())
        .unwrap()
}
fn release() -> ClientRequest {
    let mut bytes = b"VBACTA02\x03".to_vec();
    bytes.extend_from_slice(&10u128.to_le_bytes());
    bytes.extend_from_slice(&4u64.to_le_bytes());
    bytes.extend_from_slice(&1u64.to_le_bytes());
    ClientRequest {
        group: group(1),
        operation: OperationId::new(14).unwrap(),
        bytes,
    }
}
