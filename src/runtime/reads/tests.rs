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
//! Private result-accounting checks, without manufactured read barriers.
use super::*;
use crate::application::Counter;
use crate::logical_voter::{
    LogicalActivationApplication, LogicalActivationLimits, LogicalStoreMapping,
};
mod capacity;
mod construction;
fn group(id: u128) -> GroupIdentity {
    GroupIdentity {
        id: GroupId::new(id).unwrap(),
        incarnation: GroupIncarnation::new(1).unwrap(),
    }
}
fn binding() -> ReadRouterBinding {
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
    ReadRouterBinding::new(
        RuntimeAuthorization::physical(owner),
        ReadRouterGeneration::new(1).unwrap(),
    )
}
fn profile() -> LogicalActivationControlProfile {
    LogicalActivationApplication::new(
        group(1),
        vec![LogicalStoreMapping::new(
            group(1),
            NodeId::new(1).unwrap(),
            binding().owner.store.identity,
            group(2),
        )
        .unwrap()],
        LogicalActivationLimits::default(),
        Counter::new(16).unwrap(),
    )
    .unwrap()
    .control_profile()
}
fn selected() -> ReadRouter<i64> {
    // Private accounting selection is not a verified logical constructor.
    ReadRouter::new(binding(), ReadRouterLimits::default())
        .unwrap()
        .select_activation::<Vec<u8>>(profile())
        .unwrap()
}
