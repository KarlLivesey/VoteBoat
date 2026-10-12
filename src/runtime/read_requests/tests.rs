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
//! Private storage checks below mint no logical owner, barrier or permit.
use super::*;
use crate::logical_voter::{
    ActivationQuery, ActivationRead, LogicalActivationApplication, LogicalActivationLimits,
    LogicalStoreMapping,
};
mod application;
mod capacity;
mod classification;
mod construction;
mod lifecycle;
mod submission;
type Query = ActivationQuery<Vec<u8>>;
type Read = ActivationRead<i64>;
type Requests = ReadRequests<Query, Read>;
fn group(id: u128) -> GroupIdentity {
    GroupIdentity {
        id: GroupId::new(id).unwrap(),
        incarnation: GroupIncarnation::new(1).unwrap(),
    }
}
fn auth() -> RuntimeAuthorization {
    RuntimeAuthorization::physical(RuntimeOwner {
        store: StoreBinding {
            identity: StoreIdentity {
                id: StoreId::new(1).unwrap(),
                incarnation: StoreIncarnation::new(1).unwrap(),
            },
            session: StoreSession::new(1).unwrap(),
        },
        lane: ExecutionLaneId::new(1).unwrap(),
        generation: RuntimeGeneration::new(1).unwrap(),
    })
}
fn binding(generation: u64) -> ReadInvocationBinding {
    ReadInvocationBinding::new(auth(), ReadInvocationGeneration::new(generation).unwrap())
}
fn app(parent: GroupIdentity) -> LogicalActivationApplication<application::App> {
    LogicalActivationApplication::new(
        parent,
        vec![LogicalStoreMapping::new(
            parent,
            NodeId::new(1).unwrap(),
            auth().owner().store.identity,
            group(3),
        )
        .unwrap()],
        LogicalActivationLimits::default(),
        application::App::new(),
    )
    .unwrap()
}
fn selected() -> Requests {
    let profile = app(group(1)).control_profile();
    let execution = ReadRouter::<Read>::new(
        ReadRouterBinding::new(auth(), ReadRouterGeneration::new(1).unwrap()),
        ReadRouterLimits::default(),
    )
    .unwrap()
    .select_activation::<Query>(profile.clone())
    .unwrap();
    let mut r = Requests::from_binding(binding(1), ReadInvocationLimits::default(), execution)
        .unwrap_or_else(|_| panic!("matching accounting bounds"));
    // Selection is private accounting only; physical binding is not authority.
    r.control_profile = Some(profile);
    r
}
