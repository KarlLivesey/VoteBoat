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
//! Public opt-in application contracts, without alleged child/Node proofs.
use std::{collections::BTreeMap, mem::size_of};
use voteboat::{
    application::*,
    identity::*,
    log::*,
    logical_voter::*,
    membership::Configuration,
    quorum::{Limits as PolicyLimits, Policy, Tree},
    runtime::LogicalResponseCodec,
};
#[path = "logical_activation_application/application.rs"]
mod application;
#[path = "logical_activation_application/contracts.rs"]
mod contracts;
use application::App;
fn group(id: u128) -> GroupIdentity {
    GroupIdentity {
        id: GroupId::new(id).unwrap(),
        incarnation: GroupIncarnation::new(1).unwrap(),
    }
}
fn op(id: u128) -> OperationId {
    OperationId::new(id).unwrap()
}
fn config(nodes: [u64; 3], id: u64) -> Configuration {
    let voters = nodes.map(|n| NodeId::new(n).unwrap());
    Configuration::new(
        ConfigurationId::new(id).unwrap(),
        Policy::new(
            Tree::Majority(voters.map(Tree::Voter).to_vec()),
            PolicyLimits::default(),
        )
        .unwrap(),
        voters
            .into_iter()
            .map(|node| (node, store(node.get() as u128)))
            .collect(),
        BTreeMap::new(),
    )
    .unwrap()
}
fn store(id: u128) -> StoreIdentity {
    StoreIdentity {
        id: StoreId::new(id).unwrap(),
        incarnation: StoreIncarnation::new(1).unwrap(),
    }
}
fn mapping() -> LogicalStoreMapping {
    LogicalStoreMapping::new(group(1), NodeId::new(1).unwrap(), store(1), group(2)).unwrap()
}
fn app() -> LogicalActivationApplication<App> {
    LogicalActivationApplication::new(
        group(1),
        vec![mapping()],
        LogicalActivationLimits::default(),
        App::new(),
    )
    .unwrap()
}
fn intent() -> ChildActivationIntent {
    ChildActivationIntent::new(
        mapping(),
        ChildActivationOperations::reserved(1).unwrap(),
        &config([1, 2, 3], 1),
        ConfigurationId::new(2).unwrap(),
        ConfigurationId::new(3).unwrap(),
        &config([1, 2, 4], 4),
    )
    .unwrap()
}
/// The reserved begin phase of `intent()`.
fn begin_op() -> OperationId {
    ChildActivationOperations::reserved(1).unwrap().begin
}
fn entry(index: u64, operation: OperationId, bytes: Vec<u8>) -> LogEntry {
    LogEntry {
        index,
        term: 1,
        payload: EntryPayload::Command {
            operation,
            bytes: bytes.into(),
        },
    }
}
