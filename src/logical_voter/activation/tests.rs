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
use super::*;
use crate::{
    membership::Configuration,
    quorum::{Limits as PolicyLimits, Policy, Tree},
};
mod control_profile;
mod formats;
mod partition;
mod recovery;
mod semantics;
mod soak;
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
            .map(|n| {
                (
                    n,
                    StoreIdentity {
                        id: StoreId::new(n.get() as u128).unwrap(),
                        incarnation: StoreIncarnation::new(1).unwrap(),
                    },
                )
            })
            .collect(),
        BTreeMap::new(),
    )
    .unwrap()
}
fn mapping() -> LogicalStoreMapping {
    LogicalStoreMapping::new(
        group(1),
        NodeId::new(1).unwrap(),
        StoreIdentity {
            id: StoreId::new(1).unwrap(),
            incarnation: StoreIncarnation::new(1).unwrap(),
        },
        group(2),
    )
    .unwrap()
}
fn ops(n: u64) -> ChildActivationOperations {
    ChildActivationOperations::reserved(n).unwrap()
}
/// Activation number `n` with its reserved phase IDs.
fn intent(n: u64) -> ChildActivationIntent {
    ChildActivationIntent::new(
        mapping(),
        ChildActivationOperations::reserved(n).unwrap(),
        &config([1, 2, 3], 1),
        ConfigurationId::new(2).unwrap(),
        ConfigurationId::new(3).unwrap(),
        &config([1, 2, 4], 4),
    )
    .unwrap()
}
fn app(intents: usize) -> LogicalActivationApplication<Counter> {
    LogicalActivationApplication::new(
        group(1),
        vec![mapping()],
        LogicalActivationLimits {
            intents,
            ..LogicalActivationLimits::default()
        },
        Counter::new(16).unwrap(),
    )
    .unwrap()
}
fn entry(index: u64, operation: OperationId, bytes: Vec<u8>) -> LogEntry {
    LogEntry {
        index,
        term: 4,
        payload: EntryPayload::Command {
            operation,
            bytes: bytes.into(),
        },
    }
}
fn apply(
    a: &mut LogicalActivationApplication<Counter>,
    c: ControlCommand,
) -> ChildActivationReceipt {
    let operation = a.control.operation(&c).unwrap();
    let e = entry(
        a.applied_index() + 1,
        operation,
        codec::command(&c, CONTROL_BYTES).unwrap(),
    );
    let ActivationReceipt::Control(r) = a.apply_batch(&[e]).unwrap().remove(0) else {
        panic!("control receipt")
    };
    r
}
fn begin(
    a: &mut LogicalActivationApplication<Counter>,
    i: &ChildActivationIntent,
) -> ChildActivationReceipt {
    apply(a, ControlCommand::Begin(Box::new(i.clone())))
}
fn final_command(i: &ChildActivationIntent) -> ControlCommand {
    ControlCommand::Outcome {
        begin: i.operations.begin,
        fact: state::FinalFact {
            operation: i.operations.membership,
            configuration: i.target_configuration,
            position: ActivationPosition { index: 31, term: 3 },
        },
    }
}
fn finish(
    a: &mut LogicalActivationApplication<Counter>,
    i: &ChildActivationIntent,
) -> ChildActivationReceipt {
    let outcome = apply(a, final_command(i));
    apply(
        a,
        ControlCommand::Release {
            begin: i.operations.begin,
            outcome: outcome.first,
        },
    )
}
