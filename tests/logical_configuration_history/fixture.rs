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
fn group(id: u128) -> GroupIdentity {
    GroupIdentity {
        id: GroupId::new(id).unwrap(),
        incarnation: GroupIncarnation::new(1).unwrap(),
    }
}
fn store(id: u64) -> StoreIdentity {
    StoreIdentity {
        id: StoreId::new(u128::from(id)).unwrap(),
        incarnation: StoreIncarnation::new(1).unwrap(),
    }
}
pub fn op(n: u128) -> OperationId {
    OperationId::new(n).unwrap()
}
fn cid(n: u64) -> ConfigurationId {
    ConfigurationId::new(n).unwrap()
}
fn config(id: u64, voters: &[u64], learners: &[u64]) -> Configuration {
    Configuration::new(
        cid(id),
        Policy::new(
            Tree::Majority(
                voters
                    .iter()
                    .map(|n| Tree::Voter(NodeId::new(*n).unwrap()))
                    .collect(),
            ),
            PolicyLimits::default(),
        )
        .unwrap(),
        voters
            .iter()
            .map(|n| (NodeId::new(*n).unwrap(), store(*n)))
            .collect(),
        learners
            .iter()
            .map(|n| (NodeId::new(*n).unwrap(), store(*n)))
            .collect(),
    )
    .unwrap()
}
pub fn app(limits: LogicalLimits) -> LogicalApplication {
    let child = config(1, &[2, 3, 4], &[]);
    LogicalApplication::new(
        LogicalStoreMapping::new(group(1), NodeId::new(1).unwrap(), store(1), group(2)).unwrap(),
        Bootstrap {
            group: group(2),
            configuration: child.id(),
            policy: child.policy().clone(),
            voter_stores: child.voter_stores().clone(),
        },
        Bootstrap {
            group: group(1),
            configuration: cid(1),
            policy: Policy::new(
                Tree::Voter(NodeId::new(1).unwrap()),
                PolicyLimits::default(),
            )
            .unwrap(),
            voter_stores: BTreeMap::from([(NodeId::new(1).unwrap(), store(1))]),
        },
        limits,
        NativeLogCodec,
        NativeSnapshotCodec,
        NativeWireCodec::with_membership(WireLimits::default()).unwrap(),
    )
    .unwrap()
}
fn record(
    index: u64,
    term: u64,
    operation: u128,
    expected: u64,
    change: ConfigurationChange,
) -> LogEntry {
    LogEntry {
        index,
        term,
        payload: EntryPayload::Configuration(Box::new(ConfigurationRecord {
            operation: op(operation),
            expected: cid(expected),
            change,
        })),
    }
}
pub fn learners() -> LogEntry {
    record(
        2,
        2,
        10,
        1,
        ConfigurationChange::Learners(config(2, &[2, 3, 4], &[5])),
    )
}
pub fn joint() -> LogEntry {
    record(
        4,
        3,
        11,
        2,
        ConfigurationChange::Joint {
            id: cid(3),
            next: config(4, &[2, 3, 5], &[]),
        },
    )
}
pub fn final_entry() -> LogEntry {
    record(7, 5, 11, 3, ConfigurationChange::Final { id: cid(4) })
}
pub fn prefix() -> Vec<LogEntry> {
    vec![noop(1, 1), learners(), noop(3, 2), joint()]
}
pub fn suffix() -> Vec<LogEntry> {
    vec![noop(5, 4), noop(6, 4), final_entry()]
}
fn noop(index: u64, term: u64) -> LogEntry {
    LogEntry {
        index,
        term,
        payload: EntryPayload::Noop,
    }
}
pub fn history(a: &LogicalApplication, operation: u128) -> LogicalConfigurationOperation {
    let LogicalRead::ConfigurationOperation(h) = a
        .read_at(
            a.applied_index(),
            LogicalQuery::ConfigurationOperation(op(operation)),
        )
        .unwrap()
    else {
        panic!("history")
    };
    *h
}
pub fn encoded(a: &LogicalApplication, h: LogicalConfigurationOperation) -> Vec<u8> {
    let mut out = vec![
        0;
        a.read_encoding_bound(&LogicalQuery::ConfigurationOperation(h.operation))
            .unwrap()
    ];
    let n = a
        .encode_read(&LogicalRead::ConfigurationOperation(Box::new(h)), &mut out)
        .unwrap();
    out.truncate(n);
    out
}

pub fn many_learners() -> Vec<LogEntry> {
    let learners: Vec<_> = (5..=64).collect();
    vec![
        noop(1, 1),
        record(
            2,
            2,
            10,
            1,
            ConfigurationChange::Learners(config(2, &[2, 3, 4], &learners)),
        ),
    ]
}
