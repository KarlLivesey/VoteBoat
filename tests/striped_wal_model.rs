// SPDX-License-Identifier: RPL-1.5
//! Independent finite dependency model; no production striped provider is implied.
//! Ordinary Raft chooses immutable commands and legal suffix/snapshot changes.
//! This model checks local storage release/recovery, not Raft quorum safety.
//! Scope: one fixed group/incarnation, two payload lanes/control mirrors, three
//! fixed positions, one previous/selected suffix pair, at most one crash followed
//! by one device loss. Snapshot storage is a separate surviving provider. Byte
//! framing, corruption, reconfiguration, repeated faults and repair are not modeled.
#[path = "striped_wal_model/mod.rs"]
mod striped_wal_model;
use striped_wal_model::{search, types::*};

#[test]
fn finite_striped_storage_dependencies_preserve_prefix_promises_and_recovery() {
    for scenario in [Scenario::Prefix, Scenario::Suffix, Scenario::Snapshot] {
        let graph = search(scenario, Rule::Strict).unwrap();
        assert!(graph.states > 1 && graph.edges > graph.states && graph.terminals > 0);
        assert_eq!(graph.frontier, 0);
        println!("striped {scenario:?}: {graph:?}; caps(states=100000,edges=500000,depth=64)");
    }
}

#[test]
fn weakened_striped_storage_rules_generate_and_replay_original_violations() {
    for (scenario, rule, expected) in [
        (Scenario::Prefix, Rule::MaximumPrefix, Violation::PrefixHole),
        (
            Scenario::Prefix,
            Rule::SingleControlCopy,
            Violation::LostPromise,
        ),
        (
            Scenario::Suffix,
            Rule::ReuseOldGeneration,
            Violation::WrongGeneration,
        ),
        (
            Scenario::Snapshot,
            Rule::EarlySnapshot,
            Violation::MissingSnapshot,
        ),
    ] {
        let failure = search(scenario, rule).expect_err("weakened rule must fail");
        assert_eq!(failure.violation, expected);
        assert_eq!(failure.replay(scenario, rule), expected);
        println!("striped {rule:?}: {failure:?}");
    }
}
