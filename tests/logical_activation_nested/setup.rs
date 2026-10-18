// SPDX-License-Identifier: RPL-1.5
// Copyright (c) 2026 Karl Livesey
use super::{application, support};
use std::collections::BTreeMap;
use voteboat::{
    identity::*,
    log::*,
    log_format::NativeLogCodec,
    logical_voter::*,
    quorum::{Limits, Policy, Tree},
    runtime::*,
    snapshot::SnapshotLimits,
    snapshot_format::NativeSnapshotCodec,
    wire_format::NativeWireCodec,
};
pub const BASE: u128 = 356000;
pub fn physical_ids(slot: usize) -> std::ops::RangeInclusive<u64> {
    (slot as u64 * 3 + 1)..=(slot as u64 * 3 + 3)
}
pub fn certificate_slot(id: u64) -> u64 {
    (id - 1) % 3 + 1
}
pub fn certificate(id: u64) -> &'static [u8] {
    match id {
        1 => include_bytes!("../fixtures/tls/node1.der"),
        2 => include_bytes!("../fixtures/tls/node2.der"),
        3 => include_bytes!("../fixtures/tls/node3.der"),
        _ => unreachable!(),
    }
}
fn bootstrap(group: u128, nodes: impl Iterator<Item = u64>, store_offset: u128) -> Bootstrap {
    let nodes: Vec<_> = nodes.collect();
    Bootstrap {
        group: support::group(group),
        configuration: ConfigurationId::new(1).unwrap(),
        policy: Policy::new(
            Tree::Majority(
                nodes
                    .iter()
                    .map(|&n| Tree::Voter(support::node(n)))
                    .collect(),
            ),
            Limits::default(),
        )
        .unwrap(),
        voter_stores: nodes
            .into_iter()
            .map(|n| {
                (
                    support::node(n),
                    support::identity(store_offset + u128::from(n)),
                )
            })
            .collect(),
    }
}
pub fn bottom(slot: usize) -> Bootstrap {
    bootstrap(811 + slot as u128, physical_ids(slot), 1000)
}
pub fn middle() -> Bootstrap {
    bootstrap(801, 201..=203, 1800)
}
pub fn top() -> Bootstrap {
    bootstrap(800, 101..=101, 2900)
}
pub fn lower_mapping(slot: usize) -> LogicalStoreMapping {
    LogicalStoreMapping::new(
        middle().group,
        support::node(201 + slot as u64),
        support::identity(2001 + slot as u128),
        bottom(slot).group,
    )
    .unwrap()
}
pub fn upper_mapping() -> LogicalStoreMapping {
    LogicalStoreMapping::new(
        top().group,
        support::node(101),
        support::identity(3001),
        middle().group,
    )
    .unwrap()
}
pub fn top_app() -> LogicalActivationApplication<application::App> {
    LogicalActivationApplication::new(
        top().group,
        vec![upper_mapping()],
        LogicalActivationLimits {
            intents: 2,
            business_operations: 16,
            command_bytes: 1024,
            checkpoint_bytes: 32 * 1024,
        },
        application::App::new(),
    )
    .unwrap()
}
pub fn upper_limits() -> LogicalLimits {
    LogicalLimits {
        parent_log: LogLimits {
            max_groups: 1,
            max_entries_per_group: 32,
            max_command_bytes: 1024,
            max_batch_bytes: 8192,
            max_batch_units: 1,
            max_pending_units: 8,
            max_snapshot_bytes: 64 * 1024,
            ..LogLimits::default()
        },
        parent_snapshots: SnapshotLimits {
            max_application_bytes: 32 * 1024,
            max_metadata_bytes: 8192,
            max_chunk_bytes: 4096,
        },
        max_command_bytes: 64 * 1024,
        max_actions: 128,
        max_retained_action_bytes: 192 * 1024,
        control_actions: 16,
        control_action_bytes: 128 * 1024,
        max_capsule_bytes: 8192,
        max_parent_checkpoint_bytes: 64 * 1024,
        max_child_checkpoint_bytes: 512 * 1024,
    }
    .validate()
    .unwrap()
}
pub fn lower_limits() -> LogicalLimits {
    LogicalLimits {
        parent_log: LogLimits {
            max_groups: 1,
            max_entries_per_group: 64,
            max_command_bytes: upper_limits().max_command_bytes,
            max_batch_bytes: 256 * 1024,
            max_batch_units: 1,
            max_pending_units: 8,
            max_snapshot_bytes: 2 * 1024 * 1024,
            ..LogLimits::default()
        },
        parent_snapshots: SnapshotLimits {
            max_application_bytes: upper_limits().max_child_checkpoint_bytes,
            max_metadata_bytes: 8192,
            max_chunk_bytes: 8192,
        },
        max_command_bytes: 1024 * 1024,
        max_actions: 4096,
        max_retained_action_bytes: 4 * 1024 * 1024,
        control_actions: 128,
        control_action_bytes: 2 * 1024 * 1024,
        max_capsule_bytes: 768 * 1024,
        max_parent_checkpoint_bytes: 1024 * 1024,
        max_child_checkpoint_bytes: 16 * 1024 * 1024,
    }
    .validate()
    .unwrap()
}
pub fn wire() -> NativeWireCodec {
    NativeWireCodec::with_leadership_transfer(voteboat::wire::WireLimits {
        max_command_bytes: lower_limits().max_command_bytes,
        ..Default::default()
    })
    .unwrap()
}
pub fn upper_app() -> LogicalApplication {
    LogicalApplication::new(
        upper_mapping(),
        middle(),
        top(),
        upper_limits(),
        NativeLogCodec,
        NativeSnapshotCodec,
        wire(),
    )
    .unwrap()
    .with_activation_control_profile(top_app().control_profile())
    .unwrap()
}
pub fn lower_app(slot: usize) -> LogicalApplication {
    if slot >= 3 {
        return LogicalApplication::new_enrollment_candidate(
            lower_mapping(slot),
            bottom(slot),
            middle(),
            lower_limits(),
            (NativeLogCodec, NativeSnapshotCodec, wire()),
            LogicalEnrollmentPlan {
                parent: middle().group,
                target: voteboat::secure::PeerIdentity {
                    node: support::node(201 + slot as u64),
                    store: support::identity(2001 + slot as u128),
                },
                operation: OperationId::new(BASE + if slot == 3 { 1 } else { 101 }).unwrap(),
                expected: ConfigurationId::new(if slot == 3 { 1 } else { 4 }).unwrap(),
                configuration: ConfigurationId::new(if slot == 3 { 2 } else { 5 }).unwrap(),
                source_application_schema: LOGICAL_APPLICATION_SCHEMA,
            },
        )
        .unwrap();
    }
    LogicalApplication::new(
        lower_mapping(slot),
        bottom(slot),
        middle(),
        lower_limits(),
        NativeLogCodec,
        NativeSnapshotCodec,
        wire(),
    )
    .unwrap()
}
pub fn intent() -> ChildActivationIntent {
    let old = voteboat::membership::Membership::replay(&middle(), &[], 0)
        .unwrap()
        .stable()
        .clone();
    let target = voteboat::membership::Configuration::new(
        ConfigurationId::new(4).unwrap(),
        Policy::new(
            Tree::Majority(
                [201, 202, 204]
                    .map(|n| Tree::Voter(support::node(n)))
                    .to_vec(),
            ),
            Limits::default(),
        )
        .unwrap(),
        [201, 202, 204]
            .into_iter()
            .map(|n| (support::node(n), support::identity(1800 + u128::from(n))))
            .collect(),
        BTreeMap::new(),
    )
    .unwrap();
    let op = |n| OperationId::new(BASE + n).unwrap();
    ChildActivationIntent::new(
        upper_mapping(),
        ChildActivationOperations {
            begin: op(0),
            learners: op(1),
            membership: op(2),
            outcome: op(3),
            release: op(4),
        },
        &old,
        ConfigurationId::new(2).unwrap(),
        ConfigurationId::new(3).unwrap(),
        &target,
    )
    .unwrap()
}
pub fn timers(parent: GroupIdentity) -> TimerConfig {
    if parent == top().group {
        TimerConfig {
            heartbeat_ms: 500,
            election_min_ms: 45000,
            election_spread_ms: 15000,
            expirations_per_poll: 32,
        }
    } else {
        TimerConfig {
            heartbeat_ms: 500,
            election_min_ms: 6000,
            election_spread_ms: 4000,
            expirations_per_poll: 32,
        }
    }
}
pub fn network_limits() -> LogicalNetworkLimits {
    LogicalNetworkLimits {
        frame_bytes: 4 * 1024 * 1024,
        decoded_bytes: 8 * 1024 * 1024,
    }
}
pub fn outbound_limits() -> voteboat::outbound::OutboundLimits {
    let standard = voteboat::outbound::OutboundLimits::default();
    voteboat::outbound::OutboundLimits {
        batch_bytes: 4 * 1024 * 1024,
        peer: voteboat::outbound::OutboundBudget {
            max: voteboat::outbound::OutboundUsage {
                bytes: 8 * 1024 * 1024,
                ..standard.peer.max
            },
            ..standard.peer
        },
        ..standard
    }
}
pub fn read_limits() -> ReadInvocationLimits {
    ReadInvocationLimits {
        result_bytes: 16 * 1024 * 1024,
        bytes: 64 * 1024 * 1024,
        ..Default::default()
    }
}

pub fn second_intent() -> ChildActivationIntent {
    let old = current_configuration(4, [201, 202, 204]);
    let target = current_configuration(7, [201, 204, 205]);
    let op = |n| OperationId::new(BASE + 100 + n).unwrap();
    ChildActivationIntent::new(
        upper_mapping(),
        ChildActivationOperations {
            begin: op(0),
            learners: op(1),
            membership: op(2),
            outcome: op(3),
            release: op(4),
        },
        &old,
        ConfigurationId::new(5).unwrap(),
        ConfigurationId::new(6).unwrap(),
        &target,
    )
    .unwrap()
}
fn current_configuration(id: u64, voters: [u64; 3]) -> voteboat::membership::Configuration {
    voteboat::membership::Configuration::new(
        ConfigurationId::new(id).unwrap(),
        Policy::new(
            Tree::Majority(voters.map(|n| Tree::Voter(support::node(n))).to_vec()),
            Limits::default(),
        )
        .unwrap(),
        voters
            .into_iter()
            .map(|n| (support::node(n), support::identity(1800 + u128::from(n))))
            .collect(),
        BTreeMap::new(),
    )
    .unwrap()
}
