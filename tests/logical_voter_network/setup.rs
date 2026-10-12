// SPDX-License-Identifier: RPL-1.5
// Copyright (c) 2026 Karl Livesey
use super::support;
use voteboat::{
    identity::*,
    log::*,
    log_format::NativeLogCodec,
    logical_voter::*,
    quorum::{Limits, Policy, Tree},
    snapshot::SnapshotLimits,
    snapshot_format::NativeSnapshotCodec,
    wire_format::NativeWireCodec,
};

pub fn mapping(slot: usize) -> LogicalStoreMapping {
    LogicalStoreMapping::new(
        support::group(800),
        support::node(11 + slot as u64),
        support::identity(101 + slot as u128),
        support::group(801 + slot as u128),
    )
    .unwrap()
}
pub fn physical_ids(slot: usize) -> std::ops::RangeInclusive<u64> {
    (slot as u64 * 3 + 1)..=(slot as u64 * 3 + 3)
}
pub fn certificate_slot(id: u64) -> u64 {
    (id - 1) % 3 + 1
}
pub fn child(slot: usize) -> Bootstrap {
    Bootstrap {
        group: mapping(slot).child,
        configuration: ConfigurationId::new(1).unwrap(),
        policy: Policy::new(
            Tree::Majority(
                physical_ids(slot)
                    .map(|id| Tree::Voter(support::node(id)))
                    .collect(),
            ),
            Limits::default(),
        )
        .unwrap(),
        voter_stores: physical_ids(slot)
            .map(|id| (support::node(id), support::identity(1000 + u128::from(id))))
            .collect(),
    }
}
pub fn parent() -> Bootstrap {
    Bootstrap {
        group: support::group(800),
        configuration: ConfigurationId::new(1).unwrap(),
        policy: Policy::new(
            Tree::Majority((11..=13).map(|id| Tree::Voter(support::node(id))).collect()),
            Limits::default(),
        )
        .unwrap(),
        voter_stores: (11..=13)
            .map(|id| (support::node(id), support::identity(u128::from(id + 90))))
            .collect(),
    }
}
pub fn limits() -> LogicalLimits {
    LogicalLimits {
        parent_log: LogLimits {
            max_groups: 1,
            max_entries_per_group: 16,
            max_command_bytes: 1024,
            max_batch_bytes: 8192,
            max_batch_units: 1,
            max_pending_units: 8,
            max_snapshot_bytes: 64 * 1024,
            ..LogLimits::default()
        },
        parent_snapshots: SnapshotLimits {
            max_application_bytes: 4096,
            max_metadata_bytes: 4096,
            max_chunk_bytes: 1024,
        },
        max_command_bytes: 32 * 1024,
        max_actions: 64,
        max_retained_action_bytes: 512 * 1024,
        control_actions: 8,
        control_action_bytes: 64 * 1024,
        max_capsule_bytes: 8192,
        max_parent_checkpoint_bytes: 32 * 1024,
        max_child_checkpoint_bytes: 1024 * 1024,
    }
    .validate()
    .unwrap()
}
pub fn app(slot: usize) -> LogicalApplication {
    LogicalApplication::new(
        mapping(slot),
        child(slot),
        parent(),
        limits(),
        NativeLogCodec,
        NativeSnapshotCodec,
        NativeWireCodec::with_leadership_transfer(voteboat::wire::WireLimits::default()).unwrap(),
    )
    .unwrap()
}
pub fn certificate(id: u64) -> &'static [u8] {
    match id {
        1 => include_bytes!("../fixtures/tls/node1.der"),
        2 => include_bytes!("../fixtures/tls/node2.der"),
        3 => include_bytes!("../fixtures/tls/node3.der"),
        _ => unreachable!(),
    }
}
