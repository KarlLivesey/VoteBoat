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
};
#[path = "setup/profile.rs"]
mod profile;
pub use profile::{
    client_limits, network_limits, persistence_limits, physical_ingress_limits,
    physical_log_limits, physical_outbound_limits, physical_transport_limits, physical_wire_codec,
    read_limits, snapshot_budget, wire_codec,
};

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
fn bootstrap(group: u128, ids: std::ops::RangeInclusive<u64>, stores: u128) -> Bootstrap {
    Bootstrap {
        group: support::group(group),
        configuration: ConfigurationId::new(1).unwrap(),
        policy: Policy::new(
            Tree::Majority(
                ids.clone()
                    .map(|id| Tree::Voter(support::node(id)))
                    .collect(),
            ),
            Limits::default(),
        )
        .unwrap(),
        voter_stores: ids
            .map(|id| {
                (
                    support::node(id),
                    support::identity(stores + u128::from(id)),
                )
            })
            .collect(),
    }
}
pub fn bottom(slot: usize) -> Bootstrap {
    bootstrap(811 + slot as u128, physical_ids(slot), 1000)
}
pub fn middle(slot: usize) -> Bootstrap {
    bootstrap(
        801 + slot as u128,
        (201 + slot as u64 * 3)..=(203 + slot as u64 * 3),
        1800,
    )
}
pub fn top() -> Bootstrap {
    bootstrap(800, 101..=103, 2900)
}
pub fn lower_mapping(slot: usize) -> LogicalStoreMapping {
    LogicalStoreMapping::new(
        support::group(801 + (slot / 3) as u128),
        support::node(201 + slot as u64),
        support::identity(2001 + slot as u128),
        support::group(811 + slot as u128),
    )
    .unwrap()
}
pub fn upper_mapping(slot: usize) -> LogicalStoreMapping {
    LogicalStoreMapping::new(
        support::group(800),
        support::node(101 + slot as u64),
        support::identity(3001 + slot as u128),
        support::group(801 + slot as u128),
    )
    .unwrap()
}
pub fn upper_limits() -> LogicalLimits {
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
        max_actions: 256,
        max_retained_action_bytes: 512 * 1024,
        control_actions: 16,
        control_action_bytes: 64 * 1024,
        max_capsule_bytes: 8192,
        max_parent_checkpoint_bytes: 32 * 1024,
        max_child_checkpoint_bytes: profile::upper_checkpoint_bound(),
    }
    .validate()
    .unwrap()
}
pub fn lower_limits() -> LogicalLimits {
    let upper = upper_limits();
    let snapshots = SnapshotLimits {
        max_application_bytes: upper.max_child_checkpoint_bytes,
        max_metadata_bytes: 4096,
        max_chunk_bytes: 4096,
    };
    let parent = (32usize.checked_mul(upper.max_command_bytes).unwrap())
        .checked_add(profile::STATIC_SCOPE_BYTES)
        .unwrap()
        .max(
            snapshots
                .max_file_bytes()
                .checked_add(profile::COMMAND_FRAMING_BYTES)
                .unwrap(),
        );
    let capsule = parent.checked_add(profile::COMMAND_FRAMING_BYTES).unwrap();
    let command = capsule.checked_add(profile::COMMAND_FRAMING_BYTES).unwrap();
    let limits = LogicalLimits {
        parent_log: LogLimits {
            max_groups: 1,
            max_entries_per_group: 32,
            max_command_bytes: upper.max_command_bytes,
            max_batch_bytes: 64 * 1024,
            max_batch_units: 1,
            max_pending_units: 8,
            max_snapshot_bytes: snapshots.max_file_bytes(),
            ..LogLimits::default()
        },
        parent_snapshots: snapshots,
        max_command_bytes: command,
        max_actions: 4096,
        max_retained_action_bytes: 8 * 1024 * 1024,
        control_actions: 128,
        // Protect two full selected control commands without reducing total history.
        control_action_bytes: command.checked_mul(2).unwrap(),
        max_capsule_bytes: capsule,
        max_parent_checkpoint_bytes: parent,
        max_child_checkpoint_bytes: 16 * 1024 * 1024,
    }
    .validate()
    .unwrap();
    assert!(profile::checkpoint_bound(limits) <= limits.max_child_checkpoint_bytes);
    limits
}

fn app(
    mapping: LogicalStoreMapping,
    child: Bootstrap,
    parent: Bootstrap,
    limits: LogicalLimits,
) -> LogicalApplication {
    let app = LogicalApplication::new(
        mapping,
        child,
        parent,
        limits,
        NativeLogCodec,
        NativeSnapshotCodec,
        wire_codec(limits),
    )
    .unwrap();
    profile::assert_static_scope(&app);
    app
}
pub fn lower_app(slot: usize) -> LogicalApplication {
    app(
        lower_mapping(slot),
        bottom(slot),
        middle(slot / 3),
        lower_limits(),
    )
}
pub fn upper_app(slot: usize) -> LogicalApplication {
    app(upper_mapping(slot), middle(slot), top(), upper_limits())
}

/// Declared functional profile for two serial inner-quorum levels, not a
/// measured performance guarantee. Physical child timers stay native.
pub fn timers(parent: GroupIdentity) -> voteboat::runtime::TimerConfig {
    if parent == top().group {
        voteboat::runtime::TimerConfig {
            heartbeat_ms: 500,
            election_min_ms: 45_000,
            election_spread_ms: 15_000,
            expirations_per_poll: 32,
            append_hold_ms: voteboat::runtime::TimerConfig::DEFAULT_APPEND_HOLD_MS,
        }
    } else if (0..3).any(|slot| parent == middle(slot).group) {
        voteboat::runtime::TimerConfig {
            heartbeat_ms: 500,
            election_min_ms: 6_000,
            election_spread_ms: 4_000,
            expirations_per_poll: 32,
            append_hold_ms: voteboat::runtime::TimerConfig::DEFAULT_APPEND_HOLD_MS,
        }
    } else {
        voteboat::native::startup::NativeTimingProfile::Throughput.timers()
    }
}
