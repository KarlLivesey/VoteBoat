// SPDX-License-Identifier: RPL-1.5
// Copyright (c) 2026 Karl Livesey
use super::support;
use voteboat::{
    identity::*, log::*, logical_voter::*, quorum::*, runtime::*, snapshot::SnapshotLimits,
};
pub fn mapping() -> LogicalStoreMapping {
    LogicalStoreMapping::new(
        support::group(800),
        support::node(11),
        support::identity(101),
        support::group(801),
    )
    .unwrap()
}
pub fn child() -> Bootstrap {
    support::bootstrap(801, 3)
}
pub fn app() -> LogicalApplication {
    let parent = Bootstrap {
        group: mapping().parent,
        configuration: ConfigurationId::new(1).unwrap(),
        policy: Policy::new(
            Tree::Majority(vec![Tree::Voter(mapping().voter)]),
            Limits::default(),
        )
        .unwrap(),
        voter_stores: [(mapping().voter, mapping().store)].into(),
    };
    LogicalApplication::new(
        mapping(),
        child(),
        parent,
        limits(),
        voteboat::log_format::NativeLogCodec,
        voteboat::snapshot_format::NativeSnapshotCodec,
        voteboat::wire_format::NativeWireCodec::with_leadership_transfer(Default::default())
            .unwrap(),
    )
    .unwrap()
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
            ..Default::default()
        },
        parent_snapshots: SnapshotLimits {
            max_application_bytes: 4096,
            max_metadata_bytes: 4096,
            max_chunk_bytes: 1024,
        },
        max_command_bytes: 32 * 1024,
        max_actions: 128,
        max_retained_action_bytes: 256 * 1024,
        control_actions: 8,
        control_action_bytes: 32 * 1024,
        max_capsule_bytes: 8192,
        max_parent_checkpoint_bytes: 32 * 1024,
        max_child_checkpoint_bytes: 1024 * 1024,
    }
    .validate()
    .unwrap()
}
pub fn timers(_: GroupIdentity) -> TimerConfig {
    TimerConfig {
        election_min_ms: 60_000,
        election_spread_ms: 1000,
        ..Default::default()
    }
}
pub fn read_limits() -> ReadInvocationLimits {
    ReadInvocationLimits::default()
}
pub fn outbound_limits() -> voteboat::outbound::OutboundLimits {
    Default::default()
}
pub fn network_limits() -> LogicalNetworkLimits {
    Default::default()
}
