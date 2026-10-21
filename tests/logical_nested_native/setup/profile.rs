// SPDX-License-Identifier: RPL-1.5
// Copyright (c) 2026 Karl Livesey
//! Declared finite static history, not a universal metadata/lifetime proof.
use super::*;
use voteboat::{
    application::{BoundedReadableStateMachine, CheckpointStateMachine},
    membership::Membership,
    runtime::{ClientRouterLimits, ReadInvocationLimits, ReadRouterLimits},
    snapshot::SnapshotMetadata,
    snapshot_format::SnapshotCodec,
    snapshot_worker::snapshot_load_reservation,
    transport::TransportLimits,
    wire::WireLimits,
    wire_format::NativeWireCodec,
};
pub const STATIC_SCOPE_BYTES: usize = 16 * 1024;
pub const COMMAND_FRAMING_BYTES: usize = 2048;
const ACTION_BYTES: usize = 177;
const REFERENCE_BYTES: usize = 104;

pub fn checkpoint_bound(limits: LogicalLimits) -> usize {
    [
        limits.max_retained_action_bytes,
        limits.max_actions.checked_mul(ACTION_BYTES).unwrap(),
        limits.max_parent_checkpoint_bytes,
        limits
            .parent_snapshots
            .max_file_bytes()
            .checked_add(REFERENCE_BYTES + 8)
            .unwrap()
            .checked_mul(2)
            .unwrap(),
        STATIC_SCOPE_BYTES,
    ]
    .into_iter()
    .try_fold(0usize, usize::checked_add)
    .unwrap()
}
pub fn upper_checkpoint_bound() -> usize {
    // Exported checkpoints normalize retained capacity to encoded length.
    checkpoint_bound(LogicalLimits {
        max_actions: 256,
        max_retained_action_bytes: 512 * 1024,
        max_parent_checkpoint_bytes: 32 * 1024,
        parent_snapshots: SnapshotLimits {
            max_application_bytes: 4096,
            max_metadata_bytes: 4096,
            max_chunk_bytes: 1024,
        },
        ..LogicalLimits::default()
    })
}
pub fn assert_static_scope(app: &LogicalApplication) {
    // No configuration changes/enrollment/profile are selected in this history.
    let membership = Membership::replay(app.child_bootstrap(), &[], 0).unwrap();
    let metadata = SnapshotMetadata {
        bootstrap: app.child_bootstrap().clone(),
        membership: Some(Box::new(membership)),
        index: 1,
        term: 1,
        application_schema: app.schema_version(),
    };
    // Public prefix includes membership plus additional bootstrap/framing bytes.
    // This scalar size check grants no runtime snapshot or quorum authority.
    let encoded = NativeSnapshotCodec
        .prefix(&metadata, 1, app.limits().parent_snapshots)
        .unwrap();
    let fixed = app
        .checkpoint(app.limits().max_child_checkpoint_bytes)
        .unwrap()
        .len()
        .checked_add(encoded.len())
        .unwrap()
        .checked_add(56 + REFERENCE_BYTES + 8)
        .unwrap();
    assert!(fixed <= STATIC_SCOPE_BYTES, "exact static scope {fixed}");
}
pub fn physical_log_limits() -> LogLimits {
    let command = lower_limits().max_command_bytes;
    let batch = command
        .checked_mul(2)
        .unwrap()
        .max(physical_transport_limits().decoded_bytes);
    let reserve = LogLimits::default().control_reserve_bytes;
    LogLimits {
        max_command_bytes: command,
        max_batch_bytes: batch,
        // ST2 I5: the WAL above the 75% high-water mark must hold the largest
        // batch plus the control reserve, so the usable WAL is four times that.
        max_wal_bytes: batch
            .checked_add(reserve)
            .and_then(|b| b.checked_mul(4))
            .and_then(|u| u.checked_add(reserve))
            .unwrap(),
        control_reserve_bytes: reserve,
        ..LogLimits::default()
    }
    .validate()
    .unwrap()
}
pub fn wire_codec(limits: LogicalLimits) -> NativeWireCodec {
    let frame = limits
        .max_parent_checkpoint_bytes
        .max(limits.parent_snapshots.max_file_bytes())
        .max(limits.parent_log.max_command_bytes)
        .checked_add(COMMAND_FRAMING_BYTES)
        .unwrap();
    NativeWireCodec::with_leadership_transfer(WireLimits {
        max_command_bytes: limits.parent_log.max_command_bytes,
        max_snapshot_bytes: limits.parent_snapshots.max_application_bytes,
        max_frame_bytes: frame,
        max_decoded_bytes: frame.checked_mul(2).unwrap(),
        ..WireLimits::default()
    })
    .unwrap()
}
pub fn physical_wire_codec() -> NativeWireCodec {
    let limits = lower_limits();
    let frame = limits
        .max_child_checkpoint_bytes
        .max(limits.max_command_bytes)
        .checked_add(STATIC_SCOPE_BYTES)
        .unwrap();
    NativeWireCodec::with_leadership_transfer(WireLimits {
        max_command_bytes: limits.max_command_bytes,
        max_snapshot_bytes: limits.max_child_checkpoint_bytes,
        max_frame_bytes: frame,
        max_decoded_bytes: frame.checked_mul(2).unwrap(),
        ..WireLimits::default()
    })
    .unwrap()
}
pub fn physical_transport_limits() -> TransportLimits {
    use voteboat::wire::WireCodec;
    let wire = physical_wire_codec().limits();
    TransportLimits {
        send_frame_bytes: wire.max_frame_bytes,
        receive_frame_bytes: wire.max_frame_bytes,
        decoded_bytes: wire.max_decoded_bytes,
    }
}
pub fn physical_outbound_limits() -> voteboat::outbound::OutboundLimits {
    use voteboat::outbound::{OutboundBudget, OutboundLimits, OutboundUsage};
    let base = OutboundLimits::default();
    let batch = physical_transport_limits()
        .send_frame_bytes
        .checked_add(COMMAND_FRAMING_BYTES)
        .unwrap();
    let peer = OutboundBudget {
        max: OutboundUsage {
            bytes: batch.checked_mul(3).unwrap(),
            ..base.peer.max
        },
        background: OutboundUsage {
            bytes: batch.checked_mul(2).unwrap(),
            ..base.peer.background
        },
        ..base.peer
    };
    OutboundLimits {
        batch_bytes: batch,
        peer,
        node: OutboundBudget {
            max: OutboundUsage {
                bytes: peer.max.bytes.checked_mul(2).unwrap(),
                ..base.node.max
            },
            background: OutboundUsage {
                bytes: peer.background.bytes.checked_mul(2).unwrap(),
                ..base.node.background
            },
            ..base.node
        },
        ..base
    }
    .validate()
    .unwrap()
}
pub fn physical_ingress_limits() -> voteboat::runtime::IngressLimits {
    let batch = physical_transport_limits().decoded_bytes;
    let owned = batch.checked_add(STATIC_SCOPE_BYTES).unwrap();
    voteboat::runtime::IngressLimits {
        batch_bytes: batch,
        background_bytes: owned,
        control_bytes: owned,
        bytes: owned.checked_mul(3).unwrap(),
        ..Default::default()
    }
    .validate()
    .unwrap()
}

pub fn client_limits(limits: LogicalLimits) -> ClientRouterLimits {
    ClientRouterLimits {
        command_bytes: limits.max_command_bytes,
        ..Default::default()
    }
}
pub fn read_limits(limits: LogicalLimits) -> (ReadInvocationLimits, ReadRouterLimits) {
    let result = limits
        .max_parent_checkpoint_bytes
        .max(limits.max_command_bytes)
        .checked_add(STATIC_SCOPE_BYTES)
        .unwrap();
    (
        ReadInvocationLimits {
            result_bytes: result,
            ..Default::default()
        },
        ReadRouterLimits {
            result_bytes: result,
            ..Default::default()
        },
    )
}
pub fn network_limits() -> LogicalNetworkLimits {
    let limits = lower_limits();
    let frame = limits
        .max_command_bytes
        .checked_add(COMMAND_FRAMING_BYTES)
        .unwrap();
    LogicalNetworkLimits {
        frame_bytes: frame,
        decoded_bytes: frame.checked_mul(2).unwrap(),
    }
}
pub fn snapshot_budget(limits: LogicalLimits) -> (usize, usize) {
    let allowance = snapshot_load_reservation(limits.parent_snapshots).unwrap();
    (
        allowance.checked_mul(3).unwrap().checked_add(4096).unwrap(),
        allowance.checked_add(4096).unwrap(),
    )
}
pub fn persistence_limits(limits: LogicalLimits) -> voteboat::worker::WorkerLimits {
    let batch = limits.parent_log.max_batch_bytes;
    voteboat::worker::WorkerLimits {
        max_requests: 8,
        max_units: 8,
        max_bytes: (512 * 1024).max(
            batch
                .checked_mul(8)
                .unwrap()
                .checked_add(STATIC_SCOPE_BYTES)
                .unwrap(),
        ),
        control_requests: 2,
        control_units: 2,
        control_bytes: (128 * 1024).max(
            batch
                .checked_mul(2)
                .unwrap()
                .checked_add(STATIC_SCOPE_BYTES)
                .unwrap(),
        ),
        batch_units: 1,
        batch_bytes: batch,
    }
    .validate()
    .unwrap()
}

#[test]
fn whole_static_history_and_cross_layer_capabilities_are_declared_before_activation() {
    let upper = upper_limits();
    let lower = lower_limits();
    assert_eq!(upper.max_child_checkpoint_bytes, 635456);
    assert_eq!(lower.max_parent_checkpoint_bytes, 1064960);
    assert_eq!(lower.max_command_bytes, 1069056);
    assert_eq!(checkpoint_bound(lower), 11474368);
    assert!(lower.parent_snapshots.max_application_bytes >= upper.max_child_checkpoint_bytes);
    assert!(
        lower.max_command_bytes >= lower.parent_snapshots.max_file_bytes() + COMMAND_FRAMING_BYTES
    );
    assert!(lower.control_action_bytes >= 2 * lower.max_command_bytes);
    assert!(physical_log_limits().max_command_bytes >= lower.max_command_bytes);
    let physical = physical_log_limits();
    let usable = physical.max_wal_bytes - physical.control_reserve_bytes;
    assert_eq!(physical.max_wal_bytes, 139_591_680);
    assert!(
        usable - usable * 75 / 100 >= physical.max_batch_bytes + physical.control_reserve_bytes
    );
    persistence_limits(upper);
    persistence_limits(lower);
    physical_ingress_limits();
    physical_outbound_limits();
    voteboat::native::transport::NativeTransportFactory::new(
        physical_wire_codec(),
        physical_transport_limits(),
    )
    .unwrap();
    for slot in 0..9 {
        let app = lower_app(slot);
        let (invocation, execution) = read_limits(lower);
        for query in [
            LogicalQuery::Image,
            LogicalQuery::Outcome(OperationId::new(1).unwrap()),
        ] {
            let bound = app.read_result_bound(&query).unwrap();
            assert!(bound <= invocation.result_bytes && bound <= execution.result_bytes);
        }
    }
    for slot in 0..3 {
        assert_static_scope(&upper_app(slot));
    }
}

#[test]
fn complete_declared_middle_journal_and_snapshot_framing_fit_lower_bounds() {
    use std::collections::BTreeMap;
    use voteboat::{contracts::HardState, log_format::LogCodec};
    let limits = lower_limits();
    let bootstrap = middle(0);
    let mut groups = BTreeMap::new();
    apply_batch(
        &mut groups,
        &[LogMutation::Create(bootstrap.clone())],
        limits.parent_log,
    )
    .unwrap();
    // Opaque payloads measure the public codec's declared capacity. These local
    // size fixtures are never proposed/applied to a replica or quorum evidence.
    for index in 1..=limits.parent_log.max_entries_per_group {
        let index = u64::try_from(index).unwrap();
        let revision = groups[&bootstrap.group].revision;
        apply_batch(
            &mut groups,
            &[LogMutation::Update(LogUpdate {
                group: bootstrap.group,
                expected_revision: revision,
                hard_state: HardState {
                    term: 1,
                    voted_for: None,
                },
                commit_index: index,
                suffix: Some(Suffix {
                    from: index,
                    entries: vec![LogEntry {
                        index,
                        term: 1,
                        payload: EntryPayload::Command {
                            operation: OperationId::new(u128::from(index)).unwrap(),
                            bytes: vec![0; limits.parent_log.max_command_bytes].into(),
                        },
                    }]
                    .into(),
                }),
                snapshot: None,
                snapshot_membership: None,
            })],
            limits.parent_log,
        )
        .unwrap();
    }
    let bytes = NativeLogCodec
        .encode_checkpoint(
            33,
            &groups,
            limits.parent_log,
            limits.max_parent_checkpoint_bytes,
        )
        .unwrap();
    assert!(bytes.len() <= limits.max_parent_checkpoint_bytes);
    let metadata = SnapshotMetadata {
        bootstrap: bootstrap.clone(),
        membership: Some(Box::new(Membership::replay(&bootstrap, &[], 0).unwrap())),
        index: 1,
        term: 1,
        application_schema: 1,
    };
    let prefix = NativeSnapshotCodec
        .prefix(
            &metadata,
            upper_limits().max_child_checkpoint_bytes,
            limits.parent_snapshots,
        )
        .unwrap();
    assert!(
        prefix.len() + upper_limits().max_child_checkpoint_bytes + 4
            <= limits.parent_snapshots.max_file_bytes()
    );
    eprintln!(
        "355 full middle journal={} bound={} upper checkpoint={} snapshot file bound={}",
        bytes.len(),
        limits.max_parent_checkpoint_bytes,
        upper_limits().max_child_checkpoint_bytes,
        limits.parent_snapshots.max_file_bytes()
    );
}
