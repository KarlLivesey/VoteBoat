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
//! Constructor/bound preflight; these scalar checks mint no runtime authority.
use super::*;
use voteboat::application::{BoundedReadableStateMachine, CheckpointStateMachine, StateMachine};
#[test]
fn selected_three_cell_storage_snapshot_wire_and_control_bounds_are_coherent() {
    let timers = groups::timers();
    timers.validate().unwrap();
    assert_eq!(timers.heartbeat_ms, 250);
    assert_eq!(timers.election_min_ms, 3000);
    assert_eq!(timers.election_spread_ms, 2000);
    assert_eq!(timers.expirations_per_poll, 32);
    let limits = groups::storage_limits();
    physical_storage_bounds(limits);
    let ordinary = fixture::storage_limits().max_retained_action_bytes
        - fixture::storage_limits().control_action_bytes;
    assert_eq!(
        limits.max_retained_action_bytes - limits.control_action_bytes,
        ordinary
    );
    assert_eq!(ordinary, 64 * 1024);
    assert_eq!(
        limits.max_command_bytes,
        limits.parent_snapshots.max_file_bytes() + 1024
    );
    assert_eq!(limits.control_action_bytes, 2 * limits.max_command_bytes);
    assert_eq!(limits.parent_log.max_entries_per_group, 16);
    assert_eq!(limits.parent_log.max_command_bytes, 1024);
    let work = snapshots::work_limits().validate().unwrap();
    let allowance =
        voteboat::snapshot_worker::snapshot_load_reservation(limits.parent_snapshots).unwrap();
    assert!(
        allowance
            .checked_mul(2)
            .and_then(|v| v.checked_add(4096))
            .unwrap()
            <= work.max_bytes
    );
    let wire = groups::wire_limits().validate().unwrap();
    assert!(limits.max_command_bytes <= wire.max_command_bytes);
    assert!(limits.max_child_checkpoint_bytes <= wire.max_snapshot_bytes);
    voteboat::native::transport::NativeTransportFactory::new(
        groups::wire_codec(),
        voteboat::transport::TransportLimits::default(),
    )
    .unwrap();
    let client = groups::client_limits();
    let reads = ReadInvocationLimits::default();
    assert!(client.command_bytes >= limits.max_command_bytes);
    assert!(client.receipt_bytes >= std::mem::size_of::<LogicalReceipt>());
    for slot in 0..3 {
        let app = groups::application(slot);
        assert_eq!(
            app.activation_control_profile().unwrap().mappings(),
            (0..3).map(groups::mapping).collect::<Vec<_>>()
        );
        for query in [
            LogicalQuery::Image,
            LogicalQuery::Metadata,
            LogicalQuery::Outcome(OperationId::new(1).unwrap()),
        ] {
            assert!(app.read_result_bound(&query).unwrap() <= reads.result_bytes);
        }
        let requirements = app.deployment_requirements().unwrap();
        assert_eq!(requirements.command_bytes, wire.max_command_bytes);
        assert_eq!(requirements.snapshot_bytes, wire.max_snapshot_bytes);
        assert!(
            app.checkpoint(limits.max_child_checkpoint_bytes)
                .unwrap()
                .len()
                <= limits.max_child_checkpoint_bytes
        );
    }
}

#[path = "preflight/lifetime.rs"]
mod lifetime;

fn physical_storage_bounds(limits: LogicalLimits) {
    let physical = groups::physical_log_limits().validate().unwrap();
    assert!(physical.max_command_bytes >= limits.max_command_bytes);
    assert!(physical.max_snapshot_bytes >= limits.max_child_checkpoint_bytes);
    let worker = voteboat::worker::WorkerLimits::default();
    assert!(worker.batch_bytes <= physical.max_batch_bytes);
    assert!(worker.batch_units <= physical.max_batch_units);
}
