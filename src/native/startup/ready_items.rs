// SPDX-License-Identifier: RPL-1.5
// Copyright (c) 2026 Karl Livesey
use super::*;
#[test]
fn ready_visit_selection_changes_only_quantum_and_keeps_derived_resource_floors() {
    for log in [
        LogLimits::default(),
        LogLimits {
            max_batch_bytes: 8 * 1024 * 1024,
            ..LogLimits::default()
        },
    ] {
        for groups in [1, 2, 256] {
            let baseline = startup_shard_limits(log, groups, 16).unwrap();
            for selected in [1, 16, 32, 64] {
                let limits = startup_shard_limits(log, groups, selected).unwrap();
                assert_eq!(limits.visit_items, selected);
                assert_eq!(limits.max_groups, baseline.max_groups);
                assert_eq!(limits.max_items, baseline.max_items);
                assert_eq!(limits.max_bytes, baseline.max_bytes);
                // The queue grows only to hold the client router's admissions.
                assert_eq!(
                    limits.group_items,
                    baseline.group_items.max(
                        startup_client_limits(selected).group_requests
                            + baseline.group_control_items
                    )
                );
                assert_eq!(limits.group_bytes, baseline.group_bytes);
                assert_eq!(limits.control_items, baseline.control_items);
                assert_eq!(limits.control_bytes, baseline.control_bytes);
                assert_eq!(limits.group_control_items, baseline.group_control_items);
                assert_eq!(limits.group_control_bytes, baseline.group_control_bytes);
                assert_eq!(limits.background_items, baseline.background_items);
                assert_eq!(limits.background_bytes, baseline.background_bytes);
                assert_eq!(limits.max_event_bytes, baseline.max_event_bytes);
                assert_eq!(limits.visit_bytes, baseline.visit_bytes);
                assert_eq!(limits.visit_ms, baseline.visit_ms);
            }
        }
    }
    for invalid in [0, 1025, usize::MAX] {
        assert_eq!(
            startup_shard_limits(LogLimits::default(), 1, invalid)
                .unwrap_err()
                .stage,
            "runtime limits"
        );
    }
}
#[test]
fn wide_ready_visits_size_the_group_queue_and_core_batches() {
    for groups in [1, 256] {
        let baseline = startup_shard_limits(LogLimits::default(), groups, 16).unwrap();
        for selected in [256, 1024] {
            let limits = startup_shard_limits(LogLimits::default(), groups, selected).unwrap();
            assert_eq!(limits.visit_items, selected);
            let clients = startup_client_limits(selected);
            assert_eq!(clients.group_requests, (4 * selected).min(clients.requests));
            assert_eq!(
                limits.group_items,
                baseline
                    .group_items
                    .max(clients.group_requests + baseline.group_control_items)
            );
            assert!(limits.group_items >= selected + baseline.group_control_items);
            assert_eq!(limits.max_items, baseline.max_items);
            assert_eq!(limits.visit_bytes, baseline.visit_bytes);
            let batch = crate::raft::BatchLimits::for_ready_visit(selected).unwrap();
            assert_eq!(batch.max_proposal_entries, selected);
            assert_eq!(batch.max_replication_entries, selected);
        }
    }
    for selected in [1, 16] {
        assert_eq!(
            startup_client_limits(selected).group_requests,
            ClientRouterLimits::default().group_requests
        );
    }
    for selected in [1, 16, 64] {
        assert_eq!(
            crate::raft::BatchLimits::for_ready_visit(selected).unwrap(),
            crate::raft::BatchLimits::default()
        );
    }
}
