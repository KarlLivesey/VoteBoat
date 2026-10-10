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
#[test]
fn automatic_hundred_group_checkpoints_reclaim_and_reopen() {
    let root =
        std::env::temp_dir().join(format!("voteboat-auto-checkpoints-{}", std::process::id()));
    std::fs::create_dir_all(&root).unwrap();
    let mut nodes = facade_make(&root, false);
    for g in 1..=100 {
        nodes[0].control(group(g), NodeControl::Campaign).unwrap();
    }
    facade_drive(&mut nodes, |nodes| {
        nodes.iter().all(|n| {
            n.local()
                .applications
                .values()
                .all(|a| a.read_applied(a.applied_index()) == Ok(7))
        })
    });
    facade_proposals(&mut nodes, 2, 3, 10);
    let last_time = automatic_checkpoint_waves(&mut nodes);
    facade_close_at(nodes, last_time);
    let mut nodes = facade_make(&root, true);
    for g in 1..=100 {
        nodes[0].control(group(g), NodeControl::Campaign).unwrap();
    }
    facade_drive(&mut nodes, |nodes| {
        (1..=100).all(|g| nodes[0].local().owner.core(group(g)).unwrap().role() == Role::Leader)
    });
    facade_proposals(&mut nodes, 2, 3, 10);
    facade_proposals(&mut nodes, 3, 4, 14);
    facade_close(nodes);
    std::fs::remove_dir_all(root).unwrap();
}

fn automatic_checkpoint_waves(nodes: &mut [Facade]) -> MonoTime {
    for n in nodes.iter_mut() {
        n.configure_checkpoints(Some(CheckpointPolicy {
            min_entries: 1,
            interval_ms: 1,
            scan_groups: 8,
            max_in_flight: 4,
        }))
        .unwrap();
        n.configure_wal_maintenance(Some(WalMaintenancePolicy {
            interval_ms: 1,
            retry_ms: 1,
            max_bytes: LogLimits::default().max_wal_bytes,
        }))
        .unwrap();
    }
    let mut last_time = 0;
    let mut reclaimed = 0;
    for round in 1..=100 {
        last_time = round;
        facade_drive_at(nodes, MonoTime(round), |nodes| {
            nodes.iter().all(|n| {
                let local = n.local();
                n.checkpoints().pending.is_empty()
                    && local.owner.is_drained()
                    && n.replica_usage().leases() == 0
                    && local.persistence.is_drained()
                    && n.wal_maintenance().pending.is_none()
                    && local
                        .snapshots
                        .as_ref()
                        .is_some_and(|s| s.router.is_drained() && s.worker.is_drained())
            })
        });
        for n in nodes.iter() {
            let report = n
                .wal_maintenance()
                .last_completion
                .as_ref()
                .unwrap()
                .result
                .as_ref()
                .unwrap();
            reclaimed += report.before_bytes - report.after_bytes;
            assert!(n.checkpoints().last_admission_error.is_none());
        }
        if nodes.iter().all(|n| {
            n.local().owner.groups().all(|g| {
                n.local().owner.core(g).unwrap().state().base_index()
                    == n.local().applications[&g].applied_index()
            })
        }) {
            break;
        }
    }
    for n in nodes.iter() {
        assert!(n.local().owner.groups().all(|g| n
            .local()
            .owner
            .core(g)
            .unwrap()
            .state()
            .base_index()
            == n.local().applications[&g].applied_index()));
        assert!(matches!(
            n.checkpoints().last_result,
            Some(CheckpointResult::Completed { .. })
        ));
    }
    assert!(reclaimed > 0);
    MonoTime(last_time)
}
