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
use voteboat::contracts::StorageError;
fn policy() -> WalMaintenancePolicy {
    WalMaintenancePolicy {
        interval_ms: 10,
        retry_ms: 3,
        max_bytes: 1024,
    }
}
fn tick(n: &mut Boat, time: u64) {
    n.poll(MonoTime(time), NodePollBudget::default()).unwrap();
}
fn enabled() -> Boat {
    let mut p = parts(1, false);
    p.local.persistence.reclaim_supported = true;
    let mut n = boat(p);
    n.configure_wal_maintenance(Some(policy())).unwrap();
    n
}
#[test]
fn policy_is_optional_validated_before_work_and_clock_does_not_wrap() {
    let mut n = boat(parts(1, false));
    tick(&mut n, 100);
    assert_eq!(n.wal_maintenance(), &WalMaintenanceStatus::default());
    assert!(matches!(
        n.configure_wal_maintenance(Some(policy())),
        Err(NodeError::Replica(ReplicaError::Worker(
            WorkerError::Unsupported
        )))
    ));
    assert_eq!(n.wal_maintenance(), &WalMaintenanceStatus::default());
    let mut n = enabled();
    let before = n.wal_maintenance().clone();
    for bad in [
        WalMaintenancePolicy {
            interval_ms: 0,
            ..policy()
        },
        WalMaintenancePolicy {
            retry_ms: 0,
            ..policy()
        },
        WalMaintenancePolicy {
            max_bytes: 0,
            ..policy()
        },
        WalMaintenancePolicy {
            max_bytes: 4097,
            ..policy()
        },
    ] {
        assert_eq!(
            n.configure_wal_maintenance(Some(bad)),
            Err(NodeError::InvalidLimits)
        );
        assert_eq!(n.wal_maintenance(), &before);
    }
    assert_eq!(
        n.poll(
            MonoTime(1),
            NodePollBudget {
                replica: ReplicaPollBudget {
                    worker_events: 0,
                    ..Default::default()
                },
                ..Default::default()
            }
        )
        .unwrap_err(),
        NodeError::InvalidLimits
    );
    assert_eq!(n.wal_maintenance(), &before);
    tick(&mut n, 9);
    assert!(n.wal_maintenance().pending.is_none());
    assert_eq!(
        n.configure_wal_maintenance(Some(WalMaintenancePolicy {
            interval_ms: u64::MAX,
            ..policy()
        })),
        Err(NodeError::InvalidLimits)
    );
    assert_eq!(
        n.poll(MonoTime(8), Default::default()).unwrap_err(),
        NodeError::TimeWentBack
    );
    assert!(n.wal_maintenance().pending.is_none());
}
#[test]
fn deadlines_coalesce_and_manual_results_remain_owned() {
    let mut n = enabled();
    let manual = n.reclaim(1000).unwrap();
    tick(&mut n, 10);
    assert!(n.wal_maintenance().pending.is_none());
    assert_eq!(
        n.wal_maintenance().last_admission_error,
        Some(WorkerError::Overloaded)
    );
    assert_eq!(n.wal_maintenance().next_deadline, Some(MonoTime(13)));
    assert_eq!(n.poll_reclaim().unwrap().request, manual);
    tick(&mut n, 12);
    assert!(n.wal_maintenance().pending.is_none());
    tick(&mut n, 100);
    let automatic = n.wal_maintenance().pending.unwrap();
    assert!(n.poll_reclaim().is_none());
    assert!(n.reclaim(1024).is_err());
    assert!(n.configure_wal_maintenance(None).is_err());
    tick(&mut n, 100);
    assert_eq!(
        n.wal_maintenance()
            .last_completion
            .as_ref()
            .unwrap()
            .request,
        automatic
    );
    assert_eq!(n.wal_maintenance().next_deadline, Some(MonoTime(110)));
    tick(&mut n, 109);
    assert!(n.wal_maintenance().pending.is_none());
    n.configure_wal_maintenance(None).unwrap();
    tick(&mut n, 1000);
    assert!(n.wal_maintenance().pending.is_none());
    assert!(n.poll_reclaim().is_none());
    n.begin_shutdown();
    for _ in 0..100 {
        tick(&mut n, 1000);
        if n.is_drained() {
            break;
        }
    }
    assert!(n.is_drained());
}
#[test]
fn overload_and_storage_refusal_back_off_without_a_poll_loop() {
    let mut p = parts(1, false);
    p.local.persistence.reclaim_supported = true;
    p.local.persistence.reclaim_reject = Some(WorkerError::Overloaded);
    p.local.persistence.reclaim_error = Some(StorageError::Rejected("budget"));
    let mut n = boat(p);
    n.configure_wal_maintenance(Some(policy())).unwrap();
    tick(&mut n, 10);
    assert_eq!(n.wal_maintenance().next_deadline, Some(MonoTime(13)));
    tick(&mut n, 12);
    assert!(n.wal_maintenance().pending.is_none());
    tick(&mut n, 13);
    tick(&mut n, 14);
    assert_eq!(
        n.wal_maintenance().last_completion.as_ref().unwrap().result,
        Err(StorageError::Rejected("budget"))
    );
    assert_eq!(n.wal_maintenance().next_deadline, Some(MonoTime(17)));
    tick(&mut n, 16);
    assert!(n.wal_maintenance().pending.is_none());
    tick(&mut n, 17);
    tick(&mut n, 18);
    assert!(n
        .wal_maintenance()
        .last_completion
        .as_ref()
        .unwrap()
        .result
        .is_ok());
    assert_eq!(n.wal_maintenance().next_deadline, Some(MonoTime(28)));
}
#[test]
fn held_job_drains_on_shutdown_and_abort_retains_recovery_ticket() {
    for abort in [false, true] {
        let mut p = parts(1, false);
        p.local.persistence.reclaim_supported = true;
        let hold = p.local.persistence.reclaim_hold.clone();
        hold.set(true);
        let mut n = boat(p);
        n.configure_wal_maintenance(Some(policy())).unwrap();
        tick(&mut n, 10);
        let ticket = n.wal_maintenance().pending.unwrap();
        tick(&mut n, 100);
        assert_eq!(n.wal_maintenance().pending, Some(ticket));
        if abort {
            n.abort();
            let r = n.into_recovery().unwrap_or_else(|_| panic!("not failed"));
            assert_eq!(r.maintenance.pending, Some(ticket));
            assert_eq!(r.replica.usage().reclaims, 1);
        } else {
            n.begin_shutdown();
            assert_eq!(n.configure_wal_maintenance(None), Err(NodeError::Closed));
            tick(&mut n, 100);
            assert!(!n.is_drained());
            hold.set(false);
            tick(&mut n, 100);
            tick(&mut n, 100);
            assert!(n.is_drained());
            assert_eq!(
                n.wal_maintenance()
                    .last_completion
                    .as_ref()
                    .unwrap()
                    .request,
                ticket
            );
            assert!(n.poll_reclaim().is_none());
        }
    }
}
#[test]
fn fatal_completion_preserves_diagnostics_and_wrong_receipt_preserves_ownership() {
    for wrong in [false, true] {
        let mut p = parts(1, false);
        p.local.persistence.reclaim_supported = true;
        p.local.persistence.reclaim_wrong = wrong;
        p.local.persistence.reclaim_error = Some(StorageError::Uncertain("publication".into()));
        let mut n = boat(p);
        n.configure_wal_maintenance(Some(policy())).unwrap();
        tick(&mut n, 10);
        let ticket = n.wal_maintenance().pending.unwrap();
        assert!(n.poll(MonoTime(11), Default::default()).is_err());
        assert_eq!(n.state(), NodeState::RecoveryRequired);
        let r = n.into_recovery().unwrap_or_else(|_| panic!("not failed"));
        if wrong {
            assert_eq!(r.maintenance.pending, Some(ticket));
            assert!(r.replica.failed_reclaim().is_some());
        } else {
            assert_eq!(r.maintenance.last_completion.unwrap().request, ticket);
            assert!(r.maintenance.pending.is_none());
            assert!(r.maintenance.next_deadline.is_none());
        }
    }
}
#[test]
fn permanent_admission_refusal_stops_schedule_but_service_stays_running() {
    let mut p = parts(1, false);
    p.local.persistence.reclaim_supported = true;
    p.local.persistence.reclaim_reject = Some(WorkerError::Unsupported);
    let mut n = boat(p);
    n.configure_wal_maintenance(Some(policy())).unwrap();
    tick(&mut n, 10);
    tick(&mut n, 100);
    assert_eq!(n.state(), NodeState::Running);
    assert_eq!(
        n.wal_maintenance().last_admission_error,
        Some(WorkerError::Unsupported)
    );
    assert!(n.wal_maintenance().next_deadline.is_none());
    assert!(n.wal_maintenance().pending.is_none());
}
