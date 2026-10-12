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
#[test]
fn original_image_and_held_credits_release_only_through_exact_completion() {
    let mut f = Fixture::new(1);
    let before = f.owner.core(group(1)).unwrap().state().clone();
    let ticket = f.admit();
    let charge = f.router.usage();
    assert_eq!(charge.requests, 1);
    assert!(charge.image_bytes > 2 * ALLOWANCE);
    let event = f.event();
    f.deliver(event).unwrap();
    let output = f.router.poll_export().unwrap();
    assert_eq!(output.ticket(), ticket);
    assert_eq!(f.router.usage(), charge);
    assert_eq!(f.router.recovery_usage(), charge);
    assert!(!f.router.is_drained());
    assert!(!f.router.checkpoint_exports_are_drained());
    assert!(f.router.poll_export().is_none());
    assert_eq!(
        f.router
            .export_checkpoint(&mut f.owner, &mut f.worker, &f.app, group(1)),
        Err(SnapshotRouteError::Overloaded)
    );
    assert_eq!(
        f.router.cancel_export(ticket),
        Err(SnapshotRouteError::StaleCompletion)
    );
    let SnapshotExportOutcome::Loaded(image) = f.router.complete_export(output).unwrap() else {
        panic!("image");
    };
    assert_eq!(image, f.worker.image);
    assert_eq!(f.owner.core(group(1)).unwrap().state(), &before);
    assert!(f.router.is_drained());
    assert!(f.router.checkpoint_exports_are_drained());
    assert_eq!(f.router.usage(), SnapshotRouterUsage::default());
    assert_eq!(
        f.router.cancel_export(ticket),
        Err(SnapshotRouteError::StaleCompletion)
    );
}
#[test]
fn pending_cancel_and_close_retain_original_work_until_terminal_drain() {
    let mut f = Fixture::new(1);
    let ticket = f.admit();
    let charge = f.router.usage();
    f.router.cancel_export(ticket).unwrap();
    f.router.close_exports();
    assert_eq!(f.router.usage(), charge);
    assert!(!f.worker.is_drained());
    assert_eq!(
        f.router
            .export_checkpoint(&mut f.owner, &mut f.worker, &f.app, group(1)),
        Err(SnapshotRouteError::Closed)
    );
    let event = f.event();
    f.deliver(event).unwrap();
    assert!(f.router.poll_export().is_none());
    assert!(f.router.is_drained());
    assert!(f.worker.is_drained());
    let mut f = Fixture::new(1);
    let ticket = f.admit();
    let event = f.event();
    f.deliver(event).unwrap();
    f.router.cancel_export(ticket).unwrap();
    assert!(f.router.is_drained());
    assert!(f.router.poll_export().is_none());
}
#[test]
fn real_cross_instance_completion_refuses_without_releasing_either_scope() {
    let mut a = Fixture::new(1);
    let mut b = Fixture::new(2);
    b.worker.shared = a.worker.shared.clone();
    a.admit();
    b.admit();
    assert_eq!(a.worker.shared.get(), 2);
    a.worker.close();
    let event = a.event();
    a.deliver(event).unwrap();
    let original = a.router.poll_export().unwrap();
    let charge = b.router.usage();
    let rejected = b.router.complete_export(original).unwrap_err();
    assert_eq!(rejected.reason, SnapshotRouteError::StaleCompletion);
    assert_eq!(b.router.usage(), charge);
    a.router.complete_export(*rejected.completion).unwrap();
    assert!(a.router.is_drained());
    assert_eq!(b.worker.shared.get(), 1);
    let event = b.event();
    b.deliver(event).unwrap();
    b.complete();
    assert_eq!(b.worker.shared.get(), 0);
    assert!(b.router.is_drained());
}
#[test]
fn missing_anchor_unknown_group_and_recovery_budget_refuse_before_worker_admission() {
    let mut f = Fixture::new(1);
    assert_eq!(
        f.router
            .export_checkpoint(&mut f.owner, &mut f.worker, &f.app, group(99)),
        Err(SnapshotRouteError::UnknownGroup)
    );
    let (log, core, app) = committed(1);
    let mut owner = providers::owner(&log, core);
    assert_eq!(
        f.router
            .export_checkpoint(&mut owner, &mut f.worker, &app, group(1)),
        Err(SnapshotRouteError::MissingAnchor)
    );
    let mut tight = SnapshotRouter::new_with_recovery_limits(
        f.owner.identity(),
        f.worker.binding(),
        SnapshotRouterLimits {
            requests: 2,
            max_image_bytes: ALLOWANCE,
        },
        SnapshotRecoveryLimits {
            requests: 1,
            image_bytes: 2 * ALLOWANCE,
        },
    )
    .unwrap();
    assert_eq!(
        tight.export_checkpoint(&mut f.owner, &mut f.worker, &f.app, group(1)),
        Err(SnapshotRouteError::TooLarge)
    );
    assert!(f.worker.is_drained());
    assert!(tight.is_drained());
}
