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
fn worker_refusal_precedes_credit_and_invalid_accepted_ticket_is_drained_without_success() {
    let mut f = Fixture::new(1);
    f.worker.close();
    assert_eq!(
        f.router
            .export_checkpoint(&mut f.owner, &mut f.worker, &f.app, group(1)),
        Err(SnapshotRouteError::Worker(SnapshotWorkError::Closed))
    );
    assert_eq!(f.router.usage(), SnapshotRouterUsage::default());
    assert!(f.worker.is_drained());
    let mut f = Fixture::new(1);
    f.worker.bad_ticket = true;
    assert_eq!(
        f.router
            .export_checkpoint(&mut f.owner, &mut f.worker, &f.app, group(1)),
        Err(SnapshotRouteError::ProviderContract)
    );
    assert!(f.owner.is_failed());
    assert_eq!(f.router.usage().requests, 1);
    assert!(!f.worker.is_drained());
    assert_eq!(
        f.router
            .drain_failed_exports(&mut f.owner, &mut f.worker, 1),
        Ok(1)
    );
    assert!(f.worker.is_drained());
    assert!(f.router.is_drained());
    assert!(f.router.poll_export().is_none());
}
#[test]
fn wrong_owner_or_worker_preserves_original_selected_instance() {
    let mut f = Fixture::new(1);
    let mut other = Fixture::new(2);
    assert_eq!(
        f.router
            .export_checkpoint(&mut other.owner, &mut f.worker, &f.app, group(1)),
        Err(SnapshotRouteError::WrongOwner)
    );
    assert_eq!(
        f.router
            .export_checkpoint(&mut f.owner, &mut other.worker, &f.app, group(1)),
        Err(SnapshotRouteError::WrongWorker)
    );
    assert!(f.router.is_drained());
    assert!(f.worker.is_drained());
    assert!(other.worker.is_drained());
    f.admit();
    let event = f.event();
    f.deliver(event).unwrap();
    f.complete();
    assert!(f.router.is_drained());
}
