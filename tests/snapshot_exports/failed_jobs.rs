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
fn failed_export_drain_consumes_the_other_original_snapshot_job_exactly_once() {
    for discard_first in [false, true] {
        let mut f = Fixture::with_groups(1, true);
        f.admit();
        let event = f.event();
        f.deliver(event).unwrap();
        let original = f.router.poll_export().unwrap();
        f.owner.admit(group(2), Event::Checkpoint).unwrap();
        f.owner.advance(MonoTime(0), 1).unwrap();
        let lease = f.owner.take_effect().unwrap().unwrap();
        assert!(matches!(lease.effect, Effect::CheckpointRequired { .. }));
        f.router
            .submit(&mut f.owner, &mut f.worker, lease, &f.app)
            .unwrap();
        assert_eq!(f.router.usage().requests, 2);
        let mut clients = ClientRouter::<CounterReceipt>::new(
            ClientRouterBinding::new(
                f.owner.authorization(),
                ClientRouterGeneration::new(1).unwrap(),
            ),
            ClientRouterLimits::default(),
        )
        .unwrap();
        clients.abort(&mut f.owner).unwrap();
        if discard_first {
            f.router.discard_failed(&mut f.owner).unwrap();
        }
        let held = f.router.usage();
        assert_eq!(held.requests, 2);
        assert_eq!(
            f.router
                .drain_failed_exports(&mut f.owner, &mut f.worker, 0),
            Ok(0)
        );
        assert_eq!(f.router.usage(), held);
        assert!(!f.worker.is_drained());
        assert_eq!(
            f.router
                .drain_failed_exports(&mut f.owner, &mut f.worker, 1),
            Ok(1)
        );
        assert!(f.worker.is_drained());
        assert_eq!(f.router.usage().requests, 1);
        assert_eq!(f.owner.usage(), EffectOwnerUsage::default());
        assert!(f.owner.is_failed());
        assert!(matches!(
            f.router.complete_export(original).unwrap(),
            SnapshotExportOutcome::Loaded(_)
        ));
        assert!(f.router.is_drained());
        assert_eq!(
            f.router
                .drain_failed_exports(&mut f.owner, &mut f.worker, 1),
            Ok(0)
        );
        assert_eq!(f.router.usage(), SnapshotRouterUsage::default());
    }
}
#[test]
fn failed_only_consumer_refuses_a_live_owner_without_polling_or_releasing_work() {
    let mut f = Fixture::new(1);
    f.admit();
    let held = f.router.usage();
    assert_eq!(
        f.router
            .drain_failed_exports(&mut f.owner, &mut f.worker, 1),
        Err(SnapshotRouteError::ProviderContract)
    );
    assert_eq!(f.router.usage(), held);
    assert!(!f.worker.is_drained());
    let event = f.event();
    f.deliver(event).unwrap();
    f.complete();
    assert!(f.router.is_drained());
}

#[test]
fn export_observation_drain_is_independent_of_healthy_ordinary_snapshot_work() {
    let mut f = Fixture::with_groups(1, true);
    f.owner.admit(group(2), Event::Checkpoint).unwrap();
    f.owner.advance(MonoTime(0), 1).unwrap();
    let lease = f.owner.take_effect().unwrap().unwrap();
    assert!(matches!(lease.effect, Effect::CheckpointRequired { .. }));
    f.router
        .submit(&mut f.owner, &mut f.worker, lease, &f.app)
        .unwrap();
    assert!(!f.router.is_drained());
    assert!(f.router.checkpoint_exports_are_drained());
    assert!(!f.worker.is_drained());
    let held = f.router.usage();
    assert!(f.router.checkpoint_exports_are_drained());
    assert_eq!(f.router.usage(), held);
    let mut clients = ClientRouter::<CounterReceipt>::new(
        ClientRouterBinding::new(
            f.owner.authorization(),
            ClientRouterGeneration::new(1).unwrap(),
        ),
        ClientRouterLimits::default(),
    )
    .unwrap();
    clients.abort(&mut f.owner).unwrap();
    assert_eq!(
        f.router
            .drain_failed_exports(&mut f.owner, &mut f.worker, 1),
        Ok(1)
    );
    assert!(f.router.is_drained());
    assert!(f.router.checkpoint_exports_are_drained());
    assert_eq!(f.owner.usage(), EffectOwnerUsage::default());
}
