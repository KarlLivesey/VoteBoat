// SPDX-License-Identifier: RPL-1.5
// Copyright (c) 2026 Karl Livesey
use super::*;

#[test]
fn same_group_export_backpressures_original_live_checkpoint_until_exact_consumption() {
    for exported in [false, true] {
        let mut f = Fixture::with_suffix(1);
        let ticket = f.admit();
        let output = if exported {
            let event = f.event();
            f.deliver(event).unwrap();
            Some(f.router.poll_export().unwrap())
        } else {
            None
        };
        f.owner.admit(group(1), Event::Checkpoint).unwrap();
        f.owner.advance(MonoTime(0), 1).unwrap();
        let lease = f.owner.take_effect().unwrap().unwrap();
        assert!(matches!(lease.effect, Effect::CheckpointRequired { .. }));
        let original = lease.ticket;
        let state = f.owner.core(group(1)).unwrap().state().clone();
        let router_usage = f.router.usage();
        let worker_usage = f.worker.usage();
        let rejected = f
            .router
            .submit(&mut f.owner, &mut f.worker, lease, &f.app)
            .unwrap_err();
        assert_eq!(rejected.reason, SnapshotRouteError::Overloaded);
        assert_eq!(rejected.lease.ticket, original);
        assert!(!f.owner.is_failed());
        assert_eq!(f.owner.usage().leased, 1);
        assert_eq!(f.owner.core(group(1)).unwrap().state(), &state);
        assert_eq!(f.router.usage(), router_usage);
        assert_eq!(f.worker.usage(), worker_usage);
        let output = output.unwrap_or_else(|| {
            let event = f.event();
            f.deliver(event).unwrap();
            f.router.poll_export().unwrap()
        });
        assert_eq!(output.ticket(), ticket);
        let SnapshotExportOutcome::Loaded(image) = f.router.complete_export(output).unwrap() else {
            panic!("original export required");
        };
        assert_eq!(image.metadata.index, 1);
        let admitted = f
            .router
            .submit(&mut f.owner, &mut f.worker, *rejected.lease, &f.app)
            .unwrap();
        assert!(admitted.sequence > ticket.request.sequence);
        assert_eq!(f.worker.pending.as_ref().unwrap().1.visit, original.visit);
        assert!(matches!(
            f.worker.pending.as_ref().unwrap().1.job,
            SnapshotJob::Publish { .. }
        ));
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
        assert!(f.worker.is_drained());
        assert!(f.router.is_drained());
        assert_eq!(f.owner.usage(), EffectOwnerUsage::default());
    }
}
