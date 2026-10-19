// SPDX-License-Identifier: RPL-1.5
// Copyright (c) 2026 Karl Livesey
use super::harness;
use super::{lower::Lower, progress, setup, Middle, Top, UpperBridge};
use voteboat::{
    application::CheckpointStateMachine, logical_voter::*, outbound::OutboundQueue, runtime::*,
};
pub fn export(
    lower: &mut Lower<'_>,
    bridge: &mut UpperBridge<'_>,
    driver: &mut ChildActivationDriver<'_, Top>,
) -> SnapshotExportCompletion {
    export_for(
        lower,
        bridge,
        driver,
        3,
        setup::intent().learners_configuration(),
    )
}
pub fn export_for(
    lower: &mut Lower<'_>,
    bridge: &mut UpperBridge<'_>,
    driver: &mut ChildActivationDriver<'_, Top>,
    count: usize,
    configuration: voteboat::identity::ConfigurationId,
) -> SnapshotExportCompletion {
    let frontier = bridge
        .inspect_backing(0, |p| {
            p.owner
                .core(setup::middle().group)
                .unwrap()
                .state()
                .commit_index
        })
        .unwrap();
    bridge.checkpoint_backing(0).unwrap();
    let end = harness::Wait::new(&lower.clock, harness::LIVENESS_MS);
    while !bridge
        .inspect_backing(0, |p| {
            p.owner
                .core(setup::middle().group)
                .unwrap()
                .state()
                .snapshot
                .is_some_and(|reference| reference.index >= frontier)
        })
        .unwrap()
    {
        progress::driver_step(lower, bridge, driver, count);
        assert!(end.within(&lower.clock), "actual source checkpoint");
    }
    let ticket = loop {
        match bridge.export_checkpoint_backing(0) {
            Ok(ticket) => break ticket,
            Err(SnapshotRouteError::Overloaded) => {
                progress::driver_step(lower, bridge, driver, count)
            }
            error => panic!("original source export {error:?}"),
        }
        assert!(end.within(&lower.clock), "original export admission");
    };
    loop {
        progress::driver_step(lower, bridge, driver, count);
        if let Some(output) = bridge.poll_checkpoint_export_backing(0) {
            assert_eq!(output.ticket(), ticket);
            let SnapshotExportOutcome::Loaded(snapshot) = output.outcome() else {
                panic!("original loaded image");
            };
            assert_eq!(snapshot.metadata.application_schema, 1);
            assert_eq!(
                snapshot.metadata.membership.as_ref().unwrap().id(),
                configuration
            );
            return output;
        }
        assert!(end.within(&lower.clock), "original source export output");
    }
}
pub fn detach<'a>(
    lower: &mut Lower<'_>,
    bridge: UpperBridge<'a>,
    driver: &mut ChildActivationDriver<'_, Top>,
) -> (LogicalActivationContinuation, &'a mut [Middle]) {
    detach_for(lower, bridge, driver, 3)
}
pub fn detach_for<'a>(
    lower: &mut Lower<'_>,
    mut bridge: UpperBridge<'a>,
    driver: &mut ChildActivationDriver<'_, Top>,
    count: usize,
) -> (LogicalActivationContinuation, &'a mut [Middle]) {
    let end = harness::Wait::new(&lower.clock, harness::LIVENESS_MS);
    loop {
        match driver.detach_child(bridge) {
            Ok(detached) => return detached,
            Err(original) => bridge = *original,
        }
        progress::driver_step(lower, &mut bridge, driver, count);
        assert!(
            end.within(&lower.clock),
            "original bridge observations handback"
        );
    }
}
pub fn target(
    lower: &mut Lower<'_>,
    nodes: &mut [Middle],
    grant: LogicalLearnerEnrollment,
) -> VerifiedLogicalImage {
    let target = &mut lower.bridges[3];
    target
        .begin_enrollment(voteboat::identity::OperationId::new(356200).unwrap(), grant)
        .unwrap_or_else(|e| panic!("actual target admission {:?}", e.reason));
    let end = harness::Wait::new(&lower.clock, harness::LIVENESS_MS);
    let before: Vec<_> = nodes
        .iter()
        .map(|n| n.local().outbound.peer_usage(super::support::node(204)))
        .collect();
    let original: Vec<_> = nodes.iter().map(super::controls::deferred).collect();
    assert!(
        original.iter().any(|batches| !batches.is_empty()),
        "actual pre-descriptor original proof-bearing batch"
    );
    loop {
        lower.middle(nodes);
        if let Some(image) = lower.bridges[3].take_image() {
            assert!(matches!(
                setup::lower_app(3).initialization(),
                LogicalInitialization::EnrollmentCandidate(_)
            ));
            assert_eq!(setup::lower_app(3).schema_version(), 2);
            assert_eq!(setup::upper_app().schema_version(), 1);
            assert_eq!(image.mapping(), setup::lower_mapping(3));
            for ((node, usage), original) in nodes.iter().zip(&before).zip(&original) {
                assert!(
                    node.local()
                        .outbound
                        .peer_usage(super::support::node(204))
                        .bytes
                        >= usage.bytes
                );
                assert!(node
                    .inspect_deferred()
                    .all(|b| b.ticket().peer == super::support::node(204)));
                let retained = super::controls::deferred(node);
                assert!(
                    original.iter().all(|batch| retained.contains(batch)),
                    "same original ticket/effect/operation/heap payload"
                );
            }
            return image;
        }
        assert!(
            end.within(&lower.clock),
            "actual candidate Applied and correlated fresh reads"
        );
    }
}
