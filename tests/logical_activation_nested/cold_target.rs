// SPDX-License-Identifier: RPL-1.5
// Copyright (c) 2026 Karl Livesey
//! Target-only cold reconstruction from durable import, never the lost image.
use super::harness::{self, Observed};
use super::{
    cluster::{ChildNode, Cluster},
    lower::Lower,
    setup, Middle,
};
use voteboat::{
    application::{ReadableStateMachine, StateMachine},
    identity::OperationId,
    logical_voter::*,
    native::startup::NativeOpenMode,
};
type Bridge<'a> = LogicalBackingBridge<'a, ChildNode>;
pub fn recover<'a>(
    lower: &mut Lower<'_>,
    source: &mut [Middle],
    cluster: &'a mut Cluster,
    grant: LogicalLearnerEnrollment,
) -> (VerifiedLogicalImage, Bridge<'a>) {
    let original = source
        .iter()
        .map(super::controls::deferred)
        .collect::<Vec<_>>();
    assert!(original.iter().any(|b| !b.is_empty()));
    let (old_binding, expected, parent, suffix) = import_and_close(lower, source, cluster, grant);
    cluster.close();
    eprintln!("365 same3 original target native workers joined; reopening imported stores");
    cluster.open(NativeOpenMode::Recover);
    let mut bridge = Bridge::new(
        &mut cluster.nodes,
        setup::lower_mapping(3),
        cluster.representative,
    )
    .unwrap();
    let fresh = claim(lower, source, &mut bridge, 356262);
    assert!(fresh.binding().session > old_binding.session);
    assert_eq!(fresh.binding().identity, old_binding.identity);
    assert_eq!(fresh.image().parent, parent);
    assert!(fresh.claim().first_index > suffix);
    for node in 0..3 {
        bridge.inspect_backing(node, |p| {
            let core=p.owner.core(setup::bottom(3).group).unwrap();
            assert!(core.storage_binding().session.get() > 1);
            assert_eq!(record(&p.applications[&setup::bottom(3).group]), expected);
            assert!(core.state().snapshot.unwrap().index < suffix);
            assert!(matches!(&core.state().entry_at(suffix).unwrap().payload, voteboat::log::EntryPayload::Command { operation, .. } if operation.get()==356261));
        }).unwrap();
    }
    for (node, held) in source.iter().zip(original) {
        let current = super::controls::deferred(node);
        assert!(
            held.iter().all(|batch| current.contains(batch)),
            "same original deferred ticket/effect/operation/heap through target reopen"
        );
    }
    (fresh, bridge)
}
fn import_and_close(
    lower: &mut Lower<'_>,
    source: &mut [Middle],
    cluster: &mut Cluster,
    grant: LogicalLearnerEnrollment,
) -> (
    voteboat::identity::StoreBinding,
    LogicalEnrollmentRecord,
    voteboat::log::GroupLog,
    u64,
) {
    let mut bridge = Bridge::new(
        &mut cluster.nodes,
        setup::lower_mapping(3),
        cluster.representative,
    )
    .unwrap();
    bridge
        .begin_enrollment(OperationId::new(356200).unwrap(), grant)
        .unwrap_or_else(|e| panic!("original import {:?}", e.reason));
    let imported = image(lower, source, &mut bridge);
    let expected = bridge
        .inspect_backing(0, |p| record(&p.applications[&setup::bottom(3).group]))
        .unwrap();
    assert_eq!(expected.operation.get(), 356200);
    let old_binding = imported.binding();
    let parent = imported.image().parent.clone();
    drop(imported);
    close_bridge(lower, source, bridge);
    cluster.checkpoint();
    let mut bridge = Bridge::new(
        &mut cluster.nodes,
        setup::lower_mapping(3),
        cluster.representative,
    )
    .unwrap();
    let suffix_image = claim(lower, source, &mut bridge, 356261);
    let suffix = suffix_image.claim().first_index;
    assert_eq!(suffix_image.image().parent, parent);
    drop(suffix_image);
    close_bridge(lower, source, bridge);
    (old_binding, expected, parent, suffix)
}
fn record(app: &LogicalApplication) -> LogicalEnrollmentRecord {
    let LogicalRead::Enrollment(view) = app
        .read_at(app.applied_index(), LogicalQuery::Enrollment)
        .unwrap()
    else {
        panic!("actual V2 materialization")
    };
    *view.record.unwrap()
}
fn claim(
    lower: &mut Lower<'_>,
    source: &mut [Middle],
    bridge: &mut Bridge<'_>,
    operation: u128,
) -> VerifiedLogicalImage {
    bridge
        .begin_claim(OperationId::new(operation).unwrap())
        .unwrap();
    image(lower, source, bridge)
}
fn image(
    lower: &mut Lower<'_>,
    source: &mut [Middle],
    bridge: &mut Bridge<'_>,
) -> VerifiedLogicalImage {
    let end = harness::Wait::new(&lower.clock, harness::LIVENESS_MS);
    loop {
        lower.middle_with(source, Observed::default().bridge(bridge));
        bridge.advance(lower.now(), Lower::budget()).unwrap();
        if let Some(image) = bridge.take_image() {
            return image;
        }
        assert!(
            end.within(&lower.clock),
            "actual imported candidate fresh Claim/Enrollment/Image/Snapshot"
        );
    }
}
fn close_bridge(lower: &mut Lower<'_>, source: &mut [Middle], mut bridge: Bridge<'_>) {
    bridge.close();
    let end = harness::Wait::new(&lower.clock, harness::LIVENESS_MS);
    while !bridge.is_drained() {
        lower.middle_with(source, Observed::default().bridge(&bridge));
        bridge.advance(lower.now(), Lower::budget()).unwrap();
        assert!(
            end.within(&lower.clock),
            "same actual target original observation drain"
        );
    }
}
