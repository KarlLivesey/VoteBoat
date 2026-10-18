// SPDX-License-Identifier: RPL-1.5
// Copyright (c) 2026 Karl Livesey
//! Actual held source export refusal conserves the original owner and bytes.
use super::*;
use voteboat::{
    application::CheckpointStateMachine, log_format::NativeLogCodec, secure::PeerIdentity,
    snapshot_format::NativeSnapshotCodec, wire_format::NativeWireCodec,
};
#[test]
fn source_enrollment_without_original_authorization_returns_same_live_export_tcp() {
    history(NativePeerProtocol::TcpTls);
}
#[cfg(feature = "quic")]
#[test]
fn source_enrollment_without_original_authorization_returns_same_live_export_quic() {
    history(NativePeerProtocol::Quic);
}
fn history(protocol: NativePeerProtocol) {
    let _serial = crate::history_guard();
    let mut parent_stores = member::Member::new(protocol);
    let mut source_stores = member::Member::new(protocol);
    let mut parent_bridge =
        LogicalChildBridge::new(&mut parent_stores.nodes, setup::mapping(), support::node(1))
            .unwrap();
    let mut source_bridge =
        LogicalChildBridge::new(&mut source_stores.nodes, setup::mapping(), support::node(1))
            .unwrap();
    let parent_image = drive::claim_in(&mut parent_bridge, &parent_stores.clock, 351000, false);
    let source_image = drive::claim_in(&mut source_bridge, &source_stores.clock, 351100, false);
    let mut parent = fixture::FacadeNode::from_parts(
        &parent_image,
        fixture::build(&parent_image),
        fixture::limits(),
        MonoTime(0),
    )
    .unwrap_or_else(|r| panic!("{:?}", r.reason));
    let mut source = fixture::FacadeNode::from_parts(
        &source_image,
        fixture::build(&source_image),
        fixture::limits(),
        MonoTime(0),
    )
    .unwrap_or_else(|r| panic!("{:?}", r.reason));
    let output = export(&mut source_bridge, &mut source, &source_stores.clock);
    refuse(&mut parent, &mut source, &mut source_bridge, output);
    source.close_intake().unwrap();
    progress::drain(&mut source_bridge, &mut source, &source_stores.clock);
    parent.close_intake().unwrap();
    progress::drain(&mut parent_bridge, &mut parent, &parent_stores.clock);
    drop(
        source
            .into_parts()
            .unwrap_or_else(|_| panic!("source drain")),
    );
    drop(
        parent
            .into_parts()
            .unwrap_or_else(|_| panic!("parent drain")),
    );
    source_bridge.close();
    parent_bridge.close();
    assert!(source_bridge.is_drained() && parent_bridge.is_drained());
    drop(source_bridge);
    drop(parent_bridge);
    source_stores.finish();
    parent_stores.finish();
}
fn refuse(
    parent: &mut fixture::FacadeNode,
    source: &mut fixture::FacadeNode,
    bridge: &mut aliases::Bridge<'_>,
    output: SnapshotExportCompletion,
) {
    let ticket = output.ticket();
    let SnapshotExportOutcome::Loaded(snapshot) = output.outcome() else {
        panic!("original live image")
    };
    let pointer = snapshot.application.as_ptr();
    let held = bridge
        .inspect_backing(0, |p| p.snapshots.as_ref().unwrap().router.usage())
        .unwrap();
    let mut driver = ChildActivationDriver::new(
        parent,
        history::intent(351200),
        ChildActivationDriverLimits::default(),
    )
    .unwrap_or_else(|r| panic!("{:?}", r.reason));
    let rejected = driver
        .enroll_logical_learner(source, &candidate(), output)
        .unwrap_err();
    assert_eq!(rejected.reason, ChildActivationDriverError::WrongBinding);
    let LogicalEnrollmentExport::Original(output) = rejected.export else {
        panic!("must retain opaque original")
    };
    assert_eq!(output.ticket(), ticket);
    let SnapshotExportOutcome::Loaded(snapshot) = output.outcome() else {
        panic!("same original image")
    };
    assert_eq!(snapshot.application.as_ptr(), pointer);
    assert_eq!(
        bridge
            .inspect_backing(0, |p| p.snapshots.as_ref().unwrap().router.usage())
            .unwrap(),
        held
    );
    assert!(!bridge.is_drained());
    assert!(bridge.poll_checkpoint_export_backing(0).is_none());
    assert!(matches!(
        bridge
            .complete_checkpoint_export_backing(0, *output)
            .unwrap(),
        SnapshotExportOutcome::Loaded(_)
    ));
    assert!(bridge
        .inspect_backing(0, |p| p.snapshots.as_ref().unwrap().router.is_drained())
        .unwrap());
    driver
        .try_reclaim()
        .unwrap_or_else(|_| panic!("original unused parent observation"));
}
fn export(
    bridge: &mut aliases::Bridge<'_>,
    node: &mut fixture::FacadeNode,
    clock: &harness::SimClock,
) -> SnapshotExportCompletion {
    bridge.checkpoint_backing(0).unwrap();
    let end = harness::hang_guard();
    while !bridge
        .inspect_backing(0, |p| {
            p.owner
                .core(setup::mapping().child)
                .unwrap()
                .state()
                .snapshot
                .is_some()
        })
        .unwrap()
    {
        progress::step(bridge, node, clock, 4);
        assert!(Instant::now() < end, "original backing checkpoint");
    }
    let ticket = loop {
        match bridge.export_checkpoint_backing(0) {
            Ok(ticket) => break ticket,
            Err(SnapshotRouteError::Overloaded) => {
                progress::step(bridge, node, clock, 4);
                assert!(Instant::now() < end, "original checkpoint ownership drain");
            }
            error => panic!("original export admission {error:?}"),
        }
    };
    loop {
        progress::step(bridge, node, clock, 4);
        if let Some(output) = bridge.poll_checkpoint_export_backing(0) {
            assert_eq!(output.ticket(), ticket);
            return output;
        }
        assert!(Instant::now() < end, "original backing load");
    }
}
fn candidate() -> LogicalApplication {
    let mut limits = setup::limits();
    limits.control_action_bytes = 64 * 1024;
    let target = PeerIdentity {
        node: support::node(4),
        store: support::identity(4),
    };
    LogicalApplication::new_enrollment_candidate(
        LogicalStoreMapping::new(
            setup::mapping().child,
            target.node,
            target.store,
            support::group(804),
        )
        .unwrap(),
        voteboat::log::Bootstrap {
            group: support::group(804),
            ..setup::child()
        },
        setup::child(),
        limits,
        (
            NativeLogCodec,
            NativeSnapshotCodec,
            NativeWireCodec::new(voteboat::wire::WireLimits::default()).unwrap(),
        ),
        LogicalEnrollmentPlan {
            parent: setup::mapping().child,
            target,
            operation: OperationId::new(351201).unwrap(),
            expected: ConfigurationId::new(1).unwrap(),
            configuration: ConfigurationId::new(2).unwrap(),
            source_application_schema: fixture::app().schema_version(),
        },
    )
    .unwrap()
}
