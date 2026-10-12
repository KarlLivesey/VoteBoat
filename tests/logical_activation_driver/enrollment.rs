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
//! Original live child export and native fourth-learner enrollment.
//! This pilot has one logical parent; full three-parent activation is separate.
use super::*;
use voteboat::{application::StateMachine, snapshot::Snapshot};
#[test]
fn live_child_export_enrollment_joint_final_and_original_release_tcp() {
    history(NativePeerProtocol::TcpTls, false);
}
#[cfg(feature = "quic")]
#[test]
fn live_child_export_enrollment_joint_final_and_original_release_quic() {
    history(NativePeerProtocol::Quic, false);
}
#[test]
fn original_readiness_admission_stop_retains_authorization_until_dispatch_tcp() {
    history(NativePeerProtocol::TcpTls, true);
}
#[cfg(feature = "quic")]
#[test]
fn original_readiness_admission_stop_retains_authorization_until_dispatch_quic() {
    history(NativePeerProtocol::Quic, true);
}
fn history(protocol: NativePeerProtocol, stop: bool) {
    let _serial = cluster::HISTORY.lock().unwrap();
    let mut cohort = member::Member::new(protocol);
    let clock = cohort.clock;
    let mut bridge =
        LogicalChildBridge::new(&mut cohort.nodes, setup::mapping(), support::node(1)).unwrap();
    let image = drive::claim_in(&mut bridge, clock, 5201, false);
    let original_binding = image.representative();
    let mut parent = LogicalNode::from_parts(
        &image,
        fixture::build(&image),
        fixture::limits(),
        MonoTime(0),
    )
    .unwrap_or_else(|e| panic!("actual parent {:?}", e.reason));
    progress::elect(&mut bridge, &mut parent, clock);
    let original = super::history::intent(6200);
    let mut driver = ChildActivationDriver::new(
        &mut parent,
        original.clone(),
        ChildActivationDriverLimits::default(),
    )
    .unwrap_or_else(|e| panic!("actual driver {:?}", e.reason));
    phase(
        &mut driver,
        &mut bridge,
        clock,
        ChildActivationDriverPhase::Enrolling,
    );
    let first = driver.original_begin_position().unwrap();
    let snapshot = export(&mut driver, &mut bridge, clock);
    assert_eq!(
        snapshot.metadata.membership.as_ref().unwrap().id(),
        original.learners_configuration()
    );
    assert_eq!(
        snapshot
            .metadata
            .membership
            .as_ref()
            .unwrap()
            .stable()
            .learners()
            .get(&support::node(4)),
        Some(&support::identity(4))
    );
    // No serving/quorum assertion comes from the exported data. Real enrollment
    // validates/creates selected original stores; actual readiness comes later.
    phase(
        &mut driver,
        &mut bridge,
        clock,
        ChildActivationDriverPhase::Enrolling,
    );
    let (continuation, _) = driver
        .detach_child(bridge)
        .unwrap_or_else(|_| panic!("original drained continuation"));
    cohort.enroll(&snapshot);
    let mut bridge = driver
        .attach_enrolled_child(continuation, &mut cohort.nodes)
        .unwrap_or_else(|e| panic!("same original session attach {:?}", e.reason));
    let node = if stop {
        stop_original_readiness(driver, &mut bridge, clock)
    } else {
        phase(
            &mut driver,
            &mut bridge,
            clock,
            ChildActivationDriverPhase::Released,
        );
        assert_eq!(driver.original_begin_position(), Some(first));
        let node = driver
            .try_reclaim()
            .unwrap_or_else(|_| panic!("original terminal observations consumed"));
        assert_terminal(node, &bridge, &original, first, original_binding);
        node
    };
    node.close_intake().unwrap();
    progress::until(&mut bridge, node, clock, fixture::FacadeNode::is_drained);
    bridge.close();
    assert!(bridge.is_drained());
    drop(bridge);
    drop(
        parent
            .into_parts()
            .unwrap_or_else(|_| panic!("actual facade drain")),
    );
    cohort.finish();
}
fn phase(
    driver: &mut ChildActivationDriver<'_, fixture::FacadeNode>,
    bridge: &mut aliases::Bridge<'_>,
    clock: Instant,
    wanted: ChildActivationDriverPhase,
) {
    let end = Instant::now() + Duration::from_secs(15);
    loop {
        progress::child_step(driver, bridge, clock, 4);
        if driver.phase() == wanted && bridge.is_drained() {
            return;
        }
        assert!(
            Instant::now() < end,
            "actual child phase {:?} expected {:?}",
            driver.phase(),
            wanted
        );
    }
}
fn export(
    driver: &mut ChildActivationDriver<'_, fixture::FacadeNode>,
    bridge: &mut aliases::Bridge<'_>,
    clock: Instant,
) -> Snapshot {
    bridge.checkpoint_backing(0).unwrap();
    let end = Instant::now() + Duration::from_secs(15);
    loop {
        progress::child_step(driver, bridge, clock, 3);
        if bridge
            .inspect_backing(0, |local| {
                local
                    .owner
                    .core(setup::mapping().child)
                    .unwrap()
                    .state()
                    .snapshot
                    .is_some()
            })
            .unwrap()
        {
            break;
        }
        assert!(Instant::now() < end, "original durable checkpoint anchor");
    }
    let ticket = loop {
        match bridge.export_checkpoint_backing(0) {
            Ok(ticket) => break ticket,
            Err(SnapshotRouteError::Overloaded) => progress::child_step(driver, bridge, clock, 3),
            Err(reason) => panic!("original export admission {reason:?}"),
        }
        assert!(
            Instant::now() < end,
            "original maintenance before export admission"
        );
    };
    let output = loop {
        progress::child_step(driver, bridge, clock, 3);
        if let Some(output) = bridge.poll_checkpoint_export_backing(0) {
            break output;
        }
        assert!(Instant::now() < end, "original Node export result");
    };
    assert_eq!(output.ticket(), ticket);
    assert_eq!(
        bridge.cancel_checkpoint_export_backing(0, ticket),
        Err(SnapshotRouteError::StaleCompletion)
    );
    let rejected = bridge
        .complete_checkpoint_export_backing(1, output)
        .unwrap_err();
    assert_eq!(rejected.reason, SnapshotRouteError::WrongOwner);
    assert_eq!(rejected.completion.ticket(), ticket);
    match bridge
        .complete_checkpoint_export_backing(0, *rejected.completion)
        .unwrap()
    {
        SnapshotExportOutcome::Loaded(image) => image,
        SnapshotExportOutcome::Failed(reason) => panic!("original live export failed {reason:?}"),
    }
}

fn assert_child_final(
    bridge: &aliases::Bridge<'_>,
    original: &ChildActivationIntent,
    final_position: ActivationPosition,
    original_binding: RepresentativeBinding,
) {
    for index in [0, 1, 3] {
        bridge
            .inspect_backing(index, |local| {
                let c = local.owner.core(setup::mapping().child).unwrap();
                assert_eq!(c.membership().id(), original.target_configuration());
                assert!(c.membership().joint().is_none());
                assert!(c.membership().is_voter(support::node(4)));
                assert!(!c.membership().is_voter(support::node(3)));
                assert!(c.state().commit_index >= final_position.index);
                assert!(
                    local.applications[&setup::mapping().child].applied_index()
                        >= final_position.index
                );
                assert_eq!(
                    local.applications[&setup::mapping().child]
                        .metadata()
                        .representative,
                    Some(original_binding)
                );
            })
            .unwrap();
    }
}

fn assert_terminal(
    node: &fixture::FacadeNode,
    bridge: &aliases::Bridge<'_>,
    original: &ChildActivationIntent,
    first: ActivationPosition,
    binding: RepresentativeBinding,
) {
    let status = node.local().applications[&setup::mapping().parent]
        .status(original.operations().begin)
        .unwrap();
    assert_eq!(&status.intent, original);
    assert_eq!(status.begin.first, first);
    assert!(status.outcome.is_some());
    assert!(status.release.is_some());
    assert_child_final(bridge, original, status.child_final.unwrap(), binding);
}
fn stop_original_readiness<'p>(
    mut driver: ChildActivationDriver<'p, fixture::FacadeNode>,
    bridge: &mut aliases::Bridge<'_>,
    clock: Instant,
) -> &'p mut fixture::FacadeNode {
    let end = Instant::now() + Duration::from_secs(15);
    loop {
        progress::child_step(&mut driver, bridge, clock, 4);
        let usage = bridge.usage();
        if driver.phase() == ChildActivationDriverPhase::Enrolling
            && usage.actions == 0
            && usage.child_attempts == 1
        {
            break;
        }
        assert!(Instant::now() < end, "original readiness admission");
    }
    driver.stop_waiting_with_child(bridge).unwrap();
    progress::child_step(&mut driver, bridge, clock, 0);
    assert!(!driver.is_drained());
    assert!(!bridge.is_drained());
    assert_eq!(bridge.usage().child_attempts, 1);
    while !driver.is_drained() {
        progress::child_step(&mut driver, bridge, clock, 4);
        assert!(
            Instant::now() < end,
            "original readiness/cancel dispatch cleanup"
        );
    }
    assert_eq!(bridge.usage().child_attempts, 0);
    let node = driver
        .try_reclaim()
        .unwrap_or_else(|_| panic!("original readiness owning drain"));
    assert!(node.local().applications[&setup::mapping().parent]
        .active()
        .is_some());
    node
}
