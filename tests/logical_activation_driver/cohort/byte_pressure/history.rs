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
//! Actual distinct-child replacement while four ordinary outputs stay exported.
use super::*;
#[test]
fn full_child_replacement_retains_original_client_byte_and_read_count_pressure_tcp() {
    history(NativePeerProtocol::TcpTls);
}
#[cfg(feature = "quic")]
#[test]
fn full_child_replacement_retains_original_client_byte_and_read_count_pressure_quic() {
    history(NativePeerProtocol::Quic);
}
fn history(protocol: NativePeerProtocol) {
    let _serial = crate::history_guard();
    let clock = &harness::SimClock::default();
    eprintln!("338 stage=actual_native_bootstraps");
    let mut target_members = members::Members::new(protocol, 0, clock);
    let mut own_members = members::Members::new(protocol, 1, clock);
    let mut sibling_members = members::Members::new(protocol, 2, clock);
    let mut target = bridge(&mut target_members, 0);
    let mut own = bridge(&mut own_members, 1);
    let mut sibling = bridge(&mut sibling_members, 2);
    let images = [
        drive::claim_in(&mut target, clock, 33501, false),
        drive::claim_in(&mut own, clock, 33502, false),
        drive::claim_in(&mut sibling, clock, 33503, false),
    ];
    let representative = images[0].representative();
    let (channels, [left, mut parent, right]) = construction::parents(&images, protocol, clock);
    let mut peers = progress::Peers {
        parents: [left, right],
        bridges: [own, sibling],
        clock: clock.clone(),
    };
    peers.elect(&mut parent, &mut target);
    eprintln!("338 stage=original_business_outputs_and_pressure");
    let held = holding::Held::new(&mut peers, &mut parent, &mut target);
    held.refused(&mut parent);
    held.assert_retained(&parent);
    let original = super::super::super::history::intent(335_400);
    stopped_reclaim(&mut parent, &original, &held);
    let mut driver = ChildActivationDriver::new(
        &mut parent,
        original.clone(),
        ChildActivationDriverLimits::default(),
    )
    .unwrap_or_else(|e| panic!("held ordinary admission {:?}", e.reason));
    eprintln!("338 stage=original_begin_status_learners");
    assertions::phase(
        &mut driver,
        &mut peers,
        &mut target,
        ChildActivationDriverPhase::Enrolling,
    );
    let first = driver.original_begin_position().unwrap();
    let snapshot = export::snapshot(&mut driver, &mut peers, &mut target);
    assert_eq!(
        snapshot.metadata.membership.as_ref().unwrap().id(),
        original.learners_configuration()
    );
    assertions::phase(
        &mut driver,
        &mut peers,
        &mut target,
        ChildActivationDriverPhase::Enrolling,
    );
    let (continuation, _) = driver
        .detach_child(target)
        .unwrap_or_else(|_| panic!("original target detach"));
    eprintln!("338 stage=original_enrollment_joint_final_outcome_release");
    target_members.enroll(&snapshot);
    let mut target = driver
        .attach_enrolled_child(continuation, &mut target_members.nodes)
        .unwrap_or_else(|e| panic!("actual fourth attach {:?}", e.reason));
    assertions::phase(
        &mut driver,
        &mut peers,
        &mut target,
        ChildActivationDriverPhase::Released,
    );
    assert_eq!(driver.original_begin_position(), Some(first));
    let node = driver
        .try_reclaim()
        .unwrap_or_else(|_| panic!("own observations drained with original host outputs held"));
    settle(node, &mut peers, &mut target, &original);
    assertions::terminal(node, &peers, &target, &original, first, representative);
    held.assert_retained(node);
    held.refused(node);
    eprintln!("338 stage=original_outputs_completed_and_business_after");
    held.complete(node);
    business::requests(&mut peers, &mut parent, &mut target, 2, 5, 12);
    let [left, right] = cleanup::close(peers, &mut parent, &mut target);
    drop(target);
    let middle = parent
        .into_parts()
        .unwrap_or_else(|_| panic!("actual parent drain"));
    channels.close(
        left.sessions
            .into_values()
            .chain(middle.sessions.into_values())
            .chain(right.sessions.into_values()),
    );
    target_members.finish();
    own_members.finish();
    sibling_members.finish();
}
fn stopped_reclaim(
    parent: &mut fixture::FacadeNode,
    intent: &ChildActivationIntent,
    held: &holding::Held,
) {
    let mut driver = ChildActivationDriver::new(
        parent,
        intent.clone(),
        ChildActivationDriverLimits::default(),
    )
    .unwrap_or_else(|e| panic!("actual clear observer stream {:?}", e.reason));
    driver.stop_waiting().unwrap();
    assert!(driver.is_drained());
    let parent = driver
        .try_reclaim()
        .unwrap_or_else(|_| panic!("original borrow not returned"));
    held.assert_retained(parent);
}
pub(super) fn bridge(members: &mut members::Members, slot: usize) -> aliases::Bridge<'_> {
    LogicalChildBridge::new(
        &mut members.nodes,
        groups::mapping(slot),
        support::node(groups::ids(slot)[0]),
    )
    .unwrap()
}
fn settle(
    parent: &mut fixture::FacadeNode,
    peers: &mut progress::Peers<'_>,
    target: &mut aliases::Bridge<'_>,
    intent: &ChildActivationIntent,
) {
    use voteboat::application::StateMachine;
    let end = harness::Wait::new(&peers.clock, harness::LIVENESS_MS);
    loop {
        peers.initial(parent, target);
        let status = parent.local().applications[&intent.mapping().parent]
            .status(intent.operations().begin)
            .unwrap();
        let final_index = status.child_final.unwrap().index;
        let parents = peers.parents.iter().all(|p| {
            p.local().applications[&intent.mapping().parent].status(intent.operations().begin)
                == Some(status.clone())
        });
        let child = [0, 1, 3].into_iter().all(|i| {
            target
                .inspect_backing(i, |l| {
                    l.owner
                        .core(intent.mapping().child)
                        .unwrap()
                        .membership()
                        .id()
                        == intent.target_configuration()
                        && l.applications[&intent.mapping().child].applied_index() >= final_index
                })
                .unwrap()
        });
        if parents && child {
            return;
        }
        assert!(
            end.within(&peers.clock),
            "actual full pressured convergence"
        );
    }
}
