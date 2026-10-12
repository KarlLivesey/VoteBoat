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
//! Actual nine original physical owners, distinct driver backing, real fourth.
use super::*;
#[test]
fn three_logical_parents_ten_native_owners_replace_distinct_child_tcp() {
    history(NativePeerProtocol::TcpTls);
}
#[cfg(feature = "quic")]
#[test]
fn three_logical_parents_ten_native_owners_replace_distinct_child_quic() {
    history(NativePeerProtocol::Quic);
}
fn history(protocol: NativePeerProtocol) {
    let _serial = cluster::HISTORY.lock().unwrap();
    let clock = Instant::now();
    eprintln!("332 stage=original_native_bootstraps");
    let mut target_members = members::Members::new(protocol, 0, clock);
    let mut own_members = members::Members::new(protocol, 1, clock);
    let mut sibling_members = members::Members::new(protocol, 2, clock);
    let mut target = bridge(&mut target_members, 0);
    let mut own = bridge(&mut own_members, 1);
    let mut sibling = bridge(&mut sibling_members, 2);
    let images = [
        drive::claim_in(&mut target, clock, 33201, false),
        drive::claim_in(&mut own, clock, 33202, false),
        drive::claim_in(&mut sibling, clock, 33203, false),
    ];
    let representative = images[0].representative();
    let now = MonoTime(clock.elapsed().as_millis() as u64);
    let (channels, [left, mut parent, right]) = assembly::parents(&images, protocol, now);
    let mut peers = progress::Peers {
        parents: [left, right],
        bridges: [own, sibling],
        clock,
    };
    eprintln!("332 stage=three_parent_election");
    peers.elect(&mut parent, &mut target);
    eprintln!("332 stage=original_business_before");
    business::requests(&mut peers, &mut parent, &mut target, 33301, 7, 7);
    let original = super::super::history::intent(33400);
    let mut driver = ChildActivationDriver::new(
        &mut parent,
        original.clone(),
        ChildActivationDriverLimits::default(),
    )
    .unwrap_or_else(|e| panic!("original distinct parent {:?}", e.reason));
    wrong_cell(&mut driver, &mut target, peers.now());
    assertions::phase(
        &mut driver,
        &mut peers,
        &mut target,
        ChildActivationDriverPhase::Enrolling,
    );
    let first = driver.original_begin_position().unwrap();
    eprintln!("332 stage=original_learners_checkpoint_export");
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
        .unwrap_or_else(|_| panic!("genuine target drain"));
    eprintln!("332 stage=original_fourth_enrollment");
    target_members.enroll(&snapshot);
    let mut target = driver
        .attach_enrolled_child(continuation, &mut target_members.nodes)
        .unwrap_or_else(|e| panic!("original distinct target attach {:?}", e.reason));
    assertions::phase(
        &mut driver,
        &mut peers,
        &mut target,
        ChildActivationDriverPhase::Released,
    );
    eprintln!("332 stage=original_parent_release");
    assert_eq!(driver.original_begin_position(), Some(first));
    let node = driver
        .try_reclaim()
        .unwrap_or_else(|_| panic!("original parent terminal observer drain"));
    settle(node, &mut peers, &mut target, &original);
    assertions::terminal(node, &peers, &target, &original, first, representative);
    eprintln!("332 stage=original_business_after");
    business::requests(&mut peers, &mut parent, &mut target, 33302, 5, 12);
    eprintln!("332 stage=original_independent_cleanup");
    let [left, right] = cleanup::close(peers, &mut parent, &mut target);
    drop(target);
    let middle = parent
        .into_parts()
        .unwrap_or_else(|_| panic!("original driver parent drain"));
    channels.close(
        left.sessions
            .into_values()
            .chain(middle.sessions.into_values())
            .chain(right.sessions.into_values()),
        MonoTime(clock.elapsed().as_millis() as u64),
    );
    target_members.finish();
    own_members.finish();
    sibling_members.finish();
}
fn settle(
    parent: &mut fixture::FacadeNode,
    peers: &mut progress::Peers<'_>,
    target: &mut aliases::Bridge<'_>,
    intent: &ChildActivationIntent,
) {
    use voteboat::application::StateMachine;
    let end = Instant::now() + Duration::from_secs(15);
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
            Instant::now() < end,
            "actual terminal parent/child convergence"
        );
    }
}

fn bridge(members: &mut members::Members, slot: usize) -> aliases::Bridge<'_> {
    LogicalChildBridge::new(
        &mut members.nodes,
        groups::mapping(slot),
        support::node(groups::ids(slot)[0]),
    )
    .unwrap()
}
fn wrong_cell(
    driver: &mut ChildActivationDriver<'_, fixture::FacadeNode>,
    target: &mut aliases::Bridge<'_>,
    now: MonoTime,
) {
    let usage = target.usage();
    assert_eq!(
        driver.advance_with_child(
            now,
            ChildActivationDriverBudget::default(),
            fixture::budget(4),
            target
        ),
        Err(ChildActivationDriverError::WrongBinding)
    );
    assert_eq!(target.usage(), usage);
}
