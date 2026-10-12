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
//! Real retained Learners stage refuses promotion without a bound learner.
use super::*;
use aliases::Bridge;
use voteboat::{application::StateMachine, log::EntryPayload};
#[test]
fn real_parent_intent_replicates_learners_but_cannot_promote_absent_native_tcp() {
    history(NativePeerProtocol::TcpTls);
}
#[cfg(feature = "quic")]
#[test]
fn real_parent_intent_replicates_learners_but_cannot_promote_absent_native_quic() {
    history(NativePeerProtocol::Quic);
}
fn history(protocol: NativePeerProtocol) {
    let _serial = cluster::HISTORY.lock().unwrap();
    let mut cohort = super::member::Member::new(protocol);
    let clock = cohort.clock;
    let mut bridge =
        LogicalChildBridge::new(&mut cohort.nodes, setup::mapping(), support::node(1)).unwrap();
    let image = drive::claim_in(&mut bridge, clock, 5001, false);
    let mut parent = LogicalNode::from_parts(
        &image,
        fixture::build(&image),
        fixture::limits(),
        MonoTime(0),
    )
    .unwrap_or_else(|e| panic!("actual parent {:?}", e.reason));
    progress::elect(&mut bridge, &mut parent, clock);
    let original = super::history::intent(6000);
    let mut driver = ChildActivationDriver::new(
        &mut parent,
        original.clone(),
        ChildActivationDriverLimits::default(),
    )
    .unwrap_or_else(|e| panic!("real driver {:?}", e.reason));
    let end = Instant::now() + Duration::from_secs(15);
    loop {
        step(&mut driver, &mut bridge, clock);
        if driver.phase() == ChildActivationDriverPhase::Enrolling {
            break;
        }
        assert!(
            Instant::now() < end,
            "original child Learners progress: {:?}",
            driver.phase()
        );
    }
    for _ in 0..32 {
        step(&mut driver, &mut bridge, clock);
    }
    assert!(driver.original_begin_position().is_some());
    for node in 0..3 {
        bridge.inspect_backing(node, |local| {
            let c = local.owner.core(setup::mapping().child).unwrap();
            assert_eq!(c.membership().id(), original.learners_configuration());
            assert!(c.membership().joint().is_none());
            assert_eq!(c.membership().stable().learners().get(&support::node(4)), Some(&support::identity(4)));
            assert!(!c.membership().is_voter(support::node(4)));
            assert!(c.ready_learner().is_none());
            let assignment = c.state().entries.iter().find(|e| matches!(&e.payload, EntryPayload::Configuration(r) if r.operation == original.operations().learners)).unwrap();
            assert!(assignment.index <= c.state().commit_index);
            assert!(assignment.index <= local.applications[&setup::mapping().child].applied_index());
        }).unwrap();
    }
    driver.stop_waiting_with_child(&mut bridge).unwrap();
    while !driver.is_drained() {
        step(&mut driver, &mut bridge, clock);
        assert!(Instant::now() < end, "original administrative cleanup");
    }
    let node = driver
        .try_reclaim()
        .unwrap_or_else(|_| panic!("original borrower drained"));
    assert!(node.local().applications[&setup::mapping().parent]
        .status(original.operations().begin)
        .unwrap()
        .outcome
        .is_none());
    node.close_intake().unwrap();
    progress::until(&mut bridge, node, clock, fixture::FacadeNode::is_drained);
    bridge.close();
    assert!(
        bridge.is_drained(),
        "original bridge usage {:?}, failure {:?}",
        bridge.usage(),
        bridge.failure()
    );
    drop(bridge);
    drop(
        parent
            .into_parts()
            .unwrap_or_else(|_| panic!("original parent drain")),
    );
    cohort.finish();
}
fn step(
    driver: &mut ChildActivationDriver<'_, fixture::FacadeNode>,
    bridge: &mut Bridge<'_>,
    clock: Instant,
) {
    driver
        .advance_with_child(
            MonoTime(clock.elapsed().as_millis() as u64),
            ChildActivationDriverBudget::default(),
            fixture::budget(4),
            bridge,
        )
        .unwrap();
    std::thread::park_timeout(Duration::from_millis(1));
}
