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
use aliases::Bridge;
use fixture::{budget, FacadeNode};
pub fn step(bridge: &mut Bridge<'_>, node: &mut FacadeNode, clock: Instant, completions: usize) {
    pump(bridge, node, clock, completions);
    node.poll(
        MonoTime(clock.elapsed().as_millis() as u64),
        LogicalNodePollBudget::default(),
    )
    .unwrap();
}
pub fn pump(bridge: &mut Bridge<'_>, node: &mut FacadeNode, clock: Instant, completions: usize) {
    bridge
        .advance(
            MonoTime(clock.elapsed().as_millis() as u64),
            budget(completions),
        )
        .unwrap();
    node.pump_backing(|p| {
        bridge.pump_persistence(p.persistence).unwrap();
        bridge.pump_release(p.releases).unwrap();
    });
}
pub fn until(
    bridge: &mut Bridge<'_>,
    node: &mut FacadeNode,
    clock: Instant,
    mut done: impl FnMut(&FacadeNode) -> bool,
) {
    let end = Instant::now() + Duration::from_secs(15);
    while !done(node) {
        step(bridge, node, clock, 4);
        assert!(Instant::now() < end, "original parent progress");
        std::thread::park_timeout(Duration::from_millis(1));
    }
}
pub fn advance(
    driver: &mut ChildActivationDriver<'_, FacadeNode>,
    bridge: &mut Bridge<'_>,
    clock: Instant,
    completions: usize,
) {
    driver
        .advance(
            MonoTime(clock.elapsed().as_millis() as u64),
            ChildActivationDriverBudget::default(),
            |p| {
                bridge.advance(
                    MonoTime(clock.elapsed().as_millis() as u64),
                    budget(completions),
                )?;
                bridge.pump_persistence(p.persistence)?;
                bridge.pump_release(p.releases).map(|_| ())
            },
        )
        .unwrap();
}
pub fn phase(
    driver: &mut ChildActivationDriver<'_, FacadeNode>,
    bridge: &mut Bridge<'_>,
    clock: Instant,
    wanted: ChildActivationDriverPhase,
) {
    let end = Instant::now() + Duration::from_secs(15);
    while driver.phase() != wanted {
        advance(driver, bridge, clock, 4);
        assert!(
            Instant::now() < end,
            "original driver progress {:?}",
            driver.phase()
        );
        std::thread::park_timeout(Duration::from_millis(1));
    }
}
pub fn stop<'a>(
    mut driver: ChildActivationDriver<'a, FacadeNode>,
    bridge: &mut Bridge<'_>,
    clock: Instant,
) -> &'a mut FacadeNode {
    driver.stop_waiting().unwrap();
    let end = Instant::now() + Duration::from_secs(15);
    while !driver.is_drained() {
        advance(&mut driver, bridge, clock, 4);
        assert!(Instant::now() < end, "original observer cleanup");
    }
    driver
        .try_reclaim()
        .unwrap_or_else(|_| panic!("original borrower drain"))
}

pub fn elect(bridge: &mut Bridge<'_>, node: &mut FacadeNode, clock: Instant) {
    use voteboat::application::StateMachine;
    node.campaign().unwrap();
    until(bridge, node, clock, |n| {
        let core = n.local().owner.core(setup::mapping().parent).unwrap();
        core.role() == Role::Leader
            && core.state().commit_index > 0
            && n.local().clients.is_drained()
            && n.local().reads.is_drained()
            && n.local().results.is_drained()
            && n.local().applications[&setup::mapping().parent].applied_index()
                >= core.state().commit_index
    });
}
pub fn child_step(
    driver: &mut ChildActivationDriver<'_, FacadeNode>,
    bridge: &mut Bridge<'_>,
    clock: Instant,
    polls: usize,
) {
    let mut budget = fixture::budget(4);
    budget.node_polls = polls;
    driver
        .advance_with_child(
            MonoTime(clock.elapsed().as_millis() as u64),
            ChildActivationDriverBudget::default(),
            budget,
            bridge,
        )
        .unwrap();
    std::thread::park_timeout(Duration::from_millis(1));
}
