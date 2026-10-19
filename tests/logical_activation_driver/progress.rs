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
use harness::{FixtureClock, Observed, SimClock};
/// Protocol time for one round over the native nodes this bridge polls.
pub fn round(bridge: &Bridge<'_>, clock: &SimClock) -> MonoTime {
    clock.round(Observed::default().bridge(bridge))
}
pub fn step(bridge: &mut Bridge<'_>, node: &mut FacadeNode, clock: &SimClock, completions: usize) {
    pump(bridge, node, clock, completions);
    node.poll(clock.now(), LogicalNodePollBudget::default())
        .unwrap();
}
pub fn pump(bridge: &mut Bridge<'_>, node: &mut FacadeNode, clock: &SimClock, completions: usize) {
    let now = round(bridge, clock);
    bridge.advance(now, budget(completions)).unwrap();
    node.pump_backing(|p| {
        bridge.pump_persistence(p.persistence).unwrap();
        bridge.pump_release(p.releases).unwrap();
    });
}
pub fn until(
    bridge: &mut Bridge<'_>,
    node: &mut FacadeNode,
    clock: &SimClock,
    mut done: impl FnMut(&FacadeNode) -> bool,
) {
    let end = harness::Wait::new(clock, harness::LIVENESS_MS);
    while !done(node) {
        step(bridge, node, clock, 4);
        assert!(end.within(clock), "original parent progress");
        std::thread::park_timeout(Duration::from_millis(1));
    }
}
pub fn advance(
    driver: &mut ChildActivationDriver<'_, FacadeNode>,
    bridge: &mut Bridge<'_>,
    clock: &SimClock,
    completions: usize,
) {
    let now = round(bridge, clock);
    driver
        .advance(now, ChildActivationDriverBudget::default(), |p| {
            bridge.advance(now, budget(completions))?;
            bridge.pump_persistence(p.persistence)?;
            bridge.pump_release(p.releases).map(|_| ())
        })
        .unwrap();
}
pub fn phase(
    driver: &mut ChildActivationDriver<'_, FacadeNode>,
    bridge: &mut Bridge<'_>,
    clock: &SimClock,
    wanted: ChildActivationDriverPhase,
) {
    let end = harness::Wait::new(clock, harness::LIVENESS_MS);
    while driver.phase() != wanted {
        advance(driver, bridge, clock, 4);
        assert!(
            end.within(clock),
            "original driver progress {:?}",
            driver.phase()
        );
        std::thread::park_timeout(Duration::from_millis(1));
    }
}
pub fn stop<'a>(
    mut driver: ChildActivationDriver<'a, FacadeNode>,
    bridge: &mut Bridge<'_>,
    clock: &SimClock,
) -> &'a mut FacadeNode {
    driver.stop_waiting().unwrap();
    let end = harness::Wait::new(clock, harness::LIVENESS_MS);
    while !driver.is_drained() {
        advance(&mut driver, bridge, clock, 4);
        assert!(end.within(clock), "original observer cleanup");
    }
    driver
        .try_reclaim()
        .unwrap_or_else(|_| panic!("original borrower drain"))
}

pub fn elect(bridge: &mut Bridge<'_>, node: &mut FacadeNode, clock: &SimClock) {
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
    clock: &SimClock,
    polls: usize,
) {
    let mut budget = fixture::budget(4);
    budget.node_polls = polls;
    let now = round(bridge, clock);
    driver
        .advance_with_child(now, ChildActivationDriverBudget::default(), budget, bridge)
        .unwrap();
    std::thread::park_timeout(Duration::from_millis(1));
}
/// Pump until the parent has drained and the bridge reports every child's
/// accepted lower work settled, within the original 15 s bound in protocol
/// time. A drained parent can leave follower persistence in flight, and the
/// bridge contract makes the host keep pumping until it completes, so this
/// checks that both drain within the bound, not that the bridge has already
/// drained at the instant the parent does.
pub fn drain(bridge: &mut Bridge<'_>, node: &mut FacadeNode, clock: &SimClock) {
    let end = harness::Wait::new(clock, harness::LIVENESS_MS);
    while !(node.is_drained() && bridge.is_drained()) {
        if node.is_drained() {
            let now = round(bridge, clock);
            bridge.advance(now, budget(4)).unwrap();
        } else {
            step(bridge, node, clock, 4);
        }
        assert!(end.within(clock), "original parent and bridge drain");
        std::thread::park_timeout(Duration::from_millis(1));
    }
}
