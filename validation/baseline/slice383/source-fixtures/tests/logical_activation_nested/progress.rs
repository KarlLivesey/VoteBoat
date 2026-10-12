// SPDX-License-Identifier: RPL-1.5
// Copyright (c) 2026 Karl Livesey
use super::{lower::Lower, setup, Middle, Top, UpperBridge};
use std::time::{Duration, Instant};
use voteboat::{application::StateMachine, logical_voter::*, raft::Role};
pub fn step(lower: &mut Lower<'_>, bridge: &mut UpperBridge<'_>, top: &mut Top, count: usize) {
    lower.pump_bridge(bridge, count);
    lower.advance();
    lower.pump_bridge(bridge, count);
    bridge.advance(lower.now(), budget()).unwrap();
    top.pump_backing(|p| {
        bridge.pump_persistence(p.persistence).unwrap();
        bridge.pump_release(p.releases).unwrap();
        if let Some(s) = p.snapshots {
            bridge.pump_snapshots(s, p.persistence).unwrap();
        }
    });
    top.poll(lower.now(), LogicalNodePollBudget::default())
        .unwrap();
    std::thread::park_timeout(Duration::from_millis(1));
}
pub fn budget() -> LogicalBridgeBudget<LogicalNodePollBudget> {
    LogicalBridgeBudget {
        node_polls: 4,
        completions: 8,
        transitions: 1,
        node: LogicalNodePollBudget::default(),
    }
}
pub fn driver_step(
    lower: &mut Lower<'_>,
    bridge: &mut UpperBridge<'_>,
    driver: &mut ChildActivationDriver<'_, Top>,
    count: usize,
) {
    lower.pump_bridge(bridge, count);
    lower.advance();
    lower.pump_bridge(bridge, count);
    driver
        .advance_with_child(
            lower.now(),
            ChildActivationDriverBudget::default(),
            budget(),
            bridge,
        )
        .unwrap_or_else(|e| {
            panic!(
                "genuine nested activation {:?} phase={:?}",
                e,
                driver.phase()
            )
        });
    std::thread::park_timeout(Duration::from_millis(1));
}
pub fn phase(
    lower: &mut Lower<'_>,
    bridge: &mut UpperBridge<'_>,
    driver: &mut ChildActivationDriver<'_, Top>,
    count: usize,
    wanted: ChildActivationDriverPhase,
) {
    let started = Instant::now();
    let mut progressed = started;
    let stage = |phase| match phase {
        ChildActivationDriverPhase::Begun => 1_u8,
        ChildActivationDriverPhase::Enrolling => 2,
        ChildActivationDriverPhase::Completed => 4,
        ChildActivationDriverPhase::Released => 8,
        _ => 0,
    };
    let mut seen = match driver.phase() {
        ChildActivationDriverPhase::Enrolling => 3,
        ChildActivationDriverPhase::Completed => 7,
        ChildActivationDriverPhase::Released => 15,
        phase => stage(phase),
    };
    while driver.phase() != wanted {
        driver_step(lower, bridge, driver, count);
        let now = Instant::now();
        let fresh = stage(driver.phase()) & !seen;
        if fresh != 0 {
            seen |= fresh;
            progressed = now;
        }
        if now.duration_since(started) >= Duration::from_secs(90)
            || now.duration_since(progressed) >= Duration::from_secs(30)
        {
            super::diagnostic::phase(lower, bridge, driver, count);
            panic!(
                "nested activation phase={:?} want={wanted:?}",
                driver.phase()
            );
        }
    }
}
pub fn elect_middle(lower: &mut Lower<'_>, nodes: &mut [Middle]) {
    nodes[0].campaign().unwrap();
    let end = Instant::now() + Duration::from_secs(15);
    loop {
        lower.middle(nodes);
        let core = nodes[0].local().owner.core(setup::middle().group).unwrap();
        let index = core.state().commit_index;
        if core.role() == Role::Leader
            && core.state().term_at(index) == Some(core.state().hard_state.term)
            && nodes
                .iter()
                .all(|n| n.local().applications[&setup::middle().group].applied_index() >= index)
            && nodes.iter().all(Middle::work_is_drained)
        {
            return;
        }
        assert!(Instant::now() < end, "original middle election");
    }
}
pub fn claim_upper(lower: &mut Lower<'_>, bridge: &mut UpperBridge<'_>) -> VerifiedLogicalImage {
    bridge
        .begin_claim(voteboat::identity::OperationId::new(356100).unwrap())
        .unwrap();
    let end = Instant::now() + Duration::from_secs(15);
    loop {
        lower.pump_bridge(bridge, 3);
        lower.advance();
        lower.pump_bridge(bridge, 3);
        bridge.advance(lower.now(), budget()).unwrap();
        if let Some(image) = bridge.take_image() {
            return image;
        }
        assert!(Instant::now() < end, "original upper Claim");
        std::thread::park_timeout(Duration::from_millis(1));
    }
}
pub fn elect_top(lower: &mut Lower<'_>, bridge: &mut UpperBridge<'_>, top: &mut Top) {
    top.campaign().unwrap();
    let end = Instant::now() + Duration::from_secs(15);
    loop {
        step(lower, bridge, top, 3);
        let core = top.local().owner.core(setup::top().group).unwrap();
        let index = core.state().commit_index;
        if core.role() == Role::Leader
            && index > 0
            && core.state().term_at(index) == Some(core.state().hard_state.term)
            && top.local().applications[&setup::top().group].applied_index() >= index
            && top.work_is_drained()
        {
            return;
        }
        assert!(Instant::now() < end, "original top election");
    }
}
