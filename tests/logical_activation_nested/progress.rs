// SPDX-License-Identifier: RPL-1.5
// Copyright (c) 2026 Karl Livesey
use super::{
    harness::{self, Observed},
    lower::Lower,
    setup, Middle, Top, UpperBridge,
};
use std::time::Duration;
use voteboat::{application::StateMachine, logical_voter::*, raft::Role};
pub fn step(lower: &mut Lower<'_>, bridge: &mut UpperBridge<'_>, top: &mut Top, count: usize) {
    lower.pump_bridge(bridge, count);
    lower.advance_with(Observed::default().backing(bridge).local(top.local()));
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
    lower.advance_with(Observed::default().backing(bridge));
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
    // The original 90 s absolute and 30 s inactivity watchdog, in protocol time.
    let end = harness::Wait::new(&lower.clock, 90_000);
    let started = lower.now();
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
        let now = lower.now();
        let fresh = stage(driver.phase()) & !seen;
        if fresh != 0 {
            seen |= fresh;
            progressed = now;
        }
        let elapsed = now.0 - started.0;
        let inactivity = now.0 - progressed.0;
        let absolute_expired = !end.within(&lower.clock);
        let inactivity_expired =
            wanted != ChildActivationDriverPhase::Released && inactivity >= 30_000;
        if absolute_expired || inactivity_expired {
            super::diagnostic::phase(lower, bridge, driver, count);
            panic!(
                "nested activation phase={:?} want={wanted:?} elapsed_ms={elapsed} inactivity_ms={inactivity} absolute90={absolute_expired} inactivity30={inactivity_expired}",
                driver.phase()
            );
        }
    }
}
pub fn elect_middle(lower: &mut Lower<'_>, nodes: &mut [Middle]) {
    nodes[0].campaign().unwrap();
    let end = harness::Wait::new(&lower.clock, harness::LIVENESS_MS);
    loop {
        lower.middle(nodes);
        let core = nodes[0].local().owner.core(setup::middle().group).unwrap();
        let index = core.state().commit_index;
        let leader = core.role() == Role::Leader;
        let pending = core.has_pending_dependency();
        let committed_term = core.state().term_at(index);
        let current_term = committed_term == Some(core.state().hard_state.term);
        let applied = nodes
            .iter()
            .all(|n| n.local().applications[&setup::middle().group].applied_index() >= index);
        // Durable election readiness precedes the actual verified Claim; normal
        // child consensus work need not be globally idle while intake is open.
        if leader && !pending && current_term && applied {
            return;
        }
        if !end.within(&lower.clock) {
            eprintln!(
                "393 original middle election leader={leader} pending={pending} current_term={current_term} all_applied={applied} old_all_work_drained={} term={} commit={index} committed_term={committed_term:?}",
                nodes.iter().all(Middle::work_is_drained), core.state().hard_state.term
            );
            super::diagnostic::middle_shutdown(lower, nodes);
            panic!("original middle election");
        }
    }
}
pub fn claim_upper(lower: &mut Lower<'_>, bridge: &mut UpperBridge<'_>) -> VerifiedLogicalImage {
    bridge
        .begin_claim(voteboat::identity::OperationId::new(356100).unwrap())
        .unwrap();
    let end = harness::Wait::new(&lower.clock, harness::LIVENESS_MS);
    loop {
        lower.pump_bridge(bridge, 3);
        lower.advance_with(Observed::default().backing(bridge));
        lower.pump_bridge(bridge, 3);
        bridge.advance(lower.now(), budget()).unwrap();
        if let Some(image) = bridge.take_image() {
            return image;
        }
        assert!(end.within(&lower.clock), "original upper Claim");
        std::thread::park_timeout(Duration::from_millis(1));
    }
}
pub fn elect_top(lower: &mut Lower<'_>, bridge: &mut UpperBridge<'_>, top: &mut Top) {
    top.campaign().unwrap();
    let end = harness::Wait::new(&lower.clock, harness::LIVENESS_MS);
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
        assert!(end.within(&lower.clock), "original top election");
    }
}
