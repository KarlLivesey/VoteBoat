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
//! Independent parent backing and target administrative ownership.
use super::*;
use harness::{FixtureClock, SimClock};
use voteboat::application::StateMachine;
pub struct Peers<'a> {
    pub parents: [fixture::FacadeNode; 2],
    pub bridges: [aliases::Bridge<'a>; 2],
    // Shared with the members and channels; one round per complete poll of
    // every parent and bridge, never derived from wall-clock time.
    pub clock: SimClock,
}
impl Peers<'_> {
    pub fn now(&self) -> MonoTime {
        self.clock.now()
    }
    /// Every round polls all three bridges, so all ten native nodes decide
    /// whether durable I/O holds protocol time still.
    fn tick(&self, target: &aliases::Bridge<'_>) -> MonoTime {
        let settled =
            harness::bridge_settled(target) && self.bridges.iter().all(harness::bridge_settled);
        self.clock.round(settled)
    }
    pub fn initial(&mut self, leader: &mut fixture::FacadeNode, target: &mut aliases::Bridge<'_>) {
        let now = self.tick(target);
        self.bridges[0].advance(now, fixture::budget(4)).unwrap();
        pump(&mut self.bridges[0], leader);
        leader.poll(now, LogicalNodePollBudget::default()).unwrap();
        target.advance(now, fixture::budget(4)).unwrap();
        self.others(target);
    }
    pub fn shutdown_step(
        &mut self,
        leader: &mut fixture::FacadeNode,
        target: &mut aliases::Bridge<'_>,
    ) -> [LogicalNodeShutdownProgress; 3] {
        let now = self.tick(target);
        self.bridges[0].advance(now, fixture::budget(4)).unwrap();
        pump(&mut self.bridges[0], leader);
        let leader_done = shutdown(leader, now);
        target.advance(now, fixture::budget(4)).unwrap();
        pump(target, &mut self.parents[0]);
        let left_done = shutdown(&mut self.parents[0], now);
        self.bridges[1].advance(now, fixture::budget(4)).unwrap();
        pump(&mut self.bridges[1], &mut self.parents[1]);
        let right_done = shutdown(&mut self.parents[1], now);
        std::thread::park_timeout(Duration::from_millis(1));
        [leader_done, left_done, right_done]
    }
    pub fn step(
        &mut self,
        driver: &mut ChildActivationDriver<'_, fixture::FacadeNode>,
        target: &mut aliases::Bridge<'_>,
    ) {
        let now = self.tick(target);
        driver
            .advance(now, ChildActivationDriverBudget::default(), |p| {
                let bridge = &mut self.bridges[0];
                bridge.advance(now, fixture::budget(4))?;
                bridge.pump_persistence(p.persistence)?;
                bridge.pump_release(p.releases)?;
                if let Some(snapshots) = p.snapshots {
                    bridge.pump_snapshots(snapshots, p.persistence)?;
                }
                Ok(())
            })
            .unwrap();
        driver
            .advance_child(now, fixture::budget(4), target)
            .unwrap();
        self.others(target);
    }
    fn others(&mut self, target: &mut aliases::Bridge<'_>) {
        let now = self.now();
        pump(target, &mut self.parents[0]);
        self.parents[0]
            .poll(now, LogicalNodePollBudget::default())
            .unwrap();
        self.bridges[1].advance(now, fixture::budget(4)).unwrap();
        pump(&mut self.bridges[1], &mut self.parents[1]);
        self.parents[1]
            .poll(now, LogicalNodePollBudget::default())
            .unwrap();
        std::thread::park_timeout(Duration::from_millis(1));
    }
    pub fn elect(&mut self, leader: &mut fixture::FacadeNode, target: &mut aliases::Bridge<'_>) {
        leader.campaign().unwrap();
        let end = harness::hang_guard();
        loop {
            self.initial(leader, target);
            let core = leader.local().owner.core(setup::mapping().parent).unwrap();
            let index = core.state().commit_index;
            if core.role() == Role::Leader
                && index > 0
                && core.state().term_at(index) == Some(core.state().hard_state.term)
                && leader.local().applications[&setup::mapping().parent].applied_index() >= index
                && self.parents.iter().all(|p| {
                    p.local().applications[&setup::mapping().parent].applied_index() >= index
                })
                && leader.local().clients.is_drained()
                && leader.local().reads.is_drained()
                && leader.local().results.is_drained()
            {
                return;
            }
            assert!(
                Instant::now() < end,
                "actual three-parent current-term election"
            );
        }
    }
}
pub fn pump(bridge: &mut aliases::Bridge<'_>, parent: &mut fixture::FacadeNode) {
    parent.pump_backing(|p| {
        bridge.pump_persistence(p.persistence).unwrap();
        bridge.pump_release(p.releases).unwrap();
        if let Some(snapshots) = p.snapshots {
            bridge.pump_snapshots(snapshots, p.persistence).unwrap();
        }
    });
}

fn shutdown(parent: &mut fixture::FacadeNode, now: MonoTime) -> LogicalNodeShutdownProgress {
    parent
        .poll_shutdown(now, LogicalNodePollBudget::default())
        .unwrap_or_else(|e| {
            panic!(
                "original host-requested cleanup {:?} completed={:?} closures={:?}",
                e.reason, e.progress.completions, e.progress.peer_closures,
            )
        })
}
