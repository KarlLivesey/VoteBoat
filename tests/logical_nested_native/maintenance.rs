// SPDX-License-Identifier: RPL-1.5
// Copyright (c) 2026 Karl Livesey
use super::{aliases::UpperBridge, setup};
use voteboat::{application::StateMachine, snapshot_worker::SnapshotWorker};
#[derive(Clone, Copy)]
struct Request {
    base: u64,
    frontier: u64,
}
#[derive(Default)]
pub struct Maintenance {
    pending: [Option<Request>; 9],
    pub completed: usize,
}
impl Maintenance {
    pub fn poll(&mut self, bridges: &mut [UpperBridge<'_>], admit: bool) {
        for (cohort, bridge) in bridges.iter_mut().enumerate() {
            for slot in 0..3 {
                let index = cohort * 3 + slot;
                let state = bridge
                    .inspect_backing(slot, |local| {
                        let core = local.owner.core(setup::middle(cohort).group).unwrap();
                        let snapshots = local.snapshots.as_ref().unwrap();
                        (
                            core.state().base_index(),
                            core.state().commit_index,
                            core.state().entries.len(),
                            local.applications[&setup::middle(cohort).group].applied_index(),
                            snapshots.router.is_drained() && snapshots.worker.is_drained(),
                        )
                    })
                    .unwrap();
                if let Some(request) = self.pending[index] {
                    let port_drained = bridge
                        .pump_backing(slot, |p| p.unwrap().snapshots.unwrap().is_drained())
                        .unwrap();
                    if state.0 > request.base
                        && state.0 >= request.frontier
                        && state.4
                        && port_drained
                    {
                        self.pending[index] = None;
                        self.completed += 1;
                        eprintln!("nested original checkpoint completed middle={index} base={} frontier={}", state.0, request.frontier);
                    }
                } else if admit && state.2 >= 16 && state.1 > state.0 && state.3 >= state.1 {
                    bridge.checkpoint_backing(slot).unwrap();
                    assert_eq!(
                        bridge
                            .inspect_backing(slot, |local| local
                                .owner
                                .core(setup::middle(cohort).group)
                                .unwrap()
                                .state()
                                .base_index())
                            .unwrap(),
                        state.0,
                        "checkpoint admission is not durable completion"
                    );
                    self.pending[index] = Some(Request {
                        base: state.0,
                        frontier: state.1,
                    });
                    eprintln!(
                        "nested original checkpoint admitted middle={index} base={} frontier={}",
                        state.0, state.1
                    );
                }
            }
        }
    }
}

/// This is a refusal against actual facade owners, not a constructed proof.
pub fn check_scope(nodes: &mut [Vec<super::aliases::Middle>]) {
    use voteboat::logical_voter::{LogicalBackingChild, LogicalBackingPollError, LogicalNodeError};
    for node in nodes.iter_mut().flatten() {
        let before = (node.local().owner.identity(), node.local().owner.usage());
        assert_eq!(
            LogicalBackingChild::checkpoint(node, setup::top().group),
            Err(LogicalBackingPollError::LogicalNode(Box::new(
                LogicalNodeError::WrongBinding
            )))
        );
        assert_eq!(
            (node.local().owner.identity(), node.local().owner.usage()),
            before
        );
    }
}
