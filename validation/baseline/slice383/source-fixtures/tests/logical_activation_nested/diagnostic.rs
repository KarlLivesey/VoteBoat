// SPDX-License-Identifier: RPL-1.5
// Copyright (c) 2026 Karl Livesey
//! Failure-only original observations; no completion or readiness authority.
use super::{lower::Lower, setup, Top, UpperBridge};
use voteboat::{
    application::StateMachine, logical_voter::*, outbound::OutboundQueue, worker::PersistenceWorker,
};

pub fn phase(
    lower: &Lower<'_>,
    bridge: &UpperBridge<'_>,
    driver: &mut ChildActivationDriver<'_, Top>,
    count: usize,
) {
    eprintln!(
        "375 original driver phase={:?} failure={:?} begin={:?} drained={} intent={:?}; parent ticket/core=UnavailableUnderDriverBorrow",
        driver.phase(), driver.failure(), driver.original_begin_position(), driver.is_drained(), driver.intent()
    );
    driver.pump(|p| {
        eprintln!(
            "375 original parent ports persistence_drained={} releases={:?} snapshot={:?}",
            p.persistence.is_drained(),
            p.releases.usage(),
            p.snapshots.map(|s| (
                s.pending_request(),
                s.operation(),
                s.cache_bytes(),
                s.is_drained()
            ))
        );
    });
    work("upper", bridge);
    eprintln!(
        "375 original upper observation_drained={} fully_drained={} usage={:?}",
        bridge.observations_are_drained(),
        bridge.is_drained(),
        bridge.usage()
    );
    for slot in 0..count {
        let inspected = bridge.inspect_backing(slot, |p| {
            let core = p.owner.core(setup::middle().group).unwrap();
            eprintln!(
                "375 middle slot={slot} owner={:?} role={:?} term={} configuration={:?} last={} commit={} applied={} pending={} readiness={:?} ready={:?} visit={:?}",
                p.owner.identity(), core.role(), core.state().hard_state.term, core.membership().id(),
                core.state().last_index(), core.state().commit_index,
                p.applications[&setup::middle().group].applied_index(), core.has_pending_dependency(),
                core.pending_learner_readiness(), core.ready_learner(), p.owner.inspect_visit(setup::middle().group)
            );
            eprintln!(
                "375 middle slot={slot} accounting owner={:?} persistence={:?} reads={:?} clients={:?} results={:?} outbound={:?} exports_drained={:?}; configuration_stream=UnavailableUnderBridgeBorrow",
                p.owner.usage(), p.persistence.usage(), p.reads.usage(), p.clients.usage(), p.results.usage(), p.outbound.usage(),
                p.snapshots.as_ref().map(|s| s.router.checkpoint_exports_are_drained())
            );
        });
        eprintln!("375 middle slot={slot} inspection={inspected:?}");
        if let Some(child) = lower.bridges.get(slot) {
            eprintln!("375 lower slot={slot}");
            work("lower", child);
        }
    }
}

fn work<N: LogicalBackingChild>(label: &str, bridge: &LogicalBackingBridge<'_, N>) {
    bridge.inspect_work(|work| {
        let Some(work) = work else {
            eprintln!("375 {label} original work=None");
            return;
        };
        eprintln!(
            "375 {label} work phase={:?} operation={:?} bytes={:?} capsule={} attempt={:?} retained_output={}",
            work.phase(), work.operation(), work.command_bytes().map(<[u8]>::len),
            work.capsule().is_some(), work.attempt(), work.retained_output()
        );
        if let Some(attempt) = work.attempt() {
            if let LogicalBridgeChildTicket::Read { ticket, .. } = attempt.ticket() {
                let inspected = bridge.inspect_backing(attempt.node(), |p| {
                    eprintln!("375 {label} original read={:?}", p.reads.inspect(ticket));
                });
                eprintln!("375 {label} read inspection={inspected:?}");
            }
        }
    });
}
