// SPDX-License-Identifier: RPL-1.5
// Copyright (c) 2026 Karl Livesey
//! Failure-only original observations; no completion or readiness authority.
use super::{lower::Lower, setup, Middle, Top, UpperBridge};
use voteboat::{
    application::StateMachine, logical_voter::*, outbound::OutboundQueue,
    snapshot_worker::SnapshotWorker, worker::PersistenceWorker,
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

pub fn shutdown(lower: &Lower<'_>, bridge: &UpperBridge<'_>, top: &mut Top, count: usize) {
    eprintln!(
        "387 shutdown top state={:?} drained={} work_drained={} failure={:?}; upper own_drained={} full_drained={} failure={:?} usage={:?}",
        top.state(), top.is_drained(), top.work_is_drained(), top.failure(),
        bridge.observations_are_drained(), bridge.is_drained(), bridge.failure(), bridge.usage()
    );
    let local = top.local();
    eprintln!(
        "387 top original accounting owner={:?} persistence={:?} reads={:?} clients={:?} results={:?} outbound={:?}",
        local.owner.usage(), local.persistence.usage(), local.reads.usage(),
        local.clients.usage(), local.results.usage(), local.outbound.usage()
    );
    top.pump_backing(|p| {
        eprintln!(
            "387 top original backing persistence_drained={} releases={:?} snapshot={:?}",
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
    work("shutdown upper", bridge);
    for slot in 0..count {
        let inspected = bridge.inspect_backing(slot, |p| {
            let core = p.owner.core(setup::middle().group).unwrap();
            eprintln!(
                "387 middle slot={slot} role={:?} last={} commit={} applied={} pending={} readiness={:?} owner={:?} persistence={:?} reads={:?} clients={:?} results={:?} outbound={:?} exports_drained={:?}; node/session state=UnavailableUnderBridgeBorrow",
                core.role(), core.state().last_index(), core.state().commit_index,
                p.applications[&setup::middle().group].applied_index(), core.has_pending_dependency(),
                core.pending_learner_readiness(), p.owner.usage(), p.persistence.usage(),
                p.reads.usage(), p.clients.usage(), p.results.usage(), p.outbound.usage(),
                p.snapshots.as_ref().map(|s| s.router.checkpoint_exports_are_drained())
            );
        });
        eprintln!("387 middle slot={slot} inspection={inspected:?}");
        if let Some(child) = lower.bridges.get(slot) {
            eprintln!("387 shutdown lower slot={slot}");
            work("shutdown lower", child);
        }
    }
}

pub fn middle_shutdown(lower: &Lower<'_>, nodes: &mut [Middle]) {
    eprintln!("389 SAME middle close owners={} observed={} session_closing_timers=UnavailablePrivateState", nodes.len(), nodes.len().min(5));
    for (slot, node) in nodes.iter_mut().enumerate().take(5) {
        eprintln!(
            "389 middle slot={slot} state={:?} drained={} work_drained={} failure={:?} configuration_drained={}",
            node.state(), node.is_drained(), node.work_is_drained(), node.failure(),
            node.configuration_requests_are_drained()
        );
        let local = node.local();
        let group = setup::middle().group;
        let core = local.owner.core(group).unwrap();
        eprintln!(
            "389 middle slot={slot} original owner={:?} group={group:?} role={:?} term={} configuration={:?} last={} commit={} applied={} pending={} visit={:?} deadline={:?}",
            local.owner.identity(), core.role(), core.state().hard_state.term,
            core.membership().id(), core.state().last_index(), core.state().commit_index,
            local.applications[&group].applied_index(), core.has_pending_dependency(),
            local.owner.inspect_visit(group), local.owner.deadline(group)
        );
        eprintln!(
            "389 middle slot={slot} original accounting owner={:?} persistence={:?} reads={:?} clients={:?} results={:?} outbound={:?} snapshots={:?}",
            local.owner.usage(), local.persistence.usage(), local.reads.usage(),
            local.clients.usage(), local.results.usage(), local.outbound.usage(),
            local.snapshots.as_ref().map(|s| (s.router.usage(), s.router.is_drained(),
                s.worker.usage(), s.worker.is_drained()))
        );
        match core.connection_replicas(16) {
            Ok(peers) => {
                for (peer, store) in peers {
                    eprintln!("389 middle slot={slot} original peer={peer:?} store={store:?} outbound={:?}", local.outbound.peer_usage(peer));
                }
            }
            Err(error) => eprintln!("389 middle slot={slot} peer inspection={error:?}"),
        }
        let deferred = node.inspect_deferred().count();
        eprintln!(
            "389 middle slot={slot} deferred_count={deferred} omitted={}",
            deferred.saturating_sub(16)
        );
        for batch in node.inspect_deferred().take(16) {
            eprintln!(
                "389 middle slot={slot} original deferred_ticket={:?}",
                batch.ticket()
            );
        }
        node.pump_backing(|p| {
            eprintln!(
                "389 middle slot={slot} original backing persistence_drained={} releases={:?} snapshot={:?}",
                p.persistence.is_drained(), p.releases.usage(),
                p.snapshots.map(|s| (s.pending_request(), s.operation(), s.cache_bytes(), s.is_drained()))
            );
        });
        if let Some(child) = lower.bridges.get(slot) {
            eprintln!(
                "389 SAME lower slot={slot} own_drained={} full_drained={} failure={:?} usage={:?}",
                child.observations_are_drained(),
                child.is_drained(),
                child.failure(),
                child.usage()
            );
            work("389 middle close lower", child);
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
