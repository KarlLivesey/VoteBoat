// SPDX-License-Identifier: RPL-1.5
// Copyright (c) 2026 Karl Livesey
//! Failure-only shared-reference views; no progress or quorum authority.
use super::{cluster::ChildNode, setup};
use voteboat::{
    application::StateMachine, identity::GroupIdentity, outbound::OutboundQueue,
    snapshot_worker::SnapshotWorker, worker::PersistenceWorker,
};

pub fn retained(slot: usize, nodes: &[ChildNode]) -> Vec<String> {
    let group = setup::bottom(slot).group;
    nodes
        .iter()
        .map(|node| {
            format!(
                "state={:?},failure={:?},drained={},core={},local={},network={}",
                node.state(),
                node.failure(),
                node.is_drained(),
                core(node, group),
                local(node, group),
                network(node),
            )
        })
        .collect()
}

fn core(node: &ChildNode, group: GroupIdentity) -> String {
    let local = node.local();
    let core = local.owner.core(group).unwrap();
    let state = core.state();
    format!(
        "node={:?},owner={:?},group={group:?},configuration={:?},role={:?},hard={:?},base={},last={},commit={},applied={},snapshot={:?},pending={},visit={:?},deadline={:?}",
        core.local_node(),
        local.owner.identity(),
        core.membership().id(),
        core.role(),
        state.hard_state,
        state.base_index(),
        state.last_index(),
        state.commit_index,
        local.applications[&group].applied_index(),
        state.snapshot,
        core.has_pending_dependency(),
        local.owner.inspect_visit(group),
        local.owner.deadline(group),
    )
}

fn local(node: &ChildNode, group: GroupIdentity) -> String {
    let local = node.local();
    let snapshots = local.snapshots.as_ref().map(|parts| {
        (
            parts.router.usage(),
            parts.router.is_drained(),
            parts.worker.usage(),
            parts.worker.is_drained(),
        )
    });
    format!(
        "group={group:?},checkpoints={:?},owner={:?}/drained={},worker={:?}/drained={},results={:?}/drained={},clients={:?}/drained={},reads={:?}/drained={},outbound={:?}/drained={},snapshots={snapshots:?},replica={:?},configuration_drained={}",
        node.checkpoints(),
        local.owner.usage(),
        local.owner.is_drained(),
        local.persistence.usage(),
        local.persistence.is_drained(),
        local.results.usage(),
        local.results.is_drained(),
        local.clients.usage(),
        local.clients.is_drained(),
        local.reads.usage(),
        local.reads.is_drained(),
        local.outbound.usage(),
        local.outbound.is_drained(),
        node.replica_usage(),
        node.configuration_requests_are_drained(),
    )
}

fn network(node: &ChildNode) -> String {
    let Some(peers) = node.peers() else {
        return "None".into();
    };
    let roster = peers.roster();
    let bindings = roster
        .tracked_peers()
        .map(|peer| (peer, roster.peer_identity(peer), roster.binding(peer)))
        .collect::<Vec<_>>();
    format!(
        "driver={:?}/drained={},roster={:?}/drained={}/fenced={},connector={:?}/drained={},ingress={:?}/drained={},next_deadline={:?},original_peers={bindings:?},provider_close_status=AssociationUnavailable",
        peers.usage(),
        peers.is_drained(),
        roster.usage(),
        roster.is_drained(),
        roster.is_fenced(),
        peers.connector_usage(),
        peers.connector_is_drained(),
        peers.ingress().usage(),
        peers.ingress().is_drained(),
        peers.next_deadline(),
    )
}
