// SPDX-License-Identifier: RPL-1.5
// Copyright (c) 2026 Karl Livesey
//! Recover two logical tiers exclusively from the same closed physical stores.
use super::{
    cluster::{Cluster, HISTORY},
    host::Host,
    lower::Lower,
    middle, setup, write,
};
use std::time::Instant;
use voteboat::{
    application::{ReadableStateMachine, StateMachine},
    identity::StoreBinding,
    log::{EntryPayload, GroupLog},
    native::{connect::NativePeerProtocol, startup::NativeOpenMode},
    snapshot_worker::SnapshotWorker,
};

struct Facts {
    top: Vec<GroupLog>,
    top_bindings: Vec<StoreBinding>,
    middle_bindings: Vec<StoreBinding>,
}

#[test]
fn tcp_two_levels_recover_actual_checkpoint_suffix_and_retry_from_closed_stores() {
    history(NativePeerProtocol::TcpTls);
}

#[test]
#[cfg(feature = "quic")]
fn quic_two_levels_recover_actual_checkpoint_suffix_and_retry_from_closed_stores() {
    history(NativePeerProtocol::Quic);
}

fn history(protocol: NativePeerProtocol) {
    let _serial = HISTORY.lock().unwrap();
    let clock = Instant::now();
    let mut bottom = (0..9)
        .map(|i| Cluster::new(protocol, i, clock))
        .collect::<Vec<_>>();
    let mut lower = Lower::new(&mut bottom, clock);
    let images = lower.claims(100000);
    let (mut middle_nodes, channels) = middle::build(&images, protocol, &mut lower);
    let middle_bindings = bindings(&middle_nodes);
    middle::elect(&mut lower, &mut middle_nodes);
    let mut host = Host::new(lower, &mut middle_nodes, protocol, 200000, &channels);
    host.elect();
    write::write(&mut host, 300000, 5, false);
    settle(&mut host);
    checkpoint(&mut host);
    let suffix = write::write(&mut host, 300001, 0, false);
    settle(&mut host);
    super::read::read(&mut host);
    let facts = capture(&host, middle_bindings, suffix.position.index);
    let mut lower = host.close();
    let middle_states = middle::close(&mut lower, middle_nodes, channels);
    drop(lower);
    drop(images);
    for cluster in &mut bottom {
        cluster.checkpoint();
        cluster.close();
    }
    eprintln!("cold nested original27 physical owners joined; reopening same stores");
    for cluster in &mut bottom {
        cluster.open(NativeOpenMode::Recover);
    }
    let mut lower = Lower::new(&mut bottom, clock);
    let fresh = lower.claims(500000);
    assert_eq!(fresh.len(), middle_states.len());
    for ((image, expected), binding) in fresh.iter().zip(&middle_states).zip(&facts.middle_bindings)
    {
        assert_eq!(&image.image().parent, expected);
        assert_eq!(image.binding().identity, binding.identity);
        assert!(image.binding().session > binding.session);
    }
    let (mut middle_nodes, channels) = middle::build(&fresh, protocol, &mut lower);
    middle::elect(&mut lower, &mut middle_nodes);
    let mut host = Host::new(lower, &mut middle_nodes, protocol, 600000, &channels);
    restored(&host, &facts);
    host.elect();
    write::write(&mut host, 300000, 5, true);
    write::write(&mut host, 300001, 0, true);
    super::read::read(&mut host);
    super::convergence::finish(&mut host);
    let mut lower = host.close();
    middle::close(&mut lower, middle_nodes, channels);
    drop(lower);
    for mut cluster in bottom {
        cluster.checkpoint();
        cluster.finish();
    }
}

fn bindings(nodes: &[Vec<super::aliases::Middle>]) -> Vec<StoreBinding> {
    nodes
        .iter()
        .flatten()
        .map(|node| node.local().owner.identity().store)
        .collect()
}

fn settle(host: &mut Host<'_, '_>) {
    while host.writes.iter().any(|write| !write.done()) {
        host.poll();
    }
    host.writes.last().unwrap().assert_final(host);
}

fn checkpoint(host: &mut Host<'_, '_>) {
    host.phase = "cold_upper_checkpoint";
    let group = setup::top().group;
    let frontier = host.nodes[0].local().applications[&group].applied_index();
    for node in &mut host.nodes {
        node.checkpoint().unwrap();
    }
    host.until(|host| {
        host.nodes.iter().all(|node| {
            let local = node.local();
            let core = local.owner.core(group).unwrap();
            let snapshots = local.snapshots.as_ref().unwrap();
            core.state()
                .snapshot
                .is_some_and(|reference| reference.index >= frontier)
                && core.state().base_index() >= frontier
                && snapshots.router.is_drained()
                && snapshots.worker.is_drained()
        })
    });
    eprintln!("cold nested original upper checkpoint completed frontier={frontier}");
}

fn capture(host: &Host<'_, '_>, middle_bindings: Vec<StoreBinding>, suffix_index: u64) -> Facts {
    let group = setup::top().group;
    let top = host
        .nodes
        .iter()
        .map(|node| {
            let local = node.local();
            let state = local.owner.core(group).unwrap().state();
            let anchor = state.snapshot.expect("actual upper durable checkpoint");
            let suffix = state
                .entry_at(suffix_index)
                .expect("original post-checkpoint suffix");
            assert!(suffix.index > anchor.index);
            assert!(
                matches!(&suffix.payload, EntryPayload::Command { operation, bytes }
            if operation.get() == 300001 && bytes == &0i64.to_le_bytes())
            );
            assert_eq!(
                local.applications[&group].applied_index(),
                state.commit_index
            );
            state.clone()
        })
        .collect();
    Facts {
        top,
        top_bindings: host
            .nodes
            .iter()
            .map(|node| node.local().owner.identity().store)
            .collect(),
        middle_bindings,
    }
}

fn restored(host: &Host<'_, '_>, facts: &Facts) {
    assert_eq!(host.nodes.len(), facts.top.len());
    let group = setup::top().group;
    for ((node, expected), old) in host.nodes.iter().zip(&facts.top).zip(&facts.top_bindings) {
        let local = node.local();
        assert_eq!(local.owner.core(group).unwrap().state(), expected);
        assert_eq!(local.owner.identity().store.identity, old.identity);
        assert!(local.owner.identity().store.session > old.session);
        assert_eq!(
            local.applications[&group].applied_index(),
            expected.commit_index
        );
        assert_eq!(
            local.applications[&group]
                .read_at(expected.commit_index, ())
                .unwrap(),
            5
        );
    }
    eprintln!("cold nested original upper anchor/suffix/application restored before election");
}
