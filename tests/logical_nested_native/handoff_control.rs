// SPDX-License-Identifier: RPL-1.5
// Copyright (c) 2026 Karl Livesey
//! Original bridge observations settle while an actual child checkpoint stays owned.
use super::{
    aliases::Top,
    cluster::{serial, Cluster},
    host::Host,
    lower::Lower,
    middle, setup,
};
use std::time::{Duration, Instant};
use voteboat::{
    logical_voter::*, native::connect::NativePeerProtocol, snapshot_worker::SnapshotWorker,
};
#[path = "handoff_control/held.rs"]
mod held;
#[test]
fn tcp_observation_handoff_retains_original_child_checkpoint_until_actual_drain() {
    let _serial = serial();
    let clock = Instant::now();
    let protocol = NativePeerProtocol::TcpTls;
    let mut bottom = (0..9)
        .map(|slot| Cluster::new(protocol, slot, clock))
        .collect::<Vec<_>>();
    let mut lower = Lower::new(&mut bottom, clock);
    let images = lower.claims(410000);
    let (mut middle, channels) = middle::build(&images, protocol, &mut lower);
    middle::elect(&mut lower, &mut middle);
    let mut host = Host::new(lower, &mut middle, protocol, 420000, &channels);
    assert!(host.upper.iter().all(|b| b.observations_are_drained()));
    host.phase = "close";
    for node in &mut host.nodes {
        node.close_intake().unwrap();
    }
    host.upper[0].checkpoint_backing(0).unwrap();
    let end = Instant::now() + Duration::from_secs(15);
    while host.upper[0]
        .inspect_backing(0, |p| p.snapshots.as_ref().unwrap().worker.usage().requests)
        .unwrap()
        == 0
    {
        host.poll();
        assert!(
            Instant::now() < end,
            "actual original child checkpoint must be accepted"
        );
    }
    host.lower.held = true;
    let held = held::capture(&mut host);
    host.until(|h| h.nodes.iter().all(Top::is_drained));
    assert!(host.upper.iter().all(|b| b.observations_are_drained()));
    assert!(!host.upper[0].is_drained());
    for _ in 0..16 {
        host.poll();
        held.assert_same(&mut host);
    }
    let mut lower = host.reclaim_closed();
    let local = middle[0][0].local();
    assert_eq!(local.owner.identity(), held.owner);
    assert_eq!(local.snapshots.as_ref().unwrap().worker.usage(), held.usage);
    assert_eq!(
        local
            .owner
            .core(setup::middle(0).group)
            .unwrap()
            .state()
            .snapshot,
        held.snapshot
    );
    assert!(!middle[0][0].work_is_drained());
    middle[0][0]
        .pump_backing(|p| assert_eq!(p.snapshots.unwrap().pending_request(), Some(held.request)));
    lower.held = false;
    let states = middle::close(&mut lower, middle, channels);
    assert!(states[0].snapshot.is_some());
    assert!(states[0].base_index() >= held.frontier);
    assert!(lower.drained());
    drop(lower);
    for mut child in bottom {
        child.checkpoint();
        child.finish();
    }
}
