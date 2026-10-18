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
use voteboat::snapshot_worker::SnapshotWorker;

#[test]
fn logical_facade_close_drives_original_held_checkpoint_tcp() {
    history(NativePeerProtocol::TcpTls);
}
#[cfg(feature = "quic")]
#[test]
fn logical_facade_close_drives_original_held_checkpoint_quic() {
    history(NativePeerProtocol::Quic);
}

fn history(protocol: NativePeerProtocol) {
    let _serial = serial();
    let mut cohort = Cluster::new_with_application(protocol, fixture::child_application());
    let clock = cohort.clock;
    let mut bridge =
        LogicalChildBridge::new(&mut cohort.nodes, setup::mapping(), support::node(1)).unwrap();
    let image = claim_in(&mut bridge, clock, 9000, false);
    let mut node = FacadeNode::from_parts(
        &image,
        fixture::attach_snapshots(build(&image), &image),
        fixture::limits(),
        MonoTime(0),
    )
    .unwrap_or_else(|e| panic!("actual shutdown facade {:?}", e.reason));
    restart::elect(&mut bridge, &mut node, clock);
    assert_eq!(
        restart::write(&mut bridge, &mut node, clock, 9001, 5).outcome,
        CounterOutcome::Value(5)
    );
    let before = node
        .local()
        .owner
        .core(setup::mapping().parent)
        .unwrap()
        .state()
        .clone();
    node.checkpoint().unwrap();
    until(&mut bridge, &mut node, clock, |n| {
        n.local()
            .snapshots
            .as_ref()
            .unwrap()
            .worker
            .usage()
            .requests
            == 1
    });
    node.close_intake().unwrap();
    shutdown_step(&mut bridge, &mut node, clock, 0);
    assert_eq!(node.state(), NodeState::Draining);
    assert_eq!(node.local().owner.deadline(setup::mapping().parent), None);
    assert!(!node.work_is_drained());
    for _ in 0..16 {
        shutdown_step(&mut bridge, &mut node, clock, 0);
        assert_eq!(node.state(), NodeState::Draining);
        let local = node.local();
        let state = local.owner.core(setup::mapping().parent).unwrap().state();
        assert_eq!(state.snapshot, before.snapshot);
        assert_eq!(state.base_index(), before.base_index());
        assert_eq!(local.snapshots.as_ref().unwrap().worker.usage().requests, 1);
        assert!(!node.work_is_drained());
    }
    shutdown_until(&mut bridge, &mut node, clock);
    let state = node
        .local()
        .owner
        .core(setup::mapping().parent)
        .unwrap()
        .state();
    let reference = state
        .snapshot
        .expect("original checkpoint bound during close");
    assert_eq!(reference.index, before.commit_index);
    assert_eq!(state.base_index(), reference.index);
    assert!(state.entries.is_empty());
    let parts = node
        .into_parts()
        .unwrap_or_else(|_| panic!("original checkpoint work must fully drain"));
    assert!(parts.persistence.is_drained());
    assert!(parts.snapshots.as_ref().unwrap().is_drained());
    bridge.close();
    assert!(bridge.is_drained());
    drop(bridge);
    drop(parts);
    cohort.finish();
}

fn shutdown_step(
    bridge: &mut Bridge<'_>,
    node: &mut FacadeNode,
    clock: Instant,
    completions: usize,
) {
    node.pump_backing(|p| {
        bridge.pump_persistence(p.persistence).unwrap();
        bridge.pump_release(p.releases).unwrap();
        if let Some(snapshots) = p.snapshots {
            bridge.pump_snapshots(snapshots, p.persistence).unwrap();
        }
    });
    bridge
        .advance(
            MonoTime(clock.elapsed().as_millis() as u64),
            budget(completions),
        )
        .unwrap();
    node.pump_backing(|p| {
        bridge.pump_persistence(p.persistence).unwrap();
        bridge.pump_release(p.releases).unwrap();
        if let Some(snapshots) = p.snapshots {
            bridge.pump_snapshots(snapshots, p.persistence).unwrap();
        }
    });
    let progress = node
        .poll_shutdown(MonoTime(0), LogicalNodePollBudget::default())
        .unwrap_or_else(|r| {
            panic!(
                "original checkpoint shutdown {:?}, progress {:?}",
                r.reason, r.progress
            )
        });
    assert!(
        progress.completions.is_empty(),
        "no parent network sessions in this fixture"
    );
    assert!(progress.peer_closures.is_empty());
}
fn shutdown_until(bridge: &mut Bridge<'_>, node: &mut FacadeNode, clock: Instant) {
    let end = Instant::now() + Duration::from_secs(15);
    while !node.is_drained() {
        shutdown_step(bridge, node, clock, 4);
        assert!(Instant::now() < end, "actual logical facade progress");
        std::thread::park_timeout(Duration::from_millis(1));
    }
}
