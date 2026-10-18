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
//! Actual single logical voter backed by three physical voters. No peer replies
//! are fabricated. Full recursive multi-voter qualification is a separate target.
use super::{
    aliases::Bridge,
    cluster::{serial, Cluster},
    drive::claim_in,
    setup, support,
};
use std::time::{Duration, Instant};
use voteboat::{
    application::*, logical_voter::*, native::connect::NativePeerProtocol, raft::Role, runtime::*,
};
#[path = "facade/abort.rs"]
mod abort;
#[path = "facade/administration.rs"]
mod administration;
#[path = "facade/cancel.rs"]
mod cancel;
#[path = "facade/construction_capacity.rs"]
mod construction_capacity;
#[path = "facade/exports.rs"]
mod exports;
#[path = "facade/fixture.rs"]
mod fixture;
#[path = "facade/histories.rs"]
mod histories;
#[path = "facade/restart.rs"]
mod restart;
#[path = "facade/shutdown.rs"]
mod shutdown;
#[path = "facade/shutdown_service.rs"]
mod shutdown_service;
use fixture::{budget, build, FacadeNode};

#[test]
fn logical_facade_original_provider_ownership_and_result_gates_tcp() {
    history(NativePeerProtocol::TcpTls);
}
#[cfg(feature = "quic")]
#[test]
fn logical_facade_original_provider_ownership_and_result_gates_quic() {
    history(NativePeerProtocol::Quic);
}
fn history(protocol: NativePeerProtocol) {
    let _serial = serial();
    let mut cohort = Cluster::new_with_application(protocol, fixture::child_application());
    let clock = cohort.clock;
    let image = cohort.claim(1, 8000, false);
    let parts = construction_capacity::return_original_parts(&image, build(&image));
    let original = parts.local.owner.identity();
    let rejected = FacadeNode::from_parts(
        &image,
        parts,
        LogicalNodeLimits {
            replica: ReplicaDriverLimits {
                leases: 0,
                ..ReplicaDriverLimits::default()
            },
            ..LogicalNodeLimits::default()
        },
        MonoTime(0),
    )
    .err()
    .expect("invalid driver bound");
    assert_eq!(
        rejected.reason,
        LogicalNodeError::Replica(ReplicaError::InvalidLimits)
    );
    assert_eq!(rejected.parts.local.owner.identity(), original);
    assert!(rejected.parts.persistence.is_drained());
    let mut node = FacadeNode::from_parts(&image, *rejected.parts, fixture::limits(), MonoTime(0))
        .unwrap_or_else(|e| panic!("actual logical facade: {:?}", e.reason));
    assert_eq!(
        node.endpoint_chain(setup::mapping().parent).unwrap(),
        *image.endpoint_chain()
    );
    assert_eq!(
        node.endpoint_chain(support::group(999)),
        Err(LogicalEndpointError::WrongBinding)
    );
    let mut lower =
        LogicalChildBridge::new(&mut cohort.nodes, setup::mapping(), support::node(1)).unwrap();
    claim_in(&mut lower, clock, 8001, false);
    // Reclaiming this exact member increments its logical session. The old
    // facade is not reused against that new original Claim.
    node.abort();
    let recovered = node
        .into_recovery()
        .unwrap_or_else(|_| panic!("latched recovery"));
    assert_eq!(recovered.reason, LogicalNodeError::Closed);
    assert_eq!(recovered.parts.local.owner.identity(), original);
    assert!(recovered.pending.is_empty());
    assert!(recovered.parts.persistence.is_drained());
    lower.close();
    assert!(lower.is_drained());
    drop(lower);
    drop(recovered);
    let mut bridge =
        LogicalChildBridge::new(&mut cohort.nodes, setup::mapping(), support::node(1)).unwrap();
    let current = claim_in(&mut bridge, clock, 8003, false);
    let mut node =
        FacadeNode::from_parts(&current, build(&current), fixture::limits(), MonoTime(0))
            .unwrap_or_else(|e| panic!("current facade: {:?}", e.reason));
    node.campaign().unwrap();
    for _ in 0..32 {
        step(&mut bridge, &mut node, clock, 0);
        assert!(node.poll_client().is_none());
        assert!(!node.work_is_drained());
    }
    until(&mut bridge, &mut node, clock, |n| {
        let core = n.local().owner.core(setup::mapping().parent).unwrap();
        core.role() == Role::Leader
            && core.state().term_at(core.state().commit_index) == Some(core.state().hard_state.term)
            && n.work_is_drained()
    });
    histories::requests(&mut bridge, &mut node, clock);
    histories::close_inflight(&mut bridge, &mut node, clock);
    node.close_intake().unwrap();
    until(&mut bridge, &mut node, clock, FacadeNode::is_drained);
    node.abort();
    assert_eq!(node.state(), NodeState::Drained);
    let parts = node
        .into_parts()
        .unwrap_or_else(|_| panic!("actual logical drain"));
    assert!(parts.persistence.is_drained());
    assert!(parts.sessions.is_empty());
    bridge.close();
    assert!(bridge.is_drained());
    drop(bridge);
    drop(parts);
    cohort.finish();
}
fn step(bridge: &mut Bridge<'_>, node: &mut FacadeNode, clock: Instant, completions: usize) {
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
    node.poll(MonoTime(0), LogicalNodePollBudget::default())
        .unwrap();
}
fn until(
    bridge: &mut Bridge<'_>,
    node: &mut FacadeNode,
    clock: Instant,
    mut done: impl FnMut(&FacadeNode) -> bool,
) {
    let end = Instant::now() + Duration::from_secs(15);
    while !done(node) {
        step(bridge, node, clock, 4);
        assert!(Instant::now() < end, "actual logical facade progress");
        std::thread::park_timeout(Duration::from_millis(1));
    }
}
