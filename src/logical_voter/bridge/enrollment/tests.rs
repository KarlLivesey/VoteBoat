// SPDX-License-Identifier: RPL-1.5
// Copyright (c) 2026 Karl Livesey
//! Actual target-side durability/read observations; source fixture is not a grant proof.
use super::*;
use crate::application::{CheckpointStateMachine, StateMachine};
use crate::native::connect::NativePeerProtocol;
use crate::{
    log_format::NativeLogCodec, snapshot_format::NativeSnapshotCodec, wire_format::NativeWireCodec,
};
use std::time::{Duration, Instant};
mod cluster;
mod controls;
mod fixture;

#[test]
fn native_tcp_candidate_original_applied_and_fresh_enrollment_image_snapshot() {
    history(NativePeerProtocol::TcpTls, false);
}
#[cfg(feature = "quic")]
#[test]
fn native_quic_candidate_original_unknown_and_owned_recovery_fresh_reads() {
    history(NativePeerProtocol::Quic, true);
}
fn budget(completions: usize) -> LogicalBridgeBudget {
    LogicalBridgeBudget {
        node_polls: 3,
        completions,
        transitions: 1,
        node: NodePollBudget::default(),
    }
}
fn tick(bridge: &mut LogicalBackingBridge<'_, cluster::Child>, clock: Instant, completions: usize) {
    if let Err(error) = bridge.advance(
        MonoTime(clock.elapsed().as_millis() as u64),
        budget(completions),
    ) {
        let phase = bridge.inspect_work(|w| w.map(|w| (w.phase(), w.attempt(), w.operation())));
        let bytes = bridge.inspect_work(|w| w.and_then(|w| w.command_bytes().map(<[u8]>::to_vec)));
        let records: Vec<_> = (0..3)
            .map(|n| {
                bridge
                    .inspect_backing(n, |local| {
                        let a = &local.applications[&fixture::mapping().child];
                        let c = local.owner.core(fixture::mapping().child).unwrap();
                        (
                            c.role(),
                            c.state().commit_index,
                            a.applied_index(),
                            a.state
                                .records
                                .get(&OperationId::new(11).unwrap())
                                .map(|r| {
                                    (r.first_index, r.receipt, Some(&r.command) == bytes.as_ref())
                                }),
                        )
                    })
                    .unwrap()
            })
            .collect();
        panic!("actual target error {error:?}, phase {phase:?}, records {records:?}");
    }
    std::thread::park_timeout(Duration::from_millis(1));
}
fn wait_phase(
    bridge: &mut LogicalBackingBridge<'_, cluster::Child>,
    clock: Instant,
    phase: LogicalBridgeWorkPhase,
) {
    let end = Instant::now() + Duration::from_secs(15);
    while !bridge.inspect_work(|w| w.is_some_and(|w| w.phase() == phase && w.attempt().is_some())) {
        tick(bridge, clock, 1);
        assert!(Instant::now() < end, "actual target phase {phase:?}");
    }
}
fn history(protocol: NativePeerProtocol, cancel: bool) {
    let app = fixture::application();
    let enrollment = fixture::enrollment(&app);
    let source_bytes = enrollment.import.snapshot.application.clone();
    let source_generation = enrollment.import.origin.export.reference.generation;
    let mut host = cluster::Cluster::new(protocol, app.clone());
    let clock = host.clock;
    let mut bridge =
        LogicalBackingBridge::new(&mut host.nodes, fixture::mapping(), fixture::node(1)).unwrap();
    controls::pre_intake(&mut bridge, &app);
    bridge
        .begin_enrollment(OperationId::new(11).unwrap(), enrollment)
        .unwrap();
    assert!(bridge.take_image().is_none());
    bridge.stop_enrollment_waiting().unwrap();
    let (recovery, nodes) = bridge
        .into_enrollment_recovery()
        .unwrap_or_else(|_| panic!("unadmitted original input handback"));
    let mut wrong = LogicalBackingBridge::new(nodes, fixture::mapping(), fixture::node(2)).unwrap();
    let recovery = match wrong.resume_enrollment(recovery) {
        Err(rejected) => {
            assert_eq!(rejected.reason, LogicalBridgeError::StaleRepresentative);
            rejected.recovery
        }
        Ok(()) => panic!("different selected representative cannot resume original work"),
    };
    let nodes = wrong.nodes;
    let mut bridge =
        LogicalBackingBridge::new(nodes, fixture::mapping(), fixture::node(1)).unwrap();
    bridge
        .resume_enrollment(recovery)
        .unwrap_or_else(|_| panic!("same original streams resume"));
    wait_phase(&mut bridge, clock, LogicalBridgeWorkPhase::Applying);
    let original = bridge.inspect_work(|w| w.unwrap().command_bytes().unwrap().to_vec());
    if cancel {
        bridge = controls::cancel_admitted(bridge, clock, &original);
    }
    let end = Instant::now() + Duration::from_secs(15);
    while !bridge
        .inspect_backing(0, |local| {
            local.applications[&fixture::mapping().child]
                .state
                .enrollment
                .is_some()
        })
        .unwrap()
    {
        tick(&mut bridge, clock, 0);
        assert!(bridge.take_image().is_none());
        assert!(
            Instant::now() < end,
            "actual import applied with observations held"
        );
    }
    assert!(!bridge.observations_are_drained());
    let image = loop {
        tick(&mut bridge, clock, 1);
        if let Some(image) = bridge.take_image() {
            break image;
        }
        assert!(Instant::now() < end, "actual fresh candidate startup reads");
    };
    controls::verified(&image, &app, &source_bytes, source_generation);
    bridge.begin_claim(OperationId::new(14).unwrap()).unwrap();
    let reclaimed = loop {
        tick(&mut bridge, clock, 1);
        if let Some(image) = bridge.take_image() {
            break image;
        }
        assert!(Instant::now() < end, "actual normal candidate reclaim");
    };
    controls::reclaimed(&image, &reclaimed, &app);
    bridge.close();
    assert!(bridge.is_drained());
    drop(bridge);
    host.finish();
}
