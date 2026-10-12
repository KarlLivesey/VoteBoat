// SPDX-License-Identifier: RPL-1.5
// Copyright (c) 2026 Karl Livesey
//! Second opaque source grant and candidate, keeping the first lineage current.
use super::{
    channels::Channels, lower::Lower, parts, progress, setup, support, Middle, Top, UpperBridge,
};
use std::time::{Duration, Instant};
use voteboat::{
    identity::*, logical_voter::*, native::connect::NativePeerProtocol, outbound::OutboundQueue,
    secure::SecureSession,
};
pub type Session = LogicalPeerSession<Box<dyn SecureSession>>;
pub fn prepare<'a, 'p>(
    lower: &mut Lower<'_>,
    mut bridge: UpperBridge<'a>,
    top: &'p mut Top,
) -> (
    ChildActivationDriver<'p, Top>,
    LogicalActivationContinuation,
    voteboat::runtime::SnapshotExportCompletion,
) {
    let intent = setup::second_intent();
    let mut driver = ChildActivationDriver::new(
        top,
        intent.clone(),
        ChildActivationDriverLimits {
            retained_bytes: 64 * 1024 * 1024,
            ..Default::default()
        },
    )
    .unwrap_or_else(|e| panic!("second driver {:?}", e.reason));
    progress::phase(
        lower,
        &mut bridge,
        &mut driver,
        4,
        ChildActivationDriverPhase::Enrolling,
    );
    let output = super::enrollment::export_for(
        lower,
        &mut bridge,
        &mut driver,
        4,
        intent.learners_configuration(),
    );
    let (continuation, _) = super::enrollment::detach_for(lower, bridge, &mut driver, 4);
    (driver, continuation, output)
}
pub fn finish<'a>(
    lower: &mut Lower<'_>,
    nodes: &'a mut Vec<Middle>,
    mut driver: ChildActivationDriver<'_, Top>,
    continuation: LogicalActivationContinuation,
    output: voteboat::runtime::SnapshotExportCompletion,
    descriptors: &[LogicalPeerDescriptor],
    protocol: NativePeerProtocol,
) -> (UpperBridge<'a>, Channels) {
    let intent = driver.intent().clone();
    let begin = driver.original_begin_position().unwrap();
    retire(lower, nodes, support::node(203));
    let grant = super::controls::grant_for(&mut driver, nodes, output, 4);
    let image = target(lower, nodes, grant);
    let channels = attach(lower, nodes, descriptors, &image, protocol);
    let mut bridge = driver
        .attach_enrolled_child(continuation, nodes)
        .unwrap_or_else(|e| panic!("second original children {:?}", e.reason));
    progress::phase(
        lower,
        &mut bridge,
        &mut driver,
        5,
        ChildActivationDriverPhase::Released,
    );
    assert_eq!(driver.original_begin_position(), Some(begin));
    let top = driver
        .try_reclaim()
        .unwrap_or_else(|_| panic!("second original parent drained"));
    let status = top.local().applications[&setup::top().group]
        .status(intent.operations().begin)
        .unwrap();
    assert_eq!(status.intent, intent);
    assert!(status.outcome.is_some() && status.release.is_some());
    for slot in [0, 3, 4] {
        bridge
            .inspect_backing(slot, |p| {
                let core = p.owner.core(setup::middle().group).unwrap();
                assert_eq!(core.membership().id(), ConfigurationId::new(7).unwrap());
                assert_eq!(
                    core.membership()
                        .stable()
                        .voter_stores()
                        .keys()
                        .copied()
                        .collect::<Vec<_>>(),
                    [201, 204, 205].map(support::node)
                );
            })
            .unwrap();
    }
    (bridge, channels)
}
fn retire(lower: &mut Lower<'_>, nodes: &mut [Middle], peer: NodeId) {
    let original = nodes[0].local().outbound.usage();
    assert_eq!(
        nodes[0].retire_peer_session(support::node(202)).err(),
        Some(LogicalNodeError::NotQuiescent)
    );
    assert_eq!(nodes[0].local().outbound.usage(), original);
    let mut pending = vec![0, 1, 3];
    let end = Instant::now() + Duration::from_secs(15);
    while !pending.is_empty() {
        let mut next = Vec::new();
        for slot in pending {
            match nodes[slot].retire_peer_session(peer) {
                Ok(session) => {
                    assert!(session.work_is_drained());
                    assert_eq!(
                        nodes[slot].local().outbound.peer_usage(peer),
                        Default::default()
                    );
                    lower.retain_retired(session, nodes[slot].local().owner.authorization());
                }
                Err(LogicalNodeError::NotQuiescent) => next.push(slot),
                Err(error) => panic!("genuine obsolete session {error:?}"),
            }
        }
        pending = next;
        if !pending.is_empty() {
            lower.middle(nodes);
        }
        assert!(Instant::now() < end, "original obsolete session drain");
    }
}
fn target(
    lower: &mut Lower<'_>,
    nodes: &mut [Middle],
    grant: LogicalLearnerEnrollment,
) -> VerifiedLogicalImage {
    lower.bridges[4]
        .begin_enrollment(OperationId::new(356250).unwrap(), grant)
        .unwrap_or_else(|e| panic!("second original import {:?}", e.reason));
    let original = nodes
        .iter()
        .map(super::controls::deferred)
        .collect::<Vec<_>>();
    assert!(original.iter().any(|b| !b.is_empty()));
    let end = Instant::now() + Duration::from_secs(15);
    loop {
        lower.middle(nodes);
        if let Some(image) = lower.bridges[4].take_image() {
            for (node, held) in nodes.iter().zip(&original) {
                let current = super::controls::deferred(node);
                assert!(held.iter().all(|batch| current.contains(batch)));
            }
            assert_eq!(image.mapping(), setup::lower_mapping(4));
            return image;
        }
        assert!(
            Instant::now() < end,
            "second Applied/fresh Enrollment/Image/Snapshot"
        );
    }
}
fn attach(
    lower: &Lower<'_>,
    nodes: &mut Vec<Middle>,
    descriptors: &[LogicalPeerDescriptor],
    image: &VerifiedLogicalImage,
    protocol: NativePeerProtocol,
) -> Channels {
    let selected = [0, 1, 3];
    let mut descriptors = selected
        .into_iter()
        .map(|i| descriptors[i].clone())
        .collect::<Vec<_>>();
    descriptors.push(LogicalPeerDescriptor::from_verified(image, &setup::lower_app(4)).unwrap());
    let target = parts::build(
        image,
        setup::upper_app(),
        setup::lower_limits(),
        lower.now(),
    );
    let bindings = selected
        .into_iter()
        .map(|i| {
            (
                nodes[i].local().owner.authorization(),
                nodes[i].local().outbound.binding(),
            )
        })
        .chain(std::iter::once((
            target.local.owner.authorization(),
            target.local.outbound.binding(),
        )))
        .collect::<Vec<_>>();
    let mut channels = Channels::new(&descriptors, &bindings, protocol, lower.now(), true);
    for (slot, node) in selected.into_iter().enumerate() {
        let session = channels
            .take_sessions(slot, &descriptors)
            .remove(&support::node(205))
            .unwrap();
        nodes[node]
            .attach_peer_session(session)
            .unwrap_or_else(|e| panic!("second target attach {:?}", e.reason));
    }
    let mut target = target;
    target.sessions = channels.take_sessions(3, &descriptors);
    nodes.push(
        Middle::from_parts(
            image,
            target,
            parts::node_limits::<LogicalReceipt, LogicalRead>(setup::lower_limits()),
            lower.now(),
        )
        .unwrap_or_else(|e| panic!("second candidate owner {:?}", e.reason)),
    );
    channels
}
