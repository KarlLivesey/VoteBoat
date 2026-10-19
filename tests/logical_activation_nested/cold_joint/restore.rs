// SPDX-License-Identifier: RPL-1.5
// Copyright (c) 2026 Karl Livesey
//! New owners obtain new Claim provenance, never reuse a consumed continuation.
use super::*;
use crate::{business, channels::Channels, parts, support};
use voteboat::{application::CounterReceipt, outbound::OutboundQueue};
pub(super) fn complete(
    clusters: &mut [Cluster],
    protocol: NativePeerProtocol,
    clock: &SimClock,
    facts: Facts,
) {
    let mut lower = Lower::new(clusters, clock);
    let images = (0..4)
        .map(|slot| lower.claim(slot, 367110 + slot as u128))
        .collect::<Vec<_>>();
    for ((image, old), binding) in images.iter().zip(&facts.middle).zip(&facts.bindings) {
        assert_eq!(&image.image().parent, old);
        assert_eq!(image.binding().identity, binding.identity);
        assert!(image.binding().session > binding.session);
        assert!(image
            .image()
            .parent
            .entries
            .iter()
            .any(|e| e.index <= old.commit_index));
    }
    let (mut middle, channels) = middle(&lower, &images, protocol);
    progress::elect_middle(&mut lower, &mut middle);
    let mut bridge =
        UpperBridge::new(&mut middle, setup::upper_mapping(), support::node(201)).unwrap();
    let upper = claim(&mut lower, &mut bridge);
    assert_eq!(upper.image().parent, facts.top);
    assert_eq!(upper.binding().identity, facts.top_binding.identity);
    assert!(upper.binding().session > facts.top_binding.session);
    let mut top = Top::from_parts(
        &upper,
        parts::build(&upper, setup::top_app(), setup::upper_limits(), lower.now()),
        parts::node_limits::<ActivationReceipt<CounterReceipt>, ActivationRead<i64>>(
            setup::upper_limits(),
        ),
        lower.now(),
    )
    .unwrap_or_else(|e| panic!("fresh recovered top {:?}", e.reason));
    let status = top.local().applications[&setup::top().group]
        .status(setup::intent().operations().begin)
        .unwrap();
    assert_eq!(status.begin.first, facts.begin);
    assert!(status.outcome.is_none() && status.release.is_none());
    progress::elect_top(&mut lower, &mut bridge, &mut top);
    let mut driver = history::begin_driver(&mut top, setup::intent());
    progress::phase(
        &mut lower,
        &mut bridge,
        &mut driver,
        4,
        ChildActivationDriverPhase::Released,
    );
    assert_eq!(driver.original_begin_position(), Some(facts.begin));
    let returned = driver
        .try_reclaim()
        .unwrap_or_else(|_| panic!("fresh driver original observations drained"));
    history::terminal(returned, &bridge, &setup::intent());
    business::request(&mut lower, &mut bridge, returned, 4, 356300, 7, 7);
    business::request(&mut lower, &mut bridge, returned, 4, 367301, 5, 12);
    super::super::shutdown::close(&mut lower, &mut bridge, returned, 4);
    bridge.close();
    assert!(bridge.observations_are_drained());
    drop(bridge);
    drop(
        top.into_parts()
            .unwrap_or_else(|_| panic!("fresh parent settled")),
    );
    // One full rebuilt catalog owns both original and learner connections.
    super::super::shutdown::middle(&mut lower, &mut middle);
    let sessions = middle
        .into_iter()
        .flat_map(|n| {
            n.into_parts()
                .unwrap_or_else(|_| panic!("fresh Middle settled"))
                .sessions
                .into_values()
        })
        .collect::<Vec<_>>();
    channels.close(sessions);
    lower.close();
}
fn middle(
    lower: &Lower<'_>,
    images: &[VerifiedLogicalImage],
    protocol: NativePeerProtocol,
) -> (Vec<Middle>, Channels) {
    let descriptors = images
        .iter()
        .enumerate()
        .map(|(i, v)| LogicalPeerDescriptor::from_verified(v, &setup::lower_app(i)).unwrap())
        .collect::<Vec<_>>();
    let mut originals = images
        .iter()
        .map(|v| parts::build(v, setup::upper_app(), setup::lower_limits(), lower.now()))
        .collect::<Vec<_>>();
    let bindings = originals
        .iter()
        .map(|p| (p.local.owner.authorization(), p.local.outbound.binding()))
        .collect::<Vec<_>>();
    let mut channels = Channels::new(&descriptors, &bindings, protocol, &lower.clock, false);
    let nodes = originals
        .drain(..)
        .enumerate()
        .map(|(i, mut p)| {
            p.sessions = channels.take_sessions(i, &descriptors);
            Middle::from_parts(
                &images[i],
                p,
                parts::node_limits::<LogicalReceipt, LogicalRead>(setup::lower_limits()),
                lower.now(),
            )
            .unwrap_or_else(|e| panic!("fresh Joint Middle {:?}", e.reason))
        })
        .collect();
    (nodes, channels)
}
fn claim(lower: &mut Lower<'_>, bridge: &mut UpperBridge<'_>) -> VerifiedLogicalImage {
    bridge
        .begin_claim(voteboat::identity::OperationId::new(367200).unwrap())
        .unwrap();
    let end = harness::Wait::new(&lower.clock, harness::LIVENESS_MS);
    loop {
        lower.pump_bridge(bridge, 4);
        lower.advance_with(harness::Observed::default().backing(bridge));
        lower.pump_bridge(bridge, 4);
        bridge.advance(lower.now(), progress::budget()).unwrap();
        if let Some(image) = bridge.take_image() {
            return image;
        }
        assert!(end.within(&lower.clock), "fresh upper Claim at Joint");
        // A CPU yield like every other fixture loop's; no timing depends on it.
        std::thread::park_timeout(std::time::Duration::from_millis(1));
    }
}
