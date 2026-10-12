// SPDX-License-Identifier: RPL-1.5
// Copyright (c) 2026 Karl Livesey
use super::{channels::Channels, lower::Lower, parts, setup, Middle};
use voteboat::{logical_voter::*, native::connect::NativePeerProtocol, outbound::OutboundQueue};
pub fn originals(
    lower: &Lower<'_>,
    images: &[VerifiedLogicalImage],
    protocol: NativePeerProtocol,
) -> (Vec<Middle>, Channels, Vec<LogicalPeerDescriptor>) {
    let descriptors = images
        .iter()
        .enumerate()
        .map(|(slot, v)| LogicalPeerDescriptor::from_verified(v, &setup::lower_app(slot)).unwrap())
        .collect::<Vec<_>>();
    let mut originals = images
        .iter()
        .map(|v| parts::build(v, setup::upper_app(), setup::lower_limits(), lower.now()))
        .collect::<Vec<_>>();
    let bindings = originals
        .iter()
        .map(|p| (p.local.owner.authorization(), p.local.outbound.binding()))
        .collect::<Vec<_>>();
    let mut original_channels =
        Channels::new(&descriptors, &bindings, protocol, lower.now(), false);
    let middle = originals
        .drain(..)
        .enumerate()
        .map(|(slot, mut p)| {
            p.sessions = original_channels.take_sessions(slot, &descriptors);
            Middle::from_parts(
                &images[slot],
                p,
                parts::node_limits::<LogicalReceipt, LogicalRead>(setup::lower_limits()),
                lower.now(),
            )
            .unwrap_or_else(|e| panic!("actual middle {:?}", e.reason))
        })
        .collect::<Vec<_>>();

    (middle, original_channels, descriptors)
}
pub fn target(
    lower: &Lower<'_>,
    middle: &mut Vec<Middle>,
    descriptors: &mut Vec<LogicalPeerDescriptor>,
    target_image: &VerifiedLogicalImage,
    protocol: NativePeerProtocol,
) -> Channels {
    descriptors
        .push(LogicalPeerDescriptor::from_verified(target_image, &setup::lower_app(3)).unwrap());
    let mut target = parts::build(
        target_image,
        setup::upper_app(),
        setup::lower_limits(),
        lower.now(),
    );
    let bindings = middle
        .iter()
        .map(|n| {
            (
                n.local().owner.authorization(),
                n.local().outbound.binding(),
            )
        })
        .chain(std::iter::once((
            target.local.owner.authorization(),
            target.local.outbound.binding(),
        )))
        .collect::<Vec<_>>();
    let mut target_channels = Channels::new(descriptors, &bindings, protocol, lower.now(), true);
    for slot in 0..middle.len() {
        let mut sessions = target_channels.take_sessions(slot, descriptors);
        assert_eq!(sessions.len(), 1);
        super::controls::attach(
            middle,
            slot,
            sessions.remove(&super::support::node(204)).unwrap(),
        );
    }
    target.sessions = target_channels.take_sessions(3, descriptors);
    middle.push(
        Middle::from_parts(
            target_image,
            target,
            parts::node_limits::<LogicalReceipt, LogicalRead>(setup::lower_limits()),
            lower.now(),
        )
        .unwrap_or_else(|e| panic!("actual learner Node {:?}", e.reason)),
    );

    target_channels
}
