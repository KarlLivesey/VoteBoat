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
//! Real native outer connectors yield the sessions owned by each parent facade.
use super::*;
use voteboat::outbound::OutboundQueue;
pub fn parents(
    images: &[VerifiedLogicalImage; 3],
    protocol: NativePeerProtocol,
    clock: &harness::SimClock,
) -> (channels::Channels, [fixture::FacadeNode; 3]) {
    let now = harness::FixtureClock::now(clock);
    let descriptors = images
        .iter()
        .enumerate()
        .map(|(i, image)| {
            LogicalPeerDescriptor::from_verified(image, &groups::application(i)).unwrap()
        })
        .collect::<Vec<_>>();
    let mut parts = images
        .iter()
        .map(|image| {
            snapshots::attach(
                fixture::build_with_timers(image, now, groups::timers()),
                image,
            )
        })
        .collect::<Vec<_>>();
    let bindings = parts
        .iter()
        .map(|p| (p.local.owner.authorization(), p.local.outbound.binding()))
        .collect::<Vec<_>>();
    let mut channels = channels::Channels::new(&descriptors, &bindings, protocol, clock);
    for (i, part) in parts.iter_mut().enumerate() {
        part.sessions = channels.take_sessions(i, &descriptors);
    }
    let parents = parts
        .into_iter()
        .zip(images)
        .map(|(parts, image)| {
            LogicalNode::from_parts(image, parts, fixture::limits(), now)
                .unwrap_or_else(|e| panic!("actual three-parent assembly {:?}", e.reason))
        })
        .collect::<Vec<_>>()
        .try_into()
        .unwrap_or_else(|_| panic!("exact three parents"));
    (channels, parents)
}
