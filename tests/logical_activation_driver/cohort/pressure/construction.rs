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
//! Dedicated selected service pressure; original protocol and provider limits.
use super::*;
use voteboat::{application::CounterReceipt, outbound::OutboundQueue};
pub fn parents(
    images: &[VerifiedLogicalImage; 3],
    protocol: NativePeerProtocol,
    now: MonoTime,
) -> (channels::Channels, [fixture::FacadeNode; 3]) {
    let descriptors = images
        .iter()
        .enumerate()
        .map(|(i, image)| {
            LogicalPeerDescriptor::from_verified(image, &groups::application(i)).unwrap()
        })
        .collect::<Vec<_>>();
    let mut parts = images
        .iter()
        .enumerate()
        .map(|(i, image)| {
            let mut parts = snapshots::attach(
                fixture::build_with_timers(image, now, groups::timers()),
                image,
            );
            if i == 1 {
                let routers: LogicalRouterParts<
                    ActivationReceipt<CounterReceipt>,
                    ActivationQuery<()>,
                    ActivationRead<i64>,
                > = image.routers(&parts.local.owner, limits()).unwrap();
                parts.local.clients = routers.clients;
                parts.local.reads = routers.reads;
            }
            parts
        })
        .collect::<Vec<_>>();
    let bindings = parts
        .iter()
        .map(|p| (p.local.owner.authorization(), p.local.outbound.binding()))
        .collect::<Vec<_>>();
    let mut channels = channels::Channels::new(&descriptors, &bindings, protocol, now);
    for (i, part) in parts.iter_mut().enumerate() {
        part.sessions = channels.take_sessions(i, &descriptors);
    }
    let parents = parts
        .into_iter()
        .zip(images)
        .map(|(parts, image)| {
            LogicalNode::from_parts(image, parts, fixture::limits(), now)
                .unwrap_or_else(|e| panic!("selected pressure assembly {:?}", e.reason))
        })
        .collect::<Vec<_>>()
        .try_into()
        .unwrap_or_else(|_| panic!("three parents"));
    (channels, parents)
}
fn limits() -> LogicalRouterLimits {
    let mut limits = fixture::router_limits();
    limits.clients = ClientRouterLimits {
        requests: 4,
        group_requests: 4,
        bytes: 128 * 1024,
        command_bytes: 1024,
        receipt_bytes: 8192,
        control_requests: 2,
        group_control_requests: 2,
        control_bytes: 64 * 1024,
    };
    limits.invocations = ReadInvocationLimits {
        requests: 4,
        group_requests: 4,
        bytes: 128 * 1024,
        query_bytes: 1024,
        result_bytes: 8192,
        control_requests: 2,
        group_control_requests: 2,
        control_bytes: 64 * 1024,
    };
    limits.execution = ReadRouterLimits {
        results: 4,
        group_results: 4,
        bytes: 128 * 1024,
        query_bytes: 1024,
        result_bytes: 8192,
        control_results: 2,
        group_control_results: 2,
        control_bytes: 64 * 1024,
    };
    limits
}
