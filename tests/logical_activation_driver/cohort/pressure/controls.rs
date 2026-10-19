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
//! Separate original observer refusal controls with independent lifetime budget.
use super::*;
#[test]
fn genuine_native_observer_refusals_preserve_original_ownership_tcp() {
    let _serial = crate::history_guard();
    let protocol = NativePeerProtocol::TcpTls;
    let clock = &harness::SimClock::default();
    let mut target_members = members::Members::new(protocol, 0, clock);
    let mut own_members = members::Members::new(protocol, 1, clock);
    let mut sibling_members = members::Members::new(protocol, 2, clock);
    let mut target = history::bridge(&mut target_members, 0);
    let mut own = history::bridge(&mut own_members, 1);
    let mut sibling = history::bridge(&mut sibling_members, 2);
    let images = [
        drive::claim_in(&mut target, clock, 33501, false),
        drive::claim_in(&mut own, clock, 33502, false),
        drive::claim_in(&mut sibling, clock, 33503, false),
    ];
    let (channels, [left, mut parent, right]) = construction::parents(&images, protocol, clock);
    let mut peers = progress::Peers {
        parents: [left, right],
        bridges: [own, sibling],
        clock: clock.clone(),
    };
    peers.elect(&mut parent, &mut target);
    ownership::cancellation(&mut peers, &mut parent, &mut target);
    // Held::new tests original accepted/in-flight and unexported-ready refusals.
    let mut held = holding::Held::new(&mut peers, &mut parent, &mut target);
    held.reject_foreign(&mut peers, &mut parent);
    guards::stale(&held, &mut parent);
    guards::ordinary(&mut parent);
    guards::control(&mut peers, &mut parent, &mut target);
    held.assert_retained(&parent);
    held.complete(&mut parent);
    let [left, right] = cleanup::close(peers, &mut parent, &mut target);
    drop(target);
    let middle = parent
        .into_parts()
        .unwrap_or_else(|_| panic!("actual parent drain"));
    channels.close(
        left.sessions
            .into_values()
            .chain(middle.sessions.into_values())
            .chain(right.sessions.into_values()),
    );
    target_members.finish();
    own_members.finish();
    sibling_members.finish();
}
