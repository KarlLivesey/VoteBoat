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
//! Controlled real native peer close with an original authorized partial send.
use super::*;
#[path = "close/assembly.rs"]
mod assembly;
#[path = "close/cleanup.rs"]
mod cleanup;
#[path = "close/gate.rs"]
mod gate;
#[path = "close/progress.rs"]
mod progress;
#[path = "close/rejection.rs"]
mod rejection;
#[path = "close/shutdown.rs"]
mod shutdown;
#[test]
fn running_logical_owner_retains_original_partial_send_on_real_native_peer_close() {
    let _serial = cluster::HISTORY.lock().unwrap();
    let clock = Instant::now();
    let mut first = members::Members::new(NativePeerProtocol::TcpTls, 0, clock);
    let mut second = members::Members::new(NativePeerProtocol::TcpTls, 1, clock);
    let mut third = members::Members::new(NativePeerProtocol::TcpTls, 2, clock);
    let mut a = bridge(&mut first, 0);
    let mut b = bridge(&mut second, 1);
    let mut c = bridge(&mut third, 2);
    let images = [
        drive::claim_in(&mut a, clock, 33801, false),
        drive::claim_in(&mut b, clock, 33802, false),
        drive::claim_in(&mut c, clock, 33803, false),
    ];
    let observed = gate::Observed::default();
    let (channels, [mut sender, mut opposite, mut idle]) =
        assembly::parents(&images, progress::now(clock), observed.clone());
    progress::hold(&mut a, &mut sender, clock, &observed);
    progress::close_peer(&mut b, &mut opposite, clock);
    progress::fail_running(&mut a, &mut sender, clock);
    assert_eq!(observed.borrow().accepted, 7);
    let sender = cleanup::recover(sender, &mut a, clock);
    progress::close_peer(&mut c, &mut idle, clock);
    let opposite = opposite
        .into_parts()
        .unwrap_or_else(|_| panic!("original opposite drain"));
    let idle = idle
        .into_parts()
        .unwrap_or_else(|_| panic!("original idle drain"));
    b.close();
    c.close();
    assert!(a.is_drained() && b.is_drained() && c.is_drained());
    drop((a, b, c));
    channels.close(
        sender
            .sessions
            .into_values()
            .chain(opposite.sessions.into_values())
            .chain(idle.sessions.into_values()),
        progress::now(clock),
    );
    first.finish();
    second.finish();
    third.finish();
}
fn bridge(members: &mut members::Members, slot: usize) -> aliases::Bridge<'_> {
    LogicalChildBridge::new(
        &mut members.nodes,
        groups::mapping(slot),
        support::node(groups::ids(slot)[0]),
    )
    .unwrap()
}
