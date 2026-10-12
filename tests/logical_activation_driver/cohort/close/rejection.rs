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
//! Original cancellation metadata survives a later explicit host-provider fault.
use super::*;
use voteboat::{
    outbound::{LocalSendResult, OutboundBinding, OutboundQueue},
    secure::SessionError,
};
#[path = "rejection/assembly.rs"]
mod assembly;
#[path = "rejection/fault.rs"]
mod fault;

#[test]
fn shutdown_rejection_retains_original_cancellation_before_later_provider_fault() {
    let _serial = cluster::HISTORY.lock().unwrap();
    let clock = Instant::now();
    let mut first = members::Members::new(NativePeerProtocol::TcpTls, 0, clock);
    let mut second = members::Members::new(NativePeerProtocol::TcpTls, 1, clock);
    let mut third = members::Members::new(NativePeerProtocol::TcpTls, 2, clock);
    let mut a = bridge(&mut first, 0);
    let mut b = bridge(&mut second, 1);
    let mut c = bridge(&mut third, 2);
    let images = [
        drive::claim_in(&mut a, clock, 34001, false),
        drive::claim_in(&mut b, clock, 34002, false),
        drive::claim_in(&mut c, clock, 34003, false),
    ];
    let gate = gate::Observed::default();
    let fault = fault::Fault::default();
    let (channels, [mut sender, mut opposite, mut idle]) =
        assembly::parents(&images, progress::now(clock), gate.clone(), fault.clone());
    progress::hold(&mut a, &mut sender, clock, &gate);
    assert!(progress::lower_drained(&mut sender, &a));
    assert_eq!(gate.borrow().accepted, 7);
    assert_eq!(fault.injected.get(), 0);
    // Close the other original endpoints while the sender is deliberately
    // parked. Revoking its partial frame later must not turn their graceful
    // cleanup into an unrelated truncated-frame recovery history.
    progress::close_peer(&mut b, &mut opposite, clock);
    progress::close_peer(&mut c, &mut idle, clock);
    assert!(progress::lower_drained(&mut sender, &a));
    let owner = sender.local().owner.identity();
    let binding = sender.local().outbound.binding();
    reject(&mut sender, clock, &fault, binding);
    assert_eq!(sender.local().owner.identity(), owner);
    assert_eq!(gate.borrow().accepted, 7);
    // This helper consumes the actual Failed envelope retained by the original
    // LogicalPeerSession, through the same original queue. It also proves that
    // no accepted WAL/snapshot obligation was left unresolved before the fault.
    let sender = cleanup::recover(sender, &mut a, clock);
    assert_eq!(sender.local.owner.identity(), owner);
    assert!(sender.local.owner.is_failed());
    assert!(sender.local.outbound.is_drained());
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

fn reject(
    sender: &mut fixture::FacadeNode,
    clock: Instant,
    fault: &fault::Fault,
    binding: OutboundBinding,
) {
    let before = sender.local().outbound.usage();
    assert_eq!(before.messages, 2);
    sender.close_intake().unwrap();
    fault.armed.set(true);
    let error = sender
        .poll_shutdown(progress::now(clock), LogicalNodePollBudget::default())
        .expect_err("negative host-provider poll occurs after original cancellation");
    assert_eq!(fault.injected.get(), 1);
    assert!(!fault.armed.get());
    assert_eq!(
        error.reason,
        LogicalNodeError::Network(LogicalNetworkError::Session(SessionError::Io(
            std::io::ErrorKind::Other
        )))
    );
    assert_eq!(sender.failure(), Some(&error.reason));
    assert_eq!(sender.state(), NodeState::RecoveryRequired);
    assert_eq!(error.progress.completions.len(), 1);
    let done = &error.progress.completions[0];
    assert_eq!(done.ticket.binding, binding);
    assert_eq!(done.ticket.peer, support::node(13));
    assert_eq!(done.ticket.sequence, 2);
    assert_eq!(done.result, LocalSendResult::Cancelled);
    assert!(error.progress.peer_closures.iter().all(|closed| {
        closed.peer == support::node(13)
            && matches!(
                closed.reason,
                LogicalNetworkError::Closed | LogicalNetworkError::Session(SessionError::Closed)
            )
    }));
    let after = sender.local().outbound.usage();
    assert_eq!(after.messages, 1);
    assert!(after.bytes > 0 && after.bytes < before.bytes);
    let repeated = sender
        .poll_shutdown(progress::now(clock), LogicalNodePollBudget::default())
        .expect_err("original fault is latched");
    assert_eq!(repeated.reason, LogicalNodeError::RecoveryRequired);
    assert!(repeated.progress.completions.is_empty());
    assert!(repeated.progress.peer_closures.is_empty());
    assert_eq!(sender.local().outbound.usage(), after);
    assert_eq!(fault.injected.get(), 1);
    eprintln!(
        "336 original cancelled={done:?}, retained={after:?}, reason={:?}, peer_closures={:?}",
        error.reason, error.progress.peer_closures
    );
}
