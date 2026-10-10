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
use super::*;

#[test]
fn close_between_poll_and_submit_retains_ticket_and_reconnects_only_that_peer() {
    let mut f = Fixture::new();
    f.attach();
    let old = f.transports.borrow()[&node(2)].clone();
    let old_binding = old.borrow().binding;
    old.borrow_mut().submit_error = Some(TransportError::Closed);
    let retry = f.enqueue(2);
    let other = f.enqueue(3);
    let first = f
        .poll(0)
        .expect("one closed connection cannot fence the driver");
    assert_eq!(first.connection_failures, 1);
    assert!(first.completions.iter().all(|c| c.ticket != retry));
    assert_eq!(old.borrow().state, TransportState::Failed);
    assert!(f
        .driver
        .as_ref()
        .unwrap()
        .roster()
        .binding(node(2))
        .is_none());
    let next = f.poll(0).unwrap();
    assert!(next
        .completions
        .iter()
        .any(|c| c.ticket == other && c.result == LocalSendResult::Sent));
    assert_eq!(f.outbound.usage().batches, 1);
    assert_eq!(
        f.driver.as_ref().unwrap().usage().attempts,
        0,
        "backoff must apply"
    );
    let connect = f.poll(1).unwrap();
    assert_eq!(connect.connection_submissions, 1);
    let fresh = f.poll(1).unwrap();
    assert_eq!(fresh.connections, 1);
    let binding = f
        .driver
        .as_ref()
        .unwrap()
        .roster()
        .binding(node(2))
        .unwrap();
    assert!(binding.generation > old_binding.generation);
    assert_eq!(binding.peer, old_binding.peer);
    assert!(f
        .poll(1)
        .unwrap()
        .completions
        .iter()
        .any(|c| c.ticket == retry && c.result == LocalSendResult::Sent));
    assert!(f.outbound.is_drained());
    f.close(1);
}

#[test]
fn invalid_binding_at_submit_still_fences_the_driver() {
    let mut f = Fixture::new();
    f.attach();
    f.transports.borrow()[&node(2)].borrow_mut().submit_error = Some(TransportError::WrongBinding);
    f.enqueue(2);
    assert_eq!(
        f.poll(0).unwrap_err(),
        PeerDriverError::Roster(PeerRosterError::Transport(TransportError::WrongBinding))
    );
    assert!(f.driver.as_ref().unwrap().is_failed());
}
