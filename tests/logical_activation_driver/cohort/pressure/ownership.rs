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
//! Genuine original observer refusal and ownership inspection boundaries.
use super::*;
pub fn refused(parent: &mut fixture::FacadeNode) {
    let clients = parent.local().clients.usage();
    let reads = parent.local().reads.usage();
    let intent = super::super::super::history::intent(335_400);
    match ChildActivationDriver::new(parent, intent, ChildActivationDriverLimits::default()) {
        Err(e) => assert_eq!(e.reason, ChildActivationDriverError::WrongBinding),
        Ok(_) => panic!("unresolved observer accepted"),
    }
    assert_eq!(parent.local().clients.usage(), clients);
    assert_eq!(parent.local().reads.usage(), reads);
}
pub fn clear(parent: &fixture::FacadeNode) {
    let group = groups::mapping(1).parent;
    assert!(parent
        .local()
        .clients
        .retains_only_exported_ordinary_outputs(group));
    assert!(parent
        .local()
        .reads
        .retains_only_exported_ordinary_outputs(group));
    assert!(!parent
        .local()
        .clients
        .retains_only_exported_ordinary_outputs(groups::mapping(1).child));
    assert!(!parent
        .local()
        .reads
        .retains_only_exported_ordinary_outputs(groups::mapping(1).child));
    assert!(parent.local().results.is_drained());
}
pub fn cancellation(
    peers: &mut progress::Peers<'_>,
    parent: &mut fixture::FacadeNode,
    target: &mut aliases::Bridge<'_>,
) {
    let ticket = parent
        .read(groups::mapping(1).parent, ActivationQuery::Business(()))
        .unwrap();
    parent.cancel_read(ticket).unwrap();
    let output = parent.poll_read().unwrap();
    assert_eq!(output.ticket(), ticket);
    assert!(matches!(
        output.outcome(),
        ReadOutcome::Unavailable(ReadUnavailable::Cancelled)
    ));
    refused(parent);
    let end = harness::Wait::new(&peers.clock, harness::LIVENESS_MS);
    loop {
        peers.initial(parent, target);
        let inspection = parent.inspect_read(ticket).unwrap();
        if let Some(original) = inspection.cancellation() {
            assert_eq!(original.owner, ticket.binding.owner);
            assert!(inspection.exported());
            assert!(!parent
                .local()
                .reads
                .retains_only_exported_ordinary_outputs(ticket.group));
            refused(parent);
            break;
        }
        assert!(!inspection.terminal(), "actual cancellation never retained");
        assert!(end.within(&peers.clock), "original cancellation admission");
    }
    loop {
        peers.initial(parent, target);
        let inspection = parent.inspect_read(ticket).unwrap();
        if inspection.terminal() && inspection.cancellation().is_none() {
            break;
        }
        assert!(end.within(&peers.clock), "original cancellation step");
    }
    clear(parent);
    assert!(matches!(
        parent.complete_read(output).unwrap(),
        ReadOutcome::Unavailable(ReadUnavailable::Cancelled)
    ));
    assert!(parent.local().reads.is_drained());
}
