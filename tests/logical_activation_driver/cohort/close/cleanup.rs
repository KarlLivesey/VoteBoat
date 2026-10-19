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
//! Exact original failed send completion and original unaccepted cancellations.
use super::*;
use voteboat::{
    application::{ReadableStateMachine, StateMachine},
    outbound::{LocalSendResult, OutboundQueue},
    snapshot_worker::SnapshotWorker,
    transport::TransportState,
    worker::PersistenceWorker,
};
type App = LogicalActivationApplication<application::App>;
type Recovery = LogicalNodeRecovery<
    fixture::Parts,
    <App as StateMachine>::Receipt,
    <App as ReadableStateMachine>::ReadResult,
>;
pub fn recover(
    sender: fixture::FacadeNode,
    bridge: &mut aliases::Bridge<'_>,
    clock: &harness::SimClock,
) -> fixture::Parts {
    let mut recovery: Recovery = sender
        .into_recovery()
        .unwrap_or_else(|_| panic!("original failed owner"));
    assert!(recovery.parts.local.owner.is_failed());
    assert!(recovery.incoming.is_empty());
    assert!(recovery.completions.is_empty());
    consume_failed(&mut recovery);
    cancel_unaccepted(&mut recovery);
    assert!(recovery.parts.local.persistence.is_drained());
    assert!(recovery.parts.persistence.is_drained());
    assert!(recovery.parts.snapshots.as_ref().unwrap().is_drained());
    assert!(bridge.is_drained());
    bridge
        .advance(progress::now(clock), fixture::budget(4))
        .unwrap();
    recovery
        .replica
        .discard_failed(&mut recovery.parts.local.as_parts())
        .unwrap();
    assert!(recovery.replica.is_drained());
    assert_eq!(
        recovery.replica.release_usage(),
        LogicalReleaseUsage::default()
    );
    assert!(recovery.parts.local.clients.is_drained());
    assert!(recovery.parts.local.reads.is_drained());
    assert!(recovery.parts.local.results.is_drained());
    recovery.parts.local.persistence.close();
    recovery.parts.local.outbound.close();
    recovery.parts.local.results.close();
    recovery
        .parts
        .local
        .snapshots
        .as_mut()
        .unwrap()
        .worker
        .close();
    assert!(recovery.parts.local.outbound.is_drained());
    let snapshots = recovery.parts.local.snapshots.as_ref().unwrap();
    assert!(snapshots.router.is_drained());
    assert!(snapshots.worker.is_drained());
    assert!(recovery.pending.is_empty());
    assert!(recovery.replica.releases().is_drained());
    for session in recovery.parts.sessions.values_mut() {
        session.close();
    }
    bridge.close();
    assert!(bridge.is_drained());
    recovery.parts
}
fn consume_failed(recovery: &mut Recovery) {
    let peer = support::node(12);
    let before = recovery.parts.local.outbound.usage();
    let session = recovery.parts.sessions.get_mut(&peer).unwrap();
    assert_eq!(session.state(), TransportState::Failed);
    assert!(!session.is_drained());
    let completion = session.take_send().expect("original accepted partial send");
    let original = completion.ticket();
    assert_eq!(original.peer, peer);
    assert_eq!(original.binding, recovery.parts.local.outbound.binding());
    assert_eq!(completion.result(), LocalSendResult::Failed);
    assert_eq!(recovery.parts.local.outbound.usage(), before);
    assert!(session.take_send().is_none());
    let completed = recovery
        .parts
        .local
        .outbound
        .complete_logical(completion)
        .unwrap_or_else(|e| panic!("original failed completion rejected {:?}", e.reason));
    assert_eq!(completed.ticket, original);
    assert_eq!(completed.result, LocalSendResult::Failed);
    let after = recovery.parts.local.outbound.usage();
    assert_eq!(after.messages + 1, before.messages);
    assert!(after.bytes < before.bytes);
    assert!(session.is_drained());
    assert_eq!(session.state(), TransportState::Failed);
    eprintln!("332 close consumed original ticket={original:?} held={before:?} released={after:?}");
}
fn cancel_unaccepted(recovery: &mut Recovery) {
    let originals = recovery
        .pending
        .drain(..)
        .chain(recovery.parts.local.outbound.poll_logical(65536))
        .collect::<Vec<_>>();
    for batch in originals {
        let ticket = batch.ticket();
        let completed = recovery
            .parts
            .local
            .outbound
            .cancel_dispatched(batch)
            .unwrap_or_else(|e| panic!("original unaccepted batch {:?}", e.reason));
        assert_eq!(completed.ticket, ticket);
        assert_eq!(completed.result, LocalSendResult::Cancelled);
    }
    assert!(recovery.parts.local.outbound.is_drained());
}
