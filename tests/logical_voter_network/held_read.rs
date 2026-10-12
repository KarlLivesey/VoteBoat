// SPDX-License-Identifier: RPL-1.5
// Copyright (c) 2026 Karl Livesey
//! Actual peer quorum with one original child output-consumption gate held.
use super::{aliases::Bridge, cluster::Cluster, host::Host, setup};
use std::time::{Duration, Instant};
use voteboat::{
    application::*, identity::*, logical_voter::*, native::connect::NativePeerProtocol,
    outbound::OutboundQueue, raft::*, runtime::*,
};
#[path = "held_read/inspection.rs"]
mod inspection;
#[path = "held_read/phases.rs"]
mod phases;
use inspection::Held;
fn history(protocol: NativePeerProtocol) {
    let _serial = super::cluster::HISTORY.lock().unwrap();
    let mut children = std::array::from_fn(|slot| Cluster::new(protocol, slot));
    let mut host = Host::new(&mut children, protocol, 9401);
    host.elect(0);
    phases::write(&mut host);
    let ticket = phases::submit(&mut host, 0);
    let original = host.parents[0].routers.reads.inspect(ticket).unwrap();
    assert!(!original.stepped());
    assert!(!original.has_output());
    assert_eq!(original.cancellation(), None);
    inspection::refusals(&host, ticket);
    let held = inspection::wait(&mut host, ticket);
    let charged = host.parents[0].routers.reads.usage();
    let child_credit = held.child_credit(&host.bridges[0]);
    assert_eq!(charged.requests, 1);
    assert_eq!(child_credit.requests, 1);
    for _ in 0..64 {
        host.poll_with_completions(0);
        held.assert_same(&host);
        assert_eq!(held.child_credit(&host.bridges[0]), child_credit);
        assert_eq!(host.parents[0].routers.reads.usage(), charged);
        assert!(host.parents[0].routers.reads.poll().is_none());
        assert_eq!(host.parents[0].outbound.usage().messages, 0);
    }
    phases::resume(&mut host, ticket, &held, charged);
    host.until(Host::is_drained);
    host.close();
    for child in children {
        child.finish();
    }
}
#[test]
fn tcp_original_read_inspection_retains_and_resumes_real_nine_child_quorum() {
    history(NativePeerProtocol::TcpTls);
}
#[cfg(feature = "quic")]
#[test]
fn quic_original_read_inspection_retains_and_resumes_real_nine_child_quorum() {
    history(NativePeerProtocol::Quic);
}
