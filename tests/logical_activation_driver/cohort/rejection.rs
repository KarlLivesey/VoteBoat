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
//! Failed native assembly keeps original providers until explicit shutdown/join.
use super::*;
use voteboat::{
    connect::{ConnectPollBudget, PeerConnector},
    outbound::OutboundQueue,
    snapshot_worker::SnapshotWorker,
    worker::PersistenceWorker,
};
type NativeParts = voteboat::native::node::NativeNodeParts<
    LogicalApplication,
    voteboat::native::connect::NativeServiceConnector,
>;
pub fn prepared(
    mut error: Box<voteboat::native::startup::NativeStartupRejected<LogicalApplication>>,
) -> ! {
    let end = Instant::now() + Duration::from_secs(15);
    while !error.try_cleanup().unwrap() {
        assert!(Instant::now() < end, "original prepared rejection cleanup");
        std::thread::park_timeout(Duration::from_millis(1));
    }
    panic!("actual original native preparation {:?}", error.reason)
}
pub fn parts(mut parts: NativeParts, reason: impl std::fmt::Debug, now: MonoTime) -> ! {
    parts.local.owner.close_admission().unwrap();
    assert!(parts.local.owner.is_drained());
    assert!(parts.local.clients.is_drained());
    assert!(parts.local.reads.is_drained());
    assert!(parts.local.results.is_drained());
    parts.local.persistence.close();
    parts.local.outbound.close();
    assert!(parts.local.outbound.is_drained());
    let mut snapshot = parts.local.snapshots.take().unwrap();
    assert!(snapshot.router.is_drained());
    snapshot.worker.close();
    let mut peers = parts.peers.take().unwrap();
    assert!(peers.roster.is_drained());
    peers.connector.close();
    let end = Instant::now() + Duration::from_secs(15);
    while !peers.connector.is_drained() {
        assert!(peers
            .connector
            .poll(now, ConnectPollBudget::default())
            .unwrap()
            .is_empty());
        assert!(Instant::now() < end, "original rejected connector drain");
        std::thread::park_timeout(Duration::from_millis(1));
    }
    let mut dialer = peers
        .connector
        .into_dialer()
        .unwrap_or_else(|_| panic!("original rejected connector"));
    let (mut wal, mut image) = (false, false);
    loop {
        if !wal {
            wal = parts.local.persistence.try_reclaim().unwrap().is_some();
        }
        if !image {
            image = snapshot.worker.try_reclaim().unwrap().is_some();
        }
        if wal && image && dialer.as_mut().is_none_or(|d| d.try_finish().unwrap()) {
            break;
        }
        assert!(Instant::now() < end, "original rejected workers join");
        std::thread::park_timeout(Duration::from_millis(1));
    }
    panic!("actual original native assembly {reason:?}")
}
