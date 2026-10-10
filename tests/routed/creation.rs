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
use voteboat::{
    group_creation::*,
    native::{group_creation::*, snapshot_store::*},
    quorum::WeightedChild,
    snapshot::{SnapshotIdentity, SnapshotLimits},
};
#[path = "created_group.rs"]
mod group_service;
fn creation_directory() -> Directory {
    directory()
        .with_group_creation()
        .unwrap_or_else(|_| panic!("fresh schema2 directory"))
}
#[test]
fn tcp_created_group_resumes_partial_assignment_and_runs_with_metadata_offline() {
    group_service::run(NativePeerProtocol::TcpTls);
}
#[cfg(feature = "quic")]
#[test]
fn quic_created_group_resumes_partial_assignment_and_runs_with_metadata_offline() {
    group_service::run(NativePeerProtocol::Quic);
}

use voteboat::namespace_creation::*;
use voteboat::{snapshot_worker::SnapshotWorker, worker::PersistenceWorker};

// Stop core polling immediately. Accepted native I/O may still finish; discard
// its observations and reclaim actual stores before reopening. No rollback or
// hardware power-loss claim follows from owner abort.
pub(super) fn abandon<A>(mut nodes: Vec<Node<A>>, g: u128) -> BTreeMap<NodeId, GroupLog>
where
    A: ProposalAdmission + BoundedReadableStateMachine + CheckpointStateMachine,
    A::Receipt: ApplicationReceipt,
{
    for n in &mut nodes {
        n.abort();
    }
    nodes
        .into_iter()
        .map(|n| {
            let id = n.local().owner.core(group(g)).unwrap().local_node();
            let mut recovery = n
                .into_recovery()
                .unwrap_or_else(|_| panic!("aborted owner"));
            drop(recovery.peers.take());
            let mut snapshots = recovery.local.snapshots.take().unwrap();
            let deadline = Instant::now() + Duration::from_secs(5);
            let mut log = None;
            let mut snapshot_done = false;
            loop {
                if log.is_none() {
                    let _ = recovery.local.persistence.poll(64);
                    let _ = recovery.local.persistence.poll_reclaims(64);
                    if let Some(store) = recovery.local.persistence.try_reclaim().unwrap() {
                        log = Some(store.state(group(g)).unwrap());
                    }
                }
                if !snapshot_done {
                    let _ = snapshots.worker.poll(64);
                    snapshot_done = snapshots.worker.try_reclaim().unwrap().is_some();
                }
                if snapshot_done {
                    if let Some(log) = log.take() {
                        return (id, log);
                    }
                }
                assert!(
                    Instant::now() < deadline,
                    "aborted selected workers must return stores"
                );
                std::thread::park_timeout(Duration::from_millis(1));
            }
        })
        .collect()
}
#[path = "namespace_service.rs"]
mod namespace_service;
fn namespace_directory() -> Directory {
    directory()
        .with_namespace_creation()
        .unwrap_or_else(|_| panic!("schema3"))
}
#[test]
fn tcp_namespace_ready_publish_activate_survives_reopen_and_metadata_outage() {
    namespace_service::run(NativePeerProtocol::TcpTls, false);
}
#[cfg(feature = "quic")]
#[test]
fn quic_namespace_ready_publish_activate_survives_reopen_and_metadata_outage() {
    namespace_service::run(NativePeerProtocol::Quic, false);
}

#[test]
fn tcp_namespace_partial_publication_activation_unread_receipts_owner_abort() {
    namespace_service::run(NativePeerProtocol::TcpTls, true);
}
#[test]
fn tcp_namespace_cancellation_unread_receipt_and_late_readiness() {
    namespace_service::run_cancellation(NativePeerProtocol::TcpTls);
}
#[test]
fn tcp_namespace_race_recorded_owner_loss_and_recovery_schedules() {
    namespace_service::run_race(NativePeerProtocol::TcpTls);
}
#[cfg(feature = "quic")]
#[test]
fn quic_namespace_race_recorded_owner_loss_and_recovery_schedules() {
    namespace_service::run_race(NativePeerProtocol::Quic);
}
#[cfg(feature = "quic")]
#[test]
fn quic_namespace_cancellation_unread_receipt_and_late_readiness() {
    namespace_service::run_cancellation(NativePeerProtocol::Quic);
}
#[cfg(feature = "quic")]
#[test]
fn quic_namespace_partial_publication_activation_unread_receipts_owner_abort() {
    namespace_service::run(NativePeerProtocol::Quic, true);
}
