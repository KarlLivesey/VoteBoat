// SPDX-License-Identifier: RPL-1.5
// Copyright (c) 2026 Karl Livesey
//! Original one-WAL import becomes committed only through recovered protocol.
use super::{cluster::Cluster, lower::Lower, setup, Middle};
use std::time::{Duration, Instant};
use voteboat::{
    application::{ReadableStateMachine, StateMachine},
    identity::OperationId,
    log::{EntryPayload, LogEntry},
    logical_voter::*,
    native::startup::NativeOpenMode,
    runtime::{NodePollBudget, PeerDriverBudget},
    worker::PersistenceWorker,
};
#[path = "uncommitted_target/abort.rs"]
mod abort;
type Bridge<'a> = LogicalBackingBridge<'a, super::cluster::ChildNode>;
pub(super) fn partition_budget() -> NodePollBudget {
    NodePollBudget {
        peers: PeerDriverBudget {
            connection_visits: 0,
            peer_visits: 0,
            sends: 0,
            ingress: 0,
            ..PeerDriverBudget::default()
        },
        ..NodePollBudget::default()
    }
}
pub fn recover<'a>(
    lower: &mut Lower<'_>,
    source: &mut [Middle],
    cluster: &'a mut Cluster,
    grant: LogicalLearnerEnrollment,
) -> (VerifiedLogicalImage, Bridge<'a>) {
    let held = source
        .iter()
        .map(super::controls::deferred)
        .collect::<Vec<_>>();
    let (recovery, bytes) = super::unknown_target::cancel(lower, source, cluster, grant, true);
    let entry = persist(cluster, &bytes);
    let aborted = abort::join(cluster);
    eprintln!("370 same3 original providers joined/rebound with exact import WAL uncommitted");
    cluster.open_before_election(NativeOpenMode::Recover);
    let leader = &cluster.nodes[0];
    let core = leader.local().owner.core(setup::bottom(3).group).unwrap();
    assert_eq!(core.state().entry_at(entry.index), Some(&entry));
    assert!(core.state().commit_index < entry.index);
    assert!(core.storage_binding().session.get() > 1);
    for node in &cluster.nodes {
        absent(&node.local().applications[&setup::bottom(3).group]);
    }
    cluster.elect();
    let mut bridge = Bridge::new(
        &mut cluster.nodes,
        setup::lower_mapping(3),
        cluster.representative,
    )
    .unwrap();
    let rejected = bridge
        .resume_enrollment(recovery)
        .expect_err("old aborted input cannot resume against new owners");
    assert_eq!(rejected.reason, LogicalBridgeError::StaleRepresentative);
    let original = rejected.recovery;
    bridge
        .begin_claim(OperationId::new(370262).unwrap())
        .unwrap();
    let end = Instant::now() + Duration::from_secs(15);
    let fresh = loop {
        lower.middle(source);
        bridge.advance(lower.now(), Lower::budget()).unwrap();
        if let Some(image) = bridge.take_image() {
            break image;
        }
        assert!(
            Instant::now() < end,
            "fresh recovered actual Claim and original import lineage"
        );
    };
    assert!(fresh.claim().first_index > entry.index);
    assert!(fresh.binding().session.get() > 1);
    for node in 0..3 {
        bridge
            .inspect_backing(node, |p| {
                let app = &p.applications[&setup::bottom(3).group];
                let LogicalRead::Enrollment(view) = app
                    .read_at(app.applied_index(), LogicalQuery::Enrollment)
                    .unwrap()
                else {
                    panic!("fresh durable candidate lineage")
                };
                let record = view.record.unwrap();
                assert_eq!(record.operation.get(), 356200);
                assert_eq!(record.first_index, entry.index);
                let LogicalRead::Outcome(Some(status)) = app
                    .read_at(app.applied_index(), LogicalQuery::Outcome(record.operation))
                    .unwrap()
                else {
                    panic!("original imported command truth")
                };
                assert_eq!(status.command, bytes);
                assert!(
                    p.owner
                        .core(setup::bottom(3).group)
                        .unwrap()
                        .state()
                        .commit_index
                        >= entry.index
                );
            })
            .unwrap();
    }
    for (node, batches) in source.iter().zip(held) {
        let current = super::controls::deferred(node);
        assert!(batches.iter().all(|b| current.contains(b)));
    }
    drop(original);
    aborted.retire(&fresh);
    (fresh, bridge)
}
fn persist(cluster: &mut Cluster, bytes: &[u8]) -> LogEntry {
    let end = Instant::now() + Duration::from_secs(15);
    loop {
        cluster.poll_with_budget(partition_budget());
        let node = &cluster.nodes[0];
        let core = node.local().owner.core(setup::bottom(3).group).unwrap();
        let entry=core.state().entries.iter().find(|e|matches!(&e.payload,EntryPayload::Command{operation,bytes:b} if operation.get()==356200 && b==bytes));
        if let Some(entry) = entry
            .filter(|_| !core.has_pending_dependency() && node.local().persistence.is_drained())
        {
            assert!(core.state().commit_index < entry.index);
            for n in &cluster.nodes {
                absent(&n.local().applications[&setup::bottom(3).group]);
            }
            return entry.clone();
        }
        assert!(
            Instant::now() < end,
            "original target import reaches durable one-owner WAL without quorum"
        );
    }
}
fn absent(app: &LogicalApplication) {
    let LogicalRead::Enrollment(view) = app
        .read_at(app.applied_index(), LogicalQuery::Enrollment)
        .unwrap()
    else {
        panic!("actual uninitialized candidate")
    };
    assert!(view.record.is_none());
}
