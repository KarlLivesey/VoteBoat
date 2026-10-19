// SPDX-License-Identifier: RPL-1.5
// Copyright (c) 2026 Karl Livesey
//! Unknown observation preserves the original import; fresh cold Claim resolves truth.
use super::harness::{self, Observed};
use super::{cluster::Cluster, lower::Lower, setup, Middle};
use voteboat::{
    application::{ReadableStateMachine, StateMachine},
    identity::{OperationId, StoreBinding},
    log::GroupLog,
    logical_voter::*,
    native::startup::NativeOpenMode,
};
type Bridge<'a> = LogicalBackingBridge<'a, super::cluster::ChildNode>;
struct Facts {
    record: LogicalEnrollmentRecord,
    parent: GroupLog,
    binding: StoreBinding,
    bytes: Vec<u8>,
}
pub fn recover<'a>(
    lower: &mut Lower<'_>,
    source: &mut [Middle],
    cluster: &'a mut Cluster,
    grant: LogicalLearnerEnrollment,
) -> (VerifiedLogicalImage, Bridge<'a>) {
    let original = source
        .iter()
        .map(super::controls::deferred)
        .collect::<Vec<_>>();
    let (recovery, bytes) = cancel(lower, source, cluster, grant, false);
    let facts = materialized(cluster, bytes);
    cluster.close();
    eprintln!("368 canceled original import observation; same3 stores committed and joined before cold Claim");
    cluster.open(NativeOpenMode::Recover);
    let mut bridge = Bridge::new(
        &mut cluster.nodes,
        setup::lower_mapping(3),
        cluster.representative,
    )
    .unwrap();
    let rejected = bridge
        .resume_enrollment(recovery)
        .expect_err("old live-owner recovery cannot authorize new owners");
    assert_eq!(rejected.reason, LogicalBridgeError::StaleRepresentative);
    let original_recovery = rejected.recovery;
    bridge
        .begin_claim(OperationId::new(368262).unwrap())
        .unwrap();
    let end = harness::Wait::new(&lower.clock, harness::LIVENESS_MS);
    let fresh = loop {
        lower.middle_with(source, Observed::default().bridge(&bridge));
        bridge.advance(lower.now(), Lower::budget()).unwrap();
        if let Some(image) = bridge.take_image() {
            break image;
        }
        assert!(
            end.within(&lower.clock),
            "fresh cold Unknown import lineage resolution"
        );
    };
    assert_eq!(fresh.binding().identity, facts.binding.identity);
    assert!(fresh.binding().session > facts.binding.session);
    assert_eq!(fresh.image().parent, facts.parent);
    assert!(fresh.claim().first_index > facts.record.first_index);
    for node in 0..3 {
        bridge
            .inspect_backing(node, |p| {
                let app = &p.applications[&setup::bottom(3).group];
                assert_eq!(record(app), facts.record);
                let LogicalRead::Outcome(Some(status)) = app
                    .read_at(
                        app.applied_index(),
                        LogicalQuery::Outcome(facts.record.operation),
                    )
                    .unwrap()
                else {
                    panic!("original durable import result")
                };
                assert_eq!(status.command, facts.bytes);
                assert_eq!(status.first_index, facts.record.first_index);
                assert!(
                    p.owner
                        .core(setup::bottom(3).group)
                        .unwrap()
                        .storage_binding()
                        .session
                        .get()
                        > 1
                );
            })
            .unwrap();
    }
    for (node, held) in source.iter().zip(original) {
        let current = super::controls::deferred(node);
        assert!(held.iter().all(|batch| current.contains(batch)));
    }
    // The rejected whole original is retained until actual fresh durable truth is known.
    drop(original_recovery);
    (fresh, bridge)
}
pub(super) fn cancel(
    lower: &mut Lower<'_>,
    source: &mut [Middle],
    cluster: &mut Cluster,
    grant: LogicalLearnerEnrollment,
    withheld_network: bool,
) -> (LogicalEnrollmentRecovery, Vec<u8>) {
    let mut bridge = Bridge::new(
        &mut cluster.nodes,
        setup::lower_mapping(3),
        cluster.representative,
    )
    .unwrap();
    bridge
        .begin_enrollment(OperationId::new(356200).unwrap(), grant)
        .unwrap_or_else(|e| panic!("genuine source grant {:?}", e.reason));
    let end = harness::Wait::new(&lower.clock, harness::LIVENESS_MS);
    let (ticket, bytes) = loop {
        lower.middle_with(source, Observed::default().bridge(&bridge));
        bridge.advance(lower.now(), Lower::budget()).unwrap();
        if let Some(work) = bridge.inspect_work(|w| {
            w.and_then(|w| {
                (w.phase() == LogicalBridgeWorkPhase::Applying).then(|| {
                    (
                        w.attempt().unwrap().ticket(),
                        w.command_bytes().unwrap().to_vec(),
                    )
                })
            })
        }) {
            break work;
        }
        assert!(end.within(&lower.clock), "original import client admission");
    };
    let LogicalBridgeChildTicket::Client(ticket) = ticket else {
        panic!("actual import client")
    };
    assert_eq!(ticket.operation.get(), 356200);
    bridge.stop_enrollment_waiting().unwrap();
    assert_eq!(
        bridge.inspect_work(|w| w.unwrap().attempt().unwrap().ticket()),
        LogicalBridgeChildTicket::Client(ticket)
    );
    loop {
        lower.middle_with(source, Observed::default().bridge(&bridge));
        bridge
            .advance(
                lower.now(),
                LogicalBridgeBudget {
                    transitions: 0,
                    node: if withheld_network {
                        super::uncommitted_target::partition_budget()
                    } else {
                        voteboat::runtime::NodePollBudget::default()
                    },
                    ..Lower::budget()
                },
            )
            .unwrap();
        let resolved = bridge.inspect_work(|w| {
            let w = w.unwrap();
            assert_eq!(w.operation(), ticket.operation);
            assert_eq!(w.command_bytes().unwrap(), bytes);
            w.phase() == LogicalBridgeWorkPhase::Resolving && w.attempt().is_none()
        });
        if resolved
            && (0..3).all(|i| {
                bridge
                    .inspect_backing(i, |p| p.clients.is_drained() && p.reads.is_drained())
                    .unwrap()
            })
        {
            break;
        }
        assert!(bridge.take_image().is_none());
        assert!(
            end.within(&lower.clock),
            "same original Unknown observation settles"
        );
    }
    // consume.rs changes Applying→Resolving only after consuming ClientOutcome::Unknown.
    let (recovery, _nodes) = bridge
        .into_enrollment_recovery()
        .unwrap_or_else(|_| panic!("whole stopped original input handback"));
    (recovery, bytes)
}
fn materialized(cluster: &mut Cluster, bytes: Vec<u8>) -> Facts {
    let end = harness::Wait::new(&cluster.clock, harness::LIVENESS_MS);
    loop {
        cluster.poll();
        if cluster.nodes.iter().all(|n|matches!(n.local().applications[&setup::bottom(3).group].read_at(n.local().applications[&setup::bottom(3).group].applied_index(),LogicalQuery::Enrollment).unwrap(),LogicalRead::Enrollment(v) if v.record.is_some())) { break; }
        assert!(
            end.within(&cluster.clock),
            "accepted original import actually commits after Unknown"
        );
    }
    let app = &cluster.nodes[0].local().applications[&setup::bottom(3).group];
    let expected = record(app);
    let LogicalRead::Image(image) = app
        .read_at(app.applied_index(), LogicalQuery::Image)
        .unwrap()
    else {
        panic!("actual imported parent inspection only")
    };
    let binding = StoreBinding {
        identity: setup::lower_mapping(3).store,
        session: image.representative.logical_session,
    };
    assert_eq!(expected.operation.get(), 356200);
    for n in &cluster.nodes {
        assert_eq!(
            record(&n.local().applications[&setup::bottom(3).group]),
            expected
        );
    }
    Facts {
        record: expected,
        parent: image.parent,
        binding,
        bytes,
    }
}
fn record(app: &LogicalApplication) -> LogicalEnrollmentRecord {
    let LogicalRead::Enrollment(view) = app
        .read_at(app.applied_index(), LogicalQuery::Enrollment)
        .unwrap()
    else {
        panic!("actual V2 enrollment")
    };
    *view.record.unwrap()
}
