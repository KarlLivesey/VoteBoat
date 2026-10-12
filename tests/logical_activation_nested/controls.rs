// SPDX-License-Identifier: RPL-1.5
// Copyright (c) 2026 Karl Livesey
//! Inspect and conserve exact owned inputs, without minting any authority.
use super::{setup, support, Middle, Top};
use voteboat::{
    identity::OperationId, logical_voter::*, outbound::*, runtime::*, secure::SecureSession,
};
#[derive(Eq, PartialEq)]
pub struct Deferred {
    ticket: SendTicket,
    effect: EffectTicket,
    operation: OperationId,
    command_pointer: usize,
}
pub fn deferred(node: &Middle) -> Vec<Deferred> {
    node.inspect_deferred()
        .map(|b| {
            let proof = b.send().provenance();
            Deferred {
                ticket: b.ticket(),
                effect: b.send().effect(),
                operation: proof.receipt().operation,
                command_pointer: proof.command_bytes().as_ptr() as usize,
            }
        })
        .collect()
}
type Session = LogicalPeerSession<Box<dyn SecureSession>>;
pub fn attach(nodes: &mut [Middle], slot: usize, mut session: Session) {
    if slot == 0 {
        let binding = session.binding();
        let original = deferred(&nodes[1]);
        let usage = nodes[1].local().outbound.usage();
        let rejected = nodes[1]
            .attach_peer_session(session)
            .expect_err("foreign original owner must refuse");
        assert_eq!(rejected.reason, LogicalNodeError::WrongBinding);
        assert_eq!(rejected.session.binding(), binding);
        assert!(deferred(&nodes[1]) == original);
        assert_eq!(nodes[1].local().outbound.usage(), usage);
        session = rejected.session;
    }
    let usage = nodes[slot].local().outbound.peer_usage(support::node(204));
    nodes[slot]
        .attach_peer_session(session)
        .unwrap_or_else(|e| panic!("genuine target session {:?}", e.reason));
    assert_eq!(
        nodes[slot].local().outbound.peer_usage(support::node(204)),
        usage
    );
    assert_eq!(
        nodes[slot].inspect_deferred().count(),
        0,
        "metadata wait clears; queued credits remain"
    );
}
pub fn grant(
    driver: &mut ChildActivationDriver<'_, Top>,
    nodes: &mut [Middle],
    output: SnapshotExportCompletion,
) -> LogicalLearnerEnrollment {
    grant_for(driver, nodes, output, 3)
}
pub fn grant_for(
    driver: &mut ChildActivationDriver<'_, Top>,
    nodes: &mut [Middle],
    output: SnapshotExportCompletion,
    target_slot: usize,
) -> LogicalLearnerEnrollment {
    let ticket = output.ticket();
    let SnapshotExportOutcome::Loaded(snapshot) = output.outcome() else {
        panic!("original live source image");
    };
    let pointer = snapshot.application.as_ptr();
    let usage = nodes[0].local().snapshots.as_ref().unwrap().router.usage();
    assert!(usage.requests > 0);
    let rejected = driver
        .enroll_logical_learner(&mut nodes[1], &setup::lower_app(target_slot), output)
        .unwrap_err();
    assert_eq!(rejected.reason, ChildActivationDriverError::WrongBinding);
    let LogicalEnrollmentExport::Original(original) = rejected.export else {
        panic!("foreign source refusal must conserve opaque original");
    };
    assert_eq!(original.ticket(), ticket);
    let SnapshotExportOutcome::Loaded(snapshot) = original.outcome() else {
        panic!("same loaded original");
    };
    assert_eq!(snapshot.application.as_ptr(), pointer);
    assert_eq!(
        nodes[0].local().snapshots.as_ref().unwrap().router.usage(),
        usage
    );
    let grant = driver
        .enroll_logical_learner(&mut nodes[0], &setup::lower_app(target_slot), *original)
        .unwrap_or_else(|e| {
            panic!(
                "actual source351 original export={} {:?}",
                ticket.visit.sequence, e.reason
            )
        });
    assert_eq!(
        nodes[0]
            .local()
            .snapshots
            .as_ref()
            .unwrap()
            .router
            .usage()
            .requests,
        0
    );
    grant
}
