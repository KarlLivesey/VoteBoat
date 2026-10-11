// SPDX-License-Identifier: RPL-1.5
// Copyright (c) 2026 Karl Livesey
use super::*;
use voteboat::identity::OperationId;
#[test]
fn accepted_start_abort_reclaims_real_workers_and_cold_reopens() {
    let root = std::env::temp_dir().join(format!(
        "voteboat-embedded-transfer-recovery-{}",
        std::process::id()
    ));
    let tls = Path::new(env!("CARGO_MANIFEST_DIR")).join("tests/fixtures/tls");
    let mut node = open(root.to_str().unwrap(), &tls, NativeOpenMode::Create).unwrap();
    let clock = Instant::now();
    for group in groups() {
        node.control(group, NodeControl::Campaign).unwrap();
    }
    drive(&mut node, clock, |n| {
        Ok(groups().into_iter().all(|g| {
            let c = n.local().owner.core(g).unwrap();
            c.role() == voteboat::raft::Role::Leader
                && !c.has_pending_dependency()
                && n.local().applications[&g].applied_index() >= c.state().commit_index
        }))
    })
    .unwrap();
    let id = TransactionId::new(1).unwrap();
    let original = spec(id, 7, "forward").unwrap();
    let ticket = node
        .propose(ClientRequest {
            group: profile().coordinator(),
            operation: OperationId::new(17).unwrap(),
            bytes: TransferApplication::start(original).unwrap(),
        })
        .unwrap();
    assert_eq!(ticket.operation, original.phases().coordinator()[0]);
    node.abort();
    assert_eq!(node.state(), NodeState::RecoveryRequired);
    assert_eq!(node.failure(), Some(&NodeError::Aborted));
    close(node, clock).unwrap();
    let recovered = open(root.to_str().unwrap(), &tls, NativeOpenMode::Recover).unwrap();
    close(recovered, Instant::now()).unwrap();
    std::fs::remove_dir_all(root).unwrap();
}
