// SPDX-License-Identifier: RPL-1.5
// Copyright (c) 2026 Karl Livesey
use super::*;

#[test]
fn scoped_node_submissions_retain_individual_results_after_partial_refusal() {
    let mut n = elected();
    let tickets = n.with_submission_batch(|scope| {
        assert!(scope.ready_leader(group(1)));
        assert!(!scope.ready_leader(group(2)));
        assert!(!scope.ready_leader(group(99)));
        let first = scope.submit(request(1)).unwrap();
        let mut invalid = request(2);
        invalid.bytes.push(0);
        let pointer = invalid.bytes.as_ptr();
        let rejected = scope.submit(invalid).unwrap_err();
        assert_eq!(
            rejected.reason,
            ClientError::Application(ApplicationError::InvalidCommand)
        );
        assert_eq!(rejected.request.bytes.as_ptr(), pointer);
        let second = scope.submit(request(2)).unwrap();
        let duplicate = scope.submit(request(1)).unwrap();
        let mut absent = request(3);
        absent.group = group(99);
        assert_eq!(
            scope.submit(absent).unwrap_err().reason,
            ClientError::UnknownGroup
        );
        [first, second, duplicate]
    });
    settle(&mut n);
    for (offset, ticket) in tickets.into_iter().enumerate() {
        let output = n.poll_client().unwrap();
        assert_eq!(output.ticket(), ticket);
        let ClientOutcome::Applied { receipt, .. } = n.complete_client(output).unwrap() else {
            panic!("accepted original did not apply")
        };
        assert_eq!(receipt.operation, ticket.operation);
        assert_eq!(receipt.duplicate, offset == 2);
        assert_eq!(
            receipt.outcome,
            CounterOutcome::Value(if offset == 1 { 14 } else { 7 })
        );
    }
    assert!(n.local().clients.is_drained());
    shutdown(&mut n);
}

#[test]
fn scoped_node_drain_precedes_missing_application_and_preserves_request() {
    let mut n = elected();
    let groups = n
        .local()
        .owner
        .groups()
        .map(|g| DrainGroup {
            group: g,
            configuration: n.local().owner.core(g).unwrap().membership().id(),
        })
        .collect();
    n.begin_local_drain(LocalDrainRequest {
        operation: OperationId::new(100).unwrap(),
        groups,
    })
    .unwrap();
    n.with_submission_batch(|scope| {
        for group_id in [1, 99] {
            let mut command = request(7);
            command.group = group(group_id);
            command.bytes.reserve(17);
            let pointer = command.bytes.as_ptr();
            let capacity = command.bytes.capacity();
            let error = scope.submit(command).unwrap_err();
            assert_eq!(error.reason, ClientError::Draining);
            assert_eq!(error.request.bytes.as_ptr(), pointer);
            assert_eq!(error.request.bytes.capacity(), capacity);
            assert_eq!(error.request.bytes, 7i64.to_le_bytes());
        }
    });
    assert!(n.local().clients.is_drained());
    shutdown(&mut n);
}
