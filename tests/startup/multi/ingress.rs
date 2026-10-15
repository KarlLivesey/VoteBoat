// SPDX-License-Identifier: RPL-1.5
// Copyright (c) 2026 Karl Livesey
//! Real three-voter/group scoped ingress with independent host preparation.
use super::*;
use std::rc::Rc;
#[path = "ingress/application.rs"]
mod application;
#[path = "ingress/history.rs"]
mod history;
use application::{PreparedHost, Probe};
type PreparedNode = NativeNode<PreparedHost, NativeServiceConnector>;
struct ScopedHistory {
    root: PathBuf,
    addresses: Vec<std::net::SocketAddr>,
    nodes: Vec<PreparedNode>,
    clock: Instant,
    protocol: NativePeerProtocol,
    probe: Rc<Probe>,
}
struct Expected {
    ticket: ClientTicket,
    outcome: CounterOutcome,
    duplicate: bool,
}
fn request(group: usize, operation: u128, delta: i64) -> ClientRequest {
    ClientRequest {
        group: groups()[group].group,
        operation: OperationId::new(operation).unwrap(),
        bytes: delta.to_le_bytes().to_vec(),
    }
}
fn accept(
    ingress: &mut dyn ProposalIngress,
    expected: &mut Vec<Expected>,
    request: ClientRequest,
    outcome: CounterOutcome,
    duplicate: bool,
) {
    assert!(ingress.ready_leader(request.group));
    expected.push(Expected {
        ticket: ingress.submit(request).unwrap(),
        outcome,
        duplicate,
    });
}
fn originals(node: &mut PreparedNode) -> Vec<Expected> {
    node.with_submission_batch(|ingress| {
        let mut expected = Vec::new();
        for (id, delta, value) in [(1, 7, 7), (2, 2, 9)] {
            accept(
                ingress,
                &mut expected,
                request(0, id, delta),
                CounterOutcome::Value(value),
                false,
            );
        }
        let mut malformed = request(0, 99, 1);
        malformed.bytes.pop();
        let rejected = ingress.submit(malformed).unwrap_err();
        assert_eq!(
            rejected.reason,
            ClientError::Application(ApplicationError::InvalidCommand)
        );
        assert_eq!(rejected.request.group, groups()[0].group);
        assert_eq!(rejected.request.operation, OperationId::new(99).unwrap());
        assert_eq!(rejected.request.bytes, [1, 0, 0, 0, 0, 0, 0]);
        accept(
            ingress,
            &mut expected,
            request(0, 99, 1),
            CounterOutcome::Value(10),
            false,
        );
        accept(
            ingress,
            &mut expected,
            request(0, 1, 9),
            CounterOutcome::OperationConflict,
            true,
        );
        accept(
            ingress,
            &mut expected,
            request(0, 1, 7),
            CounterOutcome::Value(7),
            true,
        );
        for (g, id, delta, value) in [
            (1, 1, 3, 3),
            (1, 2, 1, 4),
            (0, 3, 5, 15),
            (2, 1, 4, 4),
            (2, 2, 2, 6),
        ] {
            accept(
                ingress,
                &mut expected,
                request(g, id, delta),
                CounterOutcome::Value(value),
                false,
            );
        }
        expected
    })
}
fn retries(node: &mut PreparedNode) -> Vec<Expected> {
    node.with_submission_batch(|ingress| {
        let mut expected = Vec::new();
        for (group, operation, delta, outcome) in [
            (0, 1, 7, CounterOutcome::Value(7)),
            (0, 1, 9, CounterOutcome::OperationConflict),
            (1, 1, 3, CounterOutcome::Value(3)),
            (1, 2, 1, CounterOutcome::Value(4)),
            (2, 1, 4, CounterOutcome::Value(4)),
            (2, 2, 2, CounterOutcome::Value(6)),
        ] {
            accept(
                ingress,
                &mut expected,
                request(group, operation, delta),
                outcome,
                true,
            );
        }
        expected
    })
}
fn history(protocol: NativePeerProtocol) {
    let mut h = ScopedHistory::new(protocol);
    h.campaign();
    let expected = originals(&mut h.nodes[0]);
    assert_eq!(expected.len(), 10);
    h.assert_idle();
    assert_eq!(h.probe.factories.get(), 4);
    let pending: Vec<Vec<_>> = h
        .probe
        .pending
        .borrow()
        .iter()
        .map(|items| items.iter().map(|(id, _)| id.get()).collect())
        .collect();
    assert_eq!(pending, [vec![1], vec![1, 2, 99], vec![1], vec![1]]);
    assert_eq!(h.probe.calls.get(), 6);
    h.collect(&expected);
    h.readback();
    h.checkpoint();
    h.close();
    h.open(NativeOpenMode::Recover);
    h.readback();
    h.campaign();
    let factories = h.probe.factories.get();
    let expected = retries(&mut h.nodes[0]);
    h.assert_idle();
    assert_eq!(h.probe.factories.get() - factories, 3);
    h.collect(&expected);
    h.readback();
    h.close();
    std::fs::remove_dir_all(h.root).unwrap();
}
#[test]
fn tcp_scoped_ingress_keeps_individual_host_outcomes_and_recovers_all_groups() {
    history(NativePeerProtocol::TcpTls);
}
#[cfg(feature = "quic")]
#[test]
fn quic_scoped_ingress_keeps_individual_host_outcomes_and_recovers_all_groups() {
    history(NativePeerProtocol::Quic);
}
