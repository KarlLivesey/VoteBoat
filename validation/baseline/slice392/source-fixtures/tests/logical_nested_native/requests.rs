// SPDX-License-Identifier: RPL-1.5
// Copyright (c) 2026 Karl Livesey
//! Role inspection routes requests; only consumed opaque outputs prove success.
use super::{host::Host, setup, watchdog::Watchdog};
use voteboat::{
    identity::OperationId,
    raft::{RaftError, Role},
    runtime::*,
};
pub const ATTEMPTS: usize = 4;
pub fn valid_step(
    step: &OwnerStep,
    clients: &[ClientTicket],
    reads: &[ReadInvocationTicket],
) -> bool {
    let Some(error) = &step.error else {
        return true;
    };
    let client = matches!(error, RaftError::Busy | RaftError::NotLeader)
        && clients.iter().any(|ticket| {
            step.visit.owner == ticket.binding.owner
                && step.visit.group == ticket.group
                && step.operation == Some(ticket.operation)
        });
    let read = matches!(
        error,
        RaftError::Busy | RaftError::NotLeader | RaftError::ReadNotReady
    ) && reads.iter().any(|ticket| {
        step.visit.owner == ticket.binding.owner
            && step.visit.group == ticket.group
            && step.read == Some(ticket.request)
    });
    client || read
}
pub fn check(host: &Host<'_, '_>, watch: &Watchdog, request: &str) {
    assert!(
        watch.within_absolute_bound(),
        "original {request} functional timeout: {}",
        super::diagnostic::retained(host)
    );
}
fn leader(host: &Host<'_, '_>) -> Option<usize> {
    host.nodes
        .iter()
        .enumerate()
        .filter_map(|(slot, n)| {
            let core = n.local().owner.core(setup::top().group).unwrap();
            (core.role() == Role::Leader).then_some((slot, core.state().hard_state.term))
        })
        .max_by_key(|(_, term)| *term)
        .map(|(slot, _)| slot)
}
pub fn client(
    host: &mut Host<'_, '_>,
    operation: OperationId,
    bytes: &[u8],
) -> (usize, ClientTicket, u64) {
    loop {
        host.poll();
        let Some(node) = leader(host) else { continue };
        let before = host.nodes[node]
            .local()
            .owner
            .core(setup::top().group)
            .unwrap()
            .state()
            .last_index();
        match host.nodes[node].propose(ClientRequest {
            group: setup::top().group,
            operation,
            bytes: bytes.to_vec(),
        }) {
            Ok(ticket) => {
                assert!(host.accepted_clients.len() < ATTEMPTS * 2);
                host.accepted_clients.push(ticket);
                host.last_proposal[node] = None;
                return (node, ticket, before);
            }
            Err(error) => {
                assert_eq!(error.request.operation, operation);
                assert_eq!(error.request.bytes, bytes);
                assert!(
                    matches!(
                        error.reason,
                        ClientError::Overloaded
                            | ClientError::Consensus(RaftError::Busy | RaftError::NotLeader)
                    ),
                    "original client admission: {error:?}"
                );
            }
        }
    }
}
pub fn read(host: &mut Host<'_, '_>, watch: &Watchdog) -> (usize, ReadInvocationTicket) {
    loop {
        host.poll();
        check(host, watch, "read routing");
        let Some(node) = leader(host) else { continue };
        match host.nodes[node].read(setup::top().group, ()) {
            Ok(ticket) => {
                assert!(host.accepted_reads.len() < ATTEMPTS);
                host.accepted_reads.push(ticket);
                host.last_read[node] = None;
                return (node, ticket);
            }
            Err(error) => assert!(
                matches!(
                    error.reason,
                    ReadInvocationError::Overloaded
                        | ReadInvocationError::InFlight
                        | ReadInvocationError::Consensus(
                            RaftError::Busy | RaftError::NotLeader | RaftError::ReadNotReady
                        )
                ),
                "original read admission: {error:?}"
            ),
        }
    }
}
