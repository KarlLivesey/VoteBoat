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
//! Original business receipt/retry and fresh scoped parent quorum reads.
use super::*;
use voteboat::{application::CounterOutcome, raft::RaftError};
pub fn requests(
    peers: &mut progress::Peers<'_>,
    parent: &mut fixture::FacadeNode,
    target: &mut aliases::Bridge<'_>,
    operation: u128,
    delta: i64,
    value: i64,
) {
    let op = OperationId::new(operation).unwrap();
    let bytes = fixture::app_with_mappings((0..3).map(groups::mapping).collect())
        .business(&delta.to_le_bytes())
        .unwrap();
    let first = write(peers, parent, target, op, &bytes, value, false);
    let retry = write(peers, parent, target, op, &bytes, value, true);
    assert!(retry.index >= first.index);
    read(peers, parent, target, first.index, value);
}
fn read(
    peers: &mut progress::Peers<'_>,
    parent: &mut fixture::FacadeNode,
    target: &mut aliases::Bridge<'_>,
    first_index: u64,
    value: i64,
) {
    let mut ticket = None;
    let end = harness::Wait::new(&peers.clock, harness::LIVENESS_MS);
    loop {
        peers.initial(parent, target);
        if ticket.is_none() {
            match parent.read(groups::mapping(1).parent, ActivationQuery::Business(())) {
                Ok(original) => ticket = Some(original),
                Err(e)
                    if matches!(
                        e.reason,
                        ReadInvocationError::Consensus(RaftError::NotLeader)
                            | ReadInvocationError::Overloaded
                    ) => {}
                Err(e) => panic!("actual original read admission {:?}", e.reason),
            }
        }
        if let Some(output) = parent.poll_read() {
            assert_eq!(Some(output.ticket()), ticket);
            let original = ticket.take().unwrap();
            match parent.complete_read(output).unwrap() {
                ReadOutcome::Read {
                    barrier,
                    result: Ok(ActivationRead::Business(observed)),
                } => {
                    assert_eq!(barrier.group(), groups::mapping(1).parent);
                    assert_eq!(barrier.request(), original.request);
                    assert!(barrier.index() >= first_index);
                    assert_eq!(observed, value);
                    return;
                }
                ReadOutcome::Unavailable(ReadUnavailable::LeadershipChanged)
                | ReadOutcome::NotRead(RaftError::NotLeader | RaftError::Busy) => (),
                other => panic!("actual parent quorum read {other:?}"),
            }
        }
        assert!(end.within(&peers.clock), "original parent fresh read");
    }
}
fn write(
    peers: &mut progress::Peers<'_>,
    parent: &mut fixture::FacadeNode,
    target: &mut aliases::Bridge<'_>,
    operation: OperationId,
    bytes: &[u8],
    value: i64,
    duplicate: bool,
) -> ProposalPosition {
    let mut ticket = None;
    let mut unknown = false;
    let end = harness::Wait::new(&peers.clock, harness::LIVENESS_MS);
    loop {
        peers.initial(parent, target);
        if ticket.is_none() {
            match parent.propose(ClientRequest {
                group: groups::mapping(1).parent,
                operation,
                bytes: bytes.to_vec(),
            }) {
                Ok(original) => ticket = Some(original),
                Err(e)
                    if matches!(
                        e.reason,
                        ClientError::Consensus(RaftError::NotLeader) | ClientError::Overloaded
                    ) => {}
                Err(e) => panic!("actual original client admission {:?}", e.reason),
            }
        }
        if let Some(output) = parent.poll_client() {
            assert_eq!(Some(output.ticket()), ticket);
            let original = ticket.take().unwrap();
            match parent.complete_client(output).unwrap() {
                ClientOutcome::Applied {
                    position,
                    receipt: ActivationReceipt::Business(receipt),
                } => {
                    assert_eq!(receipt.operation, operation);
                    assert_eq!(receipt.outcome, CounterOutcome::Value(value));
                    if duplicate || !unknown {
                        assert_eq!(receipt.duplicate, duplicate);
                    }
                    assert_eq!(receipt.index, position.index);
                    return position;
                }
                ClientOutcome::Unknown(ClientUnknown::LeadershipChanged) => {
                    unknown = true;
                    eprintln!(
                        "332 originalUnknown operation={} ticket={} same-content retry",
                        operation.get(),
                        original.sequence
                    );
                }
                ClientOutcome::NotProposed(RaftError::NotLeader | RaftError::Busy) => (),
                other => panic!("actual original parent client {other:?}"),
            }
        }
        assert!(end.within(&peers.clock), "original parent business receipt");
    }
}
