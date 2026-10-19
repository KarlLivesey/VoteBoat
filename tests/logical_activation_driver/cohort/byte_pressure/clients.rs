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
//! Derive genuine client-byte ownership from the original accepted first charge.
use super::*;
use voteboat::application::{CounterOutcome, CounterReceipt};
pub type Client = ClientCompletion<ActivationReceipt<CounterReceipt>>;
pub struct ByteCharge {
    pub normal: usize,
    pub held: usize,
    pub ordinary: usize,
    pub ordinary_count: usize,
}
pub fn hold_duplicate(
    peers: &mut progress::Peers<'_>,
    parent: &mut fixture::FacadeNode,
    target: &mut aliases::Bridge<'_>,
) -> (Client, ByteCharge) {
    let bytes = canonical(7);
    let original_capacity = bytes.capacity();
    let before = parent.local().clients.usage();
    assert_eq!(before.requests, 0);
    let operation = voteboat::application::Counter::operation(1, 1).unwrap();
    let first = parent
        .propose(ClientRequest {
            group: groups::mapping(1).parent,
            operation,
            bytes,
        })
        .unwrap();
    let normal = parent
        .local()
        .clients
        .usage()
        .bytes
        .checked_sub(before.bytes)
        .unwrap();
    let first = wait(peers, parent, target, first);
    applied(&first, operation, false);
    parent.complete_client(first).unwrap();
    assert_eq!(parent.local().clients.usage(), before);
    let l = parent.local().clients.limits();
    let ordinary = l.bytes.checked_sub(l.control_bytes).unwrap();
    let base = normal.checked_sub(original_capacity).unwrap();
    let capacity = ordinary
        .checked_sub(base)
        .unwrap()
        .checked_sub(normal.checked_sub(1).unwrap())
        .unwrap();
    let canonical = canonical(7);
    assert_eq!(canonical.capacity(), original_capacity);
    let mut bytes = Vec::with_capacity(capacity);
    bytes.extend_from_slice(&canonical);
    assert_eq!(bytes.capacity(), capacity);
    assert!(bytes.len() <= l.command_bytes && capacity <= l.bytes);
    let expected = base.checked_add(capacity).unwrap();
    let ticket = parent
        .propose(ClientRequest {
            group: groups::mapping(1).parent,
            operation,
            bytes,
        })
        .unwrap();
    assert_eq!(parent.local().clients.usage().bytes, expected);
    let output = wait(peers, parent, target, ticket);
    applied(&output, operation, true);
    let charge = ByteCharge {
        normal,
        held: expected,
        ordinary,
        ordinary_count: l.requests - l.control_requests,
    };
    assert_eq!(
        l.group_requests - l.group_control_requests,
        charge.ordinary_count
    );
    charge.assert_byte_refusal(parent);
    eprintln!(
        "338 original charge normal={} held={} ordinary={} retained_capacity={} count={}/{}",
        normal,
        expected,
        ordinary,
        capacity,
        parent.local().clients.usage().requests,
        charge.ordinary_count
    );
    (output, charge)
}
impl ByteCharge {
    pub fn assert_byte_refusal(&self, parent: &mut fixture::FacadeNode) {
        let usage = parent.local().clients.usage();
        assert_eq!(usage.requests, 1);
        assert_eq!(usage.bytes, self.held);
        assert!(usage.requests < self.ordinary_count);
        assert!(self.held + self.normal > self.ordinary);
        assert_eq!(self.ordinary - self.held, self.normal - 1);
        assert!(self.held + self.normal <= parent.local().clients.limits().bytes);
        let bytes = canonical(5);
        let pointer = bytes.as_ptr();
        let capacity = bytes.capacity();
        let rejected = parent
            .propose(ClientRequest {
                group: groups::mapping(1).parent,
                operation: voteboat::application::Counter::operation(1, 2).unwrap(),
                bytes,
            })
            .unwrap_err();
        assert_eq!(rejected.reason, ClientError::Overloaded);
        assert_eq!(rejected.request.bytes.as_ptr(), pointer);
        assert_eq!(rejected.request.bytes.capacity(), capacity);
        assert_eq!(rejected.request.bytes, canonical(5));
        assert_eq!(parent.local().clients.usage(), usage);
    }
}
fn canonical(delta: i64) -> Vec<u8> {
    fixture::app_with_mappings((0..3).map(groups::mapping).collect())
        .business(&voteboat::application::Counter::command(0, delta))
        .unwrap()
}
fn wait(
    peers: &mut progress::Peers<'_>,
    parent: &mut fixture::FacadeNode,
    target: &mut aliases::Bridge<'_>,
    ticket: ClientTicket,
) -> Client {
    let end = harness::Wait::new(&peers.clock, harness::LIVENESS_MS);
    loop {
        peers.initial(parent, target);
        if let Some(output) = parent.poll_client() {
            assert_eq!(output.ticket(), ticket);
            return output;
        }
        assert!(
            end.within(&peers.clock),
            "actual original byte-pressure client"
        );
    }
}
pub fn applied(output: &Client, operation: OperationId, duplicate: bool) {
    match output.outcome() {
        ClientOutcome::Applied {
            position,
            receipt: ActivationReceipt::Business(receipt),
        } => {
            assert_eq!(receipt.operation, operation);
            assert_eq!(receipt.index, position.index);
            assert_eq!(receipt.outcome, CounterOutcome::Value(7));
            assert_eq!(receipt.duplicate, duplicate);
        }
        other => panic!("actual original byte-pressure client {other:?}"),
    }
}
