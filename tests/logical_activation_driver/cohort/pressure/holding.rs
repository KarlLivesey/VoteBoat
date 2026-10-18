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
//! Hold original service outputs externally while exact selected credits remain.
use super::*;
use voteboat::application::{CounterOutcome, CounterReceipt};
type Client = ClientCompletion<ActivationReceipt<CounterReceipt>>;
type Read = ReadCompletion<ActivationRead<i64>>;
pub struct Held {
    clients: Vec<Client>,
    reads: Vec<Read>,
    client_usage: ClientUsage,
    read_usage: ReadInvocationUsage,
}
impl Held {
    pub fn new(
        peers: &mut progress::Peers<'_>,
        parent: &mut fixture::FacadeNode,
        target: &mut aliases::Bridge<'_>,
    ) -> Self {
        let first = client(peers, parent, target, 33301, 7, 7, false);
        let second = client(peers, parent, target, 33301, 7, 7, true);
        let a = read(peers, parent, target, ActivationQuery::Business(()));
        let b = read(peers, parent, target, ActivationQuery::Business(()));
        for output in [&a, &b] {
            assert!(matches!(
                output.outcome(),
                ReadOutcome::Read {
                    result: Ok(ActivationRead::Business(7)),
                    ..
                }
            ));
        }
        let held = Self {
            clients: vec![first, second],
            reads: vec![a, b],
            client_usage: parent.local().clients.usage(),
            read_usage: parent.local().reads.usage(),
        };
        held.assert_retained(parent);
        eprintln!(
            "335 original held usage clients={:?} reads={:?}",
            held.client_usage, held.read_usage
        );
        held
    }
    pub fn assert_retained(&self, parent: &fixture::FacadeNode) {
        assert_eq!(parent.local().clients.usage(), self.client_usage);
        assert_eq!(parent.local().reads.usage(), self.read_usage);
        assert_eq!(self.client_usage.requests, 2);
        assert_eq!(self.read_usage.requests, 2);
        assert!(!parent.local().clients.is_drained());
        assert!(!parent.local().reads.is_drained());
        assert!(!parent.is_drained());
        ownership::clear(parent);
    }
    pub fn reject_foreign(
        &mut self,
        peers: &mut progress::Peers<'_>,
        parent: &mut fixture::FacadeNode,
    ) {
        let before = peers.parents[1].local().clients.usage();
        // Move only the real original; rejection must return it unchanged.
        let a = self.clients.remove(0);
        let original = a.ticket();
        let rejected = peers.parents[1].complete_client(a).unwrap_err();
        assert_eq!(rejected.reason, ClientError::StaleTicket);
        assert_eq!(rejected.completion.ticket(), original);
        self.clients.insert(0, *rejected.completion);
        assert_eq!(peers.parents[1].local().clients.usage(), before);
        let before = peers.parents[1].local().reads.usage();
        let read = self.reads.remove(0);
        let original = read.ticket();
        let rejected = peers.parents[1].complete_read(read).unwrap_err();
        assert_eq!(rejected.reason, ReadInvocationError::StaleTicket);
        assert_eq!(rejected.completion.ticket(), original);
        self.reads.insert(0, *rejected.completion);
        assert_eq!(peers.parents[1].local().reads.usage(), before);
        self.assert_retained(parent);
    }
    pub fn tickets(&self) -> (ClientTicket, ReadInvocationTicket) {
        (self.clients[0].ticket(), self.reads[0].ticket())
    }
    pub fn complete(self, parent: &mut fixture::FacadeNode) {
        self.assert_retained(parent);
        for output in self.clients {
            assert!(matches!(
                parent.complete_client(output).unwrap(),
                ClientOutcome::Applied {
                    receipt: ActivationReceipt::Business(_),
                    ..
                }
            ));
        }
        for output in self.reads {
            assert!(matches!(
                parent.complete_read(output).unwrap(),
                ReadOutcome::Read {
                    result: Ok(ActivationRead::Business(7)),
                    ..
                }
            ));
        }
        assert!(parent.local().clients.is_drained());
        assert!(parent.local().reads.is_drained());
    }
}
fn client(
    peers: &mut progress::Peers<'_>,
    parent: &mut fixture::FacadeNode,
    target: &mut aliases::Bridge<'_>,
    id: u128,
    delta: i64,
    value: i64,
    duplicate: bool,
) -> Client {
    let operation = OperationId::new(id).unwrap();
    let app = fixture::app_with_mappings((0..3).map(groups::mapping).collect());
    let ticket = parent
        .propose(ClientRequest {
            group: groups::mapping(1).parent,
            operation,
            bytes: app.business(&delta.to_le_bytes()).unwrap(),
        })
        .unwrap();
    ownership::refused(parent); // accepted but not stepped: no observation authority.
    let end = harness::hang_guard();
    loop {
        peers.initial(parent, target);
        ownership::refused(parent); // also true just before a real ready output is exported.
        if let Some(output) = parent.poll_client() {
            assert_eq!(output.ticket(), ticket);
            match output.outcome() {
                ClientOutcome::Applied {
                    position,
                    receipt: ActivationReceipt::Business(receipt),
                } => {
                    assert_eq!(receipt.operation, operation);
                    assert_eq!(receipt.index, position.index);
                    assert_eq!(receipt.outcome, CounterOutcome::Value(value));
                    assert_eq!(receipt.duplicate, duplicate);
                }
                other => panic!("original held client {other:?}"),
            }
            return output;
        }
        assert!(Instant::now() < end, "original held client output");
    }
}
pub fn read(
    peers: &mut progress::Peers<'_>,
    parent: &mut fixture::FacadeNode,
    target: &mut aliases::Bridge<'_>,
    query: ActivationQuery<()>,
) -> Read {
    let ticket = parent.read(groups::mapping(1).parent, query).unwrap();
    ownership::refused(parent);
    let end = harness::hang_guard();
    loop {
        peers.initial(parent, target);
        ownership::refused(parent);
        if let Some(output) = parent.poll_read() {
            assert_eq!(output.ticket(), ticket);
            match output.outcome() {
                ReadOutcome::Read {
                    barrier,
                    result: Ok(_),
                } => {
                    assert_eq!(barrier.group(), ticket.group);
                    assert_eq!(barrier.request(), ticket.request);
                }
                other => panic!("original held read {other:?}"),
            }
            return output;
        }
        assert!(Instant::now() < end, "original held quorum read");
    }
}
