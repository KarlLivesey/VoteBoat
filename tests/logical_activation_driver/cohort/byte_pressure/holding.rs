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
//! Keep original duplicate and fresh read outputs externally charged throughout.
use super::*;
type Read = ReadCompletion<ActivationRead<i64>>;
pub struct Held {
    client: clients::Client,
    reads: [Read; 2],
    clients: ClientUsage,
    invocations: ReadInvocationUsage,
    charge: clients::ByteCharge,
}
impl Held {
    pub fn new(
        peers: &mut progress::Peers<'_>,
        parent: &mut fixture::FacadeNode,
        target: &mut aliases::Bridge<'_>,
    ) -> Self {
        let (client, charge) = clients::hold_duplicate(peers, parent, target);
        let reads = [read(peers, parent, target), read(peers, parent, target)];
        let held = Self {
            client,
            reads,
            clients: parent.local().clients.usage(),
            invocations: parent.local().reads.usage(),
            charge,
        };
        held.assert_retained(parent);
        eprintln!(
            "338 original held usage clients={:?} reads={:?}",
            held.clients, held.invocations
        );
        held
    }
    pub fn assert_retained(&self, parent: &fixture::FacadeNode) {
        assert_eq!(parent.local().clients.usage(), self.clients);
        assert_eq!(parent.local().reads.usage(), self.invocations);
        assert_eq!(self.clients.requests, 1);
        assert_eq!(self.invocations.requests, 2);
        assert!(!parent.local().clients.is_drained());
        assert!(!parent.local().reads.is_drained());
        assert!(!parent.is_drained());
        let group = groups::mapping(1).parent;
        assert!(parent
            .local()
            .clients
            .retains_only_exported_ordinary_outputs(group));
        assert!(parent
            .local()
            .reads
            .retains_only_exported_ordinary_outputs(group));
        assert!(parent.local().results.is_drained());
    }
    pub fn refused(&self, parent: &mut fixture::FacadeNode) {
        self.assert_retained(parent);
        self.charge.assert_byte_refusal(parent);
    }
    pub fn complete(self, parent: &mut fixture::FacadeNode) {
        self.assert_retained(parent);
        clients::applied(&self.client, OperationId::new(33301).unwrap(), true);
        parent.complete_client(self.client).unwrap();
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
fn read(
    peers: &mut progress::Peers<'_>,
    parent: &mut fixture::FacadeNode,
    target: &mut aliases::Bridge<'_>,
) -> Read {
    let ticket = parent
        .read(groups::mapping(1).parent, ActivationQuery::Business(()))
        .unwrap();
    let end = harness::Wait::new(&peers.clock, harness::LIVENESS_MS);
    loop {
        peers.initial(parent, target);
        if let Some(output) = parent.poll_read() {
            assert_eq!(output.ticket(), ticket);
            match output.outcome() {
                ReadOutcome::Read {
                    barrier,
                    result: Ok(ActivationRead::Business(7)),
                } => {
                    assert_eq!(barrier.group(), ticket.group);
                    assert_eq!(barrier.request(), ticket.request);
                }
                other => panic!("actual original byte-pressure quorum read {other:?}"),
            }
            return output;
        }
        assert!(
            end.within(&peers.clock),
            "actual original byte-pressure quorum read"
        );
    }
}
