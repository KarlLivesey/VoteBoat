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
//! Preserve original Node ownership while adapting only manifest read shapes.
use crate::{
    application::*, connect::PeerConnector, identity::ResponsibilityIdentity,
    outbound::OutboundQueue, routing::*, runtime::*, snapshot_worker::SnapshotWorker,
    transport::PeerTransportFactory, worker::PersistenceWorker,
};
/// An owning read view of an existing Node; no new worker, store or runtime.
/// Exclusive read-result consumption is required while a lookup is pending.
/// The Node remains accessible for polling and its original control APIs.
pub struct MappedManifestReadSource<N, M> {
    node: N,
    mapping: std::marker::PhantomData<fn() -> M>,
}
impl<N, M> MappedManifestReadSource<N, M> {
    pub fn new(node: N) -> Self {
        Self {
            node,
            mapping: std::marker::PhantomData,
        }
    }
    pub fn node_mut(&mut self) -> &mut N {
        &mut self.node
    }
    /// Caller must retain/resolve any accepted read ticket before dropping Node.
    pub fn into_node(self) -> N {
        self.node
    }
}
impl<
        S: ReadyScheduler,
        T: TimerService,
        E: ElectionEntropy,
        A: ProposalAdmission
            + BoundedReadableStateMachine<Query = M::Query, ReadResult = M::ReadResult>
            + CheckpointStateMachine,
        W: PersistenceWorker,
        O: OutboundQueue,
        H: SnapshotWorker,
        C: PeerConnector,
        F: PeerTransportFactory<C::Session>,
        M: ManifestReadMapping,
    > ManifestReadSource for MappedManifestReadSource<Node<S, T, E, A, W, O, H, C, F>, M>
where
    A::Receipt: ApplicationReceipt,
    C::Endpoint: Clone,
{
    type ReadResult = M::ReadResult;
    fn binding(&self) -> ReadInvocationBinding {
        self.node.local().reads.binding()
    }
    fn pending_reads(&self) -> usize {
        self.node.local().reads.usage().requests
    }
    fn submit(
        &mut self,
        request: ManifestLookup,
    ) -> Result<ReadInvocationTicket, ReadInvocationRejected<ResponsibilityIdentity>> {
        self.node
            .read(
                request.locator.authority,
                M::query(request.locator.responsibility),
            )
            .map_err(|r| ReadInvocationRejected {
                reason: r.reason,
                group: r.group,
                query: request.locator.responsibility,
            })
    }
    fn poll_result(
        &mut self,
        ticket: ReadInvocationTicket,
    ) -> Result<
        Option<ReadOutcome<Option<ResponsibilityManifest>>>,
        ReadCompletionRejected<Self::ReadResult>,
    > {
        let Some(output) = self.node.poll_read() else {
            return Ok(None);
        };
        if output.ticket() != ticket {
            return Err(ReadCompletionRejected {
                reason: ReadInvocationError::WrongBinding,
                completion: Box::new(output),
            });
        }
        self.node.complete_read(output).map(|r| {
            Some(match r {
                ReadOutcome::Read { barrier, result } => ReadOutcome::Read {
                    barrier,
                    result: result.and_then(M::manifest),
                },
                ReadOutcome::NotRead(e) => ReadOutcome::NotRead(e),
                ReadOutcome::Unavailable(e) => ReadOutcome::Unavailable(e),
            })
        })
    }
    fn cancel(&mut self, ticket: ReadInvocationTicket) -> Result<(), ReadInvocationError> {
        self.node.cancel_read(ticket)
    }
}
