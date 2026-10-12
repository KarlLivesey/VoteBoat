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
//! Physical adapter delegates the actual Node routers without constructing outputs.
use super::*;
mod activation;
impl<
        S: ReadyScheduler,
        T: TimerService,
        E: ElectionEntropy,
        L: LogCodec + Clone,
        V: SnapshotCodec + Clone,
        B: WireCodec + Clone,
        W: PersistenceWorker,
        O: OutboundQueue,
        H: SnapshotWorker,
        N: PeerNetworkConfiguration,
    > sealed::Sealed for NetworkNode<S, T, E, LogicalApplication<L, V, B>, W, O, H, N>
where
    N::Endpoint: Clone,
{
}
impl<
        S: ReadyScheduler,
        T: TimerService,
        E: ElectionEntropy,
        L: LogCodec + Clone,
        V: SnapshotCodec + Clone,
        B: WireCodec + Clone,
        W: PersistenceWorker,
        O: OutboundQueue,
        H: SnapshotWorker,
        N: PeerNetworkConfiguration,
    > LogicalBackingChild for NetworkNode<S, T, E, LogicalApplication<L, V, B>, W, O, H, N>
where
    N::Endpoint: Clone,
{
    type Scheduler = S;
    type Timers = T;
    type Entropy = E;
    type Log = L;
    type Snapshot = V;
    type Wire = B;
    type Worker = W;
    type Outbound = O;
    type Snapshots = H;
    type PollBudget = NodePollBudget;
    fn local(&self) -> &LogicalBackingLocal<Self> {
        NetworkNode::local(self)
    }
    fn state(&self) -> NodeState {
        NetworkNode::state(self)
    }
    fn endpoint_chain(
        &self,
        group: GroupIdentity,
    ) -> Result<LogicalEndpointChain, LogicalEndpointError> {
        if NetworkNode::state(self) != NodeState::Running {
            return Err(LogicalEndpointError::StaleOwner);
        }
        let local = NetworkNode::local(self);
        let core = local
            .owner
            .core(group)
            .ok_or(LogicalEndpointError::WrongBinding)?;
        LogicalEndpointChain::physical(core, local.owner.authorization())
    }
    fn poll(
        &mut self,
        now: MonoTime,
        budget: Self::PollBudget,
    ) -> Result<(), LogicalBackingPollError> {
        NetworkNode::poll(self, now, budget)
            .map(|_| ())
            .map_err(|e| LogicalBackingPollError::Node(Box::new(e)))
    }
    fn propose(&mut self, request: ClientRequest) -> Result<ClientTicket, ClientRejected> {
        NetworkNode::propose(self, request)
    }
    fn cancel_client(&mut self, ticket: ClientTicket) -> Result<(), ClientError> {
        NetworkNode::cancel_client(self, ticket)
    }
    fn cancel_read(&mut self, ticket: ReadInvocationTicket) -> Result<(), ReadInvocationError> {
        NetworkNode::cancel_read(self, ticket)
    }
    fn checkpoint(&mut self, group: GroupIdentity) -> Result<(), LogicalBackingPollError> {
        NetworkNode::control(self, group, NodeControl::Checkpoint)
            .map_err(|e| LogicalBackingPollError::Node(Box::new(e)))
    }
    fn read(
        &mut self,
        group: GroupIdentity,
        query: LogicalQuery,
    ) -> Result<ReadInvocationTicket, Box<ReadInvocationRejected<LogicalQuery>>> {
        NetworkNode::read(self, group, query).map_err(Box::new)
    }
    fn poll_client(&mut self) -> Option<ClientCompletion<LogicalReceipt>> {
        NetworkNode::poll_client(self)
    }
    fn complete_client(
        &mut self,
        output: ClientCompletion<LogicalReceipt>,
    ) -> Result<ClientOutcome<LogicalReceipt>, ClientCompletionRejected<LogicalReceipt>> {
        NetworkNode::complete_client(self, output)
    }
    fn poll_read(&mut self) -> Option<ReadCompletion<LogicalRead>> {
        NetworkNode::poll_read(self)
    }
    fn complete_read(
        &mut self,
        output: ReadCompletion<LogicalRead>,
    ) -> Result<ReadOutcome<LogicalRead>, ReadCompletionRejected<LogicalRead>> {
        NetworkNode::complete_read(self, output)
    }
    fn backing_is_drained(&self) -> bool {
        // The export consumer cannot abandon its original load/image credits.
        // Ordinary maintenance remains owned by the same live physical Node;
        // moving its borrower does not require whole-Node quiescence.
        NetworkNode::local(self)
            .snapshots
            .as_ref()
            .is_none_or(|s| s.router.checkpoint_exports_are_drained())
    }
    fn backing_pump(&mut self) -> Option<LogicalBackingPump<'_>> {
        None
    }
}
