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
use super::*;
mod activation;
impl<
        S: ReadyScheduler,
        T: TimerService,
        E: ElectionEntropy,
        U: SecureSession,
        C: LogCodec + Clone,
        V: SnapshotCodec + Clone,
        B: WireCodec + Clone,
    > bridge::backing::sealed::Sealed
    for LogicalNode<S, T, E, LogicalApplication<C, V, B>, U, C, V, B>
{
}
impl<
        S: ReadyScheduler,
        T: TimerService,
        E: ElectionEntropy,
        U: SecureSession,
        C: LogCodec + Clone,
        V: SnapshotCodec + Clone,
        B: WireCodec + Clone,
    > LogicalBackingChild for LogicalNode<S, T, E, LogicalApplication<C, V, B>, U, C, V, B>
{
    type Scheduler = S;
    type Timers = T;
    type Entropy = E;
    type Log = C;
    type Snapshot = V;
    type Wire = B;
    type Worker = LogicalPersistenceWorker;
    type Outbound = LogicalNetworkQueue;
    type Snapshots = LogicalSnapshotWorker;
    type PollBudget = LogicalNodePollBudget;
    fn local(&self) -> &LogicalBackingLocal<Self> {
        LogicalNode::local(self)
    }
    fn state(&self) -> NodeState {
        LogicalNode::state(self)
    }
    fn endpoint_chain(
        &self,
        group: GroupIdentity,
    ) -> Result<LogicalEndpointChain, LogicalEndpointError> {
        LogicalNode::endpoint_chain(self, group)
    }
    fn poll(
        &mut self,
        now: MonoTime,
        budget: Self::PollBudget,
    ) -> Result<(), LogicalBackingPollError> {
        LogicalNode::poll(self, now, budget)
            .map(|_| ())
            .map_err(|e| LogicalBackingPollError::LogicalNode(Box::new(e)))
    }
    fn propose(&mut self, request: ClientRequest) -> Result<ClientTicket, ClientRejected> {
        LogicalNode::propose(self, request)
    }
    fn cancel_client(&mut self, ticket: ClientTicket) -> Result<(), ClientError> {
        LogicalNode::cancel_client(self, ticket)
    }
    fn cancel_read(&mut self, ticket: ReadInvocationTicket) -> Result<(), ReadInvocationError> {
        LogicalNode::cancel_read(self, ticket)
    }
    fn checkpoint(&mut self, group: GroupIdentity) -> Result<(), LogicalBackingPollError> {
        if group != self.group {
            return Err(LogicalBackingPollError::LogicalNode(Box::new(
                LogicalNodeError::WrongBinding,
            )));
        }
        LogicalNode::checkpoint(self).map_err(|e| LogicalBackingPollError::LogicalNode(Box::new(e)))
    }
    fn read(
        &mut self,
        group: GroupIdentity,
        query: LogicalQuery,
    ) -> Result<ReadInvocationTicket, Box<ReadInvocationRejected<LogicalQuery>>> {
        LogicalNode::read(self, group, query).map_err(Box::new)
    }
    fn poll_client(&mut self) -> Option<ClientCompletion<LogicalReceipt>> {
        LogicalNode::poll_client(self)
    }
    fn complete_client(
        &mut self,
        output: ClientCompletion<LogicalReceipt>,
    ) -> Result<ClientOutcome<LogicalReceipt>, ClientCompletionRejected<LogicalReceipt>> {
        LogicalNode::complete_client(self, output)
    }
    fn poll_read(&mut self) -> Option<ReadCompletion<LogicalRead>> {
        LogicalNode::poll_read(self)
    }
    fn complete_read(
        &mut self,
        output: ReadCompletion<LogicalRead>,
    ) -> Result<ReadOutcome<LogicalRead>, ReadCompletionRejected<LogicalRead>> {
        LogicalNode::complete_read(self, output)
    }
    fn backing_is_drained(&self) -> bool {
        self.work_is_drained()
    }
    fn backing_pump(&mut self) -> Option<LogicalBackingPump<'_>> {
        Some(LogicalNode::backing_pump(self))
    }
}
