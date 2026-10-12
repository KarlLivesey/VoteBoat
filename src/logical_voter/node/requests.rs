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
impl<
        S: ReadyScheduler,
        T: TimerService,
        E: ElectionEntropy,
        A: ProposalAdmission + LogicalResponseCodec + CheckpointStateMachine,
        U: SecureSession,
        C: LogCodec + Clone,
        V: SnapshotCodec + Clone,
        B: WireCodec + Clone,
    > LogicalNode<S, T, E, A, U, C, V, B>
where
    A::Receipt: ApplicationReceipt,
{
    pub fn local(&self) -> &LogicalNodeLocal<S, T, E, A> {
        &self.local
    }
    pub fn state(&self) -> NodeState {
        self.state
    }
    pub fn failure(&self) -> Option<&LogicalNodeError> {
        self.failure.as_ref()
    }
    pub fn endpoint_chain(
        &self,
        group: GroupIdentity,
    ) -> Result<LogicalEndpointChain, LogicalEndpointError> {
        if self.state != NodeState::Running {
            return Err(LogicalEndpointError::StaleOwner);
        }
        let core = self
            .local
            .owner
            .core(group)
            .ok_or(LogicalEndpointError::WrongBinding)?;
        self.endpoint
            .current(self.local.owner.authorization(), core)
    }
    pub fn propose(&mut self, request: ClientRequest) -> Result<ClientTicket, ClientRejected> {
        if self.state != NodeState::Running {
            return Err(ClientRejected {
                reason: ClientError::Closed,
                request,
            });
        }
        let Some(app) = self.local.applications.get(&request.group) else {
            return Err(ClientRejected {
                reason: ClientError::UnknownGroup,
                request,
            });
        };
        self.local
            .clients
            .submit(&mut self.local.owner, app, request)
    }
    pub fn read(
        &mut self,
        group: GroupIdentity,
        query: A::Query,
    ) -> Result<ReadInvocationTicket, ReadInvocationRejected<A::Query>> {
        if self.state != NodeState::Running {
            return Err(ReadInvocationRejected {
                reason: ReadInvocationError::Closed,
                group,
                query,
            });
        }
        let Some(app) = self.local.applications.get(&group) else {
            return Err(ReadInvocationRejected {
                reason: ReadInvocationError::UnknownGroup,
                group,
                query,
            });
        };
        self.local
            .reads
            .submit_logical(&mut self.local.owner, app, group, query)
    }
    pub fn poll_client(&mut self) -> Option<ClientCompletion<A::Receipt>> {
        self.local.clients.poll()
    }
    pub fn complete_client(
        &mut self,
        output: ClientCompletion<A::Receipt>,
    ) -> Result<ClientOutcome<A::Receipt>, ClientCompletionRejected<A::Receipt>> {
        self.local.clients.complete(output)
    }
    pub fn poll_read(&mut self) -> Option<ReadCompletion<A::ReadResult>> {
        self.local.reads.poll()
    }
    /// Inspect an original retained invocation without consuming its outcome.
    /// This diagnostic snapshot is neither a quorum barrier nor read success.
    pub fn inspect_read(
        &self,
        ticket: ReadInvocationTicket,
    ) -> Result<ReadInvocationInspection, ReadInvocationError> {
        self.local.reads.inspect(ticket)
    }
    pub fn complete_read(
        &mut self,
        output: ReadCompletion<A::ReadResult>,
    ) -> Result<ReadOutcome<A::ReadResult>, ReadCompletionRejected<A::ReadResult>> {
        self.local.reads.complete(output)
    }
    pub fn cancel_client(&mut self, ticket: ClientTicket) -> Result<(), ClientError> {
        self.local.clients.cancel_wait(ticket)
    }
    pub fn cancel_read(&mut self, ticket: ReadInvocationTicket) -> Result<(), ReadInvocationError> {
        self.local.reads.cancel_wait(ticket)
    }
    /// Only these explicit controls are supported by this logical composition.
    /// Configuration execution remains a separately authorized protocol slice.
    pub fn campaign(&mut self) -> Result<(), LogicalNodeError> {
        self.control(Event::Campaign)
    }
    pub fn heartbeat(&mut self) -> Result<(), LogicalNodeError> {
        self.control(Event::Heartbeat)
    }
    pub fn checkpoint(&mut self) -> Result<(), LogicalNodeError> {
        if self.local.snapshots.is_none() {
            return Err(LogicalNodeError::Replica(ReplicaError::MissingSnapshots));
        }
        self.control(Event::Checkpoint)
    }
    fn control(&mut self, event: Event) -> Result<(), LogicalNodeError> {
        if self.state != NodeState::Running {
            return Err(LogicalNodeError::Closed);
        }
        self.local
            .owner
            .admit(self.group, event)
            .map_err(|e| LogicalNodeError::Owner(EffectOwnerError::Runtime(e.reason)))
    }
    /// Scoped lower progress remains available after upper intake closes or fails.
    /// No router, opaque output or mutable owner escapes this view.
    pub fn pump_backing<R>(
        &mut self,
        pump: impl for<'p> FnOnce(LogicalNodePump<'p, A::Receipt, A::ReadResult>) -> R,
    ) -> R {
        pump(LogicalNodePump {
            persistence: &mut self.persistence,
            releases: self.replica.releases(),
            snapshots: self.snapshots.as_mut(),
        })
    }
    pub fn backing_pump(&mut self) -> LogicalBackingPump<'_>
    where
        A: StateMachine<Receipt = LogicalReceipt> + ReadableStateMachine<ReadResult = LogicalRead>,
    {
        LogicalBackingPump {
            persistence: &mut self.persistence,
            releases: self.replica.releases(),
            snapshots: self.snapshots.as_mut(),
        }
    }
}
