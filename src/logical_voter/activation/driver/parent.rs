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
use crate::{
    log_format::LogCodec, secure::SecureSession, snapshot_format::SnapshotCodec, wire::WireCodec,
};
pub type ActivationParentClientResult<R> =
    Result<ClientOutcome<ActivationReceipt<R>>, ClientCompletionRejected<ActivationReceipt<R>>>;
pub type ActivationParentReadResult<V> =
    Result<ReadOutcome<ActivationRead<V>>, ReadCompletionRejected<ActivationRead<V>>>;
pub(super) mod sealed {
    pub trait Sealed {}
}
/// Original owning logical endpoint. Implementations are crate-sealed; selected
/// providers remain replaceable through the existing LogicalNode contracts.
pub trait LogicalActivationParent: sealed::Sealed {
    type Receipt: ApplicationReceipt;
    type Read;
    fn profile(&self, group: GroupIdentity) -> Option<LogicalActivationControlProfile>;
    fn bindings(&self) -> (RuntimeOwner, ClientRouterBinding, ReadInvocationBinding);
    fn observer_bounds(&self) -> (usize, usize);
    fn is_running(&self) -> bool;
    fn observers_are_drained(&self) -> bool;
    /// No observer stream remains except externally exported terminal ordinary
    /// outputs for this selected group. Those outputs stay host-owned/charged.
    fn observer_stream_is_clear(&self, group: GroupIdentity) -> bool;
    fn begin_bytes(&self, intent: &ChildActivationIntent) -> Result<Vec<u8>, ApplicationError>;
    fn propose(&mut self, request: ClientRequest) -> Result<ClientTicket, ClientRejected>;
    fn status(
        &mut self,
        group: GroupIdentity,
        begin: OperationId,
    ) -> Result<ReadInvocationTicket, ReadInvocationError>;
    fn poll_client(&mut self) -> Option<ClientCompletion<ActivationReceipt<Self::Receipt>>>;
    fn complete_client(
        &mut self,
        output: ClientCompletion<ActivationReceipt<Self::Receipt>>,
    ) -> ActivationParentClientResult<Self::Receipt>;
    fn poll_read(&mut self) -> Option<ReadCompletion<ActivationRead<Self::Read>>>;
    fn complete_read(
        &mut self,
        output: ReadCompletion<ActivationRead<Self::Read>>,
    ) -> ActivationParentReadResult<Self::Read>;
    fn cancel_client(&mut self, ticket: ClientTicket) -> Result<(), ClientError>;
    fn cancel_read(&mut self, ticket: ReadInvocationTicket) -> Result<(), ReadInvocationError>;
    fn poll(
        &mut self,
        now: MonoTime,
        budget: LogicalNodePollBudget,
    ) -> Result<(), LogicalNodeError>;
    fn pump<R>(
        &mut self,
        pump: impl for<'p> FnOnce(
            LogicalNodePump<'p, ActivationReceipt<Self::Receipt>, ActivationRead<Self::Read>>,
        ) -> R,
    ) -> R;
}
impl<
        S: ReadyScheduler,
        T: TimerService,
        E: ElectionEntropy,
        A: ProposalAdmission + LogicalResponseCodec + CheckpointStateMachine,
        U: SecureSession,
        C: LogCodec + Clone,
        V: SnapshotCodec + Clone,
        B: WireCodec + Clone,
    > sealed::Sealed for LogicalNode<S, T, E, LogicalActivationApplication<A>, U, C, V, B>
where
    A::Receipt: ApplicationReceipt,
{
}
impl<
        S: ReadyScheduler,
        T: TimerService,
        E: ElectionEntropy,
        A: ProposalAdmission + LogicalResponseCodec + CheckpointStateMachine,
        U: SecureSession,
        C: LogCodec + Clone,
        V: SnapshotCodec + Clone,
        B: WireCodec + Clone,
    > LogicalActivationParent for LogicalNode<S, T, E, LogicalActivationApplication<A>, U, C, V, B>
where
    A::Receipt: ApplicationReceipt,
{
    type Receipt = A::Receipt;
    type Read = A::ReadResult;
    fn profile(&self, group: GroupIdentity) -> Option<LogicalActivationControlProfile> {
        self.local()
            .applications
            .get(&group)
            .map(LogicalActivationApplication::control_profile)
    }
    fn bindings(&self) -> (RuntimeOwner, ClientRouterBinding, ReadInvocationBinding) {
        (
            self.local().owner.identity(),
            self.local().clients.binding(),
            self.local().reads.binding(),
        )
    }
    fn observer_bounds(&self) -> (usize, usize) {
        (
            self.local().clients.limits().receipt_bytes,
            self.local().reads.limits().result_bytes,
        )
    }
    fn is_running(&self) -> bool {
        self.state() == NodeState::Running
    }
    fn observers_are_drained(&self) -> bool {
        self.local().clients.is_drained()
            && self.local().reads.is_drained()
            && self.local().results.is_drained()
    }
    fn observer_stream_is_clear(&self, group: GroupIdentity) -> bool {
        self.local()
            .clients
            .retains_only_exported_ordinary_outputs(group)
            && self
                .local()
                .reads
                .retains_only_exported_ordinary_outputs(group)
            && self.local().results.is_drained()
    }
    fn begin_bytes(&self, intent: &ChildActivationIntent) -> Result<Vec<u8>, ApplicationError> {
        self.local()
            .applications
            .get(&intent.mapping.parent)
            .ok_or(ApplicationError::InvalidCommand)?
            .begin(intent)
    }
    fn propose(&mut self, request: ClientRequest) -> Result<ClientTicket, ClientRejected> {
        self.propose(request)
    }
    fn status(
        &mut self,
        group: GroupIdentity,
        begin: OperationId,
    ) -> Result<ReadInvocationTicket, ReadInvocationError> {
        self.read(group, ActivationQuery::Status(begin))
            .map_err(|e| e.reason)
    }
    fn poll_client(&mut self) -> Option<ClientCompletion<ActivationReceipt<A::Receipt>>> {
        self.poll_client()
    }
    fn complete_client(
        &mut self,
        output: ClientCompletion<ActivationReceipt<A::Receipt>>,
    ) -> ActivationParentClientResult<A::Receipt> {
        self.complete_client(output)
    }
    fn poll_read(&mut self) -> Option<ReadCompletion<ActivationRead<A::ReadResult>>> {
        self.poll_read()
    }
    fn complete_read(
        &mut self,
        output: ReadCompletion<ActivationRead<A::ReadResult>>,
    ) -> ActivationParentReadResult<A::ReadResult> {
        self.complete_read(output)
    }
    fn cancel_client(&mut self, ticket: ClientTicket) -> Result<(), ClientError> {
        self.cancel_client(ticket)
    }
    fn cancel_read(&mut self, ticket: ReadInvocationTicket) -> Result<(), ReadInvocationError> {
        self.cancel_read(ticket)
    }
    fn poll(
        &mut self,
        now: MonoTime,
        budget: LogicalNodePollBudget,
    ) -> Result<(), LogicalNodeError> {
        self.poll(now, budget).map(|_| ())
    }
    fn pump<R>(
        &mut self,
        pump: impl for<'p> FnOnce(
            LogicalNodePump<'p, ActivationReceipt<A::Receipt>, ActivationRead<A::ReadResult>>,
        ) -> R,
    ) -> R {
        self.pump_backing(pump)
    }
}
