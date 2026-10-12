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
//! Sealed endpoints retaining their original provider and completion contracts.
use super::*;
mod activation;
mod inspection;
mod physical;
pub use activation::LogicalActivationBackingChild;
pub use inspection::{
    LogicalBridgeAttemptInspection, LogicalBridgeChildTicket, LogicalBridgeWorkInspection,
    LogicalBridgeWorkPhase,
};
pub(crate) mod sealed {
    pub trait Sealed {}
}
/// The original local providers and routers remain owned by the backing endpoint.
pub type LogicalBackingLocal<N> = NodeLocalParts<
    <N as LogicalBackingChild>::Scheduler,
    <N as LogicalBackingChild>::Timers,
    <N as LogicalBackingChild>::Entropy,
    LogicalApplication<
        <N as LogicalBackingChild>::Log,
        <N as LogicalBackingChild>::Snapshot,
        <N as LogicalBackingChild>::Wire,
    >,
    <N as LogicalBackingChild>::Worker,
    <N as LogicalBackingChild>::Outbound,
    <N as LogicalBackingChild>::Snapshots,
>;
/// Full typed provider failures are retained; none is converted into child success.
#[derive(Clone, Debug, Eq, PartialEq)]
pub enum LogicalBackingPollError {
    Node(Box<NodeError>),
    Replica(Box<ReplicaError>),
    Network(LogicalNetworkError),
    LogicalNode(Box<LogicalNodeError>),
}
/// A scoped view for host-driven lower progress. It exposes neither mutable
/// backing Nodes nor the original client/read streams consumed by the bridge.
pub struct LogicalBackingPump<'a> {
    pub persistence: &'a mut LogicalPersistencePort,
    pub releases: &'a mut LogicalReleaseCoordinator<LogicalReceipt, LogicalRead>,
    pub snapshots: Option<&'a mut LogicalSnapshotPort>,
}
/// A crate-sealed backing endpoint. Inspection is not quorum evidence; the bridge
/// consumes only the endpoint's original opaque client and read completions.
/// Hosts replace the existing providers, not this safety/ownership adapter.
pub trait LogicalBackingChild: sealed::Sealed + Sized {
    type Scheduler: ReadyScheduler;
    type Timers: TimerService;
    type Entropy: ElectionEntropy;
    type Log: LogCodec + Clone;
    type Snapshot: SnapshotCodec + Clone;
    type Wire: WireCodec + Clone;
    type Worker: PersistenceWorker;
    type Outbound: OutboundQueue;
    type Snapshots: SnapshotWorker;
    type PollBudget: Copy + Default;
    fn local(&self) -> &LogicalBackingLocal<Self>;
    fn state(&self) -> NodeState;
    /// Derive the route only from this endpoint's actual current original owner.
    fn endpoint_chain(
        &self,
        group: GroupIdentity,
    ) -> Result<LogicalEndpointChain, LogicalEndpointError>;
    fn poll(
        &mut self,
        now: MonoTime,
        budget: Self::PollBudget,
    ) -> Result<(), LogicalBackingPollError>;
    /// Admit an original checkpoint control for this endpoint's exact group.
    /// Success does not report publication, compaction or durable completion.
    fn checkpoint(&mut self, group: GroupIdentity) -> Result<(), LogicalBackingPollError>;
    fn propose(&mut self, request: ClientRequest) -> Result<ClientTicket, ClientRejected>;
    fn cancel_client(&mut self, ticket: ClientTicket) -> Result<(), ClientError>;
    fn cancel_read(&mut self, ticket: ReadInvocationTicket) -> Result<(), ReadInvocationError>;
    fn read(
        &mut self,
        group: GroupIdentity,
        query: LogicalQuery,
    ) -> Result<ReadInvocationTicket, Box<ReadInvocationRejected<LogicalQuery>>>;
    fn poll_client(&mut self) -> Option<ClientCompletion<LogicalReceipt>>;
    fn complete_client(
        &mut self,
        output: ClientCompletion<LogicalReceipt>,
    ) -> Result<ClientOutcome<LogicalReceipt>, ClientCompletionRejected<LogicalReceipt>>;
    fn poll_read(&mut self) -> Option<ReadCompletion<LogicalRead>>;
    fn complete_read(
        &mut self,
        output: ReadCompletion<LogicalRead>,
    ) -> Result<ReadOutcome<LogicalRead>, ReadCompletionRejected<LogicalRead>>;
    /// Whether additional accepted lower-layer obligations have settled. Idle
    /// connected sessions are not work. This neither cancels owned work nor
    /// supplies a terminal observation for it.
    fn backing_is_drained(&self) -> bool;
    fn backing_pump(&mut self) -> Option<LogicalBackingPump<'_>>;
}
impl<N: LogicalBackingChild> LogicalBackingBridge<'_, N> {
    /// Admit maintenance on an original exclusively borrowed backing endpoint.
    /// Active parent work does not prevent admission; the host must keep pumping
    /// original dependencies and inspect actual checkpoint completion separately.
    pub fn checkpoint_backing(&mut self, node: usize) -> Result<(), LogicalBridgeError> {
        if let Some(error) = &self.failure {
            return Err(error.clone());
        }
        if self.closed {
            return Err(LogicalBridgeError::Closed);
        }
        let endpoint = self
            .nodes
            .get_mut(node)
            .ok_or(LogicalBridgeError::InvalidAssembly)?;
        endpoint
            .checkpoint(self.mapping.child)
            .map_err(|reason| LogicalBridgeError::Backing {
                node,
                reason: Box::new(reason),
            })
    }
    /// Explicitly pump the selected endpoint's original lower ports. This remains
    /// available after close/failure to settle accepted lower work. The callback
    /// cannot retain a port borrow or access the bridge's child completion streams.
    pub fn pump_backing<R>(
        &mut self,
        node: usize,
        pump: impl for<'p> FnOnce(Option<LogicalBackingPump<'p>>) -> R,
    ) -> Result<R, LogicalBridgeError> {
        let endpoint = self
            .nodes
            .get_mut(node)
            .ok_or(LogicalBridgeError::InvalidAssembly)?;
        Ok(pump(endpoint.backing_pump()))
    }
}
impl<N: LogicalBackingChild> LogicalBackingBridge<'_, N> {
    /// Inspect the original selected providers while their endpoint remains
    /// exclusively borrowed. No mutable owner or opaque output consumer escapes.
    pub fn inspect_backing<R>(
        &self,
        node: usize,
        inspect: impl for<'p> FnOnce(&'p LogicalBackingLocal<N>) -> R,
    ) -> Result<R, LogicalBridgeError> {
        let endpoint = self
            .nodes
            .get(node)
            .ok_or(LogicalBridgeError::InvalidAssembly)?;
        Ok(inspect(endpoint.local()))
    }
}
