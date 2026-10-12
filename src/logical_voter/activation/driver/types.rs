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
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum ChildActivationDriverPhase {
    Observe,
    Propose,
    AwaitBegin,
    Begun,
    Enrolling,
    Busy,
    Conflict,
    Completed,
    Released,
    Stopped,
    Failed,
}
#[derive(Clone, Debug, Eq, PartialEq)]
pub enum ChildActivationDriverError {
    InvalidLimits,
    WrongBinding,
    Overloaded,
    Closed,
    ProviderViolation,
    Application(ApplicationError),
    Client(ClientError),
    Configuration(crate::raft::RaftError),
    Administrative(ConfigurationRequestError),
    Read(ReadInvocationError),
    Snapshot(SnapshotRouteError),
    Node(Box<LogicalNodeError>),
    Lower(Box<LogicalBridgeError>),
}
#[derive(Clone, Copy, Debug)]
pub struct ChildActivationDriverLimits {
    pub command_bytes: usize,
    pub retained_bytes: usize,
}
impl Default for ChildActivationDriverLimits {
    fn default() -> Self {
        Self {
            command_bytes: 64 * 1024,
            retained_bytes: 4 * 1024 * 1024,
        }
    }
}
#[derive(Clone, Copy, Debug)]
pub struct ChildActivationDriverBudget {
    pub node: LogicalNodePollBudget,
    pub completions: usize,
    pub transitions: usize,
}
impl Default for ChildActivationDriverBudget {
    fn default() -> Self {
        Self {
            node: LogicalNodePollBudget::default(),
            completions: 1,
            transitions: 1,
        }
    }
}
pub struct ChildActivationDriverRejected<'a, P: LogicalActivationParent> {
    pub reason: ChildActivationDriverError,
    pub parent: &'a mut P,
    pub intent: Box<ChildActivationIntent>,
}
#[derive(Clone, Copy)]
pub(super) enum Attempt {
    Client(ClientTicket),
    Read(ReadInvocationTicket),
}
pub(super) enum Fault<P: LogicalActivationParent> {
    Client(Box<ClientCompletion<ActivationReceipt<P::Receipt>>>),
    Read(Box<ReadCompletion<ActivationRead<P::Read>>>),
}
/// Only the owned consumer constructs this from a genuine completed fresh read.
/// Copies of public application/status fields cannot construct it.
pub(super) struct ObservedActivationIntent {
    pub intent: ChildActivationIntent,
    pub ticket: ReadInvocationTicket,
    pub barrier: crate::raft::ReadBarrier,
    pub first: ActivationPosition,
}
impl ObservedActivationIntent {
    pub fn matches(
        &self,
        bindings: (RuntimeOwner, ClientRouterBinding, ReadInvocationBinding),
        intent: &ChildActivationIntent,
    ) -> bool {
        self.intent == *intent
            && self.ticket.binding == bindings.2
            && self.ticket.binding.owner == bindings.0
            && self.ticket.group == intent.mapping.parent
            && self.ticket.request == self.barrier.request()
            && self.barrier.group() == intent.mapping.parent
            && self.first.index > 0
            && self.first.term > 0
            && self.first.index <= self.barrier.index()
    }
}
