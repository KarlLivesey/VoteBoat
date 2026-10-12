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
//! Original parent-observation ownership for the child activation lifecycle.
//! Begun is an intermediate phase; actual child membership and terminal phases
//! require the same owned driver and original child protocol observations.
use super::*;
use crate::{
    logical_voter::{
        LogicalBridgeError, LogicalNode, LogicalNodeError, LogicalNodePollBudget, LogicalNodePump,
    },
    runtime::*,
};
mod authorization;
pub use authorization::LogicalActivationAuthorization;
mod child;
mod parent;
pub use parent::{
    ActivationParentClientResult, ActivationParentReadResult, LogicalActivationParent,
};
mod types;
pub use types::*;
mod consume;
mod enrollment;
mod lifecycle;
pub use enrollment::{LogicalEnrollmentExport, LogicalEnrollmentExportRejected};
use types::{Attempt, Fault, ObservedActivationIntent};

pub struct ChildActivationDriver<'a, P: LogicalActivationParent> {
    parent: &'a mut P,
    intent: ChildActivationIntent,
    begin: Vec<u8>,
    bindings: (RuntimeOwner, ClientRouterBinding, ReadInvocationBinding),
    pending: Option<Attempt>,
    fault: Option<Fault<P>>,
    observed: Option<ObservedActivationIntent>,
    first: Option<ActivationPosition>,
    phase: ChildActivationDriverPhase,
    failure: Option<ChildActivationDriverError>,
    stopped: bool,
    authorization: Option<LogicalActivationAuthorization>,
    child: child::ChildProgress,
    status: Option<Box<ChildActivationStatus>>,
    terminal: Option<(OperationId, Vec<u8>)>,
    retained_limit: usize,
    parent_reservation: usize,
}
impl<'a, P: LogicalActivationParent> ChildActivationDriver<'a, P> {
    pub fn new(
        parent: &'a mut P,
        intent: ChildActivationIntent,
        limits: ChildActivationDriverLimits,
    ) -> Result<Self, ChildActivationDriverRejected<'a, P>> {
        let validation = Self::preflight(parent, &intent, limits);
        let begin = match validation {
            Ok(b) => b,
            Err(reason) => {
                return Err(ChildActivationDriverRejected {
                    reason,
                    parent,
                    intent: Box::new(intent),
                });
            }
        };
        let bindings = parent.bindings();
        let parent_reservation =
            Self::reservation(parent, &begin).expect("validated parent reservation");
        Ok(Self {
            parent,
            intent,
            begin,
            bindings,
            pending: None,
            fault: None,
            observed: None,
            first: None,
            phase: ChildActivationDriverPhase::Observe,
            failure: None,
            stopped: false,
            authorization: None,
            child: child::ChildProgress::default(),
            status: None,
            terminal: None,
            retained_limit: limits.retained_bytes,
            parent_reservation,
        })
    }
    fn preflight(
        parent: &P,
        intent: &ChildActivationIntent,
        limits: ChildActivationDriverLimits,
    ) -> Result<Vec<u8>, ChildActivationDriverError> {
        if limits.command_bytes < CONTROL_BYTES
            || limits.command_bytes > 16 * 1024 * 1024
            || limits.retained_bytes > 256 * 1024 * 1024
        {
            return Err(ChildActivationDriverError::InvalidLimits);
        }
        intent
            .validate()
            .map_err(ChildActivationDriverError::Application)?;
        if !parent.is_running() {
            return Err(ChildActivationDriverError::WrongBinding);
        }
        let profile = parent
            .profile(intent.mapping.parent)
            .ok_or(ChildActivationDriverError::WrongBinding)?;
        if profile.parent() != intent.mapping.parent
            || !profile.mappings().contains(&intent.mapping)
        {
            return Err(ChildActivationDriverError::WrongBinding);
        }
        if !parent.observer_stream_is_clear(intent.mapping.parent) {
            return Err(ChildActivationDriverError::WrongBinding);
        }
        let begin = parent
            .begin_bytes(intent)
            .map_err(ChildActivationDriverError::Application)?;
        let bound = Self::reservation(parent, &begin)?;
        if begin.capacity() > limits.command_bytes || bound > limits.retained_bytes {
            return Err(ChildActivationDriverError::Overloaded);
        }
        let (owner, client, read) = parent.bindings();
        if client.owner != owner || read.owner != owner {
            return Err(ChildActivationDriverError::WrongBinding);
        }
        Ok(begin)
    }
    fn reservation(parent: &P, begin: &Vec<u8>) -> Result<usize, ChildActivationDriverError> {
        let (receipt, read) = parent.observer_bounds();
        let output_bound = receipt
            .checked_add(std::mem::size_of::<
                ClientCompletion<ActivationReceipt<P::Receipt>>,
            >())
            .and_then(|client| {
                read.checked_add(std::mem::size_of::<ReadCompletion<ActivationRead<P::Read>>>())
                    .map(|result| client.max(result))
            })
            .ok_or(ChildActivationDriverError::Overloaded)?;
        let bound = begin
            .capacity()
            .checked_mul(2)
            .and_then(|n| n.checked_add(output_bound))
            .and_then(|n| {
                n.checked_add(
                    std::mem::size_of::<Self>()
                        + std::mem::size_of::<ChildActivationStatus>()
                        + std::mem::size_of::<ObservedActivationIntent>(),
                )
            })
            .ok_or(ChildActivationDriverError::Overloaded)?;
        Ok(bound)
    }
    pub fn intent(&self) -> &ChildActivationIntent {
        &self.intent
    }
    pub fn phase(&self) -> ChildActivationDriverPhase {
        self.phase
    }
    pub fn failure(&self) -> Option<&ChildActivationDriverError> {
        self.failure.as_ref()
    }
    pub fn original_begin_position(&self) -> Option<ActivationPosition> {
        self.first
    }
    /// All driver-owned observations/dependencies are settled. Original terminal
    /// ordinary outputs exported before attachment may remain host-owned and
    /// charged; this does not imply the parent Node or its routers are drained.
    pub fn is_drained(&self) -> bool {
        self.pending.is_none()
            && self.fault.is_none()
            && self.child.configuration.is_none()
            && !self.child.waiting_history
            && !self.child.cleanup_pending
            && self
                .parent
                .observer_stream_is_clear(self.intent.mapping.parent)
    }
    pub fn pump<R>(
        &mut self,
        pump: impl for<'p> FnOnce(
            LogicalNodePump<'p, ActivationReceipt<P::Receipt>, ActivationRead<P::Read>>,
        ) -> R,
    ) -> R {
        self.parent.pump(pump)
    }
    fn fail(&mut self, error: ChildActivationDriverError) -> ChildActivationDriverError {
        self.phase = ChildActivationDriverPhase::Failed;
        self.failure = Some(error.clone());
        error
    }
}
