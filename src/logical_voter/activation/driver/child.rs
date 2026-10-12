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
//! The same owned parent driver progresses from real child observations.
use super::*;
use crate::logical_voter::bridge::activation::ObservedLogicalConfiguration;
use crate::logical_voter::{
    LogicalActivationAttachmentRejected, LogicalActivationBackingChild,
    LogicalActivationContinuation, LogicalBackingBridge, LogicalBridgeBudget,
};
use crate::membership::ConfigurationRecord;
mod history;
mod progression;
#[derive(Default)]
pub(super) struct ChildProgress {
    pub phase: usize,
    pub waiting_history: bool,
    pub configuration: Option<(usize, ConfigurationTicket)>,
    pub final_position: Option<ActivationPosition>,
    pub learners: Option<ObservedLogicalConfiguration>,
    pub learners_reservation: usize,
    pub cleanup_pending: bool,
}
impl<'p, P: LogicalActivationParent> ChildActivationDriver<'p, P> {
    /// Pump the original parent and exactly the attached child streams. This
    /// never interprets an administrative admission as a committed phase.
    pub fn advance_with_child<N: LogicalActivationBackingChild>(
        &mut self,
        now: MonoTime,
        parent_budget: ChildActivationDriverBudget,
        child_budget: LogicalBridgeBudget<N::PollBudget>,
        bridge: &mut LogicalBackingBridge<'_, N>,
    ) -> Result<(), ChildActivationDriverError> {
        if self.bindings.0.store.identity != self.intent.mapping.store {
            return Err(ChildActivationDriverError::WrongBinding);
        }
        self.advance_parent_child(
            now,
            parent_budget,
            child_budget,
            bridge,
            self.authorization.is_some(),
        )?;
        self.progress_owned_child(bridge, parent_budget.transitions)
    }
    /// Independently drive the target child. Call the existing parent advance
    /// with that parent's own backing ports; its endpoint may be another cell.
    pub fn advance_child<N: LogicalActivationBackingChild>(
        &mut self,
        now: MonoTime,
        budget: LogicalBridgeBudget<N::PollBudget>,
        bridge: &mut LogicalBackingBridge<'_, N>,
    ) -> Result<(), ChildActivationDriverError> {
        if let Some(auth) = &self.authorization {
            bridge.advance_activation(now, budget, auth)
        } else {
            bridge.advance(now, budget)
        }
        .map_err(|e| self.fail(ChildActivationDriverError::Lower(Box::new(e))))?;
        self.progress_owned_child(bridge, budget.transitions)
    }
    fn progress_owned_child<N: LogicalActivationBackingChild>(
        &mut self,
        bridge: &mut LogicalBackingBridge<'_, N>,
        transitions: usize,
    ) -> Result<(), ChildActivationDriverError> {
        if self.stopped {
            self.drain_child(bridge)?;
            return self.failure.clone().map_or(Ok(()), Err);
        }
        if self.failure.is_some() {
            return self.failure.clone().map_or(Ok(()), Err);
        }
        if self.authorization.is_none() {
            let Some(observed) = self.observed.take() else {
                return Ok(());
            };
            let profile = self
                .parent
                .profile(self.intent.mapping.parent)
                .ok_or(ChildActivationDriverError::WrongBinding)?;
            let requirements = match bridge.activation_preflight(&profile) {
                Ok(r) => r,
                Err(LogicalBridgeError::Busy) => {
                    self.observed = Some(observed);
                    return Ok(());
                }
                Err(e) => {
                    self.observed = Some(observed);
                    return Err(self.fail(ChildActivationDriverError::Lower(Box::new(e))));
                }
            };
            let child_bound = bridge.activation_result_bound();
            let extra = LogicalActivationAuthorization::retained_bound(&self.intent, &profile)
                .map_err(ChildActivationDriverError::Application)?;
            if self
                .parent_reservation
                .checked_add(child_bound)
                .and_then(|n| n.checked_add(extra))
                .and_then(|n| n.checked_add(child_bound))
                .is_none_or(|n| n > self.retained_limit)
            {
                self.observed = Some(observed);
                return Err(self.fail(ChildActivationDriverError::Overloaded));
            }
            self.authorization = Some(LogicalActivationAuthorization::mint(
                observed,
                self.bindings,
                profile,
                requirements,
            )?);
            self.child.learners_reservation = child_bound;
        }
        for _ in 0..transitions {
            if self.pending.is_some() {
                break;
            }
            if !self.progress_child(bridge)? {
                break;
            }
        }
        Ok(())
    }
    fn advance_parent_child<N: LogicalActivationBackingChild>(
        &mut self,
        now: MonoTime,
        parent_budget: ChildActivationDriverBudget,
        child_budget: LogicalBridgeBudget<N::PollBudget>,
        bridge: &mut LogicalBackingBridge<'_, N>,
        authorized: bool,
    ) -> Result<(), ChildActivationDriverError> {
        if authorized {
            bridge.advance_activation(
                now,
                child_budget,
                self.authorization
                    .as_ref()
                    .expect("selected original capability"),
            )
        } else {
            bridge.advance(now, child_budget)
        }
        .map_err(|e| self.fail(ChildActivationDriverError::Lower(Box::new(e))))?;
        self.advance(now, parent_budget, |parts| {
            bridge.pump_persistence(parts.persistence)?;
            if let Some(snapshots) = parts.snapshots {
                bridge.pump_snapshots(snapshots, parts.persistence)?;
            }
            bridge.pump_release(parts.releases)?;
            Ok(())
        })
    }
    /// Yield only the already drained original child slice for real enrollment.
    /// The parent borrow and the immutable intent remain with this driver.
    pub fn detach_child<'a, N: LogicalActivationBackingChild>(
        &mut self,
        bridge: LogicalBackingBridge<'a, N>,
    ) -> Result<(LogicalActivationContinuation, &'a mut [N]), Box<LogicalBackingBridge<'a, N>>>
    {
        if self.child.learners.is_none()
            || self.child.configuration.is_some()
            || self.child.waiting_history
        {
            return Err(Box::new(bridge));
        }
        let Some(authorization) = &self.authorization else {
            return Err(Box::new(bridge));
        };
        bridge.detach_activation(authorization)
    }
    /// Reattach the same original session, then reobserve Learners before Joint.
    pub fn attach_enrolled_child<'a, N: LogicalActivationBackingChild>(
        &mut self,
        continuation: LogicalActivationContinuation,
        nodes: &'a mut [N],
    ) -> Result<LogicalBackingBridge<'a, N>, Box<LogicalActivationAttachmentRejected<'a, N>>> {
        let Some(authorization) = &self.authorization else {
            return Err(Box::new(LogicalActivationAttachmentRejected {
                reason: LogicalBridgeError::InvalidAssembly,
                continuation,
                nodes,
            }));
        };
        let bridge = continuation.attach(nodes, authorization)?;
        self.child.phase = 0;
        self.child.learners = None;
        Ok(bridge)
    }
    pub fn stop_waiting_with_child<N: LogicalActivationBackingChild>(
        &mut self,
        bridge: &mut LogicalBackingBridge<'_, N>,
    ) -> Result<(), ChildActivationDriverError> {
        self.stop_waiting()?;
        if let Some((node, ticket)) = self.child.configuration {
            bridge
                .cancel_activation_configuration(node, ticket)
                .map_err(ChildActivationDriverError::Administrative)?;
        }
        bridge
            .stop_activation()
            .map_err(|e| ChildActivationDriverError::Lower(Box::new(e)))
    }
    fn drain_child<N: LogicalActivationBackingChild>(
        &mut self,
        bridge: &mut LogicalBackingBridge<'_, N>,
    ) -> Result<(), ChildActivationDriverError> {
        bridge
            .stop_activation()
            .map_err(|e| ChildActivationDriverError::Lower(Box::new(e)))?;
        if let Some((node, ticket)) = self.child.configuration {
            if let Some(output) = bridge.poll_activation_configuration(node) {
                if output.ticket != ticket {
                    return Err(self.fail(ChildActivationDriverError::ProviderViolation));
                }
                self.child.configuration = None;
            }
        }
        if !bridge.activation_history_pending() {
            self.child.waiting_history = false;
        }
        self.child.cleanup_pending = !bridge.activation_controls_are_drained();
        Ok(())
    }
}
