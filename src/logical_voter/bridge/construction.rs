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
impl<'a, N: LogicalBackingChild> LogicalBackingBridge<'a, N> {
    /// Select checked local child Nodes including the eligible representative.
    /// The host independently drives other child members; subset size is never
    /// quorum evidence. Only consumed actual child outcomes grant authority.
    /// No already-owned client/read stream can be attached to another bridge.
    pub fn new(
        nodes: &'a mut [N],
        mapping: LogicalStoreMapping,
        representative: NodeId,
    ) -> Result<Self, LogicalBridgeError> {
        let representative_node = validate(nodes, mapping, representative)?;
        nodes[representative_node]
            .endpoint_chain(mapping.child)
            .and_then(|chain| chain.preflight(mapping))
            .map_err(LogicalBridgeError::Endpoint)?;
        Ok(Self {
            nodes,
            mapping,
            representative_node,
            initialized: None,
            initialized_at: None,
            activation_verified: false,
            work: None,
            image: None,
            action: None,
            retained: None,
            failure: None,
            node_cursor: 0,
            output_cursor: 0,
            route_cursor: 0,
            now: None,
            closed: false,
            sequence: 0,
            release_ticket: None,
            activation_history: None,
            readiness: None,
            configuration_attempt: None,
            activation_stopping: false,
        })
    }
}
impl<N: LogicalBackingChild> LogicalBackingBridge<'_, N> {
    /// Host claim IDs live in claim space (`< 2^64`); session space belongs
    /// to the bridge's own sequences.
    pub fn begin_claim(&mut self, operation: OperationId) -> Result<(), LogicalBridgeError> {
        self.intake()?;
        if operation.get() >= command::SESSION_SPACE {
            return Err(LogicalBridgeError::InvalidAssembly);
        }
        self.work = Some(BridgeWork::new(
            BridgeStage::BeforeClaim(operation),
            self.nodes[0].local().applications[&self.mapping.child].candidate(),
        ));
        Ok(())
    }
    pub(super) fn intake(&self) -> Result<(), LogicalBridgeError> {
        if let Some(error) = &self.failure {
            return Err(error.clone());
        }
        if self.closed {
            return Err(LogicalBridgeError::Closed);
        }
        if self.work.is_some()
            || self.image.is_some()
            || self.action.is_some()
            || self.activation_history.is_some()
        {
            return Err(LogicalBridgeError::Busy);
        }
        Ok(())
    }
    pub fn take_image(&mut self) -> Option<VerifiedLogicalImage> {
        self.image.take()
    }
    pub fn mapping(&self) -> LogicalStoreMapping {
        self.mapping
    }
    pub fn failure(&self) -> Option<&LogicalBridgeError> {
        self.failure.as_ref()
    }
    pub fn close(&mut self) {
        self.closed = true;
    }
    pub fn usage(&self) -> LogicalBridgeUsage {
        let command_bytes = self.work.as_ref().map_or(0, |w| match &w.stage {
            BridgeStage::Applying { bytes, .. } | BridgeStage::Resolving { bytes, .. } => {
                bytes.capacity()
            }
            _ => 0,
        });
        LogicalBridgeUsage {
            actions: usize::from(self.work.is_some())
                + usize::from(self.action.is_some())
                + usize::from(self.activation_history.is_some()),
            command_bytes,
            child_attempts: usize::from(self.work.as_ref().is_some_and(|w| w.attempt.is_some()))
                + usize::from(self.configuration_attempt.is_some())
                + usize::from(self.readiness.is_some()),
            retained_outputs: usize::from(self.retained.is_some())
                + usize::from(
                    self.configuration_attempt
                        .as_ref()
                        .is_some_and(|a| a.has_output()),
                ),
            image_ready: self.image.is_some(),
            enrollment_bytes: self.enrollment_bytes(),
        }
    }
    /// Whether this bridge's original observations and exclusively consumed
    /// child client/read streams have settled. This permits an owning host to
    /// hand back a borrow; the child may still own independent lower work that
    /// the host must continue pumping and drain before reclaiming that child.
    /// This sampled predicate supplies no quorum or successful delivery proof.
    pub fn observations_are_drained(&self) -> bool {
        self.work.is_none()
            && self.image.is_none()
            && self.action.is_none()
            && self.retained.is_none()
            && self.release_ticket.is_none()
            && self.activation_history.is_none()
            && self.readiness.is_none()
            && self.configuration_attempt.is_none()
            && self
                .nodes
                .iter()
                .all(|n| n.local().clients.is_drained() && n.local().reads.is_drained())
    }
    /// Includes every child's original accepted lower-layer work. Observation
    /// settlement alone does not establish this full work-quiescence boundary.
    pub fn is_drained(&self) -> bool {
        self.observations_are_drained() && self.nodes.iter().all(N::backing_is_drained)
    }
    pub(super) fn route(&self) -> Option<usize> {
        self.nodes
            .iter()
            .enumerate()
            .find(|(_, n)| {
                n.state() == NodeState::Running
                    && n.local()
                        .owner
                        .core(self.mapping.child)
                        .is_some_and(|c| c.role() == Role::Leader)
            })
            .map(|(i, _)| i)
            .or_else(|| {
                (0..self.nodes.len())
                    .map(|i| (i + self.route_cursor) % self.nodes.len())
                    .find(|i| self.nodes[*i].state() == NodeState::Running)
            })
    }
    pub(crate) fn submit_body(
        &mut self,
        body: CommandBody,
    ) -> Result<OperationId, LogicalBridgeError> {
        self.intake()?;
        let representative = self
            .initialized
            .ok_or(LogicalBridgeError::StaleRepresentative)?;
        let sequence = self
            .sequence
            .checked_add(1)
            .ok_or(LogicalBridgeError::Exhausted)?;
        let operation = OperationId::new(command::session_operation(
            representative.logical_session.get(),
            sequence,
        ))
        .ok_or(LogicalBridgeError::Exhausted)?;
        let node = self.route().ok_or(LogicalBridgeError::InvalidAssembly)?;
        let core = self.nodes[node]
            .local()
            .owner
            .core(self.mapping.child)
            .ok_or(LogicalBridgeError::InvalidAssembly)?;
        let command = Command {
            operation,
            representative,
            origin_configuration: core.membership().id(),
            // `intake` proved the previous sequence's outcome was consumed:
            // the child may retire it and everything before it.
            consumed_through: self.sequence,
            body,
        };
        let app = &self.nodes[node].local().applications[&self.mapping.child];
        let bytes = app
            .encode_command(&command, app.limits().max_command_bytes)
            .map_err(LogicalBridgeError::Application)?;
        self.work = Some(BridgeWork::new(
            BridgeStage::Applying {
                command: Box::new(command),
                bytes,
            },
            false,
        ));
        self.sequence = sequence;
        Ok(operation)
    }
    pub(crate) fn take_committed_action(&mut self) -> Option<CommittedLogicalAction> {
        self.action.take()
    }
}
pub(super) fn validate<N: LogicalBackingChild>(
    nodes: &[N],
    mapping: LogicalStoreMapping,
    representative: NodeId,
) -> Result<usize, LogicalBridgeError> {
    if nodes.is_empty() || nodes.len() > 64 {
        return Err(LogicalBridgeError::InvalidAssembly);
    }
    let first = nodes[0]
        .local()
        .applications
        .get(&mapping.child)
        .ok_or(LogicalBridgeError::InvalidAssembly)?;
    let mut representative_node = None;
    for (i, node) in nodes.iter().enumerate() {
        let local = node.local();
        let app = local
            .applications
            .get(&mapping.child)
            .ok_or(LogicalBridgeError::InvalidAssembly)?;
        let core = local
            .owner
            .core(mapping.child)
            .ok_or(LogicalBridgeError::InvalidAssembly)?;
        if node.state() != NodeState::Running
            || local.applications.len() != 1
            || !local.clients.is_drained()
            || !local.reads.is_drained()
            || app.mapping() != mapping
            || !app.same_configuration(first)
            || core.state().bootstrap != *app.child_bootstrap()
            || (core.state().bootstrap.voter_stores.get(&core.local_node())
                != Some(&core.storage_binding().identity)
                && core.membership().replica_store(core.local_node())
                    != Some(core.storage_binding().identity))
            || nodes[..i].iter().any(|old| {
                old.local().owner.identity().store.identity == local.owner.identity().store.identity
            })
            || local.clients.limits().command_bytes < app.limits().max_command_bytes
            || local.clients.limits().receipt_bytes < std::mem::size_of::<LogicalReceipt>()
        {
            return Err(LogicalBridgeError::InvalidAssembly);
        }
        for query in [
            LogicalQuery::Image,
            LogicalQuery::Metadata,
            LogicalQuery::Outcome(OperationId::new(1).unwrap()),
        ] {
            let bound = app
                .read_result_bound(&query)
                .map_err(LogicalBridgeError::Application)?;
            if bound > local.reads.limits().result_bytes {
                return Err(LogicalBridgeError::InvalidAssembly);
            }
        }
        if app.candidate() {
            let bound = app
                .read_result_bound(&LogicalQuery::Enrollment)
                .map_err(LogicalBridgeError::Application)?;
            let reservation = app
                .limits()
                .max_command_bytes
                .checked_mul(2)
                .and_then(|n| {
                    app.limits()
                        .max_parent_checkpoint_bytes
                        .checked_mul(4)
                        .and_then(|r| n.checked_add(r))
                })
                .ok_or(LogicalBridgeError::InvalidAssembly)?;
            if bound > local.reads.limits().result_bytes
                || reservation > app.limits().max_child_checkpoint_bytes
            {
                return Err(LogicalBridgeError::InvalidAssembly);
            }
        }
        if core.local_node() == representative {
            if representative_node.is_some() {
                return Err(LogicalBridgeError::InvalidAssembly);
            }
            representative_node = Some(i);
        }
    }
    representative_node.ok_or(LogicalBridgeError::InvalidAssembly)
}
