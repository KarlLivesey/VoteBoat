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
//! A consumed existing session, never a new durable or quorum assertion.
use super::*;
pub struct LogicalActivationContinuation {
    mapping: LogicalStoreMapping,
    representative: RepresentativeBinding,
    sequence: u64,
    initialized_at: ReadBarrier,
    now: Option<MonoTime>,
    profile: LogicalActivationControlProfile,
    owners: Vec<OriginalChild>,
    application: Vec<u8>,
}
#[derive(Clone, Copy, Eq, PartialEq)]
struct OriginalChild {
    owner: RuntimeOwner,
    clients: ClientRouterBinding,
    reads: ReadInvocationBinding,
    results: ApplicationRouterBinding,
    worker: crate::worker::WorkerBinding,
    outbound: crate::outbound::OutboundBinding,
}
impl OriginalChild {
    fn capture<N: LogicalBackingChild>(node: &N) -> Self {
        let local = node.local();
        Self {
            owner: local.owner.identity(),
            clients: local.clients.binding(),
            reads: local.reads.binding(),
            results: local.results.binding(),
            worker: local.persistence.binding(),
            outbound: local.outbound.binding(),
        }
    }
}
impl<'a, N: LogicalActivationBackingChild> LogicalBackingBridge<'a, N> {
    pub(crate) fn detach_activation(
        self,
        authorization: &LogicalActivationAuthorization,
    ) -> Result<(LogicalActivationContinuation, &'a mut [N]), Box<Self>> {
        if !self.observations_are_drained()
            || self.closed
            || self.failure.is_some()
            || self.mapping != authorization.intent().mapping()
            || self
                .nodes
                .iter()
                .any(|n| !n.configuration_requests_are_drained())
        {
            return Err(Box::new(self));
        }
        let Some(representative) = self.initialized else {
            return Err(Box::new(self));
        };
        let Some(initialized_at) = self.initialized_at else {
            return Err(Box::new(self));
        };
        let Ok(application) =
            self.nodes[0].local().applications[&self.mapping.child].configuration_bytes()
        else {
            return Err(Box::new(self));
        };
        let continuation = LogicalActivationContinuation {
            mapping: self.mapping,
            representative,
            sequence: self.sequence,
            initialized_at,
            now: self.now,
            profile: authorization.profile().clone(),
            owners: self.nodes.iter().map(OriginalChild::capture).collect(),
            application,
        };
        Ok((continuation, self.nodes))
    }
}
impl LogicalActivationContinuation {
    pub(crate) fn attach<'a, N: LogicalActivationBackingChild>(
        self,
        nodes: &'a mut [N],
        authorization: &LogicalActivationAuthorization,
    ) -> Result<LogicalBackingBridge<'a, N>, Box<LogicalActivationAttachmentRejected<'a, N>>> {
        let validation = super::super::construction::validate(
            nodes,
            self.mapping,
            self.representative.physical.node,
        );
        let representative_node = match validation {
            Ok(i)
                if self.mapping == authorization.intent().mapping()
                    && self.profile == *authorization.profile()
                    && self.original_children(nodes, authorization)
                    && nodes[i].local().owner.identity().store
                        == StoreBinding {
                            identity: self.representative.physical.store,
                            session: self.representative.physical_session,
                        }
                    && nodes[i]
                        .local()
                        .owner
                        .core(self.mapping.child)
                        .is_some_and(|c| {
                            c.membership().voter_store(c.local_node())
                                == Some(self.representative.physical.store)
                        })
                    && nodes.iter().all(|n| {
                        n.configuration_requests_are_drained()
                            && n.local().applications[&self.mapping.child]
                                .activation_control_profile()
                                == Some(&self.profile)
                    }) =>
            {
                i
            }
            _ => {
                return Err(Box::new(LogicalActivationAttachmentRejected {
                    reason: LogicalBridgeError::StaleRepresentative,
                    continuation: self,
                    nodes,
                }))
            }
        };
        Ok(LogicalBackingBridge {
            nodes,
            mapping: self.mapping,
            representative_node,
            initialized: Some(self.representative),
            initialized_at: Some(self.initialized_at),
            activation_verified: false,
            sequence: self.sequence,
            now: self.now,
            work: None,
            image: None,
            action: None,
            retained: None,
            failure: None,
            node_cursor: 0,
            output_cursor: 0,
            route_cursor: 0,
            closed: false,
            release_ticket: None,
            activation_history: None,
            readiness: None,
            configuration_attempt: None,
            activation_stopping: false,
        })
    }
}
impl LogicalActivationContinuation {
    fn original_children<N: LogicalBackingChild>(
        &self,
        nodes: &[N],
        authorization: &LogicalActivationAuthorization,
    ) -> bool {
        let current: Vec<_> = nodes.iter().map(OriginalChild::capture).collect();
        self.owners
            .iter()
            .all(|original| current.contains(original))
            && nodes.iter().all(|node| {
                let binding = OriginalChild::capture(node);
                let core = node.local().owner.core(self.mapping.child);
                (self.owners.contains(&binding)
                    || core.is_some_and(|c| {
                        c.local_node() == authorization.learner().node
                            && c.storage_binding().identity == authorization.learner().store
                            && c.membership().replica_store(c.local_node())
                                == Some(authorization.learner().store)
                    }))
                    && node.local().applications[&self.mapping.child]
                        .configuration_bytes()
                        .is_ok_and(|bytes| bytes == self.application)
            })
    }
}
pub struct LogicalActivationAttachmentRejected<'a, N: LogicalActivationBackingChild> {
    pub reason: LogicalBridgeError,
    pub continuation: LogicalActivationContinuation,
    pub nodes: &'a mut [N],
}
