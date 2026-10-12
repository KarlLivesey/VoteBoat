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
use super::frame::{LogicalReceived, LOGICAL_FRAME_HEADER};
use super::*;
pub struct LogicalSessionRejected<S> {
    pub reason: LogicalNetworkError,
    pub session: S,
}
pub struct LogicalSendRejected {
    pub reason: LogicalNetworkError,
    pub batch: Box<LogicalNetworkBatch>,
}
pub(super) struct Sending {
    pub batch: LogicalNetworkBatch,
    pub frame: Vec<u8>,
    pub written: usize,
}
pub(crate) struct LogicalSessionNodeBinding {
    pub group: GroupIdentity,
    pub local: NodeId,
    pub peer: crate::secure::PeerIdentity,
    pub authorization: RuntimeAuthorization,
    pub outbound: OutboundBinding,
    pub logical: StoreBinding,
    pub wire_version: u16,
}
/// Caller-polled stream owner with one proof-bearing send and one complete receive slot.
pub struct LogicalPeerSession<
    T: SecureSession,
    C = NativeLogCodec,
    S = NativeSnapshotCodec,
    W = NativeWireCodec,
> {
    pub(super) session: Option<T>,
    pub(super) codec: LogicalFrameCodec<C, S, W>,
    pub(super) binding: SessionBinding,
    pub(super) authorization: RuntimeAuthorization,
    pub(super) outbound: OutboundBinding,
    pub(super) state: TransportState,
    pub(super) failure: Option<LogicalNetworkError>,
    pub(super) sending: Option<Sending>,
    pub(super) completion: Option<LogicalNetworkCompletion>,
    pub(super) incoming: Vec<u8>,
    pub(super) target: usize,
    pub(super) received: Option<LogicalReceived>,
    pub(super) last_time: Option<MonoTime>,
    pub(super) close_started: bool,
    pub(super) prefer_read: bool,
}
impl<T: SecureSession, C: LogCodec + Clone, S: SnapshotCodec + Clone, W: WireCodec + Clone>
    LogicalPeerSession<T, C, S, W>
{
    pub(crate) fn compatible_limits(&self, limits: LogicalNetworkLimits) -> bool {
        self.codec.limits.frame_bytes >= limits.frame_bytes
            && self.codec.limits.decoded_bytes >= limits.decoded_bytes
    }
    pub(crate) fn expanded_catalog(
        &self,
        target: &Self,
    ) -> Result<LogicalPeerPlan<C, S, W>, LogicalNetworkError> {
        let descriptor = target
            .codec
            .plan
            .peer(target.binding.peer)
            .ok_or(LogicalNetworkError::WrongPeer)?;
        let mut peers = self.codec.plan.peers.clone();
        if peers
            .iter()
            .any(|p| p.mapping.voter == descriptor.mapping.voter)
        {
            return Err(LogicalNetworkError::WrongPeer);
        }
        peers.push(descriptor.clone());
        let plan = LogicalPeerPlan::new(self.codec.plan.local.clone(), peers)?;
        // Validate before changing any original live session or queued frame.
        LogicalFrameCodec::new(plan.clone(), self.codec.limits)?;
        Ok(plan)
    }
    pub(crate) fn install_catalog(&mut self, plan: LogicalPeerPlan<C, S, W>) {
        // Existing sends/frames, TLS session and representative fences stay owned.
        self.codec.plan = plan;
    }
    pub(crate) fn contracted_catalog(
        &self,
        peer: NodeId,
    ) -> Result<LogicalPeerPlan<C, S, W>, LogicalNetworkError> {
        let peers = self
            .codec
            .plan
            .peers
            .iter()
            .filter(|p| p.mapping.voter != peer)
            .cloned()
            .collect();
        let plan = LogicalPeerPlan::new(self.codec.plan.local.clone(), peers)?;
        LogicalFrameCodec::new(plan.clone(), self.codec.limits)?;
        Ok(plan)
    }
    pub fn new(
        session: T,
        plan: LogicalPeerPlan<C, S, W>,
        authorization: RuntimeAuthorization,
        outbound: OutboundBinding,
        limits: LogicalNetworkLimits,
    ) -> Result<Self, LogicalSessionRejected<T>> {
        let checked = (|| {
            let binding = require_authenticated(&session).map_err(LogicalNetworkError::Session)?;
            if binding.wire_version != LOGICAL_NETWORK_VERSION
                || binding.local != plan.local.physical()
                || plan.peer(binding.peer).is_none()
                || !plan.authorization(authorization)
                || outbound.node != plan.local.mapping.voter
                || outbound.store != authorization.owner().store
            {
                return Err(LogicalNetworkError::WrongBinding);
            }
            Ok(binding)
        })();
        let binding = match checked {
            Ok(b) => b,
            Err(reason) => return Err(LogicalSessionRejected { reason, session }),
        };
        let codec = match LogicalFrameCodec::new(plan, limits) {
            Ok(c) => c,
            Err(reason) => return Err(LogicalSessionRejected { reason, session }),
        };
        Ok(Self {
            session: Some(session),
            codec,
            binding,
            authorization,
            outbound,
            state: TransportState::Open,
            failure: None,
            sending: None,
            completion: None,
            incoming: Vec::with_capacity(LOGICAL_FRAME_HEADER),
            target: LOGICAL_FRAME_HEADER,
            received: None,
            last_time: None,
            close_started: false,
            prefer_read: true,
        })
    }
    pub fn binding(&self) -> SessionBinding {
        self.binding
    }
    pub(crate) fn node_binding(&self) -> Option<LogicalSessionNodeBinding> {
        let peer = self.codec.plan.peer(self.binding.peer)?;
        Some(LogicalSessionNodeBinding {
            group: self.codec.plan.local.mapping.parent,
            local: self.codec.plan.local.mapping.voter,
            peer: crate::secure::PeerIdentity {
                node: peer.mapping.voter,
                store: peer.mapping.store,
            },
            authorization: self.authorization,
            outbound: self.outbound,
            logical: peer.logical(),
            wire_version: peer.application.wire_codec.format_version(),
        })
    }
    /// Idle healthy sessions own no accepted message work. Partial input and
    /// unconsumed original outputs still prevent work quiescence.
    pub fn work_is_drained(&self) -> bool {
        self.sends_are_drained() && self.received.is_none() && self.incoming.is_empty()
    }
    pub(crate) fn sends_are_drained(&self) -> bool {
        self.sending.is_none() && self.completion.is_none()
    }
    pub fn state(&self) -> TransportState {
        self.state
    }
    pub fn limits(&self) -> LogicalNetworkLimits {
        self.codec.limits
    }
    pub fn submit(&mut self, batch: LogicalNetworkBatch) -> Result<(), LogicalSendRejected> {
        let result = (|| {
            if self.state != TransportState::Open {
                return Err(LogicalNetworkError::Closed);
            }
            if self.sending.is_some() || self.completion.is_some() {
                return Err(LogicalNetworkError::Outbound(OutboundError::Overloaded));
            }
            let peer = self
                .codec
                .plan
                .peer(self.binding.peer)
                .ok_or(LogicalNetworkError::WrongPeer)?;
            if batch.ticket.binding != self.outbound
                || batch.ticket.peer != peer.mapping.voter
                || batch.send.authorization() != self.authorization
                || batch.send.message().to != peer.mapping.voter
            {
                return Err(LogicalNetworkError::WrongBinding);
            }
            self.codec.encode(&batch.send)
        })();
        let frame = match result {
            Ok(f) => f,
            Err(reason) => {
                return Err(LogicalSendRejected {
                    reason,
                    batch: Box::new(batch),
                })
            }
        };
        self.sending = Some(Sending {
            batch,
            frame,
            written: 0,
        });
        Ok(())
    }
    pub fn take_send(&mut self) -> Option<LogicalNetworkCompletion> {
        self.completion.take()
    }
    pub fn has_received(&self) -> bool {
        self.received.is_some()
    }
    pub fn take_received(
        &mut self,
        current: RuntimeAuthorization,
    ) -> Result<Option<LogicalReceived>, LogicalNetworkError> {
        if current != self.authorization {
            return Err(LogicalNetworkError::StaleOwner);
        }
        Ok(self.received.take())
    }
    pub fn is_drained(&self) -> bool {
        matches!(self.state, TransportState::Closed | TransportState::Failed)
            && self.sending.is_none()
            && self.completion.is_none()
            && self.received.is_none()
    }
    pub fn close(&mut self) {
        if self.state == TransportState::Open {
            self.state = TransportState::Draining;
        }
    }
    pub fn abort(&mut self) {
        self.fail(LogicalNetworkError::Closed);
    }
    pub(super) fn fail(&mut self, error: LogicalNetworkError) {
        if self.state == TransportState::Failed {
            return;
        }
        if let Some(mut session) = self.session.take() {
            session.revoke();
        }
        self.incoming = Vec::new();
        if let Some(s) = self.sending.take() {
            drop(s.frame);
            self.completion = Some(LogicalNetworkCompletion {
                batch: s.batch,
                result: LocalSendResult::Failed,
            });
        }
        self.state = TransportState::Failed;
        self.failure = Some(error);
    }
}
