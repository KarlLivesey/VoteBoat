// SPDX-License-Identifier: RPL-1.5
// Copyright (c) 2026 Karl Livesey
//! Caller-polled real adjacent channel, with original frame ownership on refusal.
use super::*;
use frame::{FrameBody, FRAME_HEADER};
mod poll;
mod validation;

enum HopPlan {
    Voter(NativeRelayPlan),
    Helper(RelayHelperPlan),
}
pub struct RelayHopRejected<S> {
    pub reason: RelayNetworkError,
    pub session: S,
}
pub struct RelayFrameRejected {
    pub reason: RelayNetworkError,
    pub frame: Box<RelayFrame>,
}
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub struct RelayHopTicket {
    pub connection: SessionBinding,
    pub sequence: u64,
}
pub struct RelayHopCompletion {
    pub ticket: RelayHopTicket,
    pub frame: RelayFrame,
    pub result: LocalSendResult,
}
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub struct RelayReceiveTicket {
    pub connection: SessionBinding,
    pub sequence: u64,
}
pub struct RelayReceived {
    pub(super) frame: RelayFrame,
    source: Option<AuthenticatedSourceView>,
    ticket: RelayReceiveTicket,
}
impl RelayReceived {
    pub fn frame(&self) -> &RelayFrame {
        &self.frame
    }
    pub fn source_view(&self) -> Option<AuthenticatedSourceView> {
        self.source
    }
    pub fn connection(&self) -> SessionBinding {
        self.ticket.connection
    }
    pub fn ticket(&self) -> RelayReceiveTicket {
        self.ticket
    }
}
struct Sending {
    ticket: RelayHopTicket,
    original: RelayFrame,
    bytes: Vec<u8>,
    written: usize,
}
/// Authenticates one actual adjacent hop. A helper is never represented as a leaf.
pub struct RelayHop<S: SecureSession> {
    session: Option<S>,
    binding: SessionBinding,
    plan: HopPlan,
    limits: RelayNetworkLimits,
    state: TransportState,
    sending: Option<Sending>,
    completion: Option<RelayHopCompletion>,
    incoming: Vec<u8>,
    target: usize,
    received: Option<RelayReceived>,
    sequence: u64,
    receive_sequence: u64,
    last_time: Option<MonoTime>,
    close_started: bool,
    prefer_read: bool,
    deadline: Option<MonoTime>,
}
impl<S: SecureSession> RelayHop<S> {
    pub fn for_voter(
        session: S,
        plan: NativeRelayPlan,
        limits: RelayNetworkLimits,
    ) -> Result<Self, RelayHopRejected<S>> {
        Self::new(session, HopPlan::Voter(plan), limits)
    }
    pub fn for_helper(
        session: S,
        plan: RelayHelperPlan,
        limits: RelayNetworkLimits,
    ) -> Result<Self, RelayHopRejected<S>> {
        Self::new(session, HopPlan::Helper(plan), limits)
    }
    fn new(
        mut session: S,
        plan: HopPlan,
        limits: RelayNetworkLimits,
    ) -> Result<Self, RelayHopRejected<S>> {
        let checked = (|| {
            let limits = limits.validate()?;
            let depth = match &plan {
                HopPlan::Voter(p) => p.tree.max_hops(),
                HopPlan::Helper(p) => p.tree.max_hops(),
            };
            if !frame::controls_fit(depth, limits) {
                return Err(RelayNetworkError::InvalidLimits);
            }
            let binding = require_authenticated(&session).map_err(RelayNetworkError::Session)?;
            if binding.wire_version != RELAY_NETWORK_VERSION {
                return Err(RelayNetworkError::WrongProfile);
            }
            let valid = match &plan {
                HopPlan::Voter(p) => {
                    binding.local == p.local()
                        && binding.peer.node == p.helper().node
                        && binding.peer.store.identity == p.helper().store
                        && p.sources().count() <= limits.source_slots
                }
                HopPlan::Helper(p) => {
                    binding.local == p.local()
                        && p.adjacent(binding.peer)
                        && p.sources().count() <= limits.source_slots
                }
            };
            if !valid {
                return Err(RelayNetworkError::WrongBinding);
            }
            Ok((binding, limits))
        })();
        let (binding, limits) = match checked {
            Ok(v) => v,
            Err(reason) => return Err(RelayHopRejected { reason, session }),
        };
        let deadline = session.drain_status().next_deadline;
        Ok(Self {
            session: Some(session),
            binding,
            plan,
            limits,
            state: TransportState::Open,
            sending: None,
            completion: None,
            incoming: Vec::with_capacity(FRAME_HEADER),
            target: FRAME_HEADER,
            received: None,
            sequence: 0,
            receive_sequence: 0,
            last_time: None,
            close_started: false,
            prefer_read: true,
            deadline,
        })
    }
    fn validate_capture(&self) -> Result<(), RelayNetworkError> {
        let session = self.session.as_ref().ok_or(RelayNetworkError::Closed)?;
        if session.security() != SessionSecurity::Authenticated
            || session.binding() != Some(self.binding)
        {
            return Err(RelayNetworkError::WrongBinding);
        }
        Ok(())
    }
    pub fn binding(&self) -> SessionBinding {
        self.binding
    }
    pub fn authenticated_binding(&self) -> Option<SessionBinding> {
        (self.state == TransportState::Open
            && self
                .session
                .as_ref()
                .and_then(|s| require_authenticated(s).ok())
                == Some(self.binding))
        .then_some(self.binding)
    }
    pub fn session_deadline(&self) -> Option<MonoTime> {
        self.deadline
    }
    pub fn state(&self) -> TransportState {
        self.state
    }
    pub fn captured_source(&self) -> Option<RelaySourceCapture> {
        let HopPlan::Helper(p) = &self.plan else {
            return None;
        };
        (self.state == TransportState::Open
            && p.source(self.binding.peer.node).is_some()
            && self
                .session
                .as_ref()
                .and_then(|s| require_authenticated(s).ok())
                == Some(self.binding))
        .then_some(RelaySourceCapture {
            binding: self.binding,
            original_wire_version: p.original_wire_version(),
            path: AuthenticatedDelegationPath::leaf(self.binding),
        })
    }
    pub fn source_opened(&self) -> Result<RelayFrame, RelayNetworkError> {
        let source = self
            .captured_source()
            .ok_or(RelayNetworkError::WrongBinding)?;
        Ok(RelayFrame {
            body: FrameBody::Opened(Box::new(source)),
        })
    }
    pub fn submit(&mut self, original: RelayFrame) -> Result<RelayHopTicket, RelayFrameRejected> {
        let checked = (|| {
            if self.state != TransportState::Open {
                return Err(RelayNetworkError::Closed);
            }
            if self.sending.is_some() || self.completion.is_some() {
                return Err(RelayNetworkError::Overloaded);
            }
            self.validate_capture()?;
            if self.session.as_ref().unwrap().state() != SessionState::Ready {
                return Err(RelayNetworkError::Closed);
            }
            self.validate_frame(&original, false)?;
            let sequence = self
                .sequence
                .checked_add(1)
                .ok_or(RelayNetworkError::Exhausted)?;
            let bytes = frame::encode(&original, self.limits.frame_bytes)?;
            Ok((
                RelayHopTicket {
                    connection: self.binding,
                    sequence,
                },
                bytes,
            ))
        })();
        let (ticket, bytes) = match checked {
            Ok(v) => v,
            Err(reason) => {
                return Err(RelayFrameRejected {
                    reason,
                    frame: Box::new(original),
                })
            }
        };
        self.sequence = ticket.sequence;
        self.sending = Some(Sending {
            ticket,
            original,
            bytes,
            written: 0,
        });
        Ok(ticket)
    }
    pub fn take_send(&mut self) -> Option<RelayHopCompletion> {
        self.completion.take()
    }
    pub fn take_received(&mut self) -> Option<RelayReceived> {
        self.received.take()
    }
    pub fn received(&self) -> Option<&RelayReceived> {
        self.received.as_ref()
    }
    pub fn can_submit(&self) -> bool {
        self.authenticated_binding().is_some()
            && self.sending.is_none()
            && self.completion.is_none()
    }
    pub(super) fn helper_matches(
        &self,
        plan: &RelayHelperPlan,
        limits: RelayNetworkLimits,
    ) -> bool {
        matches!(&self.plan, HopPlan::Helper(p) if p.local() == plan.local()
            && p.tree == plan.tree)
            && self.limits == limits
    }
    pub(super) fn voter_matches(&self, plan: &NativeRelayPlan, limits: RelayNetworkLimits) -> bool {
        matches!(&self.plan, HopPlan::Voter(p) if p.local() == plan.local() && p.helper() == plan.helper()
            && p.tree == plan.tree)
            && self.limits == limits
    }
    pub fn work_is_drained(&self) -> bool {
        self.sending.is_none()
            && self.completion.is_none()
            && self.received.is_none()
            && self.incoming.is_empty()
    }
    pub fn is_drained(&self) -> bool {
        matches!(self.state, TransportState::Closed | TransportState::Failed)
            && self.work_is_drained()
    }
    pub fn next_deadline(&mut self) -> Option<MonoTime> {
        self.deadline = self
            .session
            .as_mut()
            .and_then(|s| s.drain_status().next_deadline);
        self.deadline
    }
    pub fn close(&mut self) {
        if self.state == TransportState::Open {
            self.state = TransportState::Draining;
        }
    }
    pub fn abort(&mut self) {
        self.fail();
    }
    fn fail(&mut self) {
        if let Some(mut s) = self.session.take() {
            s.revoke();
        }
        if let Some(s) = self.sending.take() {
            self.completion = Some(RelayHopCompletion {
                ticket: s.ticket,
                frame: s.original,
                result: LocalSendResult::Failed,
            });
        }
        self.incoming = Vec::new();
        self.state = TransportState::Failed;
        self.deadline = None;
    }
    pub fn into_session(self) -> Result<Option<S>, Box<Self>> {
        if self.is_drained() {
            Ok(self.session)
        } else {
            Err(Box::new(self))
        }
    }
}
impl<S: SecureSession> std::fmt::Debug for RelayHop<S> {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("RelayHop")
            .field("binding", &self.binding)
            .field("state", &self.state)
            .field("session", &self.session.as_ref().map(|s| s.state()))
            .field(
                "sending",
                &self
                    .sending
                    .as_ref()
                    .map(|s| (s.ticket, s.written, s.bytes.len())),
            )
            .field(
                "completion",
                &self.completion.as_ref().map(|c| (c.ticket, c.result)),
            )
            .field("receive", &self.received.as_ref().map(|r| r.ticket))
            .field("partial", &(self.incoming.len(), self.target))
            .field("close_started", &self.close_started)
            .field("deadline", &self.deadline)
            .finish()
    }
}
