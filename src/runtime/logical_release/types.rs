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
//! Bounded request ownership and original child-protocol release authority.
use super::*;

pub const LOGICAL_RESPONSE_CODEC_CONTRACT_VERSION: u32 = 1;
/// Optional for physical apps; mandatory for logical mode. Encoders are faithful,
/// deterministic providers and write only initialized bytes into the given bound.
pub trait LogicalResponseCodec: BoundedStateMachine + BoundedReadableStateMachine
where
    Self::Receipt: ApplicationReceipt,
{
    fn logical_response_schema(&self) -> u64;
    /// Configuration of activation envelopes consumed by this application.
    /// Ordinary applications keep the default; this grants no protocol authority.
    fn logical_activation_control_profile(&self) -> Option<LogicalActivationControlProfile> {
        None
    }
    fn receipt_encoding_bound(&self, entries: &[LogEntry]) -> Result<usize, ApplicationError>;
    fn query_encoding_bound(&self, query: &Self::Query) -> Result<usize, ApplicationError>;
    fn read_encoding_bound(&self, query: &Self::Query) -> Result<usize, ApplicationError>;
    fn encode_receipt(
        &self,
        receipt: &Self::Receipt,
        out: &mut [u8],
    ) -> Result<usize, ApplicationError>;
    fn encode_query(&self, query: &Self::Query, out: &mut [u8]) -> Result<usize, ApplicationError>;
    fn encode_read(
        &self,
        value: &Self::ReadResult,
        out: &mut [u8],
    ) -> Result<usize, ApplicationError>;
}
#[derive(Clone, Copy, Debug)]
pub struct LogicalReleaseLimits {
    pub pending: usize,
    pub bytes: usize,
    pub capsule_bytes: usize,
    /// Reserved captures. Selected activation construction requires at least
    /// two windows, each able to retain a whole declared application batch.
    pub control_pending: usize,
    /// Complete retained capsule/request/pending bytes for every reserved
    /// window; ordinary work cannot spend this capacity.
    pub control_bytes: usize,
}
impl Default for LogicalReleaseLimits {
    fn default() -> Self {
        Self {
            pending: 256,
            bytes: 64 * 1024 * 1024,
            capsule_bytes: 128 * 1024,
            control_pending: 32,
            control_bytes: 8 * 1024 * 1024,
        }
    }
}
impl LogicalReleaseLimits {
    /// Retained footprint of a complete capture with the selected response types.
    /// `count` comes from the supported effect or application batch. This pure
    /// calculation grants no authority and does not guarantee admission.
    pub fn capture_reservation_bytes<R: ApplicationReceipt, V>(
        &self,
        count: usize,
    ) -> Result<usize, LogicalReleaseError> {
        count
            .checked_mul(self.capsule_bytes)
            .and_then(|n| n.checked_mul(4))
            .and_then(|n| {
                n.checked_add(count.checked_mul(
                    std::mem::size_of::<ExpectedCapsule>()
                        + std::mem::size_of::<LogicalReleaseRequest>(),
                )?)
            })
            .and_then(|n| n.checked_add(std::mem::size_of::<Pending<R, V>>()))
            .ok_or(LogicalReleaseError::Overloaded)
    }
    pub(super) fn validate(self) -> Result<(), LogicalReleaseError> {
        if self.pending < 2
            || self.pending > 65536
            || self.bytes == 0
            || self.bytes > 1024 * 1024 * 1024
            || self.capsule_bytes == 0
            || self.capsule_bytes > 16 * 1024 * 1024
            || self.control_pending == 0
            || self.control_pending >= self.pending
            || self.control_bytes == 0
            || self.control_bytes >= self.bytes
        {
            return Err(LogicalReleaseError::InvalidLimits);
        }
        Ok(())
    }
}
#[derive(Clone, Copy, Debug, Default, Eq, PartialEq)]
pub struct LogicalReleaseUsage {
    pub pending: usize,
    pub bytes: usize,
    pub capsules: usize,
}
#[derive(Clone, Debug, Eq, PartialEq)]
pub enum LogicalReleaseError {
    InvalidLimits,
    WrongBinding,
    Overloaded,
    StalePermit,
    ProviderViolation,
    Exhausted,
    Application(ApplicationError),
    Owner(EffectOwnerError),
    ApplicationRoute(ApplicationRouteError),
    Client(ClientError),
    Read(ReadInvocationError),
    Outbound(OutboundError),
}
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub(crate) struct LogicalReleaseTicket {
    pub(crate) authorization: RuntimeAuthorization,
    pub(crate) sequence: u64,
    pub(crate) capsule: usize,
}
pub(crate) struct LogicalReleaseRequest {
    pub(crate) ticket: LogicalReleaseTicket,
    pub(crate) authorization: RuntimeAuthorization,
    pub(crate) capsule: LogicalCapsule,
}
pub(super) struct ReleasePermit {
    pub observation: crate::logical_voter::CommittedLogicalAction,
}
impl ReleasePermit {
    pub fn capsule(&self) -> &LogicalCapsule {
        self.observation
            .capsule()
            .expect("validated capsule observation")
    }
    pub(super) fn from_committed(
        authorization: RuntimeAuthorization,
        observation: crate::logical_voter::CommittedLogicalAction,
    ) -> Result<Self, LogicalCommittedRejected> {
        let Some(logical) = authorization.logical() else {
            return Err(LogicalCommittedRejected {
                reason: LogicalReleaseError::WrongBinding,
                observation: Box::new(observation),
            });
        };
        if observation.mapping() != logical.mapping
            || observation.representative() != logical.representative
            || observation.receipt().outcome != LogicalOutcome::CapsuleAuthorized
            || observation.capsule().is_none()
        {
            return Err(LogicalCommittedRejected {
                reason: LogicalReleaseError::StalePermit,
                observation: Box::new(observation),
            });
        }
        Ok(Self { observation })
    }
}
/// Authorized original payload; its provenance is minted only by actual child
/// output consumption. Replacement queues may inspect it, but cannot fabricate it.
pub struct AuthorizedLogicalSend {
    pub(super) effect: EffectTicket,
    pub(super) authorization: RuntimeAuthorization,
    pub(super) message: Message,
    pub(super) provenance: LogicalSendProvenance,
}
impl AuthorizedLogicalSend {
    pub fn effect(&self) -> EffectTicket {
        self.effect
    }
    pub fn authorization(&self) -> RuntimeAuthorization {
        self.authorization
    }
    pub fn message(&self) -> &Message {
        &self.message
    }
    pub fn provenance(&self) -> &LogicalSendProvenance {
        &self.provenance
    }
    pub fn into_parts(self) -> (Message, LogicalSendProvenance) {
        (self.message, self.provenance)
    }
}
pub struct LogicalSendProvenance {
    pub(super) permit: ReleasePermit,
}
impl LogicalSendProvenance {
    /// Actual retained encoded-command capacity and typed original command payload.
    /// Inspection transfers no ownership and creates no authorization.
    pub fn retained_bytes(&self, limit: usize) -> Result<usize, OutboundError> {
        let CommandBody::Authorize(capsule) = self.permit.observation.command_body() else {
            return Err(OutboundError::WrongBinding);
        };
        let LogicalCapsuleBody::Protocol(message) = &capsule.body else {
            return Err(OutboundError::WrongBinding);
        };
        let (_, message_bytes) = crate::outbound::message_cost(message, limit)?;
        std::mem::size_of::<Self>()
            .checked_add(std::mem::size_of::<Command>())
            .and_then(|n| n.checked_add(self.permit.observation.bytes_capacity()))
            .and_then(|n| n.checked_add(message_bytes - std::mem::size_of::<Message>()))
            .filter(|n| *n <= limit)
            .ok_or(OutboundError::BatchTooLarge)
    }
    pub fn capsule(&self) -> &LogicalCapsule {
        self.permit.capsule()
    }
    pub fn mapping(&self) -> LogicalStoreMapping {
        self.permit.observation.mapping()
    }
    pub fn representative(&self) -> RepresentativeBinding {
        self.permit.observation.representative()
    }
    pub fn receipt(&self) -> LogicalReceipt {
        self.permit.observation.receipt()
    }
    pub fn child_owner(&self) -> RuntimeOwner {
        self.permit.observation.child_owner()
    }
    pub fn command_bytes(&self) -> &[u8] {
        self.permit.observation.bytes()
    }
}
pub struct LogicalOutboundRejected {
    pub reason: OutboundError,
    pub send: Box<AuthorizedLogicalSend>,
}
/// Extends the same bounded queue lifecycle/limits. Logical messages never use
/// raw submit: this entry accepts only the opaque original authorized send.
pub trait LogicalOutboundQueue: OutboundQueue {
    fn submit_authorized(
        &mut self,
        send: AuthorizedLogicalSend,
    ) -> Result<SendTicket, LogicalOutboundRejected>;
}
pub(crate) struct LogicalCommittedRejected {
    pub(crate) reason: LogicalReleaseError,
    pub(crate) observation: Box<crate::logical_voter::CommittedLogicalAction>,
}
pub(super) struct ExpectedCapsule {
    pub capsule: LogicalCapsule,
    pub permit: Option<ReleasePermit>,
    pub submitted: bool,
    pub settled: bool,
}
pub(super) enum Payload<R, V> {
    Send(Box<EffectLease>),
    RejectedSend {
        original: Box<EffectTicket>,
        output: Box<AuthorizedLogicalSend>,
    },
    Applied(Box<CapturedApplication<R>>),
    Read(Box<CapturedInvocation<V>>),
}
pub(super) struct Pending<R, V> {
    pub payload: Payload<R, V>,
    pub capsules: Vec<ExpectedCapsule>,
    pub charged: usize,
    pub control: bool,
}
#[derive(Debug)]
pub(crate) enum LogicalCaptureError {
    Rejected {
        reason: LogicalReleaseError,
        lease: Box<EffectLease>,
    },
    Failed {
        reason: LogicalReleaseError,
    },
}
