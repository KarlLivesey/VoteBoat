// SPDX-License-Identifier: RPL-1.5
// Copyright (c) 2026 Karl Livesey
//! Original relay output ownership and encoding; authenticated hop I/O is separate.
use crate::{
    outbound::*, raft::request_window::*, runtime::*, transport::AuthenticatedSourceView, wire::*,
};
use std::mem::size_of;
mod completion;
pub use completion::*;
mod network;
pub(crate) use network::VerifiedDelegatedSource;
pub use network::*;

pub const RELAY_OUTPUT_CONTRACT_VERSION: u32 = 1;
pub const RELAY_NETWORK_VERSION: u16 = 0x8003;
#[derive(Clone, Debug, Eq, PartialEq)]
pub enum RelayOutputError {
    InvalidLimits,
    WrongBinding,
    ProviderViolation,
    RetiredRequest,
    UnknownGroup,
    TooLarge,
    Wire(WireError),
    Outbound(OutboundError),
}
#[derive(Clone, Copy, Debug)]
pub struct RelayOutputLimits {
    pub batch_messages: usize,
    pub metadata_bytes: usize,
    pub frame_bytes: usize,
}
impl Default for RelayOutputLimits {
    fn default() -> Self {
        Self {
            batch_messages: 128,
            metadata_bytes: 256 * 1024,
            frame_bytes: 4 * 1024 * 1024,
        }
    }
}
impl RelayOutputLimits {
    fn validate(self) -> Result<Self, RelayOutputError> {
        if self.batch_messages == 0
            || self.batch_messages > 4096
            || self.metadata_bytes < size_of::<NativeRelayBatch>()
            || self.metadata_bytes > 16 * 1024 * 1024
            || self.frame_bytes == 0
            || self.frame_bytes > 16 * 1024 * 1024
        {
            return Err(RelayOutputError::InvalidLimits);
        }
        Ok(self)
    }
}
pub struct RelayCaptureRejected {
    pub reason: RelayOutputError,
    /// Every original extracted from a violating or stale provider is retained.
    pub batches: Vec<OutboundBatch>,
}
/// Only capture from the actual selected queue creates this owner. It retains
/// the original allocation/admission/ticket until exact local completion.
pub struct NativeRelayBatch {
    owner: RuntimeOwner,
    batch: OutboundBatch,
    scopes: Vec<OriginalScope>,
    limits: RelayOutputLimits,
}
impl NativeRelayBatch {
    /// Most metadata a batch of `messages` retains: every message a request
    /// window that owns a readiness request. Sizes the relay's own output
    /// limits, so no valid batch is refused by them.
    pub(in crate::native::relay) fn metadata_bound(messages: usize) -> Option<usize> {
        messages
            .checked_mul(
                size_of::<OriginalScope>()
                    + size_of::<RequestWindow>()
                    + size_of::<crate::raft::LearnerReadinessRequest>(),
            )
            .and_then(|n| n.checked_add(size_of::<Self>()))
    }
    /// Metadata this batch's scopes actually retain.
    fn metadata_bytes(scopes: &[OriginalScope]) -> Option<usize> {
        scopes.iter().try_fold(size_of::<Self>(), |sum, scope| {
            let owned = match scope {
                OriginalScope::Request(w) => size_of::<RequestWindow>() + w.owned_bytes(),
                _ => 0,
            };
            sum.checked_add(size_of::<OriginalScope>() + owned)
        })
    }
    pub fn capture<Q: ReadyScheduler, T: TimerService, E: ElectionEntropy, O: OutboundQueue>(
        owner: &EffectOwner<Q, T, E>,
        outbound: &mut O,
        limits: RelayOutputLimits,
    ) -> Result<Option<Self>, RelayCaptureRejected> {
        let preflight = || {
            let limits = limits.validate()?;
            let binding = outbound.binding();
            if owner.identity().store != binding.store {
                return Err(RelayOutputError::WrongBinding);
            }
            outbound
                .limits()
                .validate()
                .map_err(RelayOutputError::Outbound)?;
            Ok(limits)
        };
        let limits = preflight().map_err(|reason| RelayCaptureRejected {
            reason,
            batches: vec![],
        })?;
        let mut batches = outbound.poll(1);
        if batches.is_empty() {
            return Ok(None);
        }
        if batches.len() != 1 {
            return Err(RelayCaptureRejected {
                reason: RelayOutputError::ProviderViolation,
                batches,
            });
        }
        let batch = batches.pop().unwrap();
        match Self::classify(owner, outbound, &batch, limits) {
            Ok(scopes) => Ok(Some(Self {
                owner: owner.identity(),
                batch,
                scopes,
                limits,
            })),
            Err(reason) => Err(RelayCaptureRejected {
                reason,
                batches: vec![batch],
            }),
        }
    }
    fn classify<Q: ReadyScheduler, T: TimerService, E: ElectionEntropy, O: OutboundQueue>(
        owner: &EffectOwner<Q, T, E>,
        outbound: &O,
        batch: &OutboundBatch,
        limits: RelayOutputLimits,
    ) -> Result<Vec<OriginalScope>, RelayOutputError> {
        let binding = outbound.binding();
        if batch.ticket.binding != binding
            || batch.ticket.sequence == 0
            || batch.messages.is_empty()
        {
            return Err(RelayOutputError::WrongBinding);
        }
        // Before allocating: the scopes and windows alone must fit.
        let windows = batch
            .messages
            .len()
            .checked_mul(size_of::<OriginalScope>() + size_of::<RequestWindow>())
            .and_then(|n| n.checked_add(size_of::<Self>()))
            .ok_or(RelayOutputError::TooLarge)?;
        if batch.messages.len() > limits.batch_messages || windows > limits.metadata_bytes {
            return Err(RelayOutputError::TooLarge);
        }
        let mut scopes = Vec::with_capacity(batch.messages.len());
        for m in &batch.messages {
            if m.from != binding.node || m.sender != binding.store || m.to != batch.ticket.peer {
                return Err(RelayOutputError::WrongBinding);
            }
            let core = owner.core(m.group).ok_or(RelayOutputError::UnknownGroup)?;
            let scope = core.request_window(m);
            if matches!(scope, OriginalScope::Retired) {
                return Err(RelayOutputError::RetiredRequest);
            }
            scopes.push(scope);
        }
        // Exactly what is retained, including each window's readiness request.
        if Self::metadata_bytes(&scopes).is_none_or(|n| n > limits.metadata_bytes) {
            return Err(RelayOutputError::TooLarge);
        }
        Ok(scopes)
    }
    pub fn ticket(&self) -> SendTicket {
        self.batch.ticket
    }
    pub fn original(&self) -> &OutboundBatch {
        &self.batch
    }
    /// Encode original physical RPC scope, not the future helper's TLS identity.
    /// Failure retains this complete batch and produces no escaped frame.
    pub fn encode<Q: ReadyScheduler, T: TimerService, E: ElectionEntropy>(
        &self,
        owner: &EffectOwner<Q, T, E>,
        codec: &impl WireCodec,
    ) -> Result<Vec<u8>, RelayOutputError> {
        if owner.identity() != self.owner {
            return Err(RelayOutputError::WrongBinding);
        }
        for (m, scope) in self.batch.messages.iter().zip(&self.scopes) {
            let core = owner.core(m.group).ok_or(RelayOutputError::UnknownGroup)?;
            let valid = match scope {
                OriginalScope::Request(window) => window.current(core),
                OriginalScope::OneWay => matches!(core.request_window(m), OriginalScope::OneWay),
                OriginalScope::Reply => matches!(core.request_window(m), OriginalScope::Reply),
                OriginalScope::Retired => false,
            };
            if !valid {
                return Err(RelayOutputError::RetiredRequest);
            }
        }
        let scope = WireScope {
            from: self.batch.ticket.binding.node,
            sender: self.batch.ticket.binding.store,
            to: self.batch.ticket.peer,
        };
        let length = codec
            .encoded_length(scope, &self.batch.messages)
            .map_err(RelayOutputError::Wire)?;
        if length > self.limits.frame_bytes {
            return Err(RelayOutputError::TooLarge);
        }
        let mut bytes = vec![0; length];
        codec
            .encode_into(scope, &self.batch.messages, &mut bytes)
            .map_err(RelayOutputError::Wire)?;
        Ok(bytes)
    }
}
