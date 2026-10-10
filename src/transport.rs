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
//! Authenticated peer stream contract. No transport event proves durability.
use crate::{outbound::*, raft::Message, runtime::MonoTime, secure::*};
mod peers;
pub use peers::*;
mod membership;
pub use membership::*;

/// Construction-selected conversion of one ready authenticated session into
/// one multiplexed transport. The factory owns the session after this call;
/// failure closes/drops it and creates no send or receive completion. It must
/// bind the selected outbound queue and preserve the authenticated session scope.
/// No implicit connector, reactor or alternate provider may be constructed.
pub trait PeerTransportFactory<S: SecureSession> {
    type Transport: PeerTransport;
    /// Check the selected codec and transport against the declared envelope.
    /// The default refuses configuration admission; static traffic is unchanged.
    fn configuration_capacity(
        &self,
        _required: &crate::wire::ConfigurationWireRequirements<'_>,
    ) -> Result<crate::wire::ConfigurationWireCapacity, TransportError> {
        Err(TransportError::UnsupportedConfigurationAdmission)
    }
    fn build<O: OutboundQueue>(
        &mut self,
        session: S,
        outbound: &O,
    ) -> Result<Self::Transport, TransportError>;
}

pub const PEER_TRANSPORT_CONTRACT_VERSION: u32 = 4;

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum TransportState {
    Open,
    Draining,
    Closed,
    Failed,
}
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum TransportError {
    UnsupportedConfigurationAdmission,
    InvalidLimits,
    IncompatibleCodec,
    WrongBinding,
    Overloaded,
    Closed,
    Aborted,
    Truncated,
    ProviderViolation,
    Outbound(OutboundError),
    Session(SessionError),
    Wire(crate::wire::WireError),
    Buffer(crate::buffer::BufferError),
}
#[derive(Clone, Copy, Debug)]
pub struct TransportLimits {
    pub send_frame_bytes: usize,
    pub receive_frame_bytes: usize,
    pub decoded_bytes: usize,
}
impl Default for TransportLimits {
    fn default() -> Self {
        Self {
            send_frame_bytes: 1024 * 1024,
            receive_frame_bytes: 1024 * 1024,
            decoded_bytes: 4 * 1024 * 1024,
        }
    }
}
impl TransportLimits {
    pub fn validate(self) -> Result<Self, TransportError> {
        if self.send_frame_bytes == 0
            || self.send_frame_bytes > 64 * 1024 * 1024
            || self.receive_frame_bytes == 0
            || self.receive_frame_bytes > 64 * 1024 * 1024
            || self.decoded_bytes < std::mem::size_of::<Message>()
            || self.decoded_bytes > 256 * 1024 * 1024
        {
            return Err(TransportError::InvalidLimits);
        }
        Ok(self)
    }
}
#[derive(Clone, Copy, Debug)]
pub struct TransportPollBudget {
    pub session: SessionPollBudget,
    pub plaintext_calls: usize,
    pub read_bytes: usize,
    pub write_bytes: usize,
}
impl Default for TransportPollBudget {
    fn default() -> Self {
        Self {
            session: SessionPollBudget::default(),
            plaintext_calls: 16,
            read_bytes: 64 * 1024,
            write_bytes: 64 * 1024,
        }
    }
}
impl TransportPollBudget {
    pub fn validate(self) -> Result<Self, TransportError> {
        self.session.validate().map_err(TransportError::Session)?;
        if self.plaintext_calls > 4096
            || self.read_bytes > 64 * 1024 * 1024
            || self.write_bytes > 64 * 1024 * 1024
        {
            return Err(TransportError::InvalidLimits);
        }
        Ok(self)
    }
}
#[derive(Clone, Copy, Debug, Default, Eq, PartialEq)]
pub struct TransportProgress {
    pub session: SessionProgress,
    pub plaintext_calls: usize,
    pub read_bytes: usize,
    pub written_bytes: usize,
    pub received: bool,
    pub sent: bool,
}
#[derive(Clone, Copy, Debug, Default, Eq, PartialEq)]
pub struct TransportUsage {
    pub send_frame_bytes: usize,
    pub receive_frame_bytes: usize,
    pub decoded_bytes: usize,
    pub sending: bool,
    pub completion: bool,
}
#[derive(Debug)]
pub struct TransportRejected {
    pub reason: TransportError,
    pub batch: Box<OutboundBatch>,
}
#[derive(Debug)]
pub struct TransportSend {
    pub connection: SessionBinding,
    pub batch: OutboundBatch,
    pub result: LocalSendResult,
}
#[derive(Debug)]
pub struct ReceivedBatch {
    pub connection: SessionBinding,
    pub messages: Vec<Message>,
}
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub struct ReceiveInfo {
    pub connection: SessionBinding,
    pub class: MessageClass,
    pub messages: usize,
    /// Original vector capacity plus all retained nested payload capacities.
    pub bytes: usize,
}
impl ReceivedBatch {
    /// Bounded inspection shared by native and host providers. This validates
    /// scope and accounting, not cryptography; the session must authenticate.
    pub fn info(&self, limit: usize) -> Result<ReceiveInfo, TransportError> {
        let invalid = TransportError::ProviderViolation;
        let mut bytes = self
            .messages
            .capacity()
            .checked_mul(std::mem::size_of::<Message>())
            .filter(|n| *n <= limit)
            .ok_or(invalid)?;
        if self.messages.is_empty() || self.connection.local.node == self.connection.peer.node {
            return Err(invalid);
        }
        let mut class = MessageClass::Control;
        for message in &self.messages {
            if !self.connection.incoming().matches(message) {
                return Err(invalid);
            }
            let (kind, cost) = message_cost(message, limit).map_err(|_| invalid)?;
            if kind.index() > class.index() {
                class = kind;
            }
            bytes = bytes
                .checked_add(cost - std::mem::size_of::<Message>())
                .filter(|n| *n <= limit)
                .ok_or(invalid)?;
        }
        Ok(ReceiveInfo {
            connection: self.connection,
            class,
            messages: self.messages.len(),
            bytes,
        })
    }
}

/// Contract 2 is one authenticated peer connection, multiplexing groups per frame.
/// Construction selects a ready session, codec, exact outbound queue binding and
/// finite budgets. The host owns the peer roster, fair connection visits,
/// reconnects and global budgets; this handle creates no threads or listeners.
///
/// Submit transfers one dispatched outbound batch only on success. Rejection
/// returns it unchanged. One accepted batch, including its unobserved terminal
/// completion, occupies the send slot. Short stream writes retain its encoded
/// buffer and original messages. Sent means all frame bytes were accepted and
/// local channel output drained; it is never a remote Raft acknowledgement.
/// Failure releases channel/frame buffers before returning a Failed completion,
/// with unknown remote delivery. The queue owner must consume the exact returned
/// batch with OutboundQueue::complete to release its original credits.
///
/// Receive validates a fixed prefix before allocating its declared bounded
/// frame. Decode returns an entire batch or no messages. One decoded receive
/// occupies the receive slot; polling cannot consume another frame until the
/// owner takes it into separately budgeted ingress. Previously validated batches
/// remain available after a later failure; partial frames never escape. Tagged
/// bindings let the owner reject obsolete connections before ingress admission.
///
/// Poll separately bounds session I/O and plaintext work. All session progress
/// within one transport poll shares its supplied session budget, including any
/// session polls interleaved with plaintext work. Close rejects new
/// sends, drains the accepted send, then closes this channel and discards partial
/// receive work; completed receive/send slots remain observable. Abort releases
/// the channel immediately and fails accepted sends. Dropping a handle abandons
/// observation, not external progress or the queue's retained credits.
pub trait PeerTransport {
    /// Production providers attest an authenticated identity-bound channel.
    fn security(&self) -> SessionSecurity;
    fn binding(&self) -> SessionBinding;
    fn state(&self) -> TransportState;
    fn limits(&self) -> TransportLimits;
    fn usage(&self) -> TransportUsage;
    fn submit(&mut self, batch: OutboundBatch) -> Result<(), TransportRejected>;
    fn poll(
        &mut self,
        now: MonoTime,
        budget: TransportPollBudget,
    ) -> Result<TransportProgress, TransportError>;
    fn take_send(&mut self) -> Option<TransportSend>;
    /// Immutable exact metadata for the retained decoded receive. Until taken
    /// or aborted, polls cannot replace it. Inspection transfers no ownership.
    /// This enables ingress to reserve count/byte/control credits before take.
    fn received_info(&self) -> Option<ReceiveInfo>;
    fn take_received(&mut self) -> Option<ReceivedBatch>;
    fn close(&mut self);
    fn abort(&mut self);
}

impl<P: PeerTransport + ?Sized> PeerTransport for Box<P> {
    fn security(&self) -> SessionSecurity {
        (**self).security()
    }
    fn binding(&self) -> SessionBinding {
        (**self).binding()
    }
    fn state(&self) -> TransportState {
        (**self).state()
    }
    fn limits(&self) -> TransportLimits {
        (**self).limits()
    }
    fn usage(&self) -> TransportUsage {
        (**self).usage()
    }
    fn submit(&mut self, batch: OutboundBatch) -> Result<(), TransportRejected> {
        (**self).submit(batch)
    }
    fn poll(
        &mut self,
        now: MonoTime,
        budget: TransportPollBudget,
    ) -> Result<TransportProgress, TransportError> {
        (**self).poll(now, budget)
    }
    fn take_send(&mut self) -> Option<TransportSend> {
        (**self).take_send()
    }
    fn received_info(&self) -> Option<ReceiveInfo> {
        (**self).received_info()
    }
    fn take_received(&mut self) -> Option<ReceivedBatch> {
        (**self).take_received()
    }
    fn close(&mut self) {
        (**self).close()
    }
    fn abort(&mut self) {
        (**self).abort()
    }
}
