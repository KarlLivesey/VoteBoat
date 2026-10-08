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
//! Bounded outbound ownership before a transport. Local completion cannot
//! establish a durable follower acknowledgement or read authority.
use crate::{identity::*, log::EntryPayload, quorum::Tree, raft::*};
use std::mem::size_of;

pub const OUTBOUND_CONTRACT_VERSION: u32 = 1;
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub struct OutboundBinding {
    pub node: NodeId,
    pub store: StoreBinding,
    /// Fresh within this store session; never reused across queue instances.
    pub generation: OutboundGeneration,
}
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub struct SendTicket {
    pub binding: OutboundBinding,
    pub peer: NodeId,
    pub sequence: u64,
}
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum MessageClass {
    Control,
    Data,
    Background,
}
impl MessageClass {
    pub(crate) fn index(self) -> usize {
        match self {
            Self::Control => 0,
            Self::Data => 1,
            Self::Background => 2,
        }
    }
}
#[derive(Clone, Copy, Debug, Default, Eq, PartialEq)]
pub struct OutboundUsage {
    pub batches: usize,
    pub messages: usize,
    pub bytes: usize,
}
impl OutboundUsage {
    pub(crate) fn add(&mut self, other: Self) {
        self.batches += other.batches;
        self.messages += other.messages;
        self.bytes += other.bytes;
    }
    #[cfg(feature = "native")]
    pub(crate) fn sub(&mut self, other: Self) {
        self.batches -= other.batches;
        self.messages -= other.messages;
        self.bytes -= other.bytes;
    }
    pub fn fits(self, other: Self, max: Self) -> bool {
        self.batches
            .checked_add(other.batches)
            .is_some_and(|v| v <= max.batches)
            && self
                .messages
                .checked_add(other.messages)
                .is_some_and(|v| v <= max.messages)
            && self
                .bytes
                .checked_add(other.bytes)
                .is_some_and(|v| v <= max.bytes)
    }
    fn minus(self, other: Self) -> Self {
        Self {
            batches: self.batches - other.batches,
            messages: self.messages - other.messages,
            bytes: self.bytes - other.bytes,
        }
    }
}
#[derive(Clone, Copy, Debug)]
pub struct OutboundBudget {
    pub max: OutboundUsage,
    pub control: OutboundUsage,
    pub background: OutboundUsage,
}
impl OutboundBudget {
    fn validate(self) -> bool {
        let max = self.max;
        let reserve = self.control;
        let bg = self.background;
        max.batches > 1
            && max.batches <= 65536
            && max.messages > 1
            && max.messages <= 1_048_576
            && max.bytes > 0
            && max.bytes <= 1024 * 1024 * 1024
            && reserve.batches > 0
            && reserve.batches < max.batches
            && reserve.messages > 0
            && reserve.messages < max.messages
            && reserve.bytes >= size_of::<Message>() + size_of::<crate::log::LogEntry>() + 256
            && reserve.bytes < max.bytes
            && bg.batches > 0
            && bg.batches <= max.batches - reserve.batches
            && bg.messages > 0
            && bg.messages <= max.messages - reserve.messages
            && bg.bytes > 0
            && bg.bytes <= max.bytes - reserve.bytes
    }
    pub fn admits(
        self,
        usage: [OutboundUsage; 3],
        class: MessageClass,
        cost: OutboundUsage,
    ) -> bool {
        if !self.validate()
            || usage
                .iter()
                .any(|v| !OutboundUsage::default().fits(*v, self.max))
        {
            return false;
        }
        let mut total = usage[0];
        total.add(usage[1]);
        total.add(usage[2]);
        if !total.fits(cost, self.max) {
            return false;
        }
        if class != MessageClass::Control {
            let mut bulk = usage[1];
            bulk.add(usage[2]);
            if !bulk.fits(cost, self.max.minus(self.control)) {
                return false;
            }
        }
        class != MessageClass::Background || usage[2].fits(cost, self.background)
    }
}
#[derive(Clone, Copy, Debug)]
pub struct OutboundLimits {
    pub max_peers: usize,
    pub node: OutboundBudget,
    pub peer: OutboundBudget,
    pub batch_messages: usize,
    pub batch_bytes: usize,
}
impl Default for OutboundLimits {
    fn default() -> Self {
        Self {
            max_peers: 1024,
            node: OutboundBudget {
                max: OutboundUsage {
                    batches: 4096,
                    messages: 8192,
                    bytes: 64 * 1024 * 1024,
                },
                control: OutboundUsage {
                    batches: 512,
                    messages: 1024,
                    bytes: 4 * 1024 * 1024,
                },
                background: OutboundUsage {
                    batches: 128,
                    messages: 512,
                    bytes: 16 * 1024 * 1024,
                },
            },
            peer: OutboundBudget {
                max: OutboundUsage {
                    batches: 256,
                    messages: 512,
                    bytes: 4 * 1024 * 1024,
                },
                control: OutboundUsage {
                    batches: 32,
                    messages: 64,
                    bytes: 256 * 1024,
                },
                background: OutboundUsage {
                    batches: 16,
                    messages: 64,
                    bytes: 1024 * 1024,
                },
            },
            batch_messages: 128,
            batch_bytes: 1024 * 1024,
        }
    }
}
impl OutboundLimits {
    pub fn validate(self) -> Result<Self, OutboundError> {
        if self.max_peers == 0
            || self.max_peers > 65536
            || !self.node.validate()
            || !self.peer.validate()
            || !OutboundUsage::default().fits(self.peer.max, self.node.max)
            || self.batch_messages == 0
            || self.batch_messages > self.peer.max.messages
            || self.batch_bytes < size_of::<Message>() + size_of::<crate::log::LogEntry>() + 256
            || self.batch_bytes > self.peer.max.bytes
        {
            return Err(OutboundError::InvalidLimits);
        }
        Ok(self)
    }
}
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum OutboundError {
    Overloaded,
    BatchTooLarge,
    WrongBinding,
    WrongPeer,
    InvalidLimits,
    Closed,
    Exhausted,
    UnknownTicket,
    NotDispatched,
}
#[derive(Debug)]
pub struct SendRejected {
    pub reason: OutboundError,
    pub messages: Vec<Message>,
}
#[derive(Debug)]
pub struct OutboundBatch {
    pub ticket: SendTicket,
    pub messages: Vec<Message>,
}
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum LocalSendResult {
    Sent,
    Failed,
    Cancelled,
}
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub struct LocalSendCompletion {
    pub ticket: SendTicket,
    pub result: LocalSendResult,
}
#[derive(Debug)]
pub struct CompletionRejected {
    pub reason: OutboundError,
    pub batch: Box<OutboundBatch>,
}

/// Version 1 has no wire format, sockets, security or persistent delivery claim.
/// Submission transfers a same-peer owned batch only on success. Poll transfers
/// accepted batches to the transport; both queued and dispatched work retain
/// credits. Complete only after transport I/O has released its buffers; consuming
/// the batch then releases credits, even on failure/cancellation. Such a result
/// has unknown remote delivery and cannot be fed to Raft as an acknowledgement.
///
/// Rejection returns the entire batch. Fair bounded polls serve peers and traffic
/// classes without starving data behind continuous control. FIFO holds within
/// one peer/class, not between classes. Mixed batches use the most restrictive
/// class (snapshot, then command, then control). Closing rejects new admission
/// and drains accepted work. Abandoning an in-flight batch leaves credits held;
/// close is not rollback. Recover the authoritative log after process loss.
/// The caller drives polling after admission/completion and owns all downstream
/// resources. Feature inclusion creates no resources. No hidden executor/wake.
pub trait OutboundQueue {
    fn binding(&self) -> OutboundBinding;
    fn limits(&self) -> OutboundLimits;
    fn usage(&self) -> OutboundUsage;
    fn peer_usage(&self, peer: NodeId) -> OutboundUsage;
    fn submit(&mut self, messages: Vec<Message>) -> Result<SendTicket, SendRejected>;
    fn poll(&mut self, limit: usize) -> Vec<OutboundBatch>;
    fn complete(
        &mut self,
        batch: OutboundBatch,
        result: LocalSendResult,
    ) -> Result<LocalSendCompletion, CompletionRejected>;
    fn close(&mut self);
    fn is_drained(&self) -> bool {
        self.usage().batches == 0
    }
}

/// Conservative retained capacity accounting shared with runtime ingress.
/// Counts owned payloads/index units, not allocator metadata or exact RSS.
pub fn message_cost(
    message: &Message,
    limit: usize,
) -> Result<(MessageClass, usize), OutboundError> {
    let mut bytes = size_of::<Message>();
    let mut add = |n: usize| -> Result<(), OutboundError> {
        bytes = bytes
            .checked_add(n)
            .filter(|v| *v <= limit)
            .ok_or(OutboundError::BatchTooLarge)?;
        Ok(())
    };
    let class = match &message.rpc {
        Rpc::LearnerReadinessRequest(_) | Rpc::LearnerReadinessReply { .. } => {
            add(size_of::<crate::raft::LearnerReadinessRequest>())?;
            MessageClass::Control
        }
        Rpc::Append { entries, .. } | Rpc::LearnerRepair { entries, .. } => {
            if let Rpc::LearnerRepair { joint, .. } = &message.rpc {
                add(size_of::<crate::log::LogEntry>())?;
                add(joint.retained_payload_bytes())?;
            }
            add(entries
                .capacity()
                .checked_mul(size_of::<crate::log::LogEntry>())
                .ok_or(OutboundError::BatchTooLarge)?)?;
            let mut class = if matches!(message.rpc, Rpc::LearnerRepair { .. }) {
                MessageClass::Data
            } else {
                MessageClass::Control
            };
            for entry in entries {
                add(entry.retained_payload_bytes())?;
                if !matches!(entry.payload, EntryPayload::Noop) {
                    class = MessageClass::Data;
                }
            }
            class
        }
        Rpc::Snapshot { snapshot } => {
            add(size_of::<crate::snapshot::Snapshot>())?;
            add(snapshot.application.capacity())?;
            if let Some(membership) = &snapshot.metadata.membership {
                add(membership.retained_bytes())?;
            }
            let b = &snapshot.metadata.bootstrap;
            if b.voter_stores.len() > 4096 || b.policy.voters().len() > 4096 {
                return Err(OutboundError::BatchTooLarge);
            }
            add((b.voter_stores.len() + b.policy.voters().len()) * 128)?;
            let mut stack = vec![(b.policy.tree(), 0)];
            let mut count = 0;
            while let Some((tree, depth)) = stack.pop() {
                count += 1;
                if count > 16384 || depth > 32 {
                    return Err(OutboundError::BatchTooLarge);
                }
                match tree {
                    Tree::Voter(_) => (),
                    Tree::Majority(children) => {
                        add(children
                            .capacity()
                            .checked_mul(size_of::<Tree>())
                            .ok_or(OutboundError::BatchTooLarge)?)?;
                        if children.len() > 16384 - count - stack.len() {
                            return Err(OutboundError::BatchTooLarge);
                        }
                        stack.extend(children.iter().map(|t| (t, depth + 1)));
                    }
                    Tree::Weighted(children) => {
                        add(children
                            .capacity()
                            .checked_mul(size_of::<crate::quorum::WeightedChild>())
                            .ok_or(OutboundError::BatchTooLarge)?)?;
                        if children.len() > 16384 - count - stack.len() {
                            return Err(OutboundError::BatchTooLarge);
                        }
                        stack.extend(children.iter().map(|t| (&t.node, depth + 1)));
                    }
                }
            }
            MessageClass::Background
        }
        _ => MessageClass::Control,
    };
    if bytes > limit {
        return Err(OutboundError::BatchTooLarge);
    }
    Ok((class, bytes))
}
/// Metadata reservation plus vector capacity and every owned message payload.
/// Checks identity before acceptance. No transport/session authentication claim.
pub fn batch_cost(
    binding: OutboundBinding,
    messages: &[Message],
    capacity: usize,
    limits: OutboundLimits,
) -> Result<(NodeId, MessageClass, OutboundUsage), OutboundError> {
    if messages.is_empty()
        || messages.len() > limits.batch_messages
        || capacity < messages.len()
        || capacity > limits.batch_messages
    {
        return Err(OutboundError::BatchTooLarge);
    }
    let peer = messages[0].to;
    if peer == binding.node {
        return Err(OutboundError::WrongPeer);
    }
    let mut bytes = capacity
        .checked_mul(size_of::<Message>())
        .and_then(|v| v.checked_add(256))
        .ok_or(OutboundError::BatchTooLarge)?;
    let mut class = MessageClass::Control;
    for message in messages {
        if message.sender != binding.store || message.from != binding.node {
            return Err(OutboundError::WrongBinding);
        }
        if message.to != peer {
            return Err(OutboundError::WrongPeer);
        }
        let (kind, cost) = message_cost(message, limits.batch_bytes)?;
        if kind.index() > class.index() {
            class = kind;
        }
        bytes = bytes
            .checked_add(cost - size_of::<Message>())
            .ok_or(OutboundError::BatchTooLarge)?;
        if bytes > limits.batch_bytes {
            return Err(OutboundError::BatchTooLarge);
        }
    }
    Ok((
        peer,
        class,
        OutboundUsage {
            batches: 1,
            messages: messages.len(),
            bytes,
        },
    ))
}
