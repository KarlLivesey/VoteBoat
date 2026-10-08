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
//! Fixed ingress ownership guard over the public peer transport and serialized
//! effect owner. No socket, clock, application or core transition runs implicitly.
use super::*;
use crate::{
    outbound::MessageClass,
    secure::{LocalIdentity, SessionBinding},
    transport::*,
};
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub struct IngressBinding {
    pub owner: RuntimeOwner,
    pub local: LocalIdentity,
    /// Fresh, host-reserved within this runtime owner; replacement never reuses it.
    pub generation: IngressGeneration,
}
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub struct IngressTicket {
    pub binding: IngressBinding,
    pub sequence: u64,
}
#[derive(Clone, Debug, Eq, PartialEq)]
pub enum IngressError {
    InvalidLimits,
    WrongBinding,
    Overloaded,
    BatchTooLarge,
    Closed,
    Exhausted,
    ProviderViolation,
    Peer(PeerRosterError),
}
#[derive(Debug)]
pub struct IngressReceiveRejected {
    pub reason: IngressError,
    /// None means no receive ownership transferred. A violating provider's
    /// extracted payload is returned whole for explicit bounded failure handling.
    pub batch: Option<Box<ReceivedBatch>>,
}
#[derive(Clone, Copy, Debug)]
pub struct IngressLimits {
    pub batches: usize,
    pub messages: usize,
    pub bytes: usize,
    pub control_batches: usize,
    pub control_messages: usize,
    pub control_bytes: usize,
    pub background_batches: usize,
    pub background_messages: usize,
    pub background_bytes: usize,
    pub batch_messages: usize,
    pub batch_bytes: usize,
}
impl Default for IngressLimits {
    fn default() -> Self {
        Self {
            batches: 64,
            messages: 8192,
            bytes: 64 * 1024 * 1024,
            control_batches: 8,
            control_messages: 512,
            control_bytes: 8 * 1024 * 1024,
            background_batches: 8,
            background_messages: 1024,
            background_bytes: 16 * 1024 * 1024,
            batch_messages: 128,
            batch_bytes: 4 * 1024 * 1024,
        }
    }
}
impl IngressLimits {
    pub fn validate(self) -> Result<Self, IngressError> {
        if self.batches < 3
            || self.batches > 4096
            || self.messages < 3
            || self.messages > 1_048_576
            || self.bytes == 0
            || self.bytes > 1024 * 1024 * 1024
            || self.control_batches == 0
            || self.control_batches >= self.batches
            || self.control_messages == 0
            || self.control_messages >= self.messages
            || self.control_bytes == 0
            || self.control_bytes >= self.bytes
            || self.background_batches == 0
            || self.background_batches >= self.batches - self.control_batches
            || self.background_messages == 0
            || self.background_messages >= self.messages - self.control_messages
            || self.background_bytes == 0
            || self.background_bytes >= self.bytes - self.control_bytes
            || self.batch_messages == 0
            || self.batch_messages > 4096
            || self.batch_messages > self.background_messages
            || self.batch_messages > self.control_messages
            || self.batch_bytes == 0
            || self
                .batch_bytes
                .checked_add(size_of::<Pending>())
                .is_none_or(|n| n > self.background_bytes || n > self.control_bytes)
        {
            return Err(IngressError::InvalidLimits);
        }
        Ok(self)
    }
}
#[derive(Clone, Copy, Debug, Default, Eq, PartialEq)]
pub struct IngressUsage {
    pub batches: usize,
    pub messages: usize,
    pub bytes: usize,
}
impl IngressUsage {
    fn add(self, other: Self) -> Self {
        Self {
            batches: self.batches + other.batches,
            messages: self.messages + other.messages,
            bytes: self.bytes + other.bytes,
        }
    }
    fn fits(self, other: Self) -> bool {
        self.batches <= other.batches
            && self.messages <= other.messages
            && self.bytes <= other.bytes
    }
    fn release(&mut self, other: Self) {
        self.batches -= other.batches;
        self.messages -= other.messages;
        self.bytes -= other.bytes;
    }
}
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum IngressEnd {
    Drained,
    StaleConnection,
    Cancelled,
}
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub struct IngressCompletion {
    pub ticket: IngressTicket,
    pub connection: SessionBinding,
    pub admitted: usize,
    pub rejected: usize,
    pub discarded: usize,
    pub end: IngressEnd,
}
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct IngressMessageRejected {
    pub ticket: IngressTicket,
    pub group: GroupIdentity,
    pub reason: RuntimeError,
}
#[derive(Debug, Default)]
pub struct IngressProgress {
    pub visits: usize,
    pub admitted: usize,
    pub blocked: usize,
    pub discarded: usize,
    pub completed: Vec<IngressCompletion>,
    pub rejected: Vec<IngressMessageRejected>,
}
struct Pending {
    ticket: IngressTicket,
    batch: ReceivedBatch,
    info: ReceiveInfo,
    charged: IngressUsage,
    cursor: usize,
    admitted: usize,
    rejected: usize,
}
/// Exactly bounded transfer from authenticated decoded frames into an EffectOwner.
/// This is a fixed identity/ownership guard, composed over the same public
/// PeerTransport/ReadyScheduler/TimerService contracts as native and host providers.
/// It creates no alternative Raft ingress owner or pluggable safety policy.
///
/// Receive metadata reserves count/byte/class credits before extraction. Overload
/// leaves the decoded frame in the transport, so it cannot decode another. Accepted
/// frames retain their full original charge until terminal completion, even after
/// some messages transfer. Dispatch rotates batches/messages so one overloaded
/// group cannot indefinitely hold other groups. Message order is not a protocol
/// guarantee; Raft still validates terms and ordered log prefixes.
///
/// Dispatch checks current connection binding before each message transfer. Retired
/// connections discard only still-held input; an already admitted event belongs to
/// the serialized owner and cannot be rolled back. Close stops receive admission
/// and drains; abort reports cancellations and drops held RPCs. No completion here
/// proves execution, commitment, application or remote durability. All Raft effects
/// remain behind their existing exact persistence dependencies.
pub struct IngressRouter {
    binding: IngressBinding,
    limits: IngressLimits,
    pending: VecDeque<Pending>,
    usage: [IngressUsage; 3],
    sequence: u64,
    closed: bool,
}
impl IngressRouter {
    pub fn new(binding: IngressBinding, limits: IngressLimits) -> Result<Self, IngressError> {
        let limits = limits.validate()?;
        if binding.owner.store != binding.local.store {
            return Err(IngressError::WrongBinding);
        }
        Ok(Self {
            binding,
            limits,
            pending: VecDeque::with_capacity(limits.batches),
            usage: [IngressUsage::default(); 3],
            sequence: 0,
            closed: false,
        })
    }
    pub fn binding(&self) -> IngressBinding {
        self.binding
    }
    pub fn limits(&self) -> IngressLimits {
        self.limits
    }
    pub fn usage(&self) -> IngressUsage {
        self.usage
            .iter()
            .copied()
            .fold(IngressUsage::default(), IngressUsage::add)
    }
    fn charge(&self, info: ReceiveInfo) -> Result<IngressUsage, IngressError> {
        if info.connection.local != self.binding.local {
            return Err(IngressError::WrongBinding);
        }
        if info.messages > self.limits.batch_messages || info.bytes > self.limits.batch_bytes {
            return Err(IngressError::BatchTooLarge);
        }
        let charged = IngressUsage {
            batches: 1,
            messages: info.messages,
            bytes: info
                .bytes
                .checked_add(size_of::<Pending>())
                .ok_or(IngressError::BatchTooLarge)?,
        };
        let l = self.limits;
        if !self.usage().add(charged).fits(IngressUsage {
            batches: l.batches,
            messages: l.messages,
            bytes: l.bytes,
        }) {
            return Err(IngressError::Overloaded);
        }
        if info.class != MessageClass::Control
            && !self.usage[1]
                .add(self.usage[2])
                .add(charged)
                .fits(IngressUsage {
                    batches: l.batches - l.control_batches,
                    messages: l.messages - l.control_messages,
                    bytes: l.bytes - l.control_bytes,
                })
        {
            return Err(IngressError::Overloaded);
        }
        if info.class == MessageClass::Background
            && !self.usage[2].add(charged).fits(IngressUsage {
                batches: l.background_batches,
                messages: l.background_messages,
                bytes: l.background_bytes,
            })
        {
            return Err(IngressError::Overloaded);
        }
        Ok(charged)
    }
    /// Preflight rejection leaves the frame in its transport. An accepted ticket
    /// holds credits until dispatch/abort returns one exact terminal completion.
    pub fn receive<P: PeerTransport>(
        &mut self,
        roster: &mut PeerRoster<P>,
        peer: NodeId,
    ) -> Result<Option<IngressTicket>, IngressReceiveRejected> {
        let preflight = (|| {
            if self.closed {
                return Err(IngressError::Closed);
            }
            if roster.local() != self.binding.local {
                return Err(IngressError::WrongBinding);
            }
            let Some(info) = roster.received_info(peer).map_err(IngressError::Peer)? else {
                return Ok(None);
            };
            let charged = self.charge(info)?;
            let sequence = self
                .sequence
                .checked_add(1)
                .ok_or(IngressError::Exhausted)?;
            Ok(Some((info, charged, sequence)))
        })();
        let (info, charged, sequence) = match preflight {
            Ok(Some(v)) => v,
            Ok(None) => return Ok(None),
            Err(reason) => {
                return Err(IngressReceiveRejected {
                    reason,
                    batch: None,
                })
            }
        };
        let batch = match roster.take_received(peer) {
            Ok(Some(batch)) => batch,
            Ok(None) => {
                self.closed = true;
                return Err(IngressReceiveRejected {
                    reason: IngressError::ProviderViolation,
                    batch: None,
                });
            }
            Err(rejected) => {
                self.closed = true;
                return Err(IngressReceiveRejected {
                    reason: IngressError::Peer(rejected.reason),
                    batch: Some(rejected.batch),
                });
            }
        };
        if batch.info(self.limits.batch_bytes).ok() != Some(info) {
            self.closed = true;
            return Err(IngressReceiveRejected {
                reason: IngressError::ProviderViolation,
                batch: Some(Box::new(batch)),
            });
        }
        self.sequence = sequence;
        let ticket = IngressTicket {
            binding: self.binding,
            sequence,
        };
        self.usage[info.class.index()] = self.usage[info.class.index()].add(charged);
        self.pending.push_back(Pending {
            ticket,
            batch,
            info,
            charged,
            cursor: 0,
            admitted: 0,
            rejected: 0,
        });
        Ok(Some(ticket))
    }
    fn finish(&mut self, pending: Pending, end: IngressEnd) -> IngressCompletion {
        self.usage[pending.info.class.index()].release(pending.charged);
        IngressCompletion {
            ticket: pending.ticket,
            connection: pending.info.connection,
            admitted: pending.admitted,
            rejected: pending.rejected,
            discarded: pending.batch.messages.len(),
            end,
        }
    }
    pub fn dispatch<P: PeerTransport, Q: ReadyScheduler, T: TimerService, E: ElectionEntropy>(
        &mut self,
        roster: &PeerRoster<P>,
        owner: &mut EffectOwner<Q, T, E>,
        visits: usize,
    ) -> Result<IngressProgress, IngressError> {
        if visits > 65536 {
            return Err(IngressError::InvalidLimits);
        }
        if owner.identity() != self.binding.owner || roster.local() != self.binding.local {
            return Err(IngressError::WrongBinding);
        }
        if roster.is_fenced() {
            return Err(IngressError::Peer(PeerRosterError::Fenced));
        }
        let mut progress = IngressProgress::default();
        for _ in 0..visits {
            let Some(mut pending) = self.pending.pop_front() else {
                break;
            };
            progress.visits += 1;
            if roster.binding(pending.info.connection.peer.node) != Some(pending.info.connection) {
                progress.discarded += pending.batch.messages.len();
                progress
                    .completed
                    .push(self.finish(pending, IngressEnd::StaleConnection));
                continue;
            }
            let index = pending.cursor % pending.batch.messages.len();
            let message = pending.batch.messages.swap_remove(index);
            let group = message.group;
            match owner.admit(group, Event::Receive(message)) {
                Ok(()) => {
                    pending.admitted += 1;
                    progress.admitted += 1;
                }
                Err(rejected) => {
                    let Event::Receive(message) = *rejected.event else {
                        unreachable!()
                    };
                    if rejected.reason == RuntimeError::Overloaded {
                        // Restore the original vector order without allocating;
                        // only the scan cursor rotates past this blocked input.
                        let last = pending.batch.messages.len();
                        pending.batch.messages.push(message);
                        pending.batch.messages.swap(index, last);
                        pending.cursor = index + 1;
                        progress.blocked += 1;
                    } else {
                        pending.rejected += 1;
                        progress.rejected.push(IngressMessageRejected {
                            ticket: pending.ticket,
                            group,
                            reason: rejected.reason,
                        });
                    }
                }
            }
            if pending.batch.messages.is_empty() {
                progress
                    .completed
                    .push(self.finish(pending, IngressEnd::Drained));
            } else {
                self.pending.push_back(pending);
            }
        }
        Ok(progress)
    }
    pub fn close(&mut self) {
        self.closed = true;
    }
    pub fn is_drained(&self) -> bool {
        self.pending.is_empty()
    }
    pub fn abort(&mut self) -> Vec<IngressCompletion> {
        self.closed = true;
        let mut completed = Vec::new();
        while let Some(pending) = self.pending.pop_front() {
            completed.push(self.finish(pending, IngressEnd::Cancelled));
        }
        completed
    }
}
