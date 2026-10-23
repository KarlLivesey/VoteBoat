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
struct Retained {
    send: Option<AuthorizedLogicalSend>,
    ticket: SendTicket,
    effect: EffectTicket,
    operation: OperationId,
    class: MessageClass,
    cost: OutboundUsage,
}
/// Proof-bearing queue. Raw poll/complete deliberately cannot export or release logical work.
pub struct LogicalNetworkQueue {
    authorization: RuntimeAuthorization,
    binding: OutboundBinding,
    limits: OutboundLimits,
    network: LogicalNetworkLimits,
    retained: BTreeMap<u64, Retained>,
    order: VecDeque<u64>,
    sequence: u64,
    closed: bool,
}
pub struct LogicalNetworkBatch {
    pub(super) ticket: SendTicket,
    pub(super) send: AuthorizedLogicalSend,
}
impl LogicalNetworkBatch {
    pub fn ticket(&self) -> SendTicket {
        self.ticket
    }
    pub fn send(&self) -> &AuthorizedLogicalSend {
        &self.send
    }
}
pub struct LogicalNetworkCompletion {
    pub(super) batch: LogicalNetworkBatch,
    pub(super) result: LocalSendResult,
}
impl LogicalNetworkCompletion {
    pub fn ticket(&self) -> SendTicket {
        self.batch.ticket
    }
    pub fn result(&self) -> LocalSendResult {
        self.result
    }
}
pub struct LogicalCompletionRejected {
    pub reason: OutboundError,
    pub completion: Box<LogicalNetworkCompletion>,
}
pub struct LogicalBatchRejected {
    pub reason: OutboundError,
    pub batch: Box<LogicalNetworkBatch>,
}
impl LogicalNetworkQueue {
    /// `generation` must be fresh for every queue attached to this owner. An
    /// exported original completion may outlive its queue; reusing a generation
    /// and sequence would violate the caller's existing outbound binding contract.
    pub fn new(
        authorization: RuntimeAuthorization,
        generation: OutboundGeneration,
        limits: OutboundLimits,
        network: LogicalNetworkLimits,
    ) -> Result<Self, LogicalNetworkError> {
        let l = authorization
            .logical()
            .ok_or(LogicalNetworkError::WrongBinding)?;
        let limits = limits.validate().map_err(LogicalNetworkError::Outbound)?;
        let network = network.validate()?;
        let binding = OutboundBinding {
            node: l.mapping.voter,
            store: authorization.owner().store,
            generation,
        };
        Ok(Self {
            authorization,
            binding,
            limits,
            network,
            retained: BTreeMap::new(),
            order: VecDeque::new(),
            sequence: 0,
            closed: false,
        })
    }
    pub fn authorization(&self) -> RuntimeAuthorization {
        self.authorization
    }
    pub(crate) fn network_limits(&self) -> LogicalNetworkLimits {
        self.network
    }
    /// Cancel an original dispatched batch still owned by the caller. Accepted
    /// sessions retain that value until terminal completion, preventing rollback
    /// of external I/O. No scalar ticket or positive Sent result releases credits.
    pub fn cancel_dispatched(
        &mut self,
        batch: LogicalNetworkBatch,
    ) -> Result<LocalSendCompletion, LogicalBatchRejected> {
        let valid = self.retained.get(&batch.ticket.sequence).is_some_and(|r| {
            r.ticket == batch.ticket
                && r.send.is_none()
                && r.effect == batch.send.effect()
                && r.operation == batch.send.provenance().receipt().operation
                && batch.send.authorization() == self.authorization
        });
        if !valid {
            return Err(LogicalBatchRejected {
                reason: OutboundError::UnknownTicket,
                batch: Box::new(batch),
            });
        }
        self.retained.remove(&batch.ticket.sequence);
        Ok(LocalSendCompletion {
            ticket: batch.ticket,
            result: LocalSendResult::Cancelled,
        })
    }
    /// Dispatch transfers original proof ownership, retaining its queue credits.
    pub fn poll_logical(&mut self, limit: usize) -> Vec<LogicalNetworkBatch> {
        let mut out = Vec::new();
        for _ in 0..limit.min(self.order.len()) {
            let key = self.order.pop_front().unwrap();
            let r = self.retained.get_mut(&key).unwrap();
            out.push(LogicalNetworkBatch {
                ticket: r.ticket,
                send: r.send.take().unwrap(),
            });
        }
        out
    }
    pub fn complete_logical(
        &mut self,
        completion: LogicalNetworkCompletion,
    ) -> Result<LocalSendCompletion, LogicalCompletionRejected> {
        let ticket = completion.batch.ticket;
        let valid = self.retained.get(&ticket.sequence).is_some_and(|r| {
            r.ticket == ticket
                && r.send.is_none()
                && r.effect == completion.batch.send.effect()
                && r.operation == completion.batch.send.provenance().receipt().operation
                && completion.batch.send.authorization() == self.authorization
        });
        if !valid {
            return Err(LogicalCompletionRejected {
                reason: OutboundError::UnknownTicket,
                completion: Box::new(completion),
            });
        }
        self.retained.remove(&ticket.sequence);
        Ok(LocalSendCompletion {
            ticket,
            result: completion.result,
        })
    }
    fn usage_by_class(&self, peer: Option<NodeId>) -> [OutboundUsage; 3] {
        let mut out = [OutboundUsage::default(); 3];
        for r in self
            .retained
            .values()
            .filter(|r| peer.is_none_or(|p| r.ticket.peer == p))
        {
            out[r.class.index()].add(r.cost);
        }
        out
    }
    fn cost(
        &self,
        send: &AuthorizedLogicalSend,
    ) -> Result<(MessageClass, OutboundUsage), OutboundError> {
        let m = send.message();
        let (class, bytes) = message_cost(m, self.limits.batch_bytes)?;
        let proof = send.provenance();
        if proof.command_bytes().len() > self.network.frame_bytes - 512 {
            return Err(OutboundError::BatchTooLarge);
        }
        let proof_bytes = proof.retained_bytes(self.limits.batch_bytes)?;
        let bytes = bytes
            .checked_add(proof_bytes)
            .and_then(|n| n.checked_add(size_of::<AuthorizedLogicalSend>() + 512))
            .ok_or(OutboundError::BatchTooLarge)?;
        if bytes > self.limits.batch_bytes {
            return Err(OutboundError::BatchTooLarge);
        }
        Ok((
            class,
            OutboundUsage {
                batches: 1,
                messages: 1,
                bytes,
            },
        ))
    }
}
impl LogicalOutboundQueue for LogicalNetworkQueue {
    fn submit_authorized(
        &mut self,
        send: AuthorizedLogicalSend,
    ) -> Result<SendTicket, LogicalOutboundRejected> {
        let result = (|| {
            if self.closed {
                return Err(OutboundError::Closed);
            }
            let p = send.provenance();
            if send.authorization() != self.authorization
                || send.effect().visit.owner != self.authorization.owner()
                || p.capsule().origin != LogicalCapsuleOrigin::Protocol(send.effect())
                || !matches!(&p.capsule().body,
                    LogicalCapsuleBody::Protocol(message) if message.as_ref() == send.message())
                || p.receipt().outcome != LogicalOutcome::CapsuleAuthorized
                || send.message().from != self.binding.node
                || send.message().sender != self.binding.store
            {
                return Err(OutboundError::WrongBinding);
            }
            if send.message().to == self.binding.node {
                return Err(OutboundError::WrongPeer);
            }
            let (class, cost) = self.cost(&send)?;
            let peer = send.message().to;
            if !self
                .limits
                .node
                .admits(self.usage_by_class(None), class, cost)
                || !self
                    .limits
                    .peer
                    .admits(self.usage_by_class(Some(peer)), class, cost)
                || (self.peer_usage(peer).batches == 0
                    && self
                        .retained
                        .values()
                        .map(|r| r.ticket.peer)
                        .collect::<std::collections::BTreeSet<_>>()
                        .len()
                        >= self.limits.max_peers)
            {
                return Err(OutboundError::Overloaded);
            }
            let sequence = self
                .sequence
                .checked_add(1)
                .ok_or(OutboundError::Exhausted)?;
            Ok((
                SendTicket {
                    binding: self.binding,
                    peer,
                    sequence,
                },
                class,
                cost,
            ))
        })();
        let (ticket, class, cost) = match result {
            Ok(r) => r,
            Err(reason) => {
                return Err(LogicalOutboundRejected {
                    reason,
                    send: Box::new(send),
                })
            }
        };
        self.sequence = ticket.sequence;
        let r = Retained {
            ticket,
            effect: send.effect(),
            operation: send.provenance().receipt().operation,
            send: Some(send),
            class,
            cost,
        };
        self.retained.insert(ticket.sequence, r);
        self.order.push_back(ticket.sequence);
        Ok(ticket)
    }
}
impl OutboundQueue for LogicalNetworkQueue {
    fn binding(&self) -> OutboundBinding {
        self.binding
    }
    fn limits(&self) -> OutboundLimits {
        self.limits
    }
    fn usage(&self) -> OutboundUsage {
        self.retained
            .values()
            .fold(OutboundUsage::default(), |mut a, r| {
                a.add(r.cost);
                a
            })
    }
    fn peer_usage(&self, peer: NodeId) -> OutboundUsage {
        self.retained
            .values()
            .filter(|r| r.ticket.peer == peer)
            .fold(OutboundUsage::default(), |mut a, r| {
                a.add(r.cost);
                a
            })
    }
    fn submit(&mut self, messages: Vec<Message>) -> Result<SendTicket, SendRejected> {
        Err(SendRejected {
            reason: if self.closed {
                OutboundError::Closed
            } else {
                OutboundError::WrongBinding
            },
            messages,
        })
    }
    fn poll(&mut self, _limit: usize) -> Vec<OutboundBatch> {
        Vec::new()
    }
    fn complete(
        &mut self,
        batch: OutboundBatch,
        _result: LocalSendResult,
    ) -> Result<LocalSendCompletion, CompletionRejected> {
        Err(CompletionRejected {
            reason: OutboundError::WrongBinding,
            batch: Box::new(batch),
        })
    }
    fn close(&mut self) {
        self.closed = true;
    }
}
