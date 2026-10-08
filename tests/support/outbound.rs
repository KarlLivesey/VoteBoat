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
//! Independent FIFO host replacement, using only the public admission contract.
use std::collections::BTreeSet;
use voteboat::{identity::NodeId, outbound::*, raft::Message};
struct Record {
    ticket: SendTicket,
    class: MessageClass,
    cost: OutboundUsage,
    queued: Option<Vec<Message>>,
}
pub struct HostOutbound {
    binding: OutboundBinding,
    limits: OutboundLimits,
    records: Vec<Record>,
    sequence: u64,
    closed: bool,
}
fn class_index(class: MessageClass) -> usize {
    match class {
        MessageClass::Control => 0,
        MessageClass::Data => 1,
        MessageClass::Background => 2,
    }
}
fn sum(records: impl Iterator<Item = (MessageClass, OutboundUsage)>) -> [OutboundUsage; 3] {
    let mut result = [OutboundUsage::default(); 3];
    for (class, cost) in records {
        let v = &mut result[class_index(class)];
        v.batches += cost.batches;
        v.messages += cost.messages;
        v.bytes += cost.bytes;
    }
    result
}
fn total(usage: [OutboundUsage; 3]) -> OutboundUsage {
    OutboundUsage {
        batches: usage.iter().map(|u| u.batches).sum(),
        messages: usage.iter().map(|u| u.messages).sum(),
        bytes: usage.iter().map(|u| u.bytes).sum(),
    }
}
impl HostOutbound {
    pub fn new(binding: OutboundBinding, limits: OutboundLimits) -> Result<Self, OutboundError> {
        Ok(Self {
            binding,
            limits: limits.validate()?,
            records: vec![],
            sequence: 0,
            closed: false,
        })
    }
}
impl OutboundQueue for HostOutbound {
    fn binding(&self) -> OutboundBinding {
        self.binding
    }
    fn limits(&self) -> OutboundLimits {
        self.limits
    }
    fn usage(&self) -> OutboundUsage {
        total(sum(self.records.iter().map(|r| (r.class, r.cost))))
    }
    fn peer_usage(&self, peer: NodeId) -> OutboundUsage {
        total(sum(self
            .records
            .iter()
            .filter(|r| r.ticket.peer == peer)
            .map(|r| (r.class, r.cost))))
    }
    fn submit(&mut self, messages: Vec<Message>) -> Result<SendTicket, SendRejected> {
        let result = (|| {
            if self.closed {
                return Err(OutboundError::Closed);
            }
            let (peer, class, cost) =
                batch_cost(self.binding, &messages, messages.capacity(), self.limits)?;
            let peers = self
                .records
                .iter()
                .map(|r| r.ticket.peer)
                .collect::<BTreeSet<_>>();
            if (!peers.contains(&peer) && peers.len() == self.limits.max_peers)
                || !self.limits.node.admits(
                    sum(self.records.iter().map(|r| (r.class, r.cost))),
                    class,
                    cost,
                )
                || !self.limits.peer.admits(
                    sum(self
                        .records
                        .iter()
                        .filter(|r| r.ticket.peer == peer)
                        .map(|r| (r.class, r.cost))),
                    class,
                    cost,
                )
            {
                return Err(OutboundError::Overloaded);
            }
            let sequence = self
                .sequence
                .checked_add(1)
                .ok_or(OutboundError::Exhausted)?;
            Ok((peer, class, cost, sequence))
        })();
        let (peer, class, cost, sequence) = match result {
            Ok(v) => v,
            Err(reason) => return Err(SendRejected { reason, messages }),
        };
        let ticket = SendTicket {
            binding: self.binding,
            peer,
            sequence,
        };
        self.records.push(Record {
            ticket,
            class,
            cost,
            queued: Some(messages),
        });
        self.sequence = sequence;
        Ok(ticket)
    }
    fn poll(&mut self, limit: usize) -> Vec<OutboundBatch> {
        self.records
            .iter_mut()
            .filter_map(|r| {
                r.queued.take().map(|messages| OutboundBatch {
                    ticket: r.ticket,
                    messages,
                })
            })
            .take(limit)
            .collect()
    }
    fn complete(
        &mut self,
        batch: OutboundBatch,
        result: LocalSendResult,
    ) -> Result<LocalSendCompletion, CompletionRejected> {
        let index = self.records.iter().position(|r| r.ticket == batch.ticket);
        let error = if batch.ticket.binding != self.binding {
            Some(OutboundError::WrongBinding)
        } else {
            match index {
                None => Some(OutboundError::UnknownTicket),
                Some(i) if self.records[i].queued.is_some() => Some(OutboundError::NotDispatched),
                Some(_) => None,
            }
        };
        if let Some(reason) = error {
            return Err(CompletionRejected {
                reason,
                batch: Box::new(batch),
            });
        }
        self.records.remove(index.unwrap());
        Ok(LocalSendCompletion {
            ticket: batch.ticket,
            result,
        })
    }
    fn close(&mut self) {
        self.closed = true;
    }
}
