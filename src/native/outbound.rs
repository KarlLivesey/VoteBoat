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
//! Caller-driven bounded outbound scheduling. No network I/O or worker thread.
use crate::{
    admission::*, identity::NodeId, native::admission::NativeAdmissionPolicy, outbound::*,
    raft::Message,
};
use std::collections::{BTreeMap, VecDeque};

#[derive(Default)]
struct Peer {
    usage: [OutboundUsage; 3],
    queues: [VecDeque<OutboundBatch>; 3],
    cursor: usize,
}
impl Peer {
    fn queued(&self) -> bool {
        self.queues.iter().any(|q| !q.is_empty())
    }
    fn pop(&mut self) -> OutboundBatch {
        // Two control turns, one data turn, one background turn. Empty classes
        // consume no visit. A continuous control stream cannot starve bulk work.
        const ORDER: [usize; 4] = [0, 1, 0, 2];
        for _ in 0..4 {
            let index = ORDER[self.cursor];
            self.cursor = (self.cursor + 1) % 4;
            if let Some(batch) = self.queues[index].pop_front() {
                return batch;
            }
        }
        unreachable!("ready peer has queued work")
    }
}
struct Retained {
    _admission: Option<AdmissionLease>,
    peer: NodeId,
    class: MessageClass,
    cost: OutboundUsage,
    dispatched: bool,
}
pub struct NativeOutbound<P: AdmissionPolicy = NativeAdmissionPolicy> {
    policy: P,
    binding: OutboundBinding,
    limits: OutboundLimits,
    usage: [OutboundUsage; 3],
    peers: BTreeMap<NodeId, Peer>,
    ready: VecDeque<NodeId>,
    retained: BTreeMap<u64, Retained>,
    sequence: u64,
    closed: bool,
}
impl NativeOutbound {
    pub fn new(binding: OutboundBinding, limits: OutboundLimits) -> Result<Self, OutboundError> {
        let limits = limits.validate()?;
        let policy = NativeAdmissionPolicy::new(limits.node.max)
            .map_err(|_| OutboundError::InvalidLimits)?;
        Self::with_policy(binding, limits, policy)
    }
}
impl<P: AdmissionPolicy> NativeOutbound<P> {
    pub fn with_policy(
        binding: OutboundBinding,
        limits: OutboundLimits,
        policy: P,
    ) -> Result<Self, OutboundError> {
        Ok(Self {
            policy,
            binding,
            limits: limits.validate()?,
            usage: Default::default(),
            peers: BTreeMap::new(),
            ready: VecDeque::new(),
            retained: BTreeMap::new(),
            sequence: 0,
            closed: false,
        })
    }
}
fn total(usage: [OutboundUsage; 3]) -> OutboundUsage {
    let mut v = usage[0];
    v.add(usage[1]);
    v.add(usage[2]);
    v
}
impl<P: AdmissionPolicy> OutboundQueue for NativeOutbound<P> {
    fn binding(&self) -> OutboundBinding {
        self.binding
    }
    fn limits(&self) -> OutboundLimits {
        self.limits
    }
    fn usage(&self) -> OutboundUsage {
        total(self.usage)
    }
    fn peer_usage(&self, peer: NodeId) -> OutboundUsage {
        self.peers
            .get(&peer)
            .map_or(OutboundUsage::default(), |p| total(p.usage))
    }
    fn submit(&mut self, messages: Vec<Message>) -> Result<SendTicket, SendRejected> {
        let result = (|| {
            if self.closed {
                return Err(OutboundError::Closed);
            }
            let (peer, class, cost) =
                batch_cost(self.binding, &messages, messages.capacity(), self.limits)?;
            if !self.limits.node.admits(self.usage, class, cost) {
                return Err(OutboundError::Overloaded);
            }
            let peer_usage = match self.peers.get(&peer) {
                Some(p) => p.usage,
                None if self.peers.len() == self.limits.max_peers => {
                    return Err(OutboundError::Overloaded)
                }
                None => Default::default(),
            };
            if !self.limits.peer.admits(peer_usage, class, cost) {
                return Err(OutboundError::Overloaded);
            }
            let sequence = self
                .sequence
                .checked_add(1)
                .ok_or(OutboundError::Exhausted)?;
            let admission = if class == MessageClass::Control {
                None
            } else {
                Some(
                    self.policy
                        .reserve(AdmissionRequest {
                            owner: self.binding,
                            peer,
                            class,
                            cost,
                            node_usage: total(self.usage),
                            peer_usage: total(peer_usage),
                        })
                        .map_err(|_| OutboundError::Overloaded)?,
                )
            };
            Ok((peer, class, cost, sequence, admission))
        })();
        let (peer, class, cost, sequence, admission) = match result {
            Ok(v) => v,
            Err(reason) => return Err(SendRejected { reason, messages }),
        };
        let ticket = SendTicket {
            binding: self.binding,
            peer,
            sequence,
        };
        let p = self.peers.entry(peer).or_default();
        if !p.queued() {
            self.ready.push_back(peer);
        }
        p.queues[class.index()].push_back(OutboundBatch {
            ticket,
            messages,
            admission: admission.clone(),
        });
        p.usage[class.index()].add(cost);
        self.usage[class.index()].add(cost);
        self.retained.insert(
            sequence,
            Retained {
                _admission: admission,
                peer,
                class,
                cost,
                dispatched: false,
            },
        );
        self.sequence = sequence;
        Ok(ticket)
    }
    fn poll(&mut self, limit: usize) -> Vec<OutboundBatch> {
        let mut batches = Vec::new();
        for _ in 0..limit.min(self.limits.node.max.batches) {
            let Some(peer) = self.ready.pop_front() else {
                break;
            };
            let p = self.peers.get_mut(&peer).unwrap();
            let batch = p.pop();
            self.retained
                .get_mut(&batch.ticket.sequence)
                .unwrap()
                .dispatched = true;
            if p.queued() {
                self.ready.push_back(peer);
            }
            batches.push(batch);
        }
        batches
    }
    fn complete(
        &mut self,
        batch: OutboundBatch,
        result: LocalSendResult,
    ) -> Result<LocalSendCompletion, CompletionRejected> {
        let ticket = batch.ticket;
        let error = if ticket.binding != self.binding {
            Some(OutboundError::WrongBinding)
        } else {
            match self.retained.get(&ticket.sequence) {
                None => Some(OutboundError::UnknownTicket),
                Some(r) if r.peer != ticket.peer => Some(OutboundError::WrongPeer),
                Some(r) if !r.dispatched => Some(OutboundError::NotDispatched),
                Some(_) => None,
            }
        };
        if let Some(reason) = error {
            return Err(CompletionRejected {
                reason,
                batch: Box::new(batch),
            });
        }
        let r = self.retained.remove(&ticket.sequence).unwrap();
        self.usage[r.class.index()].sub(r.cost);
        let p = self.peers.get_mut(&r.peer).unwrap();
        p.usage[r.class.index()].sub(r.cost);
        if total(p.usage).batches == 0 {
            self.peers.remove(&r.peer);
        }
        Ok(LocalSendCompletion { ticket, result })
    }
    fn close(&mut self) {
        self.closed = true;
    }
}
