// SPDX-License-Identifier: RPL-1.5
// Copyright (c) 2026 Karl Livesey
//! Bounded holder of original authorized sends, including their opaque provenance.
use voteboat::{identity::*, outbound::*, raft::Message, runtime::*};
struct Record {
    ticket: SendTicket,
    class: MessageClass,
    cost: OutboundUsage,
    queued: Option<AuthorizedLogicalSend>,
    proof: Option<LogicalSendProvenance>,
}
pub struct Outbound {
    binding: OutboundBinding,
    limits: OutboundLimits,
    records: Vec<Record>,
    sequence: u64,
    closed: bool,
}
impl Outbound {
    pub fn new(binding: OutboundBinding) -> Self {
        Self {
            binding,
            limits: OutboundLimits::default(),
            records: Vec::new(),
            sequence: 0,
            closed: false,
        }
    }
    fn classes(&self, peer: Option<NodeId>) -> [OutboundUsage; 3] {
        let mut usage = [OutboundUsage::default(); 3];
        for r in self
            .records
            .iter()
            .filter(|r| peer.is_none_or(|p| r.ticket.peer == p))
        {
            let slot = match r.class {
                MessageClass::Control => 0,
                MessageClass::Data => 1,
                MessageClass::Background => 2,
            };
            usage[slot].batches += r.cost.batches;
            usage[slot].messages += r.cost.messages;
            usage[slot].bytes += r.cost.bytes;
        }
        usage
    }
    fn admit(
        &self,
        send: &AuthorizedLogicalSend,
    ) -> Result<(MessageClass, OutboundUsage, u64), OutboundError> {
        if self.closed {
            return Err(OutboundError::Closed);
        }
        if self.records.len() == 8 {
            return Err(OutboundError::Overloaded);
        }
        let (peer, class, mut cost) = batch_cost(
            self.binding,
            std::slice::from_ref(send.message()),
            1,
            self.limits,
        )?;
        // Reserve the selected command envelope for encoded bytes, the typed
        // original capsule and correlation metadata retained by its proof.
        cost.bytes += 3 * super::setup::limits().max_command_bytes
            + std::mem::size_of::<AuthorizedLogicalSend>();
        if !self.limits.node.admits(self.classes(None), class, cost)
            || !self
                .limits
                .peer
                .admits(self.classes(Some(peer)), class, cost)
        {
            return Err(OutboundError::Overloaded);
        }
        Ok((
            class,
            cost,
            self.sequence
                .checked_add(1)
                .ok_or(OutboundError::Exhausted)?,
        ))
    }
    pub fn proof(&self, ticket: SendTicket) -> &LogicalSendProvenance {
        self.records
            .iter()
            .find(|r| r.ticket == ticket)
            .unwrap()
            .proof
            .as_ref()
            .unwrap()
    }
}
impl LogicalOutboundQueue for Outbound {
    fn submit_authorized(
        &mut self,
        send: AuthorizedLogicalSend,
    ) -> Result<SendTicket, LogicalOutboundRejected> {
        let (class, cost, sequence) = match self.admit(&send) {
            Ok(v) => v,
            Err(reason) => {
                return Err(LogicalOutboundRejected {
                    reason,
                    send: Box::new(send),
                })
            }
        };
        let ticket = SendTicket {
            binding: self.binding,
            peer: send.message().to,
            sequence,
        };
        self.sequence = sequence;
        self.records.push(Record {
            ticket,
            class,
            cost,
            queued: Some(send),
            proof: None,
        });
        Ok(ticket)
    }
}
fn total(classes: [OutboundUsage; 3]) -> OutboundUsage {
    OutboundUsage {
        batches: classes.iter().map(|c| c.batches).sum(),
        messages: classes.iter().map(|c| c.messages).sum(),
        bytes: classes.iter().map(|c| c.bytes).sum(),
    }
}
impl OutboundQueue for Outbound {
    fn binding(&self) -> OutboundBinding {
        self.binding
    }
    fn limits(&self) -> OutboundLimits {
        self.limits
    }
    fn usage(&self) -> OutboundUsage {
        total(self.classes(None))
    }
    fn peer_usage(&self, peer: NodeId) -> OutboundUsage {
        total(self.classes(Some(peer)))
    }
    fn submit(&mut self, messages: Vec<Message>) -> Result<SendTicket, SendRejected> {
        Err(SendRejected {
            reason: OutboundError::WrongBinding,
            messages,
        })
    }
    fn poll(&mut self, limit: usize) -> Vec<OutboundBatch> {
        let mut batches = Vec::new();
        for record in self
            .records
            .iter_mut()
            .filter(|r| r.queued.is_some())
            .take(limit)
        {
            let (message, proof) = record.queued.take().unwrap().into_parts();
            record.proof = Some(proof);
            batches.push(OutboundBatch {
                ticket: record.ticket,
                messages: vec![message],
                admission: None,
            });
        }
        batches
    }
    fn complete(
        &mut self,
        batch: OutboundBatch,
        result: LocalSendResult,
    ) -> Result<LocalSendCompletion, CompletionRejected> {
        let index = self.records.iter().position(|r| r.ticket == batch.ticket);
        let reason = if batch.ticket.binding != self.binding {
            Some(OutboundError::WrongBinding)
        } else {
            match index {
                None => Some(OutboundError::UnknownTicket),
                Some(i) if self.records[i].queued.is_some() => Some(OutboundError::NotDispatched),
                Some(_) => None,
            }
        };
        if let Some(reason) = reason {
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
