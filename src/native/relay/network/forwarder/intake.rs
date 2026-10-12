// SPDX-License-Identifier: RPL-1.5
// Copyright (c) 2026 Karl Livesey
//! Preflight actual received originals before bounded adjacent-hop forwarding.
use super::*;
mod retirement;
impl<S: SecureSession, W: WireCodec> RelayForwarder<S, W> {
    pub(super) fn intake(
        &mut self,
        node: NodeId,
        out: &mut RelayForwarderProgress,
    ) -> Result<(), RelayNetworkError> {
        let Some(received) = self.peers[&node].hop.received() else {
            return Ok(());
        };
        if self.closed {
            self.discard_input(node, out);
            return Ok(());
        }
        let ticket = received.ticket();
        match &received.frame.body {
            FrameBody::Opened(_) | FrameBody::Closed(_) => self.source_control(node, out),
            FrameBody::Retire(_) | FrameBody::Retired(_) => self.retirement(node, out),
            FrameBody::Output(original) => {
                // This source was captured while Ready and remains owned for
                // the exact received channel, even after clean peer closure.
                let source = self
                    .sources
                    .get(&received.connection().peer.node)
                    .filter(|s| s.binding == received.connection())
                    .copied()
                    .ok_or(RelayNetworkError::WrongSource)?;
                self.prepare_output(node, *original.clone(), ticket, source, out)
            }
            FrameBody::Forwarded(f) => {
                let source = f.source.extended(received.connection())?;
                self.prepare_output(node, f.output.clone(), ticket, source, out)
            }
        }
    }
    fn prepare_output(
        &mut self,
        node: NodeId,
        original: OutputFrame,
        ticket: RelayReceiveTicket,
        source: RelaySourceCapture,
        out: &mut RelayForwarderProgress,
    ) -> Result<(), RelayNetworkError> {
        if self.sources.get(&source.source().node) != Some(&source) {
            self.discard_input(node, out);
            return Ok(());
        }
        let target = self
            .plan
            .source(original.ticket.peer)
            .ok_or(RelayNetworkError::WrongSource)?;
        let destination = self
            .plan
            .next_hop(target.node)
            .ok_or(RelayNetworkError::WrongSource)?;
        let messages = decode_output(&self.codec, &original)?;
        let existing = self
            .ledger
            .iter()
            .position(|r| r.original.ticket == original.ticket);
        if existing.is_some_and(|i| !self.ledger[i].same_original(&original)) {
            return Err(RelayNetworkError::InvalidFrame);
        }
        let new_record = if existing.is_none() {
            RequestRecord::capture(&original, source, target, &messages)?
        } else {
            None
        };
        let record_cost = new_record.as_ref().map_or(0, |r| r.cost);
        let returns = messages
            .iter()
            .zip(&original.kinds)
            .map(|(m, k)| {
                if *k != RecordKind::Reply {
                    return None;
                }
                self.ledger
                    .iter()
                    .find(|r| r.scopes.iter().any(|s| s.matches(source, m)))
                    .map(|r| r.original.ticket)
            })
            .collect();
        let frame = RelayFrame {
            body: FrameBody::Forwarded(Box::new(ForwardFrame {
                source,
                output: original,
                returns,
            })),
        };
        let cost = frame.retained_bytes();
        if !self.can_admit(target, new_record.is_some(), cost, record_cost)
            || self.peers.get(&destination).is_none_or(|p| p.closed)
        {
            self.discard_input(node, out);
            return Ok(());
        }
        if let Some(r) = new_record {
            self.retained += r.cost;
            self.ledger.push(r);
        }
        if let Some(i) = existing {
            self.ledger[i].source = source;
        }
        let received = self
            .peers
            .get_mut(&node)
            .unwrap()
            .hop
            .take_received()
            .unwrap();
        let original = match received.frame.body {
            FrameBody::Output(o) => *o,
            FrameBody::Forwarded(f) => f.output,
            _ => unreachable!(),
        };
        let FrameBody::Forwarded(mut f) = frame.body else {
            unreachable!()
        };
        f.output = original;
        let frame = RelayFrame {
            body: FrameBody::Forwarded(f),
        };
        let cost = frame.retained_bytes();
        self.retained += cost;
        self.queued += 1;
        self.peers
            .get_mut(&destination)
            .unwrap()
            .queue
            .push_back(ForwardWork {
                frame,
                input: Some(ticket),
                cost,
            });
        out.received += 1;
        Ok(())
    }
    fn can_admit(
        &self,
        target: PeerIdentity,
        new_record: bool,
        cost: usize,
        record_cost: usize,
    ) -> bool {
        let count = self.plan.sources().count();
        let per_destination = self
            .limits
            .queued_frames
            .saturating_sub(self.limits.control_frames)
            / count;
        let queued = self
            .peers
            .values()
            .flat_map(|p| p.queue.iter())
            .filter(|w| {
                w.frame
                    .original_ticket()
                    .is_some_and(|t| t.peer == target.node)
            });
        let (slots, bytes) = queued.fold((0, 0), |(n, b), w| (n + 1, b + w.cost));
        let active = self
            .peers
            .values()
            .filter_map(|p| p.active.as_ref())
            .filter(|a| a.destination == Some(target.node));
        let (slots, bytes) = active.fold((slots, bytes), |(n, b), a| (n + 1, b + a.cost));
        let ledger_full = new_record
            && self
                .ledger
                .iter()
                .filter(|r| r.original.ticket.peer == target.node)
                .count()
                >= (self.limits.request_slots / count).max(1);
        !(self.queued >= self.limits.queued_frames - self.limits.control_frames
            || slots >= per_destination.max(1)
            || bytes.saturating_add(cost)
                > self
                    .limits
                    .retained_bytes
                    .saturating_sub(self.control_reserve())
                    / count
            || ledger_full
            || self.ledger.len() + usize::from(new_record) > self.limits.request_slots
            || self
                .retained
                .saturating_add(cost)
                .saturating_add(record_cost)
                > self
                    .limits
                    .retained_bytes
                    .saturating_sub(self.control_reserve()))
    }
    fn control_reserve(&self) -> usize {
        self.limits.control_frames
            * (std::mem::size_of::<RelayFrame>() + std::mem::size_of::<RetiredFrame>())
    }
    pub(super) fn discard_input(&mut self, node: NodeId, out: &mut RelayForwarderProgress) {
        let input = self
            .peers
            .get_mut(&node)
            .unwrap()
            .hop
            .take_received()
            .unwrap();
        out.released.push(input.ticket());
        out.received += 1;
        out.blocked += 1;
        out.discarded += 1;
    }
    fn retire(&mut self, ticket: SendTicket, out: &mut RelayForwarderProgress) {
        if let Some(i) = self.ledger.iter().position(|r| r.original.ticket == ticket) {
            self.retained -= self.ledger.remove(i).cost;
            out.retired += 1;
        }
        for p in self.peers.values_mut() {
            let mut keep = VecDeque::new();
            while let Some(work) = p.queue.pop_front() {
                if work.frame.kind() != RelayFrameKind::Retire
                    && work.frame.original_ticket() == Some(ticket)
                {
                    self.retained -= work.cost;
                    self.queued -= 1;
                    out.released.extend(work.input);
                } else {
                    keep.push_back(work);
                }
            }
            p.queue = keep;
        }
    }
}
