// SPDX-License-Identifier: RPL-1.5
// Copyright (c) 2026 Karl Livesey
//! Fair bounded actual adjacent-hop progress; source failures stay local.
use super::*;
impl<S: SecureSession, W: WireCodec> RelayForwarder<S, W> {
    pub fn poll(
        &mut self,
        now: MonoTime,
        visits: usize,
        budget: TransportPollBudget,
    ) -> Result<RelayForwarderProgress, RelayNetworkError> {
        self.poll_with_admission(now, visits, budget, |_, _| true)
    }
    /// Host scheduling/backpressure over an actual complete input. False keeps
    /// its exact one-slot FIFO ownership; it grants no source/quorum authority.
    pub fn poll_with_admission(
        &mut self,
        now: MonoTime,
        visits: usize,
        budget: TransportPollBudget,
        mut admission: impl FnMut(NodeId, &RelayReceived) -> bool,
    ) -> Result<RelayForwarderProgress, RelayNetworkError> {
        budget
            .validate()
            .map_err(|_| RelayNetworkError::InvalidLimits)?;
        if visits > 4096 {
            return Err(RelayNetworkError::InvalidLimits);
        }
        if self.now.is_some_and(|t| now < t) {
            return Err(RelayNetworkError::TimeWentBack);
        }
        self.now = Some(now);
        let mut out = RelayForwarderProgress::default();
        self.flush_closes()?;
        let nodes: Vec<_> = self.peers.keys().copied().collect();
        for _ in 0..visits.min(nodes.len()) {
            let node = nodes[self.cursor % nodes.len()];
            self.cursor = (self.cursor + 1) % nodes.len();
            if let Err(e) = self.peers.get_mut(&node).unwrap().hop.poll(now, budget) {
                self.close_source(node, &mut out);
                out.source_failures.push((node, e));
            }
            self.complete(node, &mut out)?;
            let allowed = self.closed
                || self.peers[&node]
                    .hop
                    .received()
                    .is_none_or(|r| admission(node, r));
            if allowed {
                if let Err(e) = self.intake(node, &mut out) {
                    self.peers.get_mut(&node).unwrap().hop.abort();
                    self.close_source(node, &mut out);
                    out.source_failures.push((node, e));
                }
            } else {
                out.blocked += 1;
            }
            self.submit(node, &mut out)?;
        }
        if self.closed {
            self.sources.clear();
            self.pending_closes.clear();
            self.ledger.clear();
            self.retained = self.peer_metadata
                + self
                    .peers
                    .values()
                    .flat_map(|p| {
                        p.queue
                            .iter()
                            .map(|w| w.cost)
                            .chain(p.active.iter().map(|w| w.cost))
                    })
                    .sum::<usize>();
            for p in self.peers.values_mut() {
                if p.queue.is_empty() && p.active.is_none() {
                    p.hop.close();
                }
            }
        }
        Ok(out)
    }
    fn complete(
        &mut self,
        node: NodeId,
        out: &mut RelayForwarderProgress,
    ) -> Result<(), RelayNetworkError> {
        let peer = self.peers.get_mut(&node).unwrap();
        let Some(done) = peer.hop.take_send() else {
            return Ok(());
        };
        let active = peer
            .active
            .take()
            .ok_or(RelayNetworkError::ProviderViolation)?;
        if done.ticket != active.ticket {
            return Err(RelayNetworkError::ProviderViolation);
        }
        self.retained -= active.cost;
        self.queued -= 1;
        out.released.extend(active.input);
        out.completed += 1;
        out.completions.push(RelayForwardingCompletion {
            input: active.input,
            local: done,
        });
        Ok(())
    }
    fn submit(
        &mut self,
        node: NodeId,
        out: &mut RelayForwarderProgress,
    ) -> Result<(), RelayNetworkError> {
        let peer = self.peers.get_mut(&node).unwrap();
        if !peer.hop.can_submit() {
            return Ok(());
        }
        let Some(work) = peer.queue.pop_front() else {
            return Ok(());
        };
        let kind = work.frame.kind();
        let destination = work.frame.original_ticket().map(|t| t.peer);
        match peer.hop.submit(work.frame) {
            Ok(ticket) => {
                peer.active = Some(ActiveForward {
                    ticket,
                    input: work.input,
                    cost: work.cost,
                    kind,
                    destination,
                });
                out.forwarded += 1;
            }
            Err(rejected) => {
                peer.queue.push_front(ForwardWork {
                    frame: *rejected.frame,
                    input: work.input,
                    cost: work.cost,
                });
                return Err(rejected.reason);
            }
        }
        Ok(())
    }
    fn close_source(&mut self, node: NodeId, out: &mut RelayForwarderProgress) {
        let peer = self.peers.get_mut(&node).unwrap();
        if peer.closed {
            return;
        }
        peer.closed = true;
        let actual = peer.hop.binding();
        if let Some(input) = peer.hop.take_received() {
            out.released.push(input.ticket());
        }
        while let Some(work) = peer.queue.pop_front() {
            self.retained -= work.cost;
            self.queued -= 1;
            out.released.extend(work.input);
        }
        let captures: Vec<_> = self
            .sources
            .values()
            .filter(|s| s.path.hops().any(|h| h == actual))
            .copied()
            .collect();
        for capture in &captures {
            self.sources.remove(&capture.source().node);
            if !self.closed {
                self.pending_closes.insert(capture.source().node, *capture);
            }
        }
        self.ledger.retain(|r| {
            let remove = r.source.path.hops().any(|h| h == actual)
                || self.plan.next_hop(r.original.ticket.peer) == Some(node);
            if remove {
                self.retained -= r.cost;
            }
            !remove
        });
        // One exact close replaces queued announcements for this source; reserve
        // was included before accepting data. No catalog-derived observation.
        for p in self.peers.values_mut() {
            let mut keep = VecDeque::new();
            while let Some(work) = p.queue.pop_front() {
                if work
                    .frame
                    .source_capture()
                    .is_some_and(|s| captures.contains(&s))
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
