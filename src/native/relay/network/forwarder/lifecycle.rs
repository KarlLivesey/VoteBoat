// SPDX-License-Identifier: RPL-1.5
// Copyright (c) 2026 Karl Livesey
//! Actual adjacent-session attachment and explicit helper cancellation.
use super::*;
pub struct RelayForwarderHopRejected<S: SecureSession> {
    pub reason: RelayNetworkError,
    pub hop: Box<RelayHop<S>>,
}
impl<S: SecureSession, W: WireCodec> RelayForwarder<S, W> {
    pub fn attach_source_hop(
        &mut self,
        hop: RelayHop<S>,
    ) -> Result<Option<RelayHop<S>>, RelayForwarderHopRejected<S>> {
        let binding = match self.check_attachment(&hop) {
            Ok(b) => b,
            Err(reason) => {
                return Err(RelayForwarderHopRejected {
                    reason,
                    hop: Box::new(hop),
                })
            }
        };
        let capture = hop.captured_source();
        if !self.peers.contains_key(&binding.peer.node) {
            let metadata = peer_metadata_cost::<S>();
            self.retained += metadata;
            self.peer_metadata += metadata;
        }
        let previous = self.peers.insert(
            binding.peer.node,
            ForwardPeer {
                hop,
                capture,
                closed: false,
                queue: VecDeque::new(),
                active: None,
            },
        );
        if let Some(c) = capture {
            self.sources.insert(c.source().node, c);
            self.pending_closes.remove(&c.source().node);
            self.announce(c, true)
                .expect("checked attachment control credits");
        }
        let captures: Vec<_> = self
            .sources
            .values()
            .copied()
            .filter(|s| {
                s.source().node != binding.peer.node
                    && !s.path.hops().any(|h| {
                        h.local.node == binding.peer.node || h.peer.node == binding.peer.node
                    })
            })
            .collect();
        for source in captures {
            let frame = RelayFrame {
                body: FrameBody::Opened(Box::new(source)),
            };
            let cost = frame.retained_bytes();
            self.retained += cost;
            self.queued += 1;
            self.peers
                .get_mut(&binding.peer.node)
                .unwrap()
                .queue
                .push_back(ForwardWork {
                    frame,
                    input: None,
                    cost,
                });
        }
        Ok(previous.map(|p| p.hop))
    }
    fn check_attachment(&self, hop: &RelayHop<S>) -> Result<SessionBinding, RelayNetworkError> {
        if self.closed {
            return Err(RelayNetworkError::Closed);
        }
        let binding = hop
            .authenticated_binding()
            .ok_or(RelayNetworkError::WrongSource)?;
        if !hop.helper_matches(&self.plan, self.limits) {
            return Err(RelayNetworkError::WrongBinding);
        }
        let old = self.peers.get(&binding.peer.node);
        if old.is_some_and(|p| {
            !p.closed || p.active.is_some() || !p.queue.is_empty() || !p.hop.is_drained()
        }) {
            return Err(RelayNetworkError::Overloaded);
        }
        if old.is_some_and(|p| p.hop.binding() == binding) {
            return Err(RelayNetworkError::WrongBinding);
        }
        let count = self.peers.len() + usize::from(old.is_none());
        let controls = self.plan.sources().count() * count.saturating_sub(1);
        if count > self.limits.hop_slots
            || controls > self.limits.control_frames
            || parts_capture_reserve(&self.plan)
                + count * peer_metadata_cost::<S>()
                + controls
                    * (std::mem::size_of::<RelayFrame>()
                        + std::mem::size_of::<RelaySourceCapture>())
                > self.limits.retained_bytes
        {
            return Err(RelayNetworkError::InvalidLimits);
        }
        let new_capture = hop.captured_source();
        let outgoing = new_capture.map_or(0, |capture| {
            self.peers
                .iter()
                .filter(|(n, p)| {
                    !p.closed
                        && **n != binding.peer.node
                        && !capture
                            .path
                            .hops()
                            .any(|h| h.local.node == **n || h.peer.node == **n)
                })
                .count()
        });
        let incoming = self
            .sources
            .values()
            .filter(|s| {
                s.source().node != binding.peer.node
                    && !s.path.hops().any(|h| {
                        h.local.node == binding.peer.node || h.peer.node == binding.peer.node
                    })
            })
            .count();
        let additions = outgoing + incoming;
        let cost = std::mem::size_of::<RelayFrame>() + std::mem::size_of::<RelaySourceCapture>();
        let metadata = usize::from(old.is_none()) * peer_metadata_cost::<S>();
        if self.control_usage() + additions > self.limits.control_frames
            || self.queued + additions > self.limits.queued_frames
            || self
                .retained
                .saturating_add(additions * cost)
                .saturating_add(metadata)
                > self.limits.retained_bytes
        {
            return Err(RelayNetworkError::Overloaded);
        }
        Ok(binding)
    }
    /// Explicit unknown cancellation preserves accepted local completions.
    pub fn abort(&mut self) -> RelayForwarderProgress {
        self.closed = true;
        self.sources.clear();
        self.pending_closes.clear();
        let mut out = RelayForwarderProgress::default();
        for p in self.peers.values_mut() {
            p.closed = true;
            p.hop.abort();
            if let Some(received) = p.hop.take_received() {
                out.released.push(received.ticket());
                out.discarded += 1;
            }
            while let Some(work) = p.queue.pop_front() {
                self.retained -= work.cost;
                self.queued -= 1;
                out.released.extend(work.input);
            }
        }
        for r in self.ledger.drain(..) {
            self.retained -= r.cost;
        }
        out
    }
}
