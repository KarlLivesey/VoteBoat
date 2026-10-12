// SPDX-License-Identifier: RPL-1.5
// Copyright (c) 2026 Karl Livesey
//! Exact learned origin paths, with bounded deduplicated control materialization.
use super::*;
impl<S: SecureSession, W: WireCodec> RelayForwarder<S, W> {
    pub(super) fn control_usage(&self) -> usize {
        self.peers
            .values()
            .map(|p| {
                p.queue
                    .iter()
                    .filter(|w| is_control(w.frame.kind()))
                    .count()
                    + usize::from(p.active.as_ref().is_some_and(|a| is_control(a.kind)))
            })
            .sum()
    }
    pub(super) fn announce(
        &mut self,
        capture: RelaySourceCapture,
        opened: bool,
    ) -> Result<(), RelayNetworkError> {
        let recipients: Vec<_> = self
            .peers
            .iter()
            .filter(|(n, p)| {
                !p.closed
                    && **n != capture.source().node
                    && !capture
                        .path
                        .hops()
                        .any(|h| h.local.node == **n || h.peer.node == **n)
            })
            .map(|(n, _)| *n)
            .collect();
        let stale = |w: &ForwardWork| {
            matches!(
                w.frame.kind(),
                RelayFrameKind::SourceOpened | RelayFrameKind::SourceClosed
            ) && w
                .frame
                .source_capture()
                .is_some_and(|s| s.source().node == capture.source().node)
        };
        let replaced: Vec<_> = recipients
            .iter()
            .flat_map(|n| {
                self.peers[n]
                    .queue
                    .iter()
                    .filter(|w| stale(w))
                    .map(|w| w.cost)
            })
            .collect();
        let cost = std::mem::size_of::<RelayFrame>() + std::mem::size_of::<RelaySourceCapture>();
        if self.control_usage().saturating_sub(replaced.len()) + recipients.len()
            > self.limits.control_frames
            || self.queued.saturating_sub(replaced.len()) + recipients.len()
                > self.limits.queued_frames
            || self
                .retained
                .saturating_sub(replaced.iter().sum())
                .saturating_add(cost * recipients.len())
                > self.limits.retained_bytes
        {
            return Err(RelayNetworkError::Overloaded);
        }
        let frame = RelayFrame {
            body: if opened {
                FrameBody::Opened(Box::new(capture))
            } else {
                FrameBody::Closed(Box::new(capture))
            },
        };
        for node in recipients {
            let peer = self.peers.get_mut(&node).unwrap();
            peer.queue.retain(|w| {
                if stale(w) {
                    self.retained -= w.cost;
                    self.queued -= 1;
                    false
                } else {
                    true
                }
            });
            peer.queue.push_back(ForwardWork {
                frame: frame.clone(),
                input: None,
                cost,
            });
            self.retained += cost;
            self.queued += 1;
        }
        Ok(())
    }
    pub(super) fn source_control(
        &mut self,
        node: NodeId,
        out: &mut RelayForwarderProgress,
    ) -> Result<(), RelayNetworkError> {
        let received = self.peers[&node].hop.received().unwrap();
        let (original, opened) = match &received.frame.body {
            FrameBody::Opened(s) => (**s, true),
            FrameBody::Closed(s) => (**s, false),
            _ => unreachable!(),
        };
        let capture = original.extended(received.connection())?;
        let current = self.sources.get(&capture.source().node).copied();
        let changed = if opened {
            current != Some(capture)
        } else {
            current == Some(capture)
        };
        if changed && !self.closed {
            if self.announce(capture, opened) == Err(RelayNetworkError::Overloaded) {
                out.blocked += 1;
                return Ok(());
            }
            if opened {
                self.sources.insert(capture.source().node, capture);
                self.pending_closes.remove(&capture.source().node);
            } else {
                self.sources.remove(&capture.source().node);
            }
        }
        let received = self
            .peers
            .get_mut(&node)
            .unwrap()
            .hop
            .take_received()
            .unwrap();
        out.released.push(received.ticket());
        out.received += 1;
        Ok(())
    }
    pub(super) fn flush_closes(&mut self) -> Result<(), RelayNetworkError> {
        if self.closed {
            self.pending_closes.clear();
            return Ok(());
        }
        let pending: Vec<_> = self.pending_closes.values().copied().collect();
        for source in pending {
            match self.announce(source, false) {
                Ok(()) => {
                    self.pending_closes.remove(&source.source().node);
                }
                Err(RelayNetworkError::Overloaded) => break,
                Err(e) => return Err(e),
            }
        }
        Ok(())
    }
}
fn is_control(kind: RelayFrameKind) -> bool {
    matches!(
        kind,
        RelayFrameKind::SourceOpened | RelayFrameKind::SourceClosed | RelayFrameKind::Retire
    )
}
