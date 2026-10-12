// SPDX-License-Identifier: RPL-1.5
// Copyright (c) 2026 Karl Livesey
//! Original retirement traverses the same permitted route without an ACK.
use super::*;
impl<S: SecureSession, W: WireCodec> RelayForwarder<S, W> {
    pub(in crate::native::relay::network::forwarder) fn retirement(
        &mut self,
        node: NodeId,
        out: &mut RelayForwarderProgress,
    ) -> Result<(), RelayNetworkError> {
        let received = self.peers[&node].hop.received().unwrap();
        let (original, source) = match &received.frame.body {
            FrameBody::Retire(t) => (
                *t,
                self.peers[&node]
                    .hop
                    .captured_source()
                    .ok_or(RelayNetworkError::WrongSource)?,
            ),
            FrameBody::Retired(t) => (t.original, t.source.extended(received.connection())?),
            _ => unreachable!(),
        };
        let destination = self
            .plan
            .next_hop(original.peer)
            .ok_or(RelayNetworkError::WrongSource)?;
        let onward = self.plan.tree.helper(destination).is_some()
            && self.peers.get(&destination).is_some_and(|p| !p.closed);
        let cost = std::mem::size_of::<RelayFrame>() + std::mem::size_of::<RetiredFrame>();
        if onward
            && (self.control_usage() >= self.limits.control_frames
                || self.queued >= self.limits.queued_frames
                || self.retained.saturating_add(cost) > self.limits.retained_bytes)
        {
            out.blocked += 1;
            return Ok(());
        }
        self.retire(original, out);
        let received = self
            .peers
            .get_mut(&node)
            .unwrap()
            .hop
            .take_received()
            .unwrap();
        if onward {
            let frame = RelayFrame {
                body: FrameBody::Retired(Box::new(RetiredFrame { source, original })),
            };
            self.retained += cost;
            self.queued += 1;
            self.peers
                .get_mut(&destination)
                .unwrap()
                .queue
                .push_back(ForwardWork {
                    frame,
                    input: Some(received.ticket()),
                    cost,
                });
        } else {
            out.released.push(received.ticket());
        }
        out.received += 1;
        Ok(())
    }
}
