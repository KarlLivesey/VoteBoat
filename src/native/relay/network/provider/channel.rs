// SPDX-License-Identifier: RPL-1.5
// Copyright (c) 2026 Karl Livesey
//! Explicit actual helper-channel replacement preserves original collectors.
use super::*;
impl<S: SecureSession, W: WireCodec> PeerNetworkChannelReplacement for NativeRelayNetwork<S, W> {
    type Channel = RelayHop<S>;
    fn retire_channel(&mut self, now: MonoTime) -> Result<(), PeerDriverError> {
        self.check(self.owner, self.outbound, now)?;
        if self.closed {
            return Err(Self::error(RelayNetworkError::Closed));
        }
        self.sources.clear();
        self.parts.hop.close();
        self.now = now;
        Ok(())
    }
    fn replace_channel(
        &mut self,
        channel: Self::Channel,
        now: MonoTime,
    ) -> Result<Self::Channel, PeerNetworkRejected<Self::Channel>> {
        let check = || {
            self.check(self.owner, self.outbound, now)?;
            if self.closed {
                return Err(Self::error(RelayNetworkError::Closed));
            }
            if !channel.voter_matches(&self.parts.plan, self.parts.limits)
                || channel.authenticated_binding().is_none()
                || channel.binding() == self.parts.hop.binding()
            {
                return Err(PeerDriverError::WrongBinding);
            }
            if !self.parts.hop.is_drained() || self.sending.is_some() {
                return Err(PeerDriverError::NotQuiescent);
            }
            Ok(())
        };
        if let Err(reason) = check() {
            return Err(PeerNetworkRejected {
                reason,
                parts: Box::new(channel),
            });
        }
        let previous = std::mem::replace(&mut self.parts.hop, channel);
        self.sources.clear();
        self.ingress = None;
        for r in &mut self.requests {
            if !r.retired {
                r.replay = true;
            }
        }
        self.now = now;
        Ok(previous)
    }
}
impl<S: SecureSession, W: WireCodec> NativeRelayNetwork<S, W> {
    pub(super) fn poll_hop(
        &mut self,
        budget: TransportPollBudget,
        out: &mut PeerDriverProgress,
    ) -> Result<(), PeerDriverError> {
        match self.parts.hop.poll(self.now, budget) {
            Ok(_) => Ok(()),
            Err(e) if channel_loss(&e) => {
                self.sources.clear();
                out.connection_failures += 1;
                Ok(())
            }
            Err(e) => Err(Self::error(e)),
        }
    }
}
fn channel_loss(e: &RelayNetworkError) -> bool {
    matches!(
        e,
        RelayNetworkError::Closed
            | RelayNetworkError::Truncated
            | RelayNetworkError::Session(
                SessionError::Closed
                    | SessionError::Truncated
                    | SessionError::Timeout
                    | SessionError::Io(_)
            )
    )
}
