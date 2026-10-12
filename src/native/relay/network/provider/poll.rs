// SPDX-License-Identifier: RPL-1.5
// Copyright (c) 2026 Karl Livesey
//! Common Node poll/drain integration, with exact original resource handoff.
use super::*;
impl<S: SecureSession, W: WireCodec> PeerNetwork for NativeRelayNetwork<S, W> {
    type Endpoint = RelaySourceRoute;
    type Parts = NativeRelayParts<S, W>;
    type Recovery = NativeRelayRecovery<S, W>;
    fn build<O: OutboundQueue>(
        parts: Self::Parts,
        owner: RuntimeOwner,
        outbound: &O,
        limits: PeerDriverLimits,
        now: MonoTime,
    ) -> Result<Self, PeerNetworkRejected<Self::Parts>> {
        Self::construct(parts, owner, outbound, limits, now)
    }
    fn source_view(&self, source: NodeId) -> Option<AuthenticatedSourceView> {
        self.observed_source(source)
    }
    fn poll<O: OutboundQueue, Q: ReadyScheduler, T: TimerService, E: ElectionEntropy>(
        &mut self,
        owner: &mut EffectOwner<Q, T, E>,
        outbound: &mut O,
        now: MonoTime,
        budget: PeerDriverBudget,
    ) -> Result<PeerDriverProgress, PeerDriverError> {
        self.check(owner.identity(), outbound.binding(), now)?;
        if owner.is_failed() {
            return Err(PeerDriverError::Fenced);
        }
        validate_budget(budget)?;
        self.now = now;
        let result = self.poll_inner(owner, outbound, budget);
        if let Err(e) = &result {
            self.fail(e);
        }
        result
    }
    fn drain<O: OutboundQueue>(
        &mut self,
        outbound: &mut O,
        now: MonoTime,
        budget: PeerDriverBudget,
    ) -> Result<PeerDriverProgress, PeerDriverError> {
        self.check(self.owner, outbound.binding(), now)?;
        validate_budget(budget)?;
        if !self.closed {
            return Err(PeerDriverError::NotQuiescent);
        }
        self.now = now;
        let result = self.drain_inner(outbound, budget);
        if let Err(e) = &result {
            self.fail(e);
        }
        result
    }
    fn close(&mut self) {
        self.closed = true;
        self.sources.clear();
    }
    fn is_drained(&self) -> bool {
        self.closed
            && self.sending.is_none()
            && self.ingress.is_none()
            && self.parts.hop.is_drained()
            && self.rejected.is_empty()
            && self.rejected_outputs.is_empty()
    }
    fn next_deadline(&self) -> Option<MonoTime> {
        self.parts.hop.session_deadline()
    }
    fn into_parts(self) -> Result<Self::Parts, Box<Self>> {
        if self.is_drained() {
            Ok(self.parts)
        } else {
            Err(Box::new(self))
        }
    }
    fn into_recovery(mut self) -> Result<Self::Recovery, Box<Self>> {
        let Some(failure) = self.failed.take() else {
            return Err(Box::new(self));
        };
        if let Some(p) = self.sending.take() {
            if let Transmission::Original(o) = p.owner {
                self.rejected_outputs.push(*o);
            }
        }
        let (ingress, messages) = self
            .ingress
            .map_or((None, vec![]), |p| (Some(p.original), p.messages.into()));
        Ok(NativeRelayRecovery {
            parts: self.parts,
            outputs: self.rejected_outputs,
            quarantine: self.rejected,
            requests: self
                .requests
                .into_iter()
                .map(|r| (r.requests, r.frame))
                .collect(),
            orphan_scopes: self.orphan_scopes,
            rejected_receive: self.rejected_receive,
            rejected_messages: self.rejected_messages,
            completed_frames: self.completed_frames,
            ingress,
            messages,
            failure,
        })
    }
}
impl<S: SecureSession, W: WireCodec> NativeRelayNetwork<S, W> {
    fn poll_inner<Q: ReadyScheduler, T: TimerService, E: ElectionEntropy, O: OutboundQueue>(
        &mut self,
        owner: &mut EffectOwner<Q, T, E>,
        outbound: &mut O,
        budget: PeerDriverBudget,
    ) -> Result<PeerDriverProgress, PeerDriverError> {
        if self.closed {
            return self.drain_inner(outbound, budget);
        }
        let mut out = PeerDriverProgress::default();
        if budget.peer_visits > 0 {
            self.poll_hop(budget.transport, &mut out)?;
        }
        self.complete(outbound, &mut out)?;
        self.receive(&mut out)?;
        self.dispatch(owner, budget.ingress, &mut out);
        self.expire(owner);
        if budget.sends > 0 {
            self.submit(owner, outbound, &mut out)?;
        }
        Ok(out)
    }
    fn drain_inner<O: OutboundQueue>(
        &mut self,
        outbound: &mut O,
        budget: PeerDriverBudget,
    ) -> Result<PeerDriverProgress, PeerDriverError> {
        let mut out = PeerDriverProgress::default();
        if let Some(p) = self.ingress.take() {
            out.ingress.discarded += p.messages.len();
        }
        self.parts.hop.take_received();
        for _ in 0..budget.sends.min(self.limits.staged_batches) {
            let mut batches = outbound.poll(1);
            if batches.len() > 1 {
                self.rejected = batches;
                return Err(PeerDriverError::ProviderViolation);
            }
            if let Some(batch) = batches.pop() {
                self.cancel_batch(outbound, batch, LocalSendResult::Failed, &mut out)?;
            } else {
                break;
            }
        }
        if budget.peer_visits > 0 {
            self.poll_hop(budget.transport, &mut out)?;
        }
        self.complete(outbound, &mut out)?;
        self.parts.hop.take_received();
        if self.sending.is_none() {
            self.requests.clear();
            self.retained = 0;
            self.control.clear();
            self.parts.hop.close();
        }
        Ok(out)
    }
}
fn validate_budget(b: PeerDriverBudget) -> Result<(), PeerDriverError> {
    if b.peer_visits > 4096 || b.connection_visits > 4096 || b.sends > 4096 || b.ingress > 65536 {
        return Err(PeerDriverError::InvalidLimits);
    }
    b.transport.validate().map_err(PeerDriverError::Transport)?;
    b.connector.validate().map_err(PeerDriverError::Connect)?;
    Ok(())
}
