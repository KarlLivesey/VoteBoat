// SPDX-License-Identifier: RPL-1.5
// Copyright (c) 2026 Karl Livesey
//! Original queue credit is released separately from retained remote collectors.
use super::*;
impl<S: SecureSession, W: WireCodec> NativeRelayNetwork<S, W> {
    pub(super) fn complete<O: OutboundQueue>(
        &mut self,
        outbound: &mut O,
        out: &mut PeerDriverProgress,
    ) -> Result<(), PeerDriverError> {
        let Some(done) = self.parts.hop.take_send() else {
            return Ok(());
        };
        let pending = self
            .sending
            .take()
            .ok_or(PeerDriverError::ProviderViolation)?;
        if done.ticket != pending.ticket {
            return Err(PeerDriverError::ProviderViolation);
        }
        match pending.owner {
            Transmission::Original(batch) => {
                let local = match self.complete_original(*batch, outbound, done.result) {
                    Ok(local) => local,
                    Err(error) => {
                        self.completed_frames.push(done.frame);
                        return Err(error);
                    }
                };
                out.completions.push(local.local);
                if local.requests.request_count() > 0 {
                    let cost = done.frame.retained_bytes() + local.requests.retained_bytes();
                    if self.retained.saturating_add(cost) > self.parts.limits.retained_bytes {
                        return Err(PeerDriverError::ProviderViolation);
                    }
                    self.retained += cost;
                    self.requests.push(RetainedOutput {
                        requests: local.requests,
                        frame: done.frame,
                        cost,
                        replay: false,
                        retired: false,
                    });
                }
            }
            Transmission::Replay(original) => {
                if done.frame.original_ticket() != Some(original) {
                    return Err(PeerDriverError::ProviderViolation);
                }
            }
            Transmission::Retire(original) => {
                let i = self
                    .requests
                    .iter()
                    .position(|r| r.requests.ticket() == original)
                    .ok_or(PeerDriverError::ProviderViolation)?;
                self.retained -= self.requests.remove(i).cost;
            }
        }
        Ok(())
    }
    fn complete_original<O: OutboundQueue>(
        &mut self,
        batch: NativeRelayBatch,
        outbound: &mut O,
        result: LocalSendResult,
    ) -> Result<RelayLocalSend, PeerDriverError> {
        batch.complete(outbound, result).map_err(|r| {
            if let Some(batch) = r.output {
                self.rejected_outputs.push(*batch);
            }
            if let Some(scopes) = r.requests {
                self.orphan_scopes.push(scopes);
            }
            // Violating successful completion may have consumed the original;
            // preserve its remaining semantic scopes with a real retained frame
            // only where the actual hop still owns that frame (recovery).
            Self::error(RelayNetworkError::Output(r.reason))
        })
    }
    pub(super) fn expire<Q: ReadyScheduler, T: TimerService, E: ElectionEntropy>(
        &mut self,
        owner: &EffectOwner<Q, T, E>,
    ) {
        for r in &mut self.requests {
            if !r.retired
                && r.requests.current_requests(owner) == 0
                && self.control.len() < self.parts.limits.control_frames
            {
                r.retired = true;
                r.replay = false;
                self.control.push_back(r.requests.ticket());
            }
        }
    }
    pub(super) fn submit<
        Q: ReadyScheduler,
        T: TimerService,
        E: ElectionEntropy,
        O: OutboundQueue,
    >(
        &mut self,
        owner: &EffectOwner<Q, T, E>,
        outbound: &mut O,
        out: &mut PeerDriverProgress,
    ) -> Result<(), PeerDriverError> {
        if self.closed || !self.parts.hop.can_submit() || self.sending.is_some() {
            return Ok(());
        }
        if let Some(original) = self.control.pop_front() {
            let frame = RelayFrame {
                body: FrameBody::Retire(original),
            };
            self.start(frame, Transmission::Retire(original), out)?;
            return Ok(());
        }
        if let Some(r) = self
            .requests
            .iter_mut()
            .find(|r| r.replay && !r.retired && r.requests.current_requests(owner) > 0)
        {
            r.replay = false;
            let frame = r.frame.clone();
            let original = r.requests.ticket();
            self.start(frame, Transmission::Replay(original), out)?;
            return Ok(());
        }
        if self.requests.len() >= self.parts.limits.request_slots
            || self.retained.saturating_add(self.parts.capture_reserve())
                > self.parts.limits.retained_bytes
        {
            return Ok(());
        }
        self.capture(owner, outbound, out)
    }
    fn capture<Q: ReadyScheduler, T: TimerService, E: ElectionEntropy, O: OutboundQueue>(
        &mut self,
        owner: &EffectOwner<Q, T, E>,
        outbound: &mut O,
        out: &mut PeerDriverProgress,
    ) -> Result<(), PeerDriverError> {
        let limits = self.parts.output_limits();
        let batch = match NativeRelayBatch::capture(owner, outbound, limits) {
            Ok(Some(batch)) => batch,
            Ok(None) => return Ok(()),
            Err(r) if r.reason == RelayOutputError::RetiredRequest => {
                for batch in r.batches {
                    self.cancel_batch(outbound, batch, LocalSendResult::Cancelled, out)?;
                }
                return Ok(());
            }
            Err(r) => {
                self.rejected = r.batches;
                return Err(Self::error(RelayNetworkError::Output(r.reason)));
            }
        };
        if let Some(r) = self
            .requests
            .iter_mut()
            .find(|r| !r.retired && batch.same_requests(&r.requests))
        {
            r.replay = true;
            let local = self.complete_original(batch, outbound, LocalSendResult::Cancelled)?;
            out.completions.push(local.local);
            return Ok(());
        }
        let frame = match batch.frame(owner, &self.parts.codec) {
            Ok(frame) => frame,
            Err(e) => {
                self.rejected_outputs.push(batch);
                return Err(Self::error(RelayNetworkError::Output(e)));
            }
        };
        match self.parts.hop.submit(frame) {
            Ok(ticket) => {
                self.sending = Some(PendingTransmission {
                    ticket,
                    owner: Transmission::Original(Box::new(batch)),
                });
                out.sends += 1;
                Ok(())
            }
            Err(r) => {
                self.rejected_outputs.push(batch);
                Err(Self::error(r.reason))
            }
        }
    }
    fn start(
        &mut self,
        frame: RelayFrame,
        owner: Transmission,
        out: &mut PeerDriverProgress,
    ) -> Result<(), PeerDriverError> {
        let ticket = self
            .parts
            .hop
            .submit(frame)
            .map_err(|r| Self::error(r.reason))?;
        self.sending = Some(PendingTransmission { ticket, owner });
        out.sends += 1;
        Ok(())
    }
    pub(super) fn cancel_batch<O: OutboundQueue>(
        &mut self,
        outbound: &mut O,
        batch: OutboundBatch,
        result: LocalSendResult,
        out: &mut PeerDriverProgress,
    ) -> Result<(), PeerDriverError> {
        let ticket = batch.ticket;
        match outbound.complete(batch, result) {
            Ok(done) if done.ticket == ticket && done.result == result => {
                out.completions.push(done);
                Ok(())
            }
            Ok(_) => Err(PeerDriverError::ProviderViolation),
            Err(r) => {
                self.rejected.push(*r.batch);
                Err(PeerDriverError::Outbound(r.reason))
            }
        }
    }
}
