// SPDX-License-Identifier: RPL-1.5
// Copyright (c) 2026 Karl Livesey
//! Typed delegated intake rechecks the current authenticated path per message.
use super::*;
impl<S: SecureSession, W: WireCodec> NativeRelayNetwork<S, W> {
    pub(super) fn receive(&mut self, out: &mut PeerDriverProgress) -> Result<(), PeerDriverError> {
        if self.ingress.is_some() {
            return Ok(());
        }
        let Some(original) = self.parts.hop.take_received() else {
            return Ok(());
        };
        if self.closed {
            out.ingress.discarded += 1;
            return Ok(());
        }
        match &original.frame.body {
            FrameBody::Opened(capture) => {
                let source = original
                    .source_view()
                    .ok_or(PeerDriverError::ProviderViolation)?;
                self.sources.insert(capture.source().node, source);
                for r in &mut self.requests {
                    if r.requests.ticket().peer == capture.source().node && !r.retired {
                        r.replay = true;
                    }
                }
            }
            FrameBody::Closed(capture) => {
                let matches = self
                    .sources
                    .get(&capture.source().node)
                    .is_some_and(|s| s.delegation_path() == Some(capture.path()));
                if matches {
                    self.sources.remove(&capture.source().node);
                }
            }
            FrameBody::Forwarded(f) => {
                let source = original
                    .source_view()
                    .ok_or(PeerDriverError::ProviderViolation)?;
                if self.observed_source(source.source().node) != Some(source) {
                    out.ingress.discarded += f.output.kinds.len();
                    return Ok(());
                }
                let scope = WireScope {
                    from: source.source().node,
                    sender: source.source().store,
                    to: self.parts.plan.local().node,
                };
                let messages = match self.parts.codec.decode_batch(scope, &f.output.wire) {
                    Ok(messages) => messages,
                    Err(error) => {
                        self.rejected_receive = Some(original);
                        return Err(Self::error(RelayNetworkError::Wire(error)));
                    }
                };
                if messages.len() != f.output.kinds.len()
                    || messages.len() != f.returns.len()
                    || messages.capacity() > self.parts.codec.limits().max_messages
                    || messages.iter().any(|m| !scope.matches(m))
                {
                    self.rejected_messages = messages;
                    self.rejected_receive = Some(original);
                    return Err(Self::error(RelayNetworkError::InvalidFrame));
                }
                let indices = (0..messages.len()).collect();
                self.ingress = Some(PendingIngress {
                    original,
                    source,
                    messages: messages.into(),
                    indices,
                });
                out.received += 1;
            }
            _ => return Err(Self::error(RelayNetworkError::WrongSource)),
        }
        Ok(())
    }
    pub(super) fn dispatch<Q: ReadyScheduler, T: TimerService, E: ElectionEntropy>(
        &mut self,
        owner: &mut EffectOwner<Q, T, E>,
        visits: usize,
        out: &mut PeerDriverProgress,
    ) {
        let Some(mut pending) = self.ingress.take() else {
            return;
        };
        if self.closed || self.observed_source(pending.source.source().node) != Some(pending.source)
        {
            out.ingress.discarded += pending.messages.len();
            return;
        }
        for _ in 0..visits.min(pending.messages.len()) {
            let message = pending.messages.pop_front().unwrap();
            let index = pending.indices.pop_front().unwrap();
            out.ingress.visits += 1;
            let FrameBody::Forwarded(f) = &pending.original.frame.body else {
                unreachable!()
            };
            if !self.eligible(
                owner,
                pending.source,
                &message,
                f.output.kinds[index],
                f.returns[index],
            ) {
                out.ingress.discarded += 1;
                continue;
            }
            match owner.admit(message.group, Event::Receive(message)) {
                Ok(()) => out.ingress.admitted += 1,
                Err(r) => {
                    let Event::Receive(message) = *r.event else {
                        unreachable!()
                    };
                    if r.reason == RuntimeError::Overloaded {
                        pending.messages.push_back(message);
                        pending.indices.push_back(index);
                        out.ingress.blocked += 1;
                    } else {
                        out.ingress.discarded += 1;
                    }
                }
            }
        }
        if !pending.messages.is_empty() {
            self.ingress = Some(pending);
        }
    }
    fn eligible<Q: ReadyScheduler, T: TimerService, E: ElectionEntropy>(
        &self,
        owner: &EffectOwner<Q, T, E>,
        source: AuthenticatedSourceView,
        m: &Message,
        kind: RecordKind,
        original: Option<SendTicket>,
    ) -> bool {
        let reply = matches!(
            m.rpc,
            Rpc::Voted { .. }
                | Rpc::Appended { .. }
                | Rpc::Compacted { .. }
                | Rpc::SnapshotAck { .. }
                | Rpc::ReadAck
                | Rpc::LearnerReadinessReply { .. }
                | Rpc::AuthorityReply { .. }
                | Rpc::LearnerRepaired { .. }
        );
        match kind {
            RecordKind::Reply => {
                reply && original.is_some_and(|t| self.matches_reply(owner, t, source, m))
            }
            RecordKind::Request => {
                !reply
                    && original.is_none()
                    && m.context.origin == m.sender
                    && m.context.sequence > 0
                    && !matches!(
                        m.rpc,
                        Rpc::TimeoutNow { .. }
                            | Rpc::CommitNotice { .. }
                            | Rpc::CommittedLearnerRepairSnapshot { .. }
                    )
            }
            RecordKind::OneWay => {
                original.is_none()
                    && matches!(
                        m.rpc,
                        Rpc::TimeoutNow { .. }
                            | Rpc::CommitNotice { .. }
                            | Rpc::Append { .. }
                            | Rpc::CommittedLearnerRepairSnapshot { .. }
                    )
            }
        }
    }
}
