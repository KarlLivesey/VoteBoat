// SPDX-License-Identifier: RPL-1.5
// Copyright (c) 2026 Karl Livesey
//! Construction-selected ordinary-voter relay, over the shared owning runtime.
use super::*;
use crate::{connect::ConnectDirection, raft::*};
use frame::{FrameBody, RecordKind};
use std::collections::VecDeque;
mod channel;
mod configuration;
mod ingress;
mod output;
mod parts;
mod poll;
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub struct RelaySourceRoute {
    pub helper: PeerIdentity,
}
pub struct NativeRelayParts<S: SecureSession, W: WireCodec> {
    pub plan: NativeRelayPlan,
    pub hop: RelayHop<S>,
    pub codec: W,
    pub limits: RelayNetworkLimits,
    pub routes: Option<BTreeMap<NodeId, PeerRoute<RelaySourceRoute>>>,
}
struct RetainedOutput {
    requests: RelayRequestScopes,
    frame: RelayFrame,
    cost: usize,
    replay: bool,
    retired: bool,
}
enum Transmission {
    Original(Box<NativeRelayBatch>),
    Replay(SendTicket),
    Retire(SendTicket),
}
struct PendingTransmission {
    ticket: RelayHopTicket,
    owner: Transmission,
}
struct PendingIngress {
    original: RelayReceived,
    source: AuthenticatedSourceView,
    messages: VecDeque<Message>,
    indices: VecDeque<usize>,
}
/// One genuine helper channel; physical-source history is independently bounded.
pub struct NativeRelayNetwork<S: SecureSession, W: WireCodec> {
    parts: NativeRelayParts<S, W>,
    owner: RuntimeOwner,
    outbound: OutboundBinding,
    limits: PeerDriverLimits,
    sources: BTreeMap<NodeId, AuthenticatedSourceView>,
    requests: Vec<RetainedOutput>,
    retained: usize,
    control: VecDeque<SendTicket>,
    sending: Option<PendingTransmission>,
    ingress: Option<PendingIngress>,
    rejected: Vec<OutboundBatch>,
    rejected_outputs: Vec<NativeRelayBatch>,
    orphan_scopes: Vec<RelayRequestScopes>,
    rejected_receive: Option<RelayReceived>,
    rejected_messages: Vec<Message>,
    completed_frames: Vec<RelayFrame>,
    failed: Option<PeerDriverError>,
    closed: bool,
    now: MonoTime,
}
/// Actual resources on failure, including still-charged original queue outputs.
pub struct NativeRelayRecovery<S: SecureSession, W: WireCodec> {
    pub parts: NativeRelayParts<S, W>,
    pub outputs: Vec<NativeRelayBatch>,
    pub quarantine: Vec<OutboundBatch>,
    pub requests: Vec<(RelayRequestScopes, RelayFrame)>,
    pub orphan_scopes: Vec<RelayRequestScopes>,
    pub rejected_receive: Option<RelayReceived>,
    pub rejected_messages: Vec<Message>,
    pub completed_frames: Vec<RelayFrame>,
    pub ingress: Option<RelayReceived>,
    pub messages: Vec<Message>,
    pub failure: PeerDriverError,
}
impl<S: SecureSession, W: WireCodec> NativeRelayNetwork<S, W> {
    fn error(error: RelayNetworkError) -> PeerDriverError {
        PeerDriverError::Relay(error)
    }
    fn check(
        &self,
        owner: RuntimeOwner,
        outbound: OutboundBinding,
        now: MonoTime,
    ) -> Result<(), PeerDriverError> {
        if owner != self.owner || outbound != self.outbound {
            return Err(PeerDriverError::WrongBinding);
        }
        if now < self.now {
            return Err(PeerDriverError::TimeWentBack);
        }
        if self.failed.is_some() {
            return Err(PeerDriverError::Fenced);
        }
        Ok(())
    }
    fn fail(&mut self, reason: &PeerDriverError) {
        self.failed = Some(reason.clone());
        self.closed = true;
        self.sources.clear();
        self.parts.hop.abort();
    }
    fn observed_source(&self, node: NodeId) -> Option<AuthenticatedSourceView> {
        if self.closed || self.parts.hop.authenticated_binding().is_none() {
            return None;
        }
        self.sources
            .get(&node)
            .copied()
            .filter(|s| s.receiving_access() == self.parts.hop.binding())
    }
    fn matches_reply<Q: ReadyScheduler, T: TimerService, E: ElectionEntropy>(
        &self,
        owner: &EffectOwner<Q, T, E>,
        ticket: SendTicket,
        source: AuthenticatedSourceView,
        message: &Message,
    ) -> bool {
        self.requests.iter().any(|r| r.requests.matches_reply(owner, ticket, source, message))
            || self.sending.as_ref().is_some_and(|p| matches!(&p.owner, Transmission::Original(o) if o.matches_pending_reply(owner, ticket, source, message)))
    }
}
impl<S: SecureSession, W: WireCodec> std::fmt::Debug for NativeRelayNetwork<S, W> {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("NativeRelayNetwork")
            .field("hop", &self.parts.hop)
            .field("closed", &self.closed)
            .field("sending", &self.sending.as_ref().map(|p| p.ticket))
            .field("ingress", &self.ingress.as_ref().map(|p| p.messages.len()))
            .field("requests", &self.requests.len())
            .field("control", &self.control.len())
            .field("quarantine", &self.rejected.len())
            .field("rejected_outputs", &self.rejected_outputs.len())
            .field("failure", &self.failed)
            .finish()
    }
}
