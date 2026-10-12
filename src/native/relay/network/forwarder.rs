// SPDX-License-Identifier: RPL-1.5
// Copyright (c) 2026 Karl Livesey
//! Bounded nonvoting helper, consuming actual authenticated original inputs.
use super::*;
use crate::raft::*;
use frame::{ForwardFrame, FrameBody, OutputFrame, RecordKind, RetiredFrame};
use std::collections::VecDeque;
mod ledger;
use ledger::*;
mod intake;
mod lifecycle;
pub use lifecycle::RelayForwarderHopRejected;
mod poll;
mod sources;

pub struct RelayForwarderParts<S: SecureSession, W: WireCodec> {
    pub plan: RelayHelperPlan,
    pub codec: W,
    pub limits: RelayNetworkLimits,
    pub hops: Vec<RelayHop<S>>,
}
pub struct RelayForwarderRejected<S: SecureSession, W: WireCodec> {
    pub reason: RelayNetworkError,
    pub parts: Box<RelayForwarderParts<S, W>>,
}
#[derive(Default)]
pub struct RelayForwarderProgress {
    pub received: usize,
    pub forwarded: usize,
    pub completed: usize,
    pub blocked: usize,
    pub discarded: usize,
    pub retired: usize,
    pub source_failures: Vec<(NodeId, RelayNetworkError)>,
    /// Actual input owners released by a completed/cancelled local hop frame.
    pub released: Vec<RelayReceiveTicket>,
    pub completions: Vec<RelayForwardingCompletion>,
}
pub struct RelayForwardingCompletion {
    pub input: Option<RelayReceiveTicket>,
    /// Exact completed original frame. No field attests remote consumption.
    pub local: RelayHopCompletion,
}
struct ForwardWork {
    frame: RelayFrame,
    input: Option<RelayReceiveTicket>,
    cost: usize,
}
struct ActiveForward {
    ticket: RelayHopTicket,
    input: Option<RelayReceiveTicket>,
    cost: usize,
    kind: RelayFrameKind,
    destination: Option<NodeId>,
}
struct ForwardPeer<S: SecureSession> {
    hop: RelayHop<S>,
    capture: Option<RelaySourceCapture>,
    closed: bool,
    queue: VecDeque<ForwardWork>,
    active: Option<ActiveForward>,
}
/// This owner has no Raft core, membership ballot or parent WAL. Its ledger
/// correlates actual originals; only the physical voters produce replies.
pub struct RelayForwarder<S: SecureSession, W: WireCodec> {
    plan: RelayHelperPlan,
    codec: W,
    limits: RelayNetworkLimits,
    peers: BTreeMap<NodeId, ForwardPeer<S>>,
    sources: BTreeMap<NodeId, RelaySourceCapture>,
    pending_closes: BTreeMap<NodeId, RelaySourceCapture>,
    ledger: Vec<RequestRecord>,
    retained: usize,
    peer_metadata: usize,
    queued: usize,
    cursor: usize,
    now: Option<MonoTime>,
    closed: bool,
}
impl<S: SecureSession, W: WireCodec> RelayForwarder<S, W> {
    pub fn build(parts: RelayForwarderParts<S, W>) -> Result<Self, RelayForwarderRejected<S, W>> {
        let checked = Self::check_parts(&parts);
        if let Err(reason) = checked {
            return Err(RelayForwarderRejected {
                reason,
                parts: Box::new(parts),
            });
        }
        let RelayForwarderParts {
            plan,
            codec,
            limits,
            hops,
        } = parts;
        let peers: BTreeMap<_, _> = hops
            .into_iter()
            .map(|hop| {
                let capture = hop.captured_source();
                (
                    hop.binding().peer.node,
                    ForwardPeer {
                        hop,
                        capture,
                        closed: false,
                        queue: VecDeque::new(),
                        active: None,
                    },
                )
            })
            .collect();
        let sources = peers
            .values()
            .filter_map(|p: &ForwardPeer<S>| p.capture.map(|c| (c.source().node, c)))
            .collect();
        let peer_metadata = peers.len() * peer_metadata_cost::<S>();
        let retained = parts_capture_reserve(&plan) + peer_metadata;
        let mut out = Self {
            plan,
            codec,
            limits,
            peers,
            sources,
            pending_closes: BTreeMap::new(),
            ledger: vec![],
            retained,
            peer_metadata,
            queued: 0,
            cursor: 0,
            now: None,
            closed: false,
        };
        let captures: Vec<_> = out.sources.values().copied().collect();
        for capture in captures {
            out.announce(capture, true)
                .expect("checked initial control reservation");
        }
        Ok(out)
    }
    fn check_parts(parts: &RelayForwarderParts<S, W>) -> Result<(), RelayNetworkError> {
        let limits = parts.limits.validate()?;
        let wire = parts
            .codec
            .limits()
            .validate()
            .map_err(RelayNetworkError::Wire)?;
        if !frame::codec_fit(parts.plan.tree.max_hops(), limits, wire) {
            return Err(RelayNetworkError::InvalidLimits);
        }
        if parts.codec.format_version() != parts.plan.original_wire_version()
            || parts.hops.is_empty()
            || parts.hops.len() > limits.hop_slots
        {
            return Err(RelayNetworkError::WrongProfile);
        }
        let mut seen = std::collections::BTreeSet::new();
        for hop in &parts.hops {
            let binding = hop
                .authenticated_binding()
                .ok_or(RelayNetworkError::WrongSource)?;
            if !hop.helper_matches(&parts.plan, limits)
                || binding.local != parts.plan.local()
                || !parts.plan.adjacent(binding.peer)
                || !seen.insert(binding.peer.node)
            {
                return Err(RelayNetworkError::WrongBinding);
            }
        }
        let announcements = parts
            .plan
            .sources()
            .count()
            .saturating_mul(parts.hops.len().saturating_sub(1));
        if announcements > limits.control_frames
            || announcements
                .saturating_mul(
                    std::mem::size_of::<RelayFrame>() + std::mem::size_of::<RelaySourceCapture>(),
                )
                .saturating_add(parts_capture_reserve(&parts.plan))
                .saturating_add(parts.hops.len() * peer_metadata_cost::<S>())
                > limits.retained_bytes
        {
            return Err(RelayNetworkError::InvalidLimits);
        }
        Ok(())
    }
    pub fn source_capture(&self, node: NodeId) -> Option<RelaySourceCapture> {
        if self.closed {
            None
        } else {
            self.sources.get(&node).copied()
        }
    }
    pub fn pending_requests(&self) -> usize {
        self.ledger.len()
    }
    pub fn retained_bytes(&self) -> usize {
        self.retained
    }
    pub fn close(&mut self) {
        self.closed = true;
    }
    pub fn is_drained(&self) -> bool {
        self.closed
            && self.ledger.is_empty()
            && self.queued == 0
            && self
                .peers
                .values()
                .all(|p| p.queue.is_empty() && p.active.is_none() && p.hop.is_drained())
    }
    pub fn next_deadline(&mut self) -> Option<MonoTime> {
        self.peers
            .values_mut()
            .filter_map(|p| p.hop.next_deadline())
            .min()
    }
    pub fn into_parts(self) -> Result<RelayForwarderParts<S, W>, Box<Self>> {
        if !self.is_drained() {
            return Err(Box::new(self));
        }
        Ok(RelayForwarderParts {
            plan: self.plan,
            codec: self.codec,
            limits: self.limits,
            hops: self.peers.into_values().map(|p| p.hop).collect(),
        })
    }
}
fn parts_capture_reserve(plan: &RelayHelperPlan) -> usize {
    2 * plan.sources().count()
        * (std::mem::size_of::<RelaySourceCapture>() + std::mem::size_of::<NodeId>() + 64)
}
fn peer_metadata_cost<S: SecureSession>() -> usize {
    std::mem::size_of::<ForwardPeer<S>>() + std::mem::size_of::<NodeId>() + 64
}
impl<S: SecureSession, W: WireCodec> std::fmt::Debug for RelayForwarder<S, W> {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("RelayForwarder")
            .field("closed", &self.closed)
            .field("ledger", &self.ledger.len())
            .field("retained", &self.retained)
            .field("queued", &self.queued)
            .field(
                "peers",
                &self
                    .peers
                    .iter()
                    .map(|(&id, p)| (id, p.closed, p.queue.len(), p.active.is_some(), &p.hop))
                    .collect::<Vec<_>>(),
            )
            .finish()
    }
}
