// SPDX-License-Identifier: RPL-1.5
// Copyright (c) 2026 Karl Livesey
//! Checked construction preserves the exact selected channel and codec.
use super::*;
impl<S: SecureSession, W: WireCodec> NativeRelayParts<S, W> {
    pub(super) fn output_limits(&self) -> RelayOutputLimits {
        let wire = self.codec.limits();
        RelayOutputLimits {
            batch_messages: wire.max_messages,
            metadata_bytes: NativeRelayBatch::metadata_bound(wire.max_messages)
                .unwrap_or(usize::MAX),
            frame_bytes: wire.max_frame_bytes,
        }
    }
    pub(super) fn capture_reserve(&self) -> usize {
        let limits = self.output_limits();
        limits
            .frame_bytes
            .saturating_add(limits.metadata_bytes)
            .saturating_add(512)
            .saturating_add(limits.batch_messages * std::mem::size_of::<RelayRecordKind>())
    }
    fn check_parts(&self) -> Result<(), PeerDriverError> {
        self.limits
            .validate()
            .map_err(NativeRelayNetwork::<S, W>::error)?;
        let wire = self
            .codec
            .limits()
            .validate()
            .map_err(|e| NativeRelayNetwork::<S, W>::error(RelayNetworkError::Wire(e)))?;
        if self.capture_reserve() > self.limits.retained_bytes
            || !frame::codec_fit(self.plan.tree.max_hops(), self.limits, wire)
        {
            return Err(PeerDriverError::InvalidLimits);
        }
        if !self.hop.voter_matches(&self.plan, self.limits)
            || self.hop.binding().local != self.plan.local()
            || self.codec.format_version() != self.plan.original_wire_version()
            || wire.max_decoded_bytes > self.limits.decoded_bytes
            || self.plan.sources().count() > self.limits.source_slots
        {
            return Err(PeerDriverError::WrongBinding);
        }
        Ok(())
    }
}
impl<S: SecureSession, W: WireCodec> PeerNetworkParts for NativeRelayParts<S, W> {
    type Network = NativeRelayNetwork<S, W>;
    fn local(&self) -> LocalIdentity {
        self.plan.local()
    }
    fn original_wire_version(&self) -> u16 {
        self.plan.original_wire_version()
    }
    fn source_identity(&self, source: NodeId) -> Option<PeerIdentity> {
        self.plan.source(source)
    }
    fn initial_routes(&self) -> Result<PeerNetworkRoutes<Self::Network>, PeerDriverError> {
        self.check_parts()?;
        if let Some(routes) = &self.routes {
            return Ok(routes.clone());
        }
        Ok(self
            .plan
            .sources()
            .filter(|p| p.node != self.plan.local().node)
            .map(|p| {
                (
                    p.node,
                    PeerRoute {
                        store: p.store,
                        direction: ConnectDirection::Dial(RelaySourceRoute {
                            helper: self.plan.helper(),
                        }),
                    },
                )
            })
            .collect())
    }
    fn route_budget(
        &self,
        routes: &PeerNetworkRoutes<Self::Network>,
        limits: PeerDriverLimits,
    ) -> Result<ConnectionBudget, PeerDriverError> {
        self.check_parts()?;
        if limits.staged_batches == 0
            || limits.staged_batches > 65536
            || self.plan.sources().count().saturating_mul(512) > limits.metadata_bytes
        {
            return Err(PeerDriverError::InvalidLimits);
        }
        validate_routes(&self.plan, routes)?;
        ConnectionBudget::new(
            self.plan.local(),
            self.limits.source_slots,
            self.plan
                .sources()
                .filter(|p| p.node != self.plan.local().node)
                .map(|p| (p.node, p.store))
                .collect(),
        )
        .map_err(|e| PeerDriverError::Owner(EffectOwnerError::Runtime(e)))
    }
}
pub(super) fn validate_routes(
    plan: &NativeRelayPlan,
    routes: &BTreeMap<NodeId, PeerRoute<RelaySourceRoute>>,
) -> Result<(), PeerDriverError> {
    if routes.iter().any(|(&node,r)| node == plan.local().node
        || plan.source(node).is_none_or(|p| p.store != r.store)
        || !matches!(r.direction, ConnectDirection::Dial(route) if route.helper == plan.helper())) {
        return Err(PeerDriverError::WrongBinding);
    }
    Ok(())
}
impl<S: SecureSession, W: WireCodec> NativeRelayNetwork<S, W> {
    pub(super) fn construct<O: OutboundQueue>(
        parts: NativeRelayParts<S, W>,
        owner: RuntimeOwner,
        outbound: &O,
        limits: PeerDriverLimits,
        now: MonoTime,
    ) -> Result<Self, PeerNetworkRejected<NativeRelayParts<S, W>>> {
        let check = || {
            let routes = parts.initial_routes()?;
            parts.route_budget(&routes, limits)?;
            let binding = outbound.binding();
            if owner.store != parts.plan.local().store
                || binding.node != parts.plan.local().node
                || binding.store != owner.store
            {
                return Err(PeerDriverError::WrongBinding);
            }
            outbound
                .limits()
                .validate()
                .map_err(PeerDriverError::Outbound)?;
            Ok(())
        };
        if let Err(reason) = check() {
            return Err(PeerNetworkRejected {
                reason,
                parts: Box::new(parts),
            });
        }
        Ok(Self {
            parts,
            owner,
            outbound: outbound.binding(),
            limits,
            sources: BTreeMap::new(),
            requests: vec![],
            retained: 0,
            control: VecDeque::new(),
            sending: None,
            ingress: None,
            rejected: vec![],
            rejected_outputs: vec![],
            orphan_scopes: vec![],
            rejected_receive: None,
            rejected_messages: vec![],
            completed_frames: vec![],
            failed: None,
            closed: false,
            now,
        })
    }
}
