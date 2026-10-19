// SPDX-License-Identifier: RPL-1.5
// Copyright (c) 2026 Karl Livesey
//! Select coherent bounded providers before any physical connection or admission.
use super::{setup, support};
use voteboat::{
    identity::*,
    logical_voter::LogicalApplication,
    native::{
        connect::NativeServiceConnector, node::NativeNodeParts, outbound::NativeOutbound,
        transport::NativeTransportFactory,
    },
    outbound::*,
    runtime::*,
    secure::LocalIdentity,
    transport::*,
    wire::*,
    wire_format::NativeWireCodec,
};
pub fn select(
    parts: &mut NativeNodeParts<LogicalApplication, NativeServiceConnector>,
    id: u64,
    now: MonoTime,
) {
    let owner = parts.local.owner.identity();
    let local = LocalIdentity {
        node: support::node(id),
        store: owner.store,
    };
    assert!(parts.local.outbound.is_drained());
    let outbound = OutboundBinding {
        generation: OutboundGeneration::new(2).unwrap(),
        ..parts.local.outbound.binding()
    };
    parts.local.outbound = NativeOutbound::new(outbound, outbound_limits()).unwrap();
    let peers = parts.peers.as_mut().unwrap();
    assert_eq!(peers.roster.usage().connections, 0);
    peers.ingress = IngressRouter::new(
        IngressBinding {
            owner,
            local,
            generation: IngressGeneration::new(2).unwrap(),
        },
        IngressLimits {
            bytes: 512 * 1024 * 1024,
            batch_bytes: 32 * 1024 * 1024,
            control_bytes: 128 * 1024 * 1024,
            background_bytes: 128 * 1024 * 1024,
            ..Default::default()
        },
    )
    .unwrap();
    peers.factory = NativeTransportFactory::new(codec(), transport()).unwrap();
    peers.roster = PeerRoster::new(
        PeerRosterConfig {
            local,
            outbound,
            // The store session scopes generations (startup does the same).
            first_generation: SecureSessionGeneration::new(1).unwrap(),
            wire_version: 8,
            limits: PeerRosterLimits {
                peers: 2,
                connecting: 2,
                connection_bytes: 256 * 1024 * 1024,
                ..Default::default()
            },
            transport_limits: transport(),
        },
        setup::bottom((id as usize - 1) / 3)
            .voter_stores
            .into_iter()
            .filter(|(n, _)| *n != local.node)
            .collect(),
        now,
    )
    .unwrap();
}
fn codec() -> NativeWireCodec {
    NativeWireCodec::with_leadership_transfer(WireLimits {
        max_frame_bytes: 32 * 1024 * 1024,
        max_command_bytes: setup::lower_limits().max_command_bytes,
        max_snapshot_bytes: setup::lower_limits().max_child_checkpoint_bytes,
        max_decoded_bytes: 64 * 1024 * 1024,
        ..Default::default()
    })
    .unwrap()
}
fn transport() -> TransportLimits {
    TransportLimits {
        send_frame_bytes: 32 * 1024 * 1024,
        receive_frame_bytes: 32 * 1024 * 1024,
        decoded_bytes: 64 * 1024 * 1024,
    }
}
fn outbound_limits() -> OutboundLimits {
    let standard = OutboundLimits::default();
    let peer = OutboundBudget {
        max: OutboundUsage {
            bytes: 96 * 1024 * 1024,
            ..standard.peer.max
        },
        background: OutboundUsage {
            bytes: 64 * 1024 * 1024,
            ..standard.peer.background
        },
        ..standard.peer
    };
    OutboundLimits {
        batch_bytes: 32 * 1024 * 1024,
        peer,
        node: OutboundBudget {
            max: OutboundUsage {
                bytes: 192 * 1024 * 1024,
                ..standard.node.max
            },
            background: OutboundUsage {
                bytes: 128 * 1024 * 1024,
                ..standard.node.background
            },
            ..standard.node
        },
        ..standard
    }
}
