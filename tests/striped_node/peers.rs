#[cfg(feature = "quic")]
use voteboat::native::quic_connect::NativeQuicConnector;
// SPDX-License-Identifier: RPL-1.5
use super::{cluster::Cluster, support};
use std::{collections::BTreeMap, net::TcpListener, sync::Arc};
use voteboat::{
    connect::*,
    dial::*,
    identity::*,
    native::{connect::*, dial::*, tls::TlsPeer, transport::*, wire::*, worker::ThreadWake},
    outbound::OutboundBinding,
    runtime::*,
    secure::*,
    transport::*,
};
pub fn make(
    host: &Cluster,
    owner: RuntimeOwner,
    outbound: OutboundBinding,
    id: u64,
) -> PeerParts<NativeServiceConnector, NativeTransportFactory<NativeWireCodec>> {
    let local = LocalIdentity {
        node: support::node(id),
        store: owner.store,
    };
    let remote = (1..=3)
        .filter(|p| *p != id)
        .map(|p| (support::node(p), support::identity(p.into())))
        .collect::<BTreeMap<_, _>>();
    let pins = remote
        .iter()
        .map(|(node, store)| {
            (
                *node,
                support::tls::peer(LocalIdentity {
                    node: *node,
                    store: StoreBinding {
                        identity: *store,
                        session: owner.store.session,
                    },
                }),
            )
        })
        .collect::<BTreeMap<_, _>>();
    let connector = connector(host, local, remote.clone(), pins);
    let first = owner.store.session.get() * 10000;
    let roster = PeerRoster::new(
        PeerRosterConfig {
            local,
            outbound,
            first_generation: SecureSessionGeneration::new(first).unwrap(),
            last_generation: SecureSessionGeneration::new(first + 9999).unwrap(),
            wire_version: 1,
            limits: PeerRosterLimits::default(),
            transport_limits: TransportLimits::default(),
        },
        remote,
        host.now(),
    )
    .unwrap();
    PeerParts {
        admission_routes: None,
        connector,
        roster,
        factory: NativeTransportFactory::new(
            NativeWireCodec::new(Default::default()).unwrap(),
            TransportLimits::default(),
        )
        .unwrap(),
        ingress: IngressRouter::new(
            IngressBinding {
                owner,
                local,
                generation: IngressGeneration::new(1).unwrap(),
            },
            IngressLimits::default(),
        )
        .unwrap(),
        routes: (1..=3)
            .filter(|p| *p != id)
            .map(|p| {
                (
                    support::node(p),
                    if id < p {
                        ConnectDirection::Dial(host.addresses[p as usize - 1])
                    } else {
                        ConnectDirection::Accept
                    },
                )
            })
            .collect(),
    }
}

fn connector(
    host: &Cluster,
    local: LocalIdentity,
    remote: BTreeMap<NodeId, StoreIdentity>,
    pins: BTreeMap<NodeId, TlsPeer>,
) -> NativeServiceConnector {
    let id = local.node.get();
    let config = NativeConnectConfig {
        local,
        limits: ConnectLimits::default(),
        session: SessionLimits::default(),
    };
    let tls = support::tls::configuration(id);
    match host.protocol {
        NativePeerProtocol::TcpTls => {
            let dialer = NativeTcpDialer::spawn(
                local,
                remote.clone(),
                DialLimits::default(),
                Arc::new(ThreadWake::current()),
            )
            .unwrap();
            NativeServiceConnector::Tcp(Box::new(
                NativePeerConnector::new(
                    config,
                    tls,
                    pins,
                    dialer,
                    Some(TcpListener::bind(host.addresses[id as usize - 1]).unwrap()),
                    host.now(),
                )
                .unwrap_or_else(|e| panic!("striped connector: {:?}", e.reason)),
            ))
        }
        #[cfg(feature = "quic")]
        NativePeerProtocol::Quic => NativeServiceConnector::Quic(Box::new(
            NativeQuicConnector::new(
                config,
                tls,
                pins.into_iter()
                    .map(|(node, pin)| (node, (host.addresses[node.get() as usize - 1], pin)))
                    .collect(),
                std::net::UdpSocket::bind(host.addresses[id as usize - 1]).unwrap(),
                host.now(),
            )
            .unwrap(),
        )),
    }
}
