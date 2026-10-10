// SPDX-License-Identifier: RPL-1.5
// Copyright (c) 2026 Karl Livesey
use super::*;
use voteboat::{
    authorization::CredentialGeneration,
    credential_reload::*,
    native::{connect::NativePeerProtocol, peer_credentials::NativePeerMaterial},
    secure::PeerIdentity,
};
pub(super) fn generation(n: u64) -> CredentialGeneration {
    CredentialGeneration::new(n).unwrap()
}
pub(super) fn material(
    config: &NativeStartup,
    stores: &BTreeMap<NodeId, StoreIdentity>,
) -> NativePeerMaterial {
    NativePeerMaterial {
        tls: config.tls.clone(),
        peers: config
            .peers
            .iter()
            .map(|(node, peer)| {
                (
                    *node,
                    TlsPeer {
                        identity: PeerIdentity {
                            node: *node,
                            store: stores[node],
                        },
                        certificate: peer.certificate.clone(),
                        server_name: peer.server_name.clone(),
                    },
                )
            })
            .collect(),
    }
}
pub(super) fn record(
    config: &NativeStartup,
    stores: &BTreeMap<NodeId, StoreIdentity>,
    next: u64,
) -> CredentialReloadRecord {
    CredentialReloadRecord {
        owner: PeerIdentity {
            node: config.node,
            store: config.store,
        },
        request: CredentialReloadRequest {
            sequence: next - 1,
            expected: generation(next - 1),
            replacement: generation(next),
        },
        digest: material(config, stores).digest(),
    }
}
#[test]
fn mismatched_rotation_records_reject_before_files_or_bind_and_return_application() {
    let directory = root();
    let socket = std::net::TcpListener::bind("127.0.0.1:0").unwrap();
    for variant in 0..8 {
        let mut config = config(directory.clone(), NativeOpenMode::Create);
        config.listen = socket.local_addr().unwrap();
        let mut record = record(&config, &config.bootstrap.voter_stores, 2);
        let mut generation = generation(2);
        match variant {
            0 => generation = self::generation(1),
            1 => generation = self::generation(3),
            2 => record.owner.node = NodeId::new(99).unwrap(),
            3 => record.owner.store.incarnation = StoreIncarnation::new(9).unwrap(),
            4 => record.request.sequence = 0,
            5 => record.request.expected = record.request.replacement,
            6 => record.digest[0] ^= 1,
            _ => config.tls = config.tls.with_wire_version(2).unwrap(),
        }
        let mut rejected = config
            .open_with_peer_rotation(
                NativePeerRotationStartup {
                    protocol: NativePeerProtocol::TcpTls,
                    generation,
                    latest: Some(record),
                },
                app(),
                Arc::new(ThreadWake::current()),
                MonoTime(0),
            )
            .err()
            .unwrap();
        assert_eq!(
            rejected.reason.stage, "peer credentials",
            "variant {variant}"
        );
        assert_eq!(rejected.application.as_ref().unwrap().applied_index(), 0);
        assert!(rejected.try_cleanup().unwrap());
        assert!(!directory.exists());
    }
}

#[test]
fn material_digest_binds_local_tls_and_every_peer_field() {
    let config = config(root(), NativeOpenMode::Create);
    let peer = PeerIdentity {
        node: NodeId::new(2).unwrap(),
        store: config.store,
    };
    let mut original = material(&config, &config.bootstrap.voter_stores);
    original.peers.insert(
        peer.node,
        TlsPeer {
            identity: peer,
            certificate: vec![1, 2, 3],
            server_name: "node2.test".into(),
        },
    );
    let expected = original.digest();
    assert_eq!(expected, original.digest());
    for variant in 0..8 {
        let mut changed = NativePeerMaterial {
            tls: original.tls.clone(),
            peers: original.peers.clone(),
        };
        let p = changed.peers.get_mut(&peer.node).unwrap();
        match variant {
            0 => p.identity.node = NodeId::new(3).unwrap(),
            1 => p.identity.store.id = StoreId::new(5).unwrap(),
            2 => p.identity.store.incarnation = StoreIncarnation::new(5).unwrap(),
            3 => p.certificate.push(4),
            4 => p.server_name.push('x'),
            5 => changed.tls = changed.tls.with_wire_version(2).unwrap(),
            6 => {
                let p = changed.peers.remove(&peer.node).unwrap();
                changed.peers.insert(NodeId::new(3).unwrap(), p);
            }
            _ => {
                changed.tls = NativeTlsConfig::new(TlsCredentials {
                    roots: vec![include_bytes!("../fixtures/tls/ca.der").to_vec()],
                    certificate_chain: vec![include_bytes!("../fixtures/tls/node2.der").to_vec()],
                    private_key: include_bytes!("../fixtures/tls/node2-key.der").to_vec(),
                })
                .unwrap()
            }
        }
        assert_ne!(expected, changed.digest(), "variant {variant}");
    }
}

#[test]
fn static_and_member_startup_install_the_recorded_generation() {
    let root = root();
    let mut initial = config(root.clone(), NativeOpenMode::Create);
    initial.tls = initial.tls.with_wire_version(7).unwrap();
    let node = initial
        .open_with_peer_rotation_and_timers(
            NativePeerRotationStartup {
                protocol: NativePeerProtocol::TcpTls,
                generation: generation(1),
                latest: None,
            },
            NativeTimingProfile::Edge.timers(),
            app(),
            Arc::new(ThreadWake::current()),
            MonoTime(0),
        )
        .unwrap();
    assert_eq!(
        node.peers().unwrap().credential_generation(),
        Some(generation(1))
    );
    let token = node.local().owner.deadline(group()).unwrap();
    assert!((1500..3000).contains(&token.deadline.0));
    drain_wire_nodes(vec![node], Instant::now());
    let mut recovered = config(root.clone(), NativeOpenMode::Recover);
    recovered.tls = recovered.tls.with_wire_version(7).unwrap();
    let record = record(&recovered, &recovered.bootstrap.voter_stores, 2);
    let member = NativeMemberStartup {
        provisioned_stores: recovered.bootstrap.voter_stores.clone(),
        startup: recovered,
    };
    let node = member
        .open_with_peer_rotation_and_timers(
            NativePeerRotationStartup {
                protocol: NativePeerProtocol::TcpTls,
                generation: generation(2),
                latest: Some(record),
            },
            NativeTimingProfile::Edge.timers(),
            app(),
            Arc::new(ThreadWake::current()),
            MonoTime(0),
        )
        .unwrap();
    assert_eq!(
        node.peers().unwrap().credential_generation(),
        Some(generation(2))
    );
    let token = node.local().owner.deadline(group()).unwrap();
    assert!((1500..3000).contains(&token.deadline.0));
    drain_wire_nodes(vec![node], Instant::now());
    std::fs::remove_dir_all(root).unwrap();
}

fn rejected_owner_cleanup(protocol: NativePeerProtocol) {
    let root = root();
    let mut config = config(root.clone(), NativeOpenMode::Create);
    let reserve = std::net::TcpListener::bind("127.0.0.1:0").unwrap();
    let udp = std::net::UdpSocket::bind(reserve.local_addr().unwrap()).unwrap();
    config.listen = reserve.local_addr().unwrap();
    let address = config.listen;
    drop((reserve, udp));
    config.limits.replica.leases = 0;
    let mut rejected = config
        .open_with_peer_rotation(
            NativePeerRotationStartup {
                protocol,
                generation: generation(1),
                latest: None,
            },
            app(),
            Arc::new(ThreadWake::current()),
            MonoTime(0),
        )
        .err()
        .unwrap();
    assert_eq!(rejected.reason.stage, "node assembly");
    assert!(rejected.application.is_some());
    let deadline = Instant::now() + Duration::from_secs(5);
    while !rejected.try_cleanup().unwrap() {
        assert!(Instant::now() < deadline);
        std::thread::park_timeout(Duration::from_millis(1));
    }
    let _tcp = std::net::TcpListener::bind(address).unwrap();
    let _udp = std::net::UdpSocket::bind(address).unwrap();
    let config = super::config(root.clone(), NativeOpenMode::Recover);
    let log = voteboat::native::log_store::NativeLogStore::recover(
        voteboat::native::log_store::FileLogIo::open(&root).unwrap(),
        config.store,
        LogLimits::default(),
    )
    .unwrap();
    assert_eq!(log.state(group()).unwrap().bootstrap, config.bootstrap);
    drop(log);
    std::fs::remove_dir_all(root).unwrap();
}
#[test]
fn rejected_rotating_tcp_owner_releases_socket_and_workers() {
    rejected_owner_cleanup(NativePeerProtocol::TcpTls);
}
#[cfg(feature = "quic")]
#[test]
fn rejected_rotating_quic_owner_releases_socket_and_workers() {
    rejected_owner_cleanup(NativePeerProtocol::Quic);
}
