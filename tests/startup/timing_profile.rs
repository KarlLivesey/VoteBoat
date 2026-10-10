// SPDX-License-Identifier: RPL-1.5
// Copyright (c) 2026 Karl Livesey
use super::*;
use voteboat::{authorization::CredentialGeneration, native::connect::NativePeerProtocol};

#[test]
fn profiles_keep_host_timer_contract_and_refuse_unknown_selection() {
    for (name, min, max, heartbeat) in [
        ("legacy", 150, 300, 50),
        ("throughput", 1000, 2000, 100),
        ("edge", 1500, 3000, 250),
    ] {
        let profile: NativeTimingProfile = name.parse().unwrap();
        let timers = profile.timers();
        timers.validate().unwrap();
        assert_eq!(timers.heartbeat_ms, heartbeat);
        assert_eq!(timers.election_min_ms, min);
        assert_eq!(min + timers.election_spread_ms, max);
    }
    for unknown in ["", "auto", "Throughput", "edge "] {
        assert!(unknown.parse::<NativeTimingProfile>().is_err());
    }
    assert_eq!(
        NativeTimingProfile::default(),
        NativeTimingProfile::Throughput
    );
}

#[test]
fn rotated_static_and_member_timers_reject_before_io_and_return_application() {
    let protocols = [
        NativePeerProtocol::TcpTls,
        #[cfg(feature = "quic")]
        NativePeerProtocol::Quic,
    ];
    for member in [false, true] {
        for protocol in protocols {
            let directory = root();
            let socket = std::net::TcpListener::bind("127.0.0.1:0").unwrap();
            let mut config = config(directory.clone(), NativeOpenMode::Recover);
            config.tls = config.tls.with_wire_version(7).unwrap();
            config.listen = socket.local_addr().unwrap();
            let rotation = NativePeerRotationStartup {
                protocol,
                generation: CredentialGeneration::new(1).unwrap(),
                latest: None,
            };
            let mut invalid = NativeTimingProfile::Edge.timers();
            invalid.election_spread_ms = 0;
            let rejected = if member {
                NativeMemberStartup {
                    provisioned_stores: config.bootstrap.voter_stores.clone(),
                    startup: config,
                }
                .open_with_peer_rotation_and_timers(
                    rotation,
                    invalid,
                    app(),
                    Arc::new(ThreadWake::current()),
                    MonoTime(0),
                )
            } else {
                config.open_with_peer_rotation_and_timers(
                    rotation,
                    invalid,
                    app(),
                    Arc::new(ThreadWake::current()),
                    MonoTime(0),
                )
            };
            let mut rejected = rejected.err().unwrap();
            assert_eq!(rejected.reason.stage, "native startup");
            assert_eq!(rejected.reason.detail, "InvalidLimits");
            assert!(rejected.application.is_some());
            assert!(rejected.try_cleanup().unwrap());
            assert!(!directory.exists());
            assert_eq!(
                std::net::TcpListener::bind(socket.local_addr().unwrap())
                    .unwrap_err()
                    .kind(),
                std::io::ErrorKind::AddrInUse
            );
        }
    }
}
