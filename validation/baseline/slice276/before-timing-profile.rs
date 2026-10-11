// SPDX-License-Identifier: RPL-1.5
// Copyright (c) 2026 Karl Livesey
use super::*;
use voteboat::{authorization::CredentialGeneration, native::connect::NativePeerProtocol};

fn check_election_window<C, F>(node: &mut NativeNode<HostApplication, C, F>, legacy: bool)
where
    C: voteboat::connect::PeerConnector,
    C::Endpoint: Clone,
    F: voteboat::transport::PeerTransportFactory<C::Session>,
{
    let before = node.local().owner.core(group()).unwrap().state().hard_state;
    node.poll(MonoTime(950), NodePollBudget::default()).unwrap();
    let core = node.local().owner.core(group()).unwrap();
    if legacy {
        assert!(core.has_pending_dependency() || core.state().hard_state.term > before.term);
    } else {
        assert_eq!(core.role(), voteboat::raft::Role::Follower);
        assert_eq!(core.state().hard_state, before);
        assert!(
            !core.has_pending_dependency(),
            "default must not campaign before 1000ms"
        );
    }
    let deadline = Instant::now() + Duration::from_secs(5);
    loop {
        node.poll(MonoTime(2000), NodePollBudget::default())
            .unwrap();
        let core = node.local().owner.core(group()).unwrap();
        let log = core.state();
        if core.role() == voteboat::raft::Role::Leader
            && log.hard_state.term > before.term
            && log.term_at(log.commit_index) == Some(log.hard_state.term)
            && node.local().applications[&group()].applied_index() == log.commit_index
        {
            assert_eq!(
                node.local().applications[&group()].read_at(log.commit_index, ()),
                Ok(0)
            );
            return;
        }
        assert!(
            Instant::now() < deadline,
            "native default election must finish"
        );
        std::thread::park_timeout(Duration::from_millis(1));
    }
}

fn rotation(protocol: NativePeerProtocol) -> NativePeerRotationStartup {
    NativePeerRotationStartup {
        protocol,
        generation: CredentialGeneration::new(1).unwrap(),
        latest: None,
    }
}

fn selected_node(
    protocol: NativePeerProtocol,
    member: bool,
    rotated: bool,
) -> (
    PathBuf,
    NativeNode<HostApplication, voteboat::native::connect::NativeServiceConnector>,
) {
    let directory = root();
    if member {
        let mut seed = config(directory.clone(), NativeOpenMode::Create);
        seed.tls = seed.tls.with_wire_version(7).unwrap();
        let seed = if rotated {
            seed.open_with_peer_rotation(
                rotation(protocol),
                app(),
                Arc::new(ThreadWake::current()),
                MonoTime(0),
            )
        } else {
            seed.open_with_protocol(
                protocol,
                app(),
                Arc::new(ThreadWake::current()),
                MonoTime(0),
            )
        }
        .unwrap();
        drain_wire_nodes(vec![seed], Instant::now());
    }
    let mode = if member {
        NativeOpenMode::Recover
    } else {
        NativeOpenMode::Create
    };
    let mut selected = config(directory.clone(), mode);
    selected.tls = selected.tls.with_wire_version(7).unwrap();
    let wake = Arc::new(ThreadWake::current());
    let node = if member {
        let selected = NativeMemberStartup {
            provisioned_stores: selected.bootstrap.voter_stores.clone(),
            startup: selected,
        };
        if rotated {
            selected.open_with_peer_rotation(rotation(protocol), app(), wake, MonoTime(0))
        } else {
            selected.open_with_protocol(protocol, app(), wake, MonoTime(0))
        }
    } else if rotated {
        selected.open_with_peer_rotation(rotation(protocol), app(), wake, MonoTime(0))
    } else {
        selected.open_with_protocol(protocol, app(), wake, MonoTime(0))
    }
    .unwrap();
    (directory, node)
}

#[test]
fn native_defaults_plain_tcp_use_service_election_window() {
    let directory = root();
    let mut node = config(directory.clone(), NativeOpenMode::Create)
        .open(app(), Arc::new(ThreadWake::current()), MonoTime(0))
        .unwrap();
    check_election_window(&mut node, false);
    close(node);
    std::fs::remove_dir_all(directory).unwrap();
}

#[test]
fn native_defaults_static_member_and_rotated_use_service_election_window() {
    let protocols = [
        NativePeerProtocol::TcpTls,
        #[cfg(feature = "quic")]
        NativePeerProtocol::Quic,
    ];
    for protocol in protocols {
        for (member, rotated) in [(false, false), (true, false), (false, true), (true, true)] {
            let (directory, mut node) = selected_node(protocol, member, rotated);
            check_election_window(&mut node, false);
            drain_wire_nodes(
                vec![node],
                Instant::now().checked_sub(Duration::from_secs(2)).unwrap(),
            );
            std::fs::remove_dir_all(directory).unwrap();
        }
    }
}

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
