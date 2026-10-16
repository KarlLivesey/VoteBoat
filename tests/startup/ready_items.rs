// SPDX-License-Identifier: RPL-1.5
// Copyright (c) 2026 Karl Livesey
use super::*;
use voteboat::{native::connect::NativePeerProtocol, raft::Role};
#[path = "ready_items/history.rs"]
mod history;
fn invalid(
    directory: &std::path::Path,
    listen: std::net::SocketAddr,
    items: usize,
    mode: NativeOpenMode,
) -> NativeStartup {
    let mut startup = config(directory.to_path_buf(), mode);
    startup.listen = listen;
    startup.ready_visit_items = items;
    startup.tls = startup.tls.with_wire_version(8).unwrap();
    startup
}
fn refused<T, A>(result: Result<T, Box<NativeStartupRejected<A>>>) {
    let mut rejected = result.err().expect("invalid quantum admitted");
    assert_eq!(rejected.reason.stage, "runtime limits");
    assert!(rejected.application.is_some());
    assert!(rejected.try_cleanup().unwrap());
}
fn rotation() -> NativePeerRotationStartup {
    NativePeerRotationStartup {
        protocol: NativePeerProtocol::TcpTls,
        generation: voteboat::authorization::CredentialGeneration::new(1).unwrap(),
        latest: None,
    }
}
#[test]
fn invalid_quantum_refuses_every_startup_branch_before_files_listeners_or_workers() {
    let occupied = std::net::TcpListener::bind("127.0.0.1:0").unwrap();
    let listen = occupied.local_addr().unwrap();
    for items in [0, 1025, usize::MAX] {
        let directory = root();
        let wake = || Arc::new(ThreadWake::current());
        let startup = || invalid(&directory, listen, items, NativeOpenMode::Create);
        refused(startup().open(app(), wake(), MonoTime(0)));
        refused(startup().open_with_protocol(
            NativePeerProtocol::TcpTls,
            app(),
            wake(),
            MonoTime(0),
        ));
        refused(startup().prepare_for_discovery(
            NativePeerProtocol::TcpTls,
            TimerConfig::default(),
            app(),
            wake(),
            MonoTime(0),
        ));
        refused(startup().prepare_discovery_with_peer_rotation(
            rotation(),
            TimerConfig::default(),
            app(),
            wake(),
            MonoTime(0),
        ));
        let member = || {
            let startup = invalid(&directory, listen, items, NativeOpenMode::Recover);
            NativeMemberStartup {
                provisioned_stores: startup.bootstrap.voter_stores.clone(),
                startup,
            }
        };
        refused(member().open_with_protocol(
            NativePeerProtocol::TcpTls,
            app(),
            wake(),
            MonoTime(0),
        ));
        refused(member().prepare_for_discovery(
            NativePeerProtocol::TcpTls,
            TimerConfig::default(),
            app(),
            wake(),
            MonoTime(0),
        ));
        let multi = || {
            let startup = startup();
            NativeMultiStartup {
                provisioned_stores: startup.bootstrap.voter_stores.clone(),
                startup,
                additional_groups: vec![],
            }
        };
        refused(multi().open(
            NativePeerProtocol::TcpTls,
            TimerConfig::default(),
            [(group(), app())].into(),
            wake(),
            MonoTime(0),
        ));
        refused(multi().prepare_discovery_with_peer_rotation(
            rotation(),
            TimerConfig::default(),
            [(group(), app())].into(),
            wake(),
            MonoTime(0),
        ));
        #[cfg(feature = "quic")]
        refused(startup().prepare_for_discovery(
            NativePeerProtocol::Quic,
            TimerConfig::default(),
            app(),
            wake(),
            MonoTime(0),
        ));
        assert!(!directory.exists());
    }
}
#[test]
fn enrollment_rejects_invalid_quantum_before_importing_or_altering_application() {
    for items in [0, 1025, usize::MAX] {
        let directory = root();
        let mut startup = config(directory.clone(), NativeOpenMode::Create);
        startup.ready_visit_items = items;
        startup.tls = startup.tls.with_wire_version(8).unwrap();
        let incoming = voteboat::snapshot::Snapshot {
            metadata: voteboat::snapshot::SnapshotMetadata {
                bootstrap: startup.bootstrap.clone(),
                membership: None,
                index: 1,
                term: 1,
                application_schema: 1,
            },
            application: vec![7],
        };
        let member = NativeMemberStartup {
            provisioned_stores: startup.bootstrap.voter_stores.clone(),
            startup,
        };
        let mut application = app();
        assert_eq!(
            member
                .enroll_snapshot(&incoming, &mut application)
                .unwrap_err()
                .stage,
            "runtime limits"
        );
        assert_eq!(application.applied_index(), 0);
        assert!(!directory.exists());
    }
}
