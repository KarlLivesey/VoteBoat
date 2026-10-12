// SPDX-License-Identifier: RPL-1.5
// Copyright (c) 2026 Karl Livesey
// Unless explicitly acquired and licensed from Licensor under another license,
// the contents of this file are subject to the Reciprocal Public License
// ("RPL") Version 1.5, or subsequent versions as allowed by the RPL, and You may
// not copy or use this file in either source code or executable form, except
// in compliance with the terms and conditions of the RPL.
//
// All software distributed under the RPL is provided strictly on an "AS IS"
// basis, WITHOUT WARRANTY OF ANY KIND, EITHER EXPRESS OR IMPLIED, AND LICENSOR
// HEREBY DISCLAIMS ALL SUCH WARRANTIES, INCLUDING WITHOUT LIMITATION, ANY
// WARRANTIES OF MERCHANTABILITY, FITNESS FOR A PARTICULAR PURPOSE, QUIET
// ENJOYMENT, OR NON-INFRINGEMENT. See the RPL for specific language governing
// rights and limitations under the RPL.
use super::*;
use voteboat::{
    native::{
        connect::{NativePeerProtocol, NativeServiceConnector},
        log_store::{FileLogIo, NativeLogStore},
    },
    raft::RaftError,
};
#[path = "log_limits/application.rs"]
mod application;
use application::LargeApplication;
const COMMAND_BYTES: usize = 1_069_056;
const DEFAULT_REFUSAL_BYTES: usize = 70_704;
type Boat = NativeNode<LargeApplication, NativeServiceConnector>;
fn selected() -> LogLimits {
    LogLimits {
        max_command_bytes: COMMAND_BYTES,
        max_batch_bytes: 33_587_200,
        ..Default::default()
    }
}
fn open(directory: &std::path::Path, mode: NativeOpenMode, limits: Option<LogLimits>) -> Boat {
    let mut config = config(directory.to_path_buf(), mode);
    config.tls = config.tls.with_wire_version(8).unwrap();
    let wake = Arc::new(ThreadWake::current());
    let app = LargeApplication(app());
    let mut parts = match (mode, limits) {
        (NativeOpenMode::Recover, Some(limits)) => NativeMemberStartup {
            provisioned_stores: config.bootstrap.voter_stores.clone(),
            startup: config,
        }
        .prepare_for_discovery_with_log_limits(
            NativePeerProtocol::TcpTls,
            TimerConfig::default(),
            limits,
            app,
            wake,
            MonoTime(0),
        ),
        (_, Some(limits)) => config.prepare_for_discovery_with_log_limits(
            NativePeerProtocol::TcpTls,
            TimerConfig::default(),
            limits,
            app,
            wake,
            MonoTime(0),
        ),
        (_, None) => config.prepare_for_discovery(
            NativePeerProtocol::TcpTls,
            TimerConfig::default(),
            app,
            wake,
            MonoTime(0),
        ),
    }
    .unwrap();
    // Still-empty original client binding; the selected WAL/core must enforce
    // its own cap even when the host client router accepts this larger input.
    parts.local.clients = ClientRouter::new(
        parts.local.clients.binding(),
        ClientRouterLimits {
            command_bytes: COMMAND_BYTES,
            ..Default::default()
        },
    )
    .unwrap();
    let codec = voteboat::wire_format::NativeWireCodec::with_leadership_transfer(
        voteboat::wire::WireLimits {
            max_frame_bytes: 2 * COMMAND_BYTES,
            max_command_bytes: COMMAND_BYTES,
            ..Default::default()
        },
    )
    .unwrap();
    parts.peers.as_mut().unwrap().factory =
        voteboat::native::transport::NativeTransportFactory::new(
            codec,
            voteboat::transport::TransportLimits {
                send_frame_bytes: 2 * COMMAND_BYTES,
                receive_frame_bytes: 2 * COMMAND_BYTES,
                ..Default::default()
            },
        )
        .unwrap();
    Node::from_parts(parts, NodeLimits::default(), MonoTime(0))
        .unwrap_or_else(|e| panic!("{:?}", e.reason))
}
fn poll(node: &mut Boat, clock: Instant) {
    node.poll(
        MonoTime(clock.elapsed().as_millis() as u64),
        NodePollBudget::default(),
    )
    .unwrap();
}
fn wait(node: &mut Boat, clock: Instant, done: impl Fn(&Boat) -> bool) {
    let end = Instant::now() + Duration::from_secs(5);
    while !done(node) {
        poll(node, clock);
        assert!(Instant::now() < end);
        std::thread::park_timeout(Duration::from_millis(1));
    }
}
fn write(node: &mut Boat, clock: Instant, command_bytes: usize) -> ClientOutcome<CounterReceipt> {
    node.control(group(), NodeControl::Campaign).unwrap();
    wait(node, clock, |n| {
        n.local().owner.core(group()).unwrap().role() == voteboat::raft::Role::Leader
            && n.local().applications[&group()].applied_index() > 0
    });
    let mut bytes = vec![0; command_bytes];
    bytes[..8].copy_from_slice(&7i64.to_le_bytes());
    let ticket = node
        .propose(ClientRequest {
            group: group(),
            operation: OperationId::new(1).unwrap(),
            bytes,
        })
        .unwrap_or_else(|e| {
            panic!(
                "original command admission {:?}; bytes={}",
                e.reason,
                e.request.bytes.len()
            )
        });
    let end = Instant::now() + Duration::from_secs(5);
    loop {
        poll(node, clock);
        if let Some(original) = node.poll_client() {
            assert_eq!(original.ticket(), ticket);
            return node.complete_client(original).unwrap();
        }
        assert!(Instant::now() < end);
        std::thread::park_timeout(Duration::from_millis(1));
    }
}
fn close(mut node: Boat, clock: Instant) -> LogLimits {
    node.begin_shutdown();
    wait(&mut node, clock, |n| n.is_drained());
    let mut parts = node.into_parts().unwrap_or_else(|_| panic!("not drained"));
    let mut dialer = parts
        .peers
        .take()
        .unwrap()
        .connector
        .into_dialer()
        .ok()
        .unwrap();
    let mut snapshots = parts.local.snapshots.take().unwrap();
    let end = Instant::now() + Duration::from_secs(5);
    let mut store = None;
    let mut snapshot_done = false;
    loop {
        let peer_done = dialer.as_mut().is_none_or(|d| d.try_finish().unwrap());
        if store.is_none() {
            store = parts.local.persistence.try_reclaim().unwrap();
        }
        if !snapshot_done {
            snapshot_done = snapshots.worker.try_reclaim().unwrap().is_some();
        }
        if store.is_some() && snapshot_done && peer_done {
            break;
        }
        assert!(Instant::now() < end);
        std::thread::park_timeout(Duration::from_millis(1));
    }
    store.unwrap().limits()
}
#[test]
fn selected_native_create_and_member_recovery_preserve_original_large_command() {
    let directory = root();
    let clock = Instant::now();
    let mut node = open(&directory, NativeOpenMode::Create, Some(selected()));
    assert!(matches!(
        write(&mut node, clock, COMMAND_BYTES),
        ClientOutcome::Applied {
            receipt: CounterReceipt {
                outcome: CounterOutcome::Value(7),
                duplicate: false,
                ..
            },
            ..
        }
    ));
    assert_eq!(close(node, clock).max_command_bytes, COMMAND_BYTES);
    // Reopening the acknowledged larger frame through the unchanged default
    // profile is refused; recovery must never silently truncate that history.
    let mut rejected = config(directory.clone(), NativeOpenMode::Recover)
        .prepare_for_discovery(
            NativePeerProtocol::TcpTls,
            TimerConfig::default(),
            LargeApplication(app()),
            Arc::new(ThreadWake::current()),
            MonoTime(0),
        )
        .err()
        .unwrap();
    assert_eq!(rejected.application.as_ref().unwrap().applied_index(), 0);
    assert!(rejected.try_cleanup().unwrap());
    let clock = Instant::now();
    let mut node = open(&directory, NativeOpenMode::Recover, Some(selected()));
    assert_eq!(
        node.local().applications[&group()]
            .0
             .0
            .read_applied(node.local().applications[&group()].applied_index())
            .unwrap(),
        7
    );
    assert!(matches!(
        write(&mut node, clock, COMMAND_BYTES),
        ClientOutcome::Applied {
            receipt: CounterReceipt {
                outcome: CounterOutcome::Value(7),
                duplicate: true,
                ..
            },
            ..
        }
    ));
    assert_eq!(close(node, clock).max_command_bytes, COMMAND_BYTES);
    // Independent original file reopen also validates the full retained bytes.
    let config = config(directory.clone(), NativeOpenMode::Recover);
    let log = NativeLogStore::recover(
        FileLogIo::open(&directory).unwrap(),
        config.store,
        selected(),
    )
    .unwrap();
    assert!(log.state(group()).unwrap().entries.iter().any(
        |e| matches!(&e.payload,EntryPayload::Command {bytes,..} if bytes.len()==COMMAND_BYTES)
    ));
    drop(log);
    std::fs::remove_dir_all(directory).unwrap();
}
#[test]
fn default_native_prepare_keeps_default_wal_and_core_command_refusal() {
    let directory = root();
    for mode in [NativeOpenMode::Create, NativeOpenMode::Recover] {
        let clock = Instant::now();
        let mut node = open(&directory, mode, None);
        assert!(matches!(
            write(&mut node, clock, DEFAULT_REFUSAL_BYTES),
            ClientOutcome::NotProposed(RaftError::Storage(_))
        ));
        assert_eq!(
            close(node, clock).max_command_bytes,
            LogLimits::default().max_command_bytes
        );
    }
    std::fs::remove_dir_all(directory).unwrap();
}
#[test]
fn invalid_native_selected_limits_refuse_before_files_or_listener_binding() {
    for member in [false, true] {
        for limits in [
            LogLimits {
                max_command_bytes: 0,
                ..selected()
            },
            LogLimits {
                max_batch_units: 1,
                ..selected()
            },
        ] {
            let directory = root();
            let mut config = config(directory.clone(), NativeOpenMode::Create);
            let listener = std::net::TcpListener::bind("127.0.0.1:0").unwrap();
            config.listen = listener.local_addr().unwrap();
            let mut rejected = if member {
                NativeMemberStartup {
                    provisioned_stores: config.bootstrap.voter_stores.clone(),
                    startup: config,
                }
                .prepare_for_discovery_with_log_limits(
                    NativePeerProtocol::TcpTls,
                    TimerConfig::default(),
                    limits,
                    app(),
                    Arc::new(ThreadWake::current()),
                    MonoTime(0),
                )
            } else {
                config.prepare_for_discovery_with_log_limits(
                    NativePeerProtocol::TcpTls,
                    TimerConfig::default(),
                    limits,
                    app(),
                    Arc::new(ThreadWake::current()),
                    MonoTime(0),
                )
            }
            .err()
            .unwrap();
            assert_eq!(
                rejected.reason.stage,
                if limits.max_command_bytes == 0 {
                    "native startup"
                } else {
                    "storage limits"
                }
            );
            assert_eq!(rejected.application.as_ref().unwrap().applied_index(), 0);
            assert!(rejected.try_cleanup().unwrap());
            assert!(!directory.exists());
            drop(listener);
        }
    }
}

#[test]
fn invalid_selected_enrollment_limits_leave_original_application_and_files_untouched() {
    use voteboat::snapshot::{Snapshot, SnapshotMetadata};
    let directory = root();
    let startup = config(directory.clone(), NativeOpenMode::Create);
    let incoming = Snapshot {
        metadata: SnapshotMetadata {
            bootstrap: startup.bootstrap.clone(),
            membership: None,
            index: 1,
            term: 1,
            application_schema: 1,
        },
        application: vec![],
    };
    let selected = NativeMemberStartup {
        provisioned_stores: startup.bootstrap.voter_stores.clone(),
        startup,
    };
    let mut application = app();
    let before = application.checkpoint(4096).unwrap();
    assert!(selected
        .enroll_snapshot_with_log_limits(
            &incoming,
            &mut application,
            LogLimits {
                max_command_bytes: 0,
                ..Default::default()
            }
        )
        .is_err());
    assert_eq!(application.checkpoint(4096).unwrap(), before);
    assert!(!directory.exists());
}
