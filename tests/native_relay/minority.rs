// SPDX-License-Identifier: RPL-1.5
// Copyright (c) 2026 Karl Livesey
//! A live authenticated nonvoting helper cannot satisfy an isolated leader's quorum.
use super::{
    cluster::*,
    history::{finish, read},
    support::*,
};
use voteboat::{identity::OperationId, native::connect::NativePeerProtocol, runtime::*};
fn history(protocol: NativePeerProtocol) {
    let mut c = Cluster::new(protocol);
    c.campaign();
    c.until(|c| {
        c.boats[0]
            .local()
            .owner
            .core(group(352))
            .unwrap()
            .state()
            .commit_index
            >= 1
    });
    let committed = c.boats[0]
        .local()
        .owner
        .core(group(352))
        .unwrap()
        .state()
        .commit_index;
    c.gates[1].writes.set(true);
    c.gates[2].writes.set(true);
    assert!(c.helper.source_capture(node(2)).is_some());
    let write = c.boats[0]
        .propose(ClientRequest {
            group: group(352),
            operation: OperationId::new(35241).unwrap(),
            bytes: 5i64.to_le_bytes().to_vec(),
        })
        .unwrap();
    let query = c.boats[0].read(group(352), ()).unwrap();
    let until = std::time::Instant::now() + std::time::Duration::from_millis(250);
    while std::time::Instant::now() < until {
        c.poll();
        std::thread::park_timeout(std::time::Duration::from_millis(1));
    }
    assert_eq!(
        c.boats[0]
            .local()
            .owner
            .core(group(352))
            .unwrap()
            .state()
            .commit_index,
        committed
    );
    assert_eq!(c.value(0), 0);
    assert!(
        c.boats[0].poll_client().is_none(),
        "local/helper IO did not commit a voter write"
    );
    assert!(
        c.boats[0].poll_read().is_none(),
        "authenticated helper liveness did not make a fresh read quorum"
    );
    c.gates[1].writes.set(false);
    c.gates[2].writes.set(false);
    let mut wrote = false;
    let mut read_ready = false;
    let end = std::time::Instant::now() + std::time::Duration::from_secs(15);
    while !wrote || !read_ready {
        c.poll();
        if let Some(reply) = c.boats[0].poll_client() {
            assert_eq!(reply.ticket(), write);
            assert!(matches!(
                c.boats[0].complete_client(reply).unwrap(),
                ClientOutcome::Applied { .. }
            ));
            wrote = true;
        }
        if let Some(reply) = c.boats[0].poll_read() {
            assert_eq!(reply.ticket(), query);
            let ReadOutcome::Read { result, .. } = c.boats[0].complete_read(reply).unwrap() else {
                panic!("fresh voter read absent");
            };
            assert!(matches!(result, Ok(0 | 5)));
            read_ready = true;
        }
        assert!(
            std::time::Instant::now() < end,
            "restored voter majority did not progress"
        );
        std::thread::park_timeout(std::time::Duration::from_millis(1));
    }
    assert_eq!(read(&mut c), 5);
    c.until(|c| (0..3).all(|i| c.value(i) == 5));
    std::fs::remove_dir_all(finish(c)).unwrap();
}
#[test]
fn tcp_live_helper_never_replaces_the_physical_voter_majority() {
    history(NativePeerProtocol::TcpTls);
}
#[cfg(feature = "quic")]
#[test]
fn quic_live_helper_never_replaces_the_physical_voter_majority() {
    history(NativePeerProtocol::Quic);
}
