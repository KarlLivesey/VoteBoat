// SPDX-License-Identifier: RPL-1.5
// Copyright (c) 2026 Karl Livesey
//! Original native ordinary-voter majority, independent of the helper's local IO.
use super::{cluster::*, support::*};
use voteboat::{application::*, native::connect::NativePeerProtocol, runtime::*};
pub fn write(c: &mut Cluster, id: u128, delta: i64) -> CounterReceipt {
    let ticket = c.boats[0]
        .propose(ClientRequest {
            group: group(352),
            operation: Counter::operation(1, id as u64).unwrap(),
            bytes: Counter::command(0, delta),
        })
        .unwrap();
    let mut receipt = None;
    let end = std::time::Instant::now() + std::time::Duration::from_secs(15);
    while receipt.is_none() {
        c.poll();
        if let Some(reply) = c.boats[0].poll_client() {
            assert_eq!(reply.ticket(), ticket);
            let ClientOutcome::Applied { receipt: r, .. } =
                c.boats[0].complete_client(reply).unwrap()
            else {
                panic!("original write not applied")
            };
            receipt = Some(r);
        }
        assert!(std::time::Instant::now() < end, "original write timed out");
        std::thread::park_timeout(std::time::Duration::from_millis(1));
    }
    receipt.unwrap()
}
pub fn read(c: &mut Cluster) -> i64 {
    let ticket = c.boats[0].read(group(352), ()).unwrap();
    complete_read(c, ticket)
}
pub fn complete_read(c: &mut Cluster, ticket: ReadInvocationTicket) -> i64 {
    let end = std::time::Instant::now() + std::time::Duration::from_secs(15);
    loop {
        c.poll();
        if let Some(reply) = c.boats[0].poll_read() {
            assert_eq!(reply.ticket(), ticket);
            let ReadOutcome::Read { result, .. } = c.boats[0].complete_read(reply).unwrap() else {
                panic!("original read not ready")
            };
            return result.unwrap();
        }
        assert!(std::time::Instant::now() < end, "original read timed out");
        std::thread::park_timeout(std::time::Duration::from_millis(1));
    }
}
fn history(protocol: NativePeerProtocol) {
    let mut c = Cluster::new(protocol);
    c.campaign();
    write(&mut c, 1, 3);
    c.until(|c| c.boats.iter().enumerate().all(|(i, _)| c.value(i) == 3));
    let view = c.boats[0].peers().unwrap().source_view(node(2)).unwrap();
    assert_eq!(view.source().node, node(2));
    assert_eq!(view.receiving_access().peer.node, node(9));
    assert!(view.direct_access().is_none());
    c.gates[2].reads.set(true);
    c.gates[2].writes.set(true);
    write(&mut c, 2, 4);
    assert_eq!(read(&mut c), 7);
    write(&mut c, 3, 5);
    assert_eq!(read(&mut c), 12);
    assert_eq!(
        c.value(2),
        3,
        "blocked third voter did not supply majority progress"
    );
    c.gates[2].reads.set(false);
    c.gates[2].writes.set(false);
    c.until(|c| c.value(2) == 12);
    std::fs::remove_dir_all(finish(c)).unwrap();
}
pub fn finish(mut c: Cluster) -> std::path::PathBuf {
    for boat in &mut c.boats {
        boat.begin_shutdown();
    }
    c.helper.close();
    for branch in &mut c.branches {
        branch.close();
    }
    c.until(|c| {
        c.boats.iter().all(|b| b.is_drained())
            && c.helper.is_drained()
            && c.branches.iter().all(|h| h.is_drained())
    });
    for boat in c.boats {
        let mut parts = boat
            .into_parts()
            .unwrap_or_else(|_| panic!("node not reclaimed"));
        assert_eq!(
            voteboat::worker::PersistenceWorker::usage(&parts.local.persistence).requests,
            0
        );
        let mut snapshots = parts.local.snapshots.take().unwrap();
        let mut log_done = false;
        let mut snapshot_done = false;
        let deadline = std::time::Instant::now() + std::time::Duration::from_secs(5);
        while !log_done || !snapshot_done {
            if !log_done {
                log_done = parts.local.persistence.try_reclaim().unwrap().is_some();
            }
            if !snapshot_done {
                snapshot_done = snapshots.worker.try_reclaim().unwrap().is_some();
            }
            assert!(
                std::time::Instant::now() < deadline,
                "original native workers not reclaimed"
            );
            std::thread::park_timeout(std::time::Duration::from_millis(1));
        }
    }
    let _ = c
        .helper
        .into_parts()
        .unwrap_or_else(|_| panic!("helper not reclaimed"));
    for branch in c.branches {
        branch
            .into_parts()
            .unwrap_or_else(|_| panic!("tree branch not reclaimed"));
    }
    c.root
}
#[test]
fn actual_tcp_relay_keeps_three_native_voters_and_progresses_without_third() {
    history(NativePeerProtocol::TcpTls);
}
#[cfg(feature = "quic")]
#[test]
fn actual_quic_relay_keeps_three_native_voters_and_progresses_without_third() {
    history(NativePeerProtocol::Quic);
}
