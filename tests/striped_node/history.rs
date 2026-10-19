// SPDX-License-Identifier: RPL-1.5
use super::cluster::{serial, Cluster};
use std::time::{Duration, Instant};
use voteboat::{
    application::{CounterOutcome, CounterReceipt},
    identity::OperationId,
    native::connect::NativePeerProtocol,
    runtime::*,
};
fn write(
    host: &mut Cluster,
    operation: u128,
    add: i64,
    expected: i64,
    duplicate: bool,
) -> CounterReceipt {
    host.wait(|h| h.leader().is_some());
    let leader = host.leader().unwrap();
    let ticket = host.nodes[leader]
        .propose(ClientRequest {
            group: host.group,
            operation: OperationId::new(operation).unwrap(),
            bytes: add.to_le_bytes().to_vec(),
        })
        .unwrap();
    let end = Instant::now() + Duration::from_secs(15);
    loop {
        host.poll();
        if let Some(output) = host.nodes[leader].poll_client() {
            assert_eq!(output.ticket(), ticket);
            match host.nodes[leader].complete_client(output).unwrap() {
                ClientOutcome::Applied { position, receipt } => {
                    assert!(position.index > 0 && position.term > 0);
                    assert_eq!(receipt.index, position.index);
                    assert_eq!(receipt.operation, ticket.operation);
                    assert_eq!(receipt.outcome, CounterOutcome::Value(expected));
                    assert_eq!(receipt.duplicate, duplicate);
                    assert_eq!(host.nodes[leader].local().clients.usage().requests, 0);
                    return receipt;
                }
                other => panic!("original striped invocation did not apply: {other:?}"),
            }
        }
        assert!(
            Instant::now() < end,
            "original striped command completion timed out"
        );
    }
}
fn read(host: &mut Cluster, expected: i64) {
    host.wait(|h| h.leader().is_some());
    let leader = host.leader().unwrap();
    let ticket = host.nodes[leader].read(host.group, ()).unwrap();
    let end = Instant::now() + Duration::from_secs(15);
    loop {
        host.poll();
        if let Some(output) = host.nodes[leader].poll_read() {
            assert_eq!(output.ticket(), ticket);
            match host.nodes[leader].complete_read(output).unwrap() {
                ReadOutcome::Read {
                    barrier,
                    result: Ok(value),
                } => {
                    assert_eq!(barrier.group(), host.group);
                    assert_eq!(barrier.request(), ticket.request);
                    assert_eq!(value, expected);
                    assert_eq!(host.nodes[leader].local().reads.usage().requests, 0);
                    return;
                }
                other => panic!("actual striped quorum read refused: {other:?}"),
            }
        }
        assert!(Instant::now() < end, "actual striped fresh read timed out");
    }
}
fn files(host: &Cluster) {
    for id in 1..=3 {
        for lane in host.lanes(id) {
            for forbidden in [
                "log.wal",
                "CURRENT",
                "MANIFEST",
                "votes.wal",
                "log.0.wal",
                "log.1.wal",
            ] {
                assert!(!lane.join(forbidden).exists(), "no duplicate ordinary WAL");
            }
            assert!(lane.join("STRIPE-LANE").is_file());
            assert!(lane.join("STRIPE-CATALOG").is_file());
            let bytes = [0, 1]
                .into_iter()
                .map(|slot| {
                    std::fs::metadata(lane.join(format!("stripe.{slot}.wal")))
                        .unwrap()
                        .len()
                })
                .sum::<u64>();
            assert!(
                bytes > 0,
                "all fixed lanes hold actual native-record payload chunks"
            );
        }
    }
}
fn history(protocol: NativePeerProtocol) {
    let _serial = serial();
    let mut host = Cluster::new(protocol);
    host.campaign();
    write(&mut host, 1, 7, 7, false);
    write(&mut host, 1, 7, 7, true);
    read(&mut host, 7);
    host.converge(7);
    let bases = host.checkpoint();
    let second = write(&mut host, 2, 3, 10, false);
    let third = write(&mut host, 3, 4, 14, false);
    assert!(third.index > second.index);
    read(&mut host, 14);
    host.converge(14);
    for (node, base) in host.nodes.iter().zip(&bases) {
        let state = node.local().owner.core(host.group).unwrap().state();
        assert_eq!(state.base_index(), *base);
        assert!(state.commit_index > *base);
    }
    files(&host);
    host.close();
    host.open(true); // fresh own checkpoint+striped suffix assertions run before polling.
    for (node, base) in host.nodes.iter().zip(&bases) {
        assert_eq!(
            node.local()
                .owner
                .core(host.group)
                .unwrap()
                .state()
                .base_index(),
            *base
        );
    }
    host.campaign();
    write(&mut host, 1, 7, 7, true);
    write(&mut host, 2, 3, 10, true);
    write(&mut host, 3, 4, 14, true);
    read(&mut host, 14);
    write(&mut host, 4, 1, 15, false);
    read(&mut host, 15);
    host.converge(15);
    host.finish();
}
#[test]
fn tcp_striped_node_original_results_checkpoint_suffix_reopen() {
    history(NativePeerProtocol::TcpTls);
}
#[cfg(feature = "quic")]
#[test]
fn quic_striped_node_original_results_checkpoint_suffix_reopen() {
    history(NativePeerProtocol::Quic);
}
