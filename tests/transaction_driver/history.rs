// SPDX-License-Identifier: RPL-1.5
// Copyright (c) 2026 Karl Livesey
use super::{cluster::*, drive::*};
use voteboat::{native::connect::NativePeerProtocol, transaction::*};

fn commit_refusal(protocol: NativePeerProtocol) {
    let _serial = serial();
    let mut host = Cluster::new(protocol, 4, [50, 20]);
    let clock = host.clock;
    let first = spec(1, 7, false);
    let original;
    {
        let mut driver = attach(&mut host, 2);
        let ticket = driver.submit(first).unwrap();
        original = finished(&mut driver, clock, ticket, TransferDecision::Commit);
        let retry = driver.submit(first).unwrap();
        assert_eq!(
            finished(&mut driver, clock, retry, TransferDecision::Commit),
            original
        );
        let reverse = driver.submit(spec(2, 3, true)).unwrap();
        finished(&mut driver, clock, reverse, TransferDecision::Commit);
        let refused = driver.submit(spec(3, 1000, false)).unwrap();
        finished(&mut driver, clock, refused, TransferDecision::Abort);
        driver.close();
        assert_eq!(
            driver.submit(spec(4, 1, false)).unwrap_err().reason,
            DriverError::Closed
        );
        assert!(driver.is_drained());
        assert_eq!(driver.usage(), DriverUsage::default());
    }
    assert_eq!(host.balance(0), AccountRead::Balance(46));
    assert_eq!(host.balance(1), AccountRead::Balance(24));
    for group in groups() {
        let retained = host.status(group, first.id()).unwrap();
        assert_eq!(retained.spec, first);
        let phase = if group == profile().coordinator() {
            TransferPhase::Finished(TransferDecision::Commit)
        } else {
            TransferPhase::Released(TransferDecision::Commit)
        };
        assert_eq!(retained.phase, phase);
    }
    host.finish();
}
#[test]
fn native_tcp_commit_retry_reverse_and_durable_refusal() {
    commit_refusal(NativePeerProtocol::TcpTls);
}
#[cfg(feature = "quic")]
#[test]
fn native_quic_commit_retry_reverse_and_durable_refusal() {
    commit_refusal(NativePeerProtocol::Quic);
}

fn capacity(protocol: NativePeerProtocol) {
    let _serial = serial();
    let mut host = Cluster::new(protocol, 1, [10, 0]);
    let clock = host.clock;
    let first = spec(10, 2, false);
    {
        let mut driver = attach(&mut host, 1);
        let ticket = driver.submit(first).unwrap();
        assert_eq!(
            driver.submit(spec(11, 1, false)).unwrap_err().reason,
            DriverError::Overloaded
        );
        finished(&mut driver, clock, ticket, TransferDecision::Commit);
        let retry = driver.submit(first).unwrap();
        finished(&mut driver, clock, retry, TransferDecision::Commit);
        // Lifetime exhaustion must remain a rejection/unknown outcome, never a
        // fabricated business Abort or an attempt at a participant prepare.
        assert_eq!(
            driver.submit(spec(11, 1, false)).unwrap_err().reason,
            DriverError::Application(voteboat::application::ApplicationError::DedupCapacity)
        );
        assert!(driver.is_drained());
    }
    for group in groups() {
        assert!(host
            .status(group, TransactionId::new(11).unwrap())
            .is_none());
    }
    assert_eq!(host.balance(0), AccountRead::Balance(8));
    assert_eq!(host.balance(1), AccountRead::Balance(2));
    host.finish();
}
#[test]
fn native_tcp_lifetime_capacity_and_exact_retry() {
    capacity(NativePeerProtocol::TcpTls);
}
#[cfg(feature = "quic")]
#[test]
fn native_quic_lifetime_capacity_and_exact_retry() {
    capacity(NativePeerProtocol::Quic);
}
