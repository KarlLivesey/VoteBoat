// SPDX-License-Identifier: RPL-1.5
// Copyright (c) 2026 Karl Livesey
use super::{
    cluster::{serial, Cluster},
    host::Host,
    lower::Lower,
    middle,
    read::read,
    write::write,
};
use std::time::Instant;
use voteboat::native::connect::NativePeerProtocol;
fn history(protocol: NativePeerProtocol) {
    let _serial = serial();
    let clock = Instant::now();
    let mut bottom = (0..9)
        .map(|i| Cluster::new(protocol, i, clock))
        .collect::<Vec<_>>();
    let mut lower = Lower::new(&mut bottom, clock);
    let images = lower.claims(100000);
    let (mut middle, channels) = middle::build(&images, protocol, &mut lower);
    middle::elect(&mut lower, &mut middle);
    let mut host = Host::new(lower, &mut middle, protocol, 200000, &channels);
    host.elect();
    let first = write(&mut host, 300000, 5, false);
    let duplicate = write(&mut host, 300000, 5, true);
    assert_eq!(first.receipt.operation, duplicate.receipt.operation);
    assert!(duplicate.receipt.duplicate);
    read(&mut host);
    super::convergence::finish(&mut host);
    let mut lower = host.close();
    middle::close(&mut lower, middle, channels);
    drop(lower);
    for mut c in bottom {
        c.checkpoint();
        c.finish();
    }
}
#[test]
fn tcp_two_logical_levels_use_twenty_seven_native_owners() {
    history(NativePeerProtocol::TcpTls);
}
#[cfg(feature = "quic")]
#[test]
fn quic_two_logical_levels_use_twenty_seven_native_owners() {
    history(NativePeerProtocol::Quic);
}
