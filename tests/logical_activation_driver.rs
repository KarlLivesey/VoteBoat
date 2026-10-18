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
#![cfg(feature = "tls")]
//! Actual three physical child voters and one logical parent voter. This target
//! qualifies original parent observation ownership, not membership activation.
use std::time::Duration;
use voteboat::{
    identity::*, logical_voter::*, native::connect::NativePeerProtocol, raft::Role, runtime::*,
};
#[path = "logical_voter_native/aliases.rs"]
pub mod aliases;
#[path = "logical_activation_application/application.rs"]
mod application;
#[path = "logical_activation_driver/bindings.rs"]
mod bindings;
#[path = "logical_activation_driver/cancel.rs"]
mod cancel;
#[path = "logical_voter_native/cleanup.rs"]
pub mod cleanup;
#[path = "logical_voter_native/cluster.rs"]
pub mod cluster;
#[path = "logical_activation_driver/cohort.rs"]
mod cohort;
#[path = "logical_voter_native/drive.rs"]
pub mod drive;
#[path = "logical_activation_driver/enrollment.rs"]
mod enrollment;
#[path = "logical_activation_driver/fixture.rs"]
mod fixture;
#[path = "logical_voter_native/harness.rs"]
pub mod harness;
#[path = "logical_activation_driver/history.rs"]
mod history;
#[path = "logical_activation_driver/intake.rs"]
mod intake;
#[path = "logical_activation_driver/learners.rs"]
mod learners;
#[path = "logical_activation_driver/member.rs"]
mod member;
#[path = "logical_activation_driver/progress.rs"]
mod progress;
#[path = "logical_activation_driver/read_intake.rs"]
mod read_intake;
#[path = "logical_activation_driver/readiness.rs"]
mod readiness;
#[path = "logical_voter_native/setup.rs"]
pub mod setup;
#[path = "logical_activation_driver/source_export.rs"]
mod source_export;
pub mod support;

// HISTORY serializes independent test-owned hosts; it contains no shared state
// that another test must discard after a panic.
fn history_guard() -> std::sync::MutexGuard<'static, ()> {
    cluster::HISTORY
        .lock()
        .unwrap_or_else(std::sync::PoisonError::into_inner)
}

#[test]
fn serial_history_recovers_after_a_test_panic() {
    let failed = std::panic::catch_unwind(|| {
        let _serial = history_guard();
        panic!("intentional test-local serialization poison");
    });
    assert!(failed.is_err());
    let _serial = history_guard();
    assert!(cluster::HISTORY.is_poisoned());
    cluster::HISTORY.clear_poison();
}

#[test]
fn port_lease_owns_both_halves_and_refuses_while_the_node_is_absent() {
    use std::{
        io::{ErrorKind, Read},
        net::{TcpListener, TcpStream, UdpSocket},
    };
    let mut lease = harness::PortLease::acquire(NativePeerProtocol::TcpTls);
    let address = lease.address();
    // An absent node's held listener closes each connection at once.
    let mut stream = TcpStream::connect(address).unwrap();
    stream.set_read_timeout(Some(harness::HANG)).unwrap();
    match stream.read(&mut [0]) {
        Ok(n) => assert_eq!(n, 0),
        Err(e) => assert_eq!(e.kind(), ErrorKind::ConnectionReset),
    }
    // No other socket or lease can take either half.
    assert_eq!(
        TcpListener::bind(address).unwrap_err().kind(),
        ErrorKind::AddrInUse
    );
    assert_eq!(
        UdpSocket::bind(address).unwrap_err().kind(),
        ErrorKind::AddrInUse
    );
    assert_ne!(
        harness::PortLease::acquire(NativePeerProtocol::TcpTls).address(),
        address
    );
    // The node binds its own half; the unused half stays with the lease.
    lease.handoff();
    let node = TcpListener::bind(address).unwrap();
    assert_eq!(
        UdpSocket::bind(address).unwrap_err().kind(),
        ErrorKind::AddrInUse
    );
    drop(node);
    lease.reclaim();
    assert_eq!(
        TcpListener::bind(address).unwrap_err().kind(),
        ErrorKind::AddrInUse
    );
}
