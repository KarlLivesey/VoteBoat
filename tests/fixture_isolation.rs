// SPDX-License-Identifier: RPL-1.5
// Copyright (c) 2026 Karl Livesey
#![cfg(feature = "native")]
#[path = "support/ports.rs"]
mod ports;
#[path = "support/serial.rs"]
mod serial;

use std::{
    fs::OpenOptions,
    net::{TcpListener, UdpSocket},
    process::Command,
    sync::Mutex,
};

// Even CLOEXEC descriptors are present between another thread's fork and exec.
// Do not fork while asserting that this process's unwind closed the last copy.
static SPAWN: Mutex<()> = Mutex::new(());

#[test]
fn lease_child() {
    let Ok(port) = std::env::var("VOTEBOAT_TEST_LEASE_PARENT") else {
        return;
    };
    let port = port.parse::<u16>().unwrap();
    let path = std::env::temp_dir().join(format!("voteboat-test-ports/{port}.lock"));
    let lock = OpenOptions::new()
        .read(true)
        .write(true)
        .open(path)
        .unwrap();
    assert!(matches!(
        lock.try_lock(),
        Err(std::fs::TryLockError::WouldBlock)
    ));
    let lease = ports::PortLease::acquire(1);
    assert_ne!(lease.addresses::<1>()[0].port(), port);
}

#[test]
fn lease_holds_across_handoff_and_processes() {
    let _spawn = serial::serial(&SPAWN);
    let mut lease = ports::PortLease::acquire(1);
    let [address] = lease.addresses();
    assert!(TcpListener::bind(address).is_err());
    assert!(UdpSocket::bind(address).is_err());
    let mut sockets = lease.take_sockets();
    let (tcp, udp) = sockets.pop().unwrap();
    assert_eq!(tcp.local_addr().unwrap(), address);
    assert_eq!(udp.local_addr().unwrap(), address);
    let output = Command::new(std::env::current_exe().unwrap())
        .args(["--exact", "lease_child"])
        .env("VOTEBOAT_TEST_LEASE_PARENT", address.port().to_string())
        .output()
        .unwrap();
    assert!(output.status.success(), "{output:?}");
    drop((tcp, udp));
    // The existing shutdown-release assertion remains protected by the lease.
    drop((
        TcpListener::bind(address).unwrap(),
        UdpSocket::bind(address).unwrap(),
    ));
}

#[test]
fn panic_releases_port_lease_and_serial_guard_without_cascades() {
    let _spawn = serial::serial(&SPAWN);
    static LOCK: Mutex<()> = Mutex::new(());
    // Socket probes remain bound; only lock release, not a free port, is
    // asserted after unwinding. No other allocator uses this private namespace.
    let directory =
        std::env::temp_dir().join(format!("voteboat-lease-unwind-{}", std::process::id()));
    let mut address = None;
    let result = std::panic::catch_unwind(std::panic::AssertUnwindSafe(|| {
        let _serial = serial::serial(&LOCK);
        let lease = ports::PortLease::acquire_in(1, &directory);
        address = Some(lease.addresses::<1>()[0]);
        panic!("intentional fixture failure");
    }));
    assert!(result.is_err());
    assert!(LOCK.is_poisoned());
    let _serial = serial::serial(&LOCK);
    let address = address.unwrap();
    let path = directory.join(format!("{}.lock", address.port()));
    let lock = OpenOptions::new()
        .read(true)
        .write(true)
        .open(path)
        .unwrap();
    lock.try_lock()
        .expect("unwinding releases the cross-process lease");
    drop(lock);
    std::fs::remove_dir_all(directory).unwrap();
    let mut lease = ports::PortLease::acquire(1);
    let [address] = lease.addresses();
    lease.handoff();
    drop((
        TcpListener::bind(address).unwrap(),
        UdpSocket::bind(address).unwrap(),
    ));
}
