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
    os::unix::fs::{MetadataExt, PermissionsExt},
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
    let path = ports::shared_directory().join(format!("{port}.lock"));
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
    assert!(
        !(10000..20000).contains(&address.port()),
        "leased owners must not occupy the legacy routed candidate band"
    );
    assert!(TcpListener::bind(address).is_err());
    assert!(UdpSocket::bind(address).is_err());
    let mut sockets = lease.take_sockets();
    let (tcp, udp) = sockets.pop().unwrap();
    assert_eq!(tcp.local_addr().unwrap(), address);
    assert_eq!(udp.local_addr().unwrap(), address);
    drop((tcp, udp));
    // There are now no probes: only the common file lock protects this address.
    // Different TMPDIR values must not split that coordination namespace.
    let output = Command::new(std::env::current_exe().unwrap())
        .args(["--exact", "lease_child"])
        .env("VOTEBOAT_TEST_LEASE_PARENT", address.port().to_string())
        .env(
            "TMPDIR",
            std::env::temp_dir().join("different-child-tmpdir"),
        )
        .output()
        .unwrap();
    assert!(output.status.success(), "{output:?}");
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
    // Exercise the actual lease/Reservation Drop path with a private lock-only
    // reservation. Private controls must never bind ports from the shared pool.
    let directory =
        std::env::temp_dir().join(format!("voteboat-lease-unwind-{}", std::process::id()));
    ports::prepare_directory(&directory).unwrap();
    let path = directory.join("unwind.lock");
    let result = std::panic::catch_unwind(std::panic::AssertUnwindSafe(|| {
        let _serial = serial::serial(&LOCK);
        let lock = ports::open_lock(&path).unwrap();
        lock.try_lock().unwrap();
        let _lease = ports::PortLease {
            held: vec![ports::Reservation {
                address: "127.0.0.1:0".parse().unwrap(),
                sockets: None,
                _lock: lock,
            }],
        };
        panic!("intentional fixture failure");
    }));
    assert!(result.is_err());
    assert!(LOCK.is_poisoned());
    let _serial = serial::serial(&LOCK);
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

#[test]
fn namespace_modes_are_repaired_independently_of_umask() {
    let directory =
        std::env::temp_dir().join(format!("voteboat-lease-modes-{}", std::process::id()));
    std::fs::create_dir(&directory).unwrap();
    std::fs::set_permissions(&directory, std::fs::Permissions::from_mode(0o500)).unwrap();
    ports::prepare_directory(&directory).unwrap();
    assert_eq!(directory.metadata().unwrap().mode() & 0o7777, 0o1777);
    let path = directory.join("control.lock");
    std::fs::write(&path, []).unwrap();
    std::fs::set_permissions(&path, std::fs::Permissions::from_mode(0o600)).unwrap();
    let lock = ports::open_lock(&path).unwrap();
    assert_eq!(lock.metadata().unwrap().mode() & 0o7777, 0o666);
    lock.try_lock().unwrap();
    assert!(matches!(
        ports::open_lock(&path).unwrap().try_lock(),
        Err(std::fs::TryLockError::WouldBlock)
    ));
    drop(lock);
    let fresh = ports::open_lock(&directory.join("fresh.lock")).unwrap();
    assert_eq!(fresh.metadata().unwrap().mode() & 0o7777, 0o666);
    drop(fresh);
    let fresh_directory = directory.join("fresh");
    ports::prepare_directory(&fresh_directory).unwrap();
    assert_eq!(fresh_directory.metadata().unwrap().mode() & 0o7777, 0o1777);
    std::fs::remove_dir_all(directory).unwrap();
}

#[cfg(target_os = "linux")]
#[test]
fn unwritable_shared_namespace_uses_diagnosed_fallback() {
    let _spawn = serial::serial(&SPAWN);
    let output = Command::new(std::env::current_exe().unwrap())
        .args(["--exact", "fallback_child", "--nocapture"])
        .env("VOTEBOAT_TEST_PORT_LEASE_DIR", "/sys")
        .env("VOTEBOAT_TEST_FALLBACK_CHILD", "1")
        .output()
        .unwrap();
    assert!(output.status.success(), "{output:?}");
    let diagnostic = String::from_utf8(output.stderr).unwrap();
    assert!(
        diagnostic.contains("shared namespace /sys is unusable"),
        "{diagnostic}"
    );
    assert!(
        diagnostic.contains("CROSS-USER COORDINATION IS LOST"),
        "{diagnostic}"
    );
}

#[test]
fn fallback_child() {
    if std::env::var_os("VOTEBOAT_TEST_FALLBACK_CHILD").is_none() {
        return;
    }
    let lease = ports::PortLease::acquire(1);
    let [address] = lease.addresses();
    assert!(TcpListener::bind(address).is_err());
    assert!(UdpSocket::bind(address).is_err());
}

#[test]
fn candidates_exclude_ephemeral_and_all_legacy_service_ladders() {
    for ephemeral in [32768..=60999, 15000..=65000, 1024..=20000, 49152..=65535] {
        let candidates = ports::candidates(&ephemeral).collect::<std::collections::BTreeSet<_>>();
        assert!(!candidates.is_empty(), "{ephemeral:?}");
        assert!(candidates
            .iter()
            .all(|port| !ephemeral.contains(port) && *port >= 32768));
        for k in 0..512u32 {
            let base = (20000 + 128 * k) as u16;
            for offset in [1, 2, 3, 101, 102, 103] {
                if let Some(port) = base.checked_add(offset) {
                    assert!(!candidates.contains(&port), "directory {port}");
                }
            }
        }
        for k in 0..64u32 {
            let base = (14000 + 1024 * k) as u16;
            for slot in 0..4 {
                for offset in [1, 2, 3, 101, 102, 103] {
                    if let Some(port) = base.checked_add(slot * 128 + offset) {
                        assert!(!candidates.contains(&port), "transfer {port}");
                    }
                }
            }
        }
    }
    assert_eq!(ports::candidates(&(1024..=65535)).count(), 0);
}
