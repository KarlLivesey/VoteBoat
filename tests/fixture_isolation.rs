// SPDX-License-Identifier: RPL-1.5
// Copyright (c) 2026 Karl Livesey
#![cfg(feature = "native")]
#[path = "support/ports.rs"]
mod ports;
#[path = "support/serial.rs"]
mod serial;

use std::{
    fs::OpenOptions,
    net::{SocketAddr, TcpListener, UdpSocket},
    os::unix::fs::{MetadataExt, PermissionsExt},
    process::Command,
    sync::Mutex,
};

// Even CLOEXEC descriptors are present between another thread's fork and exec.
// Do not fork while asserting that this process's unwind closed the last copy.
static SPAWN: Mutex<()> = Mutex::new(());

// Filesystem-only controls must own fresh names even after a crashed run or
// when PID namespaces share their temporary directory. No socket allocation.
fn control_directory(parent: &std::path::Path, label: &str) -> std::path::PathBuf {
    use std::sync::atomic::{AtomicUsize, Ordering};
    static NEXT: AtomicUsize = AtomicUsize::new(0);
    loop {
        let path = parent.join(format!(
            "voteboat-lease-{label}-{}-{}",
            std::process::id(),
            NEXT.fetch_add(1, Ordering::Relaxed)
        ));
        match std::fs::create_dir(&path) {
            Ok(()) => return path,
            Err(error) if error.kind() == std::io::ErrorKind::AlreadyExists => continue,
            Err(error) => panic!("control directory {}: {error}", path.display()),
        }
    }
}

#[test]
fn lease_child() {
    let Ok(port) = std::env::var("VOTEBOAT_TEST_LEASE_PARENT") else {
        return;
    };
    let port = port.parse::<u16>().unwrap();
    let directory = std::path::PathBuf::from(
        std::env::var_os("VOTEBOAT_TEST_LEASE_NAMESPACE").expect("parent lease namespace"),
    );
    let path = directory.join(format!("{port}.lock"));
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
    assert_eq!(
        lease.directory, directory,
        "TMPDIR must not split the lease namespace"
    );
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
        .env("VOTEBOAT_TEST_LEASE_NAMESPACE", &lease.directory)
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
fn adoption_keeps_the_unused_half_and_reclaim_proves_release() {
    let _spawn = serial::serial(&SPAWN);
    fn check<T>(adopt: fn(&mut ports::PortLease) -> T, unused_held: fn(SocketAddr) -> bool) {
        let mut lease = ports::PortLease::acquire(1);
        let [address] = lease.addresses();
        let adopted = adopt(&mut lease);
        // The unused half stays bound in the lease while its owner runs, so
        // no other binder can take it before the release check.
        assert!(unused_held(address), "{address}: unused half released");
        // Negative control: an owner that still holds its half fails the check.
        let open = std::panic::catch_unwind(std::panic::AssertUnwindSafe(|| lease.reclaim()));
        assert!(
            open.is_err(),
            "reclaim must fail while the adopted half is open"
        );
        drop(adopted);
        lease.reclaim();
        // Both halves are held again until the lease ends.
        assert!(TcpListener::bind(address).is_err());
        assert!(UdpSocket::bind(address).is_err());
    }
    check(|l| l.adopt_tcp(0), |a| UdpSocket::bind(a).is_err());
    check(|l| l.adopt_udp(0), |a| TcpListener::bind(a).is_err());
}

#[test]
fn offset_layouts_keep_every_listener_pair_leased_through_handoff() {
    let _spawn = serial::serial(&SPAWN);
    let offsets = [1, 2, 101, 102];
    let (base, mut lease) = ports::PortLease::acquire_offsets(&offsets);
    let addresses = lease.address_list();
    assert_eq!(
        addresses.iter().map(|a| a.port()).collect::<Vec<_>>(),
        ports::offset_ports(base, &offsets).unwrap()
    );
    for address in &addresses {
        assert!(TcpListener::bind(address).is_err());
        assert!(UdpSocket::bind(address).is_err());
    }
    lease.handoff();
    let (_, other) = ports::PortLease::acquire_offsets(&offsets);
    assert!(other.address_list().iter().all(|a| !addresses.contains(a)));
    for address in addresses {
        drop((
            TcpListener::bind(address).unwrap(),
            UdpSocket::bind(address).unwrap(),
        ));
    }
}

#[test]
fn panic_releases_port_lease_and_serial_guard_without_cascades() {
    let _spawn = serial::serial(&SPAWN);
    static LOCK: Mutex<()> = Mutex::new(());
    // Exercise the actual lease/Reservation Drop path with a private lock-only
    // reservation. Private controls must never bind ports from the shared pool.
    let directory = control_directory(&std::env::temp_dir(), "unwind");
    ports::prepare_directory(&directory).unwrap();
    let path = directory.join("unwind.lock");
    let result = std::panic::catch_unwind(std::panic::AssertUnwindSafe(|| {
        let _serial = serial::serial(&LOCK);
        let lock = ports::open_lock(&path).unwrap();
        lock.try_lock().unwrap();
        let _lease = ports::PortLease {
            directory: directory.clone(),
            held: vec![ports::Reservation {
                address: "127.0.0.1:0".parse().unwrap(),
                tcp: None,
                udp: None,
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
    let directory = control_directory(&std::env::temp_dir(), "modes");
    std::fs::set_permissions(&directory, std::fs::Permissions::from_mode(0o500)).unwrap();
    ports::prepare_directory(&directory).unwrap();
    assert_eq!(directory.metadata().unwrap().mode() & 0o7777, 0o1777);
    let path = directory.join("control.lock");
    let file = OpenOptions::new()
        .write(true)
        .create_new(true)
        .open(&path)
        .unwrap();
    file.set_permissions(std::fs::Permissions::from_mode(0o600))
        .unwrap();
    drop(file);
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
    let target = directory.join("alias-target");
    let file = OpenOptions::new()
        .write(true)
        .create_new(true)
        .open(&target)
        .unwrap();
    file.set_permissions(std::fs::Permissions::from_mode(0o600))
        .unwrap();
    drop(file);
    let symlink = directory.join("symlink.lock");
    std::os::unix::fs::symlink(&target, &symlink).unwrap();
    assert!(
        ports::open_lock(&symlink).is_err(),
        "lock entries must not follow symlinks"
    );
    let hardlink = directory.join("hardlink.lock");
    std::fs::hard_link(&target, &hardlink).unwrap();
    assert!(
        ports::open_lock(&hardlink).is_err(),
        "lock entries must not alias another file"
    );
    assert_eq!(target.metadata().unwrap().mode() & 0o7777, 0o600);
    let fresh_directory = directory.join("fresh");
    ports::prepare_directory(&fresh_directory).unwrap();
    assert_eq!(fresh_directory.metadata().unwrap().mode() & 0o7777, 0o1777);
    std::fs::remove_dir_all(directory).unwrap();
}

#[cfg(target_os = "linux")]
#[test]
fn unwritable_shared_namespace_uses_diagnosed_fallback() {
    let _spawn = serial::serial(&SPAWN);
    // Retain socket exclusivity checks under the common coordination namespace.
    // The permission-control child must never bind through a private namespace.
    let lease = ports::PortLease::acquire(1);
    let [address] = lease.addresses();
    assert!(TcpListener::bind(address).is_err());
    assert!(UdpSocket::bind(address).is_err());
    // Linux sysctl directories reject mode changes even under sudo.
    let output = Command::new(std::env::current_exe().unwrap())
        .args(["--exact", "fallback_child", "--nocapture"])
        .env("VOTEBOAT_TEST_PORT_LEASE_DIR", "/proc/sys")
        .env("VOTEBOAT_TEST_FALLBACK_CHILD", "1")
        .output()
        .unwrap();
    assert!(output.status.success(), "{output:?}");
    let diagnostic = String::from_utf8(output.stderr).unwrap();
    assert!(
        diagnostic.contains("shared namespace /proc/sys is unusable"),
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
    // Simulate a stale name left by a crashed process before PID reuse.
    let stale =
        std::path::Path::new("/tmp").join(format!("voteboat-lease-owner-{}-0", std::process::id()));
    let created = match OpenOptions::new().write(true).create_new(true).open(&stale) {
        Ok(_) => true,
        Err(error) if error.kind() == std::io::ErrorKind::AlreadyExists => false,
        Err(error) => panic!("stale-name control: {error}"),
    };
    let shared = ports::shared_directory();
    let error = ports::prepare_directory(&shared)
        .expect_err("the control's shared directory is unwritable");
    let fallback = ports::fallback_directory(&shared, &error);
    assert_ne!(fallback, shared);
    ports::prepare_directory(&fallback).unwrap();
    assert_eq!(fallback.metadata().unwrap().mode() & 0o7777, 0o1777);
    let directory = control_directory(&fallback, "permission-control");
    let path = directory.join("control.lock");
    let lock = ports::open_lock(&path).unwrap();
    assert_eq!(lock.metadata().unwrap().mode() & 0o7777, 0o666);
    lock.try_lock().unwrap();
    assert!(matches!(
        ports::open_lock(&path).unwrap().try_lock(),
        Err(std::fs::TryLockError::WouldBlock)
    ));
    drop(lock);
    std::fs::remove_dir_all(directory).unwrap();
    assert!(
        stale.exists(),
        "allocation must skip, not unlink, stale probes"
    );
    if created {
        std::fs::remove_file(stale).unwrap();
    }
}

#[test]
fn candidates_exclude_ephemeral_and_all_legacy_service_ladders() {
    for ephemeral in [
        32768..=60999,
        15000..=65000,
        1024..=20000,
        49152..=65535,
        15000..=65535,
        32768..=65535,
    ] {
        let candidates = ports::candidates(&ephemeral).collect::<std::collections::BTreeSet<_>>();
        assert!(!candidates.is_empty(), "{ephemeral:?}");
        assert!(candidates
            .iter()
            .all(|port| !ephemeral.contains(port) && *port != 0 && !(10000..20000).contains(port)));
        assert!((1024u16..32768).step_by(128).all(|base| {
            [
                1, 2, 3, 4, 11, 12, 13, 14, 101, 102, 103, 104, 111, 112, 113, 114,
            ]
            .into_iter()
            .all(|offset| !candidates.contains(&(base + offset)))
        }));
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
    let low = ports::candidates(&(1024..=65535)).collect::<Vec<_>>();
    assert!(!low.is_empty());
    assert!(low.iter().all(|port| (1..1024).contains(port)));
    assert_eq!(ports::candidates(&(1..=65535)).count(), 0);
}
