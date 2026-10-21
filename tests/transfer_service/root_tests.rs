// SPDX-License-Identifier: RPL-1.5
// Copyright (c) 2026 Karl Livesey
use super::*;

// A normally dropped fixture intentionally leaves its root for the external
// cleanup. Own that retained scratch even if a control assertion unwinds.
struct RetainedRoot(PathBuf);
impl Drop for RetainedRoot {
    fn drop(&mut self) {
        if self.0.exists() {
            fs::remove_dir_all(&self.0).unwrap();
        }
    }
}

// Use the real Cluster/PortLease Drop paths, with an unstarted cluster and a
// private lock-only reservation (as fixture_isolation does for unwind). No
// sockets or shared port-namespace entries are changed by this control.
fn unstarted_fixture(base: u16, namespace: &std::path::Path) -> super::super::Cluster {
    use super::super::{ports, Cluster};
    let lock = ports::open_lock(&namespace.join(format!("{base}.lock"))).unwrap();
    lock.try_lock()
        .expect("the previous fixture released its lease");
    Cluster {
        root: Directory::new(),
        base,
        quic: false,
        admin: 3,
        peer_credentials: false,
        retirement: false,
        children: Vec::new(),
        _ports: ports::PortLease {
            directory: namespace.to_path_buf(),
            held: vec![ports::Reservation {
                address: "127.0.0.1:0".parse().unwrap(),
                tcp: None,
                udp: None,
                _lock: lock,
            }],
        },
    }
}

#[test]
fn fresh_identity_survives_delayed_cleanup_at_recycled_base() {
    let _gate = super::super::fixture_gate();
    let namespace = Directory::new();
    let namespace_path = RetainedRoot(namespace.0.clone());
    drop(namespace);
    let base = 65048;
    let first = unstarted_fixture(base, &namespace_path.0);
    let old = RetainedRoot(first.root.0.clone());
    fs::write(first.root.join("old-store"), b"A").unwrap();
    drop(first); // Real Cluster shutdown/lease release, before external removal.
    assert!(old.0.join("old-store").exists());

    let next = unstarted_fixture(base, &namespace_path.0);
    assert_eq!(next.base, base);
    assert_ne!(
        next.root.0, old.0,
        "recycled base must get a fresh root identity"
    );
    assert!(
        !next.root.join("old-store").exists(),
        "must not reopen A's stores"
    );
    fs::write(next.root.join("owner-marker"), b"B").unwrap();
    fs::create_dir(next.root.join("store")).unwrap();
    fs::write(next.root.join("store/manifest"), b"B store").unwrap();
    fs::remove_dir_all(&old.0).unwrap(); // A cleans up after B prepared stores.
    assert_eq!(fs::read(next.root.join("owner-marker")).unwrap(), b"B");
    assert_eq!(
        fs::read(next.root.join("store/manifest")).unwrap(),
        b"B store"
    );
    let path = next.root.0.clone();
    drop(next);
    fs::remove_dir_all(path).unwrap();
}

#[test]
fn stale_collisions_and_partial_setup_unwind_keep_other_roots_untouched() {
    let _gate = super::super::fixture_gate();
    let control = Directory::new();
    let parent = RetainedRoot(control.0.clone());
    drop(control);
    let sequence = NEXT.load(Ordering::Relaxed);
    let stale = candidate(&parent.0, sequence);
    fs::create_dir(&stale).unwrap();
    let stale = RetainedRoot(stale);
    fs::write(stale.0.join("old-store"), b"crashed fixture").unwrap();
    let collision = candidate(&parent.0, sequence.checked_add(1).unwrap());
    // A file at a candidate name is also somebody else's entry, never a root.
    use std::io::Write;
    let mut file = fs::OpenOptions::new()
        .write(true)
        .create_new(true)
        .open(&collision)
        .unwrap();
    file.write_all(b"occupied name").unwrap();
    drop(file);
    let result = std::panic::catch_unwind(|| {
        let partial = Directory::create(&parent.0);
        assert_ne!(partial.0, stale.0);
        assert_ne!(partial.0, collision);
        assert!(!partial.join("old-store").exists());
        fs::create_dir(partial.join("partially-prepared-store")).unwrap();
        let path = partial.0.clone();
        let unwind = std::panic::catch_unwind(|| {
            let _owner = partial;
            panic!("intentional transfer fixture setup failure");
        });
        assert!(unwind.is_err());
        assert!(
            !path.exists(),
            "unwind must remove its owned failed scratch"
        );
        let next = Directory::create(&parent.0);
        assert!(!next.join("old-store").exists());
        assert!(!next.join("partially-prepared-store").exists());
        let path = next.0.clone();
        drop(next);
        fs::remove_dir_all(path).unwrap();
    });
    assert_eq!(
        fs::read(stale.0.join("old-store")).unwrap(),
        b"crashed fixture"
    );
    assert_eq!(fs::read(&collision).unwrap(), b"occupied name");
    fs::remove_file(collision).unwrap();
    drop(stale);
    result.unwrap();
}
