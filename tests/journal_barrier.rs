// SPDX-License-Identifier: RPL-1.5
// Copyright (c) 2026 Karl Livesey
#![cfg(feature = "native")]
mod support;
use std::{cell::Cell, io, rc::Rc};
use support::*;
use voteboat::{log::*, native::log_store::*};

#[derive(Default)]
struct Ordered {
    calls: Vec<&'static str>,
    fail: Option<&'static str>,
}
impl JournalIo for Ordered {
    fn read_manifest(&mut self) -> io::Result<Vec<u8>> {
        unreachable!()
    }
    fn read_log(&mut self, _: usize) -> io::Result<Vec<u8>> {
        unreachable!()
    }
    fn append(&mut self, _: &[u8]) -> io::Result<()> {
        unreachable!()
    }
    fn truncate_log(&mut self, _: u64) -> io::Result<()> {
        unreachable!()
    }
    fn sync_log(&mut self) -> io::Result<()> {
        self.calls.push("sync");
        if self.fail == Some("sync") {
            Err(io::Error::other("sync"))
        } else {
            Ok(())
        }
    }
    fn publish_manifest(&mut self, bytes: &[u8]) -> io::Result<()> {
        assert_eq!(bytes, b"original manifest");
        self.calls.push("publish");
        if self.fail == Some("publish") {
            Err(io::Error::other("publish"))
        } else {
            Ok(())
        }
    }
}
#[test]
fn default_combined_barrier_keeps_order_exact_bytes_and_error_short_circuit() {
    for fail in [None, Some("sync"), Some("publish")] {
        let mut io = Ordered {
            fail,
            ..Default::default()
        };
        let result = io.publish_barrier(b"original manifest");
        assert_eq!(result.is_err(), fail.is_some());
        assert_eq!(
            io.calls,
            if fail == Some("sync") {
                vec!["sync"]
            } else {
                vec!["sync", "publish"]
            }
        );
    }
}
struct Combined {
    inner: ModelIo,
    calls: Rc<Cell<usize>>,
    fail: Rc<Cell<bool>>,
}
impl JournalIo for Combined {
    fn read_manifest(&mut self) -> io::Result<Vec<u8>> {
        self.inner.read_manifest()
    }
    fn read_log(&mut self, limit: usize) -> io::Result<Vec<u8>> {
        self.inner.read_log(limit)
    }
    fn append(&mut self, bytes: &[u8]) -> io::Result<()> {
        self.inner.append(bytes)
    }
    fn truncate_log(&mut self, length: u64) -> io::Result<()> {
        self.inner.truncate_log(length)
    }
    fn sync_log(&mut self) -> io::Result<()> {
        panic!("native store bypassed combined provider")
    }
    fn publish_manifest(&mut self, _: &[u8]) -> io::Result<()> {
        panic!("native store bypassed combined provider")
    }
    fn publish_barrier(&mut self, manifest: &[u8]) -> io::Result<()> {
        self.calls.set(self.calls.get() + 1);
        if self.fail.get() {
            return Err(io::Error::other("combined publication"));
        }
        self.inner.publish_barrier(manifest)
    }
}
fn combined(io: ModelIo, calls: Rc<Cell<usize>>, fail: Rc<Cell<bool>>) -> Combined {
    Combined {
        inner: io,
        calls,
        fail,
    }
}
#[test]
fn native_creation_barrier_and_recovery_use_the_selected_combined_provider() {
    let io = ModelIo::default();
    let calls = Rc::new(Cell::new(0));
    let fail = Rc::new(Cell::new(false));
    let mut store = NativeLogStore::create(
        combined(io.clone(), calls.clone(), fail.clone()),
        identity(1),
        LogLimits::default(),
    )
    .unwrap();
    assert_eq!(calls.get(), 1);
    let tickets = store
        .append_batch(vec![LogMutation::Create(bootstrap(1, 3))])
        .unwrap();
    assert!(store.state(group(1)).is_err());
    assert_eq!(store.barrier(&tickets).unwrap().tickets, tickets);
    assert_eq!(store.state(group(1)).unwrap().bootstrap, bootstrap(1, 3));
    assert_eq!(calls.get(), 2);
    let binding = store.binding();
    drop(store);
    io.0.borrow_mut().power_loss();
    let mut reopened = NativeLogStore::recover(
        combined(io, calls.clone(), fail),
        identity(1),
        LogLimits::default(),
    )
    .unwrap();
    assert_eq!(calls.get(), 3);
    assert_ne!(binding, reopened.binding());
    assert_eq!(reopened.state(group(1)).unwrap().bootstrap, bootstrap(1, 3));
    assert!(reopened.barrier(&tickets).is_err());
    assert_eq!(calls.get(), 3);
}
#[test]
fn combined_publication_error_fences_without_a_durable_result() {
    let io = ModelIo::default();
    let calls = Rc::new(Cell::new(0));
    let fail = Rc::new(Cell::new(false));
    let mut store = NativeLogStore::create(
        combined(io, calls.clone(), fail.clone()),
        identity(1),
        LogLimits::default(),
    )
    .unwrap();
    let tickets = store
        .append_batch(vec![LogMutation::Create(bootstrap(1, 3))])
        .unwrap();
    fail.set(true);
    assert!(store.barrier(&tickets).is_err());
    assert_eq!(
        store.state(group(1)),
        Err(voteboat::contracts::StorageError::Fenced)
    );
    assert_eq!(
        store.barrier(&tickets),
        Err(voteboat::contracts::StorageError::Fenced)
    );
    assert_eq!(calls.get(), 2);
}

#[test]
fn optional_replacement_default_remains_explicitly_unsupported() {
    let mut io = Ordered::default();
    assert!(!io.supports_replacement());
    assert_eq!(
        io.replace_log(b"frames", b"manifest").unwrap_err().kind(),
        io::ErrorKind::Unsupported
    );
    assert!(io.calls.is_empty());
}
