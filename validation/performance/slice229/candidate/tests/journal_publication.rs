// SPDX-License-Identifier: RPL-1.5
// Copyright (c) 2026 Karl Livesey
#![cfg(feature = "native")]
mod support;
use std::{cell::RefCell, io, rc::Rc};
use support::*;
use voteboat::{contracts::StorageError, log::*, native::log_store::*};

struct Recorded {
    inner: ModelIo,
    calls: Rc<RefCell<Vec<&'static str>>>,
}
impl JournalIo for Recorded {
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
        self.calls.borrow_mut().push("sync");
        self.inner.sync_log()
    }
    fn publish_manifest(&mut self, bytes: &[u8]) -> io::Result<()> {
        self.calls.borrow_mut().push("publish");
        self.inner.publish_manifest(bytes)
    }
}

#[test]
fn downstream_default_orders_sync_before_selection_and_preserves_errors() {
    for fault in [
        Fault::None,
        Fault::Sync,
        Fault::PublishBefore,
        Fault::PublishAfter,
    ] {
        let inner = ModelIo::default();
        {
            let mut device = inner.0.borrow_mut();
            device.log = b"new log".to_vec();
            device.synced = b"old log".to_vec();
            device.manifest = Some(b"old selection".to_vec());
            device.fault = fault;
        }
        let calls = Rc::<RefCell<Vec<_>>>::default();
        let mut journal = Recorded {
            inner: inner.clone(),
            calls: calls.clone(),
        };
        let result = journal.sync_and_publish_manifest(b"new selection");
        if matches!(fault, Fault::Sync) {
            assert_eq!(*calls.borrow(), ["sync"]);
            assert_eq!(
                inner.0.borrow().manifest.as_deref(),
                Some(b"old selection".as_slice())
            );
            assert_eq!(inner.0.borrow().synced, b"old log");
        } else {
            assert_eq!(*calls.borrow(), ["sync", "publish"]);
            assert_eq!(inner.0.borrow().synced, b"new log");
            let selected: &[u8] = if matches!(fault, Fault::PublishBefore) {
                b"old selection"
            } else {
                b"new selection"
            };
            assert_eq!(inner.0.borrow().manifest.as_deref(), Some(selected));
        }
        assert_eq!(result.is_ok(), matches!(fault, Fault::None));
    }
}

// A downstream implementation can own the entire dependency chain. The store
// must call that public operation, never bypass it through concrete primitives.
struct Combined(Recorded);
impl JournalIo for Combined {
    fn read_manifest(&mut self) -> io::Result<Vec<u8>> {
        self.0.read_manifest()
    }
    fn read_log(&mut self, limit: usize) -> io::Result<Vec<u8>> {
        self.0.read_log(limit)
    }
    fn append(&mut self, bytes: &[u8]) -> io::Result<()> {
        self.0.append(bytes)
    }
    fn truncate_log(&mut self, length: u64) -> io::Result<()> {
        self.0.truncate_log(length)
    }
    fn sync_log(&mut self) -> io::Result<()> {
        panic!("combined operation was bypassed")
    }
    fn publish_manifest(&mut self, _: &[u8]) -> io::Result<()> {
        panic!("combined operation was bypassed")
    }
    fn sync_and_publish_manifest(&mut self, bytes: &[u8]) -> io::Result<()> {
        self.0.calls.borrow_mut().push("combined");
        self.0.sync_and_publish_manifest(bytes)
    }
}
fn combined(inner: ModelIo, calls: Rc<RefCell<Vec<&'static str>>>) -> Combined {
    Combined(Recorded { inner, calls })
}

#[test]
fn create_barrier_and_recovery_use_downstream_combined_operation() {
    let inner = ModelIo::default();
    let calls = Rc::<RefCell<Vec<_>>>::default();
    let mut store = NativeLogStore::create(
        combined(inner.clone(), calls.clone()),
        identity(1),
        LogLimits::default(),
    )
    .unwrap();
    assert_eq!(*calls.borrow(), ["combined", "sync", "publish"]);
    calls.borrow_mut().clear();
    let tickets = store
        .append_batch(vec![LogMutation::Create(bootstrap(1, 3))])
        .unwrap();
    assert!(store.state(group(1)).is_err());
    assert_eq!(store.barrier(&tickets).unwrap().tickets, tickets);
    assert_eq!(*calls.borrow(), ["combined", "sync", "publish"]);
    let state = store.state(group(1)).unwrap();
    let binding = store.binding();
    drop(store);
    inner.0.borrow_mut().power_loss();
    calls.borrow_mut().clear();
    let mut recovered = NativeLogStore::recover(
        combined(inner, calls.clone()),
        identity(1),
        LogLimits::default(),
    )
    .unwrap();
    assert_eq!(*calls.borrow(), ["combined", "sync", "publish"]);
    assert_eq!(recovered.state(group(1)).unwrap(), state);
    assert!(recovered.binding().session > binding.session);
    calls.borrow_mut().clear();
    assert_eq!(recovered.barrier(&tickets), Err(StorageError::StaleTicket));
    assert!(calls.borrow().is_empty());
}

#[test]
fn combined_failure_fences_without_releasing_a_ticket_then_recovers_actual_bytes() {
    for fault in [Fault::Sync, Fault::PublishBefore, Fault::PublishAfter] {
        let inner = ModelIo::default();
        let calls = Rc::<RefCell<Vec<_>>>::default();
        let mut store = NativeLogStore::create(
            combined(inner.clone(), calls.clone()),
            identity(1),
            LogLimits::default(),
        )
        .unwrap();
        let tickets = store
            .append_batch(vec![LogMutation::Create(bootstrap(1, 3))])
            .unwrap();
        inner.0.borrow_mut().fault = fault;
        assert!(store.barrier(&tickets).is_err());
        assert_eq!(store.barrier(&tickets), Err(StorageError::Fenced));
        assert_eq!(store.state(group(1)), Err(StorageError::Fenced));
        drop(store);
        inner.0.borrow_mut().power_loss();
        let recovered =
            NativeLogStore::recover(combined(inner, calls), identity(1), LogLimits::default())
                .unwrap();
        if matches!(fault, Fault::Sync) {
            assert!(recovered.state(group(1)).is_err());
        } else {
            assert_eq!(
                recovered.state(group(1)).unwrap().bootstrap,
                bootstrap(1, 3)
            );
        }
    }
}
