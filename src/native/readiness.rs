// SPDX-License-Identifier: RPL-1.5
// Copyright (c) 2026 Karl Livesey
//! Readiness waiting for native host loops.
//!
//! A host loop polls every socket and worker channel non-blockingly and then
//! waits before its next round. Waiting with [`Readiness::wait`] instead of a
//! fixed park lets the round start as soon as a socket or worker has
//! something, while never waiting longer than the given timeout. Waiting
//! decides nothing: after it returns, the host polls exactly as before, so a
//! missed, stale or spurious readiness report can only cost latency, never
//! correctness or durability.
//!
//! This module holds the crate's only `unsafe` code: one `poll(2)` call over
//! a buffer that this module exclusively owns for the duration of the call.
#![allow(unsafe_code)]

use crate::worker::{ReadinessFd, WorkerWake};
use std::{
    io::{self, Read, Write},
    os::{fd::AsRawFd, unix::net::UnixStream},
    sync::{
        atomic::{AtomicBool, Ordering},
        Mutex,
    },
    time::Duration,
};

mod host_wait;
pub use host_wait::{HostWait, HOST_GATHER, HOST_MAX_WAIT};

/// Reusable descriptor buffer, so steady-state waits do not allocate.
#[derive(Default)]
pub struct Readiness {
    polled: Vec<libc::pollfd>,
}
impl Readiness {
    /// Wait until any descriptor in `fds` is readable (or writable, when it
    /// asks for that), hung up or errored, or until `timeout` (rounded up to
    /// whole milliseconds) passes. Returns whether any descriptor reported an
    /// event. An interrupted wait returns `Ok(false)`. Closed or reused
    /// descriptor numbers are harmless: the kernel reports them as invalid
    /// rather than touching memory.
    pub fn wait(&mut self, fds: &[ReadinessFd], timeout: Duration) -> io::Result<bool> {
        self.polled.clear();
        self.polled.extend(fds.iter().map(|f| libc::pollfd {
            fd: f.fd,
            events: if f.write {
                libc::POLLIN | libc::POLLOUT
            } else {
                libc::POLLIN
            },
            revents: 0,
        }));
        let count = libc::nfds_t::try_from(self.polled.len())
            .map_err(|_| io::Error::other("too many readiness descriptors"))?;
        let millis = if fds.iter().any(|f| f.immediate) {
            0
        } else {
            timeout.as_micros().div_ceil(1000).min(i32::MAX as u128) as libc::c_int
        };
        // SAFETY: `self.polled` is a live Vec, exclusively borrowed here, holding
        // exactly `count` initialized `pollfd` values. poll(2) reads `fd` and
        // `events` and writes only `revents` within those `count` elements, and
        // keeps no pointer after it returns. It does not dereference descriptor
        // values, so an invalid or reused descriptor cannot cause undefined
        // behaviour; it is reported through `revents` instead.
        let result = unsafe { libc::poll(self.polled.as_mut_ptr(), count, millis) };
        if result < 0 {
            let error = io::Error::last_os_error();
            return if error.kind() == io::ErrorKind::Interrupted {
                Ok(false)
            } else {
                Err(error)
            };
        }
        Ok(result > 0)
    }
}

/// Worker wakeup that a readiness wait observes: one byte on a local socket
/// pair per burst of wakes. It deliberately does not unpark the host: a host
/// that gathers briefly before a round must not be cut short by every wake.
///
/// A wake is never lost. [`HostWake::drain`] empties the socket and only then
/// clears the pending flag: a wake arriving after the clear sends a fresh byte
/// that the next wait sees immediately, and a wake arriving before it was
/// preceded by its event, which the host's next poll (after the drain) reads.
/// Clearing first would let a wake landing during the drain lose its byte
/// while leaving the flag set, silencing every later wake until a timeout.
/// At worst a wait returns early once.
pub struct HostWake {
    sender: Mutex<UnixStream>,
    receiver: Mutex<UnixStream>,
    pending: AtomicBool,
}
impl HostWake {
    /// A wake for a host that waits with [`Readiness::wait`].
    pub fn new() -> io::Result<Self> {
        let (sender, receiver) = UnixStream::pair()?;
        sender.set_nonblocking(true)?;
        receiver.set_nonblocking(true)?;
        Ok(Self {
            sender: Mutex::new(sender),
            receiver: Mutex::new(receiver),
            pending: AtomicBool::new(false),
        })
    }
    /// Descriptor to include in the host's readiness wait.
    pub fn fd(&self) -> ReadinessFd {
        ReadinessFd::read(
            self.receiver
                .lock()
                .unwrap_or_else(|e| e.into_inner())
                .as_raw_fd(),
        )
    }
    /// Consume delivered wakes after a wait, before polling the host.
    pub fn drain(&self) {
        let mut receiver = self.receiver.lock().unwrap_or_else(|e| e.into_inner());
        let mut bytes = [0; 64];
        while matches!(receiver.read(&mut bytes), Ok(n) if n > 0) {}
        self.pending.store(false, Ordering::SeqCst);
    }
}
impl WorkerWake for HostWake {
    fn wake(&self) {
        if !self.pending.swap(true, Ordering::SeqCst) {
            // A full or failed send only delays the host until its timeout.
            let _ = self
                .sender
                .lock()
                .unwrap_or_else(|e| e.into_inner())
                .write(&[1]);
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::{sync::Arc, thread, time::Instant};

    #[test]
    fn wait_times_out_without_events_and_reports_a_readable_wake() {
        let wake = HostWake::new().unwrap();
        let mut readiness = Readiness::default();
        let started = Instant::now();
        assert!(!readiness
            .wait(&[wake.fd()], Duration::from_millis(5))
            .unwrap());
        assert!(started.elapsed() >= Duration::from_millis(4));
        wake.wake();
        let started = Instant::now();
        assert!(readiness
            .wait(&[wake.fd()], Duration::from_secs(5))
            .unwrap());
        assert!(started.elapsed() < Duration::from_secs(1));
        wake.drain();
        assert!(!readiness.wait(&[wake.fd()], Duration::ZERO).unwrap());
    }

    #[test]
    fn wakes_from_other_threads_are_never_lost_across_drains() {
        let wake = Arc::new(HostWake::new().unwrap());
        let mut readiness = Readiness::default();
        for _ in 0..500 {
            let remote = wake.clone();
            thread::spawn(move || remote.wake()).join().unwrap();
            assert!(readiness
                .wait(&[wake.fd()], Duration::from_secs(5))
                .unwrap());
            wake.drain();
        }
        // A burst of wakes coalesces into at most a few bytes and one wait.
        for _ in 0..10_000 {
            wake.wake();
        }
        assert!(readiness
            .wait(&[wake.fd()], Duration::from_secs(5))
            .unwrap());
        wake.drain();
        assert!(!readiness.wait(&[wake.fd()], Duration::ZERO).unwrap());
    }

    #[test]
    fn concurrent_wakes_during_drains_are_never_lost() {
        use std::sync::atomic::AtomicUsize;
        let wake = Arc::new(HostWake::new().unwrap());
        let sent = Arc::new(AtomicUsize::new(0));
        const WAKES: usize = 20_000;
        let worker = {
            let (wake, sent) = (wake.clone(), sent.clone());
            thread::spawn(move || {
                for _ in 0..WAKES {
                    // Event first, then wake, as every worker does.
                    sent.fetch_add(1, Ordering::SeqCst);
                    wake.wake();
                }
            })
        };
        let mut readiness = Readiness::default();
        let mut seen = 0;
        while seen < WAKES {
            // A lost wake would leave the host blocked here until the timeout.
            assert!(
                readiness
                    .wait(&[wake.fd()], Duration::from_secs(5))
                    .unwrap(),
                "wake lost after {seen} of {WAKES}"
            );
            wake.drain();
            seen = sent.load(Ordering::SeqCst);
        }
        worker.join().unwrap();
    }

    #[test]
    fn invalid_descriptors_are_reported_not_undefined() {
        let mut readiness = Readiness::default();
        // A descriptor number that is not open in this process.
        assert!(readiness
            .wait(&[ReadinessFd::read(i32::MAX - 7)], Duration::ZERO)
            .unwrap());
        assert!(readiness.wait(&[], Duration::ZERO).is_ok());
    }
}
