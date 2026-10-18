// SPDX-License-Identifier: RPL-1.5
// Copyright (c) 2026 Karl Livesey
use std::sync::{Mutex, MutexGuard, PoisonError};

/// These locks protect only `()`, never mutable fixture data. A root panic must
/// remain visible without manufacturing failures in every subsequent test.
pub fn serial(lock: &'static Mutex<()>) -> MutexGuard<'static, ()> {
    lock.lock().unwrap_or_else(PoisonError::into_inner)
}
