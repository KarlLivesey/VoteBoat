// SPDX-License-Identifier: RPL-1.5
// Copyright (c) 2026 Karl Livesey
//! Test-local visits in actual production loops; absent from release builds.
use std::cell::Cell;

#[derive(Clone, Copy)]
pub(crate) enum Visit {
    Assigned,
    Inspected,
    Prefix,
    Query,
    IndexNeed,
}
thread_local! {
    static VISITS: Cell<[usize; 5]> = const { Cell::new([0; 5]) };
}
pub(crate) fn visit(kind: Visit) {
    VISITS.with(|visits| {
        let mut counts = visits.get();
        counts[kind as usize] += 1;
        visits.set(counts);
    });
}
pub(crate) fn take() -> [usize; 5] {
    VISITS.with(|visits| visits.replace([0; 5]))
}
