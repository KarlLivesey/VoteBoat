// SPDX-License-Identifier: RPL-1.5
// Copyright (c) 2026 Karl Livesey
//! Empty receiver storage retained only by one exclusively borrowed client.
use super::OperationId;
use tokio::sync::oneshot;

type Wait = oneshot::Receiver<OperationId>;

pub(super) struct WaitCache {
    waits: Vec<Wait>,
    retain_limit: usize,
}
impl WaitCache {
    pub(super) fn new(retain_limit: usize) -> Self {
        Self {
            waits: Vec::new(),
            retain_limit,
        }
    }
    pub(super) fn retain_limit(&self) -> usize {
        self.retain_limit
    }
    pub(super) fn lease(&mut self, width: usize) -> Lease<'_> {
        let lease = Lease(self);
        // The previous lease left no live endpoints. reserve_exact's argument
        // is additional to length (zero), not additional to existing capacity.
        if lease.0.waits.capacity() < width {
            lease.0.waits.reserve_exact(width);
        }
        lease
    }
}

pub(super) struct Lease<'a>(&'a mut WaitCache);
impl Lease<'_> {
    pub(super) fn push(&mut self, wait: Wait) {
        self.0.waits.push(wait);
    }
    pub(super) fn drain(&mut self) -> std::vec::Drain<'_, Wait> {
        self.0.waits.drain(..)
    }
}
impl Drop for Lease<'_> {
    fn drop(&mut self) {
        // Also runs after partial construction, a failed send or cancellation.
        // A dropped Drain closes its remaining endpoints before this lease.
        self.0.waits.clear();
        if self.0.waits.capacity() > self.0.retain_limit {
            self.0.waits = Vec::new();
        }
    }
}

#[cfg(test)]
mod tests;
