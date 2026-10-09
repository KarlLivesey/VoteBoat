// SPDX-License-Identifier: RPL-1.5
// Copyright (c) 2026 Karl Livesey
// Unless explicitly acquired and licensed from Licensor under another license,
// the contents of this file are subject to the Reciprocal Public License
// ("RPL") Version 1.5, or subsequent versions as allowed by the RPL, and You may
// not copy or use this file in either source code or executable form, except
// in compliance with the terms and conditions of the RPL.
//
// All software distributed under the RPL is provided strictly on an "AS IS"
// basis, WITHOUT WARRANTY OF ANY KIND, EITHER EXPRESS OR IMPLIED, AND LICENSOR
// HEREBY DISCLAIMS ALL SUCH WARRANTIES, INCLUDING WITHOUT LIMITATION, ANY
// WARRANTIES OF MERCHANTABILITY, FITNESS FOR A PARTICULAR PURPOSE, QUIET
// ENJOYMENT, OR NON-INFRINGEMENT. See the RPL for specific language governing
// rights and limitations under the RPL.
//! Finite, shareable reservations with lazily allocated owned Vec storage.
use crate::buffer::*;
use std::sync::{
    atomic::{AtomicUsize, Ordering},
    Arc,
};
#[derive(Default)]
struct Budget {
    bytes: AtomicUsize,
    leases: AtomicUsize,
}
#[derive(Default)]
struct Credits {
    total: Budget,
    bulk: Budget,
}
/// Clone shares credits, while close is scoped to that view. No buffers are
/// cached: each lease frees its allocation before returning reserved credits.
#[derive(Clone)]
pub struct NativeBufferPool {
    credits: Arc<Credits>,
    limits: BufferLimits,
    closed: bool,
    control_reserve: Option<BufferLimits>,
}
impl NativeBufferPool {
    pub fn new(limits: BufferLimits) -> Result<Self, BufferError> {
        Ok(Self::build(limits.validate()?, None))
    }
    /// Shared bulk work cannot consume this byte/lease headroom. Control is
    /// still bounded by total limits and may compete with other control work.
    pub fn new_with_control_reserve(
        limits: BufferLimits,
        reserve: BufferLimits,
    ) -> Result<Self, BufferError> {
        Ok(Self::build(
            limits.validate_control_reserve(reserve)?,
            Some(reserve),
        ))
    }
    fn build(limits: BufferLimits, control_reserve: Option<BufferLimits>) -> Self {
        Self {
            credits: Arc::new(Credits::default()),
            limits,
            closed: false,
            control_reserve,
        }
    }
    /// Restricted bulk reservations; zero when no reserve was selected.
    pub fn bulk_usage(&self) -> BufferUsage {
        self.credits.bulk.usage()
    }
    fn bulk_limits(&self) -> BufferLimits {
        let reserve = self.control_reserve.unwrap_or(BufferLimits {
            reserved_bytes: 0,
            leases: 0,
        });
        BufferLimits {
            reserved_bytes: self.limits.reserved_bytes - reserve.reserved_bytes,
            leases: self.limits.leases - reserve.leases,
        }
    }
}
fn reserve(counter: &AtomicUsize, amount: usize, limit: usize) -> Result<(), BufferError> {
    let mut current = counter.load(Ordering::Relaxed);
    // Bounded contention is overload, never an unbounded retry loop.
    for _ in 0..16 {
        let next = current
            .checked_add(amount)
            .filter(|v| *v <= limit)
            .ok_or(BufferError::Overloaded)?;
        match counter.compare_exchange_weak(current, next, Ordering::AcqRel, Ordering::Relaxed) {
            Ok(_) => return Ok(()),
            Err(actual) => current = actual,
        }
    }
    Err(BufferError::Overloaded)
}
impl Budget {
    fn acquire(&self, bytes: usize, limits: BufferLimits) -> Result<(), BufferError> {
        reserve(&self.leases, 1, limits.leases)?;
        if let Err(error) = reserve(&self.bytes, bytes, limits.reserved_bytes) {
            self.leases.fetch_sub(1, Ordering::AcqRel);
            return Err(error);
        }
        Ok(())
    }
    fn release(&self, bytes: usize) {
        self.bytes.fetch_sub(bytes, Ordering::AcqRel);
        self.leases.fetch_sub(1, Ordering::AcqRel);
    }
    fn usage(&self) -> BufferUsage {
        BufferUsage {
            reserved_bytes: self.bytes.load(Ordering::Acquire),
            leases: self.leases.load(Ordering::Acquire),
        }
    }
}
pub struct NativeFrameBuffer {
    bytes: Vec<u8>,
    reservation: usize,
    bulk: bool,
    credits: Arc<Credits>,
}
impl AsRef<[u8]> for NativeFrameBuffer {
    fn as_ref(&self) -> &[u8] {
        &self.bytes
    }
}
impl AsMut<[u8]> for NativeFrameBuffer {
    fn as_mut(&mut self) -> &mut [u8] {
        &mut self.bytes
    }
}
impl FrameBuffer for NativeFrameBuffer {
    fn reservation(&self) -> usize {
        self.reservation
    }
    fn capacity(&self) -> usize {
        self.bytes.capacity()
    }
    fn resize(&mut self, len: usize) -> Result<(), BufferError> {
        if len > self.reservation {
            return Err(BufferError::TooLarge);
        }
        if len > self.bytes.capacity() {
            self.bytes
                .try_reserve_exact(len - self.bytes.len())
                .map_err(|_| BufferError::AllocationFailed)?;
        }
        self.bytes.resize(len, 0);
        Ok(())
    }
}
impl Drop for NativeFrameBuffer {
    fn drop(&mut self) {
        self.bytes = Vec::new();
        self.credits.total.release(self.reservation);
        if self.bulk {
            self.credits.bulk.release(self.reservation);
        }
    }
}
impl BufferPool for NativeBufferPool {
    type Buffer = NativeFrameBuffer;
    fn limits(&self) -> BufferLimits {
        self.limits
    }
    fn usage(&self) -> BufferUsage {
        self.credits.total.usage()
    }
    fn control_reserve(&self) -> Option<BufferLimits> {
        self.control_reserve
    }
    fn acquire(&self, reservation: usize, initial_len: usize) -> Result<Self::Buffer, BufferError> {
        self.acquire_class(BufferClass::Bulk, reservation, initial_len)
    }
    fn acquire_class(
        &self,
        class: BufferClass,
        reservation: usize,
        initial_len: usize,
    ) -> Result<Self::Buffer, BufferError> {
        if self.closed {
            return Err(BufferError::Closed);
        }
        if reservation == 0 || initial_len > reservation || reservation > self.limits.reserved_bytes
        {
            return Err(BufferError::TooLarge);
        }
        let bulk = class == BufferClass::Bulk && self.control_reserve.is_some();
        if bulk {
            self.credits.bulk.acquire(reservation, self.bulk_limits())?;
        }
        if let Err(error) = self.credits.total.acquire(reservation, self.limits) {
            if bulk {
                self.credits.bulk.release(reservation);
            }
            return Err(error);
        }
        let mut buffer = NativeFrameBuffer {
            bytes: Vec::new(),
            reservation,
            bulk,
            credits: self.credits.clone(),
        };
        buffer.resize(initial_len)?;
        Ok(buffer)
    }
    fn close(&mut self) {
        self.closed = true;
    }
}
