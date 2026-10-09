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
struct Credits {
    bytes: AtomicUsize,
    leases: AtomicUsize,
}
/// Clone shares credits, while close is scoped to that view. No buffers are
/// cached: each lease frees its allocation before returning reserved credits.
#[derive(Clone)]
pub struct NativeBufferPool {
    credits: Arc<Credits>,
    limits: BufferLimits,
    closed: bool,
}
impl NativeBufferPool {
    pub fn new(limits: BufferLimits) -> Result<Self, BufferError> {
        Ok(Self {
            credits: Arc::new(Credits {
                bytes: AtomicUsize::new(0),
                leases: AtomicUsize::new(0),
            }),
            limits: limits.validate()?,
            closed: false,
        })
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
pub struct NativeFrameBuffer {
    bytes: Vec<u8>,
    reservation: usize,
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
        self.credits
            .bytes
            .fetch_sub(self.reservation, Ordering::AcqRel);
        self.credits.leases.fetch_sub(1, Ordering::AcqRel);
    }
}
impl BufferPool for NativeBufferPool {
    type Buffer = NativeFrameBuffer;
    fn limits(&self) -> BufferLimits {
        self.limits
    }
    fn usage(&self) -> BufferUsage {
        BufferUsage {
            reserved_bytes: self.credits.bytes.load(Ordering::Acquire),
            leases: self.credits.leases.load(Ordering::Acquire),
        }
    }
    fn acquire(&self, reservation: usize, initial_len: usize) -> Result<Self::Buffer, BufferError> {
        if self.closed {
            return Err(BufferError::Closed);
        }
        if reservation == 0 || initial_len > reservation || reservation > self.limits.reserved_bytes
        {
            return Err(BufferError::TooLarge);
        }
        reserve(&self.credits.leases, 1, self.limits.leases)?;
        if let Err(e) = reserve(&self.credits.bytes, reservation, self.limits.reserved_bytes) {
            self.credits.leases.fetch_sub(1, Ordering::AcqRel);
            return Err(e);
        }
        let mut buffer = NativeFrameBuffer {
            bytes: Vec::new(),
            reservation,
            credits: self.credits.clone(),
        };
        buffer.resize(initial_len)?;
        Ok(buffer)
    }
    fn close(&mut self) {
        self.closed = true;
    }
}
