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
//! Independent downstream pool; no native implementation internals.
use std::{cell::Cell, rc::Rc};
use voteboat::buffer::*;
#[derive(Clone)]
pub struct HostPool {
    usage: Rc<Cell<BufferUsage>>,
    limits: BufferLimits,
    closed: bool,
    reserve: Option<BufferLimits>,
    bulk: Rc<Cell<BufferUsage>>,
    calls: Rc<Cell<[usize; 2]>>,
}
impl HostPool {
    pub fn new(bytes: usize, leases: usize) -> Self {
        Self {
            usage: Rc::new(Cell::new(BufferUsage::default())),
            limits: BufferLimits {
                reserved_bytes: bytes,
                leases,
            },
            closed: false,
            reserve: None,
            bulk: Rc::new(Cell::new(BufferUsage::default())),
            calls: Rc::new(Cell::new([0; 2])),
        }
    }
    pub fn with_control_reserve(bytes: usize, leases: usize, reserve: BufferLimits) -> Self {
        let mut pool = Self::new(bytes, leases);
        pool.limits.validate_control_reserve(reserve).unwrap();
        pool.reserve = Some(reserve);
        pool
    }
    pub fn class_calls(&self) -> [usize; 2] {
        self.calls.get()
    }
}
pub struct HostBuffer {
    bytes: Vec<u8>,
    reservation: usize,
    usage: Rc<Cell<BufferUsage>>,
    bulk: Option<Rc<Cell<BufferUsage>>>,
}
impl AsRef<[u8]> for HostBuffer {
    fn as_ref(&self) -> &[u8] {
        &self.bytes
    }
}
impl AsMut<[u8]> for HostBuffer {
    fn as_mut(&mut self) -> &mut [u8] {
        &mut self.bytes
    }
}
impl FrameBuffer for HostBuffer {
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
            self.bytes.reserve_exact(len - self.bytes.len());
        }
        self.bytes.resize(len, 0);
        Ok(())
    }
}
impl Drop for HostBuffer {
    fn drop(&mut self) {
        self.bytes = Vec::new();
        let mut usage = self.usage.get();
        usage.reserved_bytes -= self.reservation;
        usage.leases -= 1;
        self.usage.set(usage);
        if let Some(bulk) = &self.bulk {
            let mut usage = bulk.get();
            usage.reserved_bytes -= self.reservation;
            usage.leases -= 1;
            bulk.set(usage);
        }
    }
}
impl BufferPool for HostPool {
    type Buffer = HostBuffer;
    fn limits(&self) -> BufferLimits {
        self.limits
    }
    fn usage(&self) -> BufferUsage {
        self.usage.get()
    }
    fn control_reserve(&self) -> Option<BufferLimits> {
        self.reserve
    }
    fn acquire(&self, reservation: usize, initial_len: usize) -> Result<HostBuffer, BufferError> {
        self.acquire_class(BufferClass::Bulk, reservation, initial_len)
    }
    fn acquire_class(
        &self,
        class: BufferClass,
        reservation: usize,
        initial_len: usize,
    ) -> Result<HostBuffer, BufferError> {
        let mut calls = self.calls.get();
        calls[if class == BufferClass::Control { 1 } else { 0 }] += 1;
        self.calls.set(calls);
        if self.closed {
            return Err(BufferError::Closed);
        }
        if reservation == 0 || reservation > self.limits.reserved_bytes || initial_len > reservation
        {
            return Err(BufferError::TooLarge);
        }
        let mut usage = self.usage.get();
        if usage.leases == self.limits.leases
            || reservation > self.limits.reserved_bytes - usage.reserved_bytes
        {
            return Err(BufferError::Overloaded);
        }
        let bulk = if class == BufferClass::Bulk {
            self.reserve
        } else {
            None
        };
        if let Some(reserve) = bulk {
            let mut used = self.bulk.get();
            if used.leases >= self.limits.leases - reserve.leases
                || reservation
                    > self.limits.reserved_bytes - reserve.reserved_bytes - used.reserved_bytes
            {
                return Err(BufferError::Overloaded);
            }
            used.leases += 1;
            used.reserved_bytes += reservation;
            self.bulk.set(used);
        }
        usage.leases += 1;
        usage.reserved_bytes += reservation;
        self.usage.set(usage);
        let mut buffer = HostBuffer {
            bytes: Vec::new(),
            reservation,
            usage: self.usage.clone(),
            bulk: bulk.map(|_| self.bulk.clone()),
        };
        buffer.resize(initial_len)?;
        Ok(buffer)
    }
    fn close(&mut self) {
        self.closed = true;
    }
}
