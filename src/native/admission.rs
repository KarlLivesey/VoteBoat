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
//! Caller-selected shareable bulk budget; no I/O, clocks, locks or workers.
use crate::{
    admission::*,
    outbound::{MessageClass, OutboundUsage},
};
use std::sync::{
    atomic::{AtomicUsize, Ordering},
    Arc,
};
struct Credits {
    batches: AtomicUsize,
    messages: AtomicUsize,
    bytes: AtomicUsize,
}
/// Clones share finite bulk credits. Closing one view leaves other views and
/// accepted reservations valid. Queue hard limits/control reserve still apply.
#[derive(Clone)]
pub struct NativeAdmissionPolicy {
    credits: Arc<Credits>,
    max: OutboundUsage,
    closed: bool,
}
impl NativeAdmissionPolicy {
    pub fn new(max: OutboundUsage) -> Result<Self, AdmissionError> {
        if max.batches == 0
            || max.batches > 65536
            || max.messages == 0
            || max.messages > 1_048_576
            || max.bytes == 0
            || max.bytes > 1024 * 1024 * 1024
        {
            return Err(AdmissionError::InvalidLimits);
        }
        Ok(Self {
            credits: Arc::new(Credits {
                batches: AtomicUsize::new(0),
                messages: AtomicUsize::new(0),
                bytes: AtomicUsize::new(0),
            }),
            max,
            closed: false,
        })
    }
    pub fn limits(&self) -> OutboundUsage {
        self.max
    }
    /// Fields are individually bounded samples, not a coherent concurrent view.
    pub fn usage(&self) -> OutboundUsage {
        OutboundUsage {
            batches: self.credits.batches.load(Ordering::Acquire),
            messages: self.credits.messages.load(Ordering::Acquire),
            bytes: self.credits.bytes.load(Ordering::Acquire),
        }
    }
}
fn reserve(counter: &AtomicUsize, amount: usize, max: usize) -> Result<(), AdmissionError> {
    let mut current = counter.load(Ordering::Relaxed);
    for _ in 0..16 {
        let next = current
            .checked_add(amount)
            .filter(|v| *v <= max)
            .ok_or(AdmissionError::Overloaded)?;
        match counter.compare_exchange_weak(current, next, Ordering::AcqRel, Ordering::Relaxed) {
            Ok(_) => return Ok(()),
            Err(actual) => current = actual,
        }
    }
    Err(AdmissionError::Overloaded)
}
struct Permit {
    credits: Arc<Credits>,
    cost: OutboundUsage,
}
impl Drop for Permit {
    fn drop(&mut self) {
        self.credits
            .bytes
            .fetch_sub(self.cost.bytes, Ordering::AcqRel);
        self.credits
            .messages
            .fetch_sub(self.cost.messages, Ordering::AcqRel);
        self.credits
            .batches
            .fetch_sub(self.cost.batches, Ordering::AcqRel);
    }
}
impl AdmissionPolicy for NativeAdmissionPolicy {
    fn reserve(&self, request: AdmissionRequest) -> Result<AdmissionLease, AdmissionError> {
        if self.closed {
            return Err(AdmissionError::Closed);
        }
        let cost = request.cost;
        if request.class == MessageClass::Control
            || request.peer == request.owner.node
            || cost.batches != 1
            || cost.messages == 0
            || cost.bytes < std::mem::size_of::<crate::raft::Message>()
        {
            return Err(AdmissionError::InvalidRequest);
        }
        reserve(&self.credits.batches, cost.batches, self.max.batches)?;
        if let Err(error) = reserve(&self.credits.messages, cost.messages, self.max.messages) {
            self.credits
                .batches
                .fetch_sub(cost.batches, Ordering::AcqRel);
            return Err(error);
        }
        if let Err(error) = reserve(&self.credits.bytes, cost.bytes, self.max.bytes) {
            self.credits
                .messages
                .fetch_sub(cost.messages, Ordering::AcqRel);
            self.credits
                .batches
                .fetch_sub(cost.batches, Ordering::AcqRel);
            return Err(error);
        }
        Ok(AdmissionLease::new(Permit {
            credits: self.credits.clone(),
            cost,
        }))
    }
    fn close(&mut self) {
        self.closed = true;
    }
}
