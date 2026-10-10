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
//! Fixed memory timing sink with explicit runtime ownership and no clock reads.
use crate::{
    observability::*,
    runtime::{MonoTime, RuntimeOwner},
};

pub struct NativeTimingObserver {
    owner: RuntimeOwner,
    sampled_at: Option<MonoTime>,
    histograms: [TimingHistogram; 4],
    closed: bool,
}
impl NativeTimingObserver {
    pub fn new(owner: RuntimeOwner) -> Self {
        Self {
            owner,
            sampled_at: None,
            histograms: [TimingHistogram::default(); 4],
            closed: false,
        }
    }
}
impl TimingObserver for NativeTimingObserver {
    fn record_timing(&mut self, sample: TimingSample) -> Result<(), ObservationError> {
        if self.closed {
            return Err(ObservationError::Closed);
        }
        if sample.owner != self.owner {
            return Err(ObservationError::WrongOwner);
        }
        if self.sampled_at.is_some_and(|t| t > sample.sampled_at) {
            return Err(ObservationError::ClockRegressed);
        }
        self.histograms[sample.kind as usize].record(sample.elapsed_ns)?;
        self.sampled_at = Some(sample.sampled_at);
        Ok(())
    }
    fn snapshot_timing(&self, kind: TimingKind) -> TimingSnapshot {
        TimingSnapshot {
            owner: self.owner,
            sampled_at: self.sampled_at,
            kind,
            histogram: self.histograms[kind as usize],
            closed: self.closed,
        }
    }
    fn close(&mut self) {
        self.closed = true;
    }
}
