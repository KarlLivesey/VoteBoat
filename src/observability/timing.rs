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
//! Fixed-cardinality, volatile duration observations supplied by the host.
use super::ObservationError;
use crate::runtime::{MonoTime, RuntimeOwner};

/// Outcomes describe the local measured boundary, never remote durability.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
#[repr(usize)]
pub enum TimingKind {
    PollCompleted,
    PollFailed,
    ConnectionCompleted,
    ConnectionInterrupted,
}
impl TimingKind {
    pub const ALL: [Self; 4] = [
        Self::PollCompleted,
        Self::PollFailed,
        Self::ConnectionCompleted,
        Self::ConnectionInterrupted,
    ];
}
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub struct TimingSample {
    pub owner: RuntimeOwner,
    pub sampled_at: MonoTime,
    pub kind: TimingKind,
    pub elapsed_ns: u64,
}
/// Bucket 0 is exactly zero; bucket i covers 2^(i-1)..=2^i-1 ns.
/// The final bucket ends at u64::MAX. No raw samples or labels are retained.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub struct TimingHistogram {
    pub count: u64,
    pub total_ns: u128,
    pub min_ns: Option<u64>,
    pub max_ns: Option<u64>,
    pub buckets: [u64; 65],
}
impl Default for TimingHistogram {
    fn default() -> Self {
        Self {
            count: 0,
            total_ns: 0,
            min_ns: None,
            max_ns: None,
            buckets: [0; 65],
        }
    }
}
impl TimingHistogram {
    /// Refusal leaves every field unchanged; the caller can count lost samples.
    pub fn record(&mut self, elapsed_ns: u64) -> Result<(), ObservationError> {
        let bucket = (64 - elapsed_ns.leading_zeros()) as usize;
        let count = self
            .count
            .checked_add(1)
            .ok_or(ObservationError::Overloaded)?;
        let samples = self.buckets[bucket]
            .checked_add(1)
            .ok_or(ObservationError::Overloaded)?;
        let total = self
            .total_ns
            .checked_add(u128::from(elapsed_ns))
            .ok_or(ObservationError::Overloaded)?;
        self.count = count;
        self.buckets[bucket] = samples;
        self.total_ns = total;
        self.min_ns = Some(self.min_ns.map_or(elapsed_ns, |n| n.min(elapsed_ns)));
        self.max_ns = Some(self.max_ns.map_or(elapsed_ns, |n| n.max(elapsed_ns)));
        Ok(())
    }
    /// Nearest-rank bucket upper bound, not an exact percentile. Basis points
    /// must be 1..=10000. Empty or inconsistent histograms return None.
    pub fn percentile_upper_ns(&self, basis_points: u16) -> Option<u64> {
        if self.count == 0 || !(1..=10000).contains(&basis_points) {
            return None;
        }
        let total: u128 = self.buckets.iter().map(|n| u128::from(*n)).sum();
        if total != u128::from(self.count) {
            return None;
        }
        let rank = (u128::from(self.count) * u128::from(basis_points)).div_ceil(10000);
        let mut seen = 0u128;
        for (bucket, count) in self.buckets.iter().enumerate() {
            seen += u128::from(*count);
            if seen >= rank {
                return Some(if bucket == 64 {
                    u64::MAX
                } else {
                    (1u64 << bucket) - 1
                });
            }
        }
        None
    }
}
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub struct TimingSnapshot {
    pub owner: RuntimeOwner,
    pub sampled_at: Option<MonoTime>,
    pub kind: TimingKind,
    pub histogram: TimingHistogram,
    pub closed: bool,
}
/// Version 1 bounded synchronous sink. A host measures outside consensus and
/// keeps the original result on refusal. Implementations reject foreign owners,
/// regressed sample times and writes after close without mutation. Equal times
/// and repeated samples are allowed. No I/O, callback into consensus, unbounded
/// allocation or labels on recording. Restart creates a new empty view.
pub trait TimingObserver {
    fn record_timing(&mut self, sample: TimingSample) -> Result<(), ObservationError>;
    fn snapshot_timing(&self, kind: TimingKind) -> TimingSnapshot;
    fn close(&mut self);
}
