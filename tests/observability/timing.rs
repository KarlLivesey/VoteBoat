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
use super::*;

struct HostTiming {
    owner: RuntimeOwner,
    at: Option<MonoTime>,
    values: [TimingHistogram; 4],
    closed: bool,
}
impl HostTiming {
    fn new(owner: RuntimeOwner) -> Self {
        Self {
            owner,
            at: None,
            values: [TimingHistogram::default(); 4],
            closed: false,
        }
    }
}
impl TimingObserver for HostTiming {
    fn record_timing(&mut self, s: TimingSample) -> Result<(), ObservationError> {
        if self.closed {
            return Err(ObservationError::Closed);
        }
        if s.owner != self.owner {
            return Err(ObservationError::WrongOwner);
        }
        if self.at.is_some_and(|t| t > s.sampled_at) {
            return Err(ObservationError::ClockRegressed);
        }
        self.values[s.kind as usize].record(s.elapsed_ns)?;
        self.at = Some(s.sampled_at);
        Ok(())
    }
    fn snapshot_timing(&self, kind: TimingKind) -> TimingSnapshot {
        TimingSnapshot {
            owner: self.owner,
            sampled_at: self.at,
            kind,
            histogram: self.values[kind as usize],
            closed: self.closed,
        }
    }
    fn close(&mut self) {
        self.closed = true;
    }
}
fn conformance(mut sink: impl TimingObserver) {
    let owner = owner(1);
    for kind in TimingKind::ALL {
        assert_eq!(sink.snapshot_timing(kind).histogram.count, 0);
        sink.record_timing(TimingSample {
            owner,
            sampled_at: MonoTime(5),
            kind,
            elapsed_ns: 7,
        })
        .unwrap();
    }
    let before = sink.snapshot_timing(TimingKind::PollCompleted);
    for (owner, time, error) in [
        (super::owner(2), 6, ObservationError::WrongOwner),
        (
            RuntimeOwner {
                generation: RuntimeGeneration::new(2).unwrap(),
                ..owner
            },
            6,
            ObservationError::WrongOwner,
        ),
        (owner, 4, ObservationError::ClockRegressed),
    ] {
        assert_eq!(
            sink.record_timing(TimingSample {
                owner,
                sampled_at: MonoTime(time),
                kind: TimingKind::PollCompleted,
                elapsed_ns: 99
            }),
            Err(error)
        );
        assert_eq!(sink.snapshot_timing(TimingKind::PollCompleted), before);
    }
    sink.record_timing(TimingSample {
        owner,
        sampled_at: MonoTime(5),
        kind: TimingKind::PollCompleted,
        elapsed_ns: 0,
    })
    .unwrap();
    let h = sink.snapshot_timing(TimingKind::PollCompleted).histogram;
    assert_eq!(
        (h.count, h.total_ns, h.min_ns, h.max_ns),
        (2, 7, Some(0), Some(7))
    );
    assert_eq!(h.percentile_upper_ns(5000), Some(0));
    assert_eq!(h.percentile_upper_ns(9900), Some(7));
    sink.close();
    let frozen = sink.snapshot_timing(TimingKind::PollCompleted);
    assert!(frozen.closed);
    assert_eq!(
        sink.record_timing(TimingSample {
            owner,
            sampled_at: MonoTime(6),
            kind: TimingKind::PollCompleted,
            elapsed_ns: 1
        }),
        Err(ObservationError::Closed)
    );
    assert_eq!(sink.snapshot_timing(TimingKind::PollCompleted), frozen);
}
#[test]
fn host_timing_sink_conforms() {
    conformance(HostTiming::new(owner(1)));
}
#[cfg(feature = "native")]
#[test]
fn native_timing_sink_conforms_and_restart_is_empty() {
    use voteboat::native::observability::NativeTimingObserver;
    conformance(NativeTimingObserver::new(owner(1)));
    let mut next = owner(1);
    next.store.session = StoreSession::new(2).unwrap();
    let fresh = NativeTimingObserver::new(next);
    for kind in TimingKind::ALL {
        let snapshot = fresh.snapshot_timing(kind);
        assert_eq!(snapshot.owner, next);
        assert_eq!(snapshot.histogram, TimingHistogram::default());
        assert_eq!(snapshot.sampled_at, None);
        assert!(!snapshot.closed);
    }
}
#[test]
fn timing_buckets_cover_zero_every_power_boundary_and_maximum() {
    for value in std::iter::once(0)
        .chain((0..64).flat_map(|bit| {
            let value = 1u64 << bit;
            [value - 1, value, value.saturating_add(1)]
        }))
        .chain([u64::MAX])
    {
        let mut h = TimingHistogram::default();
        h.record(value).unwrap();
        let upper = h.percentile_upper_ns(10000).unwrap();
        let mut expected = 0u64;
        while expected < value {
            expected = expected.saturating_mul(2).saturating_add(1);
        }
        assert_eq!(upper, expected);
        assert_eq!(h.buckets.iter().sum::<u64>(), 1);
        assert_eq!(
            (h.count, h.total_ns, h.min_ns, h.max_ns),
            (1, u128::from(value), Some(value), Some(value))
        );
    }
}
#[test]
fn invalid_percentiles_inconsistent_counts_and_exhaustion_are_explicit() {
    let mut h = TimingHistogram::default();
    assert_eq!(h.percentile_upper_ns(9900), None);
    for value in [1, 3, 5, 7] {
        h.record(value).unwrap();
    }
    assert_eq!(h.percentile_upper_ns(5000), Some(3));
    assert_eq!(h.percentile_upper_ns(7500), Some(7));
    assert_eq!(h.percentile_upper_ns(0), None);
    assert_eq!(h.percentile_upper_ns(10001), None);
    h.count = u64::MAX;
    let before = h;
    assert_eq!(h.record(42), Err(ObservationError::Overloaded));
    assert_eq!(h, before);
    assert_eq!(h.percentile_upper_ns(9900), None);
    h.count = 4;
    h.total_ns = u128::MAX;
    let before = h;
    assert_eq!(h.record(1), Err(ObservationError::Overloaded));
    assert_eq!(h, before);
    let mut h = TimingHistogram::default();
    h.buckets[1] = u64::MAX;
    let before = h;
    assert_eq!(h.record(1), Err(ObservationError::Overloaded));
    assert_eq!(h, before);
}
