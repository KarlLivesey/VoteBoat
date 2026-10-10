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
//! Volatile bounded event history. Cursors never certify consensus progress.
use super::{NodeObservation, NodeState};
use crate::runtime::{MonoTime, RuntimeOwner};
use std::num::NonZeroU64;

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub struct EventGeneration(NonZeroU64);
impl EventGeneration {
    pub fn new(value: u64) -> Option<Self> {
        NonZeroU64::new(value).map(Self)
    }
    pub fn get(self) -> u64 {
        self.0.get()
    }
}
/// The host must select a new binding when replacing a volatile event stream.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub struct EventBinding {
    pub owner: RuntimeOwner,
    pub generation: EventGeneration,
}
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub struct EventCursor {
    pub binding: EventBinding,
    pub sequence: u64,
}
/// Fixed labels and values; no application payloads, credentials or arbitrary strings.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum EventKind {
    StateChanged {
        previous: Option<NodeState>,
        current: NodeState,
    },
    PollFailures {
        count: u64,
    },
    StepFailures {
        count: u64,
    },
    SnapshotProgress {
        completions: u64,
        installs: u64,
    },
    Pressure {
        ingress_blocked: u64,
        snapshot_refusals: u64,
        connection_failures: u64,
    },
}
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub struct OperationalEvent {
    pub owner: RuntimeOwner,
    pub sampled_at: MonoTime,
    pub kind: EventKind,
}
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub struct EventRecord {
    pub cursor: EventCursor,
    pub event: OperationalEvent,
}
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub struct EventLimits {
    pub events: usize,
    /// Retained record allocation capacity, excluding allocator bookkeeping.
    pub bytes: usize,
    /// Each returned page is separately caller-owned; the observer never retains it.
    pub page_events: usize,
}
impl Default for EventLimits {
    fn default() -> Self {
        Self {
            events: 256,
            bytes: 256 * size_of::<EventRecord>(),
            page_events: 16,
        }
    }
}
impl EventLimits {
    pub fn validate(self) -> Result<(), EventError> {
        if self.events == 0
            || self.events > 65_536
            || self.bytes > 16 * 1024 * 1024
            || self.page_events == 0
            || self.page_events > 256
            || self.page_events > self.events
            || self
                .events
                .checked_mul(size_of::<EventRecord>())
                .is_none_or(|n| n > self.bytes)
        {
            return Err(EventError::InvalidLimits);
        }
        Ok(())
    }
}
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub struct EventWindow {
    pub binding: EventBinding,
    pub oldest_sequence: u64,
    pub latest_sequence: u64,
    pub retained_events: usize,
    pub allocated_bytes: usize,
    pub discarded: u64,
    pub closed: bool,
}
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct EventPage {
    pub window: EventWindow,
    /// Exact sequence gap after a supplied cursor. None starts at the oldest retained record.
    pub missed: u64,
    pub records: Vec<EventRecord>,
}
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum EventError {
    InvalidLimits,
    WrongBinding,
    InvalidCursor,
    ClockRegressed,
    Closed,
    Overloaded,
    SequenceExhausted,
    AllocationFailed,
}
/// Version 1, caller-driven diagnostic seam. Recording is bounded and nonblocking.
/// Acceptance assigns a strictly increasing local sequence. A full provider may
/// reject without mutation or evict oldest records, reflected by window.discarded.
/// Reject wrong owners, regressed clocks and post-close input without mutation.
/// Read validates binding/cursor and page limits, and returns ordered copied records.
/// Close preserves retained exports; dropping a view must not close sibling views.
/// No method performs external I/O, calls consensus, or manufactures a durable token.
pub trait EventObserver {
    fn binding(&self) -> EventBinding;
    fn limits(&self) -> EventLimits;
    fn record_bounded(&mut self, event: OperationalEvent) -> Result<EventCursor, EventError>;
    fn read_events(
        &self,
        after: Option<EventCursor>,
        limit: usize,
    ) -> Result<EventPage, EventError>;
    fn close(&mut self);
}

#[derive(Clone, Copy, Debug, Default, Eq, PartialEq)]
pub struct EventDelivery {
    pub attempted: u8,
    pub recorded: u8,
    pub last_error: Option<EventError>,
}
/// At most five fixed-kind events per returned poll sample, with no event allocation.
/// The original poll result stays with the caller; sink failures are diagnostic only.
pub struct EventReporter {
    binding: EventBinding,
    state: Option<NodeState>,
    rejected: u64,
}
impl EventReporter {
    pub fn new(binding: EventBinding) -> Self {
        Self {
            binding,
            state: None,
            rejected: 0,
        }
    }
    pub fn rejected_events(&self) -> u64 {
        self.rejected
    }
    pub fn record_sample(
        &mut self,
        sink: &mut impl EventObserver,
        sample: NodeObservation,
    ) -> Result<EventDelivery, EventError> {
        if sink.binding() != self.binding || sample.owner != self.binding.owner {
            return Err(EventError::WrongBinding);
        }
        let c = sample.counters;
        let kinds = [
            (self.state != Some(sample.state)).then_some(EventKind::StateChanged {
                previous: self.state,
                current: sample.state,
            }),
            (c.failed_polls != 0).then_some(EventKind::PollFailures {
                count: c.failed_polls,
            }),
            (c.step_errors != 0).then_some(EventKind::StepFailures {
                count: c.step_errors,
            }),
            (c.snapshot_events != 0 || c.snapshot_installs != 0).then_some(
                EventKind::SnapshotProgress {
                    completions: c.snapshot_events,
                    installs: c.snapshot_installs,
                },
            ),
            (c.ingress_blocked != 0 || c.snapshot_send_refusals != 0 || c.connection_failures != 0)
                .then_some(EventKind::Pressure {
                    ingress_blocked: c.ingress_blocked,
                    snapshot_refusals: c.snapshot_send_refusals,
                    connection_failures: c.connection_failures,
                }),
        ];
        let mut delivery = EventDelivery::default();
        for kind in kinds.into_iter().flatten() {
            delivery.attempted += 1;
            match sink.record_bounded(OperationalEvent {
                owner: sample.owner,
                sampled_at: sample.sampled_at,
                kind,
            }) {
                Ok(_) => {
                    delivery.recorded += 1;
                    if matches!(kind, EventKind::StateChanged { .. }) {
                        self.state = Some(sample.state);
                    }
                }
                Err(error) => {
                    self.rejected = self.rejected.saturating_add(1);
                    delivery.last_error = Some(error);
                }
            }
        }
        Ok(delivery)
    }
}
