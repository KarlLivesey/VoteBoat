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
//! Preallocated volatile event ring; no I/O, worker or consensus ownership.
use crate::observability::*;
use std::collections::VecDeque;

pub struct NativeEventObserver {
    binding: EventBinding,
    limits: EventLimits,
    records: VecDeque<EventRecord>,
    latest: u64,
    sampled_at: Option<crate::runtime::MonoTime>,
    discarded: u64,
    closed: bool,
}
impl NativeEventObserver {
    pub fn new(binding: EventBinding, limits: EventLimits) -> Result<Self, EventError> {
        limits.validate()?;
        let mut records = VecDeque::new();
        records
            .try_reserve_exact(limits.events)
            .map_err(|_| EventError::AllocationFailed)?;
        if records
            .capacity()
            .checked_mul(size_of::<EventRecord>())
            .is_none_or(|n| n > limits.bytes)
        {
            return Err(EventError::InvalidLimits);
        }
        Ok(Self {
            binding,
            limits,
            records,
            latest: 0,
            sampled_at: None,
            discarded: 0,
            closed: false,
        })
    }
    fn window(&self) -> EventWindow {
        EventWindow {
            binding: self.binding,
            oldest_sequence: self.records.front().map_or(0, |r| r.cursor.sequence),
            latest_sequence: self.latest,
            retained_events: self.records.len(),
            allocated_bytes: self.records.capacity() * size_of::<EventRecord>(),
            discarded: self.discarded,
            closed: self.closed,
        }
    }
}
impl EventObserver for NativeEventObserver {
    fn binding(&self) -> EventBinding {
        self.binding
    }
    fn limits(&self) -> EventLimits {
        self.limits
    }
    fn record_bounded(&mut self, event: OperationalEvent) -> Result<EventCursor, EventError> {
        if self.closed {
            return Err(EventError::Closed);
        }
        if event.owner != self.binding.owner {
            return Err(EventError::WrongBinding);
        }
        if self.sampled_at.is_some_and(|time| time > event.sampled_at) {
            return Err(EventError::ClockRegressed);
        }
        let sequence = self
            .latest
            .checked_add(1)
            .ok_or(EventError::SequenceExhausted)?;
        let cursor = EventCursor {
            binding: self.binding,
            sequence,
        };
        if self.records.len() == self.limits.events {
            self.records.pop_front();
            self.discarded = self.discarded.saturating_add(1);
        }
        self.records.push_back(EventRecord { cursor, event });
        self.latest = sequence;
        self.sampled_at = Some(event.sampled_at);
        Ok(cursor)
    }
    fn read_events(
        &self,
        after: Option<EventCursor>,
        limit: usize,
    ) -> Result<EventPage, EventError> {
        if limit == 0 || limit > self.limits.page_events {
            return Err(EventError::InvalidLimits);
        }
        if after.is_some_and(|c| c.binding != self.binding) {
            return Err(EventError::WrongBinding);
        }
        if after.is_some_and(|c| c.sequence > self.latest) {
            return Err(EventError::InvalidCursor);
        }
        let sequence = after.map_or(0, |c| c.sequence);
        let window = self.window();
        let missed = after.map_or(0, |_| {
            window
                .oldest_sequence
                .saturating_sub(sequence.saturating_add(1))
        });
        let mut records = Vec::with_capacity(limit);
        records.extend(
            self.records
                .iter()
                .filter(|r| r.cursor.sequence > sequence)
                .take(limit)
                .copied(),
        );
        Ok(EventPage {
            window,
            missed,
            records,
        })
    }
    fn close(&mut self) {
        self.closed = true;
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    // Constructor fields are private; sequence exhaustion must not evict a record.
    #[test]
    fn exhausted_sequence_preserves_state() {
        use crate::{identity::*, runtime::*};
        let owner = RuntimeOwner {
            store: StoreBinding {
                identity: StoreIdentity {
                    id: StoreId::new(1).unwrap(),
                    incarnation: StoreIncarnation::new(1).unwrap(),
                },
                session: StoreSession::new(1).unwrap(),
            },
            lane: ExecutionLaneId::new(1).unwrap(),
            generation: RuntimeGeneration::new(1).unwrap(),
        };
        let mut observer = NativeEventObserver::new(
            EventBinding {
                owner,
                generation: EventGeneration::new(1).unwrap(),
            },
            EventLimits::default(),
        )
        .unwrap();
        observer.latest = u64::MAX;
        let before = observer.read_events(None, 1).unwrap();
        assert_eq!(
            observer.record_bounded(OperationalEvent {
                owner,
                sampled_at: MonoTime(1),
                kind: EventKind::PollFailures { count: 1 }
            }),
            Err(EventError::SequenceExhausted)
        );
        assert_eq!(observer.read_events(None, 1).unwrap(), before);
    }
}
