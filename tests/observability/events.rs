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
use std::collections::VecDeque;
fn binding(id: u128) -> EventBinding {
    EventBinding {
        owner: owner(id),
        generation: EventGeneration::new(1).unwrap(),
    }
}
fn event(binding: EventBinding, time: u64) -> OperationalEvent {
    OperationalEvent {
        owner: binding.owner,
        sampled_at: MonoTime(time),
        kind: EventKind::PollFailures { count: 1 },
    }
}
fn limits() -> EventLimits {
    EventLimits {
        events: 2,
        bytes: 2 * size_of::<EventRecord>(),
        page_events: 2,
    }
}
// Independent host resource. A view only owns its slot; no native provider is wrapped.
#[derive(Clone)]
struct Slot {
    binding: EventBinding,
    records: VecDeque<EventRecord>,
    latest: u64,
    discarded: u64,
    closed: bool,
}
struct HostEvents {
    slots: Rc<RefCell<[Slot; 2]>>,
    slot: usize,
}
impl EventObserver for HostEvents {
    fn binding(&self) -> EventBinding {
        self.slots.borrow()[self.slot].binding
    }
    fn limits(&self) -> EventLimits {
        limits()
    }
    fn record_bounded(&mut self, event: OperationalEvent) -> Result<EventCursor, EventError> {
        let mut slots = self.slots.borrow_mut();
        let s = &mut slots[self.slot];
        if s.closed {
            return Err(EventError::Closed);
        }
        if s.binding.owner != event.owner {
            return Err(EventError::WrongBinding);
        }
        if s.records
            .back()
            .is_some_and(|r| r.event.sampled_at > event.sampled_at)
        {
            return Err(EventError::ClockRegressed);
        }
        let sequence = s
            .latest
            .checked_add(1)
            .ok_or(EventError::SequenceExhausted)?;
        let cursor = EventCursor {
            binding: s.binding,
            sequence,
        };
        if s.records.len() == 2 {
            s.records.pop_front();
            s.discarded = s.discarded.saturating_add(1);
        }
        s.records.push_back(EventRecord { cursor, event });
        s.latest = sequence;
        Ok(cursor)
    }
    fn read_events(
        &self,
        after: Option<EventCursor>,
        limit: usize,
    ) -> Result<EventPage, EventError> {
        let slots = self.slots.borrow();
        let s = &slots[self.slot];
        if limit == 0 || limit > 2 {
            return Err(EventError::InvalidLimits);
        }
        if after.is_some_and(|c| c.binding != s.binding) {
            return Err(EventError::WrongBinding);
        }
        if after.is_some_and(|c| c.sequence > s.latest) {
            return Err(EventError::InvalidCursor);
        }
        let sequence = after.map_or(0, |c| c.sequence);
        let first = s.records.front().map_or(0, |r| r.cursor.sequence);
        let mut records = Vec::with_capacity(limit);
        records.extend(
            s.records
                .iter()
                .filter(|r| r.cursor.sequence > sequence)
                .take(limit)
                .copied(),
        );
        Ok(EventPage {
            window: EventWindow {
                binding: s.binding,
                oldest_sequence: first,
                latest_sequence: s.latest,
                retained_events: s.records.len(),
                allocated_bytes: 2 * size_of::<EventRecord>(),
                discarded: s.discarded,
                closed: s.closed,
            },
            missed: after.map_or(0, |_| first.saturating_sub(sequence.saturating_add(1))),
            records,
        })
    }
    fn close(&mut self) {
        self.slots.borrow_mut()[self.slot].closed = true;
    }
}
fn host_views() -> (HostEvents, HostEvents) {
    let slots = Rc::new(RefCell::new([1, 2].map(|id| Slot {
        binding: binding(id),
        records: VecDeque::with_capacity(2),
        latest: 0,
        discarded: 0,
        closed: false,
    })));
    (
        HostEvents {
            slots: slots.clone(),
            slot: 0,
        },
        HostEvents { slots, slot: 1 },
    )
}
fn rejection_checks(sink: &mut impl EventObserver, saved: &EventPage) {
    let b = sink.binding();
    let mut generation = b;
    generation.generation = EventGeneration::new(2).unwrap();
    for wrong in [binding(99), generation] {
        assert_eq!(
            sink.read_events(
                Some(EventCursor {
                    binding: wrong,
                    sequence: 1
                }),
                1
            ),
            Err(EventError::WrongBinding)
        );
    }
    assert_eq!(
        sink.record_bounded(event(binding(99), 11)),
        Err(EventError::WrongBinding)
    );
    assert_eq!(
        sink.record_bounded(event(b, 9)),
        Err(EventError::ClockRegressed)
    );
    assert_eq!(sink.read_events(None, 1).unwrap(), *saved);
    for size in [0, 3, usize::MAX] {
        assert_eq!(sink.read_events(None, size), Err(EventError::InvalidLimits));
    }
    assert_eq!(
        sink.read_events(
            Some(EventCursor {
                binding: b,
                sequence: 2
            }),
            1
        ),
        Err(EventError::InvalidCursor)
    );
}
fn conformance(mut sink: impl EventObserver) {
    let b = sink.binding();
    assert_eq!(sink.limits(), limits());
    let zero = EventCursor {
        binding: b,
        sequence: 0,
    };
    assert!(sink.read_events(None, 2).unwrap().records.is_empty());
    let first = sink.record_bounded(event(b, 10)).unwrap();
    let saved = sink.read_events(None, 1).unwrap();
    assert_eq!(first.sequence, 1);
    rejection_checks(&mut sink, &saved);
    for _ in 0..3 {
        sink.record_bounded(event(b, 10)).unwrap();
    }
    let page = sink.read_events(Some(zero), 1).unwrap();
    assert_eq!(page.window.retained_events, 2);
    assert_eq!(page.window.discarded, 2);
    assert_eq!(page.window.latest_sequence, 4);
    assert_eq!(page.window.oldest_sequence, 3);
    assert_eq!(page.missed, 2);
    assert_eq!(page.records.len(), 1);
    assert!(page.records.capacity() <= sink.limits().page_events);
    assert!(page.window.allocated_bytes <= sink.limits().bytes);
    assert_eq!(page.records[0].cursor.sequence, 3);
    let next = sink.read_events(Some(page.records[0].cursor), 2).unwrap();
    assert_eq!(next.missed, 0);
    assert_eq!(next.records[0].cursor.sequence, 4);
    assert_eq!(saved.records[0].cursor, first);
    sink.close();
    sink.close();
    let closed = sink.read_events(None, 2).unwrap();
    assert!(closed.window.closed);
    assert_eq!(sink.record_bounded(event(b, 11)), Err(EventError::Closed));
    assert_eq!(sink.read_events(None, 2).unwrap(), closed);
}
#[test]
fn host_event_conformance_and_shared_view_independence() {
    let (first, mut second) = host_views();
    conformance(first);
    second.record_bounded(event(second.binding(), 1)).unwrap();
    assert!(!second.read_events(None, 2).unwrap().window.closed);
}
#[cfg(feature = "native")]
#[test]
fn native_event_conformance_and_construction_limits() {
    use voteboat::native::observability::NativeEventObserver;
    conformance(NativeEventObserver::new(binding(1), limits()).unwrap());
    for l in [
        EventLimits {
            events: 0,
            ..limits()
        },
        EventLimits {
            events: 65_537,
            ..limits()
        },
        EventLimits {
            bytes: 0,
            ..limits()
        },
        EventLimits {
            page_events: 0,
            ..limits()
        },
        EventLimits {
            page_events: 3,
            ..limits()
        },
    ] {
        assert!(matches!(
            NativeEventObserver::new(binding(1), l),
            Err(EventError::InvalidLimits)
        ));
    }
    let mut other = NativeEventObserver::new(binding(2), limits()).unwrap();
    other.record_bounded(event(binding(2), 1)).unwrap();
    assert!(!other.read_events(None, 2).unwrap().window.closed);
}
#[test]
fn reporter_bounds_kinds_and_refusal_preserves_state_change_for_retry() {
    let (mut sink, _) = host_views();
    let mut reporter = EventReporter::new(sink.binding());
    let s = sample(sink.binding().owner, 1, 1);
    let delivery = reporter.record_sample(&mut sink, s).unwrap();
    assert_eq!(delivery.attempted, 5);
    assert_eq!(delivery.recorded, 5);
    assert_eq!(delivery.last_error, None);
    assert_eq!(sink.read_events(None, 2).unwrap().window.discarded, 3);
    assert_eq!(
        reporter
            .record_sample(&mut sink, sample(s.owner, 1, 0))
            .unwrap()
            .attempted,
        0
    );
    let unchanged = sink.read_events(None, 2).unwrap();
    assert_eq!(
        reporter.record_sample(&mut sink, sample(owner(99), 2, 1)),
        Err(EventError::WrongBinding)
    );
    assert_eq!(sink.read_events(None, 2).unwrap(), unchanged);
    sink.close();
    let mut shutdown = sample(s.owner, 2, 1);
    shutdown.state = NodeState::Drained;
    let failed = reporter.record_sample(&mut sink, shutdown).unwrap();
    assert_eq!(failed.recorded, 0);
    assert_eq!(failed.attempted, 5);
    assert_eq!(failed.last_error, Some(EventError::Closed));
    assert_eq!(reporter.rejected_events(), 5);
    // The failed state report remains pending, so another sample retries it.
    assert_eq!(
        reporter
            .record_sample(&mut sink, shutdown)
            .unwrap()
            .attempted,
        5
    );
}
