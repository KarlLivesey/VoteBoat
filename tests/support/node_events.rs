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
use voteboat::observability::*;
struct RefusingEvents {
    binding: EventBinding,
    closed: bool,
}
impl EventObserver for RefusingEvents {
    fn binding(&self) -> EventBinding {
        self.binding
    }
    fn limits(&self) -> EventLimits {
        EventLimits::default()
    }
    fn record_bounded(&mut self, _: OperationalEvent) -> Result<EventCursor, EventError> {
        Err(if self.closed {
            EventError::Closed
        } else {
            EventError::Overloaded
        })
    }
    fn read_events(&self, _: Option<EventCursor>, _: usize) -> Result<EventPage, EventError> {
        Err(EventError::Overloaded)
    }
    fn close(&mut self) {
        self.closed = true;
    }
}
fn poll(n: &mut Boat, sink: &mut RefusingEvents, reporter: &mut EventReporter) {
    let result = n.poll(MonoTime(0), NodePollBudget::default());
    let sample =
        NodeObservation::from_poll(n.local().owner.identity(), MonoTime(0), n.state(), &result);
    let delivery = reporter.record_sample(sink, sample).unwrap();
    assert_eq!(delivery.recorded, 0);
    assert!(delivery.attempted > 0);
    assert_eq!(
        delivery.last_error,
        Some(if sink.closed {
            EventError::Closed
        } else {
            EventError::Overloaded
        })
    );
    result.unwrap();
}
#[test]
fn failed_event_sink_cannot_change_original_write_read_or_shutdown() {
    let mut node = elected();
    let binding = EventBinding {
        owner: node.local().owner.identity(),
        generation: EventGeneration::new(1).unwrap(),
    };
    let mut sink = RefusingEvents {
        binding,
        closed: false,
    };
    let mut reporter = EventReporter::new(binding);
    let ticket = node.propose(request(1)).unwrap();
    let mut wrote = false;
    for _ in 0..100 {
        poll(&mut node, &mut sink, &mut reporter);
        if let Some(output) = node.poll_client() {
            assert_eq!(output.ticket(), ticket);
            assert!(matches!(
                node.complete_client(output).unwrap(),
                ClientOutcome::Applied { .. }
            ));
            wrote = true;
            break;
        }
    }
    assert!(wrote);
    let ticket = node.read(group(1), ()).unwrap();
    let mut read = false;
    for _ in 0..100 {
        poll(&mut node, &mut sink, &mut reporter);
        if let Some(output) = node.poll_read() {
            assert_eq!(output.ticket(), ticket);
            assert!(matches!(
                node.complete_read(output).unwrap(),
                ReadOutcome::Read { result: Ok(7), .. }
            ));
            read = true;
            break;
        }
    }
    assert!(read);
    sink.close();
    node.begin_shutdown();
    for _ in 0..100 {
        poll(&mut node, &mut sink, &mut reporter);
        if node.is_drained() {
            break;
        }
    }
    assert!(node.is_drained());
    assert!(reporter.rejected_events() > 0);
    assert_eq!(node.local().applications[&group(1)].read_applied(2), Ok(7));
}
