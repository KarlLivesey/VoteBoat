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
//! Explicit process-local diagnostics; no sink runs inside Node polling.
use voteboat::{
    native::observability::{NativeCounterObserver, NativeEventObserver},
    observability::*,
    runtime::*,
};

pub struct Diagnostics {
    counters: NativeCounterObserver,
    events: NativeEventObserver,
    reporter: EventReporter,
}
impl Diagnostics {
    pub fn new(owner: RuntimeOwner) -> Result<Self, EventError> {
        let binding = EventBinding {
            owner,
            generation: EventGeneration::new(1).unwrap(),
        };
        Ok(Self {
            counters: NativeCounterObserver::new(owner),
            events: NativeEventObserver::new(binding, EventLimits::default())?,
            reporter: EventReporter::new(binding),
        })
    }
    pub fn snapshot_counters(&self) -> CounterSnapshot {
        self.counters.snapshot_counters()
    }
    pub fn record(&mut self, sample: NodeObservation) {
        let _ = self.counters.record_bounded(sample);
        let _ = self.reporter.record_sample(&mut self.events, sample);
    }
    pub fn close(&mut self) {
        self.counters.close();
        self.events.close();
    }
    pub fn events(&self, session: &str, after: &str, count: &str) -> Result<String, String> {
        let session = session
            .parse::<u64>()
            .map_err(|_| "invalid event session")?;
        let after = after.parse::<u64>().map_err(|_| "invalid event cursor")?;
        let count = count.parse::<usize>().map_err(|_| "invalid event count")?;
        let binding = self.events.binding();
        let cursor = if session == 0 && after == 0 {
            None
        } else {
            if session != binding.owner.store.session.get() {
                return Err("stale event session".into());
            }
            Some(EventCursor {
                binding,
                sequence: after,
            })
        };
        let page = self
            .events
            .read_events(cursor, count)
            .map_err(|e| format!("{e:?}"))?;
        let next = page.records.last().map_or(after, |r| r.cursor.sequence);
        let records = page
            .records
            .iter()
            .map(render)
            .collect::<Vec<_>>()
            .join(",");
        Ok(format!("OK evidence=local_volatile store_session={} event_generation={} oldest={} latest={} discarded={} missed={} next={} rejected={} events={}",
            binding.owner.store.session.get(), binding.generation.get(), page.window.oldest_sequence,
            page.window.latest_sequence, page.window.discarded, page.missed, next, self.reporter.rejected_events(), records))
    }
}
fn render(record: &EventRecord) -> String {
    let data = match record.event.kind {
        EventKind::StateChanged { previous, current } => format!("state:{previous:?}:{current:?}"),
        EventKind::PollFailures { count } => format!("poll-failures:{count}"),
        EventKind::StepFailures { count } => format!("step-failures:{count}"),
        EventKind::SnapshotProgress {
            completions,
            installs,
        } => format!("snapshots:{completions}:{installs}"),
        EventKind::Pressure {
            ingress_blocked,
            snapshot_refusals,
            connection_failures,
        } => format!("pressure:{ingress_blocked}:{snapshot_refusals}:{connection_failures}"),
    };
    format!(
        "{}@{}:{data}",
        record.cursor.sequence, record.event.sampled_at.0
    )
}

#[cfg(test)]
mod tests {
    use super::*;
    use voteboat::identity::*;
    #[test]
    fn maximum_value_pages_fit_client_reply_budget_and_report_loss() {
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
        let mut diagnostics = Diagnostics::new(owner).unwrap();
        for _ in 0..300 {
            diagnostics
                .events
                .record_bounded(OperationalEvent {
                    owner,
                    sampled_at: MonoTime(u64::MAX),
                    kind: EventKind::Pressure {
                        ingress_blocked: u64::MAX,
                        snapshot_refusals: u64::MAX,
                        connection_failures: u64::MAX,
                    },
                })
                .unwrap();
        }
        let reply = diagnostics.events("1", "0", "16").unwrap();
        assert!(reply.len() + 1 < 4096);
        assert!(reply.contains("discarded=44 missed=44 next=60"));
        assert_eq!(reply.matches("@18446744073709551615:pressure:").count(), 16);
        assert!(diagnostics.events("1", "0", "17").is_err());
        assert!(diagnostics.events("2", "0", "1").is_err());
        diagnostics.close();
        assert_eq!(diagnostics.events("1", "0", "16").unwrap(), reply);
    }
}
