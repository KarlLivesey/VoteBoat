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
    native::observability::{NativeCounterObserver, NativeEventObserver, NativeTimingObserver},
    observability::*,
    runtime::*,
};

pub struct Diagnostics {
    counters: NativeCounterObserver,
    events: NativeEventObserver,
    reporter: EventReporter,
    timings: NativeTimingObserver,
    rejected_timings: u64,
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
            timings: NativeTimingObserver::new(owner),
            rejected_timings: 0,
        })
    }
    pub fn metrics(&self) -> String {
        let snapshot = self.counters.snapshot_counters();
        let c = snapshot.counters;
        format!("OK evidence=local_volatile store_session={} polls={} failed_polls={} owner_steps={} step_errors={} worker_events={} snapshot_events={} snapshot_installs={} persistence_batches={} applications={} peer_sends={} peer_received={} ingress_blocked={} connection_failures={}", snapshot.owner.store.session.get(), c.polls, c.failed_polls, c.owner_steps, c.step_errors, c.worker_events, c.snapshot_events, c.snapshot_installs, c.persistence_batches, c.applications, c.peer_sends, c.peer_received, c.ingress_blocked, c.connection_failures)
    }
    pub fn record(&mut self, sample: NodeObservation) {
        let _ = self.counters.record_bounded(sample);
        let _ = self.reporter.record_sample(&mut self.events, sample);
    }
    pub fn close(&mut self) {
        self.counters.close();
        self.events.close();
        self.timings.close();
    }
    pub fn record_timing(
        &mut self,
        kind: TimingKind,
        elapsed: std::time::Duration,
        time: MonoTime,
    ) {
        let sample = TimingSample {
            owner: self.counters.snapshot_counters().owner,
            sampled_at: time,
            kind,
            elapsed_ns: elapsed.as_nanos().min(u128::from(u64::MAX)) as u64,
        };
        if self.timings.record_timing(sample).is_err() {
            self.rejected_timings = self.rejected_timings.saturating_add(1);
        }
    }
    pub fn timings(&self) -> String {
        let owner = self.counters.snapshot_counters().owner;
        let mut reply = format!("OK evidence=local_volatile unit=ns store_session={} rejected={} percentile=bucket_upper_bound", owner.store.session.get(), self.rejected_timings);
        for kind in TimingKind::ALL {
            let h = self.timings.snapshot_timing(kind).histogram;
            use std::fmt::Write;
            write!(
                &mut reply,
                " {kind:?}={},sum:{},min:{},max:{},p99_upper:{}",
                h.count,
                h.total_ns,
                optional_ns(h.min_ns),
                optional_ns(h.max_ns),
                optional_ns(h.percentile_upper_ns(9900))
            )
            .unwrap();
        }
        reply
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
fn optional_ns(value: Option<u64>) -> String {
    value.map_or_else(|| "NA".into(), |n| n.to_string())
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
    fn maximum_timing_values_fit_reply_and_closed_sink_reports_loss() {
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
        for kind in TimingKind::ALL {
            diagnostics.record_timing(kind, std::time::Duration::MAX, MonoTime(u64::MAX));
        }
        let reply = diagnostics.timings();
        assert!(reply.len() < 4096);
        assert_eq!(reply.matches("p99_upper:18446744073709551615").count(), 4);
        assert!(!reply.contains("Some("));
        let before = diagnostics
            .timings
            .snapshot_timing(TimingKind::PollCompleted)
            .histogram;
        diagnostics.close();
        diagnostics.record_timing(
            TimingKind::PollCompleted,
            std::time::Duration::ZERO,
            MonoTime(u64::MAX),
        );
        assert!(diagnostics.timings().contains("rejected=1"));
        assert_eq!(
            diagnostics
                .timings
                .snapshot_timing(TimingKind::PollCompleted)
                .histogram,
            before
        );
    }
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
