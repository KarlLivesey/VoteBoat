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
//! Bounded, volatile post-poll diagnostics; never consensus or durability evidence.
use crate::runtime::{MonoTime, NodeError, NodeProgress, NodeState, RuntimeOwner};

/// Fixed cardinality, saturating event counts. These are returned progress, not
/// durable/apply watermarks, client success counts or complete I/O attribution.
#[derive(Clone, Copy, Debug, Default, Eq, PartialEq)]
pub struct NodeCounters {
    pub polls: u64,
    pub failed_polls: u64,
    pub owner_steps: u64,
    pub step_errors: u64,
    pub worker_events: u64,
    pub snapshot_events: u64,
    pub snapshot_installs: u64,
    pub persistence_batches: u64,
    pub applications: u64,
    pub peer_sends: u64,
    pub peer_received: u64,
    pub ingress_blocked: u64,
    pub connection_failures: u64,
}
impl NodeCounters {
    pub fn saturating_add(self, other: Self) -> Self {
        Self {
            polls: self.polls.saturating_add(other.polls),
            failed_polls: self.failed_polls.saturating_add(other.failed_polls),
            owner_steps: self.owner_steps.saturating_add(other.owner_steps),
            step_errors: self.step_errors.saturating_add(other.step_errors),
            worker_events: self.worker_events.saturating_add(other.worker_events),
            snapshot_events: self.snapshot_events.saturating_add(other.snapshot_events),
            snapshot_installs: self
                .snapshot_installs
                .saturating_add(other.snapshot_installs),
            persistence_batches: self
                .persistence_batches
                .saturating_add(other.persistence_batches),
            applications: self.applications.saturating_add(other.applications),
            peer_sends: self.peer_sends.saturating_add(other.peer_sends),
            peer_received: self.peer_received.saturating_add(other.peer_received),
            ingress_blocked: self.ingress_blocked.saturating_add(other.ingress_blocked),
            connection_failures: self
                .connection_failures
                .saturating_add(other.connection_failures),
        }
    }
}
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub struct NodeObservation {
    pub owner: RuntimeOwner,
    pub sampled_at: MonoTime,
    pub state: NodeState,
    pub counters: NodeCounters,
}
impl NodeObservation {
    /// Capture after poll returns. Failed polls expose no partial progress here.
    /// The caller supplies the node's post-call state and a monotonic clock.
    pub fn from_poll(
        owner: RuntimeOwner,
        sampled_at: MonoTime,
        state: NodeState,
        result: &Result<NodeProgress, NodeError>,
    ) -> Self {
        let mut counters = NodeCounters {
            polls: 1,
            failed_polls: u64::from(result.is_err()),
            ..NodeCounters::default()
        };
        if let Ok(progress) = result {
            if let Some(r) = &progress.replica {
                counters.owner_steps = r.steps.len() as u64;
                counters.step_errors = r.steps.iter().filter(|s| s.error.is_some()).count() as u64;
                counters.worker_events = r.worker_events as u64;
                counters.snapshot_events = r.snapshot_events as u64;
                counters.snapshot_installs = r.snapshot_installs as u64;
                counters.persistence_batches = r.persistence_batches as u64;
                counters.applications = r.applications as u64;
            }
            if let Some(p) = &progress.peers {
                counters.peer_sends = p.sends as u64;
                counters.peer_received = p.received as u64;
                counters.ingress_blocked = p.ingress_blocked as u64;
                counters.connection_failures = p.connection_failures as u64;
            }
        }
        Self {
            owner,
            sampled_at,
            state,
            counters,
        }
    }
}
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub struct CounterSnapshot {
    pub owner: RuntimeOwner,
    pub sampled_at: Option<MonoTime>,
    pub state: Option<NodeState>,
    pub counters: NodeCounters,
    pub closed: bool,
}
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum ObservationError {
    WrongOwner,
    ClockRegressed,
    Closed,
    /// Host sink has no bounded capacity; this sample was not retained.
    Overloaded,
}

/// Version 1 synchronous diagnostic seam. Implementations must perform bounded,
/// nonblocking work, retain no unbounded labels/events and never call consensus.
/// The host records *after* polling and keeps the original poll result even if
/// this call refuses. Snapshot exports a fixed Copy value; external I/O belongs
/// to the host outside consensus execution. Close is local to this observer view.
/// Repeated samples count repeatedly. No persistent format or authority token.
pub trait Observer {
    fn record_bounded(&mut self, sample: NodeObservation) -> Result<(), ObservationError>;
    fn snapshot_counters(&self) -> CounterSnapshot;
    fn close(&mut self);
}
