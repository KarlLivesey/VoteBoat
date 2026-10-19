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
//! U2 probe (lifetime-bounds programme): apply must never return `Err` for a
//! capacity reason on a committed entry, because a committed entry replays
//! identically after restart and the error would become a permanent crash
//! loop. The probe runs the fixed partition-across-leader-change history and
//! 1,000 seeded schedules against the Counter, and shows that it detects the
//! defect in a deliberately broken application.
#[path = "support/apply_probe.rs"]
mod apply_probe;
mod support;
use apply_probe::*;
use std::collections::BTreeSet;
use voteboat::{
    application::*,
    identity::OperationId,
    log::{EntryPayload, LogEntry},
    retention::SessionLimits,
};

const WINDOW: u64 = 4;

fn counter_setup() -> ProbeSetup<Counter> {
    // Two sessions so registrations evict; the built-in session is filled to
    // one below its window.
    let limits = SessionLimits::new(2, WINDOW as u32, 2).unwrap();
    ProbeSetup {
        make: Box::new(move || Counter::with_limits(limits).unwrap()),
        fill: (1..WINDOW)
            .map(|sequence| {
                (
                    Counter::operation(1, sequence).unwrap(),
                    Counter::command(0, 1),
                )
            })
            .collect(),
        novel: Box::new(|n| {
            if n % 4 == 3 {
                return (OperationId::new(u128::from(n) + 1).unwrap(), Vec::new());
            }
            // Acknowledgements lag by up to five, so some commands exceed the
            // window, some retire earlier receipts and some fit exactly.
            let sequence = WINDOW + n;
            let ack = (sequence - 1).saturating_sub(n % 6);
            (
                Counter::operation(1, sequence).unwrap(),
                Counter::command(ack, 1),
            )
        }),
        checkpoint_bytes: 1 << 16,
    }
}

#[test]
fn counter_apply_never_errs_across_a_partitioned_leader_change() {
    let mut probe = Probe::new(counter_setup());
    probe.partition_across_leader_change();
    assert_eq!(probe.failures, []);
    assert!(probe.stats.leader_terms.len() >= 2);
    assert!(probe.stats.admitted > WINDOW);
}

#[test]
fn counter_apply_never_errs_in_one_thousand_seeded_schedules() {
    let mut totals = ProbeStats::default();
    let mut terms = 0;
    for seed in 1..=1000u64 {
        let mut probe = Probe::new(counter_setup());
        probe.partition_across_leader_change();
        probe.schedule(seed.wrapping_mul(0x9e37_79b9_7f4a_7c15), 200);
        assert_eq!(probe.failures, [], "seed {seed}");
        let s = &probe.stats;
        totals.proposals += s.proposals;
        totals.admitted += s.admitted;
        totals.refused += s.refused;
        totals.applied_entries += s.applied_entries;
        totals.restarts += s.restarts;
        totals.checkpoints += s.checkpoints;
        terms += s.leader_terms.len() as u64;
    }
    eprintln!("{totals:?} leader_terms={terms}");
    // The schedules are not vacuous: they change leaders, refuse and admit
    // proposals, restart from checkpoints and apply many entries.
    assert!(terms >= 3000);
    assert!(totals.refused > 0 && totals.admitted > totals.refused);
    assert!(totals.restarts > 1000 && totals.checkpoints > 1000);
    assert!(totals.applied_entries > 50_000);
}

#[test]
fn seeded_schedules_are_deterministic() {
    let run = || {
        let mut probe = Probe::new(counter_setup());
        probe.partition_across_leader_change();
        probe.schedule(42, 200);
        probe.stats
    };
    assert_eq!(run(), run());
}

/// A lifetime table whose admission ignores pending commands while apply
/// enforces the capacity with an error: the defect class the probe exists to
/// find. It must be caught.
#[derive(Clone)]
struct Mutant {
    applied: u64,
    capacity: usize,
    ids: BTreeSet<OperationId>,
}
impl StateMachine for Mutant {
    type Receipt = CounterReceipt;
    fn applied_index(&self) -> u64 {
        self.applied
    }
    fn apply_batch(
        &mut self,
        entries: &[LogEntry],
    ) -> Result<Vec<CounterReceipt>, ApplicationError> {
        let mut next = self.clone();
        let mut receipts = Vec::new();
        for entry in entries {
            if let EntryPayload::Command { operation, .. } = &entry.payload {
                let duplicate = !next.ids.insert(*operation);
                if next.ids.len() > next.capacity {
                    return Err(ApplicationError::DedupCapacity);
                }
                receipts.push(CounterReceipt {
                    index: entry.index,
                    operation: *operation,
                    outcome: CounterOutcome::Value(next.ids.len() as i64),
                    duplicate,
                });
            }
            next.applied = entry.index;
        }
        *self = next;
        Ok(receipts)
    }
}
impl BoundedStateMachine for Mutant {
    fn receipt_bytes_bound(&self, entries: &[LogEntry]) -> Result<usize, ApplicationError> {
        Ok(entries.len() * size_of::<CounterReceipt>())
    }
}
impl ProposalAdmission for Mutant {
    fn validate_proposal<'a>(
        &self,
        operation: OperationId,
        _: &[u8],
        _pending: impl Iterator<Item = (OperationId, &'a [u8])>,
    ) -> Result<usize, ApplicationError> {
        if self.ids.len() < self.capacity || self.ids.contains(&operation) {
            Ok(size_of::<CounterReceipt>())
        } else {
            Err(ApplicationError::DedupCapacity)
        }
    }
}
impl CheckpointStateMachine for Mutant {
    fn schema_version(&self) -> u64 {
        1
    }
    fn checkpoint(&self, _: usize) -> Result<Vec<u8>, ApplicationError> {
        let mut bytes = self.applied.to_le_bytes().to_vec();
        for id in &self.ids {
            bytes.extend(id.get().to_le_bytes());
        }
        Ok(bytes)
    }
    fn restore_checkpoint(
        &mut self,
        _: u64,
        applied: u64,
        bytes: &[u8],
    ) -> Result<(), ApplicationError> {
        self.applied = applied;
        self.ids = bytes[8..]
            .chunks(16)
            .map(|c| OperationId::new(u128::from_le_bytes(c.try_into().unwrap())).unwrap())
            .collect();
        Ok(())
    }
}

#[test]
fn the_probe_detects_a_capacity_error_on_a_committed_entry() {
    let mut probe = Probe::new(ProbeSetup {
        make: Box::new(|| Mutant {
            applied: 0,
            capacity: 3,
            ids: BTreeSet::new(),
        }),
        fill: (1..3u128)
            .map(|id| (OperationId::new(id).unwrap(), vec![1]))
            .collect(),
        novel: Box::new(|n| (OperationId::new(100 + u128::from(n)).unwrap(), vec![1])),
        checkpoint_bytes: 1 << 16,
    });
    probe.partition_across_leader_change();
    assert!(
        probe
            .failures
            .iter()
            .any(|f| f.error == ApplicationError::DedupCapacity),
        "{:?}",
        probe.failures
    );
}
