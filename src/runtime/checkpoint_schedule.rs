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
//! Bounded, fair automatic checkpoint admission over existing snapshot effects.
use super::*;
use crate::application::StateMachine;

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub struct CheckpointPolicy {
    /// Applied entries beyond the last durable snapshot base, including noops.
    pub min_entries: u64,
    /// Host-clock delay between bounded scans; missed scans coalesce.
    pub interval_ms: u64,
    /// Maximum groups examined per due scan (1..=4096).
    pub scan_groups: usize,
    /// Bounds queued and executing automatic requests together (1..=4096).
    pub max_in_flight: usize,
}
impl CheckpointPolicy {
    /// The default for a store with these limits: checkpoint once half the
    /// retained-entry budget is applied beyond the durable base (the largest
    /// threshold `configure` accepts), leaving the other half for writes that
    /// arrive while a checkpoint runs; WAL occupancy triggers earlier ones.
    /// Measured: a quarter cost 9–17% more native durable CPU per write on
    /// macOS than half (docs/IMPLEMENTATION.md, ST2).
    pub fn for_limits(limits: crate::log::LogLimits) -> Self {
        Self {
            min_entries: (limits.max_entries_per_group as u64 / 2).max(1),
            // A scan only compares counters of at most `scan_groups` groups;
            // scanning often finds a due checkpoint before the limit.
            interval_ms: 10,
            scan_groups: 64,
            max_in_flight: 4,
        }
    }
}
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub struct ScheduledCheckpoint {
    pub admission: AdmissionTicket,
    pub target_index: u64,
    pub executed: bool,
}
#[derive(Clone, Debug, Eq, PartialEq)]
pub enum CheckpointResult {
    /// Observed durable local base, not quorum evidence or a global watermark.
    Completed { group: GroupIdentity, index: u64 },
    Rejected {
        group: GroupIdentity,
        error: RaftError,
    },
}
#[derive(Clone, Debug, Default, Eq, PartialEq)]
pub struct CheckpointStatus {
    pub policy: Option<CheckpointPolicy>,
    pub next_scan: Option<MonoTime>,
    pub cursor: Option<GroupIdentity>,
    pub pending: BTreeMap<GroupIdentity, ScheduledCheckpoint>,
    pub last_result: Option<CheckpointResult>,
    pub last_admission_error: Option<(GroupIdentity, RuntimeError)>,
}
impl CheckpointStatus {
    /// `max_entries` is the smallest retained-entry budget of the node's groups.
    pub(super) fn configure(
        &mut self,
        policy: Option<CheckpointPolicy>,
        now: MonoTime,
        max_entries: usize,
    ) -> Result<(), NodeError> {
        if !self.pending.is_empty() {
            return Err(NodeError::Owner(EffectOwnerError::Runtime(
                RuntimeError::Overloaded,
            )));
        }
        let next = if let Some(p) = policy {
            if p.min_entries == 0
                || p.interval_ms == 0
                || p.scan_groups == 0
                || p.scan_groups > 4096
                || p.max_in_flight == 0
                || p.max_in_flight > 4096
            {
                return Err(NodeError::InvalidLimits);
            }
            if p.min_entries > (max_entries / 2).max(1) as u64 {
                return Err(NodeError::Maintenance(
                    MaintenanceError::CheckpointThresholdUnreachable {
                        min_entries: p.min_entries,
                        max_entries_per_group: max_entries,
                    },
                ));
            }
            Some(MonoTime(
                now.0
                    .checked_add(p.interval_ms)
                    .ok_or(NodeError::InvalidLimits)?,
            ))
        } else {
            None
        };
        self.policy = policy;
        self.next_scan = next;
        self.cursor = None;
        self.last_admission_error = None;
        Ok(())
    }
    pub(super) fn observe<S: ReadyScheduler, T: TimerService, E: ElectionEntropy>(
        &mut self,
        owner: &EffectOwner<S, T, E>,
        steps: &[OwnerStep],
    ) {
        if self.pending.is_empty() {
            return;
        }
        for step in steps {
            let Some(admission) = step.admission else {
                continue;
            };
            let Some(pending) = self.pending.get_mut(&admission.group) else {
                continue;
            };
            if pending.admission != admission {
                continue;
            }
            if let Some(error) = &step.error {
                self.pending.remove(&admission.group);
                self.last_result = Some(CheckpointResult::Rejected {
                    group: admission.group,
                    error: error.clone(),
                });
            } else {
                pending.executed = true;
            }
        }
        self.pending.retain(|&group, pending| {
            let base = owner
                .core(group)
                .map_or(0, |core| core.state().base_index());
            if pending.executed && base >= pending.target_index {
                self.last_result = Some(CheckpointResult::Completed { group, index: base });
                false
            } else {
                true
            }
        });
    }
    /// Under WAL `pressure` a scan runs whenever no automatic checkpoint is
    /// pending and checkpoints every group with any applied entry beyond its
    /// base: large commands can fill the WAL long before `min_entries`.
    pub(super) fn schedule<
        S: ReadyScheduler,
        T: TimerService,
        E: ElectionEntropy,
        A: StateMachine,
    >(
        &mut self,
        owner: &mut EffectOwner<S, T, E>,
        applications: &BTreeMap<GroupIdentity, A>,
        now: MonoTime,
        pressure: bool,
    ) {
        let Some(policy) = self.policy else {
            return;
        };
        let regular = self.next_scan.is_some_and(|d| now >= d);
        if !regular && !(pressure && self.pending.is_empty()) {
            return;
        }
        if regular {
            self.next_scan = now.0.checked_add(policy.interval_ms).map(MonoTime);
        }
        let min_entries = if pressure { 1 } else { policy.min_entries };
        let mut first = None;
        for _ in 0..policy.scan_groups {
            if self.pending.len() >= policy.max_in_flight {
                break;
            }
            let next = owner
                .groups_after(self.cursor)
                .next()
                .or_else(|| owner.groups().next());
            let Some(group) = next else {
                break;
            };
            if first == Some(group) {
                break;
            }
            first.get_or_insert(group);
            self.cursor = Some(group);
            if self.pending.contains_key(&group) {
                continue;
            }
            let core = owner.core(group).unwrap();
            let Some(app) = applications.get(&group) else {
                continue;
            };
            let applied = app.applied_index();
            if core.has_pending_dependency()
                || applied > core.state().commit_index
                || applied.saturating_sub(core.state().base_index()) < min_entries
            {
                continue;
            }
            match owner.admit_tracked(group, Event::Checkpoint) {
                Ok(admission) => {
                    self.pending.insert(
                        group,
                        ScheduledCheckpoint {
                            admission,
                            target_index: applied,
                            executed: false,
                        },
                    );
                }
                Err(rejected) => {
                    self.last_admission_error = Some((group, rejected.reason));
                }
            }
        }
    }
}
