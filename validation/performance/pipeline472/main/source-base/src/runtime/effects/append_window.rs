// SPDX-License-Identifier: RPL-1.5
// Copyright (c) 2026 Karl Livesey
//! Whole-window output escrow and exact parked continuation ownership.
use super::*;
use crate::log::LogTicket;

// A failed preview leaves the same Core and input queue untouched. Every
// successful check is Ok and collection stops at its first Err, so this cursor
// retains the entire checked prefix without another per-original allocation.
#[derive(Default)]
pub(super) struct CheckReplay {
    accepted: usize,
    refused: Option<RaftError>,
}
impl CheckReplay {
    pub(super) fn record(&mut self, result: &Result<(), RaftError>) {
        match result {
            Ok(()) => self.accepted += 1,
            Err(error) => self.refused = Some(error.clone()),
        }
    }
    pub(super) fn next(&mut self) -> Option<Result<(), RaftError>> {
        if self.accepted != 0 {
            self.accepted -= 1;
            Some(Ok(()))
        } else {
            self.refused.take().map(Err)
        }
    }
}

#[derive(Clone, Copy)]
struct Budget {
    units: usize,
    per_unit: usize,
    total: usize,
}
pub(super) struct Window {
    units: Vec<(VisitTicket, AppendUnitId)>,
    budget: Budget,
    unused: usize,
    retired: bool,
    anchor: Option<VisitTicket>,
}
fn budget(core: &Raft, input: usize) -> Option<Budget> {
    let selected = core.append_window_limits()?;
    let per_unit = core
        .effect_reservation(input)?
        .checked_add(selected.max_retained_bytes)?;
    let frames = selected.max_units.checked_mul(
        4 * (size_of::<Active>()
            + size_of::<super::super::Visit>()
            + size_of::<PersistUnitId>()
            + size_of::<AppendUnitId>()
            + size_of::<WorkerTicket>()
            + size_of::<LogTicket>()
            + size_of::<VisitTicket>()),
    )?;
    let originals = selected.max_originals.checked_mul(
        size_of::<ReadyProposal>()
            + size_of::<SteppedProposal>()
            + 2 * size_of::<OwnerStep>()
            + size_of::<(Option<AdmissionTicket>, OperationId)>(),
    )?;
    let metadata = frames
        .checked_add(originals)?
        .checked_add(8 * size_of::<Vec<OwnerStep>>())?;
    let total = per_unit
        .checked_mul(selected.max_units)?
        .checked_add(metadata)?;
    Some(Budget {
        units: selected.max_units,
        per_unit,
        total,
    })
}
/// Project the protected output envelope for the Core's selected append window.
/// This is a configuration bound, not admission or durability authority.
pub fn append_window_effect_reservation(core: &Raft, input_bytes: usize) -> Option<usize> {
    budget(core, input_bytes).map(|bound| bound.total)
}
impl<Q: ReadyScheduler, T: TimerService, E: ElectionEntropy> EffectOwner<Q, T, E> {
    pub(crate) fn seal_append_windows(&mut self) {
        self.append_closed = true;
        self.runtime.seal_append_windows();
    }
    /// Validate the selected public worker and whole-window protected envelope.
    pub fn validate_append_pipeline(
        &self,
        worker: &impl PersistenceWorker,
    ) -> Result<(), EffectOwnerError> {
        self.validate_append_pipeline_limits(
            worker.binding(),
            worker.limits(),
            worker.max_outstanding_units_per_group(),
        )
    }
    /// Cold configuration check. A declared binding/limit grants no work authority.
    pub fn validate_append_pipeline_limits(
        &self,
        worker: WorkerBinding,
        limits: WorkerLimits,
        capacity: usize,
    ) -> Result<(), EffectOwnerError> {
        if worker != self.worker {
            return Err(EffectOwnerError::WrongWorker);
        }
        let shard = self.runtime.limits();
        let limits = limits.validate().map_err(EffectOwnerError::Worker)?;
        if capacity == 0 || capacity > 64 {
            return Err(EffectOwnerError::InvalidLimits);
        }
        for group in self.groups() {
            let core = self.core(group).unwrap();
            let Some(selected) = core.append_window_limits() else {
                continue;
            };
            let bound =
                budget(core, shard.max_event_bytes).ok_or(EffectOwnerError::ReservationTooLarge)?;
            if self.authorization().is_logical()
                || selected.max_units > capacity
                || selected.max_units > limits.max_requests - limits.control_requests
                || selected.max_units > limits.max_units - limits.control_units
                || selected.max_units > self.limits.active_visits - self.limits.control_visits
                || selected.max_originals > shard.group_items - shard.group_control_items
                || selected.max_originals > shard.max_items - shard.control_items
                || selected
                    .max_retained_bytes
                    .checked_add(
                        selected
                            .max_units
                            .checked_mul(PERSIST_UNIT_METADATA)
                            .ok_or(EffectOwnerError::ReservationTooLarge)?,
                    )
                    .is_none_or(|n| n > limits.max_bytes - limits.control_bytes)
                || PERSIST_UNIT_METADATA > limits.batch_bytes
                || bound.total > self.limits.reserved_bytes - self.limits.control_bytes
            {
                return Err(EffectOwnerError::InvalidLimits);
            }
        }
        Ok(())
    }
    pub(super) fn bulk_reserved(&self) -> usize {
        self.reserved
            - self
                .active
                .values()
                .filter(|a| a.control)
                .map(|a| a.reserved)
                .sum::<usize>()
    }
    pub(super) fn visit_slots(&self) -> usize {
        self.active.len()
            + self
                .windows
                .values()
                .map(|w| w.budget.units - w.units.len())
                .sum::<usize>()
    }
    fn reserve_append_frame(&mut self, visit: VisitTicket) -> Result<bool, EffectOwnerError> {
        if !self.windows.contains_key(&visit.group) {
            let bound = budget(
                self.core(visit.group).unwrap(),
                self.runtime.limits().max_event_bytes,
            )
            .ok_or(EffectOwnerError::ReservationTooLarge)?;
            let bulk_slots =
                self.visit_slots() - self.active.values().filter(|a| a.control).count();
            if bound.units
                > (self.limits.active_visits - self.limits.control_visits)
                    .saturating_sub(bulk_slots)
                || bound.total
                    > (self.limits.reserved_bytes - self.limits.control_bytes)
                        .saturating_sub(self.bulk_reserved())
            {
                return Ok(false);
            }
            self.reserved += bound.total;
            self.windows.insert(
                visit.group,
                Window {
                    units: Vec::with_capacity(bound.units),
                    budget: bound,
                    unused: bound.total,
                    retired: false,
                    anchor: None,
                },
            );
        }
        let window = self.windows.get_mut(&visit.group).unwrap();
        if window.retired || window.units.len() == window.budget.units {
            return Ok(false);
        }
        window.unused -= window.budget.per_unit;
        self.active.insert(
            visit.sequence,
            Active {
                visit,
                received_from: None,
                reserved: window.budget.per_unit,
                continuation_bytes: window.budget.per_unit,
                effects: VecDeque::new(),
                leased: None,
                leased_bytes: 0,
                kind: None,
                waiting: false,
                ready: false,
                control: false,
                read_captured: false,
                append: None,
            },
        );
        Ok(true)
    }
    pub(super) fn try_append_step(
        &mut self,
        visit: VisitTicket,
        now: MonoTime,
        max_items: usize,
        check: &mut impl FnMut(&Raft, &Event, &[ReadyProposal]) -> Result<(), RaftError>,
        steps: &mut Vec<OwnerStep>,
    ) -> Result<bool, EffectOwnerError> {
        if !self.reserve_append_frame(visit)? {
            if self.windows.contains_key(&visit.group) {
                self.runtime
                    .cancel_append_candidate(visit)
                    .map_err(EffectOwnerError::Runtime)?;
                return Ok(true);
            }
            return Ok(false);
        }
        let staged = self
            .runtime
            .step_append_checked(visit, now, max_items, check)
            .map_err(EffectOwnerError::Runtime)?;
        if let Some((step, id)) = staged {
            self.active.get_mut(&visit.sequence).unwrap().append = Some(id);
            self.windows
                .get_mut(&visit.group)
                .unwrap()
                .units
                .push((visit, id));
            self.install_ready_steps(visit, step, steps)?;
            return Ok(true);
        }
        let frame = self.active.remove(&visit.sequence).unwrap();
        let window = self.windows.get_mut(&visit.group).unwrap();
        window.unused += frame.reserved;
        if window.units.is_empty() {
            let window = self.windows.remove(&visit.group).unwrap();
            self.reserved -= window.budget.total;
            Ok(false)
        } else {
            self.runtime
                .cancel_append_candidate(visit)
                .map_err(EffectOwnerError::Runtime)?;
            Ok(true)
        }
    }
    pub(super) fn retire_append_window(
        &mut self,
        group: GroupIdentity,
        units: Vec<AppendUnitId>,
        effects: Vec<Effect>,
    ) -> Result<(), EffectOwnerError> {
        let window = self
            .windows
            .get_mut(&group)
            .ok_or(EffectOwnerError::StaleCompletion)?;
        if window.retired || window.units.iter().map(|(_, id)| *id).ne(units) {
            return self.fail(EffectOwnerError::ProviderContract);
        }
        let anchor = window.units.last().unwrap().0;
        window.retired = true;
        window.anchor = Some(anchor);
        self.runtime
            .activate_append_anchor(anchor)
            .map_err(EffectOwnerError::Runtime)?;
        self.active.get_mut(&anchor.sequence).unwrap().append = None;
        self.install_continuation(anchor, effects)
    }
    pub(super) fn refresh_append_window(
        &mut self,
        group: GroupIdentity,
    ) -> Result<bool, EffectOwnerError> {
        let Some(window) = self.windows.get(&group) else {
            return Ok(false);
        };
        if !window.retired
            || self.core(group).unwrap().has_pending_dependency()
            || window.units.iter().any(|(v, _)| {
                self.active
                    .get(&v.sequence)
                    .is_some_and(|a| a.leased.is_some() || a.waiting || !a.effects.is_empty())
            })
        {
            return Ok(true);
        }
        let anchor = window.anchor.unwrap();
        self.runtime
            .finish_append_visits(anchor)
            .map_err(EffectOwnerError::Runtime)?;
        let window = self.windows.remove(&group).unwrap();
        let mut released = window.unused;
        for (visit, _) in window.units {
            released += self.active.remove(&visit.sequence).unwrap().reserved;
        }
        self.reserved -= released;
        Ok(true)
    }
    pub(super) fn abandon_append_windows(&mut self) {
        for window in std::mem::take(&mut self.windows).into_values() {
            self.reserved -= window.unused;
        }
    }
}
