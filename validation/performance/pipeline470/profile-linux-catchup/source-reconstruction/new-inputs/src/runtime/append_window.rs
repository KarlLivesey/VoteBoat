// SPDX-License-Identifier: RPL-1.5
// Copyright (c) 2026 Karl Livesey
//! Scoped parked input ownership for the selected append window.
use super::*;
use crate::worker::PersistUnitId;

impl<Q: ReadyScheduler> Shard<Q> {
    pub(super) fn seal_append_windows(&mut self) {
        for (id, group) in &mut self.groups {
            group.core.seal_append_window();
            if group.visit.is_none() && !group.parked.is_empty() {
                self.scheduler.cancel(*id);
            }
        }
    }
    pub(crate) fn with_persist_unit<R>(
        &mut self,
        id: PersistUnitId,
        f: impl FnOnce(&mut Raft) -> R,
    ) -> Result<R, RuntimeError> {
        if id.append.is_none() {
            return self.with_core(id.visit(), f);
        }
        let visit = id.visit();
        if visit.owner != self.owner || id.effect.sequence == 0 {
            return Err(RuntimeError::StaleTicket);
        }
        let group = self
            .groups
            .get_mut(&visit.group)
            .ok_or(RuntimeError::StaleTicket)?;
        let original = group
            .visit
            .as_ref()
            .filter(|v| v.ticket == visit)
            .or_else(|| group.parked.get(&visit.sequence));
        if original.is_none_or(|v| v.ticket != visit || v.append != id.append) {
            return Err(RuntimeError::StaleTicket);
        }
        group.connection_validation_pending = true;
        let result = f(&mut group.core);
        self.validate_after_core_callback(visit.group)?;
        Ok(result)
    }

    pub(super) fn park_append_visit(&mut self, visit: VisitTicket) -> Result<(), RuntimeError> {
        let group = self.live(visit)?;
        if group.visit.as_ref().unwrap().append.is_none()
            || group.parked.contains_key(&visit.sequence)
        {
            return Err(RuntimeError::StaleTicket);
        }
        let original = group.visit.take().unwrap();
        group.parked.insert(visit.sequence, original);
        if !group.queues[0].is_empty() || !group.queues[2].is_empty() {
            group.core.seal_append_window();
        }
        if group.ready() && group.core.can_stage_append_window() {
            self.scheduler.enqueue(visit.group)?;
        }
        Ok(())
    }

    pub(super) fn activate_append_anchor(
        &mut self,
        visit: VisitTicket,
    ) -> Result<(), RuntimeError> {
        if visit.owner != self.owner {
            return Err(RuntimeError::StaleTicket);
        }
        let group = self
            .groups
            .get_mut(&visit.group)
            .ok_or(RuntimeError::StaleTicket)?;
        if group.visit.is_some() {
            return Err(RuntimeError::DependencyPending);
        }
        let mut original = group
            .parked
            .remove(&visit.sequence)
            .ok_or(RuntimeError::StaleTicket)?;
        if original.ticket != visit {
            return Err(RuntimeError::StaleTicket);
        }
        original.append = None;
        group.visit = Some(original);
        self.scheduler.cancel(visit.group);
        Ok(())
    }

    pub(super) fn finish_append_visits(&mut self, anchor: VisitTicket) -> Result<(), RuntimeError> {
        let group = self.live(anchor)?;
        if group.core.has_pending_dependency() {
            return Err(RuntimeError::DependencyPending);
        }
        let mut retired = std::mem::take(&mut group.parked);
        retired.insert(anchor.sequence, group.visit.take().unwrap());
        let mut total = [Usage::default(); 3];
        for visit in retired.into_values() {
            for (i, used) in visit.used.into_iter().enumerate() {
                group.usage[i].sub(used);
                total[i].items += used.items;
                total[i].bytes += used.bytes;
            }
        }
        let ready = group.ready();
        for (i, used) in total.into_iter().enumerate() {
            self.usage[i].sub(used);
        }
        if ready {
            self.scheduler.enqueue(anchor.group)?;
        }
        Ok(())
    }
    pub(super) fn cancel_append_candidate(
        &mut self,
        visit: VisitTicket,
    ) -> Result<(), RuntimeError> {
        let group = self.live(visit)?;
        if group
            .visit
            .as_ref()
            .unwrap()
            .used
            .iter()
            .any(|u| u.items != 0 || u.bytes != 0)
        {
            return Err(RuntimeError::StaleTicket);
        }
        group.visit = None;
        group.core.seal_append_window();
        self.scheduler.cancel(visit.group);
        Ok(())
    }

    pub(super) fn step_append_checked(
        &mut self,
        visit: VisitTicket,
        now: MonoTime,
        max_items: usize,
        mut check: impl FnMut(&Raft, &Event, &[ReadyProposal]) -> Result<(), RaftError>,
    ) -> Result<Option<(Stepped, AppendUnitId)>, RuntimeError> {
        self.observe(now)?;
        let limits = self.limits;
        let group = self.live(visit)?;
        let current = group.visit.as_ref().unwrap();
        if now >= current.deadline
            || !group.queues[0].is_empty()
            || !group.queues[2].is_empty()
            || !group.core.can_stage_append_window()
        {
            group.core.seal_append_window();
            return Ok(None);
        }
        let cap = max_items
            .min(limits.visit_items)
            .min(group.core.batch_limits().max_proposal_entries);
        let staging_limit = group
            .core
            .append_window_limits()
            .unwrap()
            .max_retained_bytes;
        let metadata = cap
            .checked_mul(
                size_of::<ReadyProposal>()
                    + size_of::<Proposal>()
                    + size_of::<(Option<AdmissionTicket>, OperationId)>(),
            )
            .ok_or(RuntimeError::EventTooLarge)?;
        if metadata > staging_limit {
            group.core.seal_append_window();
            return Ok(None);
        }
        let mut selected = Vec::with_capacity(cap);
        let mut cost = 0usize;
        let mut payload = 0usize;
        for queued in group.queues[1].iter().take(cap) {
            let Event::Propose { operation, bytes } = &queued.event else {
                break;
            };
            let Some(admission) = queued.admission else {
                break;
            };
            if queued.timer.is_some()
                || queued.cost > limits.visit_bytes.saturating_sub(cost)
                || check(&group.core, &queued.event, &selected).is_err()
            {
                break;
            }
            let Some(next_payload) = payload.checked_add(bytes.len()) else {
                break;
            };
            if next_payload > staging_limit - metadata {
                break;
            }
            cost += queued.cost;
            payload = next_payload;
            selected.push(ReadyProposal {
                admission,
                command: Proposal {
                    operation: *operation,
                    bytes: bytes.clone(),
                },
            });
        }
        if selected.is_empty() {
            group.core.seal_append_window();
            return Ok(None);
        }
        let originals: Vec<_> = selected
            .iter()
            .map(|p| (Some(p.admission), p.command.operation))
            .collect();
        let mut proposals = Vec::with_capacity(selected.len());
        proposals.extend(selected.into_iter().map(|p| p.command));
        let staged = match group.core.stage_append_window(proposals) {
            Ok(v) => v,
            Err(_) => {
                group.core.seal_append_window();
                return Ok(None);
            }
        };
        // No control/background input can be present on this path. Match the
        // ordinary ready collector's next cursor after its selected Data turn.
        group.turn = 3;
        for _ in 0..originals.len() {
            let queued = group.queues[1].pop_front().unwrap();
            group.visit.as_mut().unwrap().used[1].add(queued.cost);
        }
        group.visit.as_mut().unwrap().append = Some(staged.id);
        let result = Ok(vec![Effect::Persist(staged.update)]);
        Ok(Some((
            super::ready_batch::stepped(originals, result),
            staged.id,
        )))
    }
}
