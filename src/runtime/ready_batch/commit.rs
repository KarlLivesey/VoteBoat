// SPDX-License-Identifier: RPL-1.5
// Copyright (c) 2026 Karl Livesey
//! Fuse only an exact, still-local commit with an already-scheduled data prefix.
use super::*;

fn update(step: &Stepped) -> Option<&crate::log::LogUpdate> {
    if step.admission.is_some()
        || step.operation.is_some()
        || step.received_from.is_none()
        || step.timer.is_some()
        || !step.batch_members.is_empty()
    {
        return None;
    }
    let [Effect::Persist(update)] = step.result.as_ref().ok()?.as_slice() else {
        return None;
    };
    Some(update)
}

impl<Q: ReadyScheduler> Shard<Q> {
    pub(in crate::runtime) fn ready_commit_reservation(
        &mut self,
        ticket: VisitTicket,
        now: MonoTime,
        max_items: usize,
        step: &Stepped,
    ) -> Result<Option<usize>, RuntimeError> {
        self.observe(now)?;
        let limits = self.limits;
        let ordinary = self.control_profile.is_none();
        let group = self.live(ticket)?;
        let Some(update) = update(step) else {
            return Ok(None);
        };
        if !ordinary || !group.core.can_combine_ready_commit(update) {
            return Ok(None);
        }
        let Some(window) = window_at_least(group, limits, now, max_items, 1) else {
            return Ok(None);
        };
        // Keep the original effect buffer alongside the replacement and the
        // clone-staged original commands until the entire Core plan succeeds.
        let reserved = group
            .core
            .effect_reservation(window.input_bytes.max(limits.max_event_bytes))
            .and_then(|n| n.checked_add(selected_metadata(window.count)?))
            .and_then(|n| {
                n.checked_add(
                    size_of::<SteppedProposal>()
                        + 2 * size_of::<OwnerStep>()
                        + size_of::<Vec<SteppedProposal>>(),
                )
            })
            .and_then(|n| n.checked_add(window.input_bytes.checked_mul(2)?))
            .and_then(|n| {
                n.checked_add(
                    step.result
                        .as_ref()
                        .ok()?
                        .capacity()
                        .checked_mul(4 * size_of::<Effect>())?,
                )
            });
        Ok(reserved)
    }

    pub(in crate::runtime) fn combine_ready_commit(
        &mut self,
        ticket: VisitTicket,
        now: MonoTime,
        max_items: usize,
        step: &mut Stepped,
        check: &mut impl FnMut(&Raft, &Event, &[ReadyProposal]) -> Result<(), RaftError>,
    ) -> Result<bool, RuntimeError> {
        self.observe(now)?;
        let limits = self.limits;
        let group = self.live(ticket)?;
        let Some(update) = update(step) else {
            return Ok(false);
        };
        if !group.core.can_combine_ready_commit(update) {
            return Ok(false);
        }
        let Some(window) = window_at_least(group, limits, now, max_items, 1) else {
            return Ok(false);
        };
        let mut selected = Vec::with_capacity(window.count);
        for queued in group.queues[1].iter().take(window.count) {
            if check(&group.core, &queued.event, &selected).is_err() {
                break;
            }
            let Event::Propose { operation, bytes } = &queued.event else {
                unreachable!()
            };
            selected.push(ReadyProposal {
                admission: queued.admission.unwrap(),
                command: Proposal {
                    operation: *operation,
                    bytes: bytes.clone(),
                },
            });
        }
        if selected.is_empty() {
            return Ok(false);
        }
        let proposals = selected.iter().map(|p| p.command.clone()).collect();
        let Ok(effects) = group.core.combine_ready_commit(update, proposals) else {
            return Ok(false);
        };
        let result = Ok(effects);
        let members = selected
            .iter()
            .enumerate()
            .map(|(offset, selected)| SteppedProposal {
                admission: Some(selected.admission),
                operation: selected.command.operation,
                proposed: position(&result, offset, selected.command.operation),
                error: None,
            })
            .collect();
        for _ in &selected {
            let queued = group.queues[1].pop_front().unwrap();
            group.visit.as_mut().unwrap().used[1].add(queued.cost);
        }
        group.turn = (window.turn + 1) % 4;
        step.result = result;
        step.batch_members = members;
        Ok(true)
    }
}
