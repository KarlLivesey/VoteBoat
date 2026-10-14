// SPDX-License-Identifier: RPL-1.5
// Copyright (c) 2026 Karl Livesey
//! Opt-in collection of bounded already-ready ordinary original proposals.
use super::*;
mod joined;
pub(crate) use joined::JoinedReady;
pub(crate) struct ReadyProposal {
    pub(crate) admission: AdmissionTicket,
    pub(crate) command: Proposal,
}

struct Window {
    turn: usize,
    count: usize,
    input_bytes: usize,
}
fn routine_control(queued: &Queued, term: u64) -> bool {
    match &queued.event {
        Event::Heartbeat => true,
        Event::Receive(message) => {
            message.term == term && matches!(&message.rpc, Rpc::Appended { .. })
        }
        _ => false,
    }
}
fn window(group: &Group, limits: ShardLimits, now: MonoTime, max_items: usize) -> Option<Window> {
    let (turn, class) = group.next_queued(limits, now)?;
    if class != Class::Data as usize
        // Inspect only: routine replies remain queued and untrusted until the
        // normal control turn validates them. A protected tail still vetoes.
        || !group.queues[0]
            .iter()
            .all(|queued| routine_control(queued, group.core.state().hard_state.term))
        || !group.queues[2].is_empty()
        || group.core.role() != Role::Leader
        || !group.core.local_voter()
        || group.core.leadership_transfer().is_some()
    {
        return None;
    }
    let used = sum(&group.visit.as_ref()?.used);
    let count_limit = limits
        .visit_items
        .saturating_sub(used.items)
        .min(max_items)
        .min(group.core.batch_limits().max_proposal_entries);
    let byte_limit = limits.visit_bytes.saturating_sub(used.bytes);
    let log = group.core.proposal_limits();
    let remaining_entries = log
        .max_entries_per_group
        .saturating_sub(group.core.state().entries.len());
    let mut cost = 0usize;
    let mut payload = 0usize;
    let mut count = 0usize;
    for queued in group.queues[1]
        .iter()
        .take(count_limit.min(remaining_entries))
    {
        let Event::Propose { bytes, .. } = &queued.event else {
            break;
        };
        if queued.timer.is_some()
            || queued.admission.is_none()
            || bytes.len() > log.max_command_bytes
        {
            break;
        }
        let next_cost = cost.checked_add(queued.cost)?;
        let next_payload = payload.checked_add(bytes.capacity())?;
        let next_count = count + 1;
        let input = proposal_input(next_count, next_payload)?;
        let staged =
            input.checked_add(next_count.checked_mul(size_of::<crate::log::LogEntry>())?)?;
        if next_cost > byte_limit
            || staged > log.max_batch_bytes
            || group
                .core
                .state()
                .last_index()
                .checked_add(next_count as u64)
                .is_none()
        {
            break;
        }
        count = next_count;
        cost = next_cost;
        payload = next_payload;
    }
    (count > 1).then(|| Window {
        turn,
        count,
        input_bytes: proposal_input(count, payload).unwrap(),
    })
}
fn proposal_input(count: usize, payload: usize) -> Option<usize> {
    count
        .checked_mul(size_of::<Proposal>())?
        .checked_add(size_of::<Vec<Proposal>>())?
        .checked_add(payload)
}
fn selected_metadata(count: usize) -> Option<usize> {
    let originals = count.checked_mul(size_of::<(Option<AdmissionTicket>, OperationId)>())?;
    let selected = count.checked_mul(size_of::<ReadyProposal>())?;
    let members = count
        .saturating_sub(1)
        .checked_mul(size_of::<SteppedProposal>())?;
    let expanded = count.checked_mul(2)?.checked_mul(size_of::<OwnerStep>())?;
    originals
        .checked_add(selected)?
        .checked_add(members)?
        .checked_add(expanded)?
        .checked_add(4 * size_of::<Vec<SteppedProposal>>())
}
impl<Q: ReadyScheduler> Shard<Q> {
    pub(super) fn next_ready_effect_reservation(
        &mut self,
        ticket: VisitTicket,
        now: MonoTime,
        max_items: usize,
    ) -> Result<Option<usize>, RuntimeError> {
        if max_items == 0 {
            return Ok(None);
        }
        self.observe(now)?;
        let limits = self.limits;
        let ordinary = self.control_profile.is_none();
        let group = self.live(ticket)?;
        if group.core.has_pending_dependency() {
            return Err(RuntimeError::DependencyPending);
        }
        if let Some(window) = ordinary
            .then(|| window(group, limits, now, max_items))
            .flatten()
        {
            return group
                .core
                .effect_reservation(window.input_bytes.max(limits.max_event_bytes))
                .and_then(|bytes| bytes.checked_add(selected_metadata(window.count)?))
                .map(Some)
                .ok_or(RuntimeError::EventTooLarge);
        }
        self.next_effect_reservation(ticket, now)
    }
    pub(super) fn step_ready_checked(
        &mut self,
        ticket: VisitTicket,
        now: MonoTime,
        max_items: usize,
        mut check: impl FnMut(&Raft, &Event, &[ReadyProposal]) -> Result<(), RaftError>,
    ) -> Result<Option<Stepped>, RuntimeError> {
        if max_items == 0 {
            return Ok(None);
        }
        self.observe(now)?;
        self.reserved_connection_peers()?;
        let limits = self.limits;
        let ordinary = self.control_profile.is_none();
        let group = self.live(ticket)?;
        if group.core.has_pending_dependency() {
            return Err(RuntimeError::DependencyPending);
        }
        let Some(window) = ordinary
            .then(|| window(group, limits, now, max_items))
            .flatten()
        else {
            return self.step_next_checked(ticket, now, |core, event| check(core, event, &[]));
        };
        if let Err(error) = check(&group.core, &group.queues[1].front().unwrap().event, &[]) {
            return self.step_next_checked(ticket, now, |_, _| Err(error.clone()));
        }
        let stepped = collect(group, window, &mut check);
        if stepped.result.is_ok() {
            self.retain_connection_history(ticket.group)?;
        }
        Ok(Some(stepped))
    }
}
fn collect(
    group: &mut Group,
    window: Window,
    check: &mut impl FnMut(&Raft, &Event, &[ReadyProposal]) -> Result<(), RaftError>,
) -> Stepped {
    let mut selected = Vec::with_capacity(window.count);
    for offset in 0..window.count {
        let candidate = group.queues[1].front().unwrap();
        if offset > 0 && check(&group.core, &candidate.event, &selected).is_err() {
            break;
        }
        let queued = group.queues[1].pop_front().unwrap();
        group.visit.as_mut().unwrap().used[1].add(queued.cost);
        let Event::Propose { operation, bytes } = queued.event else {
            unreachable!()
        };
        selected.push(ReadyProposal {
            admission: queued.admission.unwrap(),
            command: Proposal { operation, bytes },
        });
    }
    let mut proposals = Vec::with_capacity(selected.len());
    let mut originals = Vec::with_capacity(selected.len());
    for selected in selected {
        originals.push((Some(selected.admission), selected.command.operation));
        proposals.push(selected.command);
    }
    group.turn = (window.turn + 1) % 4;
    let result = if proposals.len() == 1 {
        let proposal = proposals.pop().unwrap();
        group.core.step(Event::Propose {
            operation: proposal.operation,
            bytes: proposal.bytes,
        })
    } else {
        // The candidate window pre-reserved at least this successful prefix.
        group.core.propose_batch(proposals)
    };
    stepped(originals, result)
}
fn stepped(
    originals: Vec<(Option<AdmissionTicket>, OperationId)>,
    result: Result<Vec<Effect>, RaftError>,
) -> Stepped {
    let mut members = originals
        .into_iter()
        .enumerate()
        .map(|(offset, (admission, operation))| SteppedProposal {
            admission,
            operation,
            proposed: position(&result, offset, operation),
            error: result.as_ref().err().cloned(),
        });
    let first = members.next().unwrap();
    Stepped {
        admission: first.admission,
        operation: Some(first.operation),
        proposed: first.proposed,
        read: None,
        received_from: None,
        timer: None,
        batch_members: members.collect(),
        result,
    }
}
fn position(
    result: &Result<Vec<Effect>, RaftError>,
    offset: usize,
    operation: OperationId,
) -> Option<ProposalPosition> {
    result.as_ref().ok()?.iter().find_map(|effect| {
        let Effect::Persist(update) = effect else { return None; };
        let entry = update.suffix.as_ref()?.entries.get(offset)?;
        matches!(entry.payload, crate::log::EntryPayload::Command { operation: id, .. } if id == operation)
            .then_some(ProposalPosition { index: entry.index, term: entry.term })
    })
}

#[cfg(test)]
mod tests;
