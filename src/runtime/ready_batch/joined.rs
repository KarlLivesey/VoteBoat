// SPDX-License-Identifier: RPL-1.5
// Copyright (c) 2026 Karl Livesey
//! A non-consuming, ordered intake; failure leaves the ordinary head intact.
use super::*;

pub(crate) struct JoinedControl {
    pub(crate) admission: Option<AdmissionTicket>,
    pub(crate) received_from: NodeId,
}
pub(crate) struct JoinedReady {
    pub(crate) controls: Vec<JoinedControl>,
    pub(crate) proposals: Stepped,
}
struct Selection {
    replies: usize,
    proposals: usize,
    input_bytes: usize,
}
struct Cursor {
    turn: usize,
    offsets: [usize; 3],
    used: Usage,
}
impl Cursor {
    fn next(&self, group: &Group, limits: ShardLimits) -> Option<(usize, usize)> {
        if self.used.items >= limits.visit_items {
            return None;
        }
        const ORDER: [usize; 4] = [0, 0, 1, 2];
        (0..4)
            .map(|offset| (self.turn + offset, ORDER[(self.turn + offset) % 4]))
            .find(|(_, class)| {
                group.queues[*class]
                    .get(self.offsets[*class])
                    .is_some_and(|q| q.cost <= limits.visit_bytes.saturating_sub(self.used.bytes))
            })
    }
    fn consume(&mut self, group: &Group, turn: usize, class: usize) {
        self.used.add(group.queues[class][self.offsets[class]].cost);
        self.offsets[class] += 1;
        self.turn = (turn + 1) % 4;
    }
}
fn selection(group: &Group, limits: ShardLimits, now: MonoTime, max: usize) -> Option<Selection> {
    let visit = group.visit.as_ref()?;
    if now >= visit.deadline
        || group.core.has_pending_dependency()
        || group.core.role() != Role::Leader
        || !group.core.local_voter()
        || group.core.leadership_transfer().is_some()
        || !group.queues[2].is_empty()
        || !group.queues[0]
            .iter()
            .all(|q| routine_control(q, group.core.state().hard_state.term))
    {
        return None;
    }
    let mut cursor = Cursor {
        turn: group.turn,
        offsets: [0; 3],
        used: sum(&visit.used),
    };
    let replies = reply_prefix(group, limits, &mut cursor, max)?;
    let log = group.core.proposal_limits();
    let count_limit = max
        .saturating_sub(replies)
        .min(group.core.batch_limits().max_proposal_entries)
        .min(
            log.max_entries_per_group
                .saturating_sub(group.core.state().entries.len()),
        );
    let mut payload = 0usize;
    let mut proposals = 0;
    while proposals < count_limit {
        let Some((turn, class)) = cursor.next(group, limits) else {
            break;
        };
        if class != 1 {
            break;
        }
        let queued = &group.queues[1][proposals];
        let Event::Propose { bytes, .. } = &queued.event else {
            break;
        };
        if queued.timer.is_some()
            || queued.admission.is_none()
            || bytes.len() > log.max_command_bytes
        {
            break;
        }
        let next_payload = payload.checked_add(bytes.capacity())?;
        let next_count = proposals + 1;
        let staged = proposal_input(next_count, next_payload)?
            .checked_add(next_count.checked_mul(size_of::<crate::log::LogEntry>())?)?;
        if staged > log.max_batch_bytes
            || group
                .core
                .state()
                .last_index()
                .checked_add(next_count as u64)
                .is_none()
        {
            break;
        }
        payload = next_payload;
        proposals = next_count;
        cursor.consume(group, turn, class);
    }
    if proposals == 0 {
        return None;
    }
    Some(Selection {
        replies,
        proposals,
        input_bytes: proposal_input(proposals, payload).unwrap().checked_mul(2)?,
    })
}
fn reply_prefix(
    group: &Group,
    limits: ShardLimits,
    cursor: &mut Cursor,
    max: usize,
) -> Option<usize> {
    let reply_limit = group.core.batch_limits().max_ready_reply_inputs;
    let mut replies = 0;
    while replies < max {
        let (turn, class) = cursor.next(group, limits)?;
        if class != 0 {
            break;
        }
        let queued = &group.queues[0][replies];
        if replies >= reply_limit
            || queued.timer.is_some()
            || !matches!(
                &queued.event,
                Event::Receive(Message {
                    rpc: Rpc::Appended { success: true, .. },
                    ..
                })
            )
        {
            return None;
        }
        cursor.consume(group, turn, class);
        replies += 1;
    }
    (replies > 0).then_some(replies)
}
fn reservation(group: &Group, selected: &Selection, limits: ShardLimits) -> Option<usize> {
    let controls = selected.replies.checked_mul(size_of::<Message>())?;
    // A cloned quorum frontier BTreeMap plus its validated policy walk. Charge
    // one conservative node allocation per replica (including node overhead).
    let planning = group
        .core
        .membership()
        .replicas()
        .count()
        .checked_mul(32 * (size_of::<NodeId>() + size_of::<u64>() + size_of::<usize>()))?;
    let metadata = selected_metadata(selected.proposals)?
        .checked_add(
            selected
                .replies
                .checked_mul(size_of::<JoinedControl>() + 2 * size_of::<OwnerStep>())?,
        )?
        .checked_add((selected.replies + selected.proposals).checked_mul(size_of::<NodeId>())?)?
        .checked_add(size_of::<JoinedReady>() + 3 * size_of::<Vec<Message>>())?;
    group
        .core
        .effect_reservation(
            selected
                .input_bytes
                .checked_add(controls)?
                .max(limits.max_event_bytes),
        )?
        .checked_add(metadata)?
        .checked_add(planning)
}
impl<Q: ReadyScheduler> Shard<Q> {
    pub(in crate::runtime) fn joined_reservation(
        &mut self,
        ticket: VisitTicket,
        now: MonoTime,
        max: usize,
    ) -> Result<Option<(usize, usize)>, RuntimeError> {
        self.observe(now)?;
        let limits = self.limits;
        if self.control_profile.is_some() {
            return Ok(None);
        }
        let group = self.live(ticket)?;
        Ok(selection(group, limits, now, max).and_then(|s| {
            reservation(group, &s, limits).map(|bytes| (bytes, s.replies + s.proposals))
        }))
    }
    pub(in crate::runtime) fn step_joined_checked(
        &mut self,
        ticket: VisitTicket,
        now: MonoTime,
        max: usize,
        mut check: impl FnMut(&Raft, &Event, &[ReadyProposal]) -> Result<(), RaftError>,
    ) -> Result<Option<JoinedReady>, RuntimeError> {
        self.observe(now)?;
        self.reserved_connection_peers()?;
        let limits = self.limits;
        if self.control_profile.is_some() {
            return Ok(None);
        }
        let group = self.live(ticket)?;
        let Some(selected) = selection(group, limits, now, max) else {
            return Ok(None);
        };
        // This producer accepts only an existing plain stable roster and ordinary
        // commands: its connection union cannot grow. Registration/ordinary
        // transitions already retained that roster; no fallible post-plan walk.
        Ok(prepare(group, &selected, limits, now, &mut check))
    }
}
fn prepare(
    group: &mut Group,
    selected: &Selection,
    limits: ShardLimits,
    now: MonoTime,
    check: &mut impl FnMut(&Raft, &Event, &[ReadyProposal]) -> Result<(), RaftError>,
) -> Option<JoinedReady> {
    let mut messages = Vec::new();
    let mut controls = Vec::new();
    let mut members = Vec::new();
    messages.try_reserve_exact(selected.replies).ok()?;
    controls.try_reserve_exact(selected.replies).ok()?;
    members
        .try_reserve_exact(selected.proposals.saturating_sub(1))
        .ok()?;
    for queued in group.queues[0].iter().take(selected.replies) {
        let Event::Receive(message) = &queued.event else {
            unreachable!()
        };
        messages.push(message.clone());
        controls.push(JoinedControl {
            admission: queued.admission,
            received_from: message.from,
        });
    }
    let prefix = copy_prefix(group, selected.proposals, check)?;
    let proposals = copy_proposals(&prefix)?;
    let count = proposals.len();
    let result = Ok(group
        .core
        .propose_after_ready_replies(&messages, proposals)
        .ok()?);
    // Core succeeded. No fallible allocation/check follows before all originals
    // and controls move from their queues to this one owned visit.
    for class in std::iter::repeat_n(0, selected.replies).chain(std::iter::repeat_n(1, count)) {
        let (turn, actual_class) = group.next_queued(limits, now).unwrap();
        debug_assert_eq!(class, actual_class);
        let queued = group.queues[class].pop_front().unwrap();
        group.visit.as_mut().unwrap().used[class].add(queued.cost);
        group.turn = (turn + 1) % 4;
    }
    for (offset, original) in prefix.iter().enumerate().skip(1) {
        members.push(SteppedProposal {
            admission: Some(original.admission),
            operation: original.command.operation,
            proposed: position(&result, offset, original.command.operation),
            error: None,
        });
    }
    let first = &prefix[0];
    Some(JoinedReady {
        controls,
        proposals: Stepped {
            admission: Some(first.admission),
            operation: Some(first.command.operation),
            proposed: position(&result, 0, first.command.operation),
            read: None,
            received_from: None,
            timer: None,
            batch_members: members,
            result,
        },
    })
}
fn copy_prefix(
    group: &Group,
    count: usize,
    check: &mut impl FnMut(&Raft, &Event, &[ReadyProposal]) -> Result<(), RaftError>,
) -> Option<Vec<ReadyProposal>> {
    let mut prefix = Vec::new();
    prefix.try_reserve_exact(count).ok()?;
    for queued in group.queues[1].iter().take(count) {
        if check(&group.core, &queued.event, &prefix).is_err() {
            break;
        }
        let Event::Propose { operation, bytes } = &queued.event else {
            unreachable!()
        };
        let mut copied = Vec::new();
        copied.try_reserve_exact(bytes.len()).ok()?;
        copied.extend_from_slice(bytes);
        prefix.push(ReadyProposal {
            admission: queued.admission.unwrap(),
            command: Proposal {
                operation: *operation,
                bytes: copied,
            },
        });
    }
    if prefix.is_empty() {
        return None;
    }
    Some(prefix)
}
fn copy_proposals(prefix: &[ReadyProposal]) -> Option<Vec<Proposal>> {
    let mut proposals = Vec::new();
    proposals.try_reserve_exact(prefix.len()).ok()?;
    for entry in prefix {
        let mut copied = Vec::new();
        copied.try_reserve_exact(entry.command.bytes.len()).ok()?;
        copied.extend_from_slice(&entry.command.bytes);
        proposals.push(Proposal {
            operation: entry.command.operation,
            bytes: copied,
        });
    }
    Some(proposals)
}

#[cfg(test)]
mod tests;
