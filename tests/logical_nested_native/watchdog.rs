// SPDX-License-Identifier: RPL-1.5
// Copyright (c) 2026 Karl Livesey
//! Test stall detection only: original opaque outputs remain business authority.
use super::host::Host;
#[path = "watchdog/read_result.rs"]
mod read_result;
#[path = "watchdog/readiness.rs"]
mod readiness;
use std::time::{Duration, Instant};
use voteboat::{application::StateMachine, log::EntryPayload, runtime::*};
#[derive(Debug)]
pub struct Watchdog {
    start: Instant,
    last: Instant,
    seen: u64,
}
#[derive(Debug)]
pub struct ClientAttempt {
    pub node: usize,
    pub ticket: ClientTicket,
    pub before: u64,
    pub position: Option<ProposalPosition>,
    pub observing: bool,
    pub owners: [RuntimeOwner; 3],
    pub configurations: [voteboat::identity::ConfigurationId; 3],
    readiness: Option<readiness::Readiness>,
}
impl ClientAttempt {
    pub fn new(
        host: &Host<'_, '_>,
        node: usize,
        ticket: ClientTicket,
        before: u64,
        resumed: bool,
    ) -> Self {
        let readiness = readiness::capture(host, node, ticket, before, resumed);
        Self {
            node,
            ticket,
            before,
            position: None,
            observing: true,
            owners: std::array::from_fn(|i| host.nodes[i].local().owner.identity()),
            configurations: std::array::from_fn(|i| {
                host.nodes[i]
                    .local()
                    .owner
                    .core(ticket.group)
                    .unwrap()
                    .membership()
                    .id()
            }),
            readiness,
        }
    }
}
impl Watchdog {
    pub fn new() -> Self {
        let start = Instant::now();
        Self {
            start,
            last: start,
            seen: 0,
        }
    }
    fn observe(&mut self, mask: u64, now: MonoTime) {
        let added = mask & !self.seen;
        if added != 0 {
            self.seen |= added;
            self.last = Instant::now();
            eprintln!(
                "nested original progress now={now:?} added={added:#x} seen={:#x}",
                self.seen
            );
        }
    }
    pub fn valid(&self) -> bool {
        self.start.elapsed() < Duration::from_secs(90)
            && self.last.elapsed() < Duration::from_secs(30)
    }
    /// Functional writes retain their original total bound; latency is P9 work.
    pub(super) fn within_absolute_bound(&self) -> bool {
        self.start.elapsed() < Duration::from_secs(90)
    }
    pub fn client(
        &mut self,
        h: &Host<'_, '_>,
        attempt: &mut ClientAttempt,
        bytes: &[u8],
        ordinal: usize,
    ) {
        let ticket = attempt.ticket;
        assert!(ordinal < super::requests::ATTEMPTS);
        if let Some(readiness) = &attempt.readiness {
            assert!(ordinal > 0);
            self.observe(
                u64::from(readiness.mask(h, attempt.node)) << (40 + (ordinal - 1) * 3),
                h.lower.now(),
            );
        }
        if attempt.observing && attempt.position.is_none() {
            if let Some(step) = &h.last_proposal[attempt.node] {
                if step.visit.owner == ticket.binding.owner
                    && step.visit.group == ticket.group
                    && step.operation == Some(ticket.operation)
                {
                    attempt.position = step.proposed.filter(|p| p.index > attempt.before);
                }
            }
        }
        let Some(position) = attempt.position else {
            return;
        };
        let mask = original_prefix(h, attempt, bytes, position);
        let shifted = mask << (ordinal * 10);
        if shifted & !self.seen != 0 {
            eprintln!("nested original per-owner prefix now={:?} ticket={ticket:?} position={position:?} mask={mask:#x}; top={:?}", h.lower.now(), super::diagnostic::top(h));
        }
        self.observe(shifted, h.lower.now());
    }
    pub fn applied(&mut self, now: MonoTime, ordinal: usize) {
        assert!(ordinal < super::requests::ATTEMPTS);
        self.observe(1 << (49 + ordinal), now);
    }
    pub fn read(
        &mut self,
        h: &Host<'_, '_>,
        node: usize,
        ticket: ReadInvocationTicket,
        ordinal: usize,
    ) {
        assert!(ordinal < super::requests::ATTEMPTS);
        if h.last_read[node].as_ref().is_some_and(|s| {
            s.visit.owner == ticket.binding.owner
                && s.visit.group == ticket.group
                && s.read == Some(ticket.request)
        }) {
            assert!(ordinal < super::requests::ATTEMPTS);
            self.observe(1 << ordinal, h.lower.now());
        }
        let result = 1 << (8 + ordinal);
        if self.seen & result == 0 && read_result::observed(h, node, ticket) {
            self.observe(result, h.lower.now());
        }
    }
    pub fn read_output<R: std::fmt::Debug>(
        &mut self,
        ticket: ReadInvocationTicket,
        output: &ReadCompletion<R>,
        now: MonoTime,
        ordinal: usize,
    ) {
        assert_eq!(output.ticket(), ticket);
        assert!(ordinal < super::requests::ATTEMPTS);
        eprintln!(
            "nested original exported read now={now:?} ticket={ticket:?} outcome={:?}",
            output.outcome()
        );
        self.observe(1 << (4 + ordinal), now);
    }
}

fn original_prefix(
    h: &Host<'_, '_>,
    attempt: &ClientAttempt,
    bytes: &[u8],
    position: ProposalPosition,
) -> u64 {
    let mut mask = 1;
    for (slot, node) in h.nodes.iter().enumerate() {
        let local = node.local();
        assert_eq!(local.owner.identity(), attempt.owners[slot]);
        let core = local.owner.core(attempt.ticket.group).unwrap();
        assert_eq!(core.membership().id(), attempt.configurations[slot]);
        let state = core.state();
        let exact = state.entry_at(position.index).is_some_and(|entry| {
            entry.term == position.term && matches!(&entry.payload,
            EntryPayload::Command { operation, bytes: original } if *operation == attempt.ticket.operation && original == bytes)
        });
        if exact {
            mask |= 2 << (slot * 3);
            if state.commit_index >= position.index {
                mask |= 4 << (slot * 3);
                if local.applications[&attempt.ticket.group].applied_index() >= position.index {
                    mask |= 8 << (slot * 3);
                }
            }
        }
    }
    mask
}

pub struct Election {
    owners: [RuntimeOwner; 3],
    configuration: voteboat::identity::ConfigurationId,
    position: ProposalPosition,
}
impl Election {
    pub fn new(host: &Host<'_, '_>) -> Self {
        let core = host.nodes[0]
            .local()
            .owner
            .core(super::setup::top().group)
            .unwrap();
        Self {
            owners: std::array::from_fn(|i| host.nodes[i].local().owner.identity()),
            configuration: core.membership().id(),
            position: ProposalPosition {
                index: core.state().last_index().checked_add(1).unwrap(),
                term: core.state().hard_state.term.checked_add(1).unwrap(),
            },
        }
    }
}
impl Watchdog {
    pub fn election(&mut self, host: &Host<'_, '_>, original: &Election) {
        let mut mask = 0;
        for (slot, node) in host.nodes.iter().enumerate() {
            let local = node.local();
            assert_eq!(local.owner.identity(), original.owners[slot]);
            let core = local.owner.core(super::setup::top().group).unwrap();
            let state = core.state();
            if state.hard_state.term != original.position.term {
                continue;
            }
            let shift = slot * 4;
            if state.hard_state.voted_for == Some(super::support::node(101))
                && state.ballot_origin.is_some_and(|ballot| {
                    ballot.configuration == original.configuration
                        && ballot.candidate_store == original.owners[0].store.identity
                })
            {
                mask |= 1 << shift;
            }
            let exact = state
                .entry_at(original.position.index)
                .is_some_and(|entry| {
                    entry.term == original.position.term && entry.payload == EntryPayload::Noop
                });
            if exact {
                mask |= 2 << shift;
                if state.commit_index >= original.position.index {
                    mask |= 4 << shift;
                    if local.applications[&super::setup::top().group].applied_index()
                        >= original.position.index
                    {
                        mask |= 8 << shift;
                    }
                }
            }
        }
        self.observe(mask, host.lower.now());
    }
}
