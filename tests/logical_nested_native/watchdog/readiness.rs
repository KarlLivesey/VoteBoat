// SPDX-License-Identifier: RPL-1.5
// Copyright (c) 2026 Karl Livesey
//! Expected readiness is not durability evidence or completion authority.
use super::super::{host::Host, setup};
use voteboat::{
    application::StateMachine,
    identity::ConfigurationId,
    log::{BallotOrigin, EntryPayload},
    raft::{Raft, Role},
    runtime::*,
};
#[derive(Debug)]
pub struct Readiness {
    ticket: ClientTicket,
    configuration: ConfigurationId,
    position: ProposalPosition,
}
fn self_ballot(core: &Raft, owner: RuntimeOwner, configuration: ConfigurationId) -> bool {
    core.state().hard_state.voted_for == Some(core.local_node())
        && core.state().ballot_origin
            == Some(BallotOrigin {
                configuration,
                candidate_store: owner.store.identity,
            })
}
fn position(core: &Raft, ticket: ClientTicket) -> Option<ProposalPosition> {
    let state = core.state();
    let term = state.hard_state.term;
    if core.role() != Role::Leader
        || term == 0
        || !self_ballot(core, ticket.binding.owner, core.membership().id())
    {
        return None;
    }
    let index = match state.entries.iter().find(|entry| entry.term == term) {
        Some(entry) if entry.payload == EntryPayload::Noop => entry.index,
        Some(_) => return None,
        None if state.base_term() < term && core.has_pending_dependency() => {
            state.last_index().checked_add(1)?
        }
        None => return None,
    };
    Some(ProposalPosition { index, term })
}
pub fn capture(
    host: &Host<'_, '_>,
    node: usize,
    ticket: ClientTicket,
    before: u64,
    resumed: bool,
) -> Option<Readiness> {
    let local = host.nodes[node].local();
    assert_eq!(local.owner.identity(), ticket.binding.owner);
    assert_eq!(ticket.group, setup::top().group);
    let core = local.owner.core(ticket.group).unwrap();
    assert_eq!(core.state().last_index(), before);
    let readiness = if resumed {
        position(core, ticket).map(|position| Readiness {
            ticket,
            configuration: core.membership().id(),
            position,
        })
    } else {
        None
    };
    let first_current = core
        .state()
        .entries
        .iter()
        .find(|entry| entry.term == core.state().hard_state.term);
    let candidate = readiness
        .as_ref()
        .is_some_and(|r| r.position.index > before);
    eprintln!("nested original admitted now={:?} node={node} ticket={ticket:?} configuration={:?} role={:?} hard={:?} origin={:?} last={before} base=({}, {}) pending={} first_current={first_current:?} candidate={candidate} readiness={readiness:?}", host.lower.now(), core.membership().id(), core.role(), core.state().hard_state, core.state().ballot_origin, core.state().base_index(), core.state().base_term(), core.has_pending_dependency());
    readiness
}
impl Readiness {
    pub fn mask(&self, host: &Host<'_, '_>, node: usize) -> u32 {
        let local = host.nodes[node].local();
        assert_eq!(local.owner.identity(), self.ticket.binding.owner);
        let core = local.owner.core(self.ticket.group).unwrap();
        if core.membership().id() != self.configuration
            || core.state().hard_state.term != self.position.term
            || !self_ballot(core, self.ticket.binding.owner, self.configuration)
            || !core
                .state()
                .entry_at(self.position.index)
                .is_some_and(|entry| {
                    entry.term == self.position.term && entry.payload == EntryPayload::Noop
                })
        {
            return 0;
        }
        let mut mask = 1;
        if core.state().commit_index >= self.position.index {
            mask |= 2;
            if local.applications[&self.ticket.group].applied_index() >= self.position.index {
                mask |= 4;
            }
        }
        mask
    }
}
