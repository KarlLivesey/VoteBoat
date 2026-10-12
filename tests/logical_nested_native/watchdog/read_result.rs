// SPDX-License-Identifier: RPL-1.5
// Copyright (c) 2026 Karl Livesey
//! Original result authorization is progress; consumed completion is authority.
use super::Host;
use voteboat::{application::StateMachine, identity::*, logical_voter::*, raft::Role, runtime::*};

#[derive(Clone, Copy)]
struct Expected {
    ticket: ReadInvocationTicket,
    configuration: ConfigurationId,
    term: u64,
    commit: u64,
    applied: u64,
    schema: u64,
}
#[derive(Clone, Copy)]
struct Facts<'a> {
    binding: ReadInvocationBinding,
    view: ReadInvocationInspection,
    step: Option<&'a OwnerStep>,
    held: Option<EffectVisitInspection>,
    capsule: Option<&'a LogicalCapsule>,
}

pub(super) fn observed(h: &Host<'_, '_>, node: usize, ticket: ReadInvocationTicket) -> bool {
    let Some(node_ref) = h.nodes.get(node) else {
        return false;
    };
    let local = node_ref.local();
    let (Ok(view), Some(core)) = (
        node_ref.inspect_read(ticket),
        local.owner.core(ticket.group),
    ) else {
        return false;
    };
    if core.role() != Role::Leader || core.read_request_floor() != ticket.request.get() {
        return false;
    }
    let expected = Expected {
        ticket,
        configuration: core.membership().id(),
        term: core.state().hard_state.term,
        commit: core.state().commit_index,
        applied: local.applications[&ticket.group].applied_index(),
        schema: local.applications[&ticket.group].logical_response_schema(),
    };
    let held = local.owner.inspect_visit(ticket.group).ok().flatten();
    h.upper[node].inspect_work(|work| {
        let facts = Facts {
            binding: local.reads.binding(),
            view,
            step: h.last_read[node].as_ref(),
            held,
            capsule: work.as_ref().and_then(LogicalBridgeWorkInspection::capsule),
        };
        let matches = matches_original(expected, facts);
        if matches {
            check_expectations(expected, facts);
            eprintln!(
                "nested original read-result authorization now={:?} ticket={ticket:?} effect={:?} origin={:?} expectation_controls=12",
                h.lower.now(),
                held.and_then(EffectVisitInspection::leased),
                facts.capsule.map(|c| c.origin)
            );
        }
        matches
    })
}

fn matches_original(expected: Expected, facts: Facts<'_>) -> bool {
    let ticket = expected.ticket;
    if facts.binding != ticket.binding
        || facts.view.ticket() != ticket
        || !facts.view.stepped()
        || facts.view.terminal()
    {
        return false;
    }
    let Some(step) = facts.step else {
        return false;
    };
    if step.read != Some(ticket.request)
        || step.visit.owner != ticket.binding.owner
        || step.visit.group != ticket.group
        || step.admission != Some(facts.view.admission())
    {
        return false;
    }
    let (Some(held), Some(capsule)) = (facts.held, facts.capsule) else {
        return false;
    };
    if held.visit().owner != ticket.binding.owner
        || held.visit().group != ticket.group
        || held.kind() != Some(EffectLeaseInspectionKind::Read(ticket.request))
    {
        return false;
    }
    let LogicalCapsuleOrigin::Read {
        effect,
        ticket: original,
        barrier,
    } = capsule.origin
    else {
        return false;
    };
    held.leased() == Some(effect)
        && original == LogicalReadTicket::from(ticket)
        && barrier.group == ticket.group
        && barrier.request == ticket.request
        && barrier.configuration == expected.configuration
        && barrier.term == expected.term
        && barrier.index <= expected.commit
        && barrier.index <= expected.applied
        && matches!(&capsule.body, LogicalCapsuleBody::ReadResult { schema, query, .. }
            if *schema == expected.schema && query.is_empty())
}

// Negative controls borrow the genuine capsule; only expectations are changed.
// They construct no authority, alter no original state, and consume no output.
fn check_expectations(expected: Expected, facts: Facts<'_>) {
    let mut foreign = expected;
    foreign.ticket.sequence += 1;
    assert!(!matches_original(foreign, facts));
    foreign = expected;
    foreign.ticket.request = ReadRequestId::new(expected.ticket.request.get() + 1).unwrap();
    assert!(!matches_original(foreign, facts));
    foreign = expected;
    foreign.ticket.binding.owner.generation =
        RuntimeGeneration::new(expected.ticket.binding.owner.generation.get() + 1).unwrap();
    assert!(!matches_original(foreign, facts));
    foreign = expected;
    foreign.ticket.binding.generation =
        ReadInvocationGeneration::new(expected.ticket.binding.generation.get() + 1).unwrap();
    assert!(!matches_original(foreign, facts));
    let stale = Expected {
        configuration: ConfigurationId::new(expected.configuration.get() + 1).unwrap(),
        ..expected
    };
    assert!(!matches_original(stale, facts));
    assert!(!matches_original(
        Expected {
            term: expected.term + 1,
            ..expected
        },
        facts
    ));
    assert!(!matches_original(
        Expected {
            schema: expected.schema + 1,
            ..expected
        },
        facts
    ));
    assert!(!matches_original(
        Expected {
            commit: 0,
            ..expected
        },
        facts
    ));
    assert!(!matches_original(
        Expected {
            applied: 0,
            ..expected
        },
        facts
    ));
    assert!(!matches_original(
        expected,
        Facts {
            held: None,
            ..facts
        }
    ));
    assert!(!matches_original(
        expected,
        Facts {
            capsule: None,
            ..facts
        }
    ));
    assert!(!matches_original(
        expected,
        Facts {
            step: None,
            ..facts
        }
    ));
}
