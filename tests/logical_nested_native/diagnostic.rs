// SPDX-License-Identifier: RPL-1.5
// Copyright (c) 2026 Karl Livesey
//! Original provider inspection only; these values grant no quorum authority.
use super::{host::Host, setup};
use voteboat::{
    application::{ReadableStateMachine, StateMachine},
    identity::OperationId,
    logical_voter::*,
    outbound::OutboundQueue,
    worker::PersistenceWorker,
};
pub fn retained(host: &Host<'_, '_>) -> String {
    let middle = host
        .upper
        .iter()
        .enumerate()
        .map(|(i, b)| {
            (0..3)
                .map(|j| {
                    b.inspect_backing(j, |p| {
                        let c = p.owner.core(setup::middle(i).group).unwrap();
                        (
                            c.local_node(),
                            c.role(),
                            c.state().hard_state.term,
                            c.state().last_index(),
                            c.state().commit_index,
                            p.applications[&setup::middle(i).group].applied_index(),
                            c.has_pending_dependency(),
                            p.persistence.usage(),
                            p.clients.usage(),
                            p.reads.usage(),
                            p.owner.usage(),
                            p.outbound.usage(),
                        )
                    })
                    .unwrap()
                })
                .collect::<Vec<_>>()
        })
        .collect::<Vec<_>>();
    let bottom = host
        .lower
        .bridges
        .iter()
        .enumerate()
        .map(|(i, b)| {
            (0..3)
                .map(|j| {
                    b.inspect_backing(j, |p| {
                        let c = p.owner.core(setup::bottom(i).group).unwrap();
                        (
                            c.local_node(),
                            c.role(),
                            c.state().hard_state.term,
                            c.state().last_index(),
                            c.state().commit_index,
                            p.applications[&setup::bottom(i).group].applied_index(),
                            p.clients.usage(),
                            p.reads.usage(),
                        )
                    })
                    .unwrap()
                })
                .collect::<Vec<_>>()
        })
        .collect::<Vec<_>>();
    format!(
        "phase={}; global_work_drained={}; now={:?}; top={:?}; last_step={:?}; writes={:?}; transport={}; middle={middle:?}; original_records={:?}; upper={:?}; bottom={bottom:?}; lower={:?}",
        host.phase,host.drained(),host.lower.now(),top(host),host.last_step,
        host.writes.iter().map(super::convergence::WriteProgress::diagnostic).collect::<Vec<_>>(),host.transport(),
        actions(host),
        host.upper.iter().map(|b| b.usage()).collect::<Vec<_>>(),
        host.lower
            .bridges
            .iter()
            .map(|b| b.usage())
            .collect::<Vec<_>>()
    )
}

fn actions(host: &Host<'_, '_>) -> Vec<String> {
    host.upper
        .iter()
        .enumerate()
        .map(|(i, b)| {
            b.inspect_backing(0, |p| {
                let app = &p.applications[&setup::middle(i).group];
                let at = app.applied_index();
                let metadata = app.read_at(at, LogicalQuery::Metadata).unwrap();
                let LogicalRead::Metadata(m) = metadata else {
                    unreachable!()
                };
                let session = m.representative.unwrap().logical_session.get();
                let records = (1..=16)
                    .filter_map(|sequence| {
                        let op = OperationId::new((u128::from(session) << 64) | sequence).unwrap();
                        match app.read_at(at, LogicalQuery::Outcome(op)).unwrap() {
                            LogicalRead::Outcome(Some(s)) => {
                                Some((op, s.first_index, s.receipt.outcome, s.command.len()))
                            }
                            _ => None,
                        }
                    })
                    .collect::<Vec<_>>();
                let image = app.read_at(at, LogicalQuery::Image).unwrap();
                let LogicalRead::Image(image) = image else {
                    unreachable!()
                };
                format!(
                    "mapping={:?}, parentHard={:?}, parentRevision={:?}, parentLast={},parentBase={},parentCommit={},parentEntries={:?}, records={records:?}",
                    image.mapping, image.parent.hard_state, image.parent.revision,image.parent.last_index(),image.parent.base_index(),image.parent.commit_index,image.parent.entries
                )
            })
            .unwrap()
        })
        .collect()
}

pub fn top(host: &Host<'_, '_>) -> Vec<String> {
    host.nodes.iter().map(|n|{let p=n.local();let group=setup::top().group;let c=p.owner.core(group).unwrap();format!("node={:?},originalOwner={:?},configuration={:?},role={:?},hard={:?},base={},last={},commit={},entries={:?},reset={},pending={},applied={},commit_term={:?},deadline={:?},owner={:?},worker={:?},outbound={:?},clients={:?},reads={:?}",c.local_node(),p.owner.identity(),c.membership().id(),c.role(),c.state().hard_state,c.state().base_index(),c.state().last_index(),c.state().commit_index,c.state().entries,c.election_reset_sequence(),c.has_pending_dependency(),p.applications[&group].applied_index(),c.state().term_at(c.state().commit_index),p.owner.deadline(group),p.owner.usage(),p.persistence.usage(),p.outbound.usage(),p.clients.usage(),p.reads.usage())}).collect()
}
