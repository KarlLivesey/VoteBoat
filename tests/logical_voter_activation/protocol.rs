// SPDX-License-Identifier: RPL-1.5
// Copyright (c) 2026 Karl Livesey
use super::{actions::next_op, journal::*, types::*};

#[derive(Clone, Debug)]
pub enum Op {
    Cell(usize, Record),
    Accept(usize, Record),
    Flush(usize),
    Uncommitted(usize),
    Rollback(usize),
    ParentAccept(Entry),
    ParentPersist(usize),
    ParentReplace,
    PersistReplacement(usize),
    ParentCommit,
    ParentApply,
    ParentWait,
    Import(usize, usize),
    Checkpoint(usize),
    Recover(usize),
    Leader(usize),
    Eligibility(u8, bool),
    Use(Layer, u8, Use, bool),
    Choose,
    Overlap,
    Terminal(u8),
}
pub fn steps(state: &State, program: &[Op], weak: Weakening) -> Vec<Step> {
    let Some(op) = program.get(state.pc) else {
        return Vec::new();
    };
    match op {
        Op::Cell(c, _) | Op::ParentPersist(c) | Op::PersistReplacement(c) | Op::Import(c, _)
            if state.begun =>
        {
            io_steps(state, *c, true, weak)
        }
        Op::Flush(c) => io_steps(state, *c, true, weak),
        Op::Uncommitted(c) => io_steps(state, *c, false, weak),
        Op::Checkpoint(c) => checkpoint_steps(state, *c),
        Op::Choose => vec![Step::Choose(Layer::Child), Step::Choose(Layer::Parent)],
        Op::ParentWait => {
            let mut out = vec![if state.parent.observed == state.parent.applied {
                Step::Next
            } else {
                Step::ObserveParent
            }];
            if state.parent.observed < state.parent.applied {
                if !state.lost {
                    out.push(Step::Lose);
                }
                if !state.restarted {
                    out.push(Step::RestartParent);
                }
            }
            out
        }
        _ => vec![Step::Next],
    }
}
fn io_steps(state: &State, c: usize, commit: bool, weak: Weakening) -> Vec<Step> {
    let cell = &state.cells[c];
    let mut out = Vec::new();
    for p in 0..4 {
        if cell.physical_mask & (1 << p) == 0 {
            continue;
        }
        let disk = &cell.physical[p];
        if disk.disk_write() != cell.accepted {
            out.push(Step::Write {
                cell: c,
                physical: p,
            });
        } else if disk.durable() != cell.accepted {
            out.push(Step::Sync {
                cell: c,
                physical: p,
            });
        }
        if !state.restarted && disk.disk_write() != disk.durable() {
            out.push(Step::Restart {
                cell: c,
                physical: p,
            });
        }
    }
    if !commit {
        let n = matching(cell, cell.accepted.len());
        if n != 0 && !quorum(child_policy(&cell.accepted), n, Weakening::None) {
            out.push(Step::Next);
        }
        return out;
    }
    if cell.committed < cell.accepted.len()
        && quorum(
            child_policy(&cell.accepted),
            matching(cell, cell.accepted.len()),
            weak,
        )
    {
        out.push(Step::Commit(c));
    }
    if cell.applied < cell.committed {
        out.push(Step::Apply(c));
    }
    if cell.observed < cell.applied {
        out.push(Step::Observe(c));
        if !state.lost {
            out.push(Step::Lose);
        }
    }
    if cell.observed == cell.accepted.len() && caught_up(cell) {
        out.push(Step::Next);
    }
    out
}
fn checkpoint_steps(state: &State, c: usize) -> Vec<Step> {
    let cell = &state.cells[c];
    let prefix = &cell.accepted[..cell.applied];
    let mut out = Vec::new();
    for p in 0..4 {
        if cell.physical_mask & (1 << p) == 0 {
            continue;
        }
        if cell.physical[p].checkpoint != prefix {
            out.push(if cell.physical[p].staged.is_none() {
                Step::CheckpointWrite {
                    cell: c,
                    physical: p,
                }
            } else {
                Step::CheckpointSync {
                    cell: c,
                    physical: p,
                }
            });
        }
    }
    if out.is_empty() {
        out.push(Step::Next);
    }
    out
}
pub fn advance(state: &State, step: Step, program: &[Op], weak: Weakening) -> State {
    let mut next = state.clone();
    match step {
        Step::Next => next_op(&mut next, &program[state.pc], weak),
        Step::Write { cell, physical } => {
            let c = &mut next.cells[cell];
            c.physical[physical].written =
                c.accepted[c.physical[physical].checkpoint.len()..].to_vec();
        }
        Step::Sync { cell, physical } => {
            let p = &mut next.cells[cell].physical[physical];
            p.synced.clone_from(&p.written);
        }
        Step::Commit(c) => commit_child(&mut next, c, weak),
        Step::Apply(c) => next.cells[c].applied = next.cells[c].committed,
        Step::Observe(c) => {
            let cell = &mut next.cells[c];
            assert!(quorum(
                child_policy(&cell.accepted),
                matching(cell, cell.applied),
                weak
            ));
            // Lost output is resolved by a new quorum observation of the same
            // actual committed record, never by a caller-ready assertion.
            cell.observed = cell.applied;
        }
        Step::Lose => {
            next.lost = true;
        }
        Step::Restart { cell, physical } => {
            let p = &mut next.cells[cell].physical[physical];
            p.written.clone_from(&p.synced);
            p.staged = None;
            next.restarted = true;
        }
        Step::CheckpointWrite { cell, physical } => {
            let c = &mut next.cells[cell];
            c.physical[physical].staged = Some(c.accepted[..c.applied].to_vec());
        }
        Step::CheckpointSync { cell, physical } => {
            let p = &mut next.cells[cell].physical[physical];
            let prefix = p.staged.take().unwrap();
            let durable = p.durable();
            assert!(durable.starts_with(&prefix));
            p.synced = durable[prefix.len()..].to_vec();
            p.written.clone_from(&p.synced);
            p.checkpoint = prefix;
        }
        Step::Choose(layer) => {
            assert!(next
                .queued
                .iter()
                .flatten()
                .any(|(_, candidate)| *candidate == layer));
            next.choice = Some(layer);
            next.pc += 1;
        }
        Step::ObserveParent => {
            assert!(quorum(
                parent_policy(&image(&next.cells[next.parent.leader]).parent),
                parent_matches(&next, next.parent.applied),
                weak
            ));
            let leader = next.parent.leader;
            super::actions::recover(&mut next, leader);
            next.parent.observed = next.parent.applied;
        }
        Step::RestartParent => {
            let leader = next.parent.leader;
            super::actions::recover(&mut next, leader);
            next.lost = true;
            next.restarted = true;
        }
    }
    next
}
fn commit_child(state: &mut State, c: usize, weak: Weakening) {
    let cell = &mut state.cells[c];
    let policy = child_policy(&cell.accepted);
    let witnesses = matching(cell, cell.accepted.len());
    let joint = joint_committed(cell);
    assert!(quorum(policy, witnesses, weak));
    state.evidence.clear();
    state.evidence.push(Evidence {
        layer: Layer::Child,
        policy,
        witnesses,
        kind: Use::Commit,
        accepted: true,
        joint_committed: joint,
        configuration: ConfigurationSource::Child(cell.accepted.clone(), cell.committed),
    });
    cell.committed = cell.accepted.len();
}
