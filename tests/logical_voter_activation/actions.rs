// SPDX-License-Identifier: RPL-1.5
// Copyright (c) 2026 Karl Livesey
use super::{journal::*, protocol::Op, types::*};

pub fn next_op(state: &mut State, op: &Op, weak: Weakening) {
    match op {
        Op::Cell(c, record) if !state.begun => {
            admit(state, *c, record.clone(), weak);
            state.begun = true;
            return;
        }
        Op::Accept(c, record) => admit(state, *c, record.clone(), weak),
        Op::ParentPersist(c) | Op::PersistReplacement(c) if !state.begun => {
            let position = state.parent.accepted.len();
            let entry = state.parent.accepted[position - 1];
            let record = if matches!(op, Op::PersistReplacement(_)) {
                Record::Replace { position, entry }
            } else {
                Record::Parent { position, entry }
            };
            admit(state, *c, record, weak);
            state.begun = true;
            return;
        }
        Op::Import(c, source) if !state.begun => {
            assert_ne!(c, source);
            let mut original = image(&state.cells[*source]);
            assert_eq!(original.bytes, [0x58, 0x59]);
            assert_eq!(state.cells[*c].identity, *c as u8);
            // A different logical cell imports parent data, never another
            // cell's representative fence/physical identity.
            original.epoch = 0;
            original.representative = 0;
            admit(state, *c, Record::Import(Box::new(original)), weak);
            state.begun = true;
            return;
        }
        Op::Rollback(c) => {
            let cell = &mut state.cells[*c];
            assert_eq!(cell.accepted.len(), cell.committed + 1);
            cell.accepted.pop();
            cell.accepted.push(Record::Noop);
        }
        Op::ParentAccept(entry) => admit_parent(state, *entry, weak),
        Op::ParentReplace => {
            assert!(state.parent.accepted.len() > state.parent.committed);
            *state.parent.accepted.last_mut().unwrap() = Entry::Noop;
        }
        Op::ParentCommit => parent_commit(state, weak),
        Op::ParentApply => {
            assert_eq!(
                image(&state.cells[state.parent.leader]).parent,
                state.parent.accepted
            );
            state.parent.applied = state.parent.committed;
            replay_control(&mut state.parent);
        }
        Op::Recover(c) => recover(state, *c),
        Op::Leader(c) => {
            assert!(eligible(
                parent_policy(&image(&state.cells[*c]).parent),
                *c as u8
            ));
            assert!(state
                .evidence
                .last()
                .is_some_and(|e| e.layer == Layer::Parent
                    && e.kind == Use::Election
                    && e.accepted));
            state.parent.leader = *c;
        }
        Op::Eligibility(physical, expected) => {
            let allowed = eligible(child_policy(&state.cells[0].accepted), *physical);
            assert_eq!(allowed, *expected);
            state.removed_refused |= !allowed && *physical == 0;
            state.rollback_reeligible |= allowed && state.removed_refused;
        }
        Op::Use(layer, witnesses, kind, expected) => {
            use_evidence(state, *layer, *witnesses, *kind, *expected, weak);
        }
        Op::Overlap => overlap(state, weak),
        Op::Terminal(bit) => state.terminals |= bit,
        Op::Cell(_, _)
        | Op::Flush(_)
        | Op::Uncommitted(_)
        | Op::ParentPersist(_)
        | Op::PersistReplacement(_)
        | Op::Import(_, _)
        | Op::Checkpoint(_)
        | Op::ParentWait => {}
        Op::Choose => unreachable!("choice has an explicit action"),
    }
    state.pc += 1;
    state.begun = false;
    state.lost = false;
    state.restarted = false;
}
fn authorized(state: &State, operation: u8, layer: Layer) -> bool {
    state.parent.active == Some((operation, layer, false))
}
fn admission(state: &mut State, operation: u8, layer: Layer) {
    state.admissions.push(Admission {
        layer,
        operation,
        parent_prefix: state.parent.accepted[..state.parent.applied].to_vec(),
    });
}
fn admit(state: &mut State, c: usize, record: Record, weak: Weakening) {
    match record {
        Record::Joint(op) | Record::Final(op) => {
            assert!(authorized(state, op, Layer::Child) || weak == Weakening::IgnoreIntent);
            if matches!(record, Record::Final(_)) {
                assert!(state.cells[c].accepted[..state.cells[c].committed]
                    .contains(&Record::Joint(op)));
            }
            admission(state, op, Layer::Child);
        }
        Record::Claim {
            epoch,
            representative,
        } => {
            assert!(eligible(
                child_policy(&state.cells[c].accepted),
                representative
            ));
            assert_eq!(epoch, image(&state.cells[c]).epoch + 1);
        }
        Record::Parent { .. } | Record::Replace { .. } => {
            assert!(eligible(
                child_policy(&state.cells[c].accepted),
                image(&state.cells[c]).representative
            ));
        }
        _ => {}
    }
    let cell = &mut state.cells[c];
    assert!(caught_up(cell));
    assert_eq!(cell.applied, cell.accepted.len());
    cell.accepted.push(record);
}
fn admit_parent(state: &mut State, entry: Entry, weak: Weakening) {
    match entry {
        Entry::Joint(op) | Entry::Final(op) => {
            assert!(authorized(state, op, Layer::Parent) || weak == Weakening::IgnoreIntent);
            if matches!(entry, Entry::Final(_)) {
                assert!(state.parent.accepted[..state.parent.committed].contains(&Entry::Joint(op)));
            }
            admission(state, op, Layer::Parent);
        }
        Entry::Outcome { operation, layer } => {
            assert!(authorized(state, operation, layer));
            match layer {
                Layer::Child => {
                    let c = &state.cells[0];
                    assert!(c.accepted[..c.observed].contains(&Record::Final(operation)));
                }
                Layer::Parent => {
                    assert!(state.parent.accepted[..state.parent.committed]
                        .contains(&Entry::Final(operation)));
                }
            }
        }
        Entry::Release(op) => assert!(matches!(state.parent.active, Some((n, _, true)) if n == op)),
        Entry::Begin { operation, layer } => {
            if let Some(slot) = state
                .queued
                .iter_mut()
                .find(|slot| **slot == Some((operation, layer)))
            {
                *slot = None;
            }
        }
        Entry::Noop => {}
    }
    state.parent.accepted.push(entry);
}
fn parent_commit(state: &mut State, weak: Weakening) {
    let policy = parent_policy(&image(&state.cells[state.parent.leader]).parent);
    let witnesses = parent_matches(state, state.parent.accepted.len());
    assert!(quorum(policy, witnesses, weak));
    state.evidence.clear();
    state.evidence.push(Evidence {
        layer: Layer::Parent,
        policy,
        witnesses,
        kind: Use::Commit,
        accepted: true,
        joint_committed: parent_joint_committed(state),
        configuration: ConfigurationSource::Parent(
            image(&state.cells[state.parent.leader]).parent,
            state.parent.committed,
        ),
    });
    state.parent.committed = state.parent.accepted.len();
}
fn use_evidence(
    state: &mut State,
    layer: Layer,
    witnesses: u8,
    kind: Use,
    expected: bool,
    weak: Weakening,
) {
    let (policy, available, joint) = match layer {
        Layer::Child => (
            child_policy(&state.cells[0].accepted),
            matching(&state.cells[0], state.cells[0].accepted.len()),
            joint_committed(&state.cells[0]),
        ),
        Layer::Parent => (
            parent_policy(&image(&state.cells[state.parent.leader]).parent),
            parent_matches(state, state.parent.accepted.len()),
            parent_joint_committed(state),
        ),
    };
    let usable = available & witnesses;
    let accepted = quorum(policy, usable, weak);
    if weak == Weakening::None {
        assert_eq!(accepted, expected);
    }
    state.ready_refused |= !accepted && layer == Layer::Parent && witnesses & 8 != 0;
    state.evidence.clear();
    state.evidence.push(Evidence {
        layer,
        policy,
        witnesses: usable,
        kind,
        accepted,
        joint_committed: joint,
        configuration: match layer {
            Layer::Child => ConfigurationSource::Child(
                state.cells[0].accepted.clone(),
                state.cells[0].committed,
            ),
            Layer::Parent => ConfigurationSource::Parent(
                image(&state.cells[state.parent.leader]).parent,
                state.parent.committed,
            ),
        },
    });
}
pub fn recover(state: &mut State, c: usize) {
    let cell = &mut state.cells[c];
    // The surviving qualified ordinary child runtime supplies commit authority.
    // The replacement's image comes only from its selected synchronized bytes.
    let boundary = cell.committed;
    let selected = usize::from(image(cell).representative);
    assert!(eligible(child_policy(&cell.accepted), selected as u8));
    assert!(quorum(
        child_policy(&cell.accepted),
        matching(cell, boundary),
        Weakening::None
    ));
    let own = cell.physical[selected].durable();
    assert!(own.starts_with(&cell.accepted[..boundary]));
    for p in &mut cell.physical {
        p.written.clone_from(&p.synced);
        p.staged = None;
    }
    cell.accepted.clear();
    cell.applied = 0;
    cell.observed = 0;
    cell.accepted.extend_from_slice(&own[..boundary]);
    cell.applied = boundary;
    cell.observed = boundary; // exact fresh quorum-observed committed image
    state.recovery = Some(Recovery {
        cell: c,
        physical: selected,
        boundary,
        own_bytes: own[..boundary].to_vec(),
    });
    if c == state.parent.leader {
        let actual = image(cell).parent;
        assert!(actual.starts_with(&state.parent.accepted[..state.parent.committed]));
        state.parent.accepted.clear();
        state.parent.accepted.extend(actual);
        state.parent.applied = state.parent.committed;
        state.parent.active = None;
        state.parent.receipts.clear();
        replay_control(&mut state.parent);
    }
}
fn overlap(state: &mut State, weak: Weakening) {
    let (_, current, _) = state.parent.active.unwrap();
    let other = if current == Layer::Child {
        Layer::Parent
    } else {
        Layer::Child
    };
    let operation = if other == Layer::Child { 10 } else { 20 };
    if !authorized(state, operation, other) && weak == Weakening::None {
        state.overlap_refused = true;
        return;
    }
    // The weakened protocol really admits a transition into the other log.
    // The independent oracle inspects the recorded parent prefix, not a flag.
    match other {
        Layer::Child => admit(state, 0, Record::Joint(operation), weak),
        Layer::Parent => admit_parent(state, Entry::Joint(operation), weak),
    }
}
