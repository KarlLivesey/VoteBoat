// SPDX-License-Identifier: RPL-1.5
// Copyright (c) 2026 Karl Livesey
use super::{protocol::Op, types::*};

#[derive(Clone, Copy, Debug)]
pub enum Profile {
    Child,
    Parent,
    Overlap,
}
pub fn initial(profile: Profile) -> State {
    let mut state = State::new(!matches!(profile, Profile::Parent));
    if matches!(profile, Profile::Overlap) {
        state.queued = [Some((10, Layer::Child)), Some((20, Layer::Parent))];
    }
    state
}
fn initialize() -> Vec<Op> {
    (0..3).map(|c| Op::Cell(c, Record::Promise)).collect()
}
fn parent(ops: &mut Vec<Op>, entry: Entry, cells: usize) {
    ops.push(Op::ParentAccept(entry));
    ops.extend((0..cells).map(Op::ParentPersist));
    ops.extend([Op::ParentCommit, Op::ParentApply]);
    if matches!(
        entry,
        Entry::Begin { .. } | Entry::Outcome { .. } | Entry::Release(_)
    ) {
        ops.push(Op::ParentWait);
    }
}
fn intent(ops: &mut Vec<Op>, operation: u8, layer: Layer, cells: usize) {
    parent(ops, Entry::Begin { operation, layer }, cells);
}
fn release(ops: &mut Vec<Op>, operation: u8, layer: Layer, cells: usize) {
    parent(ops, Entry::Outcome { operation, layer }, cells);
    parent(ops, Entry::Release(operation), cells);
}
pub fn program(profile: Profile, choice: Option<Layer>) -> Vec<Op> {
    match profile {
        Profile::Child => child(),
        Profile::Parent => parent_activation(),
        Profile::Overlap => overlap(choice.unwrap_or(Layer::Child)),
    }
}
fn child() -> Vec<Op> {
    let mut ops = initialize();
    ops.push(Op::Cell(
        0,
        Record::Claim {
            epoch: 1,
            representative: 0,
        },
    ));
    intent(&mut ops, 1, Layer::Child, 3);
    ops.extend([
        Op::Accept(0, Record::Joint(1)),
        Op::Uncommitted(0),
        Op::Rollback(0),
        Op::Flush(0),
        Op::Eligibility(0, true),
        Op::Cell(0, Record::Joint(1)),
        Op::Use(Layer::Child, 0b1010, Use::Election, false),
        Op::Use(Layer::Child, 0b0110, Use::Read, true),
        Op::Accept(0, Record::Final(1)),
        Op::Eligibility(0, false),
        Op::Uncommitted(0),
        Op::Rollback(0),
        Op::Flush(0),
        Op::Eligibility(0, true),
        Op::Cell(0, Record::Final(1)),
        Op::Cell(
            0,
            Record::Claim {
                epoch: 2,
                representative: 3,
            },
        ),
        Op::Checkpoint(0),
        Op::Recover(0),
        Op::Use(Layer::Child, 0b1010, Use::Election, true),
        Op::Use(Layer::Child, 0b1010, Use::Read, true),
    ]);
    release(&mut ops, 1, Layer::Child, 3);
    ops.push(Op::Terminal(1));
    ops
}
fn parent_activation() -> Vec<Op> {
    let mut ops = initialize();
    intent(&mut ops, 2, Layer::Parent, 3);
    ops.push(Op::ParentAccept(Entry::Joint(2)));
    ops.extend((0..3).map(Op::ParentPersist));
    ops.push(Op::Use(Layer::Parent, 0b1010, Use::Election, false));
    ops.extend([
        Op::Import(3, 0),
        Op::Checkpoint(3),
        Op::Use(Layer::Parent, 0b1010, Use::Read, false),
        Op::Use(Layer::Parent, 0b0011, Use::Election, false),
        Op::ParentReplace,
    ]);
    ops.extend((0..4).map(Op::PersistReplacement));
    ops.extend([
        Op::Use(Layer::Parent, 0b0011, Use::Read, true),
        Op::ParentCommit,
        Op::ParentApply,
        Op::Recover(3),
    ]);
    parent(&mut ops, Entry::Joint(2), 4);
    ops.push(Op::Use(Layer::Parent, 0b0110, Use::Read, true));
    parent(&mut ops, Entry::Final(2), 4);
    ops.extend([
        Op::Use(Layer::Parent, 0b1010, Use::Election, true),
        Op::Leader(1),
        Op::Use(Layer::Parent, 0b1010, Use::Read, true),
    ]);
    release(&mut ops, 2, Layer::Parent, 4);
    ops.push(Op::Terminal(2));
    ops
}
fn transition(ops: &mut Vec<Op>, operation: u8, layer: Layer) {
    match layer {
        Layer::Child => ops.extend([
            Op::Cell(0, Record::Joint(operation)),
            Op::Cell(0, Record::Final(operation)),
            Op::Cell(
                0,
                Record::Claim {
                    epoch: 2,
                    representative: 3,
                },
            ),
            Op::Recover(0),
        ]),
        Layer::Parent => {
            parent(ops, Entry::Joint(operation), 4);
            parent(ops, Entry::Final(operation), 4);
            ops.extend([
                Op::Use(Layer::Parent, 0b0110, Use::Election, true),
                Op::Leader(1),
                Op::Recover(1),
            ]);
        }
    }
    release(ops, operation, layer, 4);
}
fn overlap(first: Layer) -> Vec<Op> {
    let mut ops = initialize();
    ops.extend([
        Op::Cell(
            0,
            Record::Claim {
                epoch: 1,
                representative: 0,
            },
        ),
        Op::Import(3, 0),
        Op::Checkpoint(3),
        Op::Choose,
    ]);
    let (first_op, second_op, second) = if first == Layer::Child {
        (10, 20, Layer::Parent)
    } else {
        (20, 10, Layer::Child)
    };
    // Two original submissions are concurrently queued. The selected ordering
    // is chosen nondeterministically, then both are actually parent-committed.
    intent(&mut ops, first_op, first, 4);
    intent(&mut ops, second_op, second, 4);
    ops.push(Op::Overlap);
    intent(&mut ops, second_op, second, 4); // original refused retry stays refused
    ops.push(Op::Cell(0, Record::Noop)); // ordinary child write needs no new intent
    transition(&mut ops, first_op, first);
    intent(&mut ops, second_op, second, 4); // still original refused outcome
    intent(&mut ops, second_op + 1, second, 4);
    transition(&mut ops, second_op + 1, second);
    ops.push(Op::Terminal(4));
    ops
}
