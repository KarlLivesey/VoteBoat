// SPDX-License-Identifier: RPL-1.5
// Copyright (c) 2026 Karl Livesey
use super::types::*;

pub fn child_policy(records: &[Record]) -> Policy {
    records
        .iter()
        .rev()
        .find_map(|r| match r {
            Record::Joint(_) => Some(Policy::Joint),
            Record::Final(_) => Some(Policy::New),
            _ => None,
        })
        .unwrap_or(Policy::Old)
}
pub fn parent_policy(entries: &[Entry]) -> Policy {
    entries
        .iter()
        .rev()
        .find_map(|r| match r {
            Entry::Joint(_) => Some(Policy::Joint),
            Entry::Final(_) => Some(Policy::New),
            _ => None,
        })
        .unwrap_or(Policy::Old)
}
pub fn quorum(policy: Policy, witnesses: u8, weak: Weakening) -> bool {
    let old = (witnesses & OLD).count_ones() >= 2;
    let new = (witnesses & NEW).count_ones() >= 2;
    match policy {
        Policy::Old => old,
        Policy::Joint if weak == Weakening::NewOnly => new,
        Policy::Joint => old && new,
        Policy::New => new,
    }
}
pub fn eligible(policy: Policy, physical: u8) -> bool {
    let mask = match policy {
        Policy::Old => OLD,
        Policy::Joint => ALL,
        Policy::New => NEW,
    };
    physical < 4 && mask & (1 << physical) != 0
}
pub fn image(cell: &Cell) -> Image {
    let mut out = Image::default();
    for record in &cell.accepted[..cell.applied] {
        match record {
            Record::Promise => {
                out.term = 1;
                out.vote = Some(7);
                out.bytes = vec![0x58, 0x59];
            }
            Record::Import(import) => out = *import.clone(),
            Record::Parent { position, entry } => {
                assert_eq!(*position, out.parent.len() + 1);
                out.parent.push(*entry);
            }
            Record::Replace { position, entry } => {
                out.parent.truncate(position - 1);
                out.parent.push(*entry);
            }
            Record::Claim {
                epoch,
                representative,
            } => {
                assert_eq!(*epoch, out.epoch + 1);
                out.epoch = *epoch;
                out.representative = *representative;
            }
            Record::Joint(_) | Record::Final(_) | Record::Noop => {}
        }
    }
    out
}
pub fn matching(cell: &Cell, boundary: usize) -> u8 {
    cell.physical.iter().enumerate().fold(0, |mask, (i, p)| {
        let bytes = p.durable();
        if cell.physical_mask & (1 << i) != 0 && bytes.starts_with(&cell.accepted[..boundary]) {
            mask | (1 << i)
        } else {
            mask
        }
    })
}
pub fn caught_up(cell: &Cell) -> bool {
    matching(cell, cell.accepted.len()) == cell.physical_mask
}
pub fn joint_committed(cell: &Cell) -> bool {
    cell.accepted[..cell.committed]
        .iter()
        .any(|r| matches!(r, Record::Joint(_)))
}
pub fn parent_matches(state: &State, boundary: usize) -> u8 {
    state.cells.iter().enumerate().fold(0, |mask, (i, c)| {
        let value = image(c);
        if value.term == 1
            && value.vote == Some(7)
            && value.bytes == [0x58, 0x59]
            && value.parent.starts_with(&state.parent.accepted[..boundary])
        {
            mask | (1 << i)
        } else {
            mask
        }
    })
}
pub fn parent_joint_committed(state: &State) -> bool {
    state.parent.accepted[..state.parent.committed]
        .iter()
        .any(|e| matches!(e, Entry::Joint(_)))
}
pub fn replay_control(control: &mut Control) {
    control.active = None;
    control.receipts.clear();
    for entry in &control.accepted[..control.applied] {
        match *entry {
            Entry::Begin { operation, layer } => {
                if !control.receipts.iter().any(|(op, _)| *op == operation) {
                    let accepted = control.active.is_none();
                    control.receipts.push((operation, accepted));
                    if accepted {
                        control.active = Some((operation, layer, false));
                    }
                }
            }
            Entry::Outcome { operation, layer } => {
                assert_eq!(control.active, Some((operation, layer, false)));
                control.active = Some((operation, layer, true));
            }
            Entry::Release(operation) => {
                assert!(matches!(control.active, Some((op, _, true)) if op == operation));
                control.active = None;
            }
            Entry::Joint(_) | Entry::Final(_) | Entry::Noop => {}
        }
    }
}
