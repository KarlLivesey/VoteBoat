// SPDX-License-Identifier: RPL-1.5
// Copyright (c) 2026 Karl Livesey
use std::array;

pub const OLD: u8 = 0b0111;
pub const NEW: u8 = 0b1110;
pub const ALL: u8 = 0b1111;

#[derive(Clone, Copy, Debug, Eq, Hash, PartialEq)]
pub enum Policy {
    Old,
    Joint,
    New,
}
#[derive(Clone, Copy, Debug, Eq, Hash, PartialEq)]
pub enum Layer {
    Child,
    Parent,
}
#[derive(Clone, Copy, Debug, Eq, Hash, PartialEq)]
pub enum Entry {
    Begin { operation: u8, layer: Layer },
    Outcome { operation: u8, layer: Layer },
    Release(u8),
    Joint(u8),
    Final(u8),
    Noop,
}
#[derive(Clone, Debug, Default, Eq, Hash, PartialEq)]
pub struct Image {
    pub term: u8,
    pub vote: Option<u8>,
    pub bytes: Vec<u8>,
    pub parent: Vec<Entry>,
    pub epoch: u8,
    pub representative: u8,
}
#[derive(Clone, Debug, Eq, Hash, PartialEq)]
pub enum Record {
    Promise,
    Import(Box<Image>),
    Parent { position: usize, entry: Entry },
    Replace { position: usize, entry: Entry },
    Joint(u8),
    Final(u8),
    Noop,
    Claim { epoch: u8, representative: u8 },
}
#[derive(Clone, Debug, Default, Eq, Hash, PartialEq)]
pub struct Physical {
    pub checkpoint: Vec<Record>,
    pub synced: Vec<Record>,
    pub written: Vec<Record>,
    pub staged: Option<Vec<Record>>,
}
impl Physical {
    pub fn durable(&self) -> Vec<Record> {
        self.checkpoint
            .iter()
            .chain(&self.synced)
            .cloned()
            .collect()
    }
    pub fn disk_write(&self) -> Vec<Record> {
        self.checkpoint
            .iter()
            .chain(&self.written)
            .cloned()
            .collect()
    }
}
#[derive(Clone, Debug, Eq, Hash, PartialEq)]
pub struct Cell {
    pub identity: u8,
    pub physical_mask: u8,
    pub accepted: Vec<Record>,
    pub physical: [Physical; 4],
    pub committed: usize,
    pub applied: usize,
    pub observed: usize,
}
impl Cell {
    pub fn new(identity: u8, physical_mask: u8) -> Self {
        Self {
            identity,
            physical_mask,
            accepted: Vec::new(),
            physical: array::from_fn(|_| Physical::default()),
            committed: 0,
            applied: 0,
            observed: 0,
        }
    }
}
#[derive(Clone, Copy, Debug, Eq, Hash, PartialEq)]
pub enum Use {
    Election,
    Read,
    Commit,
}
#[derive(Clone, Debug, Eq, Hash, PartialEq)]
pub struct Evidence {
    pub layer: Layer,
    pub policy: Policy,
    pub witnesses: u8,
    pub kind: Use,
    pub accepted: bool,
    pub joint_committed: bool,
    pub configuration: ConfigurationSource,
}
#[derive(Clone, Debug, Eq, Hash, PartialEq)]
pub enum ConfigurationSource {
    Child(Vec<Record>, usize),
    Parent(Vec<Entry>, usize),
}
#[derive(Clone, Debug, Eq, Hash, PartialEq)]
pub struct Admission {
    pub layer: Layer,
    pub operation: u8,
    pub parent_prefix: Vec<Entry>,
}
#[derive(Clone, Debug, Eq, Hash, PartialEq)]
pub struct Recovery {
    pub cell: usize,
    pub physical: usize,
    pub boundary: usize,
    pub own_bytes: Vec<Record>,
}
#[derive(Clone, Debug, Default, Eq, Hash, PartialEq)]
pub struct Control {
    pub accepted: Vec<Entry>,
    pub committed: usize,
    pub applied: usize,
    pub leader: usize,
    pub observed: usize,
    pub receipts: Vec<(u8, bool)>,
    pub active: Option<(u8, Layer, bool)>,
}
#[derive(Clone, Copy, Debug, Eq, Hash, PartialEq)]
pub enum Weakening {
    None,
    NewOnly,
    IgnoreIntent,
}
#[derive(Clone, Copy, Debug, Eq, Hash, PartialEq)]
pub enum Step {
    Next,
    Write { cell: usize, physical: usize },
    Sync { cell: usize, physical: usize },
    Commit(usize),
    Apply(usize),
    Observe(usize),
    Lose,
    Restart { cell: usize, physical: usize },
    CheckpointWrite { cell: usize, physical: usize },
    CheckpointSync { cell: usize, physical: usize },
    Choose(Layer),
    ObserveParent,
    RestartParent,
}
#[derive(Clone, Debug, Eq, Hash, PartialEq)]
pub struct State {
    pub cells: [Cell; 4],
    pub parent: Control,
    pub pc: usize,
    pub begun: bool,
    pub lost: bool,
    pub restarted: bool,
    pub evidence: Vec<Evidence>,
    pub admissions: Vec<Admission>,
    pub recovery: Option<Recovery>,
    pub removed_refused: bool,
    pub rollback_reeligible: bool,
    pub ready_refused: bool,
    pub overlap_refused: bool,
    pub choice: Option<Layer>,
    pub queued: [Option<(u8, Layer)>; 2],
    pub terminals: u8,
}
impl State {
    pub fn new(child_four: bool) -> Self {
        Self {
            cells: array::from_fn(|i| {
                Cell::new(i as u8, if child_four && i == 0 { ALL } else { OLD })
            }),
            parent: Control::default(),
            pc: 0,
            begun: false,
            lost: false,
            restarted: false,
            evidence: Vec::new(),
            admissions: Vec::new(),
            recovery: None,
            removed_refused: false,
            rollback_reeligible: false,
            ready_refused: false,
            overlap_refused: false,
            choice: None,
            queued: [None; 2],
            terminals: 0,
        }
    }
}
