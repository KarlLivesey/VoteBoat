// SPDX-License-Identifier: RPL-1.5
// Copyright (c) 2026 Karl Livesey
// Unless explicitly acquired and licensed from Licensor under another license,
// the contents of this file are subject to the Reciprocal Public License
// ("RPL") Version 1.5, or subsequent versions as allowed by the RPL, and You may
// not copy or use this file in either source code or executable form, except
// in compliance with the terms and conditions of the RPL.
//
// All software distributed under the RPL is provided strictly on an "AS IS"
// basis, WITHOUT WARRANTY OF ANY KIND, EITHER EXPRESS OR IMPLIED, AND LICENSOR
// HEREBY DISCLAIMS ALL SUCH WARRANTIES, INCLUDING WITHOUT LIMITATION, ANY
// WARRANTIES OF MERCHANTABILITY, FITNESS FOR A PARTICULAR PURPOSE, QUIET
// ENJOYMENT, OR NON-INFRINGEMENT. See the RPL for specific language governing
// rights and limitations under the RPL.
use std::array;
#[derive(Clone, Copy, Debug, Eq, PartialEq, Hash)]
pub enum Candidate {
    A,
    B,
}
impl Candidate {
    pub fn other(self) -> Self {
        match self {
            Self::A => Self::B,
            Self::B => Self::A,
        }
    }
    pub fn bit(self) -> u8 {
        match self {
            Self::A => 1,
            Self::B => 2,
        }
    }
}
#[derive(Clone, Copy, Debug, Eq, PartialEq, Hash)]
pub enum Value {
    X,
    Y,
}
#[derive(Clone, Copy, Debug, Eq, PartialEq, Hash)]
pub struct Entry {
    pub term: u8,
    pub value: Value,
}
#[derive(Clone, Copy, Debug, Default, Eq, PartialEq, Hash)]
pub struct Image {
    pub epoch: u8,
    pub term: u8,
    pub vote: Option<Candidate>,
    pub entries: [Option<Entry>; 2],
    pub commit: u8,
    pub revision: u8,
}
#[derive(Clone, Copy, Debug, Default, Eq, PartialEq, Hash)]
pub struct Physical {
    pub written: u8,
    pub synced: u8,
}
#[derive(Clone, Copy, Debug, Eq, PartialEq, Hash)]
pub struct Ticket {
    pub cell: u8,
    pub child_incarnation: u8,
    pub epoch: u8,
    pub physical: u8,
    pub session: u8,
    pub action: u8,
    pub effect: u8,
}
#[derive(Clone, Copy, Debug, Eq, PartialEq, Hash)]
pub enum Kind {
    Vote,
    Append,
    Read,
}
#[derive(Clone, Copy, Debug, Eq, PartialEq, Hash)]
pub struct Capsule {
    pub ticket: Ticket,
    pub parent_incarnation: u8,
    pub logical_store: u8,
    pub configuration: u8,
    pub requester: u8,
    pub requester_session: u8,
    pub term: u8,
    pub context: u8,
    pub kind: Kind,
    pub candidate: Option<Candidate>,
    pub index: u8,
    pub entry: Option<Entry>,
}
#[derive(Clone, Copy, Debug, Eq, PartialEq, Hash)]
pub enum Body {
    Claim(u8),
    Promise {
        term: u8,
        candidate: Candidate,
        last: (u8, u8),
    },
    Append {
        index: u8,
        entry: Entry,
    },
    Commit(u8),
    Capsule(Capsule),
}
#[derive(Clone, Debug, Eq, PartialEq, Hash)]
pub struct Record {
    pub ticket: Ticket,
    pub body: Body,
    pub expected_revision: u8,
    pub committed: bool,
    pub applied: bool,
}
#[derive(Clone, Debug, Default, Eq, PartialEq, Hash)]
pub struct Cell {
    pub physical: [Physical; 3],
    pub records: Vec<Record>,
    pub committed: u8,
    pub applied: u8,
    pub image: Image,
}
#[derive(Clone, Copy, Debug, Eq, PartialEq, Hash)]
pub struct Pending {
    pub cell: usize,
    pub position: u8,
    pub observed: bool,
    pub lost: bool,
    pub resolved: bool,
    pub cut: u8,
    pub probed: bool,
    pub wait_owned: bool,
}
#[derive(Clone, Copy, Debug, Eq, PartialEq, Hash)]
pub struct Release {
    pub expected: Ticket,
    pub source: Ticket,
    pub child_commit: u8,
    pub child_apply: u8,
}
#[derive(Clone, Copy, Debug, Eq, PartialEq, Hash)]
pub struct Election {
    pub term: u8,
    pub candidate: Candidate,
    pub logical: u8,
    pub counted: u8,
}
#[derive(Clone, Copy, Debug, Eq, PartialEq, Hash)]
pub struct Read {
    pub old: bool,
    pub invoked_after: Option<Value>,
    pub completed: Option<Value>,
    pub evidence: u8,
}
#[derive(Clone, Copy, Debug, Eq, PartialEq, Hash)]
pub struct Outage {
    pub cell: usize,
    pub live: u8,
    pub restored: bool,
}
#[derive(Clone, Copy, Debug, Eq, PartialEq, Hash)]
pub enum Graph {
    Promises,
    Repeated,
    Read,
    Written,
    StaleCompletion,
}
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum Weak {
    None,
    WorkerOnly,
    Written,
    CountPhysical,
    StaleCompletion,
    EraseVote,
}
#[derive(Clone, Copy, Debug, Eq, PartialEq, Hash)]
pub enum Step {
    Choose(Candidate),
    Admit,
    Write(usize),
    Sync(usize),
    Commit,
    Apply,
    Observe,
    Lose,
    Resolve,
    Cut(usize),
    Recover,
    Partition(u8),
    Repair,
    FinishChild,
    Parent,
    ConsumeHeld,
    ProbeEarly,
}
#[derive(Clone, Debug, Eq, PartialEq, Hash)]
pub struct State {
    pub graph: Graph,
    pub cursor: usize,
    pub selected: Option<Candidate>,
    pub cells: [Cell; 3],
    pub cache: [Option<Image>; 3],
    pub old_cache: [Option<Image>; 3],
    pub pending: Option<Pending>,
    pub held: Option<Ticket>,
    pub packets: Vec<Capsule>,
    pub releases: Vec<Release>,
    pub elections: Vec<Election>,
    pub grants: [[u8; 2]; 3],
    pub writes: Vec<Value>,
    pub reads: Vec<Read>,
    pub refused: u8,
    pub outage: Option<Outage>,
    pub terminal: bool,
}
impl State {
    pub fn new(graph: Graph) -> Self {
        Self {
            graph,
            cursor: 0,
            selected: None,
            cells: array::from_fn(|_| Cell::default()),
            cache: [None; 3],
            old_cache: [None; 3],
            pending: None,
            held: None,
            packets: Vec::new(),
            releases: Vec::new(),
            elections: Vec::new(),
            grants: [[0; 2]; 3],
            writes: Vec::new(),
            reads: Vec::new(),
            refused: 0,
            outage: None,
            terminal: false,
        }
    }
}
