// SPDX-License-Identifier: RPL-1.5
//! Independent application-level transition model; ordinary safe group commit is assumed.
#[derive(Clone, Copy, Debug, Eq, Hash, PartialEq)]
pub enum Mode {
    Strict,
    SkipVersion,
    PartialPrepare,
    EarlyRelease,
    TimeoutAbort,
}
#[derive(Clone, Copy, Debug, Eq, Hash, PartialEq)]
pub enum Decision {
    Commit,
    Abort,
}
#[derive(Clone, Copy, Debug, Eq, Hash, PartialEq)]
pub enum Step {
    Start,
    Prepare(u8),
    Decide,
    Apply(u8),
    All,
    Release(u8),
    Finish,
    Done,
}
impl Step {
    pub fn retained_slot(self) -> usize {
        match self {
            Self::Start => 0,
            Self::Prepare(p) => 1 + usize::from(p),
            Self::Decide => 4,
            Self::Apply(p) => 5 + usize::from(p),
            Self::All => 8,
            Self::Release(p) => 9 + usize::from(p),
            Self::Finish => 12,
            Self::Done => panic!("terminal state has no proposed phase"),
        }
    }
}
#[derive(Clone, Copy, Debug, Eq, Hash, PartialEq)]
pub struct Key {
    pub value: u8,
    pub version: u8,
    pub lock: Option<u8>,
}
#[derive(Clone, Copy, Debug, Default, Eq, Hash, PartialEq)]
pub struct Participant {
    pub prepare: Option<bool>,
    pub applied: Option<Decision>,
    pub released: bool,
}
#[derive(Clone, Copy, Debug, Eq, Hash, PartialEq)]
pub struct Pending {
    pub original: Step,
    pub delivered: bool,
    pub result: Step,
}
#[derive(Clone, Copy, Debug, Eq, Hash, PartialEq)]
pub struct Transaction {
    pub stage: Step,
    pub participants: [Participant; 3],
    pub decision: Option<Decision>,
    pub all: bool,
    pub finished: bool,
    pub pending: Option<Pending>,
    /// Authoritative committed phase results, distinct from local reply slots.
    pub retained: [Option<Step>; 13],
}
#[derive(Clone, Copy, Debug, Eq, Hash, PartialEq)]
pub struct State {
    pub keys: [[Key; 2]; 3],
    pub tx: [Transaction; 2],
    pub mode: Mode,
}
impl State {
    pub fn new(mode: Mode) -> Self {
        let key = Key {
            value: 1,
            version: 0,
            lock: None,
        };
        let tx = Transaction {
            stage: Step::Start,
            participants: [Participant::default(); 3],
            decision: None,
            all: false,
            finished: false,
            pending: None,
            retained: [None; 13],
        };
        Self {
            keys: [[key; 2]; 3],
            tx: [tx; 2],
            mode,
        }
    }
}
#[derive(Clone, Copy, Debug, Eq, Hash, PartialEq)]
pub enum Action {
    Issue(u8),
    Deliver(u8),
    Observe(u8),
    LoseThenResolve(u8),
    Timeout(u8),
}
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum Violation {
    PartialFootprint,
    UnpreparedCommit,
    SplitDecision,
    EarlyVisibility,
    DuplicateWrite,
    ReadDependencyCycle,
}
