// SPDX-License-Identifier: RPL-1.5
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum Scenario {
    Prefix,
    Suffix,
    Snapshot,
}
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum Rule {
    Strict,
    MaximumPrefix,
    SingleControlCopy,
    ReuseOldGeneration,
    EarlySnapshot,
}
#[derive(Clone, Copy, Debug, Default, Eq, PartialEq, Hash)]
pub enum Phase {
    #[default]
    Absent,
    Written,
    Durable,
}
#[derive(Clone, Copy, Debug, Eq, PartialEq, Hash)]
pub enum Generation {
    Previous,
    Selected,
}
#[derive(Clone, Copy, Debug, Default, Eq, PartialEq, Hash)]
pub struct State {
    pub control: [Phase; 2],
    pub payload: [Phase; 3],
    pub previous_payload: [Phase; 3],
    pub snapshot: Phase,
    pub crashed: bool,
    pub lost: Option<usize>,
    pub promised: bool,
    pub released: u8,
    pub recovered: bool,
    pub recovered_prefix: u8,
    pub recovered_generations: [Option<Generation>; 3],
    pub quarantined: bool,
}
impl State {
    pub fn initial(scenario: Scenario) -> Self {
        Self {
            payload: if scenario == Scenario::Snapshot {
                [Phase::Durable; 3]
            } else {
                [Phase::Absent; 3]
            },
            previous_payload: if scenario == Scenario::Suffix {
                [Phase::Durable; 3]
            } else {
                [Phase::Absent; 3]
            },
            ..Self::default()
        }
    }
    pub fn prefix(self) -> u8 {
        self.payload
            .iter()
            .take_while(|p| **p == Phase::Durable)
            .count() as u8
    }
    pub fn has_control(self) -> bool {
        self.control.contains(&Phase::Durable)
    }
    pub fn mirrored(self) -> bool {
        self.control == [Phase::Durable; 2]
    }
    pub fn terminal(self, scenario: Scenario) -> bool {
        self.mirrored()
            && self.prefix() == 3
            && self.released == 3
            && self.promised
            && (scenario != Scenario::Snapshot || self.snapshot == Phase::Durable)
    }
}
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum Action {
    WritePayload(usize),
    SyncPayload(usize),
    WriteControl(usize),
    SyncControl(usize),
    WriteSnapshot,
    SyncSnapshot,
    ReleasePrefix,
    ReleaseVote,
    Crash,
    LoseDevice(usize),
    Recover,
}
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum Violation {
    PrefixHole,
    LostPromise,
    WrongGeneration,
    MissingSnapshot,
}
#[derive(Debug)]
pub struct Graph {
    pub states: usize,
    pub edges: usize,
    pub depth: usize,
    pub terminals: usize,
    pub frontier: usize,
}
#[derive(Debug)]
pub struct Failure {
    pub violation: Violation,
    pub original_trace: Vec<Action>,
}
