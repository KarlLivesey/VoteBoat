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
//! Independent bounded physical-voter relay state; no production capabilities.
#[derive(Clone, Copy, Debug, Eq, PartialEq, Hash)]
pub enum Kind {
    Election,
    Append,
    Read,
}
#[derive(Clone, Copy, Debug, Eq, PartialEq, Hash)]
pub enum Profile {
    Election,
    Freshness,
    Append,
    Read,
    Configuration,
    Group,
    Store,
}
impl Profile {
    pub fn kind(self) -> Kind {
        match self {
            Self::Election | Self::Freshness => Kind::Election,
            Self::Append | Self::Group | Self::Store => Kind::Append,
            _ => Kind::Read,
        }
    }
    pub fn context(self, epoch: u8) -> Context {
        let kind = self.kind();
        Context {
            kind,
            group: 800,
            incarnation: 1 + u8::from(self == Self::Group && epoch == 1),
            term: if kind == Kind::Election {
                4 + u8::from(self == Self::Election && epoch == 1)
            } else {
                2
            },
            configuration: 1 + u8::from(
                matches!(self, Self::Configuration | Self::Store) && epoch == 1,
            ),
            request: 100 + u8::from(matches!(self, Self::Read | Self::Freshness) && epoch == 1),
            prefix: if self == Self::Append { 1 + epoch } else { 1 },
            last_term: 2,
            candidate: 1,
        }
    }
}
#[derive(Clone, Copy, Debug, Eq, PartialEq, Hash)]
pub struct Physical {
    pub node: u8,
    pub store: u16,
    pub incarnation: u8,
}
pub const RELAY: Physical = Physical {
    node: 9,
    store: 900,
    incarnation: 1,
};
#[derive(Clone, Copy, Debug, Eq, PartialEq, Hash)]
pub struct Context {
    pub kind: Kind,
    pub group: u16,
    pub incarnation: u8,
    pub term: u8,
    pub configuration: u8,
    pub request: u8,
    pub prefix: u8,
    pub last_term: u8,
    pub candidate: u8,
}
#[derive(Clone, Copy, Debug, Default, Eq, PartialEq, Hash)]
pub struct Image {
    pub group_incarnation: u8,
    pub term: u8,
    pub configuration: u8,
    pub vote: u8,
    pub entries: [u8; 3],
    pub terms: [u8; 3],
    pub prefix: u8,
}
#[derive(Clone, Copy, Debug, Eq, PartialEq, Hash)]
pub enum Io {
    Idle,
    Accepted,
    Written,
    Synced,
    Emitted,
}
#[derive(Clone, Copy, Debug, Eq, PartialEq, Hash)]
pub struct Actor {
    pub physical: Physical,
    pub synced: Image,
    pub written: Image,
    pub before: Image,
    pub prepared: Image,
    pub io: Io,
    pub granted: bool,
    pub crashed: bool,
}
#[derive(Clone, Copy, Debug, Eq, PartialEq, Hash)]
pub struct Original {
    pub physical: Physical,
    pub context: Context,
    pub granted: bool,
    pub before: Image,
    pub reported: Image,
    pub actual_synced: Image,
}
#[derive(Clone, Copy, Debug, Eq, PartialEq, Hash)]
pub struct ParentRequest {
    pub physical: Physical,
    pub context: Context,
    pub local_synced: Image,
}
#[derive(Clone, Copy, Debug, Eq, PartialEq, Hash)]
pub struct RequestTicket {
    pub relay: Physical,
    pub generation: u8,
    pub original: ParentRequest,
}
#[derive(Clone, Copy, Debug, Eq, PartialEq, Hash)]
pub struct Ticket {
    pub generation: u8,
    pub sequence: u8,
}
#[derive(Clone, Copy, Debug, Eq, PartialEq, Hash)]
pub struct Packet {
    pub relay: Physical,
    pub ticket: Ticket,
    pub ids: [Option<u8>; 2],
    pub terminal: bool,
}
#[derive(Clone, Debug, Eq, PartialEq, Hash)]
pub struct State {
    pub epoch: u8,
    pub actors: [Actor; 3],
    pub originals: [Option<Original>; 6],
    pub requests: [Option<ParentRequest>; 2],
    pub request_ticket: Option<RequestTicket>,
    pub retired: [bool; 2],
    pub queue: Vec<u8>,
    pub packet: Option<Packet>,
    pub obsolete: Option<Packet>,
    pub credit: Option<Ticket>,
    pub generation: u8,
    pub transmitted: u8,
    pub wire_history: u8,
    pub sequence: u8,
    pub restarted: bool,
    pub stalled: bool,
    pub claims: Vec<u8>,
    pub released: [bool; 2],
}
impl State {
    pub fn initial(profile: Profile) -> Self {
        let actors = std::array::from_fn(|i| {
            let mut image = Image {
                group_incarnation: 1,
                term: if profile.kind() == Kind::Election {
                    3
                } else {
                    2
                },
                configuration: 1,
                entries: [11, 0, 0],
                terms: [2, 0, 0],
                prefix: 1,
                ..Image::default()
            };
            if profile == Profile::Freshness && i == 2 {
                image.entries[1] = 99;
                image.terms[1] = 3;
                image.prefix = 2;
            }
            Actor {
                physical: Physical {
                    node: i as u8 + 1,
                    store: 100 + i as u16,
                    incarnation: 1,
                },
                synced: image,
                written: image,
                before: image,
                prepared: image,
                io: Io::Idle,
                granted: false,
                crashed: false,
            }
        });
        Self {
            epoch: 0,
            actors,
            originals: [None; 6],
            requests: [None; 2],
            request_ticket: None,
            retired: [false; 2],
            queue: Vec::new(),
            packet: None,
            obsolete: None,
            credit: None,
            generation: 1,
            transmitted: 0,
            wire_history: 0,
            sequence: 0,
            restarted: false,
            stalled: false,
            claims: Vec::new(),
            released: [false; 2],
        }
    }
    pub fn expected_physical(&self, profile: Profile, i: usize) -> Physical {
        Physical {
            node: i as u8 + 1,
            store: 100 + i as u16,
            incarnation: 1 + u8::from(profile == Profile::Store && self.epoch == 1 && i == 1),
        }
    }
    pub fn terminal(&self) -> bool {
        self.released[1]
            && self.retired[1]
            && self.request_ticket.is_none()
            && self.queue.is_empty()
            && self.packet.is_none()
            && self.obsolete.is_none()
            && self.actors.iter().all(|a| a.io == Io::Emitted)
    }
}
#[derive(Clone, Copy, Debug, Eq, PartialEq, Hash)]
pub enum Rule {
    Strict,
    RelayVote,
    DuplicateVote,
    WrittenAck,
    ScopeOmitted,
    FreshnessOmitted,
    OldCompletion,
}
#[derive(Clone, Copy, Debug, Eq, PartialEq, Hash)]
pub enum Action {
    IssueRequest,
    ConsumeRequest,
    ReturnRequest,
    RetireRequest,
    Receive(u8),
    Write(u8),
    Sync(u8),
    Emit(u8),
    SourceCrash(u8),
    Capture(u8),
    Backpressure(u8),
    Cancel(u8),
    Export,
    Deliver,
    Lose,
    Complete,
    Replay(u8),
    RestartRelay,
    DeliverOld,
    CompleteOld,
    DiscardOld,
    Stall,
    Resume,
    Release,
    Next,
}
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum Violation {
    Ownership,
    ImaginaryVoter,
    DuplicateVoter,
    Scope,
    Store,
    WrittenBeforeSync,
    StaleCandidate,
    MissingPrefix,
    MissingLocalPersistence,
}
#[derive(Debug, Default)]
pub struct Graph {
    pub requests_issued: usize,
    pub requests_consumed: usize,
    pub requests_returned: usize,
    pub requests_retired: usize,
    pub projected_collisions: usize,
    pub states: usize,
    pub edges: usize,
    pub depth: usize,
    pub frontier: usize,
    pub terminals: usize,
    pub loss: usize,
    pub replay: usize,
    pub relay_restart: usize,
    pub source_crash: usize,
    pub cancelled: usize,
    pub backpressure: usize,
    pub stall: usize,
}
#[derive(Debug)]
pub struct Failure {
    pub violation: Violation,
    pub trace: Vec<Action>,
}
