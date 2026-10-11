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
//! Committed/applied command abstraction; network slots are volatile and lossy.
pub const AMOUNTS: [i16; 2] = [5, 7];
#[derive(Clone, Copy, Debug, Eq, Hash, PartialEq)]
pub enum Decision {
    Commit,
    Abort,
}
impl Decision {
    pub fn opposite(self) -> Self {
        match self {
            Self::Commit => Self::Abort,
            Self::Abort => Self::Commit,
        }
    }
}
#[derive(Clone, Copy, Debug, Eq, Hash, PartialEq)]
pub enum CoordinatorPhase {
    Started,
    Decided(Decision),
    AppliedAll(Decision),
    Finished(Decision),
}
impl CoordinatorPhase {
    pub fn decision(self) -> Option<Decision> {
        match self {
            Self::Started => None,
            Self::Decided(d) | Self::AppliedAll(d) | Self::Finished(d) => Some(d),
        }
    }
    pub fn all(self) -> bool {
        matches!(self, Self::AppliedAll(_) | Self::Finished(_))
    }
}
#[derive(Clone, Copy, Debug, Eq, Hash, PartialEq)]
pub enum ParticipantPhase {
    Absent,
    Prepared,
    DecisionApplied(Decision),
    Released(Decision),
}
#[derive(Clone, Copy, Debug, Eq, Hash, PartialEq)]
pub struct Coordinator {
    pub phase: CoordinatorPhase,
    pub first: Option<Decision>,
    pub prepared: u8,
    pub refused: u8,
    pub applied: u8,
    pub released: u8,
}
#[derive(Clone, Copy, Debug, Default, Eq, Hash, PartialEq)]
pub struct Observations {
    pub prepared: u8,
    pub refused: u8,
    pub applied: u8,
    pub released: u8,
}
#[derive(Clone, Copy, Debug, Eq, Hash, PartialEq)]
pub struct Participant {
    pub phase: ParticipantPhase,
    pub prepared: bool,
    pub refused: bool,
    pub before: i16,
    pub after: i16,
}
#[derive(Clone, Copy, Debug, Eq, Hash, PartialEq)]
pub enum Message {
    Prepare(usize, usize),
    Prepared(usize, usize),
    Refused(usize, usize),
    Decide(usize, Decision),
    Apply(usize, usize, Decision),
    Applied(usize, usize, Decision),
    All(usize, Decision),
    Release(usize, usize, Decision),
    Released(usize, usize, Decision),
    Finish(usize, Decision),
}
impl Message {
    pub fn transaction(self) -> usize {
        match self {
            Self::Prepare(t, _)
            | Self::Prepared(t, _)
            | Self::Refused(t, _)
            | Self::Decide(t, _)
            | Self::Apply(t, _, _)
            | Self::Applied(t, _, _)
            | Self::All(t, _)
            | Self::Release(t, _, _)
            | Self::Released(t, _, _)
            | Self::Finish(t, _) => t,
        }
    }
}
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum Weakening {
    None,
    PrematureRelease,
    TimeoutAbort,
    UnstableDecision,
}
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum Step {
    Send(Message),
    Deliver(usize),
    Lose(usize),
    Restart,
    Timeout(usize, usize),
}
#[derive(Clone, Debug, Eq, Hash, PartialEq)]
pub struct State {
    pub coordinator: [Coordinator; 2],
    pub observed: [Observations; 2],
    pub participant: [[Participant; 2]; 2],
    pub balances: [i16; 2],
    pub locks: [Option<usize>; 2],
    pub wire: [Option<Message>; 2],
    pub initial: i16,
}
impl State {
    pub fn new(initial: i16) -> Self {
        Self {
            coordinator: [Coordinator {
                phase: CoordinatorPhase::Started,
                first: None,
                prepared: 0,
                refused: 0,
                applied: 0,
                released: 0,
            }; 2],
            observed: [Observations::default(); 2],
            participant: [[Participant {
                phase: ParticipantPhase::Absent,
                prepared: false,
                refused: false,
                before: 0,
                after: 0,
            }; 2]; 2],
            balances: [initial, 0],
            locks: [None; 2],
            wire: [None; 2],
            initial,
        }
    }
    pub fn read(&self, account: usize) -> Option<i16> {
        self.locks[account]
            .is_none()
            .then_some(self.balances[account])
    }
    pub fn steps(&self, weak: Weakening) -> Vec<Step> {
        let mut steps = Vec::new();
        for t in 0..2 {
            if self.wire[t].is_some() {
                steps.extend([Step::Deliver(t), Step::Lose(t)]);
            } else {
                steps.extend(self.messages(t, weak).into_iter().map(Step::Send));
            }
            for p in 0..2 {
                if self.participant[t][p].phase == ParticipantPhase::Prepared {
                    steps.push(Step::Timeout(t, p));
                }
            }
        }
        if self.wire.iter().any(Option::is_some) || self.observed != [Observations::default(); 2] {
            steps.push(Step::Restart);
        }
        steps
    }
    fn messages(&self, t: usize, weak: Weakening) -> Vec<Message> {
        let mut messages = Vec::new();
        let c = self.coordinator[t];
        let observed = self.observed[t];
        for p in 0..2 {
            let participant = self.participant[t][p];
            if c.phase == CoordinatorPhase::Started
                && participant.phase == ParticipantPhase::Absent
                && !participant.refused
                && (p == 0 || observed.prepared & 1 != 0)
            {
                messages.push(Message::Prepare(t, p));
            }
            if participant.prepared {
                messages.push(Message::Prepared(t, p));
            }
            if participant.refused {
                messages.push(Message::Refused(t, p));
            }
            if let ParticipantPhase::DecisionApplied(d) | ParticipantPhase::Released(d) =
                participant.phase
            {
                messages.push(Message::Applied(t, p, d));
            }
            if let ParticipantPhase::Released(d) = participant.phase {
                messages.push(Message::Released(t, p, d));
            }
        }
        if c.phase == CoordinatorPhase::Started {
            if observed.prepared == 3 {
                messages.push(Message::Decide(t, Decision::Commit));
            }
            if observed.refused != 0 {
                messages.push(Message::Decide(t, Decision::Abort));
            }
        }
        if let Some(d) = c.phase.decision() {
            for p in 0..2 {
                messages.push(Message::Apply(t, p, d));
                if c.phase.all() || weak == Weakening::PrematureRelease {
                    messages.push(Message::Release(t, p, d));
                }
            }
            if observed.applied == 3 && matches!(c.phase, CoordinatorPhase::Decided(_)) {
                messages.push(Message::All(t, d));
            }
            if observed.released == 3 && matches!(c.phase, CoordinatorPhase::AppliedAll(_)) {
                messages.push(Message::Finish(t, d));
            }
            if weak == Weakening::UnstableDecision {
                messages.push(Message::Decide(t, d.opposite()));
            }
        }
        messages
    }
    pub fn advance(&self, step: Step, weak: Weakening) -> Self {
        assert!(self.steps(weak).contains(&step), "unavailable {step:?}");
        let mut next = self.clone();
        match step {
            Step::Send(message) => next.wire[message.transaction()] = Some(message),
            Step::Deliver(t) => {
                let message = next.wire[t].take().unwrap();
                next.deliver(message, weak);
            }
            Step::Lose(t) => next.wire[t] = None,
            Step::Restart => {
                next.wire = [None; 2];
                next.observed = [Observations::default(); 2];
            }
            Step::Timeout(t, p) if weak == Weakening::TimeoutAbort => {
                next.participant[t][p].phase = ParticipantPhase::DecisionApplied(Decision::Abort);
                next.locks[p] = None;
            }
            Step::Timeout(_, _) => {}
        }
        next
    }
    fn prepare(&mut self, t: usize, p: usize) {
        if self.coordinator[t].phase != CoordinatorPhase::Started
            || self.participant[t][p].phase != ParticipantPhase::Absent
            || self.participant[t][p].refused
        {
            return;
        }
        let before = self.balances[p];
        let after = if p == 0 {
            before.checked_sub(AMOUNTS[t]).filter(|v| *v >= 0)
        } else {
            before.checked_add(AMOUNTS[t])
        };
        if self.locks[p].is_some() || after.is_none() {
            self.participant[t][p].refused = true;
            return;
        }
        self.locks[p] = Some(t);
        self.participant[t][p] = Participant {
            phase: ParticipantPhase::Prepared,
            prepared: true,
            refused: false,
            before,
            after: after.unwrap(),
        };
    }
    fn apply(&mut self, t: usize, p: usize, d: Decision) {
        if self.coordinator[t].phase.decision() != Some(d) {
            return;
        }
        let participant = &mut self.participant[t][p];
        if !matches!(
            participant.phase,
            ParticipantPhase::Absent | ParticipantPhase::Prepared
        ) {
            return;
        }
        if d == Decision::Commit {
            if !participant.prepared {
                return;
            }
            self.balances[p] = participant.after;
        }
        participant.phase = ParticipantPhase::DecisionApplied(d);
    }
    fn deliver(&mut self, message: Message, weak: Weakening) {
        let t = message.transaction();
        match message {
            Message::Prepare(_, p) => self.prepare(t, p),
            Message::Prepared(_, p) if self.coordinator[t].phase == CoordinatorPhase::Started => {
                self.observed[t].prepared |= 1 << p
            }
            Message::Refused(_, p) if self.coordinator[t].phase == CoordinatorPhase::Started => {
                self.observed[t].refused |= 1 << p
            }
            Message::Decide(_, d) => {
                let c = &mut self.coordinator[t];
                if c.phase == CoordinatorPhase::Started || weak == Weakening::UnstableDecision {
                    c.first.get_or_insert(d);
                    c.phase = CoordinatorPhase::Decided(d);
                    c.prepared = self.observed[t].prepared;
                    c.refused = self.observed[t].refused;
                    self.observed[t] = Observations::default();
                }
            }
            Message::Apply(_, p, d) => self.apply(t, p, d),
            Message::Applied(_, p, d)
                if self.coordinator[t].phase == CoordinatorPhase::Decided(d) =>
            {
                self.observed[t].applied |= 1 << p
            }
            Message::All(_, d)
                if self.observed[t].applied == 3
                    && self.coordinator[t].phase == CoordinatorPhase::Decided(d) =>
            {
                self.coordinator[t].phase = CoordinatorPhase::AppliedAll(d);
                self.coordinator[t].applied = self.observed[t].applied;
                self.observed[t] = Observations::default();
            }
            Message::Release(_, p, d)
                if self.coordinator[t].phase.all() || weak == Weakening::PrematureRelease =>
            {
                if self.participant[t][p].phase == ParticipantPhase::DecisionApplied(d) {
                    self.participant[t][p].phase = ParticipantPhase::Released(d);
                    if self.locks[p] == Some(t) {
                        self.locks[p] = None;
                    }
                }
            }
            Message::Released(_, p, d)
                if self.coordinator[t].phase == CoordinatorPhase::AppliedAll(d) =>
            {
                self.observed[t].released |= 1 << p
            }
            Message::Finish(_, d)
                if self.observed[t].released == 3
                    && self.coordinator[t].phase == CoordinatorPhase::AppliedAll(d) =>
            {
                self.coordinator[t].phase = CoordinatorPhase::Finished(d);
                self.coordinator[t].released = self.observed[t].released;
                self.observed[t] = Observations::default();
            }
            _ => {}
        }
    }
}
