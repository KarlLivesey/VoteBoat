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
use super::{program::*, types::*};
#[path = "engine/admission.rs"]
mod admission;
#[path = "engine/parent.rs"]
mod parent;
#[path = "engine/storage.rs"]
mod storage;
struct ResponseData {
    kind: Kind,
    term: u8,
    context: u8,
    candidate: Option<Candidate>,
    index: u8,
    entry: Option<Entry>,
}
impl State {
    pub fn steps(&self, weak: Weak) -> Vec<Step> {
        if self.terminal {
            return Vec::new();
        }
        let mut steps = Vec::new();
        if let Some(p) = self.pending {
            self.child_steps(p, &mut steps);
            if self.held.is_some() && matches!(self.job(), Job::Held(true)) {
                steps.push(Step::ConsumeHeld);
            }
            return steps;
        }
        match self.job() {
            Job::Choose => steps.extend([Step::Choose(Candidate::A), Step::Choose(Candidate::B)]),
            Job::Claim(..)
            | Job::Promise(..)
            | Job::Vote(..)
            | Job::Append(..)
            | Job::AppendAck(..)
            | Job::Commit(..)
            | Job::ReadAck(..)
            | Job::Held(true) => {
                steps.push(Step::Admit);
            }
            _ => steps.push(Step::Parent),
        }
        if self.held.is_some() && matches!(self.job(), Job::Held(false)) {
            steps = vec![Step::ConsumeHeld];
        }
        if self.outage.is_some_and(|o| !o.restored) {
            steps.push(Step::Repair);
        }
        if weak == Weak::WorkerOnly && matches!(self.job(), Job::ReadAck(_, true)) {
            steps.push(Step::Parent);
        }
        steps
    }
    fn child_steps(&self, p: Pending, steps: &mut Vec<Step>) {
        let cell = &self.cells[p.cell];
        let record = &cell.records[p.position as usize - 1];
        let live = self.live(p.cell);
        for physical in 0..3 {
            let store = cell.physical[physical];
            let bit = 1 << physical;
            if live & bit != 0 && store.written < p.position {
                steps.push(Step::Write(physical));
            }
            if live & bit != 0 && store.synced < store.written {
                steps.push(Step::Sync(physical));
            }
            if store.written > store.synced && p.cut == 0 {
                steps.push(Step::Cut(physical));
            }
        }
        if !record.committed
            && self.synced(p.cell, p.position) & live != 0
            && (self.synced(p.cell, p.position) & live).count_ones() >= 2
        {
            steps.push(Step::Commit);
        }
        if record.committed
            && !record.applied
            && cell.physical[record.ticket.physical as usize].synced >= p.position
        {
            steps.push(Step::Apply);
        }
        if record.applied && !p.observed && !p.lost {
            steps.extend([Step::Observe, Step::Lose]);
        }
        if record.applied
            && p.lost
            && !p.resolved
            && (self.synced(p.cell, p.position) & live).count_ones() >= 2
        {
            steps.push(Step::Resolve);
        }
        if record.applied && p.resolved && !p.observed {
            steps.push(Step::Observe);
        }
        if p.observed && cell.physical.iter().all(|s| s.synced >= p.position) {
            steps.push(Step::FinishChild);
        }
        let selected = cell.image.epoch.saturating_sub(1) as usize;
        if self.cache[p.cell].is_none()
            && cell.physical[selected].synced >= cell.applied
            && (self.synced(p.cell, cell.applied) & live).count_ones() >= 2
        {
            steps.push(Step::Recover);
        }
        if self.outage.is_none() {
            steps.extend([Step::Partition(1), Step::Partition(2), Step::Partition(4)]);
        }
        if self.outage.is_some_and(|o| !o.restored) {
            steps.push(Step::Repair);
        }
        if self.graph == Graph::Written
            && matches!(self.job(), Job::Append(0, 1, ..))
            && !p.probed
            && cell.physical.iter().any(|s| s.written >= p.position)
        {
            steps.push(Step::ProbeEarly);
        }
    }
    pub fn live(&self, cell: usize) -> u8 {
        self.outage
            .filter(|o| o.cell == cell && !o.restored)
            .map_or(7, |o| o.live)
    }
    pub fn synced(&self, cell: usize, position: u8) -> u8 {
        self.cells[cell]
            .physical
            .iter()
            .enumerate()
            .fold(0, |mask, (i, s)| {
                mask | if s.synced >= position { 1 << i } else { 0 }
            })
    }
    pub fn advance(&self, step: Step, weak: Weak) -> Self {
        assert!(
            self.steps(weak).contains(&step),
            "disabled {step:?} at {:?}",
            self.job()
        );
        let mut next = self.clone();
        match step {
            Step::Choose(candidate) => {
                next.selected = Some(candidate);
                next.cursor += 1;
            }
            Step::Admit => next.admit(weak),
            Step::Parent => next.parent(weak),
            Step::ConsumeHeld => next.consume_held(weak),
            Step::Repair if next.pending.is_none() => next.outage.as_mut().unwrap().restored = true,
            _ => next.storage(step, weak),
        }
        next
    }
}
