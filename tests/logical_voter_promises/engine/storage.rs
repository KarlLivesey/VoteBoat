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
use super::*;
impl State {
    pub(super) fn storage(&mut self, step: Step, weak: Weak) {
        let mut pending = self.pending.unwrap();
        let cell = pending.cell;
        let position = pending.position;
        match step {
            Step::Write(physical) => self.cells[cell].physical[physical].written = position,
            Step::Sync(physical) => {
                self.cells[cell].physical[physical].synced =
                    self.cells[cell].physical[physical].written
            }
            Step::Commit => {
                self.cells[cell].records[position as usize - 1].committed = true;
                self.cells[cell].committed = position;
            }
            Step::Apply => self.apply_child(pending, weak),
            Step::Observe => self.observe(&mut pending),
            Step::Lose => pending.lost = true,
            Step::Resolve => pending.resolved = true,
            Step::Cut(physical) => {
                pending.cut |= 1 << physical;
                self.cells[cell].physical[physical].written =
                    self.cells[cell].physical[physical].synced;
                if physical + 1 == self.cells[cell].image.epoch as usize {
                    self.cache[cell] = None;
                }
                if physical == 0 && self.cells[cell].image.epoch == 2 {
                    self.old_cache[cell] = None;
                }
            }
            Step::Recover => self.cache[cell] = Some(self.recovered(cell)),
            Step::Partition(live) => {
                self.outage = Some(Outage {
                    cell,
                    live,
                    restored: false,
                })
            }
            Step::Repair => self.outage.as_mut().unwrap().restored = true,
            Step::FinishChild => {
                self.pending = None;
                self.cursor += 1;
                return;
            }
            Step::ProbeEarly => {
                pending.probed = true;
                if weak == Weak::Written && !self.cells[cell].records[position as usize - 1].applied
                {
                    let ticket = self.cells[cell].records[position as usize - 1].ticket;
                    self.release(ticket, ticket);
                } else if !self.cells[cell].records[position as usize - 1].applied {
                    self.refused |= 4;
                }
            }
            _ => panic!("not a storage step"),
        }
        self.pending = Some(pending);
    }
    fn apply_child(&mut self, pending: Pending, weak: Weak) {
        let cell = pending.cell;
        let record = self.cells[cell].records[pending.position as usize - 1].clone();
        let mut image = self.cells[cell].image;
        assert_eq!(record.expected_revision, image.revision);
        match record.body {
            Body::Claim(epoch) => {
                assert_eq!(epoch, image.epoch + 1);
                if epoch == 2 {
                    self.old_cache[cell] = self.cache[cell];
                }
                image.epoch = epoch;
                if weak == Weak::EraseVote && epoch == 2 {
                    image.vote = None;
                }
            }
            Body::Promise {
                term, candidate, ..
            } => {
                image.term = term;
                image.vote = Some(candidate);
                self.grants[cell][term as usize - 1] |= candidate.bit();
            }
            Body::Append { index, entry } => {
                assert_eq!(image.term, entry.term);
                assert!(index == 1 || image.entries[index as usize - 2].is_some());
                image.entries[index as usize - 1] = Some(entry);
            }
            Body::Commit(index) => {
                assert!(image.entries[index as usize - 1].is_some());
                image.commit = index;
            }
            Body::Capsule(_) => {}
        }
        image.revision += 1;
        self.cells[cell].image = image;
        self.cells[cell].applied = pending.position;
        self.cells[cell].records[pending.position as usize - 1].applied = true;
    }
    fn observe(&mut self, pending: &mut Pending) {
        let cell = pending.cell;
        let record = self.cells[cell].records[pending.position as usize - 1].clone();
        self.cache[cell] = Some(self.cells[cell].image);
        if matches!(self.job(), Job::Vote(_, _, _, true)) {
            self.held = Some(record.ticket);
        } else {
            if let Body::Capsule(packet) = record.body {
                self.packets.push(packet);
                assert!(self.packets.len() <= 2);
            }
            self.release(record.ticket, record.ticket);
        }
        pending.observed = true;
        pending.wait_owned = false;
    }
    pub(super) fn release(&mut self, expected: Ticket, source: Ticket) {
        let cell = source.cell as usize;
        self.releases.push(Release {
            expected,
            source,
            child_commit: self.cells[cell].committed,
            child_apply: self.cells[cell].applied,
        });
    }
    pub(super) fn consume_held(&mut self, weak: Weak) {
        let source = self.held.take().unwrap();
        let expected = if let (Weak::StaleCompletion, Some(pending)) = (weak, self.pending.as_mut())
        {
            pending.wait_owned = false;
            self.cells[pending.cell].records[pending.position as usize - 1].ticket
        } else {
            source
        };
        self.release(expected, source);
    }
}

impl State {
    fn recovered(&self, cell: usize) -> Image {
        let authority = &self.cells[cell];
        let selected = authority.image.epoch.saturating_sub(1) as usize;
        let own = authority.physical[selected].synced;
        assert!(own >= authority.applied);
        let mut image = Image::default();
        for record in authority.records.iter().take(authority.applied as usize) {
            assert!(record.committed && record.applied);
            match record.body {
                Body::Claim(epoch) => image.epoch = epoch,
                Body::Promise {
                    term, candidate, ..
                } => {
                    image.term = term;
                    image.vote = Some(candidate);
                }
                Body::Append { index, entry } => image.entries[index as usize - 1] = Some(entry),
                Body::Commit(index) => image.commit = index,
                Body::Capsule(_) => {}
            }
            image.revision += 1;
        }
        image
    }
}
