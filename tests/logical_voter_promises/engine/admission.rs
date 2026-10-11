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
    pub(super) fn admit(&mut self, _weak: Weak) {
        let (cell, body, epoch, effect) = match self.job() {
            Job::Claim(c, e) => (c, Body::Claim(e), e, None),
            Job::Promise(c, term, select) => {
                let candidate = select.get(self);
                let image = self.cells[c].image;
                let last = if term == 2 { (1, 1) } else { (0, 0) };
                if term < image.term
                    || (term == image.term && image.vote.is_some_and(|old| old != candidate))
                    || last < last_entry(image)
                {
                    self.refused |= 1;
                    self.cursor += 1;
                    return;
                }
                (
                    c,
                    Body::Promise {
                        term,
                        candidate,
                        last,
                    },
                    image.epoch,
                    None,
                )
            }
            Job::Append(c, index, value, term) => (
                c,
                Body::Append {
                    index,
                    entry: Entry { term, value },
                },
                self.cells[c].image.epoch,
                None,
            ),
            Job::Commit(c, index) => (c, Body::Commit(index), self.cells[c].image.epoch, None),
            Job::Vote(c, term, select, hold) => {
                let Some(prepared) = self.prepare_vote(c, term, select, hold) else {
                    return;
                };
                prepared
            }
            Job::AppendAck(c, index) => {
                let image = self.cells[c].image;
                let entry = image.entries[index as usize - 1].unwrap();
                let packet = self.capsule(
                    c,
                    image.epoch,
                    ResponseData {
                        kind: Kind::Append,
                        term: image.term,
                        context: 2 + index,
                        candidate: None,
                        index,
                        entry: Some(entry),
                    },
                );
                (c, Body::Capsule(packet), image.epoch, None)
            }
            Job::ReadAck(c, old) => {
                let Some(prepared) = self.prepare_read(c, old) else {
                    return;
                };
                prepared
            }
            Job::Held(true) => {
                let c = 0;
                let image = self.cells[c].image;
                let packet = self.capsule(
                    c,
                    image.epoch,
                    ResponseData {
                        kind: Kind::Vote,
                        term: 1,
                        context: 7,
                        candidate: Some(Candidate::A),
                        index: 0,
                        entry: None,
                    },
                );
                (c, Body::Capsule(packet), image.epoch, Some(1))
            }
            _ => panic!("not a child admission"),
        };
        self.enqueue(cell, body, epoch, effect);
    }
    fn enqueue(&mut self, cell: usize, body: Body, epoch: u8, effect: Option<u8>) {
        let position = self.cells[cell].records.len() as u8 + 1;
        assert!(position <= 48);
        let ticket = Ticket {
            cell: cell as u8,
            child_incarnation: 1,
            epoch,
            physical: epoch - 1,
            session: epoch,
            action: position,
            effect: effect.unwrap_or(position),
        };
        let body = if let Body::Capsule(mut packet) = body {
            packet.ticket = ticket;
            Body::Capsule(packet)
        } else {
            body
        };
        let expected_revision = self.cells[cell].image.revision;
        self.cells[cell].records.push(Record {
            ticket,
            body,
            expected_revision,
            committed: false,
            applied: false,
        });
        self.pending = Some(Pending {
            cell,
            position,
            observed: false,
            lost: false,
            resolved: false,
            cut: 0,
            probed: false,
            wait_owned: true,
        });
    }
    fn prepare_vote(
        &mut self,
        c: usize,
        term: u8,
        select: Select,
        hold: bool,
    ) -> Option<(usize, Body, u8, Option<u8>)> {
        let image = self.cells[c].image;
        let candidate = select.get(self);
        if image.term != term || image.vote != Some(candidate) {
            self.refused |= 1;
            self.cursor += 1;
            return None;
        }
        let packet = self.capsule(
            c,
            image.epoch,
            ResponseData {
                kind: Kind::Vote,
                term,
                context: term,
                candidate: Some(candidate),
                index: 0,
                entry: None,
            },
        );
        Some((c, Body::Capsule(packet), image.epoch, hold.then_some(1)))
    }
    fn prepare_read(&mut self, c: usize, old: bool) -> Option<(usize, Body, u8, Option<u8>)> {
        let image = if old {
            self.old_cache[c]
        } else {
            Some(self.cells[c].image)
        };
        let Some(image) = image else {
            self.refused |= 2;
            self.cursor += 1;
            return None;
        };
        if image.epoch != self.cells[c].image.epoch || image.term != self.cells[c].image.term {
            self.refused |= 2;
            self.cursor += 1;
            return None;
        }
        let index = if old { 1 } else { 2 };
        let packet = self.capsule(
            c,
            image.epoch,
            ResponseData {
                kind: Kind::Read,
                term: image.term,
                context: if old { 5 } else { 6 },
                candidate: None,
                index,
                entry: image.entries[index as usize - 1],
            },
        );
        Some((c, Body::Capsule(packet), image.epoch, None))
    }
    pub(super) fn capsule(&self, cell: usize, epoch: u8, data: ResponseData) -> Capsule {
        let ResponseData {
            kind,
            term,
            context,
            candidate,
            index,
            entry,
        } = data;
        Capsule {
            ticket: Ticket {
                cell: cell as u8,
                child_incarnation: 1,
                epoch,
                physical: epoch - 1,
                session: epoch,
                action: 0,
                effect: 0,
            },
            parent_incarnation: 1,
            logical_store: cell as u8 + 10,
            configuration: 1,
            requester: candidate.map_or(term, |c| c.bit()),
            requester_session: 1,
            term,
            context,
            kind,
            candidate,
            index,
            entry,
        }
    }
}
fn last_entry(image: Image) -> (u8, u8) {
    image
        .entries
        .iter()
        .enumerate()
        .rev()
        .find_map(|(i, e)| e.map(|entry| (entry.term, i as u8 + 1)))
        .unwrap_or((0, 0))
}
