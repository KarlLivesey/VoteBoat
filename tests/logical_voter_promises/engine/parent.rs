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
    pub(super) fn parent(&mut self, weak: Weak) {
        match self.job() {
            Job::Election(term, select, probe) => {
                self.elect(term, select, probe, weak);
            }
            Job::Dedup => {
                self.packets
                    .dedup_by_key(|p| (p.ticket.cell, p.kind, p.term, p.context));
                if self.graph == Graph::Promises {
                    self.packets.clear();
                }
            }
            Job::Ack(value) => {
                let image = self.cache[0].unwrap();
                assert_eq!(
                    image.entries[image.commit as usize - 1].unwrap().value,
                    value
                );
                let logical: u8 = self
                    .packets
                    .iter()
                    .filter(|p| {
                        p.kind == Kind::Append
                            && p.index == image.commit
                            && p.entry == image.entries[image.commit as usize - 1]
                    })
                    .fold(0, |mask, p| mask | 1 << p.ticket.cell);
                assert!(logical.count_ones() >= 2);
                self.writes.push(value);
                self.packets.clear();
            }
            Job::Invoke(old) => self.reads.push(Read {
                old,
                invoked_after: self.writes.last().copied(),
                completed: None,
                evidence: 0,
            }),
            Job::Read(old) => {
                let context = if old { 5 } else { 6 };
                let expected = if old { Value::X } else { Value::Y };
                let logical: u8 = self
                    .packets
                    .iter()
                    .filter(|p| {
                        p.kind == Kind::Read
                            && p.context == context
                            && p.entry.is_some_and(|e| e.value == expected)
                    })
                    .fold(0, |mask, p| mask | 1 << p.ticket.cell);
                if logical.count_ones() >= 2 {
                    let read = self.reads.iter_mut().find(|r| r.old == old).unwrap();
                    read.completed = Some(expected);
                    read.evidence = logical;
                } else {
                    assert!(old);
                    self.refused |= 16;
                }
                self.packets.clear();
            }
            Job::ReadAck(cell, true) if weak == Weak::WorkerOnly => {
                if let Some(image) = self.old_cache[cell] {
                    let packet = self.capsule(
                        cell,
                        image.epoch,
                        ResponseData {
                            kind: Kind::Read,
                            term: image.term,
                            context: 5,
                            candidate: None,
                            index: 1,
                            entry: image.entries[0],
                        },
                    );
                    self.packets.push(packet);
                    assert!(self.packets.len() <= 2);
                } else {
                    self.refused |= 2;
                }
            }
            Job::Held(false) => {
                assert!(self.held.is_none());
                self.packets.clear();
            }
            Job::End => {
                assert!(self.pending.is_none() && self.held.is_none());
                self.packets.clear();
                self.terminal = true;
                return;
            }
            _ => panic!("not a parent step"),
        }
        self.cursor += 1;
    }
    fn elect(&mut self, term: u8, select: Select, probe: bool, weak: Weak) {
        let candidate = select.get(self);
        let packets = self
            .packets
            .iter()
            .filter(|p| {
                p.kind == Kind::Vote
                    && p.term == term
                    && p.context == term
                    && p.candidate == Some(candidate)
            })
            .copied()
            .collect::<Vec<_>>();
        let logical: u8 = packets.iter().fold(0, |mask, p| mask | 1 << p.ticket.cell);
        let counted = if weak == Weak::CountPhysical {
            packets
                .iter()
                .map(|p| (p.ticket.cell, p.ticket.epoch))
                .collect::<std::collections::HashSet<_>>()
                .len() as u8
        } else {
            logical.count_ones() as u8
        };
        if counted >= 2 {
            self.elections.push(Election {
                term,
                candidate,
                logical,
                counted,
            });
            self.packets.clear();
        } else {
            assert!(probe);
            self.refused |= 8;
        }
    }
}
