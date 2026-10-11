// SPDX-License-Identifier: RPL-1.5
// Copyright (c) 2026 Karl Livesey
use super::{image, types::*};
#[derive(Clone, Debug, Eq, PartialEq, Hash)]
pub struct Model {
    pub links: [bool; 3],
    pub stores: [Physical; 3],
    pub other: [CellWitness; 2],
    pub admitted: Vec<Record>,
    pub committed: ChildIndex,
    pub applied: ChildIndex,
    pub image: Image,
    pub output: Option<Completion>,
    pub observed: Option<Completion>,
    pub quorum: Option<ChildIndex>,
    pub recovered: [Option<ParentBytes>; 2],
    pub power: bool,
    pub process: bool,
    pub acknowledged: [bool; 2],
    pub served: bool,
}
impl Model {
    pub fn initial() -> Self {
        let other = [1, 2].map(|logical| {
            let mut cell = CellWitness {
                logical,
                written: std::array::from_fn(|_| Vec::new()),
                synced: std::array::from_fn(|_| Vec::new()),
                commit: ChildIndex(0),
                applied: ChildIndex(0),
            };
            // Executed fixed prelude, not fabricated commit flags: exact ordered writes,
            // each physical sync, matching majority commit, then child application.
            for physical in 0..2 {
                for tag in [Tag::X, Tag::Y] {
                    cell.written[physical].push(Entry::exact(tag));
                    cell.synced[physical] = cell.written[physical].clone();
                }
            }
            assert_eq!(cell.synced[0], cell.synced[1]);
            cell.commit = ChildIndex(cell.synced[0].len());
            cell.applied = cell.commit;
            cell
        });
        Self {
            links: [true; 3],
            stores: std::array::from_fn(|_| Physical::default()),
            other,
            admitted: Vec::new(),
            committed: ChildIndex(0),
            applied: ChildIndex(0),
            image: Image::default(),
            output: None,
            observed: None,
            quorum: None,
            recovered: [None, None],
            power: false,
            process: false,
            acknowledged: [false; 2],
            served: false,
        }
    }
    pub fn record(&self, kind: Kind, weak: Weakening) -> Record {
        let body = if kind == Kind::FillX
            || (kind == Kind::AppendX && weak != Weakening::MissingPayload)
        {
            Some([88, 0, 1])
        } else {
            None
        };
        Record {
            header: Header {
                operation: kind as u8,
                epoch: self.image.epoch,
                generation: self.image.generation,
                revision: self.image.revision,
            },
            kind,
            body,
        }
    }
    pub fn admit_kind(&mut self, kind: Kind, weak: Weakening) -> bool {
        if kind == Kind::Meta0 && !self.acknowledged[0]
            || kind == Kind::Meta1 && !self.acknowledged[1]
        {
            return false;
        }
        if self.applied.0 != self.admitted.len() || !image::allowed(&self.image, kind, weak) {
            return false;
        }
        if self.image.used & (1 << (kind as u8)) != 0 {
            return false;
        }
        if kind == Kind::Claim2 && self.recovered[1].as_ref() != image::view(&self.image).as_ref() {
            return false;
        }
        if kind == Kind::Serve2 && self.recovered[1].as_ref() != image::view(&self.image).as_ref() {
            return false;
        }
        self.admitted.push(self.record(kind, weak));
        true
    }
    pub fn step(&mut self, action: Action, weak: Weakening) -> bool {
        use Action::*;
        match action {
            Admit => PROGRAM
                .get(self.admitted.len())
                .copied()
                .is_some_and(|kind| self.admit_kind(kind, weak)),
            Disconnect(p) => {
                if !self.links[p] {
                    return false;
                }
                self.links[p] = false;
                self.quorum = None;
                true
            }
            Reconnect(p) => {
                if self.links[p] {
                    return false;
                }
                self.links[p] = true;
                true
            }
            Submit(kind) => self.admit_kind(kind, weak),
            Reference => self.admit_kind(Kind::AppendX, Weakening::MissingPayload),
            Write(p) => {
                let store = &mut self.stores[p];
                let next = store.base() + store.written.len();
                if next >= self.admitted.len() {
                    return false;
                }
                store.written.push(self.admitted[next].clone());
                true
            }
            Sync(p) => {
                let store = &mut self.stores[p];
                if store.synced.len() >= store.written.len() {
                    return false;
                }
                store.synced.push(store.written[store.synced.len()].clone());
                true
            }
            Commit => self.commit(weak),
            Apply => self.apply(weak),
            Observe => self.output.is_some_and(|output| self.consume(output)),
            Lose => self.output.take().is_some(),
            Quorum => self.discover(weak),
            Resolve => self.resolve(),
            Recover(p) => self.recover(p, weak),
            CheckpointStage(p) => self.stage(p, weak),
            CheckpointSync(p) => self.publish(p),
            CheckpointAnchor(p) => self.anchor(p),
            Power => self.cut(true),
            Process => self.cut(false),
            Ack(slot) => self.ack(slot, weak),
        }
    }
    pub fn consume(&mut self, completion: Completion) -> bool {
        if self.output != Some(completion) {
            return false;
        }
        self.output = None;
        self.observed = Some(completion);
        true
    }
    pub(super) fn matching(&self, boundary: ChildIndex, weak: Weakening) -> usize {
        let mut expected = Image::default();
        for record in self.admitted.iter().take(boundary.0) {
            if !image::update(&mut expected, record, weak) {
                return 0;
            }
        }
        self.stores
            .iter()
            .enumerate()
            .filter(|(p, s)| {
                self.links[*p] && image::decode(s, boundary, weak) == Some(expected.clone())
            })
            .count()
    }
    fn commit(&mut self, weak: Weakening) -> bool {
        let next = ChildIndex(self.committed.0 + 1);
        if next.0 > self.admitted.len() || self.matching(next, weak) < 2 {
            return false;
        }
        // Qualified ordinary-child consensus boundary, not a physical persisted marker.
        self.committed = next;
        true
    }
    fn apply(&mut self, weak: Weakening) -> bool {
        if self.output.is_some() || self.applied == self.committed {
            return false;
        }
        let record = &self.admitted[self.applied.0];
        if matches!(record.kind, Kind::Commit1 | Kind::Commit2)
            && !self.parent_witnesses(record.kind)
        {
            return false;
        }
        if !image::update(&mut self.image, record, weak) {
            return false;
        }
        self.applied.0 += 1;
        self.output = self.image.receipts.last().copied();
        self.observed = None;
        self.quorum = None;
        self.recovered = [None, None];
        if record.kind == Kind::Serve2 {
            self.served = true;
        }
        true
    }
    fn parent_witnesses(&self, kind: Kind) -> bool {
        let at = if kind == Kind::Commit1 { 1 } else { 2 };
        self.other.iter().all(|c| {
            c.applied.0 >= at
                && c.synced
                    .iter()
                    .filter(|s| {
                        s.len() >= at
                            && s[0] == Entry::exact(Tag::X)
                            && (at == 1 || s[1] == Entry::exact(Tag::Y))
                    })
                    .count()
                    >= 2
        })
    }
    fn discover(&mut self, weak: Weakening) -> bool {
        if self.quorum == Some(self.applied) || self.matching(self.applied, weak) < 2 {
            return false;
        }
        self.quorum = Some(self.applied);
        true
    }
    fn resolve(&mut self) -> bool {
        if self.output.is_some() || self.quorum != Some(self.applied) {
            return false;
        }
        let Some(receipt) = self.image.receipts.last().copied() else {
            return false;
        };
        if self.observed == Some(receipt) {
            return false;
        }
        self.output = Some(receipt);
        true
    }
    fn recover(&mut self, p: usize, weak: Weakening) -> bool {
        if self.quorum != Some(self.applied) {
            return false;
        }
        let Some(image) = image::decode(&self.stores[p], self.applied, weak) else {
            return false;
        };
        let Some(view) = image::view(&image) else {
            return false;
        };
        if self.recovered[p] == Some(view.clone()) {
            return false;
        }
        self.recovered[p] = Some(view);
        true
    }
    fn cut(&mut self, power: bool) -> bool {
        if self.power || self.process {
            return false;
        }
        let flag = if power {
            &mut self.power
        } else {
            &mut self.process
        };
        *flag = true;
        let store = &mut self.stores[0];
        store.written = store.synced.clone();
        store.staged = None;
        self.output = None;
        self.observed = None;
        self.quorum = None;
        self.recovered = [None, None];
        true
    }
    fn ack(&mut self, slot: usize, weak: Weakening) -> bool {
        let operation = if slot == 0 {
            Kind::Commit1 as u8
        } else {
            Kind::Commit2 as u8
        };
        if self.acknowledged[slot]
            || self.image.commit.0 <= slot as u8
            || !self
                .observed
                .is_some_and(|o| o.header.operation == operation)
            || self.matching(self.applied, weak) < 2
        {
            return false;
        }
        if weak != Weakening::MissingPayload && image::view(&self.image).is_none() {
            return false;
        }
        self.acknowledged[slot] = true;
        true
    }
}
