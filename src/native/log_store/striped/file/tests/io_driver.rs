// SPDX-License-Identifier: RPL-1.5
use super::*;
use std::{cell::RefCell, rc::Rc};
#[derive(Clone, Copy, Debug)]
pub(super) struct Cut {
    pub lane: usize,
    pub boundary: PublicationBoundary,
}
#[derive(Default)]
struct Fault {
    cut: Option<Cut>,
    visited: bool,
}
#[derive(Clone, Default)]
pub(super) struct Selector(Rc<RefCell<Fault>>);
impl Selector {
    pub fn arm(&self, cut: Cut) {
        *self.0.borrow_mut() = Fault {
            cut: Some(cut),
            visited: false,
        };
    }
    pub fn visited(&self) -> bool {
        self.0.borrow().visited
    }
    fn at(&self, lane: usize, boundary: PublicationBoundary) -> io::Result<()> {
        let mut fault = self.0.borrow_mut();
        if fault
            .cut
            .is_some_and(|cut| cut.lane == lane && cut.boundary == boundary)
        {
            fault.visited = true;
            fault.cut = None;
            return Err(io::Error::other("original native publication interrupted"));
        }
        Ok(())
    }
}
pub(super) struct Interrupted {
    inner: FileStripedIo,
    pub selector: Selector,
}
impl Interrupted {
    pub fn new(inner: FileStripedIo) -> Self {
        Self {
            inner,
            selector: Selector::default(),
        }
    }
}
impl StripedIo for Interrupted {
    fn lane_count(&self) -> usize {
        self.inner.lane_count()
    }
    fn read_catalog(&mut self, lane: usize, limit: usize) -> io::Result<Vec<u8>> {
        self.inner.read_catalog(lane, limit)
    }
    fn read_payload(&mut self, lane: usize, generation: u64, limit: usize) -> io::Result<Vec<u8>> {
        self.inner.read_payload(lane, generation, limit)
    }
    fn append_payload(&mut self, lane: usize, generation: u64, bytes: &[u8]) -> io::Result<()> {
        self.inner.append_payload(lane, generation, bytes)
    }
    fn sync_payload(&mut self, lane: usize, generation: u64) -> io::Result<()> {
        self.inner.sync_payload(lane, generation)
    }
    fn truncate_payload(&mut self, lane: usize, generation: u64, length: u64) -> io::Result<()> {
        self.inner.truncate_payload(lane, generation, length)
    }
    fn publish_catalog(&mut self, lane: usize, bytes: &[u8]) -> io::Result<()> {
        let selector = &self.selector;
        self.inner
            .publish_with(lane, bytes, |boundary| selector.at(lane, boundary))
    }
}
pub(super) fn cuts() -> impl Iterator<Item = Cut> {
    use PublicationPrimitive::*;
    (0..3).flat_map(|lane| {
        [
            StagingOpen,
            StagingWrite,
            StagingSync,
            Rename,
            DirectorySync,
        ]
        .into_iter()
        .flat_map(move |primitive| {
            [false, true].into_iter().map(move |after| Cut {
                lane,
                boundary: PublicationBoundary { primitive, after },
            })
        })
    })
}
pub(super) fn selects_new(cut: Cut) -> bool {
    cut.lane > 0
        || cut.boundary.primitive == PublicationPrimitive::DirectorySync
        || cut.boundary
            == PublicationBoundary {
                primitive: PublicationPrimitive::Rename,
                after: true,
            }
}
