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
use std::{cell::RefCell, io, rc::Rc};
use voteboat::native::log_store::striped::StripedIo;
#[derive(Clone, Default)]
struct Lane {
    written: [Vec<u8>; 2],
    durable: [Vec<u8>; 2],
    catalog: Option<Vec<u8>>,
    lost: bool,
}
#[derive(Clone, Default)]
struct Device {
    lanes: [Lane; 2],
    steps: usize,
    fail: Option<(usize, bool)>,
}
#[derive(Clone, Default)]
pub struct Io(Rc<RefCell<Device>>);
impl Io {
    pub fn fail(&self, step: usize, after: bool) {
        let mut d = self.0.borrow_mut();
        d.steps = 0;
        d.fail = Some((step, after));
    }
    pub fn reset_steps(&self) {
        self.0.borrow_mut().steps = 0;
    }
    pub fn steps(&self) -> usize {
        self.0.borrow().steps
    }
    pub fn crash(&self) {
        let mut d = self.0.borrow_mut();
        d.fail = None;
        for lane in &mut d.lanes {
            lane.written = lane.durable.clone();
        }
    }
    pub fn lose(&self, lane: usize) {
        self.0.borrow_mut().lanes[lane] = Lane {
            lost: true,
            ..Lane::default()
        };
    }
    pub fn fork(&self) -> Self {
        Self(Rc::new(RefCell::new(self.0.borrow().clone())))
    }
    pub fn payload_bytes(&self) -> [usize; 2] {
        let d = self.0.borrow();
        std::array::from_fn(|i| d.lanes[i].durable.iter().map(Vec::len).sum())
    }
    pub fn corrupt_payload(&self, lane: usize) {
        let mut d = self.0.borrow_mut();
        let lane = &mut d.lanes[lane];
        let p = lane.durable.iter_mut().find(|p| !p.is_empty()).unwrap();
        p[0] ^= 0x80;
        lane.written = lane.durable.clone();
    }
    pub fn corrupt_catalog(&self, lane: usize) {
        self.0.borrow_mut().lanes[lane].catalog.as_mut().unwrap()[0] ^= 1;
    }
    fn effect(&self, lane: usize, apply: impl FnOnce(&mut Lane)) -> io::Result<()> {
        let mut d = self.0.borrow_mut();
        d.steps += 1;
        let step = d.steps;
        if d.lanes[lane].lost {
            return Err(io::ErrorKind::NotFound.into());
        }
        if d.fail == Some((step, false)) {
            return Err(io::Error::other("before primitive"));
        }
        apply(&mut d.lanes[lane]);
        if d.fail == Some((step, true)) {
            return Err(io::Error::other("after primitive"));
        }
        Ok(())
    }
}
impl StripedIo for Io {
    fn lane_count(&self) -> usize {
        2
    }
    fn read_catalog(&mut self, lane: usize, limit: usize) -> io::Result<Vec<u8>> {
        let d = self.0.borrow();
        let l = &d.lanes[lane];
        if l.lost {
            return Err(io::ErrorKind::NotFound.into());
        }
        let bytes = l.catalog.clone().ok_or(io::ErrorKind::NotFound)?;
        if bytes.len() > limit {
            return Err(io::Error::other("catalog read budget"));
        }
        Ok(bytes)
    }
    fn read_payload(&mut self, lane: usize, generation: u64, limit: usize) -> io::Result<Vec<u8>> {
        let d = self.0.borrow();
        let l = &d.lanes[lane];
        if l.lost {
            return Err(io::ErrorKind::NotFound.into());
        }
        let bytes = l.written[(generation % 2) as usize].clone();
        if bytes.len() > limit {
            return Err(io::Error::other("payload read budget"));
        }
        Ok(bytes)
    }
    fn append_payload(&mut self, lane: usize, generation: u64, bytes: &[u8]) -> io::Result<()> {
        self.effect(lane, |l| l.written[(generation % 2) as usize].extend(bytes))
    }
    fn sync_payload(&mut self, lane: usize, generation: u64) -> io::Result<()> {
        self.effect(lane, |l| {
            l.durable[(generation % 2) as usize] = l.written[(generation % 2) as usize].clone()
        })
    }
    fn truncate_payload(&mut self, lane: usize, generation: u64, length: u64) -> io::Result<()> {
        self.effect(lane, |l| {
            l.written[(generation % 2) as usize].truncate(length as usize)
        })
    }
    fn publish_catalog(&mut self, lane: usize, bytes: &[u8]) -> io::Result<()> {
        self.effect(lane, |l| l.catalog = Some(bytes.to_vec()))
    }
}
