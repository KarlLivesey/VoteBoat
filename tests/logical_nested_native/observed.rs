// SPDX-License-Identifier: RPL-1.5
// Copyright (c) 2026 Karl Livesey
//! Diagnostic delegation of an original real authenticated session; no proof construction.
use std::{cell::Cell, rc::Rc};
use voteboat::{runtime::MonoTime, secure::*};
#[derive(Clone, Copy, Debug, Default)]
pub struct Counts {
    pub accepted_plaintext: usize,
    pub received_plaintext: usize,
    pub flush_observations: usize,
}
pub type Observation = ((usize, usize), Rc<Cell<Counts>>);
pub struct ObservedSession {
    original: Box<dyn SecureSession>,
    counts: Rc<Cell<Counts>>,
    awaiting_flush: Cell<bool>,
}
impl ObservedSession {
    pub fn wrap(original: Box<dyn SecureSession>) -> (Box<dyn SecureSession>, Rc<Cell<Counts>>) {
        let counts = Rc::new(Cell::new(Counts::default()));
        (
            Box::new(Self {
                original,
                counts: Rc::clone(&counts),
                awaiting_flush: Cell::new(false),
            }),
            counts,
        )
    }
}
impl SecureSession for ObservedSession {
    fn security(&self) -> SessionSecurity {
        self.original.security()
    }
    fn state(&self) -> SessionState {
        self.original.state()
    }
    fn binding(&self) -> Option<SessionBinding> {
        self.original.binding()
    }
    fn limits(&self) -> SessionLimits {
        self.original.limits()
    }
    fn poll(
        &mut self,
        now: MonoTime,
        budget: SessionPollBudget,
    ) -> Result<SessionProgress, SessionError> {
        self.original.poll(now, budget)
    }
    fn read_plaintext(&mut self, bytes: &mut [u8]) -> Result<usize, SessionError> {
        let n = self.original.read_plaintext(bytes)?;
        let mut counts = self.counts.get();
        counts.received_plaintext += n;
        self.counts.set(counts);
        Ok(n)
    }
    fn write_plaintext(&mut self, bytes: &[u8]) -> Result<usize, SessionError> {
        let n = self.original.write_plaintext(bytes)?;
        let mut counts = self.counts.get();
        counts.accepted_plaintext += n;
        self.counts.set(counts);
        if n > 0 {
            self.awaiting_flush.set(true);
        }
        Ok(n)
    }
    fn is_flushed(&self) -> bool {
        let flushed = self.original.is_flushed();
        if flushed && self.awaiting_flush.replace(false) {
            let mut counts = self.counts.get();
            counts.flush_observations += 1;
            self.counts.set(counts);
        }
        flushed
    }
    fn drain_status(&mut self) -> SessionDrainStatus {
        self.original.drain_status()
    }
    fn close(&mut self) {
        self.original.close()
    }
    fn revoke(&mut self) {
        self.original.revoke()
    }
}
