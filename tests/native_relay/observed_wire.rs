// SPDX-License-Identifier: RPL-1.5
// Copyright (c) 2026 Karl Livesey
//! Bounded original observations and an explicit selected-decoder violation.
use std::{
    cell::{Cell, RefCell},
    rc::Rc,
};
use voteboat::{
    identity::NodeId,
    native::wire::NativeWireCodec,
    raft::{Message, RequestContext, Rpc},
    wire::*,
};
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum Kind {
    Probe,
    Ack,
}
#[derive(Clone, Copy, Debug)]
pub struct Observation {
    pub at: u64,
    pub from: NodeId,
    pub context: RequestContext,
    pub kind: Kind,
    pub encoded: bool,
    pub occurrences: usize,
}
#[derive(Clone)]
pub struct ObservedWire {
    inner: NativeWireCodec,
    at: u64,
    records: Rc<RefCell<Vec<Observation>>>,
    scope_fault: Option<Rc<Cell<bool>>>,
}
impl ObservedWire {
    pub fn new(at: u64, records: Rc<RefCell<Vec<Observation>>>) -> Self {
        Self {
            inner: NativeWireCodec::with_leadership_transfer(Default::default()).unwrap(),
            at,
            records,
            scope_fault: None,
        }
    }
    pub fn with_scope_fault(mut self, fault: Rc<Cell<bool>>) -> Self {
        self.scope_fault = Some(fault);
        self
    }
    fn observe(&self, messages: &[Message], encoded: bool) {
        for m in messages {
            let kind = match m.rpc {
                Rpc::ReadProbe => Kind::Probe,
                Rpc::ReadAck => Kind::Ack,
                _ => continue,
            };
            let mut records = self.records.borrow_mut();
            if let Some(r) = records.iter_mut().find(|r| {
                r.at == self.at
                    && r.from == m.from
                    && r.context == m.context
                    && r.kind == kind
                    && r.encoded == encoded
            }) {
                r.occurrences += 1;
                continue;
            }
            assert!(records.len() < 1024, "finite observer limit exceeded");
            records.push(Observation {
                at: self.at,
                from: m.from,
                context: m.context,
                kind,
                encoded,
                occurrences: 1,
            });
        }
    }
}
impl WireCodec for ObservedWire {
    fn configuration_capacity(
        &self,
        r: &ConfigurationWireRequirements<'_>,
    ) -> Result<ConfigurationWireCapacity, WireError> {
        self.inner.configuration_capacity(r)
    }
    fn format_version(&self) -> u16 {
        self.inner.format_version()
    }
    fn header_bytes(&self) -> usize {
        self.inner.header_bytes()
    }
    fn limits(&self) -> WireLimits {
        self.inner.limits()
    }
    fn frame_length(&self, h: &[u8]) -> Result<usize, WireError> {
        self.inner.frame_length(h)
    }
    fn encoded_length(&self, s: WireScope, m: &[Message]) -> Result<usize, WireError> {
        self.inner.encoded_length(s, m)
    }
    fn encode_into(&self, s: WireScope, m: &[Message], b: &mut [u8]) -> Result<(), WireError> {
        self.inner.encode_into(s, m, b)?;
        self.observe(m, true);
        Ok(())
    }
    fn encode_batch(&self, s: WireScope, m: &[Message]) -> Result<Vec<u8>, WireError> {
        let b = self.inner.encode_batch(s, m)?;
        self.observe(m, true);
        Ok(b)
    }
    fn decode_batch(&self, s: WireScope, b: &[u8]) -> Result<Vec<Message>, WireError> {
        let mut m = self.inner.decode_batch(s, b)?;
        self.observe(&m, false);
        if self.at == 1 && self.scope_fault.as_ref().is_some_and(|f| f.replace(false)) {
            m[0].from = NodeId::new(99).unwrap();
        }
        Ok(m)
    }
}
