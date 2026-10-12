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
use state::FinalFact;
pub(super) const COMMAND: &[u8; 8] = b"VBACTA01";
pub(super) struct Encoder {
    pub bytes: Vec<u8>,
    limit: usize,
}
impl Encoder {
    pub fn new(limit: usize) -> Self {
        Self {
            bytes: Vec::new(),
            limit,
        }
    }
    pub fn put(&mut self, bytes: &[u8]) -> Result<(), ApplicationError> {
        if bytes.len() > self.limit.saturating_sub(self.bytes.len()) {
            return Err(ApplicationError::ReceiptBudget);
        }
        self.bytes.reserve_exact(bytes.len());
        self.bytes.extend_from_slice(bytes);
        if self.bytes.capacity() > self.limit {
            return Err(ApplicationError::ReceiptBudget);
        }
        Ok(())
    }
    pub fn u8(&mut self, v: u8) -> Result<(), ApplicationError> {
        self.put(&[v])
    }
    pub fn u64(&mut self, v: u64) -> Result<(), ApplicationError> {
        self.put(&v.to_le_bytes())
    }
    pub fn u128(&mut self, v: u128) -> Result<(), ApplicationError> {
        self.put(&v.to_le_bytes())
    }
    pub fn blob(&mut self, b: &[u8]) -> Result<(), ApplicationError> {
        self.u64(b.len() as u64)?;
        self.put(b)
    }
    pub fn group(&mut self, g: GroupIdentity) -> Result<(), ApplicationError> {
        self.u128(g.id.get())?;
        self.u64(g.incarnation.get())
    }
    pub fn mapping(&mut self, m: LogicalStoreMapping) -> Result<(), ApplicationError> {
        self.group(m.parent)?;
        self.u64(m.voter.get())?;
        self.u128(m.store.id.get())?;
        self.u64(m.store.incarnation.get())?;
        self.group(m.child)
    }
    pub fn position(&mut self, p: ActivationPosition) -> Result<(), ApplicationError> {
        self.u64(p.index)?;
        self.u64(p.term)
    }
}
pub(super) struct Decoder<'a> {
    pub bytes: &'a [u8],
    at: usize,
}
impl<'a> Decoder<'a> {
    pub fn new(bytes: &'a [u8]) -> Self {
        Self { bytes, at: 0 }
    }
    pub fn take(&mut self, n: usize) -> Result<&'a [u8], ApplicationError> {
        let end = self
            .at
            .checked_add(n)
            .filter(|n| *n <= self.bytes.len())
            .ok_or(ApplicationError::InvalidCommand)?;
        let b = &self.bytes[self.at..end];
        self.at = end;
        Ok(b)
    }
    pub fn u8(&mut self) -> Result<u8, ApplicationError> {
        Ok(self.take(1)?[0])
    }
    pub fn u64(&mut self) -> Result<u64, ApplicationError> {
        Ok(u64::from_le_bytes(self.take(8)?.try_into().unwrap()))
    }
    pub fn u128(&mut self) -> Result<u128, ApplicationError> {
        Ok(u128::from_le_bytes(self.take(16)?.try_into().unwrap()))
    }
    pub fn blob(&mut self, max: usize) -> Result<&'a [u8], ApplicationError> {
        let n = usize::try_from(self.u64()?)
            .ok()
            .filter(|n| *n <= max)
            .ok_or(ApplicationError::InvalidCommand)?;
        self.take(n)
    }
    pub fn id(&mut self) -> Result<OperationId, ApplicationError> {
        OperationId::new(self.u128()?).ok_or(ApplicationError::InvalidCommand)
    }
    pub fn config(&mut self) -> Result<ConfigurationId, ApplicationError> {
        ConfigurationId::new(self.u64()?).ok_or(ApplicationError::InvalidCommand)
    }
    pub fn group(&mut self) -> Result<GroupIdentity, ApplicationError> {
        Ok(GroupIdentity {
            id: GroupId::new(self.u128()?).ok_or(ApplicationError::InvalidCommand)?,
            incarnation: GroupIncarnation::new(self.u64()?)
                .ok_or(ApplicationError::InvalidCommand)?,
        })
    }
    pub fn store(&mut self) -> Result<StoreIdentity, ApplicationError> {
        Ok(StoreIdentity {
            id: StoreId::new(self.u128()?).ok_or(ApplicationError::InvalidCommand)?,
            incarnation: StoreIncarnation::new(self.u64()?)
                .ok_or(ApplicationError::InvalidCommand)?,
        })
    }
    pub fn mapping(&mut self) -> Result<LogicalStoreMapping, ApplicationError> {
        let parent = self.group()?;
        let voter = NodeId::new(self.u64()?).ok_or(ApplicationError::InvalidCommand)?;
        let store = self.store()?;
        let child = self.group()?;
        LogicalStoreMapping::new(parent, voter, store, child)
    }
    pub fn position(&mut self) -> Result<ActivationPosition, ApplicationError> {
        Ok(ActivationPosition {
            index: self.u64()?,
            term: self.u64()?,
        })
    }
    pub fn finish(self) -> Result<(), ApplicationError> {
        if self.at == self.bytes.len() {
            Ok(())
        } else {
            Err(ApplicationError::InvalidCommand)
        }
    }
    pub fn rest(&mut self) -> &'a [u8] {
        let b = &self.bytes[self.at..];
        self.at = self.bytes.len();
        b
    }
}
pub(super) fn intent(e: &mut Encoder, i: &ChildActivationIntent) -> Result<(), ApplicationError> {
    e.mapping(i.mapping)?;
    for id in i.operations.ids() {
        e.u128(id.get())?;
    }
    for id in [
        i.old_configuration,
        i.learners_configuration,
        i.joint_configuration,
        i.target_configuration,
    ] {
        e.u64(id.get())?;
    }
    for list in [&i.old, &i.target] {
        for v in list {
            e.u64(v.node.get())?;
            e.u128(v.store.id.get())?;
            e.u64(v.store.incarnation.get())?;
        }
    }
    Ok(())
}
fn decode_voters(d: &mut Decoder<'_>) -> Result<[ActivationVoter; 3], ApplicationError> {
    let mut v = Vec::with_capacity(3);
    for _ in 0..3 {
        v.push(ActivationVoter {
            node: NodeId::new(d.u64()?).ok_or(ApplicationError::InvalidCommand)?,
            store: d.store()?,
        });
    }
    v.try_into().map_err(|_| ApplicationError::InvalidCommand)
}
fn decode_intent(d: &mut Decoder<'_>) -> Result<ChildActivationIntent, ApplicationError> {
    let i = ChildActivationIntent {
        mapping: d.mapping()?,
        operations: ChildActivationOperations {
            begin: d.id()?,
            learners: d.id()?,
            membership: d.id()?,
            outcome: d.id()?,
            release: d.id()?,
        },
        old_configuration: d.config()?,
        learners_configuration: d.config()?,
        joint_configuration: d.config()?,
        target_configuration: d.config()?,
        old: decode_voters(d)?,
        target: decode_voters(d)?,
    };
    i.validate()?;
    Ok(i)
}
pub(super) fn command(c: &ControlCommand, max: usize) -> Result<Vec<u8>, ApplicationError> {
    let mut e = Encoder::new(max.min(CONTROL_BYTES));
    e.put(COMMAND)?;
    match c {
        ControlCommand::Begin(i) => {
            e.u8(1)?;
            intent(&mut e, i)?;
        }
        ControlCommand::Outcome { begin, fact } => {
            e.u8(2)?;
            e.u128(begin.get())?;
            e.u128(fact.operation.get())?;
            e.u64(fact.configuration.get())?;
            e.position(fact.position)?;
        }
        ControlCommand::Release { begin, outcome } => {
            e.u8(3)?;
            e.u128(begin.get())?;
            e.position(*outcome)?;
        }
    }
    Ok(e.bytes)
}
pub(super) enum Envelope<'a> {
    Business(&'a [u8]),
    Control(ControlCommand),
}
pub(super) fn decode(bytes: &[u8], max: usize) -> Result<Envelope<'_>, ApplicationError> {
    if bytes.len() > max {
        return Err(ApplicationError::InvalidCommand);
    }
    let mut d = Decoder::new(bytes);
    if d.take(8)? != COMMAND {
        return Err(ApplicationError::UnsupportedSchema);
    }
    let c = match d.u8()? {
        0 => return Ok(Envelope::Business(d.rest())),
        1 => ControlCommand::Begin(Box::new(decode_intent(&mut d)?)),
        2 => ControlCommand::Outcome {
            begin: d.id()?,
            fact: FinalFact {
                operation: d.id()?,
                configuration: d.config()?,
                position: d.position()?,
            },
        },
        3 => ControlCommand::Release {
            begin: d.id()?,
            outcome: d.position()?,
        },
        _ => return Err(ApplicationError::InvalidCommand),
    };
    d.finish()?;
    Ok(Envelope::Control(c))
}
