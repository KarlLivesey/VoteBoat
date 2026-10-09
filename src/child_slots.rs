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
//! Checked conversion of an already deleted child selector to a vacancy.
use crate::{
    application::*,
    deletion::*,
    identity::*,
    routing::{codec::*, *},
};
use std::mem::size_of;
pub const CHILD_SLOT_DIRECTORY_SCHEMA: u64 = 10;
pub const MAX_RETIRE_CHILD_SLOT_BYTES: usize = MAX_MANIFEST_BYTES + 216;
#[derive(Clone, Debug, Eq, PartialEq)]
/// Exact parent view and original published child deletion. Foreign evidence
/// must come from the host's authenticated quorum read; bytes and digests do
/// not establish that authority. Same-authority records are checked locally.
pub struct RetireChildSlot {
    pub before: ResponsibilityManifest,
    pub child: ChildDeletionEvidence,
}
#[derive(Clone, Debug, Eq, PartialEq)]
/// Original local applied status, not a transferable quorum certificate.
pub struct RetiredChildSlotStatus {
    pub operation: OperationId,
    pub index: u64,
    pub retirement: RetireChildSlot,
}
impl RetireChildSlot {
    fn validate(&self) -> Result<(), ApplicationError> {
        self.child.validate()?;
        let b = self.before.input();
        let c = self.child;
        let ExecutionMode::Delegated(routes) = &b.execution else {
            return Err(ApplicationError::InvalidCommand);
        };
        if b.state != ResponsibilityState::Active
            || b.generation.get() == u64::MAX
            || c.parent
                != (ParentAuthority {
                    responsibility: b.responsibility,
                    group: b.authority,
                })
            || !routes.iter().any(|r| {
                r.scope == c.scope
                    && r.target
                        == RouteTarget::Child(ChildAuthority {
                            responsibility: c.responsibility,
                            group: c.authority,
                            epoch: c.epoch,
                        })
            })
        {
            return Err(ApplicationError::InvalidCommand);
        }
        Ok(())
    }
    pub fn after(&self) -> Result<ResponsibilityManifest, ApplicationError> {
        self.validate()?;
        let mut m = self.before.clone().into_input();
        let ExecutionMode::Delegated(routes) = &mut m.execution else {
            unreachable!()
        };
        let r = routes
            .iter_mut()
            .find(|r| r.scope == self.child.scope)
            .expect("validated exact child");
        r.target = RouteTarget::Vacant;
        m.generation = RouteGeneration::new(m.generation.get() + 1).unwrap();
        ResponsibilityManifest::new(m).map_err(|_| ApplicationError::InvalidCommand)
    }
    pub fn retained_bytes(&self) -> usize {
        size_of::<Self>() + self.before.retained_bytes() - size_of::<ResponsibilityManifest>()
    }
    pub fn encode(&self, max: usize) -> Result<Vec<u8>, ApplicationError> {
        self.validate()?;
        let len = 12 + manifest_len(&self.before) + 204;
        if len > max.min(MAX_RETIRE_CHILD_SLOT_BYTES) {
            return Err(ApplicationError::InvalidCommand);
        }
        let mut b = Vec::with_capacity(len);
        b.extend(b"VBRSLOT1");
        b.extend((manifest_len(&self.before) as u32).to_le_bytes());
        put_manifest(&mut b, &self.before);
        self.child.put(&mut b);
        Ok(b)
    }
    pub fn decode(b: &[u8]) -> Result<Self, ApplicationError> {
        if b.len() > MAX_RETIRE_CHILD_SLOT_BYTES {
            return Err(ApplicationError::InvalidCommand);
        }
        let mut r = Reader::new(b);
        if r.take(8)? != b"VBRSLOT1" {
            return Err(ApplicationError::InvalidCommand);
        }
        let n = r.u32()? as usize;
        let before = read_manifest(r.take(n)?)?;
        let child = ChildDeletionEvidence::read(&mut r)?;
        if !r.done() {
            return Err(ApplicationError::InvalidCommand);
        }
        let value = Self { before, child };
        value.validate()?;
        Ok(value)
    }
}
