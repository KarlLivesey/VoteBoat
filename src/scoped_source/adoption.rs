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
//! Checked retained-grant publication observations; never foreign authentication.
use super::*;
pub const MAX_RETAINED_ADOPTION_BYTES: usize = 56 + MAX_TRANSFER_PUBLICATION_BYTES;
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct RetainedGrantAdoption {
    pub metadata_configuration: ConfigurationId,
    pub decision: TransferPublicationStatus,
}
impl RetainedGrantAdoption {
    pub fn encode(&self, max_bytes: usize) -> Result<Vec<u8>, ApplicationError> {
        let body = self.decision.encode(36 + MAX_TRANSFER_PUBLICATION_BYTES)?;
        if !self.decision.publication.intent().is_retained_insertion()
            || 20 + body.len() > max_bytes
            || 20 + body.len() > MAX_RETAINED_ADOPTION_BYTES
        {
            return Err(ApplicationError::InvalidCommand);
        }
        let mut out = Vec::with_capacity(20 + body.len());
        out.extend(b"VBSADP01");
        out.extend(self.metadata_configuration.get().to_le_bytes());
        out.extend((body.len() as u32).to_le_bytes());
        out.extend(body);
        Ok(out)
    }
    pub fn decode(bytes: &[u8]) -> Result<Self, ApplicationError> {
        if bytes.len() > MAX_RETAINED_ADOPTION_BYTES {
            return Err(ApplicationError::InvalidCommand);
        }
        let mut r = Reader::new(bytes);
        if r.take(8)? != b"VBSADP01" {
            return Err(ApplicationError::InvalidCommand);
        }
        let metadata_configuration =
            ConfigurationId::new(r.u64()?).ok_or(ApplicationError::InvalidCommand)?;
        let len = r.u32()? as usize;
        let decision = TransferPublicationStatus::decode(r.take(len)?)?;
        let value = Self {
            metadata_configuration,
            decision,
        };
        if !r.done() || value.encode(bytes.len())? != bytes {
            return Err(ApplicationError::InvalidCommand);
        }
        Ok(value)
    }
}
#[derive(Clone)]
pub(super) struct Adoption {
    pub status: RetainedGrantStatus,
    pub command: RetainedGrantAdoption,
    pub digest: ContentDigest,
}
impl Adoption {
    pub fn new(
        status: RetainedGrantStatus,
        command: RetainedGrantAdoption,
    ) -> Result<Self, ApplicationError> {
        let mut body = Vec::new();
        body.extend(b"VBSADPD1");
        body.extend(status.operation.get().to_le_bytes());
        body.extend(status.index.to_le_bytes());
        body.extend(command.encode(MAX_RETAINED_ADOPTION_BYTES)?);
        Ok(Self {
            status,
            command,
            digest: ContentDigest::sha256(&body),
        })
    }
}
