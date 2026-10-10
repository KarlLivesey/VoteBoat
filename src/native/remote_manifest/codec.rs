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
use super::{RemoteManifestError as Error, REMOTE_MANIFEST_WIRE_VERSION};
use crate::{
    identity::*,
    routing::{codec::*, *},
};
pub(super) const HEADER: usize = 92;
pub(super) const MAX_FRAME: usize = HEADER + MAX_MANIFEST_BYTES;
#[derive(Clone, Debug)]
pub(super) enum Payload {
    Request,
    Missing,
    Unavailable,
    Hint {
        manifest: Box<ResponsibilityManifest>,
        lifetime_ms: u64,
    },
}
#[derive(Clone, Debug)]
pub(super) struct Frame {
    pub sequence: u64,
    pub query: ManifestLookup,
    pub payload: Payload,
}
impl Frame {
    pub fn encode(&self) -> Result<Vec<u8>, Error> {
        let length = HEADER
            + match &self.payload {
                Payload::Hint { manifest, .. } => manifest_len(manifest),
                _ => 0,
            };
        if self.sequence == 0 || length > MAX_FRAME {
            return Err(Error::Protocol);
        }
        let mut bytes = Vec::with_capacity(length);
        bytes.extend(b"VBMD");
        bytes.push(REMOTE_MANIFEST_WIRE_VERSION);
        bytes.push(match self.payload {
            Payload::Request => 1,
            Payload::Hint { .. } => 2,
            Payload::Missing => 3,
            Payload::Unavailable => 4,
        });
        bytes.extend([0; 2]);
        bytes.extend((length as u32).to_le_bytes());
        bytes.extend(self.sequence.to_le_bytes());
        put_group(&mut bytes, self.query.locator.authority);
        put_responsibility(&mut bytes, self.query.locator.responsibility);
        bytes.extend(
            self.query
                .minimum_epoch
                .map_or(0, |e| e.get())
                .to_le_bytes(),
        );
        bytes.extend(
            self.query
                .minimum_generation
                .map_or(0, |g| g.get())
                .to_le_bytes(),
        );
        bytes.extend(
            match &self.payload {
                Payload::Hint { lifetime_ms, .. } => *lifetime_ms,
                _ => 0,
            }
            .to_le_bytes(),
        );
        if let Payload::Hint { manifest, .. } = &self.payload {
            put_manifest(&mut bytes, manifest);
        }
        Ok(bytes)
    }
    pub fn length(bytes: &[u8]) -> Result<usize, Error> {
        if bytes.len() < HEADER
            || &bytes[..4] != b"VBMD"
            || bytes[4] != REMOTE_MANIFEST_WIRE_VERSION
            || bytes[6..8] != [0, 0]
        {
            return Err(Error::Protocol);
        }
        let length = u32::from_le_bytes(bytes[8..12].try_into().unwrap()) as usize;
        if !(HEADER..=MAX_FRAME).contains(&length) {
            return Err(Error::Protocol);
        }
        Ok(length)
    }
    pub fn decode(bytes: &[u8]) -> Result<Self, Error> {
        if Self::length(bytes)? != bytes.len() {
            return Err(Error::Protocol);
        }
        let mut reader = Reader::new(&bytes[12..]);
        let result = (|| {
            let sequence = reader.u64()?;
            let query = ManifestLookup {
                locator: AuthorityLocator {
                    authority: reader.group()?,
                    responsibility: reader.responsibility()?,
                },
                minimum_epoch: OwnershipEpoch::new(reader.u64()?),
                minimum_generation: RouteGeneration::new(reader.u64()?),
            };
            let lifetime_ms = reader.u64()?;
            let payload = match bytes[5] {
                1 => Payload::Request,
                2 if lifetime_ms > 0 => Payload::Hint {
                    manifest: Box::new(read_manifest(reader.take(bytes.len() - HEADER)?)?),
                    lifetime_ms,
                },
                3 => Payload::Missing,
                4 => Payload::Unavailable,
                _ => return Err(crate::application::ApplicationError::InvalidCommand),
            };
            if !reader.done() {
                return Err(crate::application::ApplicationError::InvalidCommand);
            }
            Ok(Self {
                sequence,
                query,
                payload,
            })
        })()
        .map_err(|_| Error::Protocol)?;
        if result.encode()? != bytes {
            return Err(Error::Protocol);
        }
        Ok(result)
    }
}
