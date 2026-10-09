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
//! Borrowed bounded routed application command format 1.
use super::*;

pub(super) const DATA_HEADER: usize = 125;
pub(crate) enum Command<'a> {
    Bootstrap(&'a [u8]),
    Data {
        hint: RouteHint,
        key: &'a [u8],
        payload: &'a [u8],
    },
    Fence(OwnershipEpoch),
    ScopeFence(OwnershipEpoch, BucketRange),
}
pub(crate) fn decode(bytes: &[u8], max_payload: usize) -> Result<Command<'_>, ApplicationError> {
    if bytes.len() > MAX_ROUTED_COMMAND_BYTES {
        return Err(ApplicationError::InvalidCommand);
    }
    if bytes.starts_with(b"VBROWN01") || bytes.starts_with(b"VBROWN02") {
        return Ok(Command::Bootstrap(bytes));
    }
    if let Some(inner) = bytes.strip_prefix(b"VBRSCF01") {
        let mut r = Reader::new(inner);
        let epoch = OwnershipEpoch::new(r.u64()?).ok_or(ApplicationError::InvalidCommand)?;
        let scope = r.range()?;
        if !r.done() {
            return Err(ApplicationError::InvalidCommand);
        }
        return Ok(Command::ScopeFence(epoch, scope));
    }
    let mut r = Reader::new(bytes);
    if r.take(8)? != b"VBRCMD01" {
        return Err(ApplicationError::InvalidCommand);
    }
    let command = match r.u8()? {
        0 => {
            let responsibility = r.responsibility()?;
            let group = r.group()?;
            let application = ApplicationAdapter {
                id: ApplicationAdapterId::new(r.u128()?).ok_or(ApplicationError::InvalidCommand)?,
                version: r.u32()?,
            };
            let scheme = PartitionScheme {
                id: RoutingSchemeId::new(r.u128()?).ok_or(ApplicationError::InvalidCommand)?,
                version: r.u32()?,
            };
            let scope = r.range()?;
            let bucket = r.u16()?;
            let epoch = OwnershipEpoch::new(r.u64()?).ok_or(ApplicationError::InvalidCommand)?;
            let generation =
                RouteGeneration::new(r.u64()?).ok_or(ApplicationError::InvalidCommand)?;
            let key_len = usize::from(r.u16()?);
            let payload_len = r.u32()? as usize;
            if application.version == 0
                || scheme.version == 0
                || bucket >= 256
                || key_len > MAX_ROUTING_KEY_BYTES
                || payload_len > max_payload
            {
                return Err(ApplicationError::InvalidCommand);
            }
            let key = r.take(key_len)?;
            let payload = r.take(payload_len)?;
            Command::Data {
                hint: RouteHint {
                    responsibility,
                    group,
                    application,
                    scheme,
                    scope,
                    bucket,
                    epoch,
                    generation,
                },
                key,
                payload,
            }
        }
        1 => Command::Fence(OwnershipEpoch::new(r.u64()?).ok_or(ApplicationError::InvalidCommand)?),
        _ => return Err(ApplicationError::InvalidCommand),
    };
    if !r.done() {
        return Err(ApplicationError::InvalidCommand);
    }
    Ok(command)
}
/// Encode a location hint and semantic request. Encoding grants no authority.
pub fn encode_routed(
    hint: RouteHint,
    key: &[u8],
    payload: &[u8],
    max_bytes: usize,
) -> Result<Vec<u8>, ApplicationError> {
    let len = DATA_HEADER
        .checked_add(key.len())
        .and_then(|n| n.checked_add(payload.len()))
        .ok_or(ApplicationError::InvalidCommand)?;
    if key.len() > MAX_ROUTING_KEY_BYTES
        || payload.len() > MAX_ROUTED_PAYLOAD_BYTES
        || len > max_bytes
        || len > MAX_ROUTED_COMMAND_BYTES
        || hint.application.version == 0
        || hint.scheme.version == 0
        || hint.bucket >= 256
    {
        return Err(ApplicationError::InvalidCommand);
    }
    let mut out = Vec::with_capacity(len);
    out.extend(b"VBRCMD01");
    out.push(0);
    put_responsibility(&mut out, hint.responsibility);
    put_group(&mut out, hint.group);
    out.extend(hint.application.id.get().to_le_bytes());
    out.extend(hint.application.version.to_le_bytes());
    out.extend(hint.scheme.id.get().to_le_bytes());
    out.extend(hint.scheme.version.to_le_bytes());
    put_range(&mut out, hint.scope);
    out.extend(hint.bucket.to_le_bytes());
    out.extend(hint.epoch.get().to_le_bytes());
    out.extend(hint.generation.get().to_le_bytes());
    out.extend((key.len() as u16).to_le_bytes());
    out.extend((payload.len() as u32).to_le_bytes());
    out.extend(key);
    out.extend(payload);
    Ok(out)
}
/// Privileged local lifecycle command. Hosts must authorize it separately from
/// ordinary data ingress. This irreversible fence does not activate a target.
pub fn encode_fence(epoch: OwnershipEpoch) -> Vec<u8> {
    let mut out = Vec::with_capacity(17);
    out.extend(b"VBRCMD01");
    out.push(1);
    out.extend(epoch.get().to_le_bytes());
    out
}

/// Privileged forward-only scoped fence. Only an explicitly selected scoped
/// profile admits it; hosts must authorize lifecycle commands separately.
/// This command does not publish routes, activate a target or export data.
pub fn encode_scope_fence(epoch: OwnershipEpoch, scope: BucketRange) -> Vec<u8> {
    let mut out = Vec::with_capacity(20);
    out.extend(b"VBRSCF01");
    out.extend(epoch.get().to_le_bytes());
    put_range(&mut out, scope);
    out
}
