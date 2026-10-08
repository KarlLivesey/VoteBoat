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
//! Shared checked manifest format 1 and bounded little-endian application reader.
use crate::{application::ApplicationError, routing::*};

pub(crate) struct Reader<'a> {
    bytes: &'a [u8],
}
impl<'a> Reader<'a> {
    pub(crate) fn new(bytes: &'a [u8]) -> Self {
        Self { bytes }
    }
    pub(crate) fn take(&mut self, count: usize) -> Result<&'a [u8], ApplicationError> {
        if count > self.bytes.len() {
            return Err(ApplicationError::InvalidCommand);
        }
        let (head, tail) = self.bytes.split_at(count);
        self.bytes = tail;
        Ok(head)
    }
    pub(crate) fn done(&self) -> bool {
        self.bytes.is_empty()
    }
    pub(crate) fn u8(&mut self) -> Result<u8, ApplicationError> {
        Ok(self.take(1)?[0])
    }
    pub(crate) fn u16(&mut self) -> Result<u16, ApplicationError> {
        Ok(u16::from_le_bytes(self.take(2)?.try_into().unwrap()))
    }
    pub(crate) fn u32(&mut self) -> Result<u32, ApplicationError> {
        Ok(u32::from_le_bytes(self.take(4)?.try_into().unwrap()))
    }
    pub(crate) fn u64(&mut self) -> Result<u64, ApplicationError> {
        Ok(u64::from_le_bytes(self.take(8)?.try_into().unwrap()))
    }
    pub(crate) fn u128(&mut self) -> Result<u128, ApplicationError> {
        Ok(u128::from_le_bytes(self.take(16)?.try_into().unwrap()))
    }
    pub(crate) fn boolean(&mut self) -> Result<bool, ApplicationError> {
        match self.u8()? {
            0 => Ok(false),
            1 => Ok(true),
            _ => Err(ApplicationError::InvalidCommand),
        }
    }
    pub(crate) fn group(&mut self) -> Result<GroupIdentity, ApplicationError> {
        Ok(GroupIdentity {
            id: GroupId::new(self.u128()?).ok_or(ApplicationError::InvalidCommand)?,
            incarnation: GroupIncarnation::new(self.u64()?)
                .ok_or(ApplicationError::InvalidCommand)?,
        })
    }
    pub(crate) fn responsibility(&mut self) -> Result<ResponsibilityIdentity, ApplicationError> {
        Ok(ResponsibilityIdentity {
            id: ResponsibilityId::new(self.u128()?).ok_or(ApplicationError::InvalidCommand)?,
            incarnation: ResponsibilityIncarnation::new(self.u64()?)
                .ok_or(ApplicationError::InvalidCommand)?,
        })
    }
    pub(crate) fn operation(&mut self) -> Result<OperationId, ApplicationError> {
        OperationId::new(self.u128()?).ok_or(ApplicationError::InvalidCommand)
    }
    pub(crate) fn range(&mut self) -> Result<BucketRange, ApplicationError> {
        BucketRange::new(self.u16()?, self.u16()?).map_err(|_| ApplicationError::InvalidCommand)
    }
}
pub(crate) fn put_group(out: &mut Vec<u8>, group: GroupIdentity) {
    out.extend(group.id.get().to_le_bytes());
    out.extend(group.incarnation.get().to_le_bytes());
}
pub(crate) fn put_responsibility(out: &mut Vec<u8>, id: ResponsibilityIdentity) {
    out.extend(id.id.get().to_le_bytes());
    out.extend(id.incarnation.get().to_le_bytes());
}
pub(crate) fn put_range(out: &mut Vec<u8>, scope: BucketRange) {
    out.extend(scope.start().to_le_bytes());
    out.extend(scope.end().to_le_bytes());
}
pub(crate) fn manifest_len(m: &ResponsibilityManifest) -> usize {
    let input = m.input();
    // magic + identity + parent tag + authority + adapter + scheme + scope +
    // epoch/generation + placement + state + mode + mode-dependent body.
    8 + 24
        + 1
        + input.parent.map_or(0, |_| 48)
        + 24
        + 20
        + 20
        + 4
        + 16
        + 3
        + 1
        + 1
        + match &input.execution {
            ExecutionMode::Single(_) => 24,
            ExecutionMode::Partitioned(v) | ExecutionMode::Delegated(v) => {
                2 + v
                    .iter()
                    .map(|e| {
                        4 + 1
                            + match e.target {
                                RouteTarget::Group(_) => 24,
                                RouteTarget::Child(_) => 56,
                            }
                    })
                    .sum::<usize>()
            }
        }
}
pub(crate) fn put_manifest(out: &mut Vec<u8>, m: &ResponsibilityManifest) {
    let input = m.input();
    out.extend(b"VBMAN001");
    put_responsibility(out, input.responsibility);
    out.push(u8::from(input.parent.is_some()));
    if let Some(parent) = input.parent {
        put_responsibility(out, parent.responsibility);
        put_group(out, parent.group);
    }
    put_group(out, input.authority);
    out.extend(input.application.id.get().to_le_bytes());
    out.extend(input.application.version.to_le_bytes());
    out.extend(input.scheme.id.get().to_le_bytes());
    out.extend(input.scheme.version.to_le_bytes());
    put_range(out, input.scope);
    out.extend(input.epoch.get().to_le_bytes());
    out.extend(input.generation.get().to_le_bytes());
    out.extend((input.placement.minimum_voting_domains as u16).to_le_bytes());
    out.push(u8::from(input.placement.survive_any_single_domain_loss));
    out.push(match input.state {
        ResponsibilityState::Active => 0,
        ResponsibilityState::Fenced => 1,
    });
    let entries = match &input.execution {
        ExecutionMode::Single(group) => {
            out.push(0);
            put_group(out, *group);
            return;
        }
        ExecutionMode::Partitioned(v) => {
            out.push(1);
            v
        }
        ExecutionMode::Delegated(v) => {
            out.push(2);
            v
        }
    };
    out.extend((entries.len() as u16).to_le_bytes());
    for entry in entries {
        put_range(out, entry.scope);
        match entry.target {
            RouteTarget::Group(group) => {
                out.push(0);
                put_group(out, group);
            }
            RouteTarget::Child(child) => {
                out.push(1);
                put_responsibility(out, child.responsibility);
                put_group(out, child.group);
                out.extend(child.epoch.get().to_le_bytes());
            }
        }
    }
}
pub(crate) fn read_manifest(bytes: &[u8]) -> Result<ResponsibilityManifest, ApplicationError> {
    if bytes.len() > MAX_MANIFEST_BYTES {
        return Err(ApplicationError::InvalidCommand);
    }
    let mut reader = Reader::new(bytes);
    if reader.take(8)? != b"VBMAN001" {
        return Err(ApplicationError::InvalidCommand);
    }
    let responsibility = reader.responsibility()?;
    let parent = if reader.boolean()? {
        Some(ParentAuthority {
            responsibility: reader.responsibility()?,
            group: reader.group()?,
        })
    } else {
        None
    };
    let authority = reader.group()?;
    let application = ApplicationAdapter {
        id: ApplicationAdapterId::new(reader.u128()?).ok_or(ApplicationError::InvalidCommand)?,
        version: reader.u32()?,
    };
    let scheme = PartitionScheme {
        id: RoutingSchemeId::new(reader.u128()?).ok_or(ApplicationError::InvalidCommand)?,
        version: reader.u32()?,
    };
    let scope = reader.range()?;
    let epoch = OwnershipEpoch::new(reader.u64()?).ok_or(ApplicationError::InvalidCommand)?;
    let generation = RouteGeneration::new(reader.u64()?).ok_or(ApplicationError::InvalidCommand)?;
    let placement = crate::placement::PlacementRequirements {
        minimum_voting_domains: reader.u16()?.into(),
        survive_any_single_domain_loss: reader.boolean()?,
    };
    let state = match reader.u8()? {
        0 => ResponsibilityState::Active,
        1 => ResponsibilityState::Fenced,
        _ => return Err(ApplicationError::InvalidCommand),
    };
    let mode = reader.u8()?;
    let execution = match mode {
        0 => ExecutionMode::Single(reader.group()?),
        1 | 2 => {
            let count = usize::from(reader.u16()?);
            if count == 0 || count > MAX_MANIFEST_ROUTES {
                return Err(ApplicationError::InvalidCommand);
            }
            let mut entries = Vec::with_capacity(count);
            for _ in 0..count {
                let scope = reader.range()?;
                let target = match reader.u8()? {
                    0 => RouteTarget::Group(reader.group()?),
                    1 => RouteTarget::Child(ChildAuthority {
                        responsibility: reader.responsibility()?,
                        group: reader.group()?,
                        epoch: OwnershipEpoch::new(reader.u64()?)
                            .ok_or(ApplicationError::InvalidCommand)?,
                    }),
                    _ => return Err(ApplicationError::InvalidCommand),
                };
                entries.push(RouteEntry { scope, target });
            }
            if mode == 1 {
                ExecutionMode::Partitioned(entries)
            } else {
                ExecutionMode::Delegated(entries)
            }
        }
        _ => return Err(ApplicationError::InvalidCommand),
    };
    if !reader.done() {
        return Err(ApplicationError::InvalidCommand);
    }
    ResponsibilityManifest::new(ManifestInput {
        responsibility,
        parent,
        authority,
        application,
        scheme,
        scope,
        epoch,
        generation,
        placement,
        state,
        execution,
    })
    .map_err(|_| ApplicationError::InvalidCommand)
}
