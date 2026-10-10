// SPDX-License-Identifier: RPL-1.5
// Copyright (c) 2026 Karl Livesey
use super::*;
use crate::{routed::OwnershipFence, transfer_source::SourceExportCommitment};
pub const MAX_TRANSFER_OBSERVATION_BYTES: usize = 128 * 1024;
type Error = ApplicationError;
fn invalid() -> Error {
    Error::InvalidCommand
}
fn bytes(out: &mut Vec<u8>, value: &[u8]) {
    out.extend((value.len() as u32).to_le_bytes());
    out.extend(value);
}
fn blob<'a>(r: &mut Reader<'a>) -> Result<&'a [u8], Error> {
    let n = r.u32()? as usize;
    r.take(n)
}
fn configuration(r: &mut Reader<'_>) -> Result<ConfigurationId, Error> {
    ConfigurationId::new(r.u64()?).ok_or_else(invalid)
}
fn digest(r: &mut Reader<'_>) -> Result<ContentDigest, Error> {
    Ok(ContentDigest(r.take(32)?.try_into().unwrap()))
}
fn put_fence(out: &mut Vec<u8>, f: OwnershipFence) {
    put_group(out, f.group);
    put_responsibility(out, f.responsibility);
    out.extend(f.epoch.get().to_le_bytes());
    out.extend(f.index.to_le_bytes());
    out.extend(f.operation.get().to_le_bytes());
}
fn fence(r: &mut Reader<'_>) -> Result<OwnershipFence, Error> {
    Ok(OwnershipFence {
        group: r.group()?,
        responsibility: r.responsibility()?,
        epoch: OwnershipEpoch::new(r.u64()?).ok_or_else(invalid)?,
        index: r.u64()?,
        operation: r.operation()?,
    })
}
fn count(r: &mut Reader<'_>) -> Result<usize, Error> {
    let n = r.u16()? as usize;
    if n > MAX_MANIFEST_ROUTES {
        return Err(invalid());
    }
    Ok(n)
}
impl TransferObservation {
    /// Bounded transient wire representation, not a durable or signed proof.
    pub fn encode(&self, max_bytes: usize) -> Result<Vec<u8>, Error> {
        let mut out = Vec::new();
        out.extend(b"VBTOBS01");
        put_group(&mut out, self.read.group);
        out.extend(self.configuration.get().to_le_bytes());
        out.extend(self.index.to_le_bytes());
        match &self.value {
            ObservationValue::Intent(value) => {
                out.push(0);
                out.push(u8::from(value.is_some()));
                if let Some(v) = value {
                    out.extend(v.operation.get().to_le_bytes());
                    out.extend(v.index.to_le_bytes());
                    bytes(&mut out, &v.intent.encode(MAX_TRANSFER_INTENT_BYTES)?);
                }
            }
            ObservationValue::Publication(value) => {
                out.push(1);
                out.push(u8::from(value.is_some()));
                if let Some(v) = value {
                    bytes(
                        &mut out,
                        &v.encode(crate::transfer_publication::MAX_TRANSFER_PUBLICATION_BYTES)?,
                    );
                }
            }
            ObservationValue::Source(value) => {
                out.push(2);
                out.push(u8::from(value.is_some()));
                if let Some(v) = value {
                    put_source(&mut out, v);
                }
            }
            ObservationValue::Target(value) => {
                out.push(3);
                put_target(&mut out, value);
            }
        }
        if out.len() > max_bytes || out.len() > MAX_TRANSFER_OBSERVATION_BYTES {
            return Err(invalid());
        }
        Ok(out)
    }
    /// Caller must authenticate the live responding group and match this result
    /// to its outstanding read. Decoding cannot establish quorum authority or
    /// freshness and must never be used for cached files or untrusted records.
    pub fn decode_authenticated(bytes: &[u8]) -> Result<Self, Error> {
        if bytes.len() > MAX_TRANSFER_OBSERVATION_BYTES {
            return Err(invalid());
        }
        let mut r = Reader::new(bytes);
        if r.take(8)? != b"VBTOBS01" {
            return Err(invalid());
        }
        let group = r.group()?;
        let configuration = configuration(&mut r)?;
        let index = r.u64()?;
        if index == 0 {
            return Err(invalid());
        }
        let (kind, value) = match r.u8()? {
            0 => (
                TransferReadKind::Intent,
                ObservationValue::Intent(if r.boolean()? {
                    Some(TransferIntentStatus {
                        operation: r.operation()?,
                        index: r.u64()?,
                        intent: TransferIntent::decode(blob(&mut r)?)?,
                    })
                } else {
                    None
                }),
            ),
            1 => (
                TransferReadKind::Publication,
                ObservationValue::Publication(if r.boolean()? {
                    Some(TransferPublicationStatus::decode(blob(&mut r)?)?)
                } else {
                    None
                }),
            ),
            2 => (
                TransferReadKind::Source,
                ObservationValue::Source(if r.boolean()? {
                    Some(source(&mut r, configuration)?)
                } else {
                    None
                }),
            ),
            3 => (
                TransferReadKind::Target,
                ObservationValue::Target(target(&mut r)?),
            ),
            _ => return Err(invalid()),
        };
        let value = Self {
            read: TransferRead { group, kind },
            configuration,
            index,
            value,
        };
        if !r.done()
            || !value.valid_prefix()
            || value.encode(MAX_TRANSFER_OBSERVATION_BYTES)? != bytes
        {
            return Err(invalid());
        }
        Ok(value)
    }
    fn valid_prefix(&self) -> bool {
        let valid = |i| i > 0 && i <= self.index;
        match &self.value {
            ObservationValue::Intent(v) => v.as_ref().is_none_or(|v| valid(v.index)),
            ObservationValue::Publication(v) => v.as_ref().is_none_or(|v| valid(v.index)),
            ObservationValue::Source(v) => v
                .as_ref()
                .is_none_or(|v| v.fence.group == self.read.group && valid(v.fence.index)),
            ObservationValue::Target(v) => {
                v.group == self.read.group && valid_target_prefix(v, self.index)
            }
        }
    }
}
fn valid_target_prefix(v: &TargetStatus, prefix: u64) -> bool {
    let valid = |i| i > 0 && i <= prefix;
    v.staged_index.is_none_or(valid)
        && v.imported
            .as_ref()
            .is_none_or(|i| valid(i.index) && v.staged_index.is_some_and(|stage| stage < i.index))
        && v.activated.is_none_or(|a| {
            // Publication belongs to the metadata log, not this local prefix.
            valid(a.index)
                && a.publication_index > 0
                && v.imported.as_ref().is_some_and(|i| i.index < a.index)
        })
}
fn put_source(out: &mut Vec<u8>, v: &SourceFenceEvidence) {
    put_fence(out, v.fence);
    out.push(u8::from(v.scope.is_some()));
    if let Some(scope) = v.scope {
        put_range(out, scope);
    }
    out.extend(v.intent_digest.0);
    out.extend((v.exports.len() as u16).to_le_bytes());
    for e in &v.exports {
        put_group(out, e.target);
        put_range(out, e.scope);
        out.extend(e.digest.0);
    }
}
fn source(
    r: &mut Reader<'_>,
    configuration: ConfigurationId,
) -> Result<SourceFenceEvidence, Error> {
    let fence = fence(r)?;
    let scope = if r.boolean()? { Some(r.range()?) } else { None };
    let intent_digest = digest(r)?;
    let n = count(r)?;
    let mut exports = Vec::with_capacity(n);
    for _ in 0..n {
        exports.push(SourceExportCommitment {
            target: r.group()?,
            scope: r.range()?,
            digest: digest(r)?,
        });
    }
    Ok(SourceFenceEvidence {
        fence,
        scope,
        configuration,
        intent_digest,
        exports,
    })
}
fn put_target(out: &mut Vec<u8>, v: &TargetStatus) {
    put_group(out, v.group);
    out.extend(v.operation.get().to_le_bytes());
    out.extend(v.staged_index.unwrap_or(0).to_le_bytes());
    out.push(u8::from(v.imported.is_some()));
    if let Some(i) = &v.imported {
        out.extend(i.index.to_le_bytes());
        out.extend(i.digest.0);
        out.extend((i.sources.len() as u16).to_le_bytes());
        for s in &i.sources {
            put_fence(out, s.fence);
            out.extend(s.configuration.get().to_le_bytes());
            put_range(out, s.scope);
            out.extend(s.digest.0);
        }
    }
    out.push(u8::from(v.activated.is_some()));
    if let Some(a) = v.activated {
        out.extend(a.index.to_le_bytes());
        out.extend(a.digest.0);
        out.extend(a.metadata_configuration.get().to_le_bytes());
        out.extend(a.publication_operation.get().to_le_bytes());
        out.extend(a.publication_index.to_le_bytes());
    }
}
fn target(r: &mut Reader<'_>) -> Result<TargetStatus, Error> {
    let group = r.group()?;
    let operation = r.operation()?;
    let staged_index = match r.u64()? {
        0 => None,
        n => Some(n),
    };
    let imported = if r.boolean()? {
        let index = r.u64()?;
        let digest = digest(r)?;
        let n = count(r)?;
        let mut sources = Vec::with_capacity(n);
        for _ in 0..n {
            sources.push(ImportedSource {
                fence: fence(r)?,
                configuration: configuration(r)?,
                scope: r.range()?,
                digest: self::digest(r)?,
            });
        }
        Some(ImportStatus {
            index,
            digest,
            sources,
        })
    } else {
        None
    };
    let activated = if r.boolean()? {
        Some(ActivationStatus {
            index: r.u64()?,
            digest: digest(r)?,
            metadata_configuration: configuration(r)?,
            publication_operation: r.operation()?,
            publication_index: r.u64()?,
        })
    } else {
        None
    };
    Ok(TargetStatus {
        group,
        operation,
        staged_index,
        imported,
        activated,
    })
}
