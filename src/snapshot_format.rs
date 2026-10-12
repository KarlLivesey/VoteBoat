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
//! Pure VBSNAP01/VBSNAP02 metadata and application encoding.
use crate::{
    contracts::StorageError,
    format_crc::crc32c,
    log::{LogLimits, LogMutation},
    log_format::{LogCodec, NativeLogCodec},
    snapshot::*,
};
/// Native snapshot encoding is independently selectable from logical storage.
/// Version 1 supports static metadata; version 2 also preserves membership bases
/// and reads version-1 data. Prefix and finish must preserve
/// metadata and enforce length/format/checksum bounds before decode allocation.
/// Decoding must return the exact application bytes supplied through the stage's
/// chunks; sealing verifies this round trip before synchronization.
pub trait SnapshotCodec {
    fn format_version(&self) -> u32;
    fn prefix(
        &self,
        metadata: &SnapshotMetadata,
        application_bytes: usize,
        limits: SnapshotLimits,
    ) -> Result<Vec<u8>, StorageError>;
    fn finish(&self, body: &[u8], limits: SnapshotLimits) -> Result<[u8; 4], StorageError>;
    fn decode(&self, file: &[u8], limits: SnapshotLimits) -> Result<Snapshot, StorageError>;
}
#[derive(Clone, Copy, Default)]
pub struct NativeSnapshotCodec;
pub(crate) const HEADER: usize = 44;
fn codec_log_limits(limits: SnapshotLimits) -> LogLimits {
    LogLimits {
        max_batch_bytes: limits.max_metadata_bytes,
        max_command_bytes: 1,
        ..LogLimits::default()
    }
}
impl NativeSnapshotCodec {
    fn parse(&self, body: &[u8], limits: SnapshotLimits) -> Result<Snapshot, StorageError> {
        limits.validate()?;
        if body.len() < HEADER || !matches!(&body[..8], b"VBSNAP01" | b"VBSNAP02") {
            return Err(StorageError::Corrupt("snapshot header/version"));
        }
        let schema = u64::from_le_bytes(body[8..16].try_into().unwrap());
        let index = u64::from_le_bytes(body[16..24].try_into().unwrap());
        let term = u64::from_le_bytes(body[24..32].try_into().unwrap());
        let metadata_bytes = u32::from_le_bytes(body[32..36].try_into().unwrap()) as usize;
        let application_bytes = u64::from_le_bytes(body[36..44].try_into().unwrap());
        if metadata_bytes > limits.max_metadata_bytes
            || application_bytes == 0
            || application_bytes > limits.max_application_bytes as u64
            || body.len() != HEADER + metadata_bytes + application_bytes as usize
        {
            return Err(StorageError::Corrupt("snapshot length budgets"));
        }
        let encoded = &body[HEADER..HEADER + metadata_bytes];
        let (bootstrap_bytes, membership_bytes) = if &body[..8] == b"VBSNAP02" {
            if encoded.len() < 4 {
                return Err(StorageError::Corrupt("snapshot metadata prefix"));
            }
            let len = u32::from_le_bytes(encoded[..4].try_into().unwrap()) as usize;
            if len > encoded.len() - 4 {
                return Err(StorageError::Corrupt("snapshot bootstrap length"));
            }
            (&encoded[4..4 + len], Some(&encoded[4 + len..]))
        } else {
            (encoded, None)
        };
        let (sequence, mutations) =
            NativeLogCodec.decode_batch(bootstrap_bytes, codec_log_limits(limits))?;
        let [LogMutation::Create(bootstrap)] = mutations.as_slice() else {
            return Err(StorageError::Corrupt("snapshot bootstrap record"));
        };
        if sequence != 1 {
            return Err(StorageError::Corrupt("snapshot bootstrap sequence"));
        }
        let metadata = SnapshotMetadata {
            membership: membership_bytes
                .map(|b| NativeLogCodec::decode_membership(b, index))
                .transpose()?
                .map(Box::new),
            bootstrap: bootstrap.clone(),
            index,
            term,
            application_schema: schema,
        };
        metadata
            .validate()
            .map_err(|_| StorageError::Corrupt("snapshot metadata invalid"))?;
        if metadata
            .membership
            .as_ref()
            .is_some_and(|m| m.retained_bytes() > limits.max_metadata_bytes)
        {
            return Err(StorageError::Corrupt("snapshot membership budget"));
        }
        Ok(Snapshot {
            metadata,
            application: body[HEADER + metadata_bytes..].to_vec(),
        })
    }
}
impl SnapshotCodec for NativeSnapshotCodec {
    fn format_version(&self) -> u32 {
        2
    }
    fn prefix(
        &self,
        metadata: &SnapshotMetadata,
        application_bytes: usize,
        limits: SnapshotLimits,
    ) -> Result<Vec<u8>, StorageError> {
        limits.validate()?;
        metadata.validate()?;
        if application_bytes == 0 || application_bytes > limits.max_application_bytes {
            return Err(StorageError::Rejected("snapshot application budget"));
        }
        if metadata
            .membership
            .as_ref()
            .is_some_and(|m| m.retained_bytes() > limits.max_metadata_bytes)
        {
            return Err(StorageError::Rejected("snapshot membership budget"));
        }
        let bootstrap = NativeLogCodec.encode_batch(
            1,
            &[LogMutation::Create(metadata.bootstrap.clone())],
            codec_log_limits(limits),
        )?;
        let encoded = if let Some(membership) = &metadata.membership {
            let mut encoded = (bootstrap.len() as u32).to_le_bytes().to_vec();
            encoded.extend(bootstrap);
            encoded.extend(NativeLogCodec::encode_membership(
                membership,
                limits.max_metadata_bytes.saturating_sub(encoded.len()),
            )?);
            encoded
        } else {
            bootstrap
        };
        let mut b = if metadata.membership.is_some() {
            b"VBSNAP02".to_vec()
        } else {
            b"VBSNAP01".to_vec()
        };
        b.extend(metadata.application_schema.to_le_bytes());
        b.extend(metadata.index.to_le_bytes());
        b.extend(metadata.term.to_le_bytes());
        b.extend((encoded.len() as u32).to_le_bytes());
        b.extend((application_bytes as u64).to_le_bytes());
        b.extend(encoded);
        Ok(b)
    }
    fn finish(&self, body: &[u8], limits: SnapshotLimits) -> Result<[u8; 4], StorageError> {
        self.parse(body, limits)?;
        Ok(crc32c(body).to_le_bytes())
    }
    fn decode(&self, file: &[u8], limits: SnapshotLimits) -> Result<Snapshot, StorageError> {
        limits.validate()?;
        if file.len() < HEADER + 4 || file.len() > limits.max_file_bytes() {
            return Err(StorageError::Corrupt("snapshot file length"));
        }
        let end = file.len() - 4;
        if crc32c(&file[..end]) != u32::from_le_bytes(file[end..].try_into().unwrap()) {
            return Err(StorageError::Corrupt("snapshot checksum"));
        }
        self.parse(&file[..end], limits)
    }
}
