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
use super::{encode_error, invalid};
use crate::{contracts::HardState, format_crc::crc32c, identity::*, log::*, log_format::*};
use std::{collections::BTreeMap, io};

#[derive(Clone, Debug, Eq, PartialEq)]
pub(super) struct Part {
    pub lane: usize,
    pub offset: usize,
    pub length: usize,
    pub checksum: u32,
}
#[derive(Clone, Debug, Eq, PartialEq)]
pub(super) struct Record {
    pub length: usize,
    pub checksum: u32,
    pub control: Vec<u8>,
    pub parts: Vec<Part>,
}
impl Record {
    pub fn new(bytes: &[u8], limits: LogLimits) -> io::Result<Self> {
        Ok(Self {
            length: bytes.len(),
            checksum: crc32c(bytes),
            control: control_frame(bytes, limits)?,
            parts: Vec::new(),
        })
    }
    pub fn validate(&self, bytes: &[u8], limits: LogLimits) -> io::Result<()> {
        if bytes.len() != self.length
            || crc32c(bytes) != self.checksum
            || control_frame(bytes, limits)? != self.control
        {
            return Err(invalid("striped payload differs from selected control"));
        }
        Ok(())
    }
}
fn strip(entries: &mut [LogEntry]) {
    for entry in entries {
        if let EntryPayload::Command { bytes, .. } = &mut entry.payload {
            *bytes = Default::default();
        }
    }
}
pub(super) fn control_frame(bytes: &[u8], limits: LogLimits) -> io::Result<Vec<u8>> {
    if NativeLogCodec.is_checkpoint(bytes) {
        let (sequence, mut groups, used) = NativeLogCodec
            .decode_checkpoint(bytes, limits)
            .map_err(encode_error)?;
        if used != bytes.len() {
            return Err(invalid("striped checkpoint trailing bytes"));
        }
        for group in groups.values_mut() {
            // Explicit copy, edit and refreeze; the decoded source is unchanged.
            let mut entries = group.entries.to_vec();
            strip(&mut entries);
            group.entries = entries.into();
        }
        NativeLogCodec
            .encode_checkpoint(sequence, &groups, limits, limits.max_wal_bytes)
            .map_err(encode_error)
    } else {
        let (sequence, mut mutations) = NativeLogCodec
            .decode_batch(bytes, limits)
            .map_err(encode_error)?;
        for mutation in &mut mutations {
            if let LogMutation::Update(update) = mutation {
                if let Some(suffix) = &mut update.suffix {
                    let mut entries = suffix.entries.to_vec();
                    strip(&mut entries);
                    suffix.entries = entries.into();
                }
            }
        }
        NativeLogCodec
            .encode_batch(sequence, &mutations, limits)
            .map_err(encode_error)
    }
}
pub(super) fn control_only(bytes: &[u8], limits: LogLimits) -> io::Result<bool> {
    if NativeLogCodec.is_checkpoint(bytes) {
        return Ok(true);
    }
    let (_, mutations) = NativeLogCodec
        .decode_batch(bytes, limits)
        .map_err(encode_error)?;
    Ok(mutations.iter().all(|mutation| match mutation {
        LogMutation::Create(_) => true,
        LogMutation::Update(update) => update.suffix.as_ref().is_none_or(|suffix| {
            suffix
                .entries
                .iter()
                .all(|entry| matches!(entry.payload, EntryPayload::Noop))
        }),
    }))
}
pub(super) fn controls(
    records: &[Record],
    limits: LogLimits,
) -> io::Result<BTreeMap<GroupIdentity, GroupLog>> {
    let mut groups = BTreeMap::new();
    let mut sequence = 0;
    for (i, record) in records.iter().enumerate() {
        if i == 0 && NativeLogCodec.is_checkpoint(&record.control) {
            let (seq, state, used) = NativeLogCodec
                .decode_checkpoint(&record.control, limits)
                .map_err(encode_error)?;
            if used != record.control.len() {
                return Err(invalid("control checkpoint length"));
            }
            super::super::checkpoint::validate_state(seq, &state, limits).map_err(encode_error)?;
            sequence = seq;
            groups = state;
        } else {
            let (seq, mutations) = NativeLogCodec
                .decode_batch(&record.control, limits)
                .map_err(encode_error)?;
            if sequence.checked_add(1) != Some(seq) {
                return Err(invalid("control sequence discontinuity"));
            }
            crate::log::replay_batch(&mut groups, &mutations, limits).map_err(encode_error)?;
            sequence = seq;
        }
    }
    Ok(groups)
}
/// Diagnostic metadata surviving a missing payload lane. This carries no
/// LogTicket, durable matching prefix or authority to vote/acknowledge.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct StripedControlImage {
    pub binding: StoreBinding,
    pub payload_generation: u64,
    pub durable_log_bytes: u64,
    pub groups: BTreeMap<GroupIdentity, StripedGroupControl>,
}
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct StripedGroupControl {
    pub bootstrap: Bootstrap,
    pub revision: LogRevision,
    pub generation: LogGeneration,
    pub hard_state: HardState,
    pub ballot_origin: Option<BallotOrigin>,
    pub commit_index: u64,
    pub snapshot: Option<crate::snapshot::SnapshotRef>,
    pub last_index: u64,
    pub last_term: u64,
    pub membership: crate::membership::Membership,
}
impl TryFrom<GroupLog> for StripedGroupControl {
    type Error = io::Error;
    fn try_from(group: GroupLog) -> io::Result<Self> {
        let membership = group
            .membership()
            .map_err(|_| invalid("striped control membership"))?;
        Ok(Self {
            last_index: group.last_index(),
            last_term: group.last_term(),
            membership,
            bootstrap: group.bootstrap,
            revision: group.revision,
            generation: group.generation,
            hard_state: group.hard_state,
            ballot_origin: group.ballot_origin,
            commit_index: group.commit_index,
            snapshot: group.snapshot,
        })
    }
}
