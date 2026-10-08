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
//! Self-contained live-state image. Normal format-2 batches follow unchanged.
use super::*;
pub(super) const MAGIC: &[u8; 8] = b"VBLCPT01";

pub(super) fn encode(
    sequence: u64,
    state: &BTreeMap<GroupIdentity, GroupLog>,
    limits: LogLimits,
    max_bytes: usize,
) -> Result<Vec<u8>, StorageError> {
    let limits = limits.validate()?;
    if max_bytes < HEADER + TRAILER || max_bytes > limits.max_wal_bytes {
        return Err(StorageError::Rejected("checkpoint byte budget"));
    }
    if state.len() > limits.max_groups || state.len() > u32::MAX as usize {
        return Err(StorageError::Rejected("checkpoint group budget"));
    }
    let mut e = Encoder {
        b: Vec::new(),
        limit: max_bytes.saturating_sub(HEADER + TRAILER),
    };
    for s in state.values() {
        e.mutation(&LogMutation::Create(s.bootstrap.clone()))?;
        e.u64(s.revision.get())?;
        e.u64(s.generation.get())?;
        e.u8(u8::from(s.snapshot.is_some()))?;
        if let Some(reference) = s.snapshot {
            e.mutation(&LogMutation::Update(LogUpdate {
                snapshot_membership: s.snapshot_membership.clone(),
                group: s.bootstrap.group,
                expected_revision: LogRevision::new(1).unwrap(),
                hard_state: s.hard_state,
                commit_index: reference.index,
                suffix: None,
                snapshot: Some(reference),
            }))?;
        }
        e.mutation(&LogMutation::Update(LogUpdate {
            snapshot_membership: None,
            group: s.bootstrap.group,
            expected_revision: LogRevision::new(if s.snapshot.is_some() { 2 } else { 1 }).unwrap(),
            hard_state: s.hard_state,
            commit_index: s.commit_index,
            suffix: Some(Suffix {
                from: s
                    .base_index()
                    .checked_add(1)
                    .ok_or(StorageError::Rejected("checkpoint index exhausted"))?,
                entries: s.entries.clone(),
            }),
            snapshot: None,
        }))?;
    }
    let mut b = MAGIC.to_vec();
    b.extend(sequence.to_le_bytes());
    b.extend((state.len() as u32).to_le_bytes());
    b.extend((e.b.len() as u32).to_le_bytes());
    b.extend(crc32c(&b).to_le_bytes());
    b.extend(0u32.to_le_bytes());
    b.extend(e.b);
    let checksum = crc32c(&b);
    b.extend(END);
    b.extend(checksum.to_le_bytes());
    b.extend(0u32.to_le_bytes());
    Ok(b)
}

pub(super) fn decode(
    bytes: &[u8],
    limits: LogLimits,
) -> Result<(u64, BTreeMap<GroupIdentity, GroupLog>, usize), StorageError> {
    let limits = limits.validate()?;
    if bytes.len() < HEADER + TRAILER
        || &bytes[..8] != MAGIC
        || crc32c(&bytes[..24]) != u32::from_le_bytes(bytes[24..28].try_into().unwrap())
        || bytes[28..32] != [0; 4]
    {
        return Err(StorageError::Corrupt("checkpoint header/version/checksum"));
    }
    let count = u32::from_le_bytes(bytes[16..20].try_into().unwrap()) as usize;
    let payload = u32::from_le_bytes(bytes[20..24].try_into().unwrap()) as usize;
    let size = HEADER
        .checked_add(payload)
        .and_then(|n| n.checked_add(TRAILER))
        .ok_or(StorageError::Corrupt("checkpoint size overflow"))?;
    if size > limits.max_wal_bytes
        || size > bytes.len()
        || count > limits.max_groups
        || count > payload / 32
    {
        return Err(StorageError::Corrupt("checkpoint length/group budget"));
    }
    let end = size - TRAILER;
    if &bytes[end..end + 8] != END
        || bytes[end + 12..size] != [0; 4]
        || crc32c(&bytes[..end]) != u32::from_le_bytes(bytes[end + 8..end + 12].try_into().unwrap())
    {
        return Err(StorageError::Corrupt("checkpoint trailer/checksum"));
    }
    let sequence = u64::from_le_bytes(bytes[8..16].try_into().unwrap());
    if (sequence == 0) != (count == 0) {
        return Err(StorageError::Corrupt("checkpoint sequence/state"));
    }
    let mut d = Decoder::new(&bytes[HEADER..end]);
    let mut result = BTreeMap::new();
    // Canonical validation uses the same mandatory logical transition guards as
    // normal replay, then restores the exact persisted revision/generation.
    for _ in 0..count {
        let create = d.mutation(limits)?;
        let LogMutation::Create(bootstrap) = &create else {
            return Err(StorageError::Corrupt("checkpoint bootstrap"));
        };
        let group = bootstrap.group;
        let revision =
            LogRevision::new(d.u64()?).ok_or(StorageError::Corrupt("checkpoint revision"))?;
        let generation =
            LogGeneration::new(d.u64()?).ok_or(StorageError::Corrupt("checkpoint generation"))?;
        if generation.get() > revision.get() || revision.get() > sequence {
            return Err(StorageError::Corrupt("checkpoint generation/revision"));
        }
        let mut one = BTreeMap::new();
        apply_batch(&mut one, &[create], limits)
            .map_err(|_| StorageError::Corrupt("checkpoint bootstrap validation"))?;
        let has_snapshot = match d.u8()? {
            0 => false,
            1 => true,
            _ => return Err(StorageError::Corrupt("checkpoint snapshot flag")),
        };
        if has_snapshot {
            let snapshot = d.mutation(limits)?;
            if !matches!(&snapshot, LogMutation::Update(u) if u.group == group && u.snapshot.is_some())
            {
                return Err(StorageError::Corrupt("checkpoint snapshot record"));
            }
            apply_batch(&mut one, &[snapshot], limits)
                .map_err(|_| StorageError::Corrupt("checkpoint snapshot validation"))?;
        }
        let suffix = d.mutation(limits)?;
        if !matches!(&suffix, LogMutation::Update(u) if u.group == group && u.snapshot.is_none() && u.suffix.as_ref().is_some_and(|s| s.from == one[&group].base_index() + 1))
        {
            return Err(StorageError::Corrupt("checkpoint suffix record"));
        }
        apply_batch(&mut one, &[suffix], limits)
            .map_err(|_| StorageError::Corrupt("checkpoint suffix validation"))?;
        let mut s = one.remove(&group).unwrap();
        if revision.get() == 1
            && (has_snapshot
                || s.hard_state
                    != crate::contracts::HardState {
                        term: 0,
                        voted_for: None,
                    }
                || s.commit_index != 0
                || !s.entries.is_empty())
        {
            return Err(StorageError::Corrupt("checkpoint initial revision"));
        }
        if has_snapshot && generation.get() < 2 {
            return Err(StorageError::Corrupt("checkpoint snapshot generation"));
        }
        s.revision = revision;
        s.generation = generation;
        if result.keys().any(|g: &GroupIdentity| g.id == group.id)
            || result.insert(group, s).is_some()
        {
            return Err(StorageError::Corrupt("checkpoint duplicate group"));
        }
    }
    if d.offset != d.b.len() {
        return Err(StorageError::Corrupt("checkpoint trailing payload"));
    }
    Ok((sequence, result, size))
}

/// Mandatory logical validation also runs on host codec results. Encoding is
/// replaceable; bootstrap, vote, commit and suffix invariants are not.
pub(super) fn validate_state(
    sequence: u64,
    state: &BTreeMap<GroupIdentity, GroupLog>,
    limits: LogLimits,
) -> Result<(), StorageError> {
    if state.len() > limits.max_groups || (sequence == 0) != state.is_empty() {
        return Err(StorageError::Corrupt("checkpoint state/sequence budget"));
    }
    let mut ids = std::collections::BTreeSet::new();
    for (group, s) in state {
        if *group != s.bootstrap.group
            || !ids.insert(group.id)
            || s.generation.get() > s.revision.get()
            || s.revision.get() > sequence
            || (s.snapshot.is_some() && s.generation.get() < 2)
        {
            return Err(StorageError::Corrupt("checkpoint identity/counters"));
        }
        let mut one = BTreeMap::new();
        apply_batch(
            &mut one,
            &[LogMutation::Create(s.bootstrap.clone())],
            limits,
        )
        .map_err(|_| StorageError::Corrupt("checkpoint bootstrap invariant"))?;
        let initial = one[group].clone();
        if let Some(reference) = s.snapshot {
            apply_batch(
                &mut one,
                &[LogMutation::Update(LogUpdate {
                    snapshot_membership: s.snapshot_membership.clone(),
                    group: *group,
                    expected_revision: initial.revision,
                    hard_state: s.hard_state,
                    commit_index: reference.index,
                    suffix: None,
                    snapshot: Some(reference),
                })],
                limits,
            )
            .map_err(|_| StorageError::Corrupt("checkpoint snapshot invariant"))?;
        }
        let revision = one[group].revision;
        let from = one[group]
            .base_index()
            .checked_add(1)
            .ok_or(StorageError::Corrupt("checkpoint index exhausted"))?;
        apply_batch(
            &mut one,
            &[LogMutation::Update(LogUpdate {
                snapshot_membership: None,
                group: *group,
                expected_revision: revision,
                hard_state: s.hard_state,
                commit_index: s.commit_index,
                suffix: Some(Suffix {
                    from,
                    entries: s.entries.clone(),
                }),
                snapshot: None,
            })],
            limits,
        )
        .map_err(|_| StorageError::Corrupt("checkpoint suffix/ballot/commit invariant"))?;
        let mut checked = one.remove(group).unwrap();
        checked.revision = s.revision;
        checked.generation = s.generation;
        if checked != *s || (s.revision.get() == 1 && *s != initial) {
            return Err(StorageError::Corrupt("checkpoint state invariant"));
        }
    }
    Ok(())
}
