// SPDX-License-Identifier: RPL-1.5
// Copyright (c) 2026 Karl Livesey
use super::*;
use crate::{
    identity::*,
    runtime::{DrainGroup, LocalDrainRequest},
};
use ring::digest::{digest, SHA256};
pub(super) fn encode(record: &DrainRecord) -> Vec<u8> {
    let mut b = Vec::with_capacity(112 + record.request.groups.len() * 32);
    b.extend_from_slice(b"VBDR0001");
    b.extend_from_slice(&record.sequence.to_be_bytes());
    b.extend_from_slice(&record.owner.node.get().to_be_bytes());
    b.extend_from_slice(&record.owner.store.id.get().to_be_bytes());
    b.extend_from_slice(&record.owner.store.incarnation.get().to_be_bytes());
    b.extend_from_slice(&record.request.operation.get().to_be_bytes());
    b.push(match record.phase {
        DrainPhase::Active => 1,
        DrainPhase::Cancelled => 2,
    });
    b.extend_from_slice(&[0; 7]);
    b.extend_from_slice(&(record.request.groups.len() as u64).to_be_bytes());
    for group in &record.request.groups {
        b.extend_from_slice(&group.group.id.get().to_be_bytes());
        b.extend_from_slice(&group.group.incarnation.get().to_be_bytes());
        b.extend_from_slice(&group.configuration.get().to_be_bytes());
    }
    let sum = digest(&SHA256, &b);
    b.extend_from_slice(sum.as_ref());
    b
}
pub(super) fn decode(b: &[u8]) -> Result<DrainRecord, DrainJournalError> {
    let invalid = DrainJournalError::InvalidRecord;
    if b.len() < 112
        || b.len() > MAX_DRAIN_RECORD_BYTES
        || &b[..8] != b"VBDR0001"
        || b[65..72] != [0; 7]
        || digest(&SHA256, &b[..b.len() - 32]).as_ref() != &b[b.len() - 32..]
    {
        return Err(invalid);
    }
    let u64_at = |i| u64::from_be_bytes(b[i..i + 8].try_into().unwrap());
    let u128_at = |i| u128::from_be_bytes(b[i..i + 16].try_into().unwrap());
    let count = usize::try_from(u64_at(72)).map_err(|_| invalid)?;
    if count > crate::runtime::MAX_LOCAL_DRAIN_GROUPS || b.len() != 112 + count * 32 {
        return Err(invalid);
    }
    let mut groups = Vec::with_capacity(count);
    for i in 0..count {
        let at = 80 + i * 32;
        groups.push(DrainGroup {
            group: GroupIdentity {
                id: GroupId::new(u128_at(at)).ok_or(invalid)?,
                incarnation: GroupIncarnation::new(u64_at(at + 16)).ok_or(invalid)?,
            },
            configuration: ConfigurationId::new(u64_at(at + 24)).ok_or(invalid)?,
        });
    }
    let r = DrainRecord {
        owner: PeerIdentity {
            node: NodeId::new(u64_at(16)).ok_or(invalid)?,
            store: StoreIdentity {
                id: StoreId::new(u128_at(24)).ok_or(invalid)?,
                incarnation: StoreIncarnation::new(u64_at(40)).ok_or(invalid)?,
            },
        },
        sequence: u64_at(8),
        request: LocalDrainRequest {
            operation: OperationId::new(u128_at(48)).ok_or(invalid)?,
            groups,
        },
        phase: match b[64] {
            1 => DrainPhase::Active,
            2 => DrainPhase::Cancelled,
            _ => return Err(invalid),
        },
    };
    r.validate()?;
    Ok(r)
}
