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
use crate::{
    authorization::CredentialGeneration, credential_reload::*, identity::*, secure::PeerIdentity,
};
use ring::digest::{digest, SHA256};
pub(super) fn validate(r: CredentialReloadRecord) -> Result<(), CredentialJournalError> {
    if r.request.sequence == 0 || r.request.replacement <= r.request.expected {
        return Err(CredentialJournalError::InvalidRecord);
    }
    Ok(())
}
pub(super) fn encode(r: CredentialReloadRecord) -> [u8; 128] {
    let mut b = [0; 128];
    b[..8].copy_from_slice(b"VBCR0001");
    b[8..16].copy_from_slice(&r.owner.node.get().to_be_bytes());
    b[16..32].copy_from_slice(&r.owner.store.id.get().to_be_bytes());
    b[32..40].copy_from_slice(&r.owner.store.incarnation.get().to_be_bytes());
    b[40..48].copy_from_slice(&r.request.sequence.to_be_bytes());
    b[48..56].copy_from_slice(&r.request.expected.get().to_be_bytes());
    b[56..64].copy_from_slice(&r.request.replacement.get().to_be_bytes());
    b[64..96].copy_from_slice(&r.digest);
    let checksum = digest(&SHA256, &b[..96]);
    b[96..].copy_from_slice(checksum.as_ref());
    b
}
pub(super) fn decode(b: [u8; 128]) -> Result<CredentialReloadRecord, CredentialJournalError> {
    let invalid = CredentialJournalError::InvalidRecord;
    if &b[..8] != b"VBCR0001" || digest(&SHA256, &b[..96]).as_ref() != &b[96..] {
        return Err(invalid);
    }
    let u64_at = |i| u64::from_be_bytes(b[i..i + 8].try_into().unwrap());
    let r = CredentialReloadRecord {
        owner: PeerIdentity {
            node: NodeId::new(u64_at(8)).ok_or(invalid)?,
            store: StoreIdentity {
                id: StoreId::new(u128::from_be_bytes(b[16..32].try_into().unwrap()))
                    .ok_or(invalid)?,
                incarnation: StoreIncarnation::new(u64_at(32)).ok_or(invalid)?,
            },
        },
        request: CredentialReloadRequest {
            sequence: u64_at(40),
            expected: CredentialGeneration::new(u64_at(48)).ok_or(invalid)?,
            replacement: CredentialGeneration::new(u64_at(56)).ok_or(invalid)?,
        },
        digest: b[64..96].try_into().unwrap(),
    };
    validate(r)?;
    Ok(r)
}
