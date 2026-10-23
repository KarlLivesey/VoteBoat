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
use super::*;
use crate::logical_voter::codec::{Decoder, Encoder};
const MAGIC: &[u8; 8] = b"VBLOGN01";
pub const LOGICAL_FRAME_HEADER: usize = 16;
fn checked_length(header: &[u8], maximum: usize) -> Result<usize, LogicalNetworkError> {
    if header.len() != LOGICAL_FRAME_HEADER || &header[..8] != MAGIC {
        return Err(LogicalNetworkError::InvalidFrame);
    }
    usize::try_from(u64::from_le_bytes(header[8..16].try_into().unwrap()))
        .ok()
        .filter(|n| *n >= LOGICAL_FRAME_HEADER && *n <= maximum)
        .ok_or(LogicalNetworkError::TooLarge)
}
/// Separate negotiated framing around the original exact Authorize command.
/// Remote metadata is authenticated representative data, never a local release permit.
pub struct LogicalFrameCodec<C = NativeLogCodec, S = NativeSnapshotCodec, W = NativeWireCodec> {
    pub(super) plan: LogicalPeerPlan<C, S, W>,
    pub(super) limits: LogicalNetworkLimits,
}
impl<C: LogCodec + Clone, S: SnapshotCodec + Clone, W: WireCodec + Clone>
    LogicalFrameCodec<C, S, W>
{
    pub fn new(
        plan: LogicalPeerPlan<C, S, W>,
        limits: LogicalNetworkLimits,
    ) -> Result<Self, LogicalNetworkError> {
        let limits = limits.validate()?;
        if plan.local.application.limits.max_command_bytes > limits.frame_bytes - 512
            || plan.peers.iter().any(|p| {
                p.application.limits.max_command_bytes > limits.frame_bytes - 512
                    || p.application.wire_codec.limits().max_decoded_bytes > limits.decoded_bytes
            })
        {
            return Err(LogicalNetworkError::InvalidLimits);
        }
        Ok(Self { plan, limits })
    }
    pub fn frame_length(&self, header: &[u8]) -> Result<usize, LogicalNetworkError> {
        checked_length(header, self.limits.frame_bytes)
    }
    pub(super) fn encode(
        &self,
        send: &AuthorizedLogicalSend,
    ) -> Result<Vec<u8>, LogicalNetworkError> {
        let proof = send.provenance();
        if !self.plan.authorization(send.authorization())
            || proof.mapping() != self.plan.local.mapping
            || proof.representative() != self.plan.local.representative
            || !matches!(&proof.capsule().body,
                LogicalCapsuleBody::Protocol(message) if message.as_ref() == send.message())
        {
            return Err(LogicalNetworkError::WrongBinding);
        }
        let mut e = Encoder::new(self.limits.frame_bytes);
        let result = (|| {
            e.put(MAGIC)?;
            e.u64(0)?;
            e.owner(proof.child_owner())?;
            let r = proof.receipt();
            e.u64(r.index)?;
            e.u128(r.operation.get())?;
            e.u64(r.first_index)?;
            e.u64(r.parent_revision.map_or(0, LogRevision::get))?;
            e.u64(r.parent_generation.map_or(0, LogGeneration::get))?;
            e.u64(r.epoch.map_or(0, RepresentativeEpoch::get))?;
            e.u8(u8::from(r.duplicate))?;
            e.blob(proof.command_bytes())
        })();
        result.map_err(|_| LogicalNetworkError::TooLarge)?;
        let len = e.bytes.len() as u64;
        e.bytes[8..16].copy_from_slice(&len.to_le_bytes());
        Ok(e.bytes.into_boxed_slice().into_vec())
    }
    pub(super) fn decode(
        &self,
        physical: LocalIdentity,
        frame: &[u8],
        authorization: RuntimeAuthorization,
    ) -> Result<LogicalReceived, LogicalNetworkError> {
        if !self.plan.authorization(authorization) {
            return Err(LogicalNetworkError::StaleOwner);
        }
        let peer = self
            .plan
            .peer(physical)
            .ok_or(LogicalNetworkError::WrongPeer)?;
        if frame.len() < LOGICAL_FRAME_HEADER
            || self.frame_length(&frame[..LOGICAL_FRAME_HEADER])? != frame.len()
        {
            return Err(LogicalNetworkError::InvalidFrame);
        }
        let mut d = Decoder::new(&frame[LOGICAL_FRAME_HEADER..]);
        let (child_owner, receipt, command) = (|| {
            let child_owner = d.owner()?;
            let receipt = LogicalReceipt {
                index: d.sequence()?,
                operation: d.id128(OperationId::new)?,
                first_index: d.sequence()?,
                parent_revision: Some(d.id(LogRevision::new)?),
                parent_generation: Some(d.id(LogGeneration::new)?),
                epoch: Some(d.id(RepresentativeEpoch::new)?),
                duplicate: d.flag()?,
                outcome: LogicalOutcome::CapsuleAuthorized,
            };
            let command = peer
                .application
                .decode_command(d.blob(peer.application.limits.max_command_bytes)?)?;
            d.finish()?;
            Ok::<_, ApplicationError>((child_owner, receipt, command))
        })()
        .map_err(|_| LogicalNetworkError::InvalidFrame)?;
        self.validate(peer, child_owner, receipt, command, authorization)
    }
    fn validate(
        &self,
        peer: &LogicalPeerDescriptor<C, S, W>,
        child_owner: RuntimeOwner,
        r: LogicalReceipt,
        c: Command,
        a: RuntimeAuthorization,
    ) -> Result<LogicalReceived, LogicalNetworkError> {
        let CommandBody::Authorize(capsule) = c.body else {
            return Err(LogicalNetworkError::InvalidFrame);
        };
        let LogicalCapsuleOrigin::Protocol(effect) = capsule.origin else {
            return Err(LogicalNetworkError::InvalidFrame);
        };
        let LogicalCapsuleBody::Protocol(message) = capsule.body else {
            return Err(LogicalNetworkError::InvalidFrame);
        };
        if c.representative != peer.representative
            || r.operation != c.operation
            || r.first_index <= peer.claim_index
            || r.index < r.first_index
            || r.epoch != Some(peer.representative.epoch)
            || r.parent_revision != Some(capsule.expected_revision)
            || r.parent_generation != Some(capsule.expected_generation)
            || !peer
                .membership
                .stable()
                .voter_stores()
                .values()
                .any(|s| *s == child_owner.store.identity)
            || effect.visit.owner.store != peer.logical()
            || effect.visit.group != peer.mapping.parent
            || effect.sequence == 0
            || effect.visit.sequence == 0
            || message.group != peer.mapping.parent
            || message.from != peer.mapping.voter
            || message.sender != peer.logical()
            || message.to != self.plan.local.mapping.voter
            || !self.plan.context(message.context.origin)
        {
            return Err(LogicalNetworkError::InvalidFrame);
        }
        message_cost(&message, self.limits.decoded_bytes)
            .map_err(|_| LogicalNetworkError::TooLarge)?;
        Ok(LogicalReceived {
            message: *message,
            authorization: a,
            physical: peer.physical(),
            receipt: r,
        })
    }
}
pub struct LogicalReceived {
    message: Message,
    authorization: RuntimeAuthorization,
    physical: LocalIdentity,
    receipt: LogicalReceipt,
}
impl LogicalReceived {
    pub fn message(&self) -> &Message {
        &self.message
    }
    pub fn physical(&self) -> LocalIdentity {
        self.physical
    }
    pub fn receipt(&self) -> LogicalReceipt {
        self.receipt
    }
    /// Consumes authenticated input under its exact still-current local owner.
    /// Normal parent Raft ingress remains responsible for term/prefix/quorum rules.
    pub fn into_message(
        self,
        current: RuntimeAuthorization,
    ) -> Result<Message, LogicalReceivedRejected> {
        if current != self.authorization {
            return Err(LogicalReceivedRejected {
                reason: LogicalNetworkError::StaleOwner,
                received: Box::new(self),
            });
        }
        Ok(self.message)
    }
}
pub struct LogicalReceivedRejected {
    pub reason: LogicalNetworkError,
    pub received: Box<LogicalReceived>,
}

#[cfg(test)]
mod tests {
    use super::*;
    fn header(n: u64) -> Vec<u8> {
        let mut bytes = MAGIC.to_vec();
        bytes.extend_from_slice(&n.to_le_bytes());
        bytes
    }
    #[test]
    fn fixed_prefix_is_checked_before_declared_allocation() {
        let maximum = 1024;
        let valid = header(maximum as u64);
        for n in 0..LOGICAL_FRAME_HEADER {
            assert_eq!(
                checked_length(&valid[..n], maximum),
                Err(LogicalNetworkError::InvalidFrame)
            );
        }
        assert_eq!(checked_length(&valid, maximum), Ok(maximum));
        assert_eq!(
            checked_length(&header(LOGICAL_FRAME_HEADER as u64), maximum),
            Ok(LOGICAL_FRAME_HEADER)
        );
        for n in [0, 1, 15, 1025, u64::MAX] {
            assert_eq!(
                checked_length(&header(n), maximum),
                Err(LogicalNetworkError::TooLarge)
            );
        }
        let mut wrong = valid.clone();
        wrong[0] ^= 1;
        assert_eq!(
            checked_length(&wrong, maximum),
            Err(LogicalNetworkError::InvalidFrame)
        );
        let mut trailing = valid;
        trailing.push(0);
        assert_eq!(
            checked_length(&trailing, maximum),
            Err(LogicalNetworkError::InvalidFrame)
        );
    }
}
