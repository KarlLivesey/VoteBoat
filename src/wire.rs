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
//! Bounded peer framing contract. Integrity checks and claimed identifiers do
//! not authenticate a connection; the transport supplies a trusted peer scope.
use crate::{identity::*, quorum::Limits as PolicyLimits, raft::Message};
use std::mem::size_of;

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub struct WireScope {
    pub from: NodeId,
    pub sender: StoreBinding,
    pub to: NodeId,
}
impl WireScope {
    pub fn matches(self, message: &Message) -> bool {
        self.from == message.from && self.sender == message.sender && self.to == message.to
    }
}
#[derive(Clone, Copy, Debug)]
pub struct WireLimits {
    pub max_frame_bytes: usize,
    pub max_messages: usize,
    pub max_entries_per_message: usize,
    pub max_command_bytes: usize,
    pub max_snapshot_bytes: usize,
    /// Conservative retained decoded objects/payloads, not exact allocator RSS.
    pub max_decoded_bytes: usize,
    pub policy: PolicyLimits,
}
impl Default for WireLimits {
    fn default() -> Self {
        Self {
            max_frame_bytes: 1024 * 1024,
            max_messages: 128,
            max_entries_per_message: 1024,
            max_command_bytes: 64 * 1024,
            max_snapshot_bytes: 512 * 1024,
            max_decoded_bytes: 4 * 1024 * 1024,
            policy: PolicyLimits::default(),
        }
    }
}
impl WireLimits {
    pub fn validate(self) -> Result<Self, WireError> {
        if self.max_frame_bytes == 0
            || self.max_frame_bytes > 64 * 1024 * 1024
            || self.max_messages == 0
            || self.max_messages > 4096
            || self.max_entries_per_message == 0
            || self.max_entries_per_message > 16384
            || self.max_command_bytes == 0
            || self.max_command_bytes > self.max_frame_bytes
            || self.max_snapshot_bytes == 0
            || self.max_snapshot_bytes > self.max_frame_bytes
            || self.max_decoded_bytes < size_of::<Message>()
            || self.max_decoded_bytes > 256 * 1024 * 1024
            || self.policy.max_depth > 32
            || self.policy.max_voters == 0
            || self.policy.max_voters > 4096
            || self.policy.max_tree_nodes == 0
            || self.policy.max_tree_nodes > 16384
        {
            return Err(WireError::InvalidLimits);
        }
        Ok(self)
    }
}
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum WireError {
    UnsupportedConfigurationAdmission,
    InvalidLimits,
    UnsupportedVersion(u16),
    UnsupportedFlags,
    TooLarge,
    Truncated,
    Corrupt(&'static str),
    InvalidMessage(&'static str),
    WrongPeer,
}
/// Borrowed cold-path admission input. The index is the next local log position;
/// requirements describe the command/checkpoint envelope to be supported.
pub struct ConfigurationWireRequirements<'a> {
    pub bootstrap: &'a crate::log::Bootstrap,
    pub current: &'a crate::membership::Membership,
    pub record: &'a crate::membership::ConfigurationRecord,
    pub index: u64,
    pub committed_index: u64,
    pub application: crate::raft::ReadinessRequirements,
}
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub struct WireFootprint {
    pub frame_bytes: usize,
    pub decoded_bytes: usize,
}
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub struct ConfigurationWireCapacity {
    pub wire_version: u16,
    pub append: WireFootprint,
    pub command: WireFootprint,
    pub snapshot: WireFootprint,
}
/// Pure bounded encoding/decoding; no I/O, queue, clock or cryptography owner.
/// Providers name their wire version and fixed prefix size. The caller chooses
/// compatible formats before traffic and reads only that bounded prefix first.
/// `frame_length` validates it before frame-buffer allocation; `decode_batch`
/// accepts exactly one complete frame and validates counts before allocating
/// decoded objects. Invalid input returns no partial message batch.
///
/// Every message in a batch belongs to `scope`; different groups may share it.
/// Decode scope comes from the authenticated connection, never claimed bytes.
/// This check is consistency, not authentication. The core still checks term,
/// configuration, membership, incarnation and exact durable dependencies.
/// CRC or local decode success is not quorum evidence. Closed/failed connection
/// handling, partial stream buffers and their credits belong to transport.
/// Encoding borrows messages; transport retains the owned input until local
/// completion and separately budgets the encoded buffer. Decoded output moves
/// into separately bounded ingress. No persistent/wire compatibility fallback.
pub trait WireCodec {
    /// Pure bounded sizing/validation, without accepting work or allocating
    /// application-sized buffers. Includes the prospective membership base and
    /// retained operation IDs. Success is capacity evidence, never commitment.
    fn configuration_capacity(
        &self,
        _required: &ConfigurationWireRequirements<'_>,
    ) -> Result<ConfigurationWireCapacity, WireError> {
        Err(WireError::UnsupportedConfigurationAdmission)
    }
    fn format_version(&self) -> u16;
    fn header_bytes(&self) -> usize;
    fn limits(&self) -> WireLimits;
    fn frame_length(&self, header: &[u8]) -> Result<usize, WireError>;
    fn encode_batch(&self, scope: WireScope, messages: &[Message]) -> Result<Vec<u8>, WireError>;
    /// Compatibility default uses a temporary bounded encoded Vec. Providers
    /// supporting direct provisioning override both methods (native does).
    fn encoded_length(&self, scope: WireScope, messages: &[Message]) -> Result<usize, WireError> {
        let bytes = self.encode_batch(scope, messages)?;
        if bytes.capacity() > self.limits().max_frame_bytes {
            return Err(WireError::TooLarge);
        }
        Ok(bytes.len())
    }
    /// Encode exactly the sized frame into caller-owned storage. On error the
    /// destination may be modified, but no frame may be sent. Default allocates
    /// temporary encoding storage within the codec's independent frame limit.
    fn encode_into(
        &self,
        scope: WireScope,
        messages: &[Message],
        destination: &mut [u8],
    ) -> Result<(), WireError> {
        let bytes = self.encode_batch(scope, messages)?;
        if bytes.capacity() > self.limits().max_frame_bytes || bytes.len() != destination.len() {
            return Err(WireError::TooLarge);
        }
        destination.copy_from_slice(&bytes);
        Ok(())
    }
    fn decode_batch(&self, scope: WireScope, frame: &[u8]) -> Result<Vec<Message>, WireError>;
}
