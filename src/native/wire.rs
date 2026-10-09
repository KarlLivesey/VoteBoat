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
//! Native VBWIRE01 frames, independent of the persistent WAL/snapshot formats.
use super::vote_store::crc32c;
use crate::{
    identity::*,
    log::*,
    membership::{
        Configuration, ConfigurationChange, ConfigurationRecord, JointConfiguration, Membership,
        MAX_CONFIGURATION_OPERATIONS,
    },
    quorum::{Policy, Tree, WeightedChild},
    raft::*,
    snapshot::{Snapshot, SnapshotMetadata},
    wire::*,
};
use std::{
    collections::{BTreeMap, BTreeSet},
    mem::size_of,
};

const HEADER: usize = 24;
const OVERHEAD: usize = HEADER + 4;
const MIN_MESSAGE: usize = 129;
const MAGIC: &[u8; 8] = b"VBWIRE01";
mod capacity;
#[derive(Clone, Copy, Debug)]
pub struct NativeWireCodec {
    limits: WireLimits,
    version: u16,
}
impl NativeWireCodec {
    /// Static-configuration format 1, retained for existing peer assemblies.
    pub fn new(limits: WireLimits) -> Result<Self, WireError> {
        Self::versioned(limits, 1)
    }
    /// Explicit format 2 selection: configuration records and membership bases.
    /// This codec capability does not enable online changes in the Raft core.
    pub fn with_membership(limits: WireLimits) -> Result<Self, WireError> {
        Self::versioned(limits, 2)
    }
    /// Explicit format 3: membership records and direct witness authorization.
    /// The host must negotiate this capability; native sessions default to 1.
    pub fn with_authority(limits: WireLimits) -> Result<Self, WireError> {
        Self::versioned(limits, 3)
    }
    /// Explicit format 4 adds learner readiness; sessions still default to 1.
    pub fn with_readiness(limits: WireLimits) -> Result<Self, WireError> {
        Self::versioned(limits, 4)
    }
    /// Explicit format 5 adds bounded pre-election learner repair.
    pub fn with_learner_repair(limits: WireLimits) -> Result<Self, WireError> {
        Self::versioned(limits, 5)
    }
    /// Explicit format 6 adds historical committed checkpoint learner repair.
    pub fn with_snapshot_repair(limits: WireLimits) -> Result<Self, WireError> {
        Self::versioned(limits, 6)
    }
    fn versioned(limits: WireLimits, version: u16) -> Result<Self, WireError> {
        if limits.max_frame_bytes < OVERHEAD + MIN_MESSAGE + 4 {
            return Err(WireError::InvalidLimits);
        }
        Ok(Self {
            limits: limits.validate()?,
            version,
        })
    }
}
struct Budget {
    used: usize,
    max: usize,
}
impl Budget {
    fn charge(&mut self, bytes: usize) -> Result<(), WireError> {
        self.used = self
            .used
            .checked_add(bytes)
            .filter(|v| *v <= self.max)
            .ok_or(WireError::TooLarge)?;
        Ok(())
    }
    fn array<T>(&mut self, count: usize) -> Result<(), WireError> {
        self.charge(
            count
                .checked_mul(size_of::<T>())
                .ok_or(WireError::TooLarge)?,
        )
    }
}
fn bool_value(value: u8) -> Result<bool, WireError> {
    match value {
        0 => Ok(false),
        1 => Ok(true),
        _ => Err(WireError::InvalidMessage("boolean")),
    }
}
fn boundary(index: u64, term: u64, current: u64) -> Result<(), WireError> {
    if (index == 0) != (term == 0) || term > current {
        return Err(WireError::InvalidMessage("log boundary"));
    }
    Ok(())
}
fn validate_message(m: &Message, version: u16) -> Result<(), WireError> {
    if m.from == m.to || m.term == 0 || m.context.sequence == 0 {
        return Err(WireError::InvalidMessage("message scope/term/context"));
    }
    match &m.rpc {
        Rpc::LearnerRepair {
            joint,
            previous_index,
            previous_term,
            entries,
        } => {
            if version < 5
                || m.context.origin != m.sender
                || entries.is_empty()
                || !matches!(&joint.payload, EntryPayload::Configuration(record)
                    if matches!(&record.change, ConfigurationChange::Joint { id, .. } if *id == m.configuration))
            {
                return Err(WireError::InvalidMessage("learner repair scope/version"));
            }
            boundary(*previous_index, *previous_term, m.term)?;
            if joint.index == 0 || joint.term == 0 || joint.term > m.term {
                return Err(WireError::InvalidMessage("learner repair joint"));
            }
            let mut index = *previous_index;
            let mut term = *previous_term;
            for entry in entries {
                index = index.checked_add(1).ok_or(WireError::TooLarge)?;
                if entry.index != index
                    || entry.index > joint.index
                    || entry.term == 0
                    || entry.term < term
                    || entry.term > m.term
                    || (matches!(entry.payload, EntryPayload::Configuration(_))
                        && entry != joint.as_ref())
                    || (entry.index == joint.index && entry != joint.as_ref())
                {
                    return Err(WireError::InvalidMessage("learner repair range"));
                }
                term = entry.term;
            }
        }
        Rpc::LearnerRepaired {
            matching_index,
            matching_term,
            ..
        } => {
            if version < 5 {
                return Err(WireError::UnsupportedVersion(version));
            }
            boundary(*matching_index, *matching_term, m.term)?;
        }
        Rpc::LearnerReadinessRequest(r) | Rpc::LearnerReadinessReply { request: r, .. } => {
            let query = matches!(m.rpc, Rpc::LearnerReadinessRequest(_));
            if version < 4
                || r.group != m.group
                || r.configuration != m.configuration
                || r.context != m.context
                || r.term != m.term
                || r.leader != if query { m.from } else { m.to }
                || r.learner.node != if query { m.to } else { m.from }
                || (query && r.context.origin != m.sender)
                || (!query
                    && (r.learner.store != m.sender.identity || r.session != m.sender.session))
                || r.index == 0
                || r.index_term == 0
                || r.requirements.application_schema == 0
                || r.requirements.command_bytes == 0
                || r.requirements.snapshot_bytes == 0
            {
                return Err(WireError::InvalidMessage(
                    "readiness scope/version/capabilities",
                ));
            }
            boundary(r.index, r.index_term, r.term)?;
        }
        Rpc::AuthorityRequest {
            candidate,
            configuration,
        }
        | Rpc::AuthorityReply {
            candidate,
            configuration,
            ..
        } => {
            if version < 3
                || candidate.node == m.from
                || candidate.node == m.to
                || *configuration <= m.configuration
            {
                return Err(WireError::InvalidMessage("authority scope/version"));
            }
            let request = matches!(m.rpc, Rpc::AuthorityRequest { .. });
            if request && m.context.origin != m.sender {
                return Err(WireError::InvalidMessage("authority context"));
            }
            if let Rpc::AuthorityReply {
                committed_index,
                committed_term,
                granted,
                ..
            } = &m.rpc
            {
                if (*granted && (*committed_index == 0 || *committed_term == 0))
                    || (!*granted && (*committed_index != 0 || *committed_term != 0))
                {
                    return Err(WireError::InvalidMessage("authority boundary"));
                }
                boundary(*committed_index, *committed_term, m.term)?;
            }
        }
        Rpc::Vote {
            last_index,
            last_term,
        } => boundary(*last_index, *last_term, m.term)?,
        Rpc::Append {
            previous_index,
            previous_term,
            entries,
            ..
        } => {
            boundary(*previous_index, *previous_term, m.term)?;
            let mut index = *previous_index;
            let mut term = *previous_term;
            for entry in entries {
                index = index
                    .checked_add(1)
                    .ok_or(WireError::InvalidMessage("entry index overflow"))?;
                if entry.index != index
                    || entry.term == 0
                    || entry.term < term
                    || entry.term > m.term
                {
                    return Err(WireError::InvalidMessage("entry ordering/term"));
                }
                term = entry.term;
            }
        }
        Rpc::Snapshot { snapshot } | Rpc::LearnerRepairSnapshot { snapshot } => {
            if matches!(m.rpc, Rpc::LearnerRepairSnapshot { .. })
                && (version < 6
                    || snapshot.metadata.membership.is_none()
                    || m.context.origin != m.sender)
            {
                return Err(WireError::InvalidMessage("snapshot repair version/context"));
            }
            let meta = &snapshot.metadata;
            if meta.bootstrap.group != m.group
                || (version == 1 && meta.bootstrap.configuration != m.configuration)
                || meta.validate().is_err()
                || meta.index == 0
                || meta.index == u64::MAX
                || meta.term == 0
                || meta.term > m.term
                || meta.application_schema == 0
                || snapshot.application.is_empty()
                || !meta
                    .bootstrap
                    .voter_stores
                    .keys()
                    .eq(meta.bootstrap.policy.voters().iter())
            {
                return Err(WireError::InvalidMessage(
                    "snapshot scope/boundary/membership",
                ));
            }
        }
        Rpc::SnapshotAck { index } if *index == 0 => {
            return Err(WireError::InvalidMessage("snapshot ack boundary"))
        }
        Rpc::Compacted { index, term } => {
            if *index == 0 {
                return Err(WireError::InvalidMessage("compacted boundary"));
            }
            boundary(*index, *term, m.term)?;
        }
        _ => (),
    }
    Ok(())
}
struct Encoder<'a> {
    data: Option<&'a mut [u8]>,
    len: usize,
    limits: WireLimits,
    budget: Budget,
    version: u16,
}
impl<'a> Encoder<'a> {
    fn new(limits: WireLimits, data: Option<&'a mut [u8]>, version: u16) -> Self {
        Self {
            data,
            version,
            len: 0,
            limits,
            budget: Budget {
                used: 0,
                max: limits.max_decoded_bytes,
            },
        }
    }
    fn put(&mut self, bytes: &[u8]) -> Result<(), WireError> {
        self.len = self
            .len
            .checked_add(bytes.len())
            .filter(|v| *v <= self.limits.max_frame_bytes)
            .ok_or(WireError::TooLarge)?;
        if let Some(data) = &mut self.data {
            let start = self.len - bytes.len();
            data.get_mut(start..self.len)
                .ok_or(WireError::TooLarge)?
                .copy_from_slice(bytes);
        }
        Ok(())
    }
    fn u8(&mut self, v: u8) -> Result<(), WireError> {
        self.put(&[v])
    }
    fn u32(&mut self, v: u32) -> Result<(), WireError> {
        self.put(&v.to_le_bytes())
    }
    fn u64(&mut self, v: u64) -> Result<(), WireError> {
        self.put(&v.to_le_bytes())
    }
    fn u128(&mut self, v: u128) -> Result<(), WireError> {
        self.put(&v.to_le_bytes())
    }
    fn binding(&mut self, b: StoreBinding) -> Result<(), WireError> {
        self.u128(b.identity.id.get())?;
        self.u64(b.identity.incarnation.get())?;
        self.u64(b.session.get())
    }
    fn peer(&mut self, peer: crate::secure::PeerIdentity) -> Result<(), WireError> {
        self.u64(peer.node.get())?;
        self.u128(peer.store.id.get())?;
        self.u64(peer.store.incarnation.get())
    }
    fn tree(
        &mut self,
        tree: &Tree,
        depth: usize,
        nodes: &mut usize,
        voters: &mut usize,
    ) -> Result<(), WireError> {
        *nodes += 1;
        if depth > self.limits.policy.max_depth || *nodes > self.limits.policy.max_tree_nodes {
            return Err(WireError::TooLarge);
        }
        match tree {
            Tree::Voter(n) => {
                *voters += 1;
                if *voters > self.limits.policy.max_voters {
                    return Err(WireError::TooLarge);
                }
                self.u8(0)?;
                self.u64(n.get())
            }
            Tree::Majority(children) => {
                self.budget.array::<Tree>(children.len())?;
                self.u8(1)?;
                self.u32(children.len() as u32)?;
                for child in children {
                    self.tree(child, depth + 1, nodes, voters)?;
                }
                Ok(())
            }
            Tree::Weighted(children) => {
                self.budget.array::<WeightedChild>(children.len())?;
                self.u8(2)?;
                self.u32(children.len() as u32)?;
                for child in children {
                    self.u64(child.weight)?;
                    self.tree(&child.node, depth + 1, nodes, voters)?;
                }
                Ok(())
            }
        }
    }
    fn stores(&mut self, stores: &BTreeMap<NodeId, StoreIdentity>) -> Result<(), WireError> {
        if stores.len() > self.limits.policy.max_voters {
            return Err(WireError::TooLarge);
        }
        self.budget.charge(stores.len() * 192)?;
        self.u32(stores.len() as u32)?;
        for (node, store) in stores {
            self.u64(node.get())?;
            self.u128(store.id.get())?;
            self.u64(store.incarnation.get())?;
        }
        Ok(())
    }
    fn configuration(&mut self, configuration: &Configuration) -> Result<(), WireError> {
        if configuration
            .voter_stores()
            .len()
            .saturating_add(configuration.learners().len())
            > self.limits.policy.max_voters
        {
            return Err(WireError::TooLarge);
        }
        self.budget.charge(size_of::<Configuration>())?;
        self.u64(configuration.id().get())?;
        self.tree(configuration.policy().tree(), 0, &mut 0, &mut 0)?;
        self.budget
            .charge(configuration.policy().voters().len() * 128)?;
        self.stores(configuration.voter_stores())?;
        self.stores(configuration.learners())
    }
    fn record(&mut self, record: &ConfigurationRecord) -> Result<(), WireError> {
        if self.version < 2 {
            return Err(WireError::InvalidMessage(
                "configuration requires wire format 2",
            ));
        }
        if record.retained_bytes() > self.limits.max_command_bytes {
            return Err(WireError::TooLarge);
        }
        self.budget.charge(size_of::<ConfigurationRecord>())?;
        self.u8(1)?; // mandatory configuration subformat
        self.u128(record.operation.get())?;
        self.u64(record.expected.get())?;
        match &record.change {
            ConfigurationChange::Learners(next) => {
                self.u8(0)?;
                self.configuration(next)
            }
            ConfigurationChange::Joint { id, next } => {
                self.u8(1)?;
                self.u64(id.get())?;
                self.configuration(next)
            }
            ConfigurationChange::Final { id } => {
                self.u8(2)?;
                self.u64(id.get())
            }
        }
    }
    fn membership(&mut self, membership: &Membership) -> Result<(), WireError> {
        self.budget.charge(size_of::<Membership>())?;
        self.u8(1)?; // mandatory membership subformat
        self.configuration(membership.stable())?;
        self.u64(membership.last_configuration_index())?;
        if let Some(joint) = membership.joint() {
            self.u8(1)?;
            self.u128(joint.operation.get())?;
            self.u64(joint.id.get())?;
            self.u64(joint.index)?;
            self.configuration(&joint.next)?;
        } else {
            self.u8(0)?;
        }
        if membership.operations().len() > MAX_CONFIGURATION_OPERATIONS {
            return Err(WireError::TooLarge);
        }
        self.budget.charge(membership.operations().len() * 192)?;
        self.u32(membership.operations().len() as u32)?;
        for operation in membership.operations() {
            self.u128(operation.get())?;
        }
        Ok(())
    }
    fn entry(&mut self, entry: &LogEntry) -> Result<(), WireError> {
        self.u64(entry.index)?;
        self.u64(entry.term)?;
        match &entry.payload {
            EntryPayload::Noop => self.u8(0)?,
            EntryPayload::Configuration(record) => {
                self.u8(2)?;
                self.record(record)?;
            }
            EntryPayload::Command { operation, bytes } => {
                if bytes.len() > self.limits.max_command_bytes {
                    return Err(WireError::TooLarge);
                }
                self.budget.charge(bytes.len())?;
                self.u8(1)?;
                self.u128(operation.get())?;
                self.u32(bytes.len() as u32)?;
                self.put(bytes)?;
            }
        }
        Ok(())
    }
    fn message(&mut self, m: &Message) -> Result<(), WireError> {
        // Bound traversals as well as allocations before semantic validation.
        match &m.rpc {
            Rpc::Append { entries, .. } | Rpc::LearnerRepair { entries, .. }
                if entries.len() > self.limits.max_entries_per_message =>
            {
                return Err(WireError::TooLarge)
            }
            Rpc::Snapshot { snapshot } | Rpc::LearnerRepairSnapshot { snapshot }
                if snapshot.application.len() > self.limits.max_snapshot_bytes
                    || snapshot.metadata.bootstrap.policy.voters().len()
                        > self.limits.policy.max_voters
                    || snapshot.metadata.bootstrap.voter_stores.len()
                        > self.limits.policy.max_voters
                    || snapshot
                        .metadata
                        .membership
                        .as_ref()
                        .is_some_and(|m| m.retained_bytes() > self.limits.max_decoded_bytes) =>
            {
                return Err(WireError::TooLarge)
            }
            _ => (),
        }
        validate_message(m, self.version)?;
        self.u128(m.group.id.get())?;
        self.u64(m.group.incarnation.get())?;
        self.u64(m.configuration.get())?;
        self.u64(m.from.get())?;
        self.binding(m.sender)?;
        self.u64(m.to.get())?;
        self.u64(m.term)?;
        self.binding(m.context.origin)?;
        self.u64(m.context.sequence)?;
        match &m.rpc {
            Rpc::LearnerRepair {
                joint,
                previous_index,
                previous_term,
                entries,
            } => {
                self.u8(14)?;
                self.budget.charge(size_of::<LogEntry>())?;
                self.entry(joint)?;
                self.u64(*previous_index)?;
                self.u64(*previous_term)?;
                self.budget.array::<LogEntry>(entries.len())?;
                self.u32(entries.len() as u32)?;
                for entry in entries {
                    self.entry(entry)?;
                }
                Ok(())
            }
            Rpc::LearnerRepaired {
                success,
                matching_index,
                matching_term,
            } => {
                self.u8(15)?;
                self.u8(u8::from(*success))?;
                self.u64(*matching_index)?;
                self.u64(*matching_term)
            }
            Rpc::LearnerReadinessRequest(r) | Rpc::LearnerReadinessReply { request: r, .. } => {
                self.budget.charge(size_of::<LearnerReadinessRequest>())?;
                self.u8(if matches!(m.rpc, Rpc::LearnerReadinessRequest(_)) {
                    12
                } else {
                    13
                })?;
                self.peer(r.learner)?;
                self.u64(r.session.get())?;
                self.u64(r.index)?;
                self.u64(r.index_term)?;
                self.u64(r.requirements.application_schema)?;
                self.u64(
                    u64::try_from(r.requirements.command_bytes).map_err(|_| WireError::TooLarge)?,
                )?;
                self.u64(
                    u64::try_from(r.requirements.snapshot_bytes)
                        .map_err(|_| WireError::TooLarge)?,
                )?;
                if let Rpc::LearnerReadinessReply { ready, .. } = &m.rpc {
                    self.u8(u8::from(*ready))?;
                }
                Ok(())
            }
            Rpc::Vote {
                last_index,
                last_term,
            } => {
                self.u8(0)?;
                self.u64(*last_index)?;
                self.u64(*last_term)
            }
            Rpc::Voted { granted } => {
                self.u8(1)?;
                self.u8(u8::from(*granted))
            }
            Rpc::Append {
                previous_index,
                previous_term,
                entries,
                leader_commit,
            } => {
                if entries.len() > self.limits.max_entries_per_message {
                    return Err(WireError::TooLarge);
                }
                self.budget.array::<LogEntry>(entries.len())?;
                self.u8(2)?;
                self.u64(*previous_index)?;
                self.u64(*previous_term)?;
                self.u64(*leader_commit)?;
                self.u32(entries.len() as u32)?;
                for entry in entries {
                    self.entry(entry)?;
                }
                Ok(())
            }
            Rpc::Appended {
                success,
                matching_index,
            } => {
                self.u8(3)?;
                self.u8(u8::from(*success))?;
                self.u64(*matching_index)
            }
            Rpc::ReadProbe => self.u8(4),
            Rpc::ReadAck => self.u8(5),
            Rpc::Snapshot { snapshot } | Rpc::LearnerRepairSnapshot { snapshot } => {
                if snapshot.application.len() > self.limits.max_snapshot_bytes {
                    return Err(WireError::TooLarge);
                }
                self.budget.charge(size_of::<Snapshot>())?;
                self.budget.charge(snapshot.application.len())?;
                let meta = &snapshot.metadata;
                if meta.membership.is_some() && self.version < 2 {
                    return Err(WireError::InvalidMessage(
                        "membership requires wire format 2",
                    ));
                }
                let explicit_base =
                    meta.membership.is_some() || meta.bootstrap.configuration != m.configuration;
                self.u8(if matches!(m.rpc, Rpc::LearnerRepairSnapshot { .. }) {
                    16
                } else if explicit_base {
                    9
                } else {
                    6
                })?;
                if explicit_base {
                    self.u64(meta.bootstrap.configuration.get())?;
                }
                self.u64(meta.index)?;
                self.u64(meta.term)?;
                self.u64(meta.application_schema)?;
                self.tree(meta.bootstrap.policy.tree(), 0, &mut 0, &mut 0)?;
                self.budget
                    .charge(meta.bootstrap.policy.voters().len() * 128)?;
                self.budget
                    .charge(meta.bootstrap.voter_stores.len() * 128)?;
                self.u32(meta.bootstrap.voter_stores.len() as u32)?;
                for (node, store) in &meta.bootstrap.voter_stores {
                    self.u64(node.get())?;
                    self.u128(store.id.get())?;
                    self.u64(store.incarnation.get())?;
                }
                if explicit_base {
                    self.u8(u8::from(meta.membership.is_some()))?;
                    if let Some(membership) = &meta.membership {
                        self.membership(membership)?;
                    }
                }
                self.u32(snapshot.application.len() as u32)?;
                self.put(&snapshot.application)
            }
            Rpc::SnapshotAck { index } => {
                self.u8(7)?;
                self.u64(*index)
            }
            Rpc::Compacted { index, term } => {
                self.u8(8)?;
                self.u64(*index)?;
                self.u64(*term)
            }
            Rpc::AuthorityRequest {
                candidate,
                configuration,
            } => {
                self.u8(10)?;
                self.peer(*candidate)?;
                self.u64(configuration.get())
            }
            Rpc::AuthorityReply {
                candidate,
                configuration,
                committed_index,
                committed_term,
                granted,
            } => {
                self.u8(11)?;
                self.peer(*candidate)?;
                self.u64(configuration.get())?;
                self.u64(*committed_index)?;
                self.u64(*committed_term)?;
                self.u8(u8::from(*granted))
            }
        }
    }
    fn batch(&mut self, scope: WireScope, messages: &[Message]) -> Result<(), WireError> {
        if messages.is_empty() || messages.len() > self.limits.max_messages {
            return Err(WireError::TooLarge);
        }
        self.budget.array::<Message>(messages.len())?;
        self.put(&[0; HEADER])?;
        for message in messages {
            if !scope.matches(message) || scope.from == scope.to {
                return Err(WireError::WrongPeer);
            }
            let offset = self.len;
            self.u32(0)?;
            self.message(message)?;
            let len = self.len - offset - 4;
            if let Some(data) = &mut self.data {
                data[offset..offset + 4].copy_from_slice(&(len as u32).to_le_bytes());
            }
        }
        self.u32(0)
    }
}
struct Decoder<'a> {
    bytes: &'a [u8],
    offset: usize,
}
impl<'a> Decoder<'a> {
    fn new(bytes: &'a [u8]) -> Self {
        Self { bytes, offset: 0 }
    }
    fn remaining(&self) -> usize {
        self.bytes.len() - self.offset
    }
    fn take(&mut self, len: usize) -> Result<&'a [u8], WireError> {
        if len > self.remaining() {
            return Err(WireError::Truncated);
        }
        let result = &self.bytes[self.offset..self.offset + len];
        self.offset += len;
        Ok(result)
    }
    fn u8(&mut self) -> Result<u8, WireError> {
        Ok(self.take(1)?[0])
    }
    fn u32(&mut self) -> Result<u32, WireError> {
        Ok(u32::from_le_bytes(self.take(4)?.try_into().unwrap()))
    }
    fn u64(&mut self) -> Result<u64, WireError> {
        Ok(u64::from_le_bytes(self.take(8)?.try_into().unwrap()))
    }
    fn u128(&mut self) -> Result<u128, WireError> {
        Ok(u128::from_le_bytes(self.take(16)?.try_into().unwrap()))
    }
    fn binding(&mut self) -> Result<StoreBinding, WireError> {
        Ok(StoreBinding {
            identity: StoreIdentity {
                id: StoreId::new(self.u128()?).ok_or(WireError::InvalidMessage("zero store"))?,
                incarnation: StoreIncarnation::new(self.u64()?)
                    .ok_or(WireError::InvalidMessage("zero store incarnation"))?,
            },
            session: StoreSession::new(self.u64()?)
                .ok_or(WireError::InvalidMessage("zero store session"))?,
        })
    }
    fn peer(&mut self) -> Result<crate::secure::PeerIdentity, WireError> {
        Ok(crate::secure::PeerIdentity {
            node: NodeId::new(self.u64()?).ok_or(WireError::InvalidMessage("zero candidate"))?,
            store: StoreIdentity {
                id: StoreId::new(self.u128()?).ok_or(WireError::InvalidMessage("zero store"))?,
                incarnation: StoreIncarnation::new(self.u64()?)
                    .ok_or(WireError::InvalidMessage("zero incarnation"))?,
            },
        })
    }
    fn tree(
        &mut self,
        limits: WireLimits,
        budget: &mut Budget,
        depth: usize,
        nodes: &mut usize,
        voters: &mut usize,
    ) -> Result<Tree, WireError> {
        *nodes += 1;
        if depth > limits.policy.max_depth || *nodes > limits.policy.max_tree_nodes {
            return Err(WireError::TooLarge);
        }
        match self.u8()? {
            0 => {
                *voters += 1;
                if *voters > limits.policy.max_voters {
                    return Err(WireError::TooLarge);
                }
                Ok(Tree::Voter(
                    NodeId::new(self.u64()?).ok_or(WireError::InvalidMessage("zero voter"))?,
                ))
            }
            kind @ (1 | 2) => {
                let count = self.u32()? as usize;
                if count == 0 {
                    return Err(WireError::InvalidMessage("empty policy branch"));
                }
                if count > limits.policy.max_tree_nodes - *nodes
                    || count > self.remaining() / if kind == 1 { 9 } else { 17 }
                {
                    return Err(WireError::TooLarge);
                }
                if kind == 1 {
                    budget.array::<Tree>(count)?;
                    let mut children = Vec::with_capacity(count);
                    for _ in 0..count {
                        children.push(self.tree(limits, budget, depth + 1, nodes, voters)?);
                    }
                    Ok(Tree::Majority(children))
                } else {
                    budget.array::<WeightedChild>(count)?;
                    let mut children = Vec::with_capacity(count);
                    for _ in 0..count {
                        let weight = self.u64()?;
                        children.push(WeightedChild {
                            weight,
                            node: self.tree(limits, budget, depth + 1, nodes, voters)?,
                        });
                    }
                    Ok(Tree::Weighted(children))
                }
            }
            _ => Err(WireError::InvalidMessage("policy node kind")),
        }
    }
    fn configuration_id(&mut self) -> Result<ConfigurationId, WireError> {
        ConfigurationId::new(self.u64()?).ok_or(WireError::InvalidMessage("zero configuration"))
    }
    fn operation(&mut self) -> Result<OperationId, WireError> {
        OperationId::new(self.u128()?).ok_or(WireError::InvalidMessage("zero operation"))
    }
    fn stores(
        &mut self,
        limits: WireLimits,
        budget: &mut Budget,
    ) -> Result<BTreeMap<NodeId, StoreIdentity>, WireError> {
        let count = self.u32()? as usize;
        if count > limits.policy.max_voters {
            return Err(WireError::TooLarge);
        }
        if count > self.remaining() / 32 {
            return Err(WireError::Truncated);
        }
        budget.charge(count * 192)?;
        let mut stores = BTreeMap::new();
        for _ in 0..count {
            let node = NodeId::new(self.u64()?).ok_or(WireError::InvalidMessage("zero voter"))?;
            let id =
                StoreId::new(self.u128()?).ok_or(WireError::InvalidMessage("zero voter store"))?;
            let incarnation = StoreIncarnation::new(self.u64()?)
                .ok_or(WireError::InvalidMessage("zero voter store incarnation"))?;
            if stores
                .insert(node, StoreIdentity { id, incarnation })
                .is_some()
            {
                return Err(WireError::InvalidMessage("duplicate voter store"));
            }
        }
        Ok(stores)
    }
    fn configuration(
        &mut self,
        limits: WireLimits,
        budget: &mut Budget,
    ) -> Result<Configuration, WireError> {
        budget.charge(size_of::<Configuration>())?;
        let id = self.configuration_id()?;
        let mut voters = 0;
        let tree = self.tree(limits, budget, 0, &mut 0, &mut voters)?;
        budget.charge(voters * 128)?;
        let policy = Policy::new(tree, limits.policy)
            .map_err(|_| WireError::InvalidMessage("invalid quorum policy"))?;
        let voter_stores = self.stores(limits, budget)?;
        let learners = self.stores(limits, budget)?;
        if voter_stores.len().saturating_add(learners.len()) > limits.policy.max_voters {
            return Err(WireError::TooLarge);
        }
        Configuration::new(id, policy, voter_stores, learners)
            .map_err(|_| WireError::InvalidMessage("invalid configuration"))
    }
    fn record(
        &mut self,
        limits: WireLimits,
        budget: &mut Budget,
    ) -> Result<ConfigurationRecord, WireError> {
        budget.charge(size_of::<ConfigurationRecord>())?;
        if self.u8()? != 1 {
            return Err(WireError::InvalidMessage("configuration subformat"));
        }
        let operation = self.operation()?;
        let expected = self.configuration_id()?;
        let change = match self.u8()? {
            0 => ConfigurationChange::Learners(self.configuration(limits, budget)?),
            1 => ConfigurationChange::Joint {
                id: self.configuration_id()?,
                next: self.configuration(limits, budget)?,
            },
            2 => ConfigurationChange::Final {
                id: self.configuration_id()?,
            },
            _ => return Err(WireError::InvalidMessage("configuration change kind")),
        };
        let record = ConfigurationRecord {
            operation,
            expected,
            change,
        };
        if record.retained_bytes() > limits.max_command_bytes {
            return Err(WireError::TooLarge);
        }
        Ok(record)
    }
    fn membership(
        &mut self,
        boundary: u64,
        limits: WireLimits,
        budget: &mut Budget,
    ) -> Result<Membership, WireError> {
        budget.charge(size_of::<Membership>())?;
        if self.u8()? != 1 {
            return Err(WireError::InvalidMessage("membership subformat"));
        }
        let stable = self.configuration(limits, budget)?;
        let last_index = self.u64()?;
        let joint = match self.u8()? {
            0 => None,
            1 => Some(JointConfiguration {
                operation: self.operation()?,
                id: self.configuration_id()?,
                index: self.u64()?,
                next: self.configuration(limits, budget)?,
            }),
            _ => return Err(WireError::InvalidMessage("membership joint flag")),
        };
        let count = self.u32()? as usize;
        if count > MAX_CONFIGURATION_OPERATIONS {
            return Err(WireError::TooLarge);
        }
        if count > self.remaining() / 16 {
            return Err(WireError::Truncated);
        }
        budget.charge(count * 192)?;
        let mut operations = BTreeSet::new();
        for _ in 0..count {
            let operation = self.operation()?;
            if !operations.insert(operation) {
                return Err(WireError::InvalidMessage(
                    "duplicate configuration operation",
                ));
            }
        }
        Membership::from_checkpoint(stable, joint, last_index, operations, boundary)
            .map_err(|_| WireError::InvalidMessage("invalid membership checkpoint"))
    }
    fn entry(
        &mut self,
        limits: WireLimits,
        budget: &mut Budget,
        version: u16,
    ) -> Result<LogEntry, WireError> {
        let index = self.u64()?;
        let term = self.u64()?;
        let payload = match self.u8()? {
            0 => EntryPayload::Noop,
            1 => {
                let operation = OperationId::new(self.u128()?)
                    .ok_or(WireError::InvalidMessage("zero operation"))?;
                let len = self.u32()? as usize;
                if len > limits.max_command_bytes {
                    return Err(WireError::TooLarge);
                }
                let bytes = self.take(len)?;
                budget.charge(len)?;
                EntryPayload::Command {
                    operation,
                    bytes: bytes.to_vec(),
                }
            }
            2 if version >= 2 => {
                EntryPayload::Configuration(Box::new(self.record(limits, budget)?))
            }
            _ => return Err(WireError::InvalidMessage("entry kind")),
        };
        Ok(LogEntry {
            index,
            term,
            payload,
        })
    }
    fn message(
        &mut self,
        scope: WireScope,
        limits: WireLimits,
        budget: &mut Budget,
        version: u16,
    ) -> Result<Message, WireError> {
        let group = GroupIdentity {
            id: GroupId::new(self.u128()?).ok_or(WireError::InvalidMessage("zero group"))?,
            incarnation: GroupIncarnation::new(self.u64()?)
                .ok_or(WireError::InvalidMessage("zero group incarnation"))?,
        };
        let configuration = ConfigurationId::new(self.u64()?)
            .ok_or(WireError::InvalidMessage("zero configuration"))?;
        let from = NodeId::new(self.u64()?).ok_or(WireError::InvalidMessage("zero sender"))?;
        let sender = self.binding()?;
        let to = NodeId::new(self.u64()?).ok_or(WireError::InvalidMessage("zero recipient"))?;
        if scope != (WireScope { from, sender, to }) || from == to {
            return Err(WireError::WrongPeer);
        }
        let term = self.u64()?;
        let context = RequestContext {
            origin: self.binding()?,
            sequence: self.u64()?,
        };
        let rpc = match self.u8()? {
            14 if version >= 5 => {
                budget.charge(size_of::<LogEntry>())?;
                let joint = Box::new(self.entry(limits, budget, version)?);
                let previous_index = self.u64()?;
                let previous_term = self.u64()?;
                let count = self.u32()? as usize;
                if count > limits.max_entries_per_message || count > self.remaining() / 17 {
                    return Err(WireError::TooLarge);
                }
                budget.array::<LogEntry>(count)?;
                let mut entries = Vec::with_capacity(count);
                for _ in 0..count {
                    entries.push(self.entry(limits, budget, version)?);
                }
                Rpc::LearnerRepair {
                    joint,
                    previous_index,
                    previous_term,
                    entries,
                }
            }
            15 if version >= 5 => Rpc::LearnerRepaired {
                success: bool_value(self.u8()?)?,
                matching_index: self.u64()?,
                matching_term: self.u64()?,
            },
            0 => Rpc::Vote {
                last_index: self.u64()?,
                last_term: self.u64()?,
            },
            1 => Rpc::Voted {
                granted: bool_value(self.u8()?)?,
            },
            2 => {
                let previous_index = self.u64()?;
                let previous_term = self.u64()?;
                let leader_commit = self.u64()?;
                let count = self.u32()? as usize;
                if count > limits.max_entries_per_message || count > self.remaining() / 17 {
                    return Err(WireError::TooLarge);
                }
                budget.array::<LogEntry>(count)?;
                let mut entries = Vec::with_capacity(count);
                for _ in 0..count {
                    entries.push(self.entry(limits, budget, version)?);
                }
                Rpc::Append {
                    previous_index,
                    previous_term,
                    entries,
                    leader_commit,
                }
            }
            3 => Rpc::Appended {
                success: bool_value(self.u8()?)?,
                matching_index: self.u64()?,
            },
            4 => Rpc::ReadProbe,
            5 => Rpc::ReadAck,
            kind @ (6 | 9 | 16) => {
                if (kind == 9 && version < 2) || (kind == 16 && version < 6) {
                    return Err(WireError::InvalidMessage("RPC kind"));
                }
                let bootstrap_configuration = if kind != 6 {
                    self.configuration_id()?
                } else {
                    configuration
                };
                let index = self.u64()?;
                let term = self.u64()?;
                let application_schema = self.u64()?;
                budget.charge(size_of::<Snapshot>())?;
                let mut voters = 0;
                let tree = self.tree(limits, budget, 0, &mut 0, &mut voters)?;
                budget.charge(voters * 128)?;
                let policy = Policy::new(tree, limits.policy)
                    .map_err(|_| WireError::InvalidMessage("invalid quorum policy"))?;
                let count = self.u32()? as usize;
                if count != policy.voters().len() {
                    return Err(WireError::InvalidMessage("voter store count"));
                }
                if count > self.remaining() / 32 {
                    return Err(WireError::Truncated);
                }
                budget.charge(count * 128)?;
                let mut voter_stores = BTreeMap::new();
                for _ in 0..count {
                    let node =
                        NodeId::new(self.u64()?).ok_or(WireError::InvalidMessage("zero voter"))?;
                    let id = StoreId::new(self.u128()?)
                        .ok_or(WireError::InvalidMessage("zero voter store"))?;
                    let incarnation = StoreIncarnation::new(self.u64()?)
                        .ok_or(WireError::InvalidMessage("zero voter store incarnation"))?;
                    if voter_stores
                        .insert(node, StoreIdentity { id, incarnation })
                        .is_some()
                    {
                        return Err(WireError::InvalidMessage("duplicate voter store"));
                    }
                }
                let membership = if kind != 6 && bool_value(self.u8()?)? {
                    Some(Box::new(self.membership(index, limits, budget)?))
                } else {
                    None
                };
                let len = self.u32()? as usize;
                if len > limits.max_snapshot_bytes {
                    return Err(WireError::TooLarge);
                }
                let bytes = self.take(len)?;
                budget.charge(len)?;
                let snapshot = Box::new(Snapshot {
                    metadata: SnapshotMetadata {
                        membership,
                        bootstrap: Bootstrap {
                            group,
                            configuration: bootstrap_configuration,
                            policy,
                            voter_stores,
                        },
                        index,
                        term,
                        application_schema,
                    },
                    application: bytes.to_vec(),
                });
                if kind == 16 {
                    Rpc::LearnerRepairSnapshot { snapshot }
                } else {
                    Rpc::Snapshot { snapshot }
                }
            }
            7 => Rpc::SnapshotAck { index: self.u64()? },
            8 => Rpc::Compacted {
                index: self.u64()?,
                term: self.u64()?,
            },
            kind @ (12 | 13) if version >= 4 => {
                budget.charge(size_of::<LearnerReadinessRequest>())?;
                let request = LearnerReadinessRequest {
                    group,
                    configuration,
                    leader: if kind == 12 { from } else { to },
                    context,
                    term,
                    learner: self.peer()?,
                    session: StoreSession::new(self.u64()?)
                        .ok_or(WireError::InvalidMessage("zero readiness session"))?,
                    index: self.u64()?,
                    index_term: self.u64()?,
                    requirements: ReadinessRequirements {
                        application_schema: self.u64()?,
                        command_bytes: usize::try_from(self.u64()?)
                            .map_err(|_| WireError::TooLarge)?,
                        snapshot_bytes: usize::try_from(self.u64()?)
                            .map_err(|_| WireError::TooLarge)?,
                    },
                };
                if kind == 12 {
                    Rpc::LearnerReadinessRequest(Box::new(request))
                } else {
                    Rpc::LearnerReadinessReply {
                        request: Box::new(request),
                        ready: bool_value(self.u8()?)?,
                    }
                }
            }
            10 if version >= 3 => Rpc::AuthorityRequest {
                candidate: self.peer()?,
                configuration: self.configuration_id()?,
            },
            11 if version >= 3 => Rpc::AuthorityReply {
                candidate: self.peer()?,
                configuration: self.configuration_id()?,
                committed_index: self.u64()?,
                committed_term: self.u64()?,
                granted: bool_value(self.u8()?)?,
            },
            _ => return Err(WireError::InvalidMessage("RPC kind")),
        };
        let message = Message {
            group,
            configuration,
            from,
            sender,
            to,
            term,
            context,
            rpc,
        };
        validate_message(&message, version)?;
        if self.remaining() != 0 {
            return Err(WireError::Corrupt("trailing message bytes"));
        }
        Ok(message)
    }
}
impl WireCodec for NativeWireCodec {
    fn configuration_capacity(
        &self,
        required: &ConfigurationWireRequirements<'_>,
    ) -> Result<ConfigurationWireCapacity, WireError> {
        capacity::check(self, required)
    }
    fn format_version(&self) -> u16 {
        self.version
    }
    fn header_bytes(&self) -> usize {
        HEADER
    }
    fn limits(&self) -> WireLimits {
        self.limits
    }
    fn frame_length(&self, header: &[u8]) -> Result<usize, WireError> {
        if header.len() != HEADER {
            return Err(WireError::Truncated);
        }
        if &header[..8] != MAGIC {
            return Err(WireError::Corrupt("frame magic"));
        }
        if crc32c(&header[..20]) != u32::from_le_bytes(header[20..24].try_into().unwrap()) {
            return Err(WireError::Corrupt("header checksum"));
        }
        let version = u16::from_le_bytes(header[8..10].try_into().unwrap());
        if version != self.version {
            return Err(WireError::UnsupportedVersion(version));
        }
        if header[10..12] != [0, 0] {
            return Err(WireError::UnsupportedFlags);
        }
        let length = u32::from_le_bytes(header[12..16].try_into().unwrap()) as usize;
        let count = u32::from_le_bytes(header[16..20].try_into().unwrap()) as usize;
        if length < OVERHEAD
            || length > self.limits.max_frame_bytes
            || count == 0
            || count > self.limits.max_messages
            || count > length.saturating_sub(OVERHEAD) / (MIN_MESSAGE + 4)
        {
            return Err(WireError::TooLarge);
        }
        Ok(length)
    }
    fn encoded_length(&self, scope: WireScope, messages: &[Message]) -> Result<usize, WireError> {
        let mut count = Encoder::new(self.limits, None, self.version);
        count.batch(scope, messages)?;
        Ok(count.len)
    }
    fn encode_batch(&self, scope: WireScope, messages: &[Message]) -> Result<Vec<u8>, WireError> {
        let len = self.encoded_length(scope, messages)?;
        let mut bytes = vec![0; len];
        self.encode_into(scope, messages, &mut bytes)?;
        Ok(bytes)
    }
    fn encode_into(
        &self,
        scope: WireScope,
        messages: &[Message],
        bytes: &mut [u8],
    ) -> Result<(), WireError> {
        let expected = bytes.len();
        let mut encoder = Encoder::new(self.limits, Some(bytes), self.version);
        encoder.batch(scope, messages)?;
        if encoder.len != expected {
            return Err(WireError::TooLarge);
        }
        let len = bytes.len();
        bytes[..8].copy_from_slice(MAGIC);
        bytes[8..10].copy_from_slice(&self.version.to_le_bytes());
        bytes[12..16].copy_from_slice(&(len as u32).to_le_bytes());
        bytes[16..20].copy_from_slice(&(messages.len() as u32).to_le_bytes());
        let crc = crc32c(&bytes[..20]);
        bytes[20..24].copy_from_slice(&crc.to_le_bytes());
        let crc = crc32c(&bytes[..len - 4]);
        bytes[len - 4..].copy_from_slice(&crc.to_le_bytes());
        Ok(())
    }
    fn decode_batch(&self, scope: WireScope, frame: &[u8]) -> Result<Vec<Message>, WireError> {
        if frame.len() < HEADER {
            return Err(WireError::Truncated);
        }
        let length = self.frame_length(&frame[..HEADER])?;
        if frame.len() < length {
            return Err(WireError::Truncated);
        }
        if frame.len() != length {
            return Err(WireError::Corrupt("trailing frame bytes"));
        }
        if crc32c(&frame[..length - 4])
            != u32::from_le_bytes(frame[length - 4..].try_into().unwrap())
        {
            return Err(WireError::Corrupt("frame checksum"));
        }
        let count = u32::from_le_bytes(frame[16..20].try_into().unwrap()) as usize;
        let mut budget = Budget {
            used: 0,
            max: self.limits.max_decoded_bytes,
        };
        budget.array::<Message>(count)?;
        let mut result = Vec::with_capacity(count);
        let mut decoder = Decoder::new(&frame[HEADER..length - 4]);
        for _ in 0..count {
            let len = decoder.u32()? as usize;
            if len < MIN_MESSAGE {
                return Err(WireError::InvalidMessage("message length"));
            }
            let bytes = decoder.take(len)?;
            result.push(Decoder::new(bytes).message(
                scope,
                self.limits,
                &mut budget,
                self.version,
            )?);
        }
        if decoder.remaining() != 0 {
            return Err(WireError::Corrupt("trailing batch bytes"));
        }
        Ok(result)
    }
}
