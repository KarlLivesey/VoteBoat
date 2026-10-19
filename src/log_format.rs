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
//! Pure VBLOG002 batches and compatible live-state checkpoint formats.
use crate::{
    contracts::StorageError,
    format_crc::crc32c,
    identity::*,
    log::*,
    quorum::{Limits as PolicyLimits, Policy, Tree, WeightedChild},
};
use std::collections::BTreeMap;
pub(crate) mod checkpoint;
pub(crate) const HEADER: usize = 32;
pub(crate) const TRAILER: usize = 16;
const MAGIC: &[u8; 8] = b"VBLOG002";
const END: &[u8; 8] = b"VBLEND02";

pub trait LogCodec {
    fn format_version(&self) -> u32;
    fn encode_batch(
        &self,
        sequence: u64,
        mutations: &[LogMutation],
        limits: LogLimits,
    ) -> Result<Vec<u8>, StorageError>;
    fn frame_length(&self, header: &[u8], limits: LogLimits) -> Result<usize, StorageError>;
    fn decode_batch(
        &self,
        bytes: &[u8],
        limits: LogLimits,
    ) -> Result<(u64, Vec<LogMutation>), StorageError>;
    fn supports_checkpoint(&self) -> bool {
        false
    }
    /// Recognize a checkpoint from a prefix of at most 32 bytes.
    ///
    /// Recognition selects `decode_checkpoint`; it does not validate the image.
    /// Override this when supplying a different checkpoint marker. The default
    /// recognizes both native checkpoint formats for existing implementations.
    fn is_checkpoint(&self, header: &[u8]) -> bool {
        checkpoint::is_checkpoint(header)
    }
    fn encode_checkpoint(
        &self,
        _sequence: u64,
        _state: &BTreeMap<GroupIdentity, GroupLog>,
        _limits: LogLimits,
        _max_bytes: usize,
    ) -> Result<Vec<u8>, StorageError> {
        Err(StorageError::Rejected("log checkpoint unsupported"))
    }
    fn decode_checkpoint(
        &self,
        _bytes: &[u8],
        _limits: LogLimits,
    ) -> Result<(u64, BTreeMap<GroupIdentity, GroupLog>, usize), StorageError> {
        Err(StorageError::Corrupt("log checkpoint unsupported"))
    }
}
#[derive(Clone, Copy, Debug, Default)]
pub struct NativeLogCodec;

struct Encoder {
    b: Vec<u8>,
    limit: usize,
}
impl Encoder {
    fn put(&mut self, b: &[u8]) -> Result<(), StorageError> {
        if b.len() > self.limit.saturating_sub(self.b.len()) {
            return Err(StorageError::Rejected("encoded batch budget"));
        }
        self.b.extend(b);
        Ok(())
    }
    fn u8(&mut self, v: u8) -> Result<(), StorageError> {
        self.put(&[v])
    }
    fn u32(&mut self, v: u32) -> Result<(), StorageError> {
        self.put(&v.to_le_bytes())
    }
    fn u64(&mut self, v: u64) -> Result<(), StorageError> {
        self.put(&v.to_le_bytes())
    }
    fn u128(&mut self, v: u128) -> Result<(), StorageError> {
        self.put(&v.to_le_bytes())
    }
    fn group(&mut self, g: GroupIdentity) -> Result<(), StorageError> {
        self.u128(g.id.get())?;
        self.u64(g.incarnation.get())
    }
    fn tree(&mut self, t: &Tree, depth: usize) -> Result<(), StorageError> {
        if depth > 32 {
            return Err(StorageError::Rejected("policy depth"));
        }
        match t {
            Tree::Voter(n) => {
                self.u8(0)?;
                self.u64(n.get())
            }
            Tree::Majority(children) => {
                self.u8(1)?;
                self.u32(children.len() as u32)?;
                for child in children {
                    self.tree(child, depth + 1)?;
                }
                Ok(())
            }
            Tree::Weighted(children) => {
                self.u8(2)?;
                self.u32(children.len() as u32)?;
                for c in children {
                    self.u64(c.weight)?;
                    self.tree(&c.node, depth + 1)?;
                }
                Ok(())
            }
        }
    }
    fn mutation(&mut self, m: &LogMutation) -> Result<(), StorageError> {
        match m {
            LogMutation::Create(b) => {
                self.u8(0)?;
                self.group(b.group)?;
                self.u64(b.configuration.get())?;
                self.tree(b.policy.tree(), 0)?;
                self.u32(b.voter_stores.len() as u32)?;
                for (node, store) in &b.voter_stores {
                    self.u64(node.get())?;
                    self.u128(store.id.get())?;
                    self.u64(store.incarnation.get())?;
                }
                Ok(())
            }
            LogMutation::Update(u) => {
                if u.snapshot.is_none() && u.snapshot_membership.is_some() {
                    return Err(StorageError::Rejected("membership base without snapshot"));
                }
                self.u8(if u.snapshot_membership.is_some() {
                    3
                } else if u.snapshot.is_some() {
                    2
                } else {
                    1
                })?;
                self.group(u.group)?;
                self.u64(u.expected_revision.get())?;
                self.u64(u.hard_state.term)?;
                self.u64(u.hard_state.voted_for.map_or(0, NodeId::get))?;
                self.u64(u.commit_index)?;
                if let Some(r) = u.snapshot {
                    if u.suffix.is_some() {
                        return Err(StorageError::Rejected("snapshot/suffix conflict"));
                    }
                    self.u128(r.store.id.get())?;
                    self.u64(r.store.incarnation.get())?;
                    self.group(r.group)?;
                    self.u64(r.generation.get())?;
                    self.u64(r.configuration.get())?;
                    self.u64(r.index)?;
                    self.u64(r.term)?;
                    self.u64(r.application_schema)?;
                    self.u64(r.file_bytes)?;
                    self.u32(r.checksum)?;
                    if let Some(membership) = &u.snapshot_membership {
                        self.membership(membership)?;
                    }
                    return Ok(());
                }
                match &u.suffix {
                    None => self.u8(0),
                    Some(s) => {
                        self.u8(1)?;
                        self.u64(s.from)?;
                        self.u32(s.entries.len() as u32)?;
                        for e in &s.entries {
                            self.u64(e.index)?;
                            self.u64(e.term)?;
                            match &e.payload {
                                EntryPayload::Noop => self.u8(0)?,
                                EntryPayload::Configuration(record) => {
                                    self.u8(2)?;
                                    self.configuration_record(record)?;
                                }
                                EntryPayload::Command { operation, bytes } => {
                                    self.u8(1)?;
                                    self.u128(operation.get())?;
                                    self.u32(bytes.len() as u32)?;
                                    self.put(bytes)?;
                                }
                            }
                        }
                        Ok(())
                    }
                }
            }
        }
    }
    fn configuration(&mut self, c: &crate::membership::Configuration) -> Result<(), StorageError> {
        self.u64(c.id().get())?;
        self.tree(c.policy().tree(), 0)?;
        for stores in [c.voter_stores(), c.learners()] {
            self.u32(stores.len() as u32)?;
            for (node, store) in stores {
                self.u64(node.get())?;
                self.u128(store.id.get())?;
                self.u64(store.incarnation.get())?;
            }
        }
        Ok(())
    }
    fn configuration_record(
        &mut self,
        record: &crate::membership::ConfigurationRecord,
    ) -> Result<(), StorageError> {
        use crate::membership::ConfigurationChange;
        self.u8(1)?; // configuration-journal subformat version
        self.u128(record.operation.get())?;
        self.u64(record.expected.get())?;
        match &record.change {
            ConfigurationChange::Learners(c) => {
                self.u8(0)?;
                self.configuration(c)
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
    fn membership(&mut self, m: &crate::membership::Membership) -> Result<(), StorageError> {
        self.u8(1)?;
        self.configuration(m.stable())?;
        self.u64(m.last_configuration_index())?;
        self.u8(u8::from(m.joint().is_some()))?;
        if let Some(j) = m.joint() {
            self.u128(j.operation.get())?;
            self.u64(j.id.get())?;
            self.u64(j.index)?;
            self.configuration(&j.next)?;
        }
        self.u32(m.operations().len() as u32)?;
        for op in m.operations() {
            self.u128(op.get())?;
        }
        Ok(())
    }
}

pub(crate) struct Decoder<'a> {
    b: &'a [u8],
    offset: usize,
}
impl<'a> Decoder<'a> {
    pub(crate) fn new(b: &'a [u8]) -> Self {
        Self { b, offset: 0 }
    }
    fn take(&mut self, len: usize) -> Result<&'a [u8], StorageError> {
        if len > self.b.len().saturating_sub(self.offset) {
            return Err(StorageError::Corrupt("truncated payload"));
        }
        let result = &self.b[self.offset..self.offset + len];
        self.offset += len;
        Ok(result)
    }
    fn u8(&mut self) -> Result<u8, StorageError> {
        Ok(self.take(1)?[0])
    }
    fn u32(&mut self) -> Result<u32, StorageError> {
        Ok(u32::from_le_bytes(self.take(4)?.try_into().unwrap()))
    }
    pub(crate) fn u64(&mut self) -> Result<u64, StorageError> {
        Ok(u64::from_le_bytes(self.take(8)?.try_into().unwrap()))
    }
    pub(crate) fn u128(&mut self) -> Result<u128, StorageError> {
        Ok(u128::from_le_bytes(self.take(16)?.try_into().unwrap()))
    }
    fn group(&mut self) -> Result<GroupIdentity, StorageError> {
        Ok(GroupIdentity {
            id: GroupId::new(self.u128()?).ok_or(StorageError::Corrupt("zero group"))?,
            incarnation: GroupIncarnation::new(self.u64()?)
                .ok_or(StorageError::Corrupt("zero incarnation"))?,
        })
    }
    fn tree(&mut self, depth: usize, nodes: &mut usize) -> Result<Tree, StorageError> {
        *nodes += 1;
        if depth > 32 || *nodes > 16384 {
            return Err(StorageError::Corrupt("policy resource limit"));
        }
        match self.u8()? {
            0 => Ok(Tree::Voter(
                NodeId::new(self.u64()?).ok_or(StorageError::Corrupt("zero voter"))?,
            )),
            kind @ (1 | 2) => {
                let count = self.u32()? as usize;
                if count == 0
                    || count > 16384 - *nodes
                    || count > self.b.len().saturating_sub(self.offset)
                {
                    return Err(StorageError::Corrupt("policy child limit"));
                }
                if kind == 1 {
                    let mut children = Vec::with_capacity(count);
                    for _ in 0..count {
                        children.push(self.tree(depth + 1, nodes)?);
                    }
                    Ok(Tree::Majority(children))
                } else {
                    let mut children = Vec::with_capacity(count);
                    for _ in 0..count {
                        let weight = self.u64()?;
                        children.push(WeightedChild {
                            weight,
                            node: self.tree(depth + 1, nodes)?,
                        });
                    }
                    Ok(Tree::Weighted(children))
                }
            }
            _ => Err(StorageError::Corrupt("unknown policy node")),
        }
    }
    fn mutation(&mut self, limits: LogLimits) -> Result<LogMutation, StorageError> {
        let kind = self.u8()?;
        let group = self.group()?;
        match kind {
            0 => self.mutation_create(group),
            1..=3 => self.mutation_update(kind, group, limits),
            _ => Err(StorageError::Corrupt("mutation kind")),
        }
    }
    fn mutation_create(&mut self, group: GroupIdentity) -> Result<LogMutation, StorageError> {
        let configuration =
            ConfigurationId::new(self.u64()?).ok_or(StorageError::Corrupt("zero configuration"))?;
        let tree = self.tree(0, &mut 0)?;
        let policy = Policy::new(tree, PolicyLimits::default())
            .map_err(|_| StorageError::Corrupt("invalid policy"))?;
        let count = self.u32()? as usize;
        if count != policy.voters().len() {
            return Err(StorageError::Corrupt("voter identity count"));
        }
        let mut voter_stores = BTreeMap::new();
        for _ in 0..count {
            let node = NodeId::new(self.u64()?).ok_or(StorageError::Corrupt("zero voter"))?;
            let id = StoreId::new(self.u128()?).ok_or(StorageError::Corrupt("zero store"))?;
            let incarnation = StoreIncarnation::new(self.u64()?)
                .ok_or(StorageError::Corrupt("zero store incarnation"))?;
            if voter_stores
                .insert(node, StoreIdentity { id, incarnation })
                .is_some()
            {
                return Err(StorageError::Corrupt("duplicate voter identity"));
            }
        }
        Ok(LogMutation::Create(Bootstrap {
            group,
            configuration,
            policy,
            voter_stores,
        }))
    }
    fn mutation_update(
        &mut self,
        kind: u8,
        group: GroupIdentity,
        limits: LogLimits,
    ) -> Result<LogMutation, StorageError> {
        let expected_revision =
            LogRevision::new(self.u64()?).ok_or(StorageError::Corrupt("zero revision"))?;
        let hard_state = crate::contracts::HardState {
            term: self.u64()?,
            voted_for: NodeId::new(self.u64()?),
        };
        let commit_index = self.u64()?;
        if kind == 2 || kind == 3 {
            let reference = self.snapshot_reference()?;
            return Ok(LogMutation::Update(LogUpdate {
                snapshot_membership: if kind == 3 {
                    Some(Box::new(self.membership(reference.index)?))
                } else {
                    None
                },
                group,
                expected_revision,
                hard_state,
                commit_index,
                suffix: None,
                snapshot: Some(reference),
            }));
        }
        let suffix = self.mutation_suffix(limits)?;
        Ok(LogMutation::Update(LogUpdate {
            snapshot_membership: None,
            group,
            expected_revision,
            hard_state,
            commit_index,
            suffix,
            snapshot: None,
        }))
    }
    fn snapshot_reference(&mut self) -> Result<crate::snapshot::SnapshotRef, StorageError> {
        let store = StoreIdentity {
            id: StoreId::new(self.u128()?).ok_or(StorageError::Corrupt("zero snapshot store"))?,
            incarnation: StoreIncarnation::new(self.u64()?)
                .ok_or(StorageError::Corrupt("zero snapshot store incarnation"))?,
        };
        let reference = crate::snapshot::SnapshotRef {
            store,
            group: self.group()?,
            generation: SnapshotGeneration::new(self.u64()?)
                .ok_or(StorageError::Corrupt("zero snapshot generation"))?,
            configuration: ConfigurationId::new(self.u64()?)
                .ok_or(StorageError::Corrupt("zero snapshot configuration"))?,
            index: self.u64()?,
            term: self.u64()?,
            application_schema: self.u64()?,
            file_bytes: self.u64()?,
            checksum: self.u32()?,
        };
        Ok(reference)
    }
    fn mutation_suffix(&mut self, limits: LogLimits) -> Result<Option<Suffix>, StorageError> {
        let suffix = match self.u8()? {
            0 => None,
            1 => {
                let from = self.u64()?;
                let count = self.u32()? as usize;
                if count > limits.retained_entry_capacity()
                    || count > self.b.len().saturating_sub(self.offset) / 17
                {
                    return Err(StorageError::Corrupt("entry count budget"));
                }
                let mut entries = Vec::new();
                for _ in 0..count {
                    entries.push(self.mutation_entry(limits)?);
                }
                Some(Suffix { from, entries })
            }
            _ => return Err(StorageError::Corrupt("suffix flag")),
        };
        Ok(suffix)
    }
    fn mutation_entry(&mut self, limits: LogLimits) -> Result<LogEntry, StorageError> {
        let index = self.u64()?;
        let term = self.u64()?;
        let payload = match self.u8()? {
            0 => EntryPayload::Noop,
            2 => {
                let record = self.configuration_record()?;
                if record.retained_bytes() > limits.max_command_bytes {
                    return Err(StorageError::Corrupt("configuration payload budget"));
                }
                EntryPayload::Configuration(Box::new(record))
            }
            1 => {
                let operation = OperationId::new(self.u128()?)
                    .ok_or(StorageError::Corrupt("zero operation"))?;
                let len = self.u32()? as usize;
                if len > limits.max_command_bytes {
                    return Err(StorageError::Corrupt("command budget"));
                }
                let bytes = self.take(len)?;
                let charge = CommandBytes::retained_bytes_for_len(len)
                    .ok_or(StorageError::Corrupt("command allocation budget"))?;
                if charge > limits.max_wal_bytes {
                    return Err(StorageError::Corrupt("command allocation budget"));
                }
                EntryPayload::Command {
                    operation,
                    bytes: bytes.into(),
                }
            }
            _ => return Err(StorageError::Corrupt("entry kind")),
        };
        Ok(LogEntry {
            index,
            term,
            payload,
        })
    }
    fn configuration(&mut self) -> Result<crate::membership::Configuration, StorageError> {
        let id =
            ConfigurationId::new(self.u64()?).ok_or(StorageError::Corrupt("zero configuration"))?;
        let policy = Policy::new(self.tree(0, &mut 0)?, PolicyLimits::default())
            .map_err(|_| StorageError::Corrupt("invalid configuration policy"))?;
        let mut maps = Vec::with_capacity(2);
        for _ in 0..2 {
            let count = self.u32()? as usize;
            if count > PolicyLimits::default().max_voters
                || count > self.b.len().saturating_sub(self.offset) / 32
            {
                return Err(StorageError::Corrupt("configuration store budget"));
            }
            let mut stores = BTreeMap::new();
            for _ in 0..count {
                let node = NodeId::new(self.u64()?)
                    .ok_or(StorageError::Corrupt("zero configuration node"))?;
                let store = StoreIdentity {
                    id: StoreId::new(self.u128()?)
                        .ok_or(StorageError::Corrupt("zero configuration store"))?,
                    incarnation: StoreIncarnation::new(self.u64()?).ok_or(
                        StorageError::Corrupt("zero configuration store incarnation"),
                    )?,
                };
                if stores.insert(node, store).is_some() {
                    return Err(StorageError::Corrupt("duplicate configuration node"));
                }
            }
            maps.push(stores);
        }
        let learners = maps.pop().unwrap();
        crate::membership::Configuration::new(id, policy, maps.pop().unwrap(), learners)
            .map_err(|_| StorageError::Corrupt("invalid configuration stores"))
    }
    fn configuration_record(
        &mut self,
    ) -> Result<crate::membership::ConfigurationRecord, StorageError> {
        use crate::membership::{ConfigurationChange, ConfigurationRecord};
        if self.u8()? != 1 {
            return Err(StorageError::Corrupt("configuration journal version"));
        }
        let operation = OperationId::new(self.u128()?)
            .ok_or(StorageError::Corrupt("zero configuration operation"))?;
        let expected = ConfigurationId::new(self.u64()?)
            .ok_or(StorageError::Corrupt("zero expected configuration"))?;
        let change = match self.u8()? {
            0 => ConfigurationChange::Learners(self.configuration()?),
            1 => {
                let id = ConfigurationId::new(self.u64()?)
                    .ok_or(StorageError::Corrupt("zero joint configuration"))?;
                ConfigurationChange::Joint {
                    id,
                    next: self.configuration()?,
                }
            }
            2 => ConfigurationChange::Final {
                id: ConfigurationId::new(self.u64()?)
                    .ok_or(StorageError::Corrupt("zero final configuration"))?,
            },
            _ => return Err(StorageError::Corrupt("configuration change kind")),
        };
        Ok(ConfigurationRecord {
            operation,
            expected,
            change,
        })
    }
    fn membership(&mut self, boundary: u64) -> Result<crate::membership::Membership, StorageError> {
        use crate::membership::{JointConfiguration, Membership, MAX_CONFIGURATION_OPERATIONS};
        if self.u8()? != 1 {
            return Err(StorageError::Corrupt("membership checkpoint version"));
        }
        let stable = self.configuration()?;
        let index = self.u64()?;
        let joint = match self.u8()? {
            0 => None,
            1 => Some(JointConfiguration {
                operation: OperationId::new(self.u128()?)
                    .ok_or(StorageError::Corrupt("zero membership operation"))?,
                id: ConfigurationId::new(self.u64()?)
                    .ok_or(StorageError::Corrupt("zero joint identity"))?,
                index: self.u64()?,
                next: self.configuration()?,
            }),
            _ => return Err(StorageError::Corrupt("membership joint flag")),
        };
        let count = self.u32()? as usize;
        if count > MAX_CONFIGURATION_OPERATIONS
            || count > self.b.len().saturating_sub(self.offset) / 16
        {
            return Err(StorageError::Corrupt("membership operation budget"));
        }
        let mut operations = std::collections::BTreeSet::new();
        for _ in 0..count {
            let op = OperationId::new(self.u128()?)
                .ok_or(StorageError::Corrupt("zero membership operation"))?;
            if !operations.insert(op) {
                return Err(StorageError::Corrupt("duplicate membership operation"));
            }
        }
        Membership::from_checkpoint(stable, joint, index, operations, boundary)
            .map_err(|_| StorageError::Corrupt("invalid membership checkpoint"))
    }
}

impl NativeLogCodec {
    pub(crate) fn encode_membership(
        m: &crate::membership::Membership,
        max_bytes: usize,
    ) -> Result<Vec<u8>, StorageError> {
        let mut e = Encoder {
            b: Vec::new(),
            limit: max_bytes,
        };
        e.membership(m)?;
        Ok(e.b)
    }
    pub(crate) fn decode_membership(
        bytes: &[u8],
        boundary: u64,
    ) -> Result<crate::membership::Membership, StorageError> {
        let mut d = Decoder::new(bytes);
        let membership = d.membership(boundary)?;
        if d.offset != bytes.len() {
            return Err(StorageError::Corrupt("membership trailing bytes"));
        }
        Ok(membership)
    }
}

impl LogCodec for NativeLogCodec {
    fn format_version(&self) -> u32 {
        2
    }
    fn supports_checkpoint(&self) -> bool {
        true
    }
    fn encode_checkpoint(
        &self,
        sequence: u64,
        state: &BTreeMap<GroupIdentity, GroupLog>,
        limits: LogLimits,
        max_bytes: usize,
    ) -> Result<Vec<u8>, StorageError> {
        checkpoint::encode(sequence, state, limits, max_bytes)
    }
    fn decode_checkpoint(
        &self,
        bytes: &[u8],
        limits: LogLimits,
    ) -> Result<(u64, BTreeMap<GroupIdentity, GroupLog>, usize), StorageError> {
        checkpoint::decode(bytes, limits)
    }
    fn encode_batch(
        &self,
        sequence: u64,
        mutations: &[LogMutation],
        limits: LogLimits,
    ) -> Result<Vec<u8>, StorageError> {
        if mutations.is_empty() || mutations.len() > limits.max_batch_units {
            return Err(StorageError::Rejected("codec unit budget"));
        }
        let mut e = Encoder {
            b: Vec::new(),
            limit: limits.max_batch_bytes.saturating_sub(HEADER + TRAILER),
        };
        for m in mutations {
            e.mutation(m)?;
        }
        let mut b = MAGIC.to_vec();
        b.extend(sequence.to_le_bytes());
        b.extend((mutations.len() as u32).to_le_bytes());
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
    fn frame_length(&self, b: &[u8], limits: LogLimits) -> Result<usize, StorageError> {
        if b.len() < HEADER
            || &b[..8] != MAGIC
            || crc32c(&b[..24]) != u32::from_le_bytes(b[24..28].try_into().unwrap())
            || b[28..32] != [0; 4]
        {
            return Err(StorageError::Corrupt("batch header/version/checksum"));
        }
        let count = u32::from_le_bytes(b[16..20].try_into().unwrap()) as usize;
        let payload = u32::from_le_bytes(b[20..24].try_into().unwrap()) as usize;
        if count == 0
            || count > limits.max_batch_units
            || payload > limits.max_batch_bytes.saturating_sub(HEADER + TRAILER)
        {
            return Err(StorageError::Corrupt("batch length/unit budget"));
        }
        Ok(HEADER + payload + TRAILER)
    }
    fn decode_batch(
        &self,
        b: &[u8],
        limits: LogLimits,
    ) -> Result<(u64, Vec<LogMutation>), StorageError> {
        let size = self.frame_length(b, limits)?;
        if size != b.len() {
            return Err(StorageError::Corrupt("incomplete batch"));
        }
        let end = size - TRAILER;
        if &b[end..end + 8] != END
            || b[end + 12..] != [0; 4]
            || crc32c(&b[..end]) != u32::from_le_bytes(b[end + 8..end + 12].try_into().unwrap())
        {
            return Err(StorageError::Corrupt("batch trailer/checksum"));
        }
        let count = u32::from_le_bytes(b[16..20].try_into().unwrap()) as usize;
        let mut d = Decoder::new(&b[HEADER..end]);
        let mut mutations = Vec::new();
        for _ in 0..count {
            mutations.push(d.mutation(limits)?);
        }
        if d.offset != d.b.len() {
            return Err(StorageError::Corrupt("unparsed payload"));
        }
        Ok((u64::from_le_bytes(b[8..16].try_into().unwrap()), mutations))
    }
}

#[cfg(test)]
mod configuration_tests {
    use super::*;
    use crate::membership::*;
    fn fixture() -> ConfigurationRecord {
        let node = NodeId::new(1).unwrap();
        let store = StoreIdentity {
            id: StoreId::new(1).unwrap(),
            incarnation: StoreIncarnation::new(1).unwrap(),
        };
        ConfigurationRecord {
            operation: OperationId::new(9).unwrap(),
            expected: ConfigurationId::new(1).unwrap(),
            change: ConfigurationChange::Joint {
                id: ConfigurationId::new(2).unwrap(),
                next: Configuration::new(
                    ConfigurationId::new(3).unwrap(),
                    Policy::new(Tree::Voter(node), PolicyLimits::default()).unwrap(),
                    [(node, store)].into(),
                    BTreeMap::new(),
                )
                .unwrap(),
            },
        }
    }
    fn encoded() -> Vec<u8> {
        let mut encoder = Encoder {
            b: Vec::new(),
            limit: 4096,
        };
        encoder.configuration_record(&fixture()).unwrap();
        encoder.b
    }
    #[test]
    fn configuration_codec_roundtrip_rejects_every_truncation_and_unknown_tag() {
        let bytes = encoded();
        assert_eq!(
            Decoder::new(&bytes).configuration_record().unwrap(),
            fixture()
        );
        for cut in 0..bytes.len() {
            assert!(
                Decoder::new(&bytes[..cut]).configuration_record().is_err(),
                "cut {cut}"
            );
        }
        for (offset, value) in [(0, 2), (25, 3), (42, 3)] {
            let mut bad = bytes.clone();
            bad[offset] = value;
            assert!(
                Decoder::new(&bad).configuration_record().is_err(),
                "offset {offset}"
            );
        }
        for range in [
            1..17,
            17..25,
            26..34,
            34..42,
            43..51,
            55..63,
            63..79,
            79..87,
        ] {
            let mut bad = bytes.clone();
            bad[range].fill(0);
            assert!(Decoder::new(&bad).configuration_record().is_err());
        }
    }
    #[test]
    fn configuration_codec_bounds_store_counts_and_rejects_duplicate_or_overlapping_members() {
        let bytes = encoded();
        for offset in [51, 87] {
            let mut bad = bytes.clone();
            bad[offset..offset + 4].copy_from_slice(&u32::MAX.to_le_bytes());
            assert!(Decoder::new(&bad).configuration_record().is_err());
        }
        // One extra duplicated voter, with a truthful count and sufficient bytes.
        let mut duplicate = bytes[..87].to_vec();
        duplicate[51..55].copy_from_slice(&2u32.to_le_bytes());
        duplicate.extend_from_slice(&bytes[55..87]);
        duplicate.extend_from_slice(&bytes[87..]);
        assert!(Decoder::new(&duplicate).configuration_record().is_err());
        // The same node in the learner map cannot be treated as another voter.
        let mut overlap = bytes.clone();
        overlap[87..91].copy_from_slice(&1u32.to_le_bytes());
        overlap.extend_from_slice(&bytes[55..87]);
        assert!(Decoder::new(&overlap).configuration_record().is_err());
        let mut tiny = Encoder {
            b: Vec::new(),
            limit: bytes.len() - 1,
        };
        assert!(tiny.configuration_record(&fixture()).is_err());
    }
    #[test]
    fn membership_checkpoint_codec_rejects_truncation_versions_counts_and_changed_operation() {
        let record = fixture();
        let node = NodeId::new(1).unwrap();
        let bootstrap = Bootstrap {
            group: GroupIdentity {
                id: GroupId::new(1).unwrap(),
                incarnation: GroupIncarnation::new(1).unwrap(),
            },
            configuration: record.expected,
            policy: Policy::new(Tree::Voter(node), PolicyLimits::default()).unwrap(),
            voter_stores: [(
                node,
                StoreIdentity {
                    id: StoreId::new(1).unwrap(),
                    incarnation: StoreIncarnation::new(1).unwrap(),
                },
            )]
            .into(),
        };
        let state = Membership::replay(
            &bootstrap,
            &[LogEntry {
                index: 1,
                term: 1,
                payload: EntryPayload::Configuration(Box::new(record)),
            }],
            0,
        )
        .unwrap();
        let bytes = NativeLogCodec::encode_membership(&state, 4096).unwrap();
        assert_eq!(NativeLogCodec::decode_membership(&bytes, 1).unwrap(), state);
        for cut in 0..bytes.len() {
            assert!(
                NativeLogCodec::decode_membership(&bytes[..cut], 1).is_err(),
                "cut {cut}"
            );
        }
        for (offset, value) in [(0, 2), (66, 2)] {
            let mut bad = bytes.clone();
            bad[offset] = value;
            assert!(NativeLogCodec::decode_membership(&bad, 1).is_err());
        }
        let mut bad = bytes.clone();
        bad[156..160].copy_from_slice(&u32::MAX.to_le_bytes());
        assert!(NativeLogCodec::decode_membership(&bad, 1).is_err());
        let mut bad = bytes.clone();
        bad[160..176].copy_from_slice(&999u128.to_le_bytes());
        assert!(NativeLogCodec::decode_membership(&bad, 1).is_err());
        let mut bad = bytes.clone();
        bad.push(0);
        assert!(NativeLogCodec::decode_membership(&bad, 1).is_err());
        assert!(NativeLogCodec::decode_membership(&bytes, 0).is_err());
    }
}
