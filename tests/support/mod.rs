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
#![allow(dead_code)]
pub mod admission;
pub mod buffer;
pub mod outbound;
#[cfg(feature = "tls")]
pub mod peer_fault;
pub mod snapshot;
#[cfg(feature = "tls")]
pub mod tls;
use std::collections::BTreeMap;
#[cfg(feature = "native")]
use std::{cell::RefCell, io, rc::Rc};
#[cfg(feature = "native")]
use voteboat::native::log_store::JournalIo;
use voteboat::{contracts::*, identity::*, log::*, quorum::*};
pub fn node(id: u64) -> NodeId {
    NodeId::new(id).unwrap()
}
pub fn group(id: u128) -> GroupIdentity {
    GroupIdentity {
        id: GroupId::new(id).unwrap(),
        incarnation: GroupIncarnation::new(1).unwrap(),
    }
}
pub fn identity(id: u128) -> StoreIdentity {
    StoreIdentity {
        id: StoreId::new(id).unwrap(),
        incarnation: StoreIncarnation::new(1).unwrap(),
    }
}
pub fn bootstrap(g: u128, nodes: u64) -> Bootstrap {
    Bootstrap {
        group: group(g),
        configuration: ConfigurationId::new(1).unwrap(),
        policy: Policy::new(
            Tree::Majority((1..=nodes).map(|id| Tree::Voter(node(id))).collect()),
            Limits::default(),
        )
        .unwrap(),
        voter_stores: (1..=nodes)
            .map(|id| (node(id), identity(id as u128)))
            .collect(),
    }
}
pub fn update(state: &GroupLog, term: u64, commit: u64, suffix: Option<Suffix>) -> LogMutation {
    LogMutation::Update(LogUpdate {
        snapshot_membership: None,
        group: state.bootstrap.group,
        expected_revision: state.revision,
        hard_state: HardState {
            term,
            voted_for: None,
        },
        commit_index: commit,
        suffix,
        snapshot: None,
    })
}
pub fn entry(index: u64, term: u64, value: u8) -> LogEntry {
    LogEntry {
        index,
        term,
        payload: EntryPayload::Command {
            operation: OperationId::new(index as u128 + 100).unwrap(),
            bytes: vec![value],
        },
    }
}
pub fn append<S: LogStore>(store: &mut S, mutations: Vec<LogMutation>) -> Vec<LogTicket> {
    let tickets = store.append_batch(mutations).unwrap();
    store.barrier(&tickets).unwrap();
    tickets
}

#[derive(Clone, Copy, Debug, Default)]
#[cfg(feature = "native")]
pub enum Fault {
    #[default]
    None,
    Append(usize),
    Sync,
    PublishBefore,
    PublishAfter,
    ReplaceBefore,
    ReplaceAfter,
}
#[derive(Default)]
#[cfg(feature = "native")]
pub struct Device {
    pub log: Vec<u8>,
    pub synced: Vec<u8>,
    pub manifest: Option<Vec<u8>>,
    pub fault: Fault,
}
#[cfg(feature = "native")]
impl Device {
    pub fn power_loss(&mut self) {
        self.log = self.synced.clone();
        self.fault = Fault::None;
    }
}
#[derive(Clone, Default)]
#[cfg(feature = "native")]
pub struct ModelIo(pub Rc<RefCell<Device>>);
#[cfg(feature = "native")]
impl JournalIo for ModelIo {
    fn supports_replacement(&self) -> bool {
        true
    }
    fn replace_log(&mut self, bytes: &[u8], manifest: &[u8]) -> io::Result<()> {
        let mut d = self.0.borrow_mut();
        if matches!(d.fault, Fault::ReplaceBefore) {
            return Err(io::Error::other("before replacement publication"));
        }
        d.log = bytes.to_vec();
        d.synced = bytes.to_vec();
        d.manifest = Some(manifest.to_vec());
        if matches!(d.fault, Fault::ReplaceAfter) {
            return Err(io::Error::other("after replacement publication"));
        }
        Ok(())
    }
    fn read_manifest(&mut self) -> io::Result<Vec<u8>> {
        self.0
            .borrow()
            .manifest
            .clone()
            .ok_or(io::ErrorKind::NotFound.into())
    }
    fn read_log(&mut self, limit: usize) -> io::Result<Vec<u8>> {
        let d = self.0.borrow();
        if d.log.len() > limit {
            return Err(io::Error::other("oversized WAL"));
        }
        Ok(d.log.clone())
    }
    fn append(&mut self, bytes: &[u8]) -> io::Result<()> {
        let mut d = self.0.borrow_mut();
        if let Fault::Append(cut) = d.fault {
            d.log.extend(&bytes[..cut.min(bytes.len())]);
            return Err(io::Error::other("short write"));
        }
        d.log.extend(bytes);
        Ok(())
    }
    fn sync_log(&mut self) -> io::Result<()> {
        let mut d = self.0.borrow_mut();
        if matches!(d.fault, Fault::Sync) {
            return Err(io::Error::other("failed sync"));
        }
        d.synced = d.log.clone();
        Ok(())
    }
    fn truncate_log(&mut self, len: u64) -> io::Result<()> {
        self.0.borrow_mut().log.truncate(len as usize);
        Ok(())
    }
    fn publish_manifest(&mut self, b: &[u8]) -> io::Result<()> {
        let mut d = self.0.borrow_mut();
        if matches!(d.fault, Fault::PublishBefore) {
            return Err(io::Error::other("before rename"));
        }
        d.manifest = Some(b.to_vec());
        if matches!(d.fault, Fault::PublishAfter) {
            return Err(io::Error::other("after rename"));
        }
        Ok(())
    }
}

/// Local downstream alternative implementing the public logical store seam.
/// The native framing and platform I/O are not required by this provider.
pub struct HostLogStore {
    pub binding: StoreBinding,
    pub accepted: BTreeMap<GroupIdentity, GroupLog>,
    pub durable: BTreeMap<GroupIdentity, GroupLog>,
    pub sequence: u64,
    pub pending: Vec<LogTicket>,
}
impl HostLogStore {
    pub fn new(id: u128) -> Self {
        Self {
            binding: StoreBinding {
                identity: identity(id),
                session: StoreSession::new(1).unwrap(),
            },
            accepted: BTreeMap::new(),
            durable: BTreeMap::new(),
            sequence: 0,
            pending: Vec::new(),
        }
    }
}
impl LogStore for HostLogStore {
    fn binding(&self) -> StoreBinding {
        self.binding
    }
    fn limits(&self) -> LogLimits {
        LogLimits::default()
    }
    fn state(&self, g: GroupIdentity) -> Result<GroupLog, StorageError> {
        self.durable
            .get(&g)
            .cloned()
            .ok_or(StorageError::Rejected("unknown group"))
    }
    fn append_batch(
        &mut self,
        mutations: Vec<LogMutation>,
    ) -> Result<Vec<LogTicket>, StorageError> {
        let limits = self.limits();
        if mutations.iter().any(|m|matches!(m,LogMutation::Update(u) if u.snapshot.is_some_and(|r|r.store!=self.binding.identity))) {return Err(StorageError::WrongIdentity);}
        apply_batch(&mut self.accepted, &mutations, limits)?;
        self.sequence += 1;
        let tickets = mutations
            .iter()
            .map(|m| {
                let g = match m {
                    LogMutation::Create(b) => b.group,
                    LogMutation::Update(u) => u.group,
                };
                let s = &self.accepted[&g];
                LogTicket {
                    binding: self.binding,
                    batch: self.sequence,
                    group: g,
                    revision: s.revision,
                    generation: s.generation,
                    last_index: s.last_index(),
                    term: s.hard_state.term,
                }
            })
            .collect::<Vec<_>>();
        self.pending
            .retain(|t| self.accepted[&t.group].generation == t.generation);
        self.pending.extend(&tickets);
        Ok(tickets)
    }
    fn barrier(&mut self, tickets: &[LogTicket]) -> Result<DurableLog, StorageError> {
        if tickets.iter().any(|t| !self.pending.contains(t)) {
            return Err(StorageError::StaleTicket);
        }
        self.durable = self.accepted.clone();
        self.pending.retain(|t| !tickets.contains(t));
        Ok(DurableLog {
            tickets: tickets.to_vec(),
        })
    }
    fn fetch_range(
        &self,
        g: GroupIdentity,
        generation: LogGeneration,
        from: u64,
        count: usize,
        bytes: usize,
    ) -> Result<Vec<LogEntry>, StorageError> {
        let s = self.state(g)?;
        if generation != s.generation {
            return Err(StorageError::StaleTicket);
        }
        if from == 0 || from > s.last_index() + 1 || count == 0 || bytes == 0 {
            return Err(StorageError::Rejected("range"));
        }
        if from <= s.base_index() {
            return Err(StorageError::Compacted {
                first_index: s.base_index() + 1,
            });
        }
        let mut total = 0;
        let mut result = Vec::new();
        for e in s
            .entries
            .iter()
            .skip((from - s.base_index() - 1) as usize)
            .take(count)
        {
            let size = 37 + e.payload_bytes();
            if total + size > bytes {
                if result.is_empty() {
                    return Err(StorageError::Rejected("budget"));
                }
                break;
            }
            total += size;
            result.push(e.clone());
        }
        Ok(result)
    }
}
