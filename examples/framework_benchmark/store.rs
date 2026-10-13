// SPDX-License-Identifier: RPL-1.5
// Copyright (c) 2026 Karl Livesey
//! Exact-ticket volatile completion through the public logical store seam.
use voteboat::{contracts::StorageError, identity::*, log::*};

pub(super) struct MemoryStore {
    binding: StoreBinding,
    limits: LogLimits,
    state: OwnedLogState,
    sequence: u64,
    pending: Vec<LogTicket>,
}
impl MemoryStore {
    pub fn new(
        binding: StoreBinding,
        limits: LogLimits,
        bootstrap: Bootstrap,
    ) -> Result<Self, StorageError> {
        let mut state = OwnedLogState::new(limits)?;
        state.apply_batch(vec![LogMutation::Create(bootstrap)])?;
        Ok(Self {
            binding,
            limits,
            state,
            sequence: 0,
            pending: Vec::new(),
        })
    }
    fn completed_state(&self, group: GroupIdentity) -> Result<&GroupLog, StorageError> {
        if !self.pending.is_empty() {
            return Err(StorageError::Rejected("volatile transition not completed"));
        }
        self.state
            .get(group)
            .ok_or(StorageError::Rejected("unknown group"))
    }
}
impl LogStore for MemoryStore {
    fn binding(&self) -> StoreBinding {
        self.binding
    }
    fn limits(&self) -> LogLimits {
        self.limits
    }
    fn state(&self, group: GroupIdentity) -> Result<GroupLog, StorageError> {
        self.completed_state(group).cloned()
    }
    fn append_batch(
        &mut self,
        mutations: Vec<LogMutation>,
    ) -> Result<Vec<LogTicket>, StorageError> {
        if !self.pending.is_empty() {
            return Err(StorageError::Rejected(
                "one outstanding volatile transition",
            ));
        }
        let sequence = self
            .sequence
            .checked_add(1)
            .ok_or(StorageError::Rejected("sequence exhausted"))?;
        if mutations
            .iter()
            .any(|m| matches!(m, LogMutation::Update(u) if u.snapshot.is_some()))
        {
            return Err(StorageError::Rejected(
                "scaffold does not implement checkpoints",
            ));
        }
        let groups: Vec<_> = mutations
            .iter()
            .map(|mutation| match mutation {
                LogMutation::Create(bootstrap) => bootstrap.group,
                LogMutation::Update(update) => update.group,
            })
            .collect();
        self.state.apply_batch(mutations)?;
        self.sequence = sequence;
        let tickets: Vec<_> = groups
            .iter()
            .map(|&group| {
                let state = self.state.get(group).unwrap();
                LogTicket {
                    binding: self.binding,
                    batch: sequence,
                    group,
                    revision: state.revision,
                    generation: state.generation,
                    last_index: state.last_index(),
                    term: state.hard_state.term,
                }
            })
            .collect();
        self.pending.clone_from(&tickets);
        Ok(tickets)
    }
    fn barrier(&mut self, dependencies: &[LogTicket]) -> Result<DurableLog, StorageError> {
        if dependencies.is_empty() || self.pending.as_slice() != dependencies {
            return Err(StorageError::StaleTicket);
        }
        self.pending.clear();
        Ok(DurableLog {
            tickets: dependencies.to_vec(),
        })
    }
    fn fetch_range(
        &self,
        group: GroupIdentity,
        generation: LogGeneration,
        from: u64,
        count: usize,
        bytes: usize,
    ) -> Result<Vec<LogEntry>, StorageError> {
        let state = self.completed_state(group)?;
        if state.generation != generation {
            return Err(StorageError::StaleTicket);
        }
        if from == 0 || from > state.last_index() + 1 || count == 0 || bytes == 0 {
            return Err(StorageError::Rejected("range"));
        }
        if from <= state.base_index() {
            return Err(StorageError::Compacted {
                first_index: state.base_index() + 1,
            });
        }
        let offset = usize::try_from(from - state.base_index() - 1)
            .map_err(|_| StorageError::Rejected("range offset"))?;
        let mut used = 0usize;
        let mut result = Vec::new();
        for entry in state.entries.iter().skip(offset).take(count) {
            let size = 37 + entry.payload_bytes();
            if size > bytes - used {
                if result.is_empty() {
                    return Err(StorageError::Rejected("range bytes"));
                }
                break;
            }
            used += size;
            result.push(entry.clone());
        }
        Ok(result)
    }
}
