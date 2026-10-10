// SPDX-License-Identifier: RPL-1.5
// Copyright (c) 2026 Karl Livesey
use super::*;
use std::collections::BTreeMap;
#[derive(Clone)]
pub(super) struct HostSet {
    capacity: usize,
    applied: u64,
    value: i64,
    operations: BTreeMap<OperationId, i64>,
}
#[derive(Debug)]
pub(super) struct Receipt {
    index: u64,
    operation: OperationId,
    reply: String,
    duplicate: bool,
}
impl HostSet {
    fn new(capacity: usize) -> Self {
        Self {
            capacity,
            applied: 0,
            value: 0,
            operations: BTreeMap::new(),
        }
    }
    fn set(
        &mut self,
        operation: OperationId,
        bytes: &[u8],
        index: u64,
    ) -> Result<Receipt, ApplicationError> {
        let value = std::str::from_utf8(bytes)
            .ok()
            .and_then(|s| s.strip_prefix("set:"))
            .and_then(|s| s.parse::<i64>().ok())
            .ok_or(ApplicationError::InvalidCommand)?;
        let duplicate = self.operations.contains_key(&operation);
        let mut reply = String::with_capacity(32);
        if let Some(previous) = self.operations.get(&operation) {
            if *previous == value {
                use std::fmt::Write;
                write!(&mut reply, "set={previous}").unwrap();
            } else {
                reply.push_str("conflict");
            }
        } else {
            if self.operations.len() == self.capacity {
                return Err(ApplicationError::DedupCapacity);
            }
            self.operations.insert(operation, value);
            self.value = value;
            use std::fmt::Write;
            write!(&mut reply, "set={value}").unwrap();
        }
        Ok(Receipt {
            index,
            operation,
            reply,
            duplicate,
        })
    }
}
impl ApplicationReceipt for Receipt {
    fn index(&self) -> u64 {
        self.index
    }
    fn operation(&self) -> OperationId {
        self.operation
    }
    fn nested_bytes(&self, limit: usize) -> Result<usize, ApplicationError> {
        if self.reply.capacity() > limit {
            return Err(ApplicationError::ReceiptBudget);
        }
        Ok(self.reply.capacity())
    }
}
impl BoundedStateMachine for HostSet {
    fn receipt_bytes_bound(&self, entries: &[LogEntry]) -> Result<usize, ApplicationError> {
        entries
            .iter()
            .filter(|e| matches!(e.payload, EntryPayload::Command { .. }))
            .count()
            .checked_mul(std::mem::size_of::<Receipt>() + 32)
            .ok_or(ApplicationError::ReceiptBudget)
    }
}
impl StateMachine for HostSet {
    type Receipt = Receipt;
    fn applied_index(&self) -> u64 {
        self.applied
    }
    fn apply_batch(&mut self, entries: &[LogEntry]) -> Result<Vec<Receipt>, ApplicationError> {
        let mut next = self.clone();
        let commands = entries
            .iter()
            .filter(|e| matches!(e.payload, EntryPayload::Command { .. }))
            .count();
        let mut receipts = Vec::with_capacity(commands);
        for entry in entries {
            if entry.index != next.applied + 1 {
                return Err(ApplicationError::IndexGap);
            }
            if let EntryPayload::Command { operation, bytes } = &entry.payload {
                receipts.push(next.set(*operation, bytes, entry.index)?);
            }
            next.applied = entry.index;
        }
        *self = next;
        Ok(receipts)
    }
}
impl ReadableStateMachine for HostSet {
    type Query = String;
    type ReadResult = String;
    fn read_at(&self, required_index: u64, query: String) -> Result<String, ApplicationError> {
        if query != "value" {
            return Err(ApplicationError::InvalidCommand);
        }
        if required_index > self.applied {
            return Err(ApplicationError::NotApplied);
        }
        Ok(format!("value={}", self.value))
    }
}
impl CheckpointStateMachine for HostSet {
    fn schema_version(&self) -> u64 {
        77
    }
    fn checkpoint(&self, max_bytes: usize) -> Result<Vec<u8>, ApplicationError> {
        let size = 32 + 24 * self.operations.len();
        if size > max_bytes {
            return Err(ApplicationError::InvalidCheckpoint);
        }
        let mut bytes = Vec::with_capacity(size);
        bytes.extend(b"HOSTSET1");
        bytes.extend(u32::try_from(self.capacity).unwrap().to_le_bytes());
        bytes.extend(self.applied.to_le_bytes());
        bytes.extend(self.value.to_le_bytes());
        bytes.extend(u32::try_from(self.operations.len()).unwrap().to_le_bytes());
        for (operation, value) in &self.operations {
            bytes.extend(operation.get().to_le_bytes());
            bytes.extend(value.to_le_bytes());
        }
        Ok(bytes)
    }
    fn restore_checkpoint(
        &mut self,
        schema: u64,
        applied: u64,
        bytes: &[u8],
    ) -> Result<(), ApplicationError> {
        if schema != 77 {
            return Err(ApplicationError::UnsupportedSchema);
        }
        if bytes.len() < 32 || &bytes[..8] != b"HOSTSET1" {
            return Err(ApplicationError::InvalidCheckpoint);
        }
        let capacity = u32::from_le_bytes(bytes[8..12].try_into().unwrap()) as usize;
        let index = u64::from_le_bytes(bytes[12..20].try_into().unwrap());
        let value = i64::from_le_bytes(bytes[20..28].try_into().unwrap());
        let count = u32::from_le_bytes(bytes[28..32].try_into().unwrap()) as usize;
        if capacity != self.capacity
            || index != applied
            || count > capacity
            || count as u64 > applied
            || bytes.len() != 32 + count * 24
        {
            return Err(ApplicationError::InvalidCheckpoint);
        }
        let mut operations = BTreeMap::new();
        for record in bytes[32..].as_chunks::<24>().0 {
            let operation = OperationId::new(u128::from_le_bytes(record[..16].try_into().unwrap()))
                .ok_or(ApplicationError::InvalidCheckpoint)?;
            let value = i64::from_le_bytes(record[16..].try_into().unwrap());
            if operations.insert(operation, value).is_some() {
                return Err(ApplicationError::InvalidCheckpoint);
            }
        }
        *self = Self {
            capacity,
            applied,
            value,
            operations,
        };
        Ok(())
    }
}
pub(super) struct HostCase;
impl Scenario for HostCase {
    type Receipt = Receipt;
    type App = HostSet;
    type Reply = (String, bool);
    type Value = String;
    fn fresh(capacity: usize) -> HostSet {
        HostSet::new(capacity)
    }
    fn requirements() -> Option<voteboat::raft::ReadinessRequirements> {
        None
    }
    fn command(index: u64, operation: u128, value: i64) -> LogEntry {
        LogEntry {
            payload: EntryPayload::Command {
                operation: OperationId::new(operation).unwrap(),
                bytes: format!("set:{value}").into_bytes(),
            },
            index,
            term: 1,
        }
    }
    fn reply(receipt: &Receipt) -> Self::Reply {
        (receipt.reply.clone(), receipt.duplicate)
    }
    fn replies(replay: bool) -> Vec<Self::Reply> {
        if replay {
            vec![
                ("set=7".to_owned(), true),
                ("set=11".to_owned(), true),
                ("conflict".to_owned(), true),
                ("set=5".to_owned(), false),
            ]
        } else {
            vec![("set=7".to_owned(), false), ("set=11".to_owned(), false)]
        }
    }
    fn query() -> String {
        "value".to_owned()
    }
    fn value(result: String) -> String {
        result
    }
    fn expected_value(replay: bool) -> String {
        format!("value={}", if replay { 5 } else { 11 })
    }
}
