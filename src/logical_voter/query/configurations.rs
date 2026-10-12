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
use crate::membership::ConfigurationChange;

pub(super) fn charge(h: &LogicalConfigurationOperation) -> usize {
    size_of::<LogicalConfigurationOperation>()
        .saturating_add(h.entries.capacity().saturating_mul(size_of::<LogEntry>()))
        .saturating_add(
            h.entries
                .iter()
                .map(LogEntry::retained_payload_bytes)
                .fold(0usize, usize::saturating_add),
        )
}
impl<C: LogCodec + Clone, S: SnapshotCodec + Clone, W: WireCodec + Clone>
    LogicalApplication<C, S, W>
{
    pub(in crate::logical_voter) fn configuration_operation(
        &self,
        operation: OperationId,
    ) -> Result<LogicalConfigurationOperation, ApplicationError> {
        let mut h = LogicalConfigurationOperation {
            child: self.mapping.child,
            operation,
            child_applied: self.state.applied,
            entries: Vec::with_capacity(2),
        };
        for entry in &self.state.configurations {
            if matches!(&entry.payload, EntryPayload::Configuration(c) if c.operation == operation)
            {
                if h.entries.len() == 2 {
                    return Err(ApplicationError::InvalidCommand);
                }
                h.entries.push(entry.clone());
            }
        }
        self.validate_configuration_operation(&h)?;
        Ok(h)
    }
    pub(in crate::logical_voter) fn validate_configuration_operation(
        &self,
        h: &LogicalConfigurationOperation,
    ) -> Result<(), ApplicationError> {
        if h.child != self.mapping.child || h.entries.len() > 2 {
            return Err(ApplicationError::InvalidCommand);
        }
        for entry in &h.entries {
            let EntryPayload::Configuration(c) = &entry.payload else {
                return Err(ApplicationError::InvalidCommand);
            };
            if c.operation != h.operation
                || entry.index == 0
                || entry.index > h.child_applied
                || entry.term == 0
            {
                return Err(ApplicationError::InvalidCommand);
            }
        }
        if let Some(first) = h.entries.first() {
            validate_first(first)?;
            if let Some(second) = h.entries.get(1) {
                validate_pair(first, second)?;
            }
        }
        if charge(h) > self.limits.max_parent_checkpoint_bytes {
            return Err(ApplicationError::ReceiptBudget);
        }
        Ok(())
    }
    pub(in crate::logical_voter) fn validate_configuration_envelopes(
        &self,
    ) -> Result<(), ApplicationError> {
        let mut histories: BTreeMap<OperationId, Vec<&LogEntry>> = BTreeMap::new();
        for entry in &self.state.configurations {
            let EntryPayload::Configuration(c) = &entry.payload else {
                return Err(ApplicationError::InvalidCommand);
            };
            let history = histories.entry(c.operation).or_default();
            if history.len() == 2 {
                return Err(ApplicationError::InvalidCommand);
            }
            history.push(entry);
        }
        for (operation, entries) in histories {
            let value =
                LogicalRead::ConfigurationOperation(Box::new(LogicalConfigurationOperation {
                    child: self.mapping.child,
                    operation,
                    child_applied: self.state.applied,
                    entries: entries.into_iter().cloned().collect(),
                }));
            let mut out = vec![0; self.limits.max_parent_checkpoint_bytes];
            crate::runtime::LogicalResponseCodec::encode_read(self, &value, &mut out)?;
        }
        Ok(())
    }
}

fn validate_first(entry: &LogEntry) -> Result<(), ApplicationError> {
    let EntryPayload::Configuration(c) = &entry.payload else {
        return Err(ApplicationError::InvalidCommand);
    };
    match &c.change {
        ConfigurationChange::Learners(next) if next.id() > c.expected => Ok(()),
        ConfigurationChange::Joint { id, next } if *id > c.expected && next.id() > *id => Ok(()),
        _ => Err(ApplicationError::InvalidCommand),
    }
}
fn validate_pair(first: &LogEntry, second: &LogEntry) -> Result<(), ApplicationError> {
    let (EntryPayload::Configuration(a), EntryPayload::Configuration(b)) =
        (&first.payload, &second.payload)
    else {
        return Err(ApplicationError::InvalidCommand);
    };
    match (&a.change, &b.change) {
        (ConfigurationChange::Joint { id: joint, next }, ConfigurationChange::Final { id })
            if b.expected == *joint
                && *id == next.id()
                && second.index > first.index
                && second.term >= first.term =>
        {
            Ok(())
        }
        _ => Err(ApplicationError::InvalidCommand),
    }
}
