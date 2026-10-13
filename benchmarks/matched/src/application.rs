// SPDX-License-Identifier: RPL-1.5
// Copyright (c) 2026 Karl Livesey
use voteboat::{application::*, identity::*, log::*};

/// Like the pinned reference, this application has no business/dedup state.
/// Original invocation identities are tracked by the owning benchmark until reply.
#[derive(Clone)]
pub(super) struct Noop {
    pub group: GroupIdentity,
    pub applied: u64,
}
impl StateMachine for Noop {
    type Receipt = OperationId;
    fn validate_group(&self, group: GroupIdentity) -> Result<(), ApplicationError> {
        if group != self.group {
            return Err(ApplicationError::InvalidCommand);
        }
        Ok(())
    }
    fn applied_index(&self) -> u64 {
        self.applied
    }
    fn apply_batch(&mut self, entries: &[LogEntry]) -> Result<Vec<OperationId>, ApplicationError> {
        let mut applied = self.applied;
        let mut results = Vec::new();
        for entry in entries {
            if applied.checked_add(1) != Some(entry.index) {
                return Err(ApplicationError::IndexGap);
            }
            if let EntryPayload::Command { operation, bytes } = &entry.payload {
                if !bytes.is_empty() {
                    return Err(ApplicationError::InvalidCommand);
                }
                results.push(*operation);
            }
            applied = entry.index;
        }
        self.applied = applied;
        Ok(results)
    }
}
impl CheckpointStateMachine for Noop {
    fn schema_version(&self) -> u64 {
        1
    }
    fn checkpoint(&self, max_bytes: usize) -> Result<Vec<u8>, ApplicationError> {
        if max_bytes < 8 {
            return Err(ApplicationError::InvalidCheckpoint);
        }
        Ok(self.applied.to_le_bytes().to_vec())
    }
    fn restore_checkpoint(
        &mut self,
        schema: u64,
        applied: u64,
        bytes: &[u8],
    ) -> Result<(), ApplicationError> {
        if schema != 1 || bytes != applied.to_le_bytes() {
            return Err(ApplicationError::InvalidCheckpoint);
        }
        self.applied = applied;
        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    fn app() -> Noop {
        Noop {
            group: GroupIdentity {
                id: GroupId::new(1).unwrap(),
                incarnation: GroupIncarnation::new(1).unwrap(),
            },
            applied: 0,
        }
    }
    #[test]
    fn partial_prefix_error_keeps_original_application_frontier() {
        let mut app = app();
        let entries = [
            LogEntry {
                index: 1,
                term: 1,
                payload: EntryPayload::Noop,
            },
            LogEntry {
                index: 3,
                term: 1,
                payload: EntryPayload::Noop,
            },
        ];
        assert_eq!(app.apply_batch(&entries), Err(ApplicationError::IndexGap));
        assert_eq!(app.applied, 0);
    }
    #[test]
    fn historical_noop_checkpoint_restores_only_its_exact_boundary() {
        let app = Noop {
            applied: 5001,
            ..app()
        };
        let historical = Noop {
            applied: 4001,
            ..app.clone()
        };
        let bytes = historical.checkpoint(8).unwrap();
        let mut restored = app.clone();
        assert!(restored.restore_checkpoint(1, 5001, &bytes).is_err());
        assert_eq!(restored.applied, 5001);
        restored.restore_checkpoint(1, 4001, &bytes).unwrap();
        assert_eq!(restored.applied, 4001);
    }
}
