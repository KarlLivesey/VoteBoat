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
        let mut output = Vec::new();
        self.apply_batch_into(entries, &mut output)?;
        Ok(output)
    }
    /// One call per committed interval, as `apply_batch`, visiting each
    /// shared piece in order instead of first copying the interval.
    fn apply_entries(
        &mut self,
        entries: EntryRange<'_>,
    ) -> Result<Vec<OperationId>, ApplicationError> {
        let mut output = Vec::new();
        self.apply_entries_into(entries, &mut output)?;
        Ok(output)
    }
    fn apply_batch_into(
        &mut self,
        entries: &[LogEntry],
        output: &mut Vec<OperationId>,
    ) -> Result<(), ApplicationError> {
        self.apply_pieces_into(std::iter::once(entries), output)
    }
    fn apply_entries_into(
        &mut self,
        entries: EntryRange<'_>,
        output: &mut Vec<OperationId>,
    ) -> Result<(), ApplicationError> {
        match entries.as_slice() {
            Some(entries) => self.apply_pieces_into(std::iter::once(entries), output),
            None => self.apply_pieces_into(entries.slices(), output),
        }
    }
}
impl Noop {
    /// Validate the entire interval before advancing, with no exposed partial
    /// receipts on failure. Every replica produces every original outcome.
    fn apply_pieces_into<'a>(
        &mut self,
        pieces: impl Iterator<Item = &'a [LogEntry]>,
        output: &mut Vec<OperationId>,
    ) -> Result<(), ApplicationError> {
        output.clear();
        let result = self.stage_pieces(pieces, output);
        if result.is_err() {
            output.clear();
        }
        result
    }
    fn stage_pieces<'a>(
        &mut self,
        pieces: impl Iterator<Item = &'a [LogEntry]>,
        results: &mut Vec<OperationId>,
    ) -> Result<(), ApplicationError> {
        let mut applied = self.applied;
        for entries in pieces {
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
        }
        self.applied = applied;
        Ok(())
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
    #[test]
    fn caller_output_matches_owned_piecewise_mixed_noop_intervals() {
        for width in [1, 231, 515] {
            let mut owned = app();
            let mut reused = app();
            let mut output = Vec::with_capacity(width);
            let pointer = output.as_ptr();
            for round in 0..3 {
                let entries: Vec<_> = (0..width)
                    .map(|offset| {
                        let index = (round * width + offset + 1) as u64;
                        LogEntry {
                            index,
                            term: 1,
                            payload: if offset % 5 == 4 {
                                EntryPayload::Noop
                            } else {
                                EntryPayload::Command {
                                    operation: OperationId::new((1 << 127) + index as u128 * 7919)
                                        .unwrap(),
                                    bytes: Vec::new().into(),
                                }
                            },
                        }
                    })
                    .collect();
                let mut log = EntryLog::default();
                for chunk in entries.chunks(17) {
                    log.append(chunk.to_vec().into());
                }
                let expected = owned
                    .apply_entries(log.borrowed(0..width).unwrap())
                    .unwrap();
                reused
                    .apply_entries_into(log.borrowed(0..width).unwrap(), &mut output)
                    .unwrap();
                assert_eq!(output, expected);
                assert_eq!(reused.applied, owned.applied);
                assert_eq!(output.as_ptr(), pointer);
            }
        }
    }
    #[test]
    fn caller_noop_configuration_only_interval_advances_without_receipts() {
        use voteboat::membership::{ConfigurationChange, ConfigurationRecord};
        for width in [1, 231, 515] {
            let entries: Vec<_> = (1..=width)
                .map(|index| LogEntry {
                    index: index as u64,
                    term: 1,
                    payload: if index % 2 == 0 {
                        EntryPayload::Noop
                    } else {
                        EntryPayload::Configuration(Box::new(ConfigurationRecord {
                            operation: OperationId::new(index as u128).unwrap(),
                            expected: ConfigurationId::new(1).unwrap(),
                            change: ConfigurationChange::Final {
                                id: ConfigurationId::new(2).unwrap(),
                            },
                        }))
                    },
                })
                .collect();
            let mut log = EntryLog::default();
            for chunk in entries.chunks(17) {
                log.append(chunk.to_vec().into());
            }
            let mut output = vec![OperationId::new(u128::MAX).unwrap()];
            let pointer = output.as_ptr();
            let mut app = app();
            app.apply_entries_into(log.borrowed(0..width).unwrap(), &mut output)
                .unwrap();
            assert!(output.is_empty());
            assert_eq!(app.applied, width as u64);
            assert_eq!(output.as_ptr(), pointer);
        }
    }
}
