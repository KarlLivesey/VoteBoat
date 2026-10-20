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
use crate::contracts::StorageError;
use std::{cell::Cell, rc::Rc};
#[derive(Clone, Copy, Eq, PartialEq)]
pub(super) enum Fault {
    None,
    Refuse,
    Sequence,
    Consumed,
    Groups,
    Oversize,
    Envelope,
    BatchSequence,
    BatchPayload,
}
#[derive(Clone)]
pub(super) struct LogProvider(pub Rc<Cell<Fault>>);
impl LogCodec for LogProvider {
    fn format_version(&self) -> u32 {
        99
    }
    fn supports_checkpoint(&self) -> bool {
        true
    }
    fn encode_batch(
        &self,
        n: u64,
        m: &[LogMutation],
        l: LogLimits,
    ) -> Result<Vec<u8>, StorageError> {
        NativeLogCodec.encode_batch(n, m, l)
    }
    fn frame_length(&self, h: &[u8], l: LogLimits) -> Result<usize, StorageError> {
        NativeLogCodec.frame_length(h, l)
    }
    fn decode_batch(
        &self,
        b: &[u8],
        l: LogLimits,
    ) -> Result<(u64, Vec<LogMutation>), StorageError> {
        let (mut sequence, mut mutations) = NativeLogCodec.decode_batch(b, l)?;
        if self.0.get() == Fault::BatchSequence {
            sequence += 1;
        }
        if self.0.get() == Fault::BatchPayload {
            if let Some(LogMutation::Update(update)) = mutations.first_mut() {
                if let Some(suffix) = &mut update.suffix {
                    suffix.entries.edit(|entries| entries[0].term += 1);
                }
            }
        }
        Ok((sequence, mutations))
    }
    fn encode_checkpoint(
        &self,
        n: u64,
        s: &BTreeMap<GroupIdentity, GroupLog>,
        l: LogLimits,
        max: usize,
    ) -> Result<Vec<u8>, StorageError> {
        if self.0.get() == Fault::Refuse {
            return Err(StorageError::Rejected("controlled response encoding"));
        }
        let mut b = NativeLogCodec.encode_checkpoint(n, s, l, max)?;
        if self.0.get() == Fault::Oversize {
            b.resize(max + 1, 0);
        }
        if self.0.get() == Fault::Envelope && s.values().any(|g| !g.entries.is_empty()) {
            b.resize(max - 100, 0);
        }
        Ok(b)
    }
    fn decode_checkpoint(
        &self,
        b: &[u8],
        l: LogLimits,
    ) -> Result<(u64, BTreeMap<GroupIdentity, GroupLog>, usize), StorageError> {
        let (mut n, mut s, mut used) = NativeLogCodec.decode_checkpoint(b, l)?;
        if self.0.get() == Fault::Envelope {
            assert!(b[used..].iter().all(|v| *v == 0));
            used = b.len();
        }
        match self.0.get() {
            Fault::Sequence => n += 1,
            Fault::Consumed => used -= 1,
            Fault::Groups => {
                s.values_mut().next().unwrap().revision = LogRevision::new(99).unwrap()
            }
            _ => {}
        }
        Ok((n, s, used))
    }
}
#[derive(Clone)]
struct SnapshotProvider(Rc<Cell<bool>>);
impl SnapshotCodec for SnapshotProvider {
    fn format_version(&self) -> u32 {
        99
    }
    fn prefix(
        &self,
        m: &SnapshotMetadata,
        n: usize,
        l: SnapshotLimits,
    ) -> Result<Vec<u8>, StorageError> {
        NativeSnapshotCodec.prefix(m, n, l)
    }
    fn finish(&self, b: &[u8], l: SnapshotLimits) -> Result<[u8; 4], StorageError> {
        NativeSnapshotCodec.finish(b, l)
    }
    fn decode(&self, b: &[u8], l: SnapshotLimits) -> Result<Snapshot, StorageError> {
        let mut s = NativeSnapshotCodec.decode(b, l)?;
        if self.0.get() {
            s.application.push(3);
        }
        Ok(s)
    }
}
#[test]
fn refusing_or_nonfaithful_selected_log_codecs_cannot_encode_image() {
    for fault in [
        Fault::Refuse,
        Fault::Sequence,
        Fault::Consumed,
        Fault::Groups,
        Fault::Oversize,
    ] {
        let switch = Rc::new(Cell::new(Fault::None));
        let mut a = configured(
            LogicalLimits::default(),
            LogProvider(switch.clone()),
            NativeSnapshotCodec,
        )
        .unwrap();
        let c = Command {
            operation: OperationId::new(1).unwrap(),
            origin_configuration: ConfigurationId::new(1).unwrap(),
            representative: representative(),
            consumed_through: 0,
            body: CommandBody::Claim {
                expected_epoch: None,
            },
        };
        let bytes = a.encode_command(&c, usize::MAX).unwrap();
        a.apply_batch(&[LogEntry {
            index: 1,
            term: 1,
            payload: EntryPayload::Command {
                operation: c.operation,
                bytes: bytes.into(),
            },
        }])
        .unwrap();
        let value = a.read_at(1, LogicalQuery::Image).unwrap();
        let state = a.state.clone();
        switch.set(fault);
        let mut out = vec![0xa5; a.limits.max_parent_checkpoint_bytes];
        assert!(a.encode_read(&value, &mut out).is_err());
        assert!(out.iter().all(|b| *b == 0xa5));
        assert_eq!(a.state, state);
        assert!(configured(
            LogicalLimits::default(),
            LogProvider(switch),
            NativeSnapshotCodec
        )
        .is_err());
    }
}
#[test]
fn selected_snapshot_roundtrip_failure_leaves_output_and_state_unchanged() {
    let switch = Rc::new(Cell::new(false));
    let a = configured(
        LogicalLimits::default(),
        NativeLogCodec,
        SnapshotProvider(switch.clone()),
    )
    .unwrap();
    let snapshot = Snapshot {
        metadata: SnapshotMetadata {
            bootstrap: a.parent_bootstrap.clone(),
            membership: None,
            index: 1,
            term: 1,
            application_schema: 1,
        },
        application: vec![1, 2],
    };
    let value = LogicalRead::Snapshot(Box::new(snapshot));
    let mut out = vec![0xa5; a.limits.max_parent_checkpoint_bytes];
    assert!(a.encode_read(&value, &mut out).is_ok());
    out.fill(0xa5);
    let state = a.state.clone();
    switch.set(true);
    assert!(a.encode_read(&value, &mut out).is_err());
    assert!(out.iter().all(|b| *b == 0xa5));
    assert_eq!(a.state, state);
}

#[test]
fn original_configuration_response_rejects_nonfaithful_selected_batch_codec() {
    use crate::membership::{Configuration, ConfigurationChange, ConfigurationRecord};
    for fault in [Fault::BatchSequence, Fault::BatchPayload] {
        let switch = Rc::new(Cell::new(Fault::None));
        let mut a = configured(
            LogicalLimits::default(),
            LogProvider(switch.clone()),
            NativeSnapshotCodec,
        )
        .unwrap();
        let entry = LogEntry {
            index: 1,
            term: 1,
            payload: EntryPayload::Configuration(Box::new(ConfigurationRecord {
                operation: OperationId::new(20).unwrap(),
                expected: ConfigurationId::new(1).unwrap(),
                change: ConfigurationChange::Learners(
                    Configuration::new(
                        ConfigurationId::new(2).unwrap(),
                        a.child_bootstrap.policy.clone(),
                        a.child_bootstrap.voter_stores.clone(),
                        BTreeMap::new(),
                    )
                    .unwrap(),
                ),
            })),
        };
        a.apply_batch(&[entry]).unwrap();
        let value = a
            .read_at(
                1,
                LogicalQuery::ConfigurationOperation(OperationId::new(20).unwrap()),
            )
            .unwrap();
        let mut bytes = vec![0; a.limits.max_parent_checkpoint_bytes];
        let n = a.encode_read(&value, &mut bytes).unwrap();
        bytes.truncate(n);
        let state = a.state.clone();
        switch.set(fault);
        let mut out = vec![0xa5; a.limits.max_parent_checkpoint_bytes];
        assert!(a.encode_read(&value, &mut out).is_err());
        assert!(out.iter().all(|b| *b == 0xa5));
        assert_eq!(a.state, state);
        // Decode reports what this provider actually decodes, so sequence corruption
        // refuses; payload corruption is inspection and cannot mint a permit.
        if fault == Fault::BatchSequence {
            assert!(a.decode_configuration_operation(&bytes).is_err());
        }
    }
}
