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
use std::{cell::Cell, rc::Rc};
#[test]
fn short_read_destinations_and_overbound_values_are_atomic() {
    let mut a = app();
    claim(&mut a);
    for q in [
        LogicalQuery::Image,
        LogicalQuery::Metadata,
        LogicalQuery::Outcome(OperationId::new(1).unwrap()),
    ] {
        let value = a.read_at(1, q).unwrap();
        let n = encoded(&a, &value).len();
        for len in 0..n {
            let mut out = vec![0xa5; len];
            assert!(a.encode_read(&value, &mut out).is_err());
            assert!(out.iter().all(|b| *b == 0xa5));
        }
    }
    let mut status = LogicalActionStatus {
        operation: OperationId::new(1).unwrap(),
        first_index: 1,
        first_term: 1,
        preceding_configuration: ConfigurationId::new(1).unwrap(),
        command: vec![0; a.limits.max_command_bytes + 1],
        receipt: receipt(),
    };
    let mut out = vec![0xa5; a.limits.max_parent_checkpoint_bytes];
    let state = a.state.clone();
    assert!(a
        .encode_read(
            &LogicalRead::Outcome(Some(Box::new(status.clone()))),
            &mut out
        )
        .is_err());
    status.command.clear();
    let mut i = image(&a);
    i.pins = vec![reference(); 3];
    assert!(a
        .encode_read(&LogicalRead::Image(Box::new(i)), &mut out)
        .is_err());
    assert!(out.iter().all(|b| *b == 0xa5));
    assert_eq!(a.state, state);
}
#[test]
fn bounds_stay_stable_across_apply_checkpoint_and_restore() {
    let mut a = app();
    let queries = [
        LogicalQuery::Image,
        LogicalQuery::Outcome(OperationId::new(1).unwrap()),
        LogicalQuery::Snapshot(reference()),
        LogicalQuery::Metadata,
    ];
    let bounds = queries.map(|q| {
        (
            a.query_encoding_bound(&q).unwrap(),
            a.read_encoding_bound(&q).unwrap(),
        )
    });
    claim(&mut a);
    let checkpoint = a.checkpoint(usize::MAX).unwrap();
    let mut restored = app();
    restored
        .restore_checkpoint(LOGICAL_APPLICATION_SCHEMA, a.applied_index(), &checkpoint)
        .unwrap();
    assert_eq!(
        queries.map(|q| (
            restored.query_encoding_bound(&q).unwrap(),
            restored.read_encoding_bound(&q).unwrap()
        )),
        bounds
    );
    for query in queries {
        let original = a.read_at(1, query).unwrap();
        let recovered = restored.read_at(1, query).unwrap();
        assert_eq!(original, recovered);
        assert_eq!(encoded(&a, &original), encoded(&restored, &recovered));
    }
}
#[test]
fn accepted_state_growth_cannot_exceed_the_checked_response_envelope() {
    let switch = Rc::new(Cell::new(providers::Fault::None));
    let limits = LogicalLimits {
        max_parent_checkpoint_bytes: 32768,
        ..LogicalLimits::default()
    };
    let mut a = configured(
        limits,
        providers::LogProvider(switch.clone()),
        NativeSnapshotCodec,
    )
    .unwrap();
    let c = Command {
        operation: OperationId::new(1).unwrap(),
        origin_configuration: ConfigurationId::new(1).unwrap(),
        representative: representative(),
        body: CommandBody::Claim {
            expected_epoch: None,
        },
    };
    let bytes = a.encode_command(&c, usize::MAX).unwrap();
    let before = a.state.clone();
    switch.set(providers::Fault::Oversize);
    assert!(a
        .apply_batch(&[LogEntry {
            index: 1,
            term: 1,
            payload: EntryPayload::Command {
                operation: c.operation,
                bytes: bytes.into()
            }
        }])
        .is_err());
    assert_eq!(a.state, before);
    assert_eq!(a.applied_index(), 0);
}

#[test]
fn full_image_framing_refuses_growth_even_when_parent_codec_fits() {
    let limits = LogicalLimits {
        max_parent_checkpoint_bytes: 32768,
        ..LogicalLimits::default()
    };
    let codec = providers::LogProvider(Rc::new(Cell::new(providers::Fault::Envelope)));
    let mut a = configured(limits, codec.clone(), NativeSnapshotCodec).unwrap();
    let claim = Command {
        operation: OperationId::new(1).unwrap(),
        origin_configuration: ConfigurationId::new(1).unwrap(),
        representative: representative(),
        body: CommandBody::Claim {
            expected_epoch: None,
        },
    };
    let bytes = a.encode_command(&claim, usize::MAX).unwrap();
    a.apply_batch(&[LogEntry {
        index: 1,
        term: 1,
        payload: EntryPayload::Command {
            operation: claim.operation,
            bytes: bytes.into(),
        },
    }])
    .unwrap();
    let parent = a.state.parent.as_ref().unwrap();
    let update = LogUpdate {
        group: a.mapping.parent,
        expected_revision: parent.revision,
        hard_state: crate::contracts::HardState {
            term: 1,
            voted_for: None,
        },
        commit_index: 1,
        suffix: Some(Suffix {
            from: 1,
            entries: vec![LogEntry {
                index: 1,
                term: 1,
                payload: EntryPayload::Noop,
            }]
            .into(),
        }),
        snapshot: None,
        snapshot_membership: None,
    };
    let mut groups = BTreeMap::from([(a.mapping.parent, parent.clone())]);
    apply_batch(
        &mut groups,
        &[LogMutation::Update(update.clone())],
        limits.parent_log,
    )
    .unwrap();
    let revision = groups[&a.mapping.parent].revision.get();
    let checkpoint = codec
        .encode_checkpoint(
            revision,
            &groups,
            limits.parent_log,
            limits.max_parent_checkpoint_bytes,
        )
        .unwrap();
    assert_eq!(checkpoint.len(), limits.max_parent_checkpoint_bytes - 100);
    assert_eq!(
        codec
            .decode_checkpoint(&checkpoint, limits.parent_log)
            .unwrap(),
        (revision, groups, checkpoint.len())
    );
    let command = Command {
        operation: OperationId::new(2).unwrap(),
        origin_configuration: ConfigurationId::new(1).unwrap(),
        representative: representative(),
        body: CommandBody::Mutate {
            expected_generation: parent.generation,
            update,
            origin: origin(&a),
        },
    };
    let bytes = a.encode_command(&command, usize::MAX).unwrap();
    let before = a.checkpoint(usize::MAX).unwrap();
    assert!(a
        .apply_batch(&[LogEntry {
            index: 2,
            term: 1,
            payload: EntryPayload::Command {
                operation: command.operation,
                bytes: bytes.into()
            }
        }])
        .is_err());
    assert_eq!(a.checkpoint(usize::MAX).unwrap(), before);
    assert_eq!(a.applied_index(), 1);
}
fn origin(a: &LogicalApplication<providers::LogProvider>) -> PersistOrigin {
    let store = StoreBinding {
        identity: a.mapping.store,
        session: representative().logical_session,
    };
    let owner = crate::runtime::RuntimeOwner {
        store,
        lane: ExecutionLaneId::new(1).unwrap(),
        generation: RuntimeGeneration::new(1).unwrap(),
    };
    PersistOrigin {
        request: crate::worker::WorkerTicket {
            binding: crate::worker::WorkerBinding {
                store,
                generation: StorageWorkerGeneration::new(1).unwrap(),
            },
            sequence: 1,
        },
        visit: crate::runtime::VisitTicket {
            owner,
            group: a.mapping.parent,
            sequence: 1,
        },
    }
}

#[test]
fn staged_response_capacity_never_exceeds_the_static_encoding_bound() {
    let mut a = app();
    claim(&mut a);
    for query in [
        LogicalQuery::Image,
        LogicalQuery::Metadata,
        LogicalQuery::Outcome(OperationId::new(1).unwrap()),
    ] {
        let value = a.read_at(1, query).unwrap();
        let bound = a.read_encoding_bound(&query).unwrap();
        let bytes = a.response_bytes(&value, usize::MAX).unwrap();
        assert!(bytes.len() <= bound);
        assert!(bytes.capacity() <= bound);
        assert_eq!(decode::read(&a, &bytes), value);
    }
}
