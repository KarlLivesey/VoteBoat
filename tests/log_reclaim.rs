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
#![cfg(feature = "native")]
mod support;
use support::*;
use voteboat::{contracts::*, identity::*, log::*, native::log_store::*};

fn populated<I: JournalIo>(io: I) -> NativeLogStore<I> {
    let mut store = NativeLogStore::create(io, identity(1), LogLimits::default()).unwrap();
    append(
        &mut store,
        vec![
            LogMutation::Create(bootstrap(1, 3)),
            LogMutation::Create(bootstrap(2, 3)),
        ],
    );
    for term in 1..=20 {
        let first = store.state(group(1)).unwrap();
        let second = store.state(group(2)).unwrap();
        append(
            &mut store,
            vec![
                update(
                    &first,
                    term,
                    0,
                    Some(Suffix {
                        from: 1,
                        entries: vec![entry(1, term, term as u8)],
                    }),
                ),
                update(&second, term, 0, None),
            ],
        );
    }
    let first = store.state(group(1)).unwrap();
    let second = store.state(group(2)).unwrap();
    append(
        &mut store,
        vec![
            update(
                &first,
                20,
                1,
                Some(Suffix {
                    from: 2,
                    entries: vec![entry(2, 20, 2), entry(3, 20, 3)],
                }),
            ),
            LogMutation::Update(LogUpdate {
                snapshot_membership: None,
                group: group(2),
                expected_revision: second.revision,
                hard_state: HardState {
                    term: 20,
                    voted_for: Some(node(2)),
                },
                commit_index: 0,
                suffix: None,
                snapshot: None,
            }),
        ],
    );
    store
}
#[test]
fn rewrite_preserves_exact_live_state_and_never_reuses_tickets() {
    let io = ModelIo::default();
    let mut store = populated(io.clone());
    let before = [
        store.state(group(1)).unwrap(),
        store.state(group(2)).unwrap(),
    ];
    let binding = store.binding();
    let old_ticket = append(&mut store, vec![update(&before[0], 21, 1, None)])[0];
    let live = store.state(group(1)).unwrap();
    let report = store.reclaim(store.limits().max_wal_bytes).unwrap();
    assert!(report.after_bytes < report.before_bytes);
    assert_eq!(report.after_bytes, io.0.borrow().log.len());
    assert_eq!(store.binding(), binding);
    assert_eq!(store.state(group(1)).unwrap(), live);
    assert_eq!(store.state(group(2)).unwrap(), before[1]);
    assert_eq!(store.barrier(&[old_ticket]), Err(StorageError::StaleTicket));
    let next = append(&mut store, vec![update(&live, 22, 1, None)])[0];
    assert_eq!(next.batch, old_ticket.batch + 1);
    let expected = store.state(group(1)).unwrap();
    drop(store);
    io.0.borrow_mut().power_loss();
    let mut recovered = NativeLogStore::recover(io, identity(1), LogLimits::default()).unwrap();
    assert_eq!(recovered.state(group(1)).unwrap(), expected);
    assert_ne!(recovered.binding().session, binding.session);
    let ticket = append(&mut recovered, vec![update(&expected, 23, 1, None)])[0];
    assert_eq!(ticket.batch, next.batch + 1);
}
#[test]
fn replacement_failure_fences_and_power_loss_recovers_whole_old_or_new_state() {
    for fault in [Fault::ReplaceBefore, Fault::ReplaceAfter] {
        let io = ModelIo::default();
        let mut store = populated(io.clone());
        let expected = [
            store.state(group(1)).unwrap(),
            store.state(group(2)).unwrap(),
        ];
        io.0.borrow_mut().fault = fault;
        assert!(matches!(
            store.reclaim(store.limits().max_wal_bytes),
            Err(StorageError::Uncertain(_))
        ));
        assert_eq!(store.state(group(1)), Err(StorageError::Fenced));
        assert_eq!(store.reclaim(1024), Err(StorageError::Fenced));
        drop(store);
        io.0.borrow_mut().power_loss();
        let store = NativeLogStore::recover(io, identity(1), LogLimits::default()).unwrap();
        assert_eq!(store.state(group(1)).unwrap(), expected[0]);
        assert_eq!(store.state(group(2)).unwrap(), expected[1]);
    }
}
#[test]
fn pending_transitions_and_encoding_budget_reject_before_replacement() {
    let io = ModelIo::default();
    let mut store = populated(io.clone());
    let original = io.0.borrow().log.clone();
    assert!(matches!(store.reclaim(48), Err(StorageError::Rejected(_))));
    assert_eq!(io.0.borrow().log, original);
    let state = store.state(group(1)).unwrap();
    let tickets = store
        .append_batch(vec![update(&state, 21, 1, None)])
        .unwrap();
    assert!(matches!(
        store.reclaim(store.limits().max_wal_bytes),
        Err(StorageError::Rejected(_))
    ));
    store.barrier(&tickets).unwrap();
    assert!(
        store
            .reclaim(store.limits().max_wal_bytes)
            .unwrap()
            .after_bytes
            < original.len()
    );
    let current = io.0.borrow().log.clone();
    let report = store.reclaim(store.limits().max_wal_bytes).unwrap();
    assert_eq!(report.before_bytes, report.after_bytes);
    assert_eq!(current, io.0.borrow().log);
}
#[test]
fn image_codec_rejects_every_truncation_bit_corruption_and_invalid_state() {
    let io = ModelIo::default();
    let store = populated(io);
    let state = [
        (group(1), store.state(group(1)).unwrap()),
        (group(2), store.state(group(2)).unwrap()),
    ]
    .into_iter()
    .collect();
    let codec = NativeLogCodec;
    let bytes = codec
        .encode_checkpoint(22, &state, store.limits(), store.limits().max_wal_bytes)
        .unwrap();
    for cut in 0..bytes.len() {
        assert!(codec
            .decode_checkpoint(&bytes[..cut], store.limits())
            .is_err());
    }
    for index in 0..bytes.len() {
        let mut bad = bytes.clone();
        bad[index] ^= 1;
        assert!(codec.decode_checkpoint(&bad, store.limits()).is_err());
    }
    let mut invalid = state.clone();
    invalid.get_mut(&group(1)).unwrap().commit_index = 100;
    let bad = codec
        .encode_checkpoint(22, &invalid, store.limits(), store.limits().max_wal_bytes)
        .unwrap();
    assert!(codec.decode_checkpoint(&bad, store.limits()).is_err());
    let mut invalid = state.clone();
    invalid.get_mut(&group(1)).unwrap().generation = LogGeneration::new(100).unwrap();
    let bad = codec
        .encode_checkpoint(22, &invalid, store.limits(), store.limits().max_wal_bytes)
        .unwrap();
    assert!(codec.decode_checkpoint(&bad, store.limits()).is_err());
}
#[test]
fn native_files_keep_exclusive_selection_and_reclaim_repeatedly() {
    let path = std::env::temp_dir().join(format!("voteboat-reclaim-files-{}", std::process::id()));
    std::fs::create_dir_all(&path).unwrap();
    let mut store = populated(FileLogIo::create(&path).unwrap());
    assert!(FileLogIo::open(&path).is_err());
    for term in [21, 22, 23] {
        let before = store.state(group(1)).unwrap();
        for _ in 0..10 {
            let s = store.state(group(1)).unwrap();
            append(&mut store, vec![update(&s, term, 1, None)]);
        }
        let expected = store.state(group(1)).unwrap();
        assert!(expected.revision > before.revision);
        let report = store.reclaim(store.limits().max_wal_bytes).unwrap();
        assert!(report.after_bytes < report.before_bytes);
        drop(store);
        assert!(!path.join("log.wal").exists());
        store = NativeLogStore::recover(
            FileLogIo::open(&path).unwrap(),
            identity(1),
            LogLimits::default(),
        )
        .unwrap();
        assert_eq!(store.state(group(1)).unwrap(), expected);
    }
    drop(store);
    assert!(voteboat::native::vote_store::FileVoteIo::create(&path).is_err());
    // A stale legacy pair cannot rescue a missing selected replacement.
    let selected = path.join("log.1.wal");
    let bytes = std::fs::read(&selected).unwrap();
    std::fs::write(path.join("log.wal"), &bytes).unwrap();
    std::fs::copy(path.join("MANIFEST.1"), path.join("MANIFEST")).unwrap();
    std::fs::remove_file(&selected).unwrap();
    assert!(FileLogIo::open(&path).is_err());
    let mut corrupt = bytes.clone();
    corrupt[0] ^= 1;
    std::fs::write(&selected, corrupt).unwrap();
    assert!(NativeLogStore::recover(
        FileLogIo::open(&path).unwrap(),
        identity(1),
        LogLimits::default()
    )
    .is_err());
    std::fs::write(&selected, bytes).unwrap();
    std::fs::write(path.join("CURRENT"), b"bad selection").unwrap();
    assert!(FileLogIo::open(&path).is_err());
    assert!(FileLogIo::create(&path).is_err());
    std::fs::remove_dir_all(path).unwrap();
}

#[test]
fn live_image_can_exceed_one_batch_without_losing_cold_group_or_suffix() {
    let io = ModelIo::default();
    let limits = LogLimits {
        max_batch_bytes: 512,
        max_wal_bytes: 16384,
        max_command_bytes: 64,
        control_reserve_bytes: 512,
        ..Default::default()
    };
    let mut store = NativeLogStore::create(io.clone(), identity(1), limits).unwrap();
    append(
        &mut store,
        vec![
            LogMutation::Create(bootstrap(1, 3)),
            LogMutation::Create(bootstrap(2, 3)),
        ],
    );
    for index in 1..=24 {
        let s = store.state(group(1)).unwrap();
        append(
            &mut store,
            vec![update(
                &s,
                1,
                0,
                Some(Suffix {
                    from: index,
                    entries: vec![entry(index, 1, index as u8)],
                }),
            )],
        );
    }
    let before = [
        store.state(group(1)).unwrap(),
        store.state(group(2)).unwrap(),
    ];
    let report = store.reclaim(limits.max_wal_bytes).unwrap();
    assert!(
        report.after_bytes > limits.max_batch_bytes && report.after_bytes < report.before_bytes
    );
    drop(store);
    io.0.borrow_mut().power_loss();
    let store = NativeLogStore::recover(io, identity(1), limits).unwrap();
    assert_eq!(store.state(group(1)).unwrap(), before[0]);
    assert_eq!(store.state(group(2)).unwrap(), before[1]);
}
#[test]
fn unsupported_codec_and_host_store_decline_maintenance_before_mutation() {
    struct BatchOnly;
    impl LogCodec for BatchOnly {
        fn format_version(&self) -> u32 {
            2
        }
        fn encode_batch(
            &self,
            sequence: u64,
            mutations: &[LogMutation],
            limits: LogLimits,
        ) -> Result<Vec<u8>, StorageError> {
            NativeLogCodec.encode_batch(sequence, mutations, limits)
        }
        fn frame_length(&self, header: &[u8], limits: LogLimits) -> Result<usize, StorageError> {
            NativeLogCodec.frame_length(header, limits)
        }
        fn decode_batch(
            &self,
            bytes: &[u8],
            limits: LogLimits,
        ) -> Result<(u64, Vec<LogMutation>), StorageError> {
            NativeLogCodec.decode_batch(bytes, limits)
        }
    }
    let io = ModelIo::default();
    let mut store =
        NativeLogStore::create_with_codec(io.clone(), identity(1), LogLimits::default(), BatchOnly)
            .unwrap();
    append(&mut store, vec![LogMutation::Create(bootstrap(1, 3))]);
    let original = io.0.borrow().log.clone();
    assert!(matches!(
        store.reclaim(store.limits().max_wal_bytes),
        Err(StorageError::Rejected(_))
    ));
    assert_eq!(io.0.borrow().log, original);
    assert!(matches!(
        HostLogStore::new(1).reclaim(1024),
        Err(StorageError::Rejected(_))
    ));
}

#[test]
fn host_checkpoint_decoder_cannot_bypass_mandatory_log_invariants() {
    struct WrongState;
    impl LogCodec for WrongState {
        fn format_version(&self) -> u32 {
            2
        }
        fn encode_batch(
            &self,
            sequence: u64,
            mutations: &[LogMutation],
            limits: LogLimits,
        ) -> Result<Vec<u8>, StorageError> {
            NativeLogCodec.encode_batch(sequence, mutations, limits)
        }
        fn frame_length(&self, header: &[u8], limits: LogLimits) -> Result<usize, StorageError> {
            NativeLogCodec.frame_length(header, limits)
        }
        fn decode_batch(
            &self,
            bytes: &[u8],
            limits: LogLimits,
        ) -> Result<(u64, Vec<LogMutation>), StorageError> {
            NativeLogCodec.decode_batch(bytes, limits)
        }
        fn decode_checkpoint(
            &self,
            bytes: &[u8],
            limits: LogLimits,
        ) -> Result<
            (
                u64,
                std::collections::BTreeMap<GroupIdentity, GroupLog>,
                usize,
            ),
            StorageError,
        > {
            let (sequence, mut state, length) = NativeLogCodec.decode_checkpoint(bytes, limits)?;
            state.get_mut(&group(1)).unwrap().commit_index = 100;
            Ok((sequence, state, length))
        }
    }
    let io = ModelIo::default();
    let mut store = populated(io.clone());
    store.reclaim(store.limits().max_wal_bytes).unwrap();
    drop(store);
    let manifest = io.0.borrow().manifest.clone();
    assert!(matches!(
        NativeLogStore::recover_with_codec(
            io.clone(),
            identity(1),
            LogLimits::default(),
            WrongState
        ),
        Err(StorageError::Corrupt(_))
    ));
    assert_eq!(
        io.0.borrow().manifest,
        manifest,
        "reject before publishing a recovery session"
    );
}
