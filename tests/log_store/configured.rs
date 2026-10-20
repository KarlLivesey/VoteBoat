// SPDX-License-Identifier: RPL-1.5
// Copyright (c) 2026 Karl Livesey
//! Configured delta crash controls use the actual modeled durable byte frontier.
use super::*;
use std::collections::BTreeMap;
use voteboat::{identity::*, membership::*};

fn configured(io: ModelIo) -> (NativeLogStore<ModelIo>, Vec<LogTicket>) {
    let mut store = NativeLogStore::create(io, identity(1), LogLimits::default()).unwrap();
    append(&mut store, vec![LogMutation::Create(bootstrap(1, 3))]);
    let state = store.state(group(1)).unwrap();
    let learners = Configuration::new(
        ConfigurationId::new(2).unwrap(),
        state.bootstrap.policy.clone(),
        state.bootstrap.voter_stores.clone(),
        BTreeMap::from([(node(4), identity(4))]),
    )
    .unwrap();
    let record = LogEntry {
        index: 2,
        term: 1,
        payload: EntryPayload::Configuration(Box::new(ConfigurationRecord {
            operation: OperationId::new(500).unwrap(),
            expected: state.bootstrap.configuration,
            change: ConfigurationChange::Learners(learners),
        })),
    };
    let tickets = append(
        &mut store,
        vec![update(
            &state,
            1,
            2,
            Some(Suffix {
                from: 1,
                entries: vec![entry(1, 1, 7), record].into(),
            }),
        )],
    );
    (store, tickets)
}

#[test]
fn configured_delta_power_loss_respects_each_durability_phase_and_old_session_refusal() {
    for fault in [
        Fault::Append(10),
        Fault::Sync,
        Fault::PublishBefore,
        Fault::PublishAfter,
    ] {
        let io = ModelIo::default();
        let (mut store, original_tickets) = configured(io.clone());
        let before = store.state(group(1)).unwrap();
        let old_binding = store.binding();
        let incoming = update(
            &before,
            1,
            3,
            Some(Suffix {
                from: 3,
                entries: vec![entry(3, 1, 9)].into(),
            }),
        );
        let mut oracle = BTreeMap::from([(group(1), before.clone())]);
        apply_batch(
            &mut oracle,
            std::slice::from_ref(&incoming),
            LogLimits::default(),
        )
        .unwrap();
        io.0.borrow_mut().fault = fault;
        let accepted = match store.append_batch(vec![incoming]) {
            Err(StorageError::Uncertain(_)) => None,
            Ok(tickets) => {
                assert!(matches!(
                    store.barrier(&tickets),
                    Err(StorageError::Uncertain(_))
                ));
                Some(tickets)
            }
            other => panic!("unexpected admission {other:?}"),
        };
        assert_eq!(store.state(group(1)), Err(StorageError::Fenced));
        assert_eq!(store.barrier(&original_tickets), Err(StorageError::Fenced));
        drop(store);
        io.0.borrow_mut().power_loss();
        let mut recovered = NativeLogStore::recover(io, identity(1), LogLimits::default()).unwrap();
        let expected = match fault {
            Fault::Append(_) | Fault::Sync => &before,
            Fault::PublishBefore | Fault::PublishAfter => &oracle[&group(1)],
            _ => unreachable!(),
        };
        assert_eq!(&recovered.state(group(1)).unwrap(), expected);
        assert_eq!(recovered.binding().identity, old_binding.identity);
        assert_ne!(recovered.binding().session, old_binding.session);
        assert_eq!(
            recovered.barrier(&original_tickets),
            Err(StorageError::StaleTicket)
        );
        if let Some(tickets) = accepted {
            assert_eq!(recovered.barrier(&tickets), Err(StorageError::StaleTicket));
        }
    }
}

/// ST2 round 3 (I5): `LogUpdate::wal_bound`, which the runtime uses for exact
/// in-flight accounting against the WAL high-water mark, is never below the
/// native codec's encoding: commands of every size class, noops, a
/// configuration entry with a learner set, and a truncation-only update.
#[test]
fn wal_bound_covers_the_native_encoding_of_every_update_shape() {
    let state = {
        let mut store =
            NativeLogStore::create(ModelIo::default(), identity(1), LogLimits::default()).unwrap();
        append(&mut store, vec![LogMutation::Create(bootstrap(1, 3))]);
        store.state(group(1)).unwrap()
    };
    let learners = Configuration::new(
        ConfigurationId::new(2).unwrap(),
        state.bootstrap.policy.clone(),
        state.bootstrap.voter_stores.clone(),
        (4..=40)
            .map(|n| (node(n), identity(u128::from(n))))
            .collect(),
    )
    .unwrap();
    let configuration = LogEntry {
        index: 1,
        term: 1,
        payload: EntryPayload::Configuration(Box::new(ConfigurationRecord {
            operation: OperationId::new(500).unwrap(),
            expected: state.bootstrap.configuration,
            change: ConfigurationChange::Learners(learners),
        })),
    };
    let command = |index: u64, len: usize| LogEntry {
        index,
        term: 1,
        payload: EntryPayload::Command {
            operation: OperationId::new(u128::MAX - u128::from(index)).unwrap(),
            bytes: vec![9; len].into(),
        },
    };
    let noop = |index: u64| LogEntry {
        index,
        term: 1,
        payload: EntryPayload::Noop,
    };
    let shapes: Vec<Option<Vec<LogEntry>>> = vec![
        None,
        Some(Vec::new()),
        Some(vec![noop(1)]),
        Some(vec![configuration]),
        Some((1..=256).map(|i| command(i, 0)).collect()),
        Some((1..=64).map(|i| command(i, 4096)).collect()),
        Some(vec![command(1, 64 * 1024)]),
        Some(
            (1..=128)
                .map(|i| if i % 2 == 0 { noop(i) } else { command(i, 1) })
                .collect(),
        ),
    ];
    for entries in shapes {
        let mutation = update(
            &state,
            1,
            0,
            entries.map(|entries| Suffix {
                from: 1,
                entries: entries.into(),
            }),
        );
        let LogMutation::Update(u) = &mutation else {
            unreachable!()
        };
        let bound = u.wal_bound();
        let encoded = NativeLogCodec
            .encode_batch(1, std::slice::from_ref(&mutation), LogLimits::default())
            .unwrap()
            .len();
        assert!(encoded <= bound, "encoded {encoded} > bound {bound}");
    }
}
