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
//! Consensus log transitions and the host-replaceable storage contract.
use super::*;

#[test]
fn subset_barrier_promotes_all_written_groups_and_returns_only_requested_tickets() {
    let io = ModelIo::default();
    let mut store = NativeLogStore::create(io.clone(), identity(1), LogLimits::default()).unwrap();
    append(
        &mut store,
        (1..=3)
            .map(|g| LogMutation::Create(bootstrap(g, 3)))
            .collect(),
    );
    let cold = store.state(group(3)).unwrap();
    let first = store
        .append_batch(vec![update(&store.state(group(1)).unwrap(), 1, 0, None)])
        .unwrap();
    let second = store
        .append_batch(vec![update(&store.state(group(2)).unwrap(), 2, 0, None)])
        .unwrap();
    assert_eq!(store.state(group(2)).unwrap().hard_state.term, 0);
    assert_eq!(store.barrier(&first).unwrap().tickets, first);
    assert_eq!(store.state(group(2)).unwrap().hard_state.term, 2);
    assert_eq!(store.state(group(3)).unwrap(), cold);
    let later = store
        .append_batch(vec![update(&store.state(group(1)).unwrap(), 3, 0, None)])
        .unwrap();
    assert_eq!(store.barrier(&second).unwrap().tickets, second);
    let expected = store.state(group(1)).unwrap();
    assert_eq!(expected.hard_state.term, 3);
    assert_eq!(store.barrier(&later).unwrap().tickets, later);
    assert_eq!(store.state(group(1)).unwrap(), expected);
    drop(store);
    io.0.borrow_mut().power_loss();
    let recovered = NativeLogStore::recover(io, identity(1), LogLimits::default()).unwrap();
    assert_eq!(recovered.state(group(1)).unwrap(), expected);
    assert_eq!(recovered.state(group(2)).unwrap().hard_state.term, 2);
    assert_eq!(recovered.state(group(3)).unwrap(), cold);
}

#[test]
fn replacement_invalidates_only_its_own_generation_and_keeps_other_group_tickets() {
    let mut store =
        NativeLogStore::create(ModelIo::default(), identity(1), LogLimits::default()).unwrap();
    append(
        &mut store,
        vec![
            LogMutation::Create(bootstrap(1, 3)),
            LogMutation::Create(bootstrap(2, 3)),
        ],
    );
    let old = store
        .append_batch(vec![update(
            &store.state(group(1)).unwrap(),
            1,
            0,
            Some(Suffix {
                from: 1,
                entries: vec![entry(1, 1, 1)],
            }),
        )])
        .unwrap();
    let other = store
        .append_batch(vec![update(&store.state(group(2)).unwrap(), 1, 0, None)])
        .unwrap();
    let mut accepted = store.state(group(1)).unwrap();
    accepted.revision = old[0].revision;
    let replacement = store
        .append_batch(vec![update(
            &accepted,
            2,
            0,
            Some(Suffix {
                from: 1,
                entries: vec![entry(1, 2, 2)],
            }),
        )])
        .unwrap();
    assert_eq!(store.barrier(&old), Err(StorageError::StaleTicket));
    assert_eq!(store.barrier(&other).unwrap().tickets, other);
    assert_eq!(store.state(group(1)).unwrap().entries, vec![entry(1, 2, 2)]);
    assert_eq!(store.barrier(&replacement).unwrap().tickets, replacement);
}

#[test]
fn uncertain_multi_group_append_or_barrier_fences_and_recovers_atomic_batch() {
    for fault in [
        Fault::Append(10),
        Fault::Sync,
        Fault::PublishBefore,
        Fault::PublishAfter,
    ] {
        let io = ModelIo::default();
        let mut store =
            NativeLogStore::create(io.clone(), identity(1), LogLimits::default()).unwrap();
        append(
            &mut store,
            (1..=3)
                .map(|g| LogMutation::Create(bootstrap(g, 3)))
                .collect(),
        );
        let before: Vec<_> = (1..=3).map(|g| store.state(group(g)).unwrap()).collect();
        io.0.borrow_mut().fault = fault;
        let mutations = (1..=2)
            .map(|g| update(&before[g - 1], g as u64, 0, None))
            .collect();
        match store.append_batch(mutations) {
            Err(StorageError::Uncertain(_)) => (),
            Ok(tickets) => assert!(matches!(
                store.barrier(&tickets),
                Err(StorageError::Uncertain(_))
            )),
            other => panic!("unexpected {other:?}"),
        }
        assert_eq!(store.state(group(1)), Err(StorageError::Fenced));
        drop(store);
        io.0.borrow_mut().power_loss();
        let recovered = NativeLogStore::recover(io, identity(1), LogLimits::default()).unwrap();
        let first = recovered.state(group(1)).unwrap();
        let second = recovered.state(group(2)).unwrap();
        assert!(
            (first == before[0] && second == before[1])
                || (first.hard_state.term == 1 && second.hard_state.term == 2)
        );
        assert_eq!(recovered.state(group(3)).unwrap(), before[2]);
    }
}
