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
#[test]
fn one_group_stripes_original_bytes_and_reopens_exact_suffix_snapshot_and_sessions() {
    let (io, mut store) = seeded();
    let initial = store.state(group(1)).unwrap();
    let t = add(&mut store, 7, false);
    assert_eq!(
        store.state(group(1)).unwrap(),
        initial,
        "written is not durable"
    );
    assert_eq!(store.barrier(&t).unwrap().tickets, t);
    let s = store.state(group(1)).unwrap();
    assert_eq!(
        store
            .fetch_range(group(1), s.generation, 1, 1, 4096)
            .unwrap(),
        s.entries
    );
    let old = add(&mut store, 8, false);
    let accepted_revision = LogRevision::new(s.revision.get() + 1).unwrap();
    let replacement = store
        .append_batch(vec![LogMutation::Update(LogUpdate {
            group: group(1),
            expected_revision: accepted_revision,
            hard_state: HardState {
                term: 2,
                voted_for: Some(node(2)),
            },
            commit_index: 1,
            suffix: Some(Suffix {
                from: 2,
                entries: vec![entry(2, 2, 9)].into(),
            }),
            snapshot: None,
            snapshot_membership: None,
        })])
        .unwrap();
    assert_eq!(store.barrier(&old), Err(StorageError::StaleTicket));
    store.barrier(&replacement).unwrap();
    let expected = store.state(group(1)).unwrap();
    assert_ne!(expected.generation, s.generation);
    assert_eq!(
        store.fetch_range(group(1), s.generation, 1, 1, 4096),
        Err(StorageError::StaleTicket)
    );
    let binding = store.binding();
    drop(store);
    io.crash();
    let mut reopened = recover(io.clone()).unwrap();
    assert_eq!(reopened.state(group(1)).unwrap(), expected);
    assert!(reopened.binding().session > binding.session);
    assert_eq!(
        reopened.barrier(&replacement),
        Err(StorageError::StaleTicket)
    );
    let reference = voteboat::snapshot::SnapshotRef {
        store: identity(1),
        group: group(1),
        configuration: expected.bootstrap.configuration,
        generation: SnapshotGeneration::new(1).unwrap(),
        index: 1,
        term: 1,
        application_schema: 1,
        file_bytes: 100,
        checksum: 12,
    };
    // Raw LogStore snapshot precondition is a separately supplied durable pin.
    append(
        &mut reopened,
        vec![LogMutation::Update(LogUpdate {
            group: group(1),
            expected_revision: expected.revision,
            hard_state: expected.hard_state,
            commit_index: 1,
            suffix: None,
            snapshot: Some(reference),
            snapshot_membership: None,
        })],
    );
    let compacted = reopened.state(group(1)).unwrap();
    drop(reopened);
    io.crash();
    assert_eq!(
        recover(io.clone()).unwrap().state(group(1)).unwrap(),
        compacted
    );
    assert!(io.payload_bytes().iter().all(|bytes| *bytes > 0));
    let control = StripedJournal::inspect_control(
        &mut io.clone(),
        identity(1),
        LogLimits::default(),
        stripes(),
    )
    .unwrap();
    let c = &control.groups[&group(1)];
    assert_eq!(c.hard_state, compacted.hard_state);
    assert_eq!(c.ballot_origin, compacted.ballot_origin);
    assert_eq!(c.snapshot, compacted.snapshot);
    assert_eq!(c.generation, compacted.generation);
    assert_eq!(c.membership, compacted.membership().unwrap());
}
#[test]
fn mirrored_control_survives_either_device_loss_but_payload_loss_quarantines_recovery() {
    let (io, mut store) = seeded();
    let t = add(&mut store, 11, true);
    store.barrier(&t).unwrap();
    let s = store.state(group(1)).unwrap();
    append(
        &mut store,
        vec![LogMutation::Update(LogUpdate {
            group: group(1),
            expected_revision: s.revision,
            hard_state: HardState {
                term: 2,
                voted_for: Some(node(3)),
            },
            commit_index: s.commit_index,
            suffix: None,
            snapshot: None,
            snapshot_membership: None,
        })],
    );
    let expected = store.state(group(1)).unwrap();
    drop(store);
    for lost in 0..2 {
        let damaged = io.fork();
        damaged.crash();
        damaged.lose(lost);
        let control = StripedJournal::inspect_control(
            &mut damaged.clone(),
            identity(1),
            LogLimits::default(),
            stripes(),
        )
        .unwrap();
        assert_eq!(control.groups[&group(1)].hard_state, expected.hard_state);
        assert_eq!(
            control.groups[&group(1)].ballot_origin,
            expected.ballot_origin
        );
        assert_eq!(
            control.groups[&group(1)].commit_index,
            expected.commit_index
        );
        assert!(
            recover(damaged).is_err(),
            "missing selected payload cannot expose a voter"
        );
    }
}
#[test]
fn corrupted_controls_and_selected_payload_never_fall_back_to_older_bytes() {
    let (io, mut store) = seeded();
    let t = add(&mut store, 3, true);
    store.barrier(&t).unwrap();
    drop(store);
    for lane in 0..2 {
        let damaged = io.fork();
        damaged.crash();
        damaged.corrupt_payload(lane);
        assert!(recover(damaged).is_err());
        let damaged = io.fork();
        damaged.corrupt_catalog(lane);
        assert!(recover(damaged).is_err());
    }
}
